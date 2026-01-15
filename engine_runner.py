# engine_runner.py
# Binance USDT Perpetual RSI Scanner + Telegram Alert + Auto SHORT + Exit on 5m EMA20 touch (ROI>=0)
#
# Entry (SHORT only):
# - Universe: 24h quoteVolume >= 30,000,000 USDT AND |24h % change| top 40 (BTC/ETH anchors 포함)
# - RSI thresholds: 3m>=80 (prev>current로 꺾임), 5m>=76, 15m>=75, 1h>=73 (cascade)
# - 5m 구조 거절: 최근 2~3개 5m 캔들 고점 갱신 실패 혹은 연속 윗꼬리
# - 5m 거래량 급증: 현재 ≥ 최근 20봉 평균 × 1.2
# - edge trigger + per-symbol cooldown
#
# Exit:
# - last 5m candle HIGH touches EMA20 (high >= EMA20) AND ROI >= 0 -> close short market
#
# Manual close sync:
# - if you close manually, bot detects position=0 and resets state + cancels open orders
import time
import unicodedata
import bisect
import calendar
import json
import os
import threading
import builtins
import sys
import traceback
import importlib
import re
import copy
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Any

import ccxt
import pandas as pd
import numpy as np
import requests
import cycle_cache

try:
    import ws_manager
except Exception:
    ws_manager = None
try:
    import db_recorder as dbrec
except Exception:
    dbrec = None
try:
    import db_reconcile as dbrecon
except Exception:
    dbrecon = None
try:
    import db_pnl_report as dbpnl
except Exception:
    dbpnl = None
try:
    import executor as executor_mod
except Exception:
    executor_mod = None
from env_loader import load_env
try:
    from engines.fabio import fabio_entry_engine
    from engines.fabio import atlas_fabio_engine
except Exception:
    fabio_entry_engine = None
    atlas_fabio_engine = None
try:
    from engines.base import EngineContext
    try:
        from engines.swaggy.swaggy_engine import SwaggyEngine, SwaggyConfig
        from engines.swaggy.logs import format_cut_top, format_zone_stats
    except Exception:
        SwaggyEngine = None
        SwaggyConfig = None
        format_cut_top = None
        format_zone_stats = None
    from engines.swaggy_atlas_lab.swaggy_signal import SwaggySignalEngine as SwaggyAtlasLabEngine
    from engines.swaggy_atlas_lab.config import SwaggyConfig as SwaggyAtlasLabConfig
    from engines.swaggy_atlas_lab.config import AtlasConfig as SwaggyAtlasLabAtlasConfig
    from engines.swaggy_atlas_lab.atlas_eval import (
        evaluate_global_gate as lab_evaluate_global_gate,
        evaluate_local as lab_evaluate_local,
    )
    from engines.swaggy_atlas_lab.policy import apply_policy as lab_apply_policy
    from engines.swaggy_atlas_lab.policy import AtlasMode as SwaggyAtlasLabMode
    from engines.atlas.atlas_engine import AtlasEngine, AtlasSwaggyConfig
    from engines.rsi.engine import RsiEngine
    from engines.div15m_long.engine import Div15mLongEngine
    from engines.div15m_short.engine import Div15mShortEngine
    from engines.universe import build_universe_from_tickers
    from engines.dtfx.engine import DTFXEngine, DTFXConfig
    from engines.dtfx.core.logger import write_dtfx_log
    from engines.pumpfade.engine import PumpFadeEngine
    from engines.pumpfade.config import PumpFadeConfig
    from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine
    from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig
except Exception:
    SwaggyEngine = None
    SwaggyConfig = None
    EngineContext = None
    format_cut_top = None
    format_zone_stats = None
    SwaggyAtlasLabEngine = None
    SwaggyAtlasLabConfig = None
    SwaggyAtlasLabAtlasConfig = None
    lab_evaluate_global_gate = None
    lab_evaluate_local = None
    lab_apply_policy = None
    SwaggyAtlasLabMode = None
    AtlasEngine = None
    AtlasSwaggyConfig = None
    RsiEngine = None
    Div15mLongEngine = None
    Div15mShortEngine = None
    build_universe_from_tickers = None
    DTFXEngine = None
    DTFXConfig = None
    write_dtfx_log = None
    PumpFadeEngine = None
    PumpFadeConfig = None
    AtlasRsFailShortEngine = None
    AtlasRsFailShortConfig = None

from executor import (
    get_short_position_amount,
    get_short_roi_pct,
    get_short_position_detail,
    get_long_position_amount,
    get_long_position_detail,
    count_open_positions,
    list_open_position_symbols,
    is_hedge_mode,
    short_market,
    short_limit,
    long_market,
    close_short_market,
    close_long_market,
    cancel_open_orders,
    cancel_stop_orders,
    cancel_conditional_by_side,
    dca_short_if_needed,
    dca_long_if_needed,
    DCA_ENABLED,
    DCA_PCT,
    DCA_FIRST_PCT,
    DCA_SECOND_PCT,
    DCA_THIRD_PCT,
    set_dry_run,
    get_global_backoff_until,
    refresh_positions_cache,
    get_available_usdt,
)

swaggy_engine = None
swaggy_atlas_lab_engine = None
atlas_engine = None
atlas_swaggy_cfg = None
rsi_engine = None
div15m_engine = None
div15m_short_engine = None
dtfx_engine = None
pumpfade_engine = None
atlas_rs_fail_short_engine = None

load_env()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
# 런타임 전송용 chat_id (state에서 복원/업데이트)
CHAT_ID_RUNTIME = CHAT_ID
START_TIME = time.time()
TELEGRAM_STARTUP_GRACE_SEC = 15.0
EXIT_ICON = os.getenv("EXIT_ICON", "✅")

def print_section(title: str) -> None:
    print(f"[{title}]")
    print("---")

_PRINT_ORIG = builtins.print
_THREAD_LOG = threading.local()
_ENTRY_LOCK_MUTEX = threading.Lock()
_ENTRY_SEEN_MUTEX = threading.Lock()

def _log_error(msg: str) -> None:
    try:
        os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs", "error.log"), "a", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass

def _install_error_hooks() -> None:
    def _handle_excepthook(exc_type, exc, tb):
        text = "".join(traceback.format_exception(exc_type, exc, tb)).rstrip()
        _log_error(text)
        _PRINT_ORIG(text, file=sys.stderr)
    def _handle_thread_excepthook(args):
        text = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)).rstrip()
        _log_error(text)
        _PRINT_ORIG(text, file=sys.stderr)
    sys.excepthook = _handle_excepthook
    if hasattr(threading, "excepthook"):
        threading.excepthook = _handle_thread_excepthook

def _buffered_print(*args, **kwargs) -> None:
    buf = getattr(_THREAD_LOG, "buffer", None)
    file = kwargs.get("file")
    if buf is not None and (file is None or file is sys.stdout):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        buf.append(sep.join(str(a) for a in args) + end)
        return
    _PRINT_ORIG(*args, **kwargs)

def _set_thread_log_buffer(buf: Optional[list]) -> None:
    if buf is None:
        if hasattr(_THREAD_LOG, "buffer"):
            delattr(_THREAD_LOG, "buffer")
        return
    _THREAD_LOG.buffer = buf

builtins.print = _buffered_print

CYCLE_OHLCV_RAW_CACHE = cycle_cache.RAW_OHLCV
CURRENT_CYCLE_CACHE = cycle_cache.DF_CACHE
CYCLE_IND_CACHE = cycle_cache.IND_CACHE

class CachedExchange:
    def __init__(self, ex):
        self._ex = ex

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200):
        key = (symbol, timeframe)
        cached = CYCLE_OHLCV_RAW_CACHE.get(key)
        if cached is not None:
            return cached.get("data") or []
        miss = CURRENT_CYCLE_STATS.setdefault("raw_cache_miss", 0)
        CURRENT_CYCLE_STATS["raw_cache_miss"] = miss + 1
        return []

    def __getattr__(self, name):
        return getattr(self._ex, name)

def _kst_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=9)

def _ts_to_kst_str(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "unknown"

def _now_kst_str() -> str:
    try:
        dt = datetime.now(timezone.utc) + timedelta(hours=9)
        return dt.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return "unknown"

def _date_str_kst(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "unknown"

def _report_day_str(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "unknown"

def _get_trade_log(state: Dict[str, dict]) -> list:
    log = state.get("_trade_log")
    if not isinstance(log, list):
        log = []
        state["_trade_log"] = log
    return log

def _get_entry_guard(state: Dict[str, dict]) -> Dict[str, float]:
    guard = state.get("_entry_guard")
    if not isinstance(guard, dict):
        guard = {}
        state["_entry_guard"] = guard
    return guard

_ENTRY_PCT_WARN_TS = 0.0

def _resolve_entry_usdt(pct: Optional[float] = None) -> float:
    """entry_usdt는 사용가능 USDT 대비 퍼센트로 해석한다."""
    global _ENTRY_PCT_WARN_TS
    try:
        pct_val = float(USDT_PER_TRADE if pct is None else pct)
    except Exception:
        return 0.0
    if pct_val <= 0:
        return 0.0
    avail = None
    try:
        avail = get_available_usdt()
    except Exception:
        avail = None
    if not isinstance(avail, (int, float)) or avail <= 0:
        if executor_mod and getattr(executor_mod, "DRY_RUN", False):
            try:
                avail = float(os.getenv("SIM_USDT_BALANCE", "1000"))
            except Exception:
                avail = 1000.0
        else:
            return 0.0
    effective_pct = pct_val
    if effective_pct > 100.0:
        now = time.time()
        if (now - _ENTRY_PCT_WARN_TS) > 60.0:
            print(f"[entry_usdt] pct too high ({effective_pct:.2f}%), clamped to 100%")
            _ENTRY_PCT_WARN_TS = now
        effective_pct = 100.0
    return max(0.0, float(avail) * (effective_pct / 100.0))


def _get_entry_lock(state: Dict[str, dict]) -> Dict[str, dict]:
    lock = state.get("_entry_lock")
    if not isinstance(lock, dict):
        lock = {}
        state["_entry_lock"] = lock
    return lock

def _recent_auto_exit(state: Dict[str, dict], symbol: str, now_ts: float) -> bool:
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    if not isinstance(st, dict):
        return False
    for side in ("LONG", "SHORT"):
        last_exit_ts = _get_last_exit_ts_by_side(st, side)
        last_exit_reason = _get_last_exit_reason_by_side(st, side)
        if not isinstance(last_exit_ts, (int, float)):
            continue
        if last_exit_reason not in ("auto_exit_tp", "auto_exit_sl"):
            continue
        if (now_ts - float(last_exit_ts)) <= MANUAL_CLOSE_GRACE_SEC:
            return True
    return False

def _should_sample_short_position(symbol: str, st: Dict[str, Any], now_ts: float) -> bool:
    last_entry = float(st.get("last_entry", 0.0) or 0.0)
    if last_entry and (now_ts - last_entry) < SHORT_POS_SAMPLE_RECENT_SEC:
        return True
    if SHORT_POS_SAMPLE_DIV <= 0:
        return False
    salt = abs(hash(symbol)) % SHORT_POS_SAMPLE_DIV
    return int(now_ts) % SHORT_POS_SAMPLE_DIV == salt

def _fmt_pct_safe(val: Any) -> str:
    try:
        return f"{float(val):.2f}%"
    except Exception:
        return "N/A"

def _extract_order_id(order: Optional[dict]) -> Optional[str]:
    if not isinstance(order, dict):
        return None
    oid = order.get("id") or order.get("orderId")
    if oid:
        return str(oid)
    info = order.get("info") or {}
    oid = info.get("orderId") or info.get("id")
    if oid:
        return str(oid)
    return None

def _order_id_from_res(res: Optional[dict]) -> Optional[str]:
    if not isinstance(res, dict):
        return None
    oid = res.get("order_id") or res.get("orderId") or res.get("id")
    if oid:
        return str(oid)
    return _extract_order_id(res.get("order"))

def _fmt_float(val: Any, ndigits: int = 4) -> str:
    try:
        return f"{float(val):.{ndigits}f}"
    except Exception:
        return "N/A"

def _fmt_price_safe(entry_price: Any, sl_pct: Any, side: str = "LONG") -> str:
    try:
        entry = float(entry_price)
        pct = float(sl_pct) / 100.0
        if entry <= 0:
            return "N/A"
        if side.upper() == "SHORT":
            price = entry * (1.0 + pct)
        else:
            price = entry * (1.0 - pct)
        return f"{price:.6g}"
    except Exception:
        return "N/A"

def _fmt_entry_price(val: Any) -> str:
    try:
        return f"{float(val):.6g}"
    except Exception:
        return "N/A"

def _format_order_id_block(entry_order_id: Optional[str], exit_order_id: Optional[str] = None) -> str:
    lines = []
    if entry_order_id:
        lines.append(f"entry_id={entry_order_id}")
    if exit_order_id:
        lines.append(f"exit_id={exit_order_id}")
    return "\n".join(lines)

def _send_entry_alert(
    send_alert,
    side: str,
    symbol: str,
    engine: str,
    entry_price: Any = None,
    usdt: Any = None,
    reason: Optional[str] = None,
    sl: Optional[str] = None,
    tp: Optional[str] = None,
    live: Optional[bool] = None,
    order_info: Optional[str] = None,
    entry_order_id: Optional[str] = None,
    extras: Optional[list] = None,
) -> None:
    if not send_alert:
        return
    side_key = (side or "").upper()
    icon = "🟢" if side_key == "LONG" else "🔴" if side_key == "SHORT" else "⚪"
    side_label = "롱" if side_key == "LONG" else "숏" if side_key == "SHORT" else side_key
    lines = [f"{icon} <b>{side_label} 시그널</b>", f"<b>{symbol}</b>"]
    if entry_order_id:
        lines.append(f"entry_id={entry_order_id}")
    if entry_price is not None or usdt is not None:
        entry_disp = _fmt_entry_price(entry_price)
        if usdt is not None:
            try:
                usdt_disp = f"{float(usdt):.0f}"
            except Exception:
                usdt_disp = "N/A"
            lines.append(f"진입가≈{entry_disp} (USDT {usdt_disp})")
        else:
            lines.append(f"진입가≈{entry_disp}")
    sl_disp = sl if sl else "N/A"
    tp_disp = tp if tp else "N/A"
    lines.append(f"손절가={sl_disp} 익절가={tp_disp}")
    lines.append(f"엔진: {_display_engine_label(engine)}")
    reason_disp = reason if (reason and str(reason).strip()) else "N/A"
    lines.append(f"사유: {reason_disp}")
    send_alert("\n".join(lines))


def _compute_impulse_metrics(symbol: str) -> Optional[dict]:
    df = cycle_cache.get_df(symbol, "5m", max(FABIO_LONG_BB_PERIOD * 2, 30))
    if df.empty or len(df) < FABIO_LONG_BB_PERIOD + FABIO_LONG_IMPULSE_ATR_PERIOD:
        return None
    df = df.iloc[:-1]
    if len(df) < FABIO_LONG_BB_PERIOD + FABIO_LONG_IMPULSE_ATR_PERIOD:
        return None
    row = df.iloc[-1]
    body = abs(float(row["close"]) - float(row["open"]))
    highs = df["high"].astype(float)
    lows = df["low"].astype(float)
    closes = df["close"].astype(float)
    tr_list = []
    for i in range(1, len(df)):
        prev_close = float(closes.iloc[i - 1])
        tr_list.append(
            max(
                float(highs.iloc[i]) - float(lows.iloc[i]),
                abs(float(highs.iloc[i]) - prev_close),
                abs(float(lows.iloc[i]) - prev_close),
            )
        )
    if len(tr_list) < FABIO_LONG_IMPULSE_ATR_PERIOD:
        return None
    atr = float(np.mean(tr_list[-FABIO_LONG_IMPULSE_ATR_PERIOD:]))
    bb = closes.rolling(window=FABIO_LONG_BB_PERIOD)
    sma = bb.mean()
    std = bb.std()
    if sma.iloc[-1] is None or std.iloc[-1] is None:
        return None
    upper = sma.iloc[-1] + 2.0 * std.iloc[-1]
    bb_pos = float(row["close"]) >= upper
    return {
        "body": body,
        "atr": atr,
        "upper": upper,
        "bb_pos": bb_pos,
    }

def _get_entry_seen(state: Dict[str, dict]) -> Dict[str, dict]:
    seen = state.get("_entry_seen")
    if not isinstance(seen, dict):
        seen = {}
        state["_entry_seen"] = seen
    return seen


def _get_entry_seen_log(state: Dict[str, dict]) -> Dict[str, dict]:
    cache = state.get("_entry_seen_log")
    if not isinstance(cache, dict):
        cache = {}
        state["_entry_seen_log"] = cache
    return cache

def _entry_seen_acquire(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    engine: str,
    ttl_sec: float = 60.0,
) -> tuple[bool, str]:
    now = time.time()
    side = (side or "").upper()
    engine = (engine or "unknown").lower()
    key_side = f"{symbol}|{side}"
    key_engine = f"{symbol}|{side}|{engine}"
    with _ENTRY_SEEN_MUTEX:
        seen = _get_entry_seen(state)
        for key in (key_side, key_engine):
            rec = seen.get(key)
            if isinstance(rec, dict):
                ts = float(rec.get("ts", 0.0) or 0.0)
                if (now - ts) < ttl_sec:
                    blocked_by = str(rec.get("engine") or "unknown")
                    log_key = f"{key}|{blocked_by}"
                    log_cache = _get_entry_seen_log(state)
                    last_log_ts = float(log_cache.get(log_key, 0.0) or 0.0)
                    if (now - last_log_ts) >= ttl_sec:
                        _append_entry_gate_log(engine, symbol, f"중복차단=entry_seen_by:{blocked_by} side={side}", side=side)
                        log_cache[log_key] = now
                    return False, blocked_by
        seen[key_side] = {"ts": now, "engine": engine}
        seen[key_engine] = {"ts": now, "engine": engine}
    return True, "ok"


def _entry_seen_mark(state: Dict[str, dict], symbol: str, side: str, engine: str) -> None:
    now = time.time()
    side = (side or "").upper()
    engine = (engine or "unknown").lower()
    key_side = f"{symbol}|{side}"
    key_engine = f"{symbol}|{side}|{engine}"
    with _ENTRY_SEEN_MUTEX:
        seen = _get_entry_seen(state)
        seen[key_side] = {"ts": now, "engine": engine}
        seen[key_engine] = {"ts": now, "engine": engine}


def _entry_seen_blocked(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    engine: str,
    ttl_sec: float = 60.0,
) -> bool:
    now = time.time()
    side = (side or "").upper()
    engine = (engine or "unknown").lower()
    key_side = f"{symbol}|{side}"
    key_engine = f"{symbol}|{side}|{engine}"
    with _ENTRY_SEEN_MUTEX:
        seen = _get_entry_seen(state)
        for key in (key_side, key_engine):
            rec = seen.get(key)
            if isinstance(rec, dict):
                ts = float(rec.get("ts", 0.0) or 0.0)
                if (now - ts) < ttl_sec:
                    blocked_by = str(rec.get("engine") or "unknown")
                    log_key = f"{key}|{blocked_by}"
                    log_cache = _get_entry_seen_log(state)
                    last_log_ts = float(log_cache.get(log_key, 0.0) or 0.0)
                    if (now - last_log_ts) >= ttl_sec:
                        _append_entry_gate_log(engine, symbol, f"중복차단=entry_seen_by:{blocked_by} side={side}", side=side)
                        log_cache[log_key] = now
                    return True
    return False

def _append_entry_log(path: str, line: str) -> None:
    try:
        full_path = os.path.join("logs", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(full_path, "a", encoding="utf-8") as f:
            f.write(f"{ts} {line}\n")
    except Exception:
        pass

def _append_entry_gate_log(engine: str, symbol: str, reason: str, side: Optional[str] = None) -> None:
    try:
        date_tag = time.strftime("%Y-%m-%d")
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        path = os.path.join("entry_gate", f"entry_gate-{date_tag}.log")
        line = f"{ts} engine={engine or 'unknown'} symbol={symbol} reason={reason or 'unknown'}"
        if side and "side=" not in line:
            line = f"{line} side={side}"
        _append_log_lines(path, [line])
    except Exception:
        pass

def _ensure_log_file(path: str) -> None:
    try:
        full_path = os.path.join("logs", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "a", encoding="utf-8"):
            pass
    except Exception:
        pass

def _append_atlas_route_log(engine: str, symbol: str, payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.join("logs", "atlas"), exist_ok=True)
        date_tag = time.strftime("%Y%m%d")
        path = os.path.join("logs", "atlas", f"atlas_{date_tag}.log")
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        def _val(v: Any) -> str:
            if v is None or v == "":
                return "N/A"
            if isinstance(v, bool):
                return "1" if v else "0"
            return str(v)

        parts = [f"ts={ts}", f"engine={engine}", f"symbol={symbol}"]
        for key in (
            "side",
            "dir",
            "state",
            "regime",
            "reason",
            "trade_allowed",
            "allow_long",
            "allow_short",
            "size_mult",
            "block_reason",
        ):
            if key in payload:
                parts.append(f"{key}={_val(payload.get(key))}")
        with open(path, "a", encoding="utf-8") as f:
            f.write(" ".join(parts) + "\n")
    except Exception:
        pass

def _append_log_lines(path: str, lines: list) -> None:
    if not lines:
        return
    try:
        full_path = os.path.join("logs", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(line.rstrip("\n") + "\n")
    except Exception:
        pass

def _append_rsi_detail_log(line: str) -> None:
    try:
        date_tag = time.strftime("%Y%m%d")
        full_path = os.path.join("logs", "rsi", f"rsi_detail_{date_tag}.log")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
    except Exception:
        pass

def _append_atlas_rs_fail_short_log(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    path = f"atlas_rs_fail_short/atlas_rs_fail_short-{date_tag}.log"
    _append_log_lines(path, [line])


def _append_swaggy_atlas_lab_log(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    path = os.path.join("swaggy_atlas_lab", f"swaggy_atlas_lab-{date_tag}.log")
    _append_log_lines(path, [line])

def _run_swaggy_atlas_lab_cycle(
    swaggy_atlas_lab_engine,
    swaggy_universe,
    cached_ex,
    state,
    swaggy_cfg,
    atlas_cfg,
    active_positions_total,
    send_alert,
):
    result = {"long_hits": 0, "short_hits": 0}
    if (not SWAGGY_ATLAS_LAB_ENABLED) or (not swaggy_atlas_lab_engine) or (not swaggy_cfg) or (not atlas_cfg):
        return result
    if not swaggy_universe:
        return result

    now_ts = time.time()
    btc_df = cycle_cache.get_df(atlas_cfg.ref_symbol, swaggy_cfg.tf_mtf, limit=200)
    if btc_df is None:
        btc_df = pd.DataFrame()

    ltf_limit = max(int(swaggy_cfg.ltf_limit), 120)
    mtf_limit = 200
    htf_limit = max(int(swaggy_cfg.vp_lookback_1h), 120)
    htf2_limit = 200
    d1_limit = 120

    for symbol in swaggy_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if st.get("in_pos"):
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        try:
            long_amt = get_long_position_amount(symbol)
        except Exception:
            long_amt = 0.0
        try:
            short_amt = get_short_position_amount(symbol)
        except Exception:
            short_amt = 0.0
        if (long_amt > 0) or (short_amt > 0):
            st["in_pos"] = True
            now_seen = time.time()
            if long_amt > 0:
                _set_last_entry_state(st, "LONG", now_seen)
            if short_amt > 0:
                _set_last_entry_state(st, "SHORT", now_seen)
            state[symbol] = st
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        df_5m = cycle_cache.get_df(symbol, swaggy_cfg.tf_ltf, limit=ltf_limit)
        df_15m = cycle_cache.get_df(symbol, swaggy_cfg.tf_mtf, limit=mtf_limit)
        df_1h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf, limit=htf_limit)
        df_4h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf2, limit=htf2_limit)
        df_1d = cycle_cache.get_df(symbol, swaggy_cfg.tf_d1, limit=d1_limit)
        df_3m = cycle_cache.get_df(symbol, "3m", limit=30)
        if df_5m.empty or df_15m.empty or df_1h.empty or df_4h.empty or df_3m.empty:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        prev_phase = swaggy_atlas_lab_engine._state.get(symbol, {}).get("phase")
        signal = swaggy_atlas_lab_engine.evaluate_symbol(
            symbol,
            df_4h,
            df_1h,
            df_15m,
            df_5m,
            df_3m,
            df_1d if isinstance(df_1d, pd.DataFrame) else pd.DataFrame(),
            now_ts,
        )
        new_phase = swaggy_atlas_lab_engine._state.get(symbol, {}).get("phase")
        if prev_phase != new_phase and new_phase:
            _append_swaggy_atlas_lab_log(
                "SWAGGY_ATLAS_LAB_PHASE sym=%s prev=%s now=%s reasons=%s"
                % (symbol, prev_phase, new_phase, ",".join(signal.reasons or []))
            )

        if not signal.entry_ok or not signal.side or signal.entry_px is None:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        side = signal.side.upper()
        last_entry = _get_last_entry_ts_by_side(st, side)
        if isinstance(last_entry, (int, float)) and (now_ts - float(last_entry)) < COOLDOWN_SEC:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if _exit_cooldown_blocked(state, symbol, "swaggy_atlas_lab", side):
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        gate = None
        atlas = None
        if lab_evaluate_global_gate and lab_evaluate_local and not btc_df.empty:
            gate = lab_evaluate_global_gate(btc_df, atlas_cfg)
            atlas = lab_evaluate_local(symbol, side, df_15m, btc_df, gate, atlas_cfg)
        policy = lab_apply_policy(SwaggyAtlasLabMode.HARD, USDT_PER_TRADE, atlas) if lab_apply_policy else None
        if policy is None:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_line = (
            "SWAGGY_ATLAS_LAB_ENTRY sym=%s side=%s sw_strength=%.3f sw_reasons=%s "
            "final_usdt=%.2f atlas_pass=%s atlas_mult=%s atlas_reasons=%s policy_action=%s"
            % (
                symbol,
                side,
                float(signal.strength or 0.0),
                ",".join(signal.reasons or []),
                float(policy.final_usdt or 0.0),
                policy.atlas_pass if policy.atlas_pass is not None else "N/A",
                policy.atlas_mult if policy.atlas_mult is not None else "N/A",
                ",".join(policy.atlas_reasons or []),
                policy.policy_action or "N/A",
            )
        )
        _append_swaggy_atlas_lab_log(entry_line)
        if not policy.allow:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        cur_total = count_open_positions(force=True)
        if not isinstance(cur_total, int):
            cur_total = active_positions_total
        if cur_total >= MAX_OPEN_POSITIONS:
            _append_entry_gate_log(
                "swaggy_atlas_lab",
                symbol,
                f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side={side}",
                side=side,
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="swaggy_atlas_lab", side=side)
        if not lock_ok:
            _append_swaggy_atlas_lab_log(
                f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=ENTRY_LOCK owner={lock_owner} age_s={lock_age:.1f}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        guard_key = _entry_guard_key(state, symbol, side)
        if not _entry_guard_acquire(state, symbol, key=guard_key, engine="swaggy_atlas_lab", side=side):
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_usdt = _resolve_entry_usdt(policy.final_usdt)
        entry_order_id = None
        fill_price = None
        qty = None
        try:
            if side == "LONG":
                if LONG_LIVE_TRADING:
                    res = long_market(
                        symbol,
                        usdt_amount=entry_usdt,
                        leverage=LEVERAGE,
                        margin_mode=MARGIN_MODE,
                    )
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    st["in_pos"] = True
                    _set_last_entry_state(st, "LONG", time.time())
                    state[symbol] = st
                    _log_trade_entry(
                        state,
                        side="LONG",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=entry_usdt,
                        entry_order_id=entry_order_id,
                        meta={"reason": "swaggy_atlas_lab"},
                    )
                    result["long_hits"] += 1
                else:
                    result["long_hits"] += 1
                if send_alert:
                    _send_entry_alert(
                        send_alert,
                        side="LONG",
                        symbol=symbol,
                        engine="SWAGGY_ATLAS_LAB",
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        usdt=entry_usdt,
                        reason="SWAGGY_ATLAS_LAB",
                        live=LONG_LIVE_TRADING,
                        entry_order_id=entry_order_id,
                        sl=_fmt_price_safe(fill_price, AUTO_EXIT_LONG_SL_PCT, side="LONG"),
                        tp=None,
                    )
            else:
                if LIVE_TRADING:
                    res = short_market(
                        symbol,
                        usdt_amount=entry_usdt,
                        leverage=LEVERAGE,
                        margin_mode=MARGIN_MODE,
                    )
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    st["in_pos"] = True
                    _set_last_entry_state(st, "SHORT", time.time())
                    state[symbol] = st
                    _log_trade_entry(
                        state,
                        side="SHORT",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=entry_usdt,
                        entry_order_id=entry_order_id,
                        meta={"reason": "swaggy_atlas_lab"},
                    )
                    result["short_hits"] += 1
                else:
                    result["short_hits"] += 1
                if send_alert:
                    _send_entry_alert(
                        send_alert,
                        side="SHORT",
                        symbol=symbol,
                        engine="SWAGGY_ATLAS_LAB",
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        usdt=entry_usdt,
                        reason="SWAGGY_ATLAS_LAB",
                        live=LIVE_TRADING,
                        entry_order_id=entry_order_id,
                        sl=_fmt_price_safe(fill_price, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                        tp=None,
                    )
        except Exception as e:
            _append_swaggy_atlas_lab_log(f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=EXCHANGE_ERROR {e}")
        finally:
            _entry_guard_release(state, symbol, key=guard_key)
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab")

        time.sleep(PER_SYMBOL_SLEEP)
    return result

def _entry_guard_key(state: Dict[str, dict], symbol: str, side: str) -> str:
    cycle_ts = state.get("_current_cycle_ts")
    side = (side or "").upper()
    if cycle_ts:
        return f"{symbol}|{side}|{cycle_ts}"
    return f"{symbol}|{side}"


def _get_last_exit_ts_by_side(st: Dict[str, Any], side: str) -> Optional[float]:
    side_key = (side or "").upper()
    if side_key == "LONG":
        val = st.get("last_exit_ts_long")
    elif side_key == "SHORT":
        val = st.get("last_exit_ts_short")
    else:
        val = None
    if isinstance(val, (int, float)):
        return float(val)
    val = st.get("last_exit_ts")
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _get_last_exit_reason_by_side(st: Dict[str, Any], side: str) -> Optional[str]:
    side_key = (side or "").upper()
    if side_key == "LONG":
        val = st.get("last_exit_reason_long")
    elif side_key == "SHORT":
        val = st.get("last_exit_reason_short")
    else:
        val = None
    if isinstance(val, str):
        return val
    val = st.get("last_exit_reason")
    if isinstance(val, str):
        return val
    return None


def _set_last_exit_state(st: Dict[str, Any], side: str, ts: float, reason: str) -> None:
    try:
        ts_val = float(ts)
    except Exception:
        return
    st["last_exit_ts"] = ts_val
    st["last_exit_reason"] = reason
    side_key = (side or "").upper()
    if side_key == "LONG":
        st["last_exit_ts_long"] = ts_val
        st["last_exit_reason_long"] = reason
    elif side_key == "SHORT":
        st["last_exit_ts_short"] = ts_val
        st["last_exit_reason_short"] = reason


def _get_last_entry_ts_by_side(st: Dict[str, Any], side: str) -> Optional[float]:
    side_key = (side or "").upper()
    if side_key == "LONG":
        val = st.get("last_entry_ts_long")
    elif side_key == "SHORT":
        val = st.get("last_entry_ts_short")
    else:
        val = None
    if isinstance(val, (int, float)):
        return float(val)
    val = st.get("last_entry")
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _set_last_entry_state(st: Dict[str, Any], side: str, ts: float) -> None:
    try:
        ts_val = float(ts)
    except Exception:
        return
    st["last_entry"] = ts_val
    side_key = (side or "").upper()
    if side_key == "LONG":
        st["last_entry_ts_long"] = ts_val
    elif side_key == "SHORT":
        st["last_entry_ts_short"] = ts_val


def _atlasfabio_entry_gate(
    symbol: str,
    side: str,
    st: dict,
    now_ts: float,
    gate_tier: str,
    gate_score: Optional[float],
    live_ok: bool,
    max_positions: Optional[int] = None,
    active_positions: Optional[int] = None,
) -> tuple[bool, str]:
    if not live_ok:
        return False, "live_off"
    if st.get("in_pos"):
        return False, "in_pos"
    last_entry = _get_last_entry_ts_by_side(st, side)
    if isinstance(last_entry, (int, float)) and (now_ts - float(last_entry)) < COOLDOWN_SEC:
        return False, "cooldown"
    last_exit_ts = _get_last_exit_ts_by_side(st, side)
    if isinstance(last_exit_ts, (int, float)) and (now_ts - float(last_exit_ts)) < EXIT_COOLDOWN_SEC:
        _append_entry_gate_log(
            "atlasfabio",
            symbol,
            f"청산쿨다운={int(EXIT_COOLDOWN_SEC)}s side={side}",
        )
        return False, "exit_cooldown"
    if isinstance(max_positions, int) and isinstance(active_positions, int):
        if active_positions >= max_positions:
            _append_entry_gate_log(
                "atlasfabio",
                symbol,
                f"포지션제한={active_positions}/{max_positions} side={side}",
                side=side,
            )
            return False, "max_pos"
    return True, "ok"


def _atlas_tier_from_score(score: Optional[float]) -> str:
    if isinstance(score, (int, float)):
        if score >= ATLASFABIO_STRONG_SCORE:
            return "STRONG"
        if score >= ATLASFABIO_MID_SCORE:
            return "MID"
    return "NO"


def _entry_guard_acquire(
    state: Dict[str, dict],
    symbol: str,
    ttl_sec: float = 5.0,
    key: Optional[str] = None,
    engine: Optional[str] = None,
    side: Optional[str] = None,
) -> bool:
    guard = _get_entry_guard(state)
    now = time.time()
    gkey = key or symbol
    ts = float(guard.get(gkey, 0.0) or 0.0)
    if (now - ts) < ttl_sec:
        _append_entry_gate_log(engine or "unknown", symbol, f"entry_guard_ttl side={side or 'N/A'}", side=side)
        return False
    guard[gkey] = now
    return True


def _exit_cooldown_blocked(
    state: Dict[str, dict],
    symbol: str,
    engine: str,
    side: str,
    ttl_sec: Optional[float] = None,
) -> bool:
    if ttl_sec is None:
        ttl_sec = EXIT_COOLDOWN_SEC
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    last_exit_ts = _get_last_exit_ts_by_side(st, side)
    if not isinstance(last_exit_ts, (int, float)):
        return False
    now = time.time()
    if (now - float(last_exit_ts)) < ttl_sec:
        _append_entry_gate_log(
            engine,
            symbol,
            f"청산쿨다운={int(ttl_sec)}s side={side}",
            side=side,
        )
        return True
    return False

def _entry_guard_release(state: Dict[str, dict], symbol: str, key: Optional[str] = None) -> None:
    guard = _get_entry_guard(state)
    if key:
        guard.pop(key, None)
        return
    # legacy cleanup
    keys = [k for k in guard.keys() if str(k).startswith(f"{symbol}|") or k == symbol]
    for k in keys:
        guard.pop(k, None)


def _entry_lock_acquire(
    state: Dict[str, dict],
    symbol: str,
    owner: str,
    ttl_sec: float = 10.0,
    side: Optional[str] = None,
) -> tuple[bool, Optional[str], Optional[float]]:
    lock = _get_entry_lock(state)
    now = time.time()
    with _ENTRY_LOCK_MUTEX:
        cur = lock.get(symbol)
        if isinstance(cur, dict):
            expires = float(cur.get("expires", 0.0) or 0.0)
            if now < expires and cur.get("owner"):
                held_by = str(cur.get("owner"))
                _append_entry_gate_log(owner, symbol, f"entry_lock_held_by={held_by}", side=side)
                return False, held_by, expires - now
        lock[symbol] = {"owner": owner, "expires": now + ttl_sec, "ts": now}
    return True, None, None


def _entry_lock_release(state: Dict[str, dict], symbol: str, owner: Optional[str] = None) -> None:
    lock = _get_entry_lock(state)
    with _ENTRY_LOCK_MUTEX:
        cur = lock.get(symbol)
        if not isinstance(cur, dict):
            return
        if owner and cur.get("owner") != owner:
            return
        lock.pop(symbol, None)

def _log_trade_entry(
    state: Dict[str, dict],
    side: str,
    symbol: str,
    entry_ts: float,
    entry_price: Optional[float],
    qty: Optional[float],
    usdt: Optional[float],
    entry_order_id: Optional[str] = None,
    meta: Optional[dict] = None,
) -> None:
    log = _get_trade_log(state)
    tr = {
        "side": side,
        "symbol": symbol,
        "entry_ts": float(entry_ts),
        "entry_ts_ms": int(entry_ts * 1000),
        "entry_price": entry_price,
        "qty": qty,
        "usdt": usdt,
        "entry_order_id": entry_order_id,
        "status": "open",
        "meta": meta or {},
    }
    tr["engine_label"] = _engine_label_from_reason((tr.get("meta") or {}).get("reason"))
    if dbrec:
        try:
            dbrec.record_engine_signal(
                symbol=symbol,
                side=side,
                engine=tr.get("engine_label"),
                reason=(tr.get("meta") or {}).get("reason"),
                meta=tr.get("meta"),
                ts=entry_ts,
            )
        except Exception:
            pass
    log.append(tr)
    _append_entry_event(tr)
    try:
        st = state.get(symbol) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        _set_last_entry_state(st, side, entry_ts)
        state[symbol] = st
    except Exception:
        pass
    if entry_order_id:
        st = state.get(symbol) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        st[f"entry_order_id_{side.lower()}"] = entry_order_id
        state[symbol] = st

def _get_open_trade(state: Dict[str, dict], side: str, symbol: str) -> Optional[dict]:
    log = _get_trade_log(state)
    for tr in reversed(log):
        if tr.get("status") == "open" and tr.get("side") == side and tr.get("symbol") == symbol:
            return tr
    return None

def _trade_has_entry(tr: Optional[dict]) -> bool:
    if not isinstance(tr, dict):
        return False
    for key in ("entry_ts", "entry_ts_ms", "entry_price", "qty"):
        val = tr.get(key)
        if isinstance(val, (int, float)) and val > 0:
            return True
    return False

def _get_open_symbols(state: Dict[str, dict], side: str) -> List[str]:
    log = _get_trade_log(state)
    out: List[str] = []
    seen = set()
    for tr in reversed(log):
        if tr.get("status") != "open" or tr.get("side") != side:
            continue
        sym = tr.get("symbol")
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out

def _engine_label_from_reason(reason: Optional[str]) -> str:
    key = (reason or "").lower()
    if key in ("atlasfabio_long", "atlasfabio_short"):
        return "ATLASFABIO"
    if key in ("swaggy_long", "swaggy_short"):
        return "SWAGGY"
    if key == "swaggy_atlas_lab":
        return "SWAGGY_ATLAS_LAB"
    if key in ("dtfx_long", "dtfx_short"):
        return "DTFX"
    if key == "div15m_long":
        return "DIV15M_LONG"
    if key == "div15m_short":
        return "DIV15M_SHORT"
    if key == "pumpfade_short":
        return "PUMPFADE"
    if key == "atlas_rs_fail_short":
        return "ATLAS_RS_FAIL_SHORT"
    if key == "short_entry":
        return "RSI"
    if key == "long_entry":
        return "SCALP"
    if key in ("manual", "manual_entry"):
        return "MANUAL"
    return "UNKNOWN"

def _display_engine_label(label: Optional[str]) -> str:
    name = (label or "").strip() or "UNKNOWN"
    overrides = {
        "ATLAS_RS_FAIL_SHORT": "아틀라스 숏",
        "SWAGGY_ATLAS_LAB": "스웨기랩",
        "ATLASFABIO": "파비오",
    }
    return overrides.get(name, name)

def _close_trade(
    state: Dict[str, dict],
    side: str,
    symbol: str,
    exit_ts: float,
    exit_price: Optional[float],
    pnl_usdt: Optional[float],
    reason: str,
    exit_order_id: Optional[str] = None,
) -> None:
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    st["last_entry"] = float(exit_ts)
    _set_last_exit_state(st, side, exit_ts, reason)
    st["in_pos"] = False
    state[symbol] = st
    if dbrec:
        try:
            dbrec.record_position_snapshot(
                symbol=symbol,
                side=side,
                qty=0.0,
                avg_entry=None,
                unreal_pnl=None,
                realized_pnl=pnl_usdt,
                ts=exit_ts,
                source=reason,
            )
        except Exception:
            pass
    log = _get_trade_log(state)
    for tr in reversed(log):
        if tr.get("side") == side and tr.get("symbol") == symbol and tr.get("status") == "open":
            tr["exit_ts"] = float(exit_ts)
            tr["exit_price"] = exit_price
            tr["pnl_usdt"] = pnl_usdt
            tr["status"] = "closed"
            tr["exit_reason"] = reason
            if exit_order_id:
                tr["exit_order_id"] = exit_order_id
            elif reason == "auto_exit_sl":
                meta = tr.get("meta") or {}
                sl_id = meta.get("sl_order_id")
                if sl_id:
                    tr["exit_order_id"] = sl_id
            try:
                entry_px = tr.get("entry_price")
                if isinstance(entry_px, (int, float)) and isinstance(exit_price, (int, float)) and entry_px > 0:
                    if side == "LONG":
                        tr["roi_pct"] = (float(exit_price) - float(entry_px)) / float(entry_px) * 100.0
                    else:
                        tr["roi_pct"] = (float(entry_px) - float(exit_price)) / float(entry_px) * 100.0
            except Exception:
                pass
            if not tr.get("engine_label"):
                tr["engine_label"] = _engine_label_from_reason((tr.get("meta") or {}).get("reason"))
            _update_report_csv(tr)
            return
    log.append(
        {
            "side": side,
            "symbol": symbol,
            "entry_ts": None,
            "entry_price": None,
            "qty": None,
            "usdt": None,
            "status": "closed",
            "exit_ts": float(exit_ts),
            "exit_price": exit_price,
            "pnl_usdt": pnl_usdt,
            "exit_reason": reason,
            "exit_order_id": exit_order_id,
            "meta": {},
        }
    )

def _prune_trade_log(state: Dict[str, dict], keep_days: int = 14) -> None:
    log = _get_trade_log(state)
    cutoff_ts = time.time() - (keep_days * 86400)
    kept = []
    for tr in log:
        ts = tr.get("exit_ts") or tr.get("entry_ts") or 0
        if ts and ts >= cutoff_ts:
            kept.append(tr)
    state["_trade_log"] = kept

def _fmt_report_symbol_row(symbol: str, engine: str, pnl_val: Optional[float], win_flag: Optional[bool]) -> str:
    if isinstance(symbol, str):
        symbol = _report_symbol_key(symbol)
    engine = _display_engine_label(engine)
    pnl_str = f"{pnl_val:+.1f}" if isinstance(pnl_val, (int, float)) else "N/A"
    if win_flag is True:
        wl = "승"
    elif win_flag is False:
        wl = "패"
    else:
        wl = "N/A"
    symbol_fmt = _pad_display(symbol, 15)
    engine_fmt = _pad_display(engine, 10)
    pnl_fmt = f"{pnl_str:>13}"
    wl_fmt = f"{wl:^4}"
    return f"| {symbol_fmt} | {engine_fmt} | {pnl_fmt} | {wl_fmt} |"

def _report_symbol_key(sym: Optional[str]) -> str:
    if not isinstance(sym, str):
        return ""
    key = sym.replace(":USDT", "").replace("/USDT", "")
    if key.endswith("USDT") and "/" not in key:
        key = key[:-4]
    return key

def _display_width(text: str) -> int:
    width = 0
    for ch in text:
        if unicodedata.east_asian_width(ch) in ("W", "F"):
            width += 2
        else:
            width += 1
    return width

def _pad_display(text: str, width: int) -> str:
    if not isinstance(text, str):
        text = ""
    cur = _display_width(text)
    if cur >= width:
        return text
    return text + (" " * (width - cur))

def _summarize_trade_rows(trade_rows: Dict[str, list]) -> tuple[Dict[str, dict], Dict[str, dict]]:
    totals = {
        "LONG": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
        "SHORT": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
    }
    engine_totals: Dict[str, dict] = {"LONG": {}, "SHORT": {}}
    for side in ("LONG", "SHORT"):
        for rec in trade_rows.get(side) or []:
            pnl = rec.get("pnl")
            engine = rec.get("engine") or "DB"
            totals[side]["count"] += 1
            if isinstance(pnl, (int, float)):
                totals[side]["pnl"] += float(pnl)
                totals[side]["pnl_valid"] += 1
                if pnl > 0:
                    totals[side]["wins"] += 1
                elif pnl < 0:
                    totals[side]["losses"] += 1
            eng = engine_totals[side].setdefault(
                engine, {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0}
            )
            eng["count"] += 1
            if isinstance(pnl, (int, float)):
                eng["pnl"] += float(pnl)
                eng["pnl_valid"] += 1
                if pnl > 0:
                    eng["wins"] += 1
                elif pnl < 0:
                    eng["losses"] += 1
    return totals, engine_totals

def _load_income_trade_rows(start_date: str, end_date: str) -> List[dict]:
    if not dbrec:
        return []
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except Exception:
        return []
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    start_ts = calendar.timegm(start_dt.timetuple())
    end_ts = calendar.timegm((end_dt + timedelta(days=1)).timetuple())
    db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
    out: List[dict] = []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        income_rows = cur.execute(
            """
            SELECT ts, symbol, side, income, raw_json
            FROM income
            WHERE income_type='REALIZED_PNL' AND ts >= ? AND ts < ?
            ORDER BY ts ASC
            """,
            (start_ts, end_ts),
        ).fetchall()
        fill_rows = cur.execute(
            """
            SELECT fill_id, side, order_id, fee, fee_asset
            FROM fills
            WHERE ts >= ? AND ts < ?
            """,
            (start_ts, end_ts),
        ).fetchall()
        conn.close()
    except Exception:
        return []
    fill_side_by_id = {}
    fill_order_by_id = {}
    fee_by_order_id: Dict[str, float] = {}
    for fill_id, side, order_id, fee, fee_asset in fill_rows:
        if fill_id is None:
            continue
        fill_side_by_id[str(fill_id)] = str(side or "").lower()
        if order_id is not None:
            fill_order_by_id[str(fill_id)] = str(order_id)
            if isinstance(fee, (int, float)) and (fee_asset or "").upper() == "USDT":
                key = str(order_id)
                fee_by_order_id[key] = fee_by_order_id.get(key, 0.0) + float(fee)
    for ts, symbol, side_field, income, raw_json in income_rows:
        if not symbol:
            continue
        try:
            pnl = float(income)
        except Exception:
            continue
        side = (side_field or "").upper()
        order_id = None
        fee_usdt = None
        if side not in ("LONG", "SHORT"):
            trade_id = None
            if raw_json:
                try:
                    raw_obj = json.loads(raw_json)
                except Exception:
                    raw_obj = None
                if isinstance(raw_obj, dict):
                    trade_id = raw_obj.get("tradeId") or raw_obj.get("id")
            if trade_id and str(trade_id) in fill_side_by_id:
                fside = fill_side_by_id.get(str(trade_id))
                if fside == "sell":
                    side = "LONG"
                elif fside == "buy":
                    side = "SHORT"
                order_id = fill_order_by_id.get(str(trade_id))
                if order_id is not None:
                    fee_usdt = fee_by_order_id.get(str(order_id))
        if side not in ("LONG", "SHORT"):
            continue
        out.append(
            {
                "ts": ts,
                "symbol": symbol,
                "side": side,
                "pnl": pnl,
                "order_id": order_id,
                "fee_usdt": fee_usdt,
            }
        )
    return out

def _load_db_daily_rows(report_date: str) -> Optional[List[dict]]:
    if not dbrec or not dbrec.ENABLED or not dbpnl:
        return None
    try:
        income_symbols = set()
        try:
            db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            rows = cur.execute(
                """
                SELECT DISTINCT symbol
                FROM income
                WHERE date(datetime(ts, 'unixepoch')) = ?
                """,
                (report_date,),
            ).fetchall()
            conn.close()
            income_symbols = {r[0] for r in rows if r and r[0]}
        except Exception:
            income_symbols = set()
        if dbrecon and executor_mod and hasattr(executor_mod, "exchange"):
            try:
                _, entry_by_symbol = _load_entry_events_map(report_date)
                symbols = {sym for (sym, _side) in entry_by_symbol.keys() if sym}
            except Exception:
                symbols = set()
            if not symbols:
                try:
                    pos_syms = list_open_position_symbols(force=True)
                    symbols |= set(pos_syms.get("long") or set())
                    symbols |= set(pos_syms.get("short") or set())
                except Exception:
                    pass
            symbols |= income_symbols
            def _norm(sym: str) -> str:
                if "/" in sym:
                    return sym
                if sym.endswith("USDT"):
                    base = sym[:-4]
                    return f"{base}/USDT:USDT"
                return sym
            norm_syms = sorted({_norm(sym) for sym in symbols if sym})
            if symbols:
                lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
                since_ts = time.time() - float(lookback)
                dbrecon.sync_exchange_state(executor_mod.exchange, since_ts=since_ts, symbols=norm_syms)
            lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
            dbrecon.sync_income(executor_mod.exchange, since_ts=time.time() - float(lookback))
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        out_path = os.path.join("reports", "db_pnl_last3d.csv")
        daily_path = os.path.join("reports", "db_pnl_daily_last3d.csv")
        lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
        dbpnl.build_report(db_path, int(lookback), out_path, daily_path)
        if not os.path.exists(daily_path):
            return None
        rows = []
        with open(daily_path, "r", encoding="utf-8") as f:
            header = None
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if header is None:
                    header = parts
                    continue
                if len(parts) < len(header):
                    parts += [""] * (len(header) - len(parts))
                rec = {header[i]: parts[i] for i in range(len(header))}
                if rec.get("day") == report_date:
                    rows.append(rec)
        return rows
    except Exception:
        return None

def _load_db_daily_rows_range(start_date: str, end_date: str) -> Optional[List[dict]]:
    if not dbrec or not dbrec.ENABLED or not dbpnl:
        return None
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except Exception:
        return None
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    try:
        income_symbols = set()
        try:
            db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            rows = cur.execute(
                """
                SELECT DISTINCT symbol
                FROM income
                WHERE date(datetime(ts, 'unixepoch')) BETWEEN ? AND ?
                """,
                (start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")),
            ).fetchall()
            conn.close()
            income_symbols = {r[0] for r in rows if r and r[0]}
        except Exception:
            income_symbols = set()
        if dbrecon and executor_mod and hasattr(executor_mod, "exchange"):
            symbols = set()
            try:
                cur = start_dt
                while cur <= end_dt:
                    day = cur.strftime("%Y-%m-%d")
                    _, entry_by_symbol = _load_entry_events_map(day)
                    symbols |= {sym for (sym, _side) in entry_by_symbol.keys() if sym}
                    cur += timedelta(days=1)
            except Exception:
                pass
            if not symbols:
                try:
                    pos_syms = list_open_position_symbols(force=True)
                    symbols |= set(pos_syms.get("long") or set())
                    symbols |= set(pos_syms.get("short") or set())
                except Exception:
                    pass
            symbols |= income_symbols
            if symbols:
                def _norm(sym: str) -> str:
                    if "/" in sym:
                        return sym
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                        return f"{base}/USDT:USDT"
                    return sym
                norm_syms = sorted({_norm(sym) for sym in symbols if sym})
                lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
                since_ts = time.time() - float(lookback)
                dbrecon.sync_exchange_state(executor_mod.exchange, since_ts=since_ts, symbols=norm_syms)
            lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
            dbrecon.sync_income(executor_mod.exchange, since_ts=time.time() - float(lookback))
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        out_path = os.path.join("reports", "db_pnl_last3d.csv")
        daily_path = os.path.join("reports", "db_pnl_daily_last3d.csv")
        lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
        dbpnl.build_report(db_path, int(lookback), out_path, daily_path)
        if not os.path.exists(daily_path):
            return None
        rows = []
        with open(daily_path, "r", encoding="utf-8") as f:
            header = None
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if header is None:
                    header = parts
                    continue
                if len(parts) < len(header):
                    parts += [""] * (len(header) - len(parts))
                rec = {header[i]: parts[i] for i in range(len(header))}
                day = rec.get("day")
                if not day:
                    continue
                try:
                    day_dt = datetime.strptime(day, "%Y-%m-%d")
                except Exception:
                    continue
                if start_dt <= day_dt <= end_dt:
                    rows.append(rec)
        return rows
    except Exception:
        return None

def _format_report_output(
    totals: Dict[str, dict],
    engine_totals: Dict[str, dict],
    symbol_rows: Dict[str, dict],
    report_label: str,
    compact: bool,
    entry_count: int,
) -> str:
    count_width = 4
    pnl_width = 29
    win_width = 6
    wl_width = 3
    total_exits = totals["LONG"]["count"] + totals["SHORT"]["count"]
    total_pnl = totals["LONG"]["pnl"] + totals["SHORT"]["pnl"]
    total_pnl_valid = totals["LONG"]["pnl_valid"] + totals["SHORT"]["pnl_valid"]
    total_wins = totals["LONG"]["wins"] + totals["SHORT"]["wins"]
    total_losses = totals["LONG"]["losses"] + totals["SHORT"]["losses"]
    total_pnl_int = int(total_pnl) if total_pnl_valid > 0 else None

    def _format_total(t: dict) -> str:
        if t["pnl_valid"] > 0 and t["notional"] > 0:
            total_pct = (t["pnl"] / t["notional"]) * 100.0
            pnl_part = f"{total_pct:+.2f}%"
        else:
            pnl_part = "N/A"
        if t["pnl_valid"] > 0:
            pnl_int = int(t["pnl"])
            pnl_usdt_part = f"{pnl_int:+d} USDT"
        else:
            pnl_usdt_part = "N/A"
        total_outcomes = t["wins"] + t["losses"]
        win_rate = (t["wins"] / total_outcomes * 100.0) if total_outcomes > 0 else 0.0
        losses = t["losses"]
        if compact:
            return f"총청산={t['count']} 총수익={pnl_usdt_part} 승률={win_rate:.1f}% 승={t['wins']} 패={losses}"
        pnl_fmt = f"{pnl_usdt_part:<{pnl_width}}"
        count_fmt = f"{t['count']:<{count_width}}"
        win_fmt = f"{win_rate:.1f}%"
        win_fmt = f"{win_fmt:<{win_width}}"
        win_cnt = f"{t['wins']:<{wl_width}}"
        loss_cnt = f"{losses:<{wl_width}}"
        return f"{count_fmt} | {pnl_fmt} | {win_fmt} | {win_cnt} | {loss_cnt}"

    def _summary_header(label: str) -> str:
        return (
            f"| {label:<10} | {'총청산':<{count_width}} | {'총수익':<{pnl_width}} | "
            f"{'승률':<{win_width}} | {'승':<{wl_width}} | {'패':<{wl_width}} |"
        )

    def _summary_sep() -> str:
        label_sep = "-" * 11
        count_sep = "-" * (count_width + 2)
        pnl_sep = "-" * (pnl_width + 2)
        win_sep = "-" * (win_width + 2)
        wl_sep = "-" * (wl_width + 2)
        return f"|{label_sep}|{count_sep}|{pnl_sep}|{win_sep}|{wl_sep}|{wl_sep}|"

    def total_line(side: str) -> list:
        if compact:
            return [f"- <b>{_format_total(totals[side])}</b>"]
        return [
            _summary_header("구분"),
            _summary_sep(),
            f"| 합계       | {_format_total(totals[side])} |",
        ]

    def engine_lines(side: str) -> list:
        if compact:
            return [f"- {_display_engine_label(eng)} {_format_total(t)}" for eng, t in sorted(engine_totals[side].items())]
        lines = [
            _summary_header("엔진"),
            _summary_sep(),
        ]
        for eng, t in sorted(engine_totals[side].items()):
            disp = _display_engine_label(eng)
            lines.append(f"| {disp:<10} | {_format_total(t)} |")
        return lines

    def symbol_lines(side: str) -> list:
        bucket = symbol_rows.get(side) or {}
        if not bucket:
            return ["(symbols) none"]
        if isinstance(bucket, list):
            lines = [
                "| Symbol          | Engine     | PnL           | 결과 |",
                "|-----------------|------------|---------------|------|",
            ]
            ordered = sorted(
                bucket,
                key=lambda item: (
                    item.get("ts") if isinstance(item.get("ts"), (int, float)) else float("inf"),
                    item.get("symbol") or "",
                    item.get("engine") or "",
                ),
            )
            for rec in ordered:
                lines.append(
                    _fmt_report_symbol_row(
                        rec.get("symbol") or "",
                        rec.get("engine") or "",
                        rec.get("pnl"),
                        rec.get("win"),
                    )
                )
            return lines
        lines = [
            "| Symbol          | Engine     | PnL           | 결과 |",
            "|-----------------|------------|---------------|------|",
        ]
        ordered = sorted(
            bucket.items(),
            key=lambda item: (
                item[1].get("ts") if isinstance(item[1].get("ts"), (int, float)) else float("inf"),
                item[0][0],
                item[0][1],
            ),
        )
        for (symbol, engine), rec in ordered:
            lines.append(_fmt_report_symbol_row(symbol, engine, rec.get("pnl"), rec.get("win")))
        return lines

    total_outcomes = total_wins + total_losses
    total_win_rate = (total_wins / total_outcomes * 100.0) if total_outcomes > 0 else 0.0
    total_pnl_str = f"{total_pnl_int:+d} USDT" if total_pnl_int is not None else "N/A"
    lines = [
        f"📊 일일 리포트 (KST, {report_label})",
        f"- <b>총 진입={entry_count} 총 청산={total_exits} 총 수익={total_pnl_str} 승률={total_win_rate:.1f}% 승={total_wins} 패={total_losses}</b>",
        "",
        "🔴 SHORT",
    ]
    lines.extend(total_line("SHORT"))
    if not compact:
        lines.append("")
    lines.extend(engine_lines("SHORT"))
    lines.append("🔎 SHORT SYMBOLS")
    lines.append("<pre>")
    lines.extend(symbol_lines("SHORT"))
    lines.append("</pre>")
    lines.append("🟢 LONG")
    lines.extend(total_line("LONG"))
    if not compact:
        lines.append("")
    lines.extend(engine_lines("LONG"))
    lines.append("🔎 LONG SYMBOLS")
    lines.append("<pre>")
    lines.extend(symbol_lines("LONG"))
    lines.append("</pre>")
    return "\n".join(lines)

def _build_daily_report(state: Dict[str, dict], report_date: str, compact: bool = False) -> str:
    db_rows = _load_db_daily_rows(report_date)
    records = None if db_rows else _read_report_csv_records(report_date)
    totals = {
        "LONG": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
        "SHORT": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
    }
    engine_totals = {"LONG": {}, "SHORT": {}}
    symbol_rows = {"LONG": {}, "SHORT": {}}
    symbol_trade_rows = {"LONG": [], "SHORT": []}
    symbol_trade_rows = None
    use_trade_rows = False
    entry_by_symbol = {}
    try:
        _, entry_by_symbol = _load_entry_events_map(report_date)
    except Exception:
        entry_by_symbol = {}

    entry_engine_map: Dict[Tuple[str, str], Tuple[float, str]] = {}
    entry_event_index: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    entry_event_index: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    for (sym, side), records_by_sym in entry_by_symbol.items():
        side_key = (side or "").upper()
        sym_key = _report_symbol_key(sym)
        if not sym_key or side_key not in ("LONG", "SHORT"):
            continue
        ev_list = entry_event_index.setdefault((sym_key, side_key), [])
        for rec in records_by_sym:
            ts = rec.get("entry_ts")
            eng = rec.get("engine") or "unknown"
            if isinstance(ts, (int, float)):
                ev_list.append((float(ts), eng))
            cur = entry_engine_map.get((sym_key, side_key))
            if cur is None or (isinstance(ts, (int, float)) and ts > cur[0]):
                entry_engine_map[(sym_key, side_key)] = (float(ts) if isinstance(ts, (int, float)) else 0.0, eng)

    def _engine_for(symbol: str, side_key: str) -> str:
        key = (_report_symbol_key(symbol), side_key)
        rec = entry_engine_map.get(key)
        return rec[1] if rec else "DB"

    def _engine_for_trade(symbol: str, side_key: str, exec_ts: Optional[float]) -> str:
        key = (_report_symbol_key(symbol), side_key)
        events = entry_event_index.get(key)
        if not events:
            return _engine_for(symbol, side_key)
        if exec_ts is None:
            return _engine_for(symbol, side_key)
        events_sorted = sorted(events, key=lambda item: item[0])
        ts_list = [item[0] for item in events_sorted]
        idx = bisect.bisect_right(ts_list, float(exec_ts)) - 1
        if idx >= 0:
            return events_sorted[idx][1]
        return _engine_for(symbol, side_key)

    def _update_symbol_row(
        side_key: str,
        symbol: str,
        engine: str,
        pnl_val: Optional[float],
        win_flag: Optional[bool],
        exec_ts: Optional[float],
    ) -> None:
        if not symbol:
            return
        key = (symbol, engine or "unknown")
        bucket = symbol_rows.get(side_key)
        if bucket is None:
            return
        rec = bucket.get(key)
        if rec is None:
            bucket[key] = {"pnl": pnl_val, "win": win_flag, "ts": exec_ts}
            return
        cur_pnl = rec.get("pnl")
        if isinstance(cur_pnl, (int, float)) or isinstance(pnl_val, (int, float)):
            if not isinstance(cur_pnl, (int, float)):
                cur_pnl = 0.0
            if not isinstance(pnl_val, (int, float)):
                pnl_val = 0.0
            rec["pnl"] = float(cur_pnl) + float(pnl_val)
        cur_ts = rec.get("ts")
        if isinstance(exec_ts, (int, float)):
            if not isinstance(cur_ts, (int, float)) or exec_ts < float(cur_ts):
                rec["ts"] = float(exec_ts)
        cur_win = rec.get("win")
        if cur_win is True:
            return
        if cur_win is False and win_flag is None:
            return
        if win_flag is True:
            rec["win"] = True
        elif win_flag is False and cur_win is None:
            rec["win"] = False

    def _fmt_symbol_row(symbol: str, engine: str, pnl_val: Optional[float], win_flag: Optional[bool]) -> str:
        return _fmt_report_symbol_row(symbol, engine, pnl_val, win_flag)
    if db_rows:
        symbol_trade_rows = {"LONG": [], "SHORT": []}
        trade_rows = _load_income_trade_rows(report_date, report_date)
        grouped = {"LONG": {}, "SHORT": {}}
        for tr in trade_rows:
            side = tr.get("side")
            if side not in ("LONG", "SHORT"):
                continue
            symbol = tr.get("symbol") or ""
            pnl = tr.get("pnl")
            win_flag = True if isinstance(pnl, (int, float)) and pnl > 0 else False if isinstance(pnl, (int, float)) and pnl < 0 else None
            engine = _engine_for_trade(symbol, side, tr.get("ts"))
            order_id = tr.get("order_id")
            fee_usdt = tr.get("fee_usdt")
            key = str(order_id) if order_id else f"{symbol}|{side}|{tr.get('ts')}"
            bucket = grouped[side]
            rec = bucket.get(key)
            if rec is None:
                bucket[key] = {
                    "symbol": symbol,
                    "engine": engine,
                    "pnl": pnl,
                    "win": win_flag,
                    "ts": tr.get("ts"),
                    "order_id": order_id,
                    "fee_usdt": fee_usdt if isinstance(fee_usdt, (int, float)) else None,
                }
            else:
                if isinstance(pnl, (int, float)):
                    rec["pnl"] = float(rec.get("pnl") or 0.0) + float(pnl)
                if win_flag is True:
                    rec["win"] = True
                elif win_flag is False and rec.get("win") is None:
                    rec["win"] = False
                if isinstance(tr.get("ts"), (int, float)):
                    cur_ts = rec.get("ts")
                    if not isinstance(cur_ts, (int, float)) or tr.get("ts") < cur_ts:
                        rec["ts"] = tr.get("ts")
                if rec.get("fee_usdt") is None and isinstance(fee_usdt, (int, float)):
                    rec["fee_usdt"] = fee_usdt
        for rec in grouped[side].values():
            fee_usdt = rec.get("fee_usdt")
            if isinstance(fee_usdt, (int, float)) and isinstance(rec.get("pnl"), (int, float)):
                rec["pnl"] = float(rec.get("pnl")) - float(fee_usdt)
            if isinstance(rec.get("pnl"), (int, float)):
                rec["win"] = True if rec.get("pnl") > 0 else False if rec.get("pnl") < 0 else None
        symbol_trade_rows["LONG"] = list(grouped["LONG"].values())
        symbol_trade_rows["SHORT"] = list(grouped["SHORT"].values())
        use_trade_rows = bool(symbol_trade_rows["LONG"] or symbol_trade_rows["SHORT"])
        for rec in db_rows:
            side = str(rec.get("pos_side") or "").upper()
            if side not in ("LONG", "SHORT"):
                continue
            pnl_raw = rec.get("realized_pnl_net") or rec.get("realized_pnl")
            try:
                pnl = float(pnl_raw) if pnl_raw != "" else None
            except Exception:
                pnl = None
            try:
                notional = float(rec.get("notional_sum")) if rec.get("notional_sum") != "" else 0.0
            except Exception:
                notional = 0.0
            win_flag = None
            if isinstance(pnl, (int, float)):
                if pnl > 0:
                    win_flag = True
                elif pnl < 0:
                    win_flag = False
            if not use_trade_rows:
                totals[side]["count"] += 1
                if isinstance(pnl, (int, float)):
                    totals[side]["pnl"] += float(pnl)
                    totals[side]["pnl_valid"] += 1
                    if pnl > 0:
                        totals[side]["wins"] += 1
                    elif pnl < 0:
                        totals[side]["losses"] += 1
                if isinstance(notional, (int, float)) and notional > 0:
                    totals[side]["notional"] += notional
                symbol = rec.get("symbol") or ""
                engine = _engine_for(symbol, side)
                eng = engine_totals[side].setdefault(
                    engine, {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0}
                )
                eng["count"] += 1
                if isinstance(pnl, (int, float)):
                    eng["pnl"] += float(pnl)
                    eng["pnl_valid"] += 1
                    if pnl > 0:
                        eng["wins"] += 1
                    elif pnl < 0:
                        eng["losses"] += 1
                if isinstance(notional, (int, float)) and notional > 0:
                    eng["notional"] += notional
                _update_symbol_row(side, symbol, engine, pnl, win_flag, None)
        if use_trade_rows:
            totals, engine_totals = _summarize_trade_rows(symbol_trade_rows)
    elif records is not None:
        for rec in records:
            if not rec.get("exit_ts"):
                continue
            if rec.get("pnl") == "":
                continue
            side = str(rec.get("side") or "").upper()
            if side not in totals:
                continue
            try:
                entry_price = float(rec.get("entry_price")) if rec.get("entry_price") != "" else None
            except Exception:
                entry_price = None
            try:
                exit_price = float(rec.get("exit_price")) if rec.get("exit_price") != "" else None
            except Exception:
                exit_price = None
            try:
                qty = float(rec.get("qty")) if rec.get("qty") != "" else None
            except Exception:
                qty = None
            try:
                pnl = float(rec.get("pnl")) if rec.get("pnl") != "" else None
            except Exception:
                pnl = None
            pnl_pct = None
            win_flag = None
            if isinstance(pnl, (int, float)) and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                notional = entry_price * qty
                if notional > 0:
                    pnl_pct = (float(pnl) / notional) * 100.0
            elif isinstance(entry_price, (int, float)) and isinstance(exit_price, (int, float)) and entry_price > 0:
                if side == "LONG":
                    pnl_pct = (exit_price - entry_price) / entry_price * 100.0
                elif side == "SHORT":
                    pnl_pct = (entry_price - exit_price) / entry_price * 100.0
            engine = rec.get("engine") or "unknown"
            totals[side]["count"] += 1
            if isinstance(pnl, (int, float)):
                totals[side]["pnl"] += float(pnl)
                totals[side]["pnl_valid"] += 1
                win_flag = pnl > 0
                if pnl < 0:
                    win_flag = False
            if isinstance(pnl_pct, (int, float)):
                win_flag = pnl_pct > 0
                totals[side]["pnl_valid"] += 1 if win_flag is None else 0
            if win_flag is True:
                totals[side]["wins"] += 1
            elif win_flag is False:
                totals[side]["losses"] += 1
            if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                totals[side]["notional"] += entry_price * qty
            eng = engine_totals[side].setdefault(
                engine, {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0}
            )
            eng["count"] += 1
            win_flag = None
            if isinstance(pnl, (int, float)):
                eng["pnl"] += float(pnl)
                eng["pnl_valid"] += 1
                win_flag = pnl > 0
                if pnl < 0:
                    win_flag = False
            if isinstance(pnl_pct, (int, float)):
                win_flag = pnl_pct > 0
                eng["pnl_valid"] += 1 if win_flag is None else 0
            if win_flag is True:
                eng["wins"] += 1
            elif win_flag is False:
                eng["losses"] += 1
            if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                eng["notional"] += entry_price * qty
            symbol = rec.get("symbol") or ""
            exec_ts = None
            exit_ts_str = rec.get("exit_ts")
            if exit_ts_str:
                try:
                    exec_ts = datetime.strptime(exit_ts_str, "%Y-%m-%d %H:%M:%S").timestamp()
                except Exception:
                    exec_ts = None
            _update_symbol_row(side, symbol, engine, pnl, win_flag, exec_ts)
    else:
        log = _get_trade_log(state)
        for tr in log:
            if tr.get("status") != "closed":
                continue
            exit_ts = tr.get("exit_ts")
            if not exit_ts:
                continue
            if _report_day_str(exit_ts) != report_date:
                continue
            side = str(tr.get("side") or "").upper()
            if side not in totals:
                continue
            entry_price = tr.get("entry_price")
            exit_price = tr.get("exit_price")
            qty = tr.get("qty")
            pnl = tr.get("pnl_usdt")
            pnl_pct = None
            win_flag = None
            if isinstance(pnl, (int, float)) and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                notional = entry_price * qty
                if notional > 0:
                    pnl_pct = (float(pnl) / notional) * 100.0
            elif isinstance(entry_price, (int, float)) and isinstance(exit_price, (int, float)) and entry_price > 0:
                if side == "LONG":
                    pnl_pct = (exit_price - entry_price) / entry_price * 100.0
                elif side == "SHORT":
                    pnl_pct = (entry_price - exit_price) / entry_price * 100.0
            raw_reason = (tr.get("meta") or {}).get("reason") or tr.get("exit_reason") or "unknown"
            reason_map = {
                "atlasfabio_long": "atlas-fabio",
                "atlasfabio_short": "atlas-fabio",
                "swaggy_long": "atlas-swaggy",
                "swaggy_short": "atlas-swaggy",
                "dtfx_long": "dtfx",
                "dtfx_short": "dtfx",
                "short_entry": "rsi",
                "long_entry": "scalp",
                "manual_close": "manual",
                "auto_exit_tp": "tp",
                "auto_exit_sl": "sl",
                "unknown": "rsi",
            }
            reason = reason_map.get(raw_reason, raw_reason)
            totals[side]["count"] += 1
            if isinstance(pnl, (int, float)):
                totals[side]["pnl"] += float(pnl)
                totals[side]["pnl_valid"] += 1
                win_flag = pnl > 0
                if pnl < 0:
                    win_flag = False
            if isinstance(pnl_pct, (int, float)):
                win_flag = pnl_pct > 0
                totals[side]["pnl_valid"] += 1 if win_flag is None else 0
            if win_flag is True:
                totals[side]["wins"] += 1
            elif win_flag is False:
                totals[side]["losses"] += 1
            if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                totals[side]["notional"] += entry_price * qty
            eng = engine_totals[side].setdefault(
                reason, {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0}
            )
            eng["count"] += 1
            win_flag = None
            if isinstance(pnl, (int, float)):
                eng["pnl"] += float(pnl)
                eng["pnl_valid"] += 1
                win_flag = pnl > 0
                if pnl < 0:
                    win_flag = False
            if isinstance(pnl_pct, (int, float)):
                win_flag = pnl_pct > 0
                eng["pnl_valid"] += 1 if win_flag is None else 0
            if win_flag is True:
                eng["wins"] += 1
            elif win_flag is False:
                eng["losses"] += 1
            if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and entry_price > 0:
                eng["notional"] += entry_price * qty
            symbol = tr.get("symbol") or ""
            exec_ts = tr.get("exit_ts") if isinstance(tr.get("exit_ts"), (int, float)) else None
            _update_symbol_row(side, symbol, reason, pnl, win_flag, exec_ts)

    entry_count = sum(len(v) for v in entry_by_symbol.values()) if entry_by_symbol else 0
    rows_out = symbol_trade_rows if use_trade_rows else symbol_rows
    return _format_report_output(totals, engine_totals, rows_out, report_date, compact, entry_count)

def _build_range_report(state: Dict[str, dict], start_date: str, end_date: str, compact: bool = False) -> str:
    db_rows = _load_db_daily_rows_range(start_date, end_date)
    if not db_rows:
        return _build_daily_report(state, start_date, compact=compact)
    entry_by_symbol: dict = {}
    entry_count = 0
    entry_engine_map: Dict[Tuple[str, str], Tuple[float, str]] = {}
    totals = {
        "LONG": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
        "SHORT": {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0},
    }
    engine_totals = {"LONG": {}, "SHORT": {}}
    symbol_rows = {"LONG": {}, "SHORT": {}}
    try:
        cur = start_dt
        while cur <= end_dt:
            day = cur.strftime("%Y-%m-%d")
            _, entry_by_symbol = _load_entry_events_map(day)
            entry_count += sum(len(v) for v in entry_by_symbol.values())
            for (sym, side), records_by_sym in entry_by_symbol.items():
                side_key = (side or "").upper()
                if side_key not in ("LONG", "SHORT"):
                    continue
                sym_key = _report_symbol_key(sym)
                if not sym_key:
                    continue
            ev_list = entry_event_index.setdefault((sym_key, side_key), [])
            for rec in records_by_sym:
                ts = rec.get("entry_ts")
                eng = rec.get("engine") or "unknown"
                if isinstance(ts, (int, float)):
                    ev_list.append((float(ts), eng))
                cur_rec = entry_engine_map.get((sym_key, side_key))
                if cur_rec is None or (isinstance(ts, (int, float)) and ts > cur_rec[0]):
                    entry_engine_map[(sym_key, side_key)] = (float(ts) if isinstance(ts, (int, float)) else 0.0, eng)
            cur += timedelta(days=1)
    except Exception:
        entry_count = 0
        entry_engine_map = {}

    def _engine_for(symbol: str, side_key: str) -> str:
        key = (_report_symbol_key(symbol), side_key)
        rec = entry_engine_map.get(key)
        return rec[1] if rec else "DB"

    def _engine_for_trade(symbol: str, side_key: str, exec_ts: Optional[float]) -> str:
        key = (_report_symbol_key(symbol), side_key)
        events = entry_event_index.get(key)
        if not events:
            return _engine_for(symbol, side_key)
        if exec_ts is None:
            return _engine_for(symbol, side_key)
        events_sorted = sorted(events, key=lambda item: item[0])
        ts_list = [item[0] for item in events_sorted]
        idx = bisect.bisect_right(ts_list, float(exec_ts)) - 1
        if idx >= 0:
            return events_sorted[idx][1]
        return _engine_for(symbol, side_key)

    trade_rows = _load_income_trade_rows(start_date, end_date)
    grouped = {"LONG": {}, "SHORT": {}}
    for tr in trade_rows:
        side = tr.get("side")
        if side not in ("LONG", "SHORT"):
            continue
        symbol = tr.get("symbol") or ""
        pnl = tr.get("pnl")
        win_flag = True if isinstance(pnl, (int, float)) and pnl > 0 else False if isinstance(pnl, (int, float)) and pnl < 0 else None
        engine = _engine_for_trade(symbol, side, tr.get("ts"))
        order_id = tr.get("order_id")
        fee_usdt = tr.get("fee_usdt")
        key = str(order_id) if order_id else f"{symbol}|{side}|{tr.get('ts')}"
        bucket = grouped[side]
        rec = bucket.get(key)
        if rec is None:
            bucket[key] = {
                "symbol": symbol,
                "engine": engine,
                "pnl": pnl,
                "win": win_flag,
                "ts": tr.get("ts"),
                "order_id": order_id,
                "fee_usdt": fee_usdt if isinstance(fee_usdt, (int, float)) else None,
            }
        else:
            if isinstance(pnl, (int, float)):
                rec["pnl"] = float(rec.get("pnl") or 0.0) + float(pnl)
            if win_flag is True:
                rec["win"] = True
            elif win_flag is False and rec.get("win") is None:
                rec["win"] = False
            if isinstance(tr.get("ts"), (int, float)):
                cur_ts = rec.get("ts")
                if not isinstance(cur_ts, (int, float)) or tr.get("ts") < cur_ts:
                    rec["ts"] = tr.get("ts")
            if rec.get("fee_usdt") is None and isinstance(fee_usdt, (int, float)):
                rec["fee_usdt"] = fee_usdt
    symbol_trade_rows["LONG"] = list(grouped["LONG"].values())
    symbol_trade_rows["SHORT"] = list(grouped["SHORT"].values())
    for rec in grouped["LONG"].values():
        fee_usdt = rec.get("fee_usdt")
        if isinstance(fee_usdt, (int, float)) and isinstance(rec.get("pnl"), (int, float)):
            rec["pnl"] = float(rec.get("pnl")) - float(fee_usdt)
        if isinstance(rec.get("pnl"), (int, float)):
            rec["win"] = True if rec.get("pnl") > 0 else False if rec.get("pnl") < 0 else None
    for rec in grouped["SHORT"].values():
        fee_usdt = rec.get("fee_usdt")
        if isinstance(fee_usdt, (int, float)) and isinstance(rec.get("pnl"), (int, float)):
            rec["pnl"] = float(rec.get("pnl")) - float(fee_usdt)
        if isinstance(rec.get("pnl"), (int, float)):
            rec["win"] = True if rec.get("pnl") > 0 else False if rec.get("pnl") < 0 else None
    use_trade_rows = bool(symbol_trade_rows["LONG"] or symbol_trade_rows["SHORT"])
    for rec in db_rows:
        side = str(rec.get("pos_side") or "").upper()
        if side not in ("LONG", "SHORT"):
            continue
        pnl_raw = rec.get("realized_pnl_net") or rec.get("realized_pnl")
        try:
            pnl = float(pnl_raw) if pnl_raw != "" else None
        except Exception:
            pnl = None
        try:
            notional = float(rec.get("notional_sum")) if rec.get("notional_sum") != "" else 0.0
        except Exception:
            notional = 0.0
        win_flag = None
        if isinstance(pnl, (int, float)):
            if pnl > 0:
                win_flag = True
            elif pnl < 0:
                win_flag = False
        if not use_trade_rows:
            totals[side]["count"] += 1
            if isinstance(pnl, (int, float)):
                totals[side]["pnl"] += float(pnl)
                totals[side]["pnl_valid"] += 1
                if pnl > 0:
                    totals[side]["wins"] += 1
                elif pnl < 0:
                    totals[side]["losses"] += 1
            if isinstance(notional, (int, float)) and notional > 0:
                totals[side]["notional"] += notional
            symbol = rec.get("symbol") or ""
            engine = _engine_for(symbol, side)
            eng = engine_totals[side].setdefault(
                engine, {"count": 0, "pnl": 0.0, "pnl_valid": 0, "notional": 0.0, "wins": 0, "losses": 0}
            )
            eng["count"] += 1
            if isinstance(pnl, (int, float)):
                eng["pnl"] += float(pnl)
                eng["pnl_valid"] += 1
                if pnl > 0:
                    eng["wins"] += 1
                elif pnl < 0:
                    eng["losses"] += 1
            if isinstance(notional, (int, float)) and notional > 0:
                eng["notional"] += notional
            _update_symbol_row(side, symbol, engine, pnl, win_flag, None)
    label = f"{start_date}~{end_date}"
    if use_trade_rows:
        totals, engine_totals = _summarize_trade_rows(symbol_trade_rows)
    rows_out = symbol_trade_rows if use_trade_rows else symbol_rows
    return _format_report_output(totals, engine_totals, rows_out, label, compact, entry_count)

def _run_atlas_fabio_cycle(
    universe_structure,
    cached_ex,
    state,
    fabio_cfg,
    fabio_cfg_mid,
    atlas_cfg,
    active_positions_total,
    dir_hint,
    send_alert,
    entry_callback=None,
    now_ts: Optional[float] = None,
):
    result = {"long_hits": 0, "short_hits": 0}
    buf = []
    _set_thread_log_buffer(buf)

    def _atlasfabio_log_path() -> str:
        path = state.get("_atlasfabio_funnel_log_path") if isinstance(state, dict) else None
        if isinstance(path, str) and path:
            return path
        return os.path.join("logs", "fabio", "atlasfabio_funnel.log")

    def _append_atlasfabio_log(line: str) -> None:
        try:
            os.makedirs(os.path.dirname(_atlasfabio_log_path()), exist_ok=True)
            with open(_atlasfabio_log_path(), "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def _strength_mult(strength: float) -> float:
        if strength >= 0.85:
            return 1.2
        if strength >= 0.70:
            return 1.0
        if strength >= 0.55:
            return 0.7
        return 0.0

    if not ATLAS_FABIO_ENABLED:
        _set_thread_log_buffer(None)
        result["log"] = buf
        return result
    if not (fabio_cfg and atlas_cfg and fabio_entry_engine and atlas_fabio_engine):
        _set_thread_log_buffer(None)
        result["log"] = buf
        return result
    if not isinstance(active_positions_total, int):
        active_positions_total = sum(1 for st in state.values() if isinstance(st, dict) and st.get("in_pos"))
    now_ts = now_ts if isinstance(now_ts, (int, float)) else time.time()

    funnel = {
        "candidates_total": 0,
        "evaluated": 0,
        "skip_precheck": 0,
        "skip_eval_none": 0,
        "skip_warmup": 0,
        "skip_dup_candle": 0,
        "trigger_seen": 0,
        "entry_ready": 0,
        "blocked_ltf": 0,
        "blocked_cooldown": 0,
        "blocked_in_pos": 0,
        "no_signal": 0,
        "entry_signal": 0,
        "entry_order_ok": 0,
        "entry_blocked_guard": 0,
        "entry_lock_skip": 0,
        "entry_paper_ok": 0,
        "entry_gate_skip": 0,
        "entry_gate_skip_in_pos": 0,
        "entry_gate_skip_cooldown": 0,
        "entry_gate_skip_lock": 0,
        "entry_gate_skip_max_pos": 0,
        "entry_gate_skip_live_off": 0,
        "entry_gate_skip_exchange": 0,
        "atlas_gate_calls_long": 0,
        "atlas_gate_calls_short": 0,
        "atlas_block": 0,
        "d1_block": 0,
        "side_block": 0,
        "no_side_allowed": 0,
        "no_pullback": 0,
        "weak_signal": 0,
        "no_trigger": 0,
        "entry_live_off": 0,
        "trigger_hard": 0,
        "trigger_soft": 0,
        "strength_lt_055": 0,
        "strength_055_070": 0,
        "strength_070_085": 0,
        "strength_ge_085": 0,
        "data_missing": 0,
        "data_missing_rsi": 0,
        "data_missing_atr": 0,
        "data_missing_vol": 0,
        "data_missing_dist": 0,
    }

    for symbol in universe_structure:
        funnel["candidates_total"] += 1
        allowed_dir = None
        if isinstance(dir_hint, dict):
            allowed_dir = dir_hint.get(symbol)
        st = state.get(symbol, {"in_pos": False, "last_ok": False, "last_entry": 0})
        if st.get("in_pos"):
            funnel["skip_precheck"] += 1
            funnel["blocked_in_pos"] += 1
            continue
        if allowed_dir in ("LONG", "SHORT"):
            last_entry = _get_last_entry_ts_by_side(st, allowed_dir)
            if isinstance(last_entry, (int, float)) and (now_ts - float(last_entry)) < COOLDOWN_SEC:
                funnel["skip_precheck"] += 1
                funnel["blocked_cooldown"] += 1
                continue

        if allowed_dir == "LONG":
            funnel["atlas_gate_calls_long"] += 1
            gate = _atlasfabio_gate_long(symbol, atlas_cfg)
        elif allowed_dir == "SHORT":
            funnel["atlas_gate_calls_short"] += 1
            gate = _atlasfabio_gate_short(symbol, atlas_cfg)
        else:
            gate = _atlasfabio_gate(symbol, atlas_cfg) if atlas_cfg else {"status": "skip"}
        if gate.get("status") != "ok":
            funnel["skip_warmup"] += 1
            continue

        trade_allowed = bool(gate.get("trade_allowed"))
        allow_long = bool(gate.get("allow_long"))
        allow_short = bool(gate.get("allow_short"))
        size_mult = float(gate.get("size_mult") or 1.0)
        gate_line = (
            "ATLASFABIO_GATE sym=%s trade_allowed=%s allow_long=%s allow_short=%s size_mult=%.2f"
            % (
                symbol,
                "Y" if trade_allowed else "N",
                "Y" if allow_long else "N",
                "Y" if allow_short else "N",
                size_mult,
            )
        )
        print(gate_line)
        _append_atlasfabio_log(gate_line)

        if not trade_allowed:
            funnel["atlas_block"] += 1
            block_reason = gate.get("block_reason") or "ATLAS_BLOCK"
            if block_reason == "D1_EMA7_DIST":
                funnel["d1_block"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason={block_reason}")
            continue

        allowed_sides = []
        if allow_long:
            allowed_sides.append("LONG")
        if allow_short:
            allowed_sides.append("SHORT")
        if not allowed_sides:
            funnel["no_side_allowed"] += 1
            _append_atlasfabio_log(
                "ATLASFABIO_SKIP sym=%s reason=NO_SIDE_ALLOWED gate_allow_long=%s gate_allow_short=%s"
                % (symbol, "Y" if allow_long else "N", "Y" if allow_short else "N")
            )
            continue

        side_results = []
        had_eval_none = False
        had_warmup = False
        had_dup = False
        for side in allowed_sides:
            bucket_key = f"_fabio_atlas_{side.lower()}"
            fabio_res = fabio_entry_engine.evaluate_symbol(
                symbol,
                cached_ex,
                state,
                fabio_cfg,
                bucket_key=bucket_key,
                side_hint=side,
            )
            if not isinstance(fabio_res, dict) or not fabio_res:
                had_eval_none = True
                continue
            if fabio_res.get("status") == "warmup":
                had_warmup = True
                continue
            if fabio_res.get("block_reason") == "dup_candle":
                had_dup = True
                continue
            side_results.append((side, fabio_res))

        if not side_results:
            if had_eval_none:
                funnel["skip_eval_none"] += 1
            if had_warmup:
                funnel["skip_warmup"] += 1
            if had_dup:
                funnel["skip_dup_candle"] += 1
            continue

        funnel["evaluated"] += 1

        def _fmt_num(val: Optional[float], digits: int = 4) -> str:
            if isinstance(val, (int, float)):
                return f"{val:.{digits}f}"
            return "N/A"

        rsi_3m_prev = None
        rsi_3m_last = None
        rsi_5m_prev = None
        rsi_5m_last = None
        if rsi_engine:
            rsi_3m_prev, rsi_3m_last = rsi_engine.fetch_rsi_last2(symbol, "3m")
            rsi_5m_prev, rsi_5m_last = rsi_engine.fetch_rsi_last2(symbol, "5m")

        missing_side_results = []
        valid_side_results = []
        for side, fabio_res in side_results:
            missing = []
            if fabio_res.get("rsi") is None:
                missing.append("rsi")
            if fabio_res.get("atr_now") is None:
                missing.append("atr")
            if fabio_res.get("vol_avg") is None:
                missing.append("vol_avg")
            if fabio_res.get("vol_ratio") is None:
                missing.append("vol_ratio")
            if fabio_res.get("dist_to_ema20") is None:
                missing.append("dist")
            if missing:
                missing_side_results.append((side, fabio_res, missing))
            else:
                valid_side_results.append((side, fabio_res))

        if not valid_side_results:
            funnel["data_missing"] += 1
            side, fabio_res, missing = missing_side_results[0]
            for miss_item in missing:
                if miss_item == "rsi":
                    funnel["data_missing_rsi"] += 1
                elif miss_item == "atr":
                    funnel["data_missing_atr"] += 1
                elif miss_item in ("vol_avg", "vol_ratio"):
                    funnel["data_missing_vol"] += 1
                elif miss_item == "dist":
                    funnel["data_missing_dist"] += 1
            tf_info = []
            raw_by_tf = {}
            for tf in ("3m", "5m", "15m", "1h"):
                raw = cycle_cache.get_raw(symbol, tf) or []
                raw_by_tf[tf] = raw
                bars = len(raw)
                last_ts = raw[-1][0] if raw else None
                tf_info.append(f"bars_{tf}={bars}")
                tf_info.append(f"last_ts_{tf}={last_ts if last_ts is not None else 'N/A'}")
            series_len_info = "series_len={3m:%d,5m:%d,15m:%d}" % (
                len(raw_by_tf.get("3m") or []),
                len(raw_by_tf.get("5m") or []),
                len(raw_by_tf.get("15m") or []),
            )
            rsi_len = rsi_engine.config.rsi_len if rsi_engine else None
            key_3m = f"{symbol}:3m:rsi_series:{rsi_len}" if rsi_len else "N/A"
            key_5m = f"{symbol}:5m:rsi_series:{rsi_len}" if rsi_len else "N/A"
            series_key_info = f"series_key={{rsi3m:'{key_3m}',rsi5m:'{key_5m}'}}"
            values_info = (
                "values={rsi3m_last=%s,rsi5m_last=%s,atr=%s,vol_now=%s,vol_avg=%s,vol_ratio=%s,dist=%s}"
                % (
                    _fmt_num(rsi_3m_last, 2),
                    _fmt_num(rsi_5m_last, 2),
                    _fmt_num(fabio_res.get("atr_now"), 4),
                    _fmt_num(fabio_res.get("vol_now"), 2),
                    _fmt_num(fabio_res.get("vol_avg"), 2),
                    _fmt_num(fabio_res.get("vol_ratio"), 2),
                    _fmt_num(fabio_res.get("dist_to_ema20"), 4),
                )
            )
            rsi_map_keys = []
            for (sym_key, tf_key, ind_key) in cycle_cache.IND_CACHE.keys():
                if sym_key != symbol:
                    continue
                if not isinstance(ind_key, tuple) or not ind_key:
                    continue
                if ind_key[0] != "rsi_series":
                    continue
                if len(ind_key) < 2:
                    continue
                rsi_map_keys.append(f"{tf_key}:{ind_key[1]}")
            rsi_map_keys.sort()
            requested_keys = "['3m','5m']"
            resolved_key_3m = f"{symbol}:3m:rsi_series:{rsi_len}" if rsi_len else "N/A"
            resolved_key_5m = f"{symbol}:5m:rsi_series:{rsi_len}" if rsi_len else "N/A"
            rsi_map_info = (
                "rsi_map_keys=%s requested_keys=%s resolved_key_3m=%s resolved_key_5m=%s"
                % (rsi_map_keys, requested_keys, resolved_key_3m, resolved_key_5m)
            )
            miss_line = (
                "ATLASFABIO_SKIP sym=%s reason=DATA_MISSING missing=%s %s %s %s %s %s"
                % (
                    symbol,
                    "[" + ",".join(missing) + "]",
                    values_info,
                    series_len_info,
                    series_key_info,
                    rsi_map_info,
                    " ".join(tf_info),
                )
            )
            print(miss_line)
            _append_atlasfabio_log(miss_line)
            continue

        backtest_mode = bool(state.get("_atlasfabio_backtest_mode")) if isinstance(state, dict) else False
        def _finalize_backtest_live_off(reason: str) -> bool:
            if not backtest_mode:
                return False
            if reason != "live_off":
                return False
            funnel["entry_live_off"] += 1
            funnel["entry_signal"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_ENTRY sym={symbol} pass=Y mode=BACKTEST")
            return True

        candidates = []
        for side, fabio_res in valid_side_results:
            trigger_ok = bool(fabio_res.get("signal_trigger_ok"))
            trigger_hard = bool(fabio_res.get("trigger_hard"))
            trigger_soft = bool(fabio_res.get("trigger_soft"))
            trigger_mode = fabio_res.get("trigger_mode") or "NONE"
            strength = float(fabio_res.get("signal_strength") or 0.0)
            factors = fabio_res.get("signal_factors") or {}
            weights = fabio_res.get("signal_weights") or {}
            trigger_factors = fabio_res.get("trigger_factors") or {}
            signal_side = fabio_res.get("signal_side")
            atr_now = fabio_res.get("atr_now")
            if isinstance(atr_now, (int, float)) and abs(atr_now) <= 1e-8:
                atr_line = "FABIO_ATR_ZERO sym=%s atr=%s ltf=%s" % (
                    symbol,
                    _fmt_num(atr_now, 8),
                    fabio_cfg.timeframe_ltf,
                )
                print(atr_line)
                _append_atlasfabio_log(atr_line)

            if trigger_hard:
                funnel["trigger_hard"] += 1
            if trigger_soft:
                funnel["trigger_soft"] += 1
            if strength < 0.55:
                funnel["strength_lt_055"] += 1
            elif strength < 0.70:
                funnel["strength_055_070"] += 1
            elif strength < 0.85:
                funnel["strength_070_085"] += 1
            else:
                funnel["strength_ge_085"] += 1

            if (rsi_3m_last is None) or (rsi_5m_last is None):
                rsi_map_keys = []
                for (sym_key, tf_key, ind_key) in cycle_cache.IND_CACHE.keys():
                    if sym_key != symbol:
                        continue
                    if not isinstance(ind_key, tuple) or not ind_key:
                        continue
                    if ind_key[0] != "rsi_series":
                        continue
                    if len(ind_key) < 2:
                        continue
                    rsi_map_keys.append(f"{tf_key}:{ind_key[1]}")
                rsi_map_keys.sort()
                bars_3m = len(cycle_cache.get_raw(symbol, "3m") or [])
                bars_5m = len(cycle_cache.get_raw(symbol, "5m") or [])
                rsi_len = rsi_engine.config.rsi_len if rsi_engine else None
                lookup_line = (
                    "FABIO_RSI_LOOKUP sym=%s rsi3m=%s rsi5m=%s bars_3m=%d bars_5m=%d rsi_len=%s rsi_map_keys=%s"
                    % (
                        symbol,
                        _fmt_num(rsi_3m_last, 2),
                        _fmt_num(rsi_5m_last, 2),
                        bars_3m,
                        bars_5m,
                        str(rsi_len) if rsi_len is not None else "N/A",
                        rsi_map_keys,
                    )
                )
                print(lookup_line)
                _append_atlasfabio_log(lookup_line)

            fabio_signal_line = (
                "FABIO_SIGNAL sym=%s side=%s trigger_ok=%s strength=%.2f"
                % (symbol, signal_side or side, "Y" if trigger_ok else "N", strength)
            )
            fabio_factors_line = (
                "FABIO_FACTORS sym=%s ema_align=%s rsi_rev=%s vol=%s retest=%s struct=%s dist=%s weights=%s"
                % (
                    symbol,
                    "Y" if factors.get("ema_align") else "N",
                    "Y" if factors.get("rsi_reversal") else "N",
                    "Y" if factors.get("vol") else "N",
                    "Y" if factors.get("retest") else "N",
                    "Y" if factors.get("structure") else "N",
                    "Y" if factors.get("dist") else "N",
                    weights,
                )
            )
            fabio_trigger_decision = (
                "FABIO_TRIGGER_DECISION sym=%s hard=%s soft=%s mode=%s soft_min=%.2f"
                % (
                    symbol,
                    "Y" if trigger_hard else "N",
                    "Y" if trigger_soft else "N",
                    trigger_mode,
                    ATLASFABIO_MIN_STRENGTH_SOFT,
                )
            )
            if trigger_ok or strength >= ATLASFABIO_MIN_STRENGTH:
                print(fabio_signal_line)
                print(fabio_factors_line)
                print(fabio_trigger_decision)
                _append_atlasfabio_log(fabio_signal_line)
                _append_atlasfabio_log(fabio_factors_line)
                _append_atlasfabio_log(fabio_trigger_decision)

            if not trigger_ok:
                trigger_line = (
                    "FABIO_TRIGGER_FACTORS sym=%s rsi=%s retest=%s vol=%s struct=%s ema=%s "
                    "rsi3m_prev=%s rsi3m_last=%s rsi5m_prev=%s rsi5m_last=%s "
                    "rsi_ltf=%s vol_now=%s vol_avg=%s vol_ratio=%s dist=%.4f atr=%s"
                    % (
                        symbol,
                        "Y" if trigger_factors.get("rsi") else "N",
                        "Y" if trigger_factors.get("retest") else "N",
                        "Y" if trigger_factors.get("vol") else "N",
                        "Y" if trigger_factors.get("struct") else "N",
                        "Y" if trigger_factors.get("ema") else "N",
                        _fmt_num(rsi_3m_prev, 2),
                        _fmt_num(rsi_3m_last, 2),
                        _fmt_num(rsi_5m_prev, 2),
                        _fmt_num(rsi_5m_last, 2),
                        _fmt_num(fabio_res.get("rsi"), 2),
                        _fmt_num(fabio_res.get("vol_now"), 2),
                        _fmt_num(fabio_res.get("vol_avg"), 2),
                        _fmt_num(fabio_res.get("vol_ratio"), 2),
                        float(fabio_res.get("dist_to_ema20") or 0.0),
                        _fmt_num(fabio_res.get("atr_now"), 6),
                    )
                )
                print(trigger_line)
                print(fabio_trigger_decision)
                _append_atlasfabio_log(trigger_line)
                _append_atlasfabio_log(fabio_trigger_decision)

            if trigger_ok and (signal_side == side):
                candidates.append((side, fabio_res))

        if candidates:
            funnel["trigger_seen"] += len(candidates)
        else:
            funnel["no_trigger"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=NO_TRIGGER")
            continue

        def _candidate_key(item):
            side, fabio_res = item
            strength = float(fabio_res.get("signal_strength") or 0.0)
            mode = fabio_res.get("trigger_mode") or "NONE"
            hard_flag = 1 if mode == "HARD" else 0
            return (strength, hard_flag)

        best_side, best_res = max(candidates, key=_candidate_key)
        if len(allowed_sides) > 1:
            cand_parts = []
            for side, fabio_res in candidates:
                cand_parts.append(f"{side}:{float(fabio_res.get('signal_strength') or 0.0):.2f}")
            _append_atlasfabio_log(
                "ATLASFABIO_SIDE_SELECT sym=%s candidates=[%s] chosen=%s"
                % (symbol, ", ".join(cand_parts), best_side)
            )

        side = best_side
        _append_atlas_route_log(
            "ATLASFABIO",
            symbol,
            {
                "side": side,
                "dir": gate.get("st_dir"),
                "state": gate.get("state"),
                "regime": gate.get("regime"),
                "reason": gate.get("reason"),
                "trade_allowed": gate.get("trade_allowed"),
                "allow_long": gate.get("allow_long"),
                "allow_short": gate.get("allow_short"),
                "size_mult": size_mult,
            },
        )
        trigger_hard = bool(best_res.get("trigger_hard"))
        trigger_soft = bool(best_res.get("trigger_soft"))
        trigger_mode = best_res.get("trigger_mode") or "NONE"
        strength = float(best_res.get("signal_strength") or 0.0)
        reasons = best_res.get("signal_reasons") or []

        if "retest" not in reasons:
            funnel["no_pullback"] += 1
            _append_atlasfabio_log(
                "ATLASFABIO_SKIP sym=%s reason=NO_PULLBACK strength=%.2f reasons=%s"
                % (symbol, strength, reasons)
            )
            continue

        if trigger_hard:
            min_strength = ATLASFABIO_MIN_STRENGTH
        elif trigger_soft:
            min_strength = ATLASFABIO_MIN_STRENGTH_SOFT
        else:
            min_strength = ATLASFABIO_MIN_STRENGTH
        if strength < min_strength:
            funnel["weak_signal"] += 1
            _append_atlasfabio_log(
                "ATLASFABIO_SKIP sym=%s reason=WEAK_SIGNAL strength=%.2f < %.2f mode=%s"
                % (symbol, strength, min_strength, trigger_mode)
            )
            continue

        strength_mult = _strength_mult(strength)
        final_usdt = _resolve_entry_usdt(USDT_PER_TRADE * size_mult * strength_mult)
        ltf_mode = "5m_or_3m"
        hit5m, hit3m, ltf_ok = _atlasfabio_ltf_hit(symbol, side, mode=ltf_mode)
        if not ltf_ok and best_res.get("trend_cont_trigger"):
            ltf_ok = True
        if not ltf_ok:
            funnel["blocked_ltf"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=LTF_BLOCK")
            continue

        try:
            refresh_positions_cache(force=True)
            if side == "LONG":
                existing_amt = get_long_position_amount(symbol)
            else:
                existing_amt = get_short_position_amount(symbol)
        except Exception:
            existing_amt = 0.0
        if isinstance(existing_amt, (int, float)) and existing_amt > 0:
            st["in_pos"] = True
            state[symbol] = st
            funnel["blocked_in_pos"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=IN_POS")
            continue

        funnel["entry_ready"] += 1

        cur_total = count_open_positions(force=True)
        if not isinstance(cur_total, int):
            cur_total = active_positions_total
        gate_ok, gate_reason = _atlasfabio_entry_gate(
            symbol,
            side,
            st,
            now_ts,
            "NA",
            None,
            live_ok=LONG_LIVE_TRADING if side == "LONG" else LIVE_TRADING,
            max_positions=MAX_OPEN_POSITIONS,
            active_positions=cur_total,
        )
        if (not gate_ok) and backtest_mode and gate_reason == "live_off":
            funnel["entry_live_off"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_ENTRY sym={symbol} pass=Y mode=BACKTEST")
            gate_ok = True
        if not gate_ok:
            funnel["entry_gate_skip"] += 1
            if gate_reason == "in_pos":
                funnel["entry_gate_skip_in_pos"] += 1
            elif gate_reason == "cooldown":
                funnel["entry_gate_skip_cooldown"] += 1
            elif gate_reason == "max_pos":
                funnel["entry_gate_skip_max_pos"] += 1
            elif gate_reason == "live_off":
                funnel["entry_gate_skip_live_off"] += 1
                if _finalize_backtest_live_off(gate_reason):
                    continue
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=ENTRY_GATE_{gate_reason}")
            continue

        lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="atlasfabio", side=side)
        if not lock_ok:
            funnel["entry_lock_skip"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=ENTRY_LOCK")
            print(f"[ENTRY-LOCK] sym={symbol} owner=atlasfabio ok=0 held_by={lock_owner} age_s={lock_age:.1f}")
            continue
        if _entry_seen_blocked(state, symbol, side, "atlasfabio"):
            funnel["entry_lock_skip"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=ENTRY_SEEN")
            _entry_lock_release(state, symbol, owner="atlasfabio")
            continue
        entry_line = (
            "ATLASFABIO_ENTRY sym=%s side=%s pass=Y strength=%.2f atlas_mult=%.2f strength_mult=%.2f final_usdt=%.2f reasons=%s"
            % (symbol, side, strength, size_mult, strength_mult, final_usdt, reasons)
        )
        print(entry_line)
        _append_atlasfabio_log(entry_line)
        if not backtest_mode:
            _append_entry_log("fabio/atlasfabio_entries.log", entry_line)
        if callable(entry_callback):
            try:
                entry_callback(
                    symbol=symbol,
                    side=side,
                    strength=strength,
                    reasons=reasons,
                    gate=gate,
                    fabio_res=best_res,
                )
            except Exception:
                pass

        funnel["entry_signal"] += 1
        if side == "LONG":
            result["long_hits"] += 1
        else:
            result["short_hits"] += 1

        cur_total = count_open_positions(force=True)
        if not isinstance(cur_total, int):
            cur_total = active_positions_total
        if cur_total >= MAX_OPEN_POSITIONS:
            funnel["entry_gate_skip_max_pos"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=MAX_POS")
            _append_entry_gate_log(
                "atlasfabio",
                symbol,
                f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side={side}",
                side=side,
            )
            _entry_lock_release(state, symbol, owner="atlasfabio")
            continue

        if ATLAS_FABIO_PAPER:
            funnel["entry_paper_ok"] += 1
            _append_atlasfabio_log(f"ATLASFABIO_ENTRY sym={symbol} pass=Y mode=PAPER")
            _entry_lock_release(state, symbol, owner="atlasfabio")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if side == "LONG":
            if LONG_LIVE_TRADING:
                guard_key = _entry_guard_key(state, symbol, "LONG")
                if not _entry_guard_acquire(state, symbol, key=guard_key, engine="atlasfabio", side=side):
                    funnel["entry_blocked_guard"] += 1
                    _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=ENTRY_GUARD")
                    _entry_lock_release(state, symbol, owner="atlasfabio")
                else:
                    try:
                        res = long_market(
                            symbol,
                            usdt_amount=final_usdt,
                            leverage=LEVERAGE,
                            margin_mode=MARGIN_MODE,
                        )
                        entry_order_id = _order_id_from_res(res)
                        fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                        qty = res.get("amount") or res.get("order", {}).get("amount")
                        if res.get("order") or res.get("status") in ("ok", "dry_run"):
                            funnel["entry_order_ok"] += 1
                            _entry_seen_mark(state, symbol, "LONG", "atlasfabio")
                        st = state.get(symbol, {})
                        st["in_pos"] = True
                        _set_last_entry_state(st, "LONG", time.time())
                        state[symbol] = st
                        _log_trade_entry(
                            state,
                            side="LONG",
                            symbol=symbol,
                            entry_ts=time.time(),
                            entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                            qty=qty if isinstance(qty, (int, float)) else None,
                            usdt=final_usdt,
                            entry_order_id=entry_order_id,
                            meta={"reason": "atlasfabio_long"},
                        )
                        active_positions_total += 1
                        if send_alert:
                            _send_entry_alert(
                                send_alert,
                                side="LONG",
                                symbol=symbol,
                                engine="ATLASFABIO",
                                entry_price=fill_price,
                                usdt=final_usdt,
                                reason="ATLAS + FABIO",
                                live=LONG_LIVE_TRADING,
                                entry_order_id=entry_order_id,
                                sl=_fmt_price_safe(fill_price, AUTO_EXIT_LONG_SL_PCT, side="LONG"),
                                tp=None,
                            )
                    except Exception as e:
                        funnel["entry_gate_skip_exchange"] += 1
                        _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=EXCHANGE_ERROR {e}")
                    finally:
                        _entry_guard_release(state, symbol, key=guard_key)
                        _entry_lock_release(state, symbol, owner="atlasfabio")
            else:
                _append_atlasfabio_log(f"ATLASFABIO_ENTRY sym={symbol} pass=Y mode=SIGNAL")
                _send_entry_alert(
                    send_alert,
                    side="LONG",
                    symbol=symbol,
                    engine="ATLASFABIO",
                    entry_price=None,
                    usdt=final_usdt,
                    reason="ATLAS + FABIO",
                    live=LONG_LIVE_TRADING,
                    order_info="(알림 전용)",
                    sl=None,
                    tp=None,
                )
                _entry_lock_release(state, symbol, owner="atlasfabio")
        else:
            order_info = "(알림 전용)"
            entry_price_disp = None
            entry_order_id = None
            if LIVE_TRADING:
                guard_key = _entry_guard_key(state, symbol, "SHORT")
                if not _entry_guard_acquire(state, symbol, key=guard_key, engine="atlasfabio", side=side):
                    funnel["entry_blocked_guard"] += 1
                    _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=ENTRY_GUARD")
                    _entry_lock_release(state, symbol, owner="atlasfabio")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                try:
                    if get_short_position_amount(symbol) > 0:
                        _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=IN_POS")
                        _entry_lock_release(state, symbol, owner="atlasfabio")
                        time.sleep(PER_SYMBOL_SLEEP)
                        continue
                    res = short_market(symbol, usdt_amount=final_usdt, leverage=LEVERAGE, margin_mode=MARGIN_MODE)
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    order_info = (
                        f"entry_price={fill_price} qty={qty} usdt={final_usdt:.2f}"
                    )
                    entry_price_disp = fill_price
                    st["in_pos"] = True
                    _set_last_entry_state(st, "SHORT", time.time())
                    funnel["entry_order_ok"] += 1
                    state[symbol] = st
                    if res.get("order") or res.get("status") in ("ok", "dry_run"):
                        _entry_seen_mark(state, symbol, "SHORT", "atlasfabio")
                    _log_trade_entry(
                        state,
                        side="SHORT",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=final_usdt,
                        entry_order_id=entry_order_id,
                        meta={"reason": "atlasfabio_short"},
                    )
                    active_positions_total += 1
                    if send_alert:
                        _send_entry_alert(
                            send_alert,
                            side="SHORT",
                            symbol=symbol,
                            engine="ATLASFABIO",
                            entry_price=fill_price,
                            usdt=final_usdt,
                            reason=reasons,
                            live=LIVE_TRADING,
                            order_info=order_info,
                            entry_order_id=entry_order_id,
                            sl=_fmt_price_safe(fill_price, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                            tp=None,
                        )
                except Exception as e:
                    order_info = f"order_error: {e}"
                    funnel["entry_gate_skip_exchange"] += 1
                    _append_atlasfabio_log(f"ATLASFABIO_SKIP sym={symbol} reason=EXCHANGE_ERROR {e}")
                finally:
                    _entry_guard_release(state, symbol, key=guard_key)
                    _entry_lock_release(state, symbol, owner="atlasfabio")
            else:
                _append_atlasfabio_log(f"ATLASFABIO_ENTRY sym={symbol} pass=Y mode=SIGNAL")
                _entry_lock_release(state, symbol, owner="atlasfabio")
            _send_entry_alert(
                send_alert,
                side="SHORT",
                symbol=symbol,
                engine="ATLASFABIO",
                entry_price=entry_price_disp,
                usdt=final_usdt,
                live=LIVE_TRADING,
                order_info=order_info,
                sl=_fmt_price_safe(entry_price_disp, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                tp=None,
            )
            time.sleep(PER_SYMBOL_SLEEP)

    _set_thread_log_buffer(None)
    funnel_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    funnel_line = (
        "{ts} [atlasfabio-funnel] total={candidates_total} evaluated={evaluated} precheck={skip_precheck} "
        "eval_none={skip_eval_none} gate_calls_long={atlas_gate_calls_long} gate_calls_short={atlas_gate_calls_short} "
        "warmup={skip_warmup} dup={skip_dup_candle} trigger_seen={trigger_seen} "
        "entry_ready={entry_ready} entry={entry_signal} order_ok={entry_order_ok} "
        "ltf_block={blocked_ltf} cooldown={blocked_cooldown} in_pos={blocked_in_pos} "
        "gate_skip={entry_gate_skip} gate_in_pos={entry_gate_skip_in_pos} gate_cooldown={entry_gate_skip_cooldown} "
        "gate_maxpos={entry_gate_skip_max_pos} gate_live_off={entry_gate_skip_live_off} "
        "exchange_skip={entry_gate_skip_exchange} atlas_block={atlas_block} d1_block={d1_block} side_block={side_block} "
        "no_side_allowed={no_side_allowed} no_pullback={no_pullback} "
        "entry_live_off={entry_live_off} "
        "trigger_hard={trigger_hard} trigger_soft={trigger_soft} "
        "strength_lt_055={strength_lt_055} strength_055_070={strength_055_070} "
        "strength_070_085={strength_070_085} strength_ge_085={strength_ge_085} "
        "weak_signal={weak_signal} no_trigger={no_trigger} data_missing={data_missing} "
        "data_missing_rsi={data_missing_rsi} data_missing_atr={data_missing_atr} "
        "data_missing_vol={data_missing_vol} data_missing_dist={data_missing_dist}".format(ts=funnel_ts, **funnel)
    )
    if not state.get("_atlasfabio_backtest_quiet"):
        print(funnel_line)
        _append_atlasfabio_log(funnel_line)
    result["log"] = buf
    return result
def _run_dtfx_cycle(
    dtfx_engine,
    dtfx_universe,
    cached_ex,
    state,
    dtfx_cfg,
    active_positions_total,
    send_alert,
):
    result = {"long_hits": 0, "short_hits": 0}
    buf = []
    _set_thread_log_buffer(buf)
    if not dtfx_engine or not dtfx_cfg:
        _set_thread_log_buffer(None)
        result["log"] = buf
        return result
    if not dtfx_universe:
        print("[dtfx] 스캔 대상 없음")
        _set_thread_log_buffer(None)
        result["log"] = buf
        return result
    now_ts = time.time()
    ctx = EngineContext(
        exchange=cached_ex,
        state=state,
        now_ts=now_ts,
        logger=print,
        config=dtfx_cfg,
    )
    for symbol in dtfx_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if st.get("in_pos"):
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        signals = dtfx_engine.scan_symbol(ctx, symbol)
        if not signals:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        for sig in signals:
            side = (sig.side or "").upper()
            if side not in ("LONG", "SHORT"):
                continue
            last_entry = _get_last_entry_ts_by_side(st, side)
            if isinstance(last_entry, (int, float)) and (now_ts - float(last_entry)) < COOLDOWN_SEC:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "LONG":
                try:
                    existing_amt = get_long_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
            if side == "SHORT":
                try:
                    existing_amt = get_short_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    st["in_pos"] = True
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
            cur_total = count_open_positions(force=True)
            if not isinstance(cur_total, int):
                cur_total = active_positions_total
            if cur_total >= MAX_OPEN_POSITIONS:
                print(f"[dtfx] 제한 {cur_total}/{MAX_OPEN_POSITIONS} → 스킵 ({symbol})")
                _append_entry_gate_log(
                    "dtfx",
                    symbol,
                    f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side={side}",
                    side=side,
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _exit_cooldown_blocked(state, symbol, "dtfx", side):
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "LONG" and not LONG_LIVE_TRADING:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "SHORT" and not LIVE_TRADING:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="dtfx", side=side)
            if not lock_ok:
                print(f"[ENTRY-LOCK] sym={symbol} owner=dtfx ok=0 held_by={lock_owner} age_s={lock_age:.1f}")
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            guard_key = _entry_guard_key(state, symbol, side)
            if not _entry_guard_acquire(state, symbol, key=guard_key, engine="dtfx", side=side):
                print(f"[dtfx] {side} 중복 차단 ({symbol})")
                _entry_lock_release(state, symbol, owner="dtfx")
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _entry_seen_blocked(state, symbol, side, "dtfx"):
                _entry_lock_release(state, symbol, owner="dtfx")
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            try:
                if side == "LONG":
                    res = long_market(symbol, usdt_amount=_resolve_entry_usdt(), leverage=LEVERAGE, margin_mode=MARGIN_MODE)
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    result["long_hits"] += 1
                    _set_last_entry_state(st, "LONG", time.time())
                    state[symbol] = st
                    if res.get("order") or res.get("status") in ("ok", "dry_run"):
                        _entry_seen_mark(state, symbol, "LONG", "dtfx")
                    _log_trade_entry(
                        state,
                        side="LONG",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=_resolve_entry_usdt(),
                        entry_order_id=entry_order_id,
                        meta={"reason": "dtfx_long", "event": sig.pattern},
                    )
                    active_positions_total += 1
                    if write_dtfx_log:
                        touch_ts = None
                        try:
                            touch_ts = (sig.meta or {}).get("dtfx", {}).get("touch", {}).get("ts")
                        except Exception:
                            touch_ts = None
                        payload = {
                            "ts": int(touch_ts or time.time() * 1000),
                            "engine": (sig.meta or {}).get("dtfx_engine", "dtfx"),
                            "symbol": symbol,
                            "tf_ltf": (sig.meta or {}).get("dtfx_tf_ltf", dtfx_cfg.tf_ltf),
                            "tf_mtf": (sig.meta or {}).get("dtfx_tf_mtf", dtfx_cfg.tf_mtf),
                            "tf_htf": (sig.meta or {}).get("dtfx_tf_htf", dtfx_cfg.tf_htf),
                            "event": (sig.meta or {}).get("dtfx_event", sig.pattern),
                            "state": (sig.meta or {}).get("dtfx_state", "ENTRY"),
                            "context": {
                                "side": "LONG",
                                "entry_price": fill_price,
                                "qty": qty,
                                "usdt": _resolve_entry_usdt(),
                                "dtfx": (sig.meta or {}).get("dtfx", {}),
                            },
                        }
                        write_dtfx_log(payload, prefix="entries", mode="entry")
                    if send_alert:
                        _send_entry_alert(
                            send_alert,
                            side="LONG",
                            symbol=symbol,
                            engine="DTFX",
                            entry_price=fill_price,
                            usdt=_resolve_entry_usdt(),
                            reason=f"event={sig.pattern}",
                            live=LONG_LIVE_TRADING,
                            entry_order_id=entry_order_id,
                            sl=_fmt_price_safe(fill_price, AUTO_EXIT_LONG_SL_PCT, side="LONG"),
                        )
                else:
                    res = short_market(symbol, usdt_amount=_resolve_entry_usdt(), leverage=LEVERAGE, margin_mode=MARGIN_MODE)
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    result["short_hits"] += 1
                    st["in_pos"] = True
                    _set_last_entry_state(st, "SHORT", time.time())
                    state[symbol] = st
                    if res.get("order") or res.get("status") in ("ok", "dry_run"):
                        _entry_seen_mark(state, symbol, "SHORT", "dtfx")
                    _log_trade_entry(
                        state,
                        side="SHORT",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=_resolve_entry_usdt(),
                        entry_order_id=entry_order_id,
                        meta={"reason": "dtfx_short", "event": sig.pattern},
                    )
                    active_positions_total += 1
                    if write_dtfx_log:
                        touch_ts = None
                        try:
                            touch_ts = (sig.meta or {}).get("dtfx", {}).get("touch", {}).get("ts")
                        except Exception:
                            touch_ts = None
                        payload = {
                            "ts": int(touch_ts or time.time() * 1000),
                            "engine": (sig.meta or {}).get("dtfx_engine", "dtfx"),
                            "symbol": symbol,
                            "tf_ltf": (sig.meta or {}).get("dtfx_tf_ltf", dtfx_cfg.tf_ltf),
                            "tf_mtf": (sig.meta or {}).get("dtfx_tf_mtf", dtfx_cfg.tf_mtf),
                            "tf_htf": (sig.meta or {}).get("dtfx_tf_htf", dtfx_cfg.tf_htf),
                            "event": (sig.meta or {}).get("dtfx_event", sig.pattern),
                            "state": (sig.meta or {}).get("dtfx_state", "ENTRY"),
                            "context": {
                                "side": "SHORT",
                                "entry_price": fill_price,
                                "qty": qty,
                                "usdt": _resolve_entry_usdt(),
                                "dtfx": (sig.meta or {}).get("dtfx", {}),
                            },
                        }
                        write_dtfx_log(payload, prefix="entries", mode="entry")
                    if send_alert:
                        _send_entry_alert(
                            send_alert,
                            side="SHORT",
                            symbol=symbol,
                            engine="DTFX",
                            entry_price=fill_price,
                            usdt=_resolve_entry_usdt(),
                            reason=f"event={sig.pattern}",
                            live=LIVE_TRADING,
                            entry_order_id=entry_order_id,
                            sl=_fmt_price_safe(fill_price, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                        )
            except Exception as e:
                print(f"[dtfx] order error {symbol} side={side}: {e}")
            finally:
                _entry_guard_release(state, symbol, key=guard_key)
                _entry_lock_release(state, symbol, owner="dtfx")
        time.sleep(PER_SYMBOL_SLEEP)
    _set_thread_log_buffer(None)
    result["log"] = buf
    return result

def _pumpfade_tf_seconds(tf: str) -> int:
    try:
        unit = tf[-1]
        val = int(tf[:-1])
    except Exception:
        return 0
    if unit == "m":
        return val * 60
    if unit == "h":
        return val * 3600
    if unit == "d":
        return val * 86400
    return 0

def _run_pumpfade_cycle(
    pumpfade_engine,
    pumpfade_universe,
    state: Dict[str, dict],
    send_alert,
    pumpfade_cfg,
):
    result = {"hits": 0}
    if not pumpfade_engine or not pumpfade_cfg:
        return result
    if not pumpfade_universe:
        print("[pumpfade] 스캔 대상 없음")
        return result
    now_ts = time.time()
    tf_sec = _pumpfade_tf_seconds(pumpfade_cfg.tf_trigger) or 900
    bucket = state.setdefault("_pumpfade", {})
    date_tag = time.strftime("%Y%m%d")
    _ensure_log_file(f"pumpfade/pumpfade_live_{date_tag}.log")
    _ensure_log_file(f"pumpfade/pumpfade_entries_{date_tag}.log")

    def _pumpfade_log(msg: str) -> None:
        _append_entry_log(f"pumpfade/pumpfade_live_{date_tag}.log", msg)

    ctx = EngineContext(exchange=exchange, state=state, now_ts=now_ts, logger=_pumpfade_log, config=pumpfade_cfg)

    for symbol in pumpfade_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        pf = bucket.get(symbol, {})
        pending_id = pf.get("pending_order_id")
        pending_deadline = float(pf.get("pending_deadline_ts") or 0.0)
        pending_prior_hh = pf.get("pending_prior_hh")
        in_pos = bool(st.get("in_pos"))

        try:
            existing_amt = get_short_position_amount(symbol)
        except Exception:
            existing_amt = 0.0

        if pending_id:
            if existing_amt > 0:
                st["in_pos"] = True
                _set_last_entry_state(st, "SHORT", time.time())
                state[symbol] = st
                detail = get_short_position_detail(symbol)
                _log_trade_entry(
                    state,
                    side="SHORT",
                    symbol=symbol,
                    entry_ts=time.time(),
                    entry_price=detail.get("entry") if isinstance(detail.get("entry"), (int, float)) else None,
                    qty=detail.get("qty") if isinstance(detail.get("qty"), (int, float)) else None,
                    usdt=_resolve_entry_usdt(),
                    meta={"reason": "pumpfade_short", "pending_order_id": pending_id},
                )
                pf["pending_order_id"] = None
                pf["pending_deadline_ts"] = None
                bucket[symbol] = pf
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if pending_deadline and now_ts >= pending_deadline:
                cancel_open_orders(symbol)
                pf["pending_order_id"] = None
                pf["pending_deadline_ts"] = None
                pf["last_attempt_ts"] = time.time()
                bucket[symbol] = pf
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if isinstance(pending_prior_hh, (int, float)):
                df15 = cycle_cache.get_df(symbol, pumpfade_cfg.tf_trigger, limit=3)
                if not df15.empty and len(df15) >= 2:
                    try:
                        last_high = float(df15["high"].iloc[-1])
                        if last_high >= float(pending_prior_hh) * (1 + pumpfade_cfg.failure_eps):
                            cancel_open_orders(symbol)
                            pf["pending_order_id"] = None
                            pf["pending_deadline_ts"] = None
                            pf["last_attempt_ts"] = time.time()
                            bucket[symbol] = pf
                            time.sleep(PER_SYMBOL_SLEEP)
                            continue
                    except Exception:
                        pass

        if in_pos and existing_amt <= 0:
            st["in_pos"] = False
            state[symbol] = st
            pf["cooldown_until_ts"] = now_ts + tf_sec * int(pumpfade_cfg.cooldown_bars)
            bucket[symbol] = pf

        if existing_amt > 0:
            st["in_pos"] = True
            state[symbol] = st
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        cooldown_until = float(pf.get("cooldown_until_ts") or 0.0)
        if cooldown_until and now_ts < cooldown_until:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if pending_id:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        sig = pumpfade_engine.on_tick(ctx, symbol)
        if not sig:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        meta = sig.meta or {}
        hh_n = meta.get("hh_n")
        atr15 = meta.get("atr15")
        last_hh = pf.get("last_retest_hh")
        last_prior_hh = pf.get("last_prior_hh")
        if isinstance(hh_n, (int, float)) and isinstance(last_hh, (int, float)) and isinstance(atr15, (int, float)):
            if abs(float(hh_n) - float(last_hh)) <= float(atr15) * 0.2:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        cur_total = count_open_positions(force=True)
        if not isinstance(cur_total, int):
            cur_total = 0
        if cur_total >= MAX_OPEN_POSITIONS:
            _append_entry_gate_log(
                "pumpfade",
                symbol,
                f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side=SHORT",
                side="SHORT",
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if _exit_cooldown_blocked(state, symbol, "pumpfade", "SHORT"):
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_price = float(sig.entry_price or 0.0)
        if entry_price <= 0:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        order_info = "(알림 전용)"
        entry_usdt = _resolve_entry_usdt()
        try:
            date_tag = time.strftime("%Y%m%d")
            _append_entry_log(
                f"pumpfade/pumpfade_entries_{date_tag}.log",
                "engine=pumpfade side=SHORT symbol=%s state=ENTRY_READY hh=%s prior_hh=%s retest=Y "
                "failure_high=%s failure_low=%s confirm_close=%s confirm=%s confirm_sub=%s aggr=%s vol_ok=%s vol_rel=%.2f "
                "rsi=%s rsi_turn=%s macd_inc=%s entry=%.6g reasons=%s"
                % (
                    symbol,
                    f"{meta.get('hh_n'):.6g}" if isinstance(meta.get("hh_n"), (int, float)) else "N/A",
                    f"{meta.get('prior_hh'):.6g}" if isinstance(meta.get("prior_hh"), (int, float)) else "N/A",
                    f"{meta.get('failure_high'):.6g}" if isinstance(meta.get("failure_high"), (int, float)) else "N/A",
                    f"{meta.get('failure_low'):.6g}" if isinstance(meta.get("failure_low"), (int, float)) else "N/A",
                    f"{meta.get('confirm_close'):.6g}" if isinstance(meta.get("confirm_close"), (int, float)) else "N/A",
                    f"{meta.get('confirm_type') or 'N/A'}",
                    f"{meta.get('confirm_subtype') or meta.get('confirm_type') or 'N/A'}",
                    "1" if meta.get("aggressive_mode") else "0",
                    "Y" if meta.get("vol_ok") else "N",
                    (
                        float(meta.get("vol_failure") or 0.0) / float(meta.get("vol_peak") or 1.0)
                        if isinstance(meta.get("vol_failure"), (int, float)) and isinstance(meta.get("vol_peak"), (int, float)) and meta.get("vol_peak")
                        else 0.0
                    ),
                    f"{meta.get('rsi'):.2f}" if isinstance(meta.get("rsi"), (int, float)) else "N/A",
                    "Y" if meta.get("rsi_turn") else "N",
                    "Y" if meta.get("macd_hist_increasing") else "N",
                    entry_price,
                    ",".join(meta.get("reasons") or []),
                ),
            )
        except Exception:
            pass
        if LIVE_TRADING:
            lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="pumpfade", side="SHORT")
            if not lock_ok:
                print(f"[ENTRY-LOCK] sym={symbol} owner=pumpfade ok=0 held_by={lock_owner} age_s={lock_age:.1f}")
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            try:
                guard_key = _entry_guard_key(state, symbol, "SHORT")
                if not _entry_guard_acquire(state, symbol, key=guard_key, engine="pumpfade", side="SHORT"):
                    print(f"[pumpfade] 숏 중복 차단 ({symbol})")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                if _entry_seen_blocked(state, symbol, "SHORT", "pumpfade"):
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                try:
                    res = short_market(
                        symbol,
                        usdt_amount=entry_usdt,
                        leverage=LEVERAGE,
                        margin_mode=MARGIN_MODE,
                    )
                    order_id = _order_id_from_res(res)
                    status = res.get("status")
                    order_info = f"market usdt={entry_usdt} status={status}"
                    if status in ("ok",) and order_id:
                        _entry_seen_mark(state, symbol, "SHORT", "pumpfade")
                        pf["pending_order_id"] = None
                        pf["pending_deadline_ts"] = None
                        pf["last_retest_hh"] = hh_n if isinstance(hh_n, (int, float)) else last_hh
                        prior_hh = meta.get("prior_hh")
                        if isinstance(prior_hh, (int, float)):
                            pf["pending_prior_hh"] = prior_hh
                            pf["last_prior_hh"] = prior_hh
                        bucket[symbol] = pf
                    elif status == "dry_run":
                        st["in_pos"] = True
                        _set_last_entry_state(st, "SHORT", time.time())
                        state[symbol] = st
                        _entry_seen_mark(state, symbol, "SHORT", "pumpfade")
                        _log_trade_entry(
                            state,
                            side="SHORT",
                            symbol=symbol,
                            entry_ts=time.time(),
                            entry_price=entry_price,
                            qty=None,
                            usdt=entry_usdt,
                            meta={"reason": "pumpfade_short", "dry_run": True},
                        )
                finally:
                    _entry_guard_release(state, symbol, key=guard_key)
            finally:
                _entry_lock_release(state, symbol, owner="pumpfade")
        reason = ",".join(meta.get("reasons") or [])
        _send_entry_alert(
            send_alert,
            side="SHORT",
            symbol=symbol,
            engine="PUMPFADE",
            entry_price=entry_price,
            usdt=entry_usdt,
            reason=reason or "ENTRY_READY",
            live=LIVE_TRADING,
            order_info=order_info,
            sl=_fmt_price_safe(meta.get("failure_high"), AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
            tp=None,
        )
        result["hits"] += 1
        time.sleep(PER_SYMBOL_SLEEP)
    return result

def _run_atlas_rs_fail_short_cycle(
    arsf_engine,
    arsf_universe,
    state: Dict[str, dict],
    send_alert,
    arsf_cfg,
):
    result = {"hits": 0}
    if not arsf_engine or not arsf_cfg:
        return result
    if not arsf_universe:
        return result
    now_ts = time.time()
    date_tag = time.strftime("%Y-%m-%d")
    _ensure_log_file(f"atlas_rs_fail_short/atlas_rs_fail_short-{date_tag}.log")
    gate = _compute_atlas_swaggy_gate(state)
    if isinstance(gate, dict):
        _append_atlas_rs_fail_short_log(
            "[atlas-rs-fail-short] gate regime=%s ref=%s allow_long=%s allow_short=%s score_cfg=%.2f"
            % (
                gate.get("regime") or "n/a",
                getattr(atlas_swaggy_cfg, "ref_symbol", "n/a"),
                gate.get("allow_long"),
                gate.get("allow_short"),
                getattr(arsf_cfg, "min_score", 0.0) or 0.0,
            )
        )
    ctx = EngineContext(
        exchange=exchange,
        state=state,
        now_ts=now_ts,
        logger=_append_atlas_rs_fail_short_log,
        config=arsf_cfg,
    )
    def _arsf_skip(symbol: str, reason: str, entry_price: Optional[float] = None) -> None:
        _append_atlas_rs_fail_short_log(
            f"[atlas-rs-fail-short] skip sym={symbol} reason={reason} entry={_fmt_float(entry_price)}"
        )
    for symbol in arsf_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if st.get("in_pos"):
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        sig = arsf_engine.on_tick(ctx, symbol)
        if not sig:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        result["hits"] += 1
        entry_price = sig.entry_price
        meta = sig.meta if isinstance(sig.meta, dict) else {}
        size_mult = meta.get("size_mult", 1.0)
        print(f"[arsf] entry_ready sym={symbol} price={_fmt_float(entry_price)} size_mult={_fmt_float(size_mult)}")
        try:
            date_tag = time.strftime("%Y%m%d")
            atlas = meta.get("atlas") if isinstance(meta.get("atlas"), dict) else {}
            tech = meta.get("tech") if isinstance(meta.get("tech"), dict) else {}
            _append_entry_log(
                f"atlas_rs_fail_short/atlas_rs_fail_short_entries_{date_tag}.log",
                "engine=atlas_rs_fail_short side=SHORT symbol=%s entry=%.6g size_mult=%.3f "
                "confirm=%s atlas_regime=%s atlas_dir=%s score=%s rs_z=%s rs_z_slow=%s wick_ratio=%s"
                % (
                    symbol,
                    float(entry_price or 0.0),
                    float(size_mult or 1.0),
                    tech.get("confirm_type") or "N/A",
                    atlas.get("regime") or "N/A",
                    atlas.get("dir") or "N/A",
                    atlas.get("score") if atlas.get("score") is not None else "N/A",
                    atlas.get("rs_z") if atlas.get("rs_z") is not None else "N/A",
                    atlas.get("rs_z_slow") if atlas.get("rs_z_slow") is not None else "N/A",
                    tech.get("wick_ratio") if tech.get("wick_ratio") is not None else "N/A",
                ),
            )
        except Exception:
            pass
        if not LIVE_TRADING:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        bucket = state.get("_atlas_rs_fail_short") if isinstance(state, dict) else None
        cur_total = count_open_positions(force=True)
        if isinstance(cur_total, int) and cur_total >= MAX_OPEN_POSITIONS:
            _append_entry_gate_log(
                "atlas_rs_fail_short",
                symbol,
                f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side=SHORT",
                side="SHORT",
            )
            _arsf_skip(symbol, "MAX_POS", entry_price)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if _exit_cooldown_blocked(state, symbol, "atlas_rs_fail_short", "SHORT"):
            _arsf_skip(symbol, "EXIT_COOLDOWN", entry_price)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        snapshot = state.get("_atlas_rs_fail_short_snapshot") if isinstance(state.get("_atlas_rs_fail_short_snapshot"), dict) else {}
        snap = snapshot.get(symbol) if isinstance(snapshot, dict) else None
        if not isinstance(snap, dict):
            _arsf_skip(symbol, "ATLAS_SNAPSHOT_MISSING", entry_price)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if snap.get("dir") != "BEAR":
            _arsf_skip(symbol, "ATLAS_DIR_FLIP", entry_price)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        ticker = state.get("_tickers", {}).get(symbol) if isinstance(state.get("_tickers"), dict) else None
        cur_px = None
        if isinstance(ticker, dict):
            cur_px = ticker.get("last") or ticker.get("close")
        try:
            cur_px = float(cur_px) if cur_px is not None else None
        except Exception:
            cur_px = None
        tech = meta.get("tech") if isinstance(meta.get("tech"), dict) else {}
        atr = tech.get("atr")
        try:
            atr = float(atr) if atr is not None else None
        except Exception:
            atr = None
        if cur_px is not None and entry_price is not None and atr is not None:
            if abs(cur_px - float(entry_price)) > atr * 0.2:
                _arsf_skip(symbol, "CHASE_SKIP", entry_price)
                time.sleep(PER_SYMBOL_SLEEP)
                continue
        lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="atlas_rs_fail_short", side="SHORT")
        if not lock_ok:
            print(f"[ENTRY-LOCK] sym={symbol} owner=atlas_rs_fail_short ok=0 held_by={lock_owner} age_s={lock_age:.1f}")
            _arsf_skip(symbol, "ENTRY_LOCK", entry_price)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        try:
            guard_key = _entry_guard_key(state, symbol, "SHORT")
            if not _entry_guard_acquire(state, symbol, key=guard_key, engine="atlas_rs_fail_short", side="SHORT"):
                _arsf_skip(symbol, "ENTRY_GUARD", entry_price)
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            try:
                entry_usdt = _resolve_entry_usdt(USDT_PER_TRADE * float(size_mult or 1.0))
                res = short_market(
                    symbol,
                    usdt_amount=entry_usdt,
                    leverage=LEVERAGE,
                    margin_mode=MARGIN_MODE,
                )
                entry_order_id = _order_id_from_res(res)
                fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                qty = res.get("amount") or res.get("order", {}).get("amount")
                st["in_pos"] = True
                _set_last_entry_state(st, "SHORT", time.time())
                state[symbol] = st
                if isinstance(bucket, dict):
                    rec = bucket.get(symbol)
                    if isinstance(rec, dict):
                        now_entry_ts = time.time()
                        rec["last_entry_ts"] = now_entry_ts
                        bucket[symbol] = rec
                _log_trade_entry(
                    state,
                    side="SHORT",
                    symbol=symbol,
                    entry_ts=time.time(),
                    entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                    qty=qty if isinstance(qty, (int, float)) else None,
                    usdt=entry_usdt,
                    entry_order_id=entry_order_id,
                    meta={"reason": "atlas_rs_fail_short"},
                )
                _send_entry_alert(
                    send_alert,
                    side="SHORT",
                    symbol=symbol,
                    engine="ATLAS_RS_FAIL_SHORT",
                    entry_price=fill_price if fill_price is not None else entry_price,
                    usdt=entry_usdt,
                    reason="PULLBACK_FAIL",
                    live=LIVE_TRADING,
                    entry_order_id=entry_order_id,
                    sl=_fmt_price_safe(fill_price or entry_price, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                )
            except Exception as e:
                print(f"[atlas-rs-fail-short] order error {symbol}: {e}")
            finally:
                _entry_guard_release(state, symbol, key=guard_key)
        finally:
            _entry_lock_release(state, symbol, owner="atlas_rs_fail_short")
        time.sleep(PER_SYMBOL_SLEEP)
    return result


def _calc_realized_pnl_from_trades(ex, symbol: str, since_ms: int) -> Optional[float]:
    try:
        trades = ex.fetch_my_trades(symbol, since=since_ms)
    except Exception:
        return None
    pnl = 0.0
    found = False
    for t in trades or []:
        info = t.get("info") or {}
        rp = info.get("realizedPnl")
        if rp is None:
            continue
        try:
            pnl += float(rp)
            found = True
        except Exception:
            continue
    return pnl if found else None

def _trade_order_id(trade: dict) -> Optional[str]:
    if not isinstance(trade, dict):
        return None
    oid = trade.get("order") or trade.get("orderId")
    if oid:
        return str(oid)
    info = trade.get("info") or {}
    oid = info.get("orderId") or info.get("orderID") or info.get("order_id")
    if oid:
        return str(oid)
    return None

def _trade_price_amount(trade: dict) -> tuple[Optional[float], Optional[float]]:
    info = trade.get("info") or {}
    price = trade.get("price") or info.get("price")
    amount = trade.get("amount") or info.get("qty") or info.get("executedQty")
    try:
        price = float(price)
    except Exception:
        price = None
    try:
        amount = float(amount)
    except Exception:
        amount = None
    return price, amount

def _trade_realized_pnl(trade: dict) -> Optional[float]:
    info = trade.get("info") or {}
    rp = info.get("realizedPnl")
    if rp is None:
        rp = info.get("realizedProfit")
    try:
        return float(rp)
    except Exception:
        return None

def _fetch_fapi_user_trades_range(ex, symbol: str, start_ms: int, end_ms: int, limit: int = 1000) -> list:
    if not symbol:
        return []
    try:
        market = ex.market(symbol)
        symbol_id = market.get("id") if isinstance(market, dict) else None
    except Exception:
        symbol_id = None
    if not symbol_id:
        return []
    since = start_ms
    out = []
    last_ts = None
    while True:
        params = {"symbol": symbol_id, "startTime": since, "endTime": end_ms, "limit": limit}
        try:
            if hasattr(ex, "fapiPrivateGetUserTrades"):
                batch = ex.fapiPrivateGetUserTrades(params)
            else:
                batch = ex.fapiPrivate_get_user_trades(params)
        except Exception as e:
            print(f"[report-api] fapi userTrades failed sym={symbol} err={e}")
            break
        if not batch:
            break
        progressed = False
        for raw in batch:
            ts = raw.get("time") or raw.get("timestamp")
            if isinstance(ts, str):
                try:
                    ts = int(float(ts))
                except Exception:
                    ts = None
            if not isinstance(ts, (int, float)):
                continue
            ts = int(ts)
            if ts < start_ms or ts >= end_ms:
                continue
            out.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "side": raw.get("side"),
                    "price": raw.get("price"),
                    "amount": raw.get("qty"),
                    "order": raw.get("orderId") or raw.get("orderID"),
                    "info": raw,
                }
            )
            if last_ts is None or ts > last_ts:
                last_ts = ts
                progressed = True
        if not progressed:
            break
        since = (last_ts or since) + 1
        if last_ts is not None and last_ts >= end_ms:
            break
    return out

def _fetch_my_trades_range(ex, symbol: Optional[str], start_ms: int, end_ms: int, limit: int = 500) -> list:
    if symbol and (hasattr(ex, "fapiPrivateGetUserTrades") or hasattr(ex, "fapiPrivate_get_user_trades")):
        return _fetch_fapi_user_trades_range(ex, symbol, start_ms, end_ms, limit=1000)
    since = start_ms
    out = []
    last_ts = None
    while True:
        try:
            if symbol:
                batch = ex.fetch_my_trades(symbol, since=since, limit=limit)
            else:
                batch = ex.fetch_my_trades(since=since, limit=limit)
        except Exception as e:
            sym_tag = symbol if symbol else "*"
            print(f"[report-api] fetch_my_trades failed sym={sym_tag} err={e}")
            break
        if not batch:
            break
        progressed = False
        for t in batch:
            ts = t.get("timestamp")
            if not isinstance(ts, (int, float)):
                continue
            ts = int(ts)
            if ts < start_ms:
                continue
            if ts >= end_ms:
                continue
            out.append(t)
            if last_ts is None or ts > last_ts:
                last_ts = ts
                progressed = True
        if not progressed:
            break
        since = (last_ts or since) + 1
        if last_ts is not None and last_ts >= end_ms:
            break
    return out

def _read_report_csv_records(report_date: str) -> Optional[list]:
    report_path = os.path.join("reports", f"{report_date}.csv")
    if not os.path.exists(report_path):
        return None
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            rows = [line.rstrip("\n") for line in f.readlines()]
        if not rows:
            return None
        header = rows[0].split(",")
        records = []
        for row in rows[1:]:
            if not row:
                continue
            parts = row.split(",")
            if len(parts) < len(header):
                parts += [""] * (len(header) - len(parts))
            records.append({header[i]: parts[i] for i in range(len(header))})
        return records
    except Exception:
        return None

def _load_entry_events_map(report_date: Optional[str] = None) -> tuple[dict, dict]:
    by_id = {}
    by_symbol_side = {}
    paths: list[str] = []
    entry_dir = os.path.join("logs", "entry")
    if os.path.isdir(entry_dir):
        try:
            for name in sorted(os.listdir(entry_dir)):
                if name.startswith("entry_events-") and name.endswith(".log"):
                    paths.append(os.path.join(entry_dir, name))
        except Exception:
            pass
    legacy_path = os.path.join("logs", "entry_events.log")
    if os.path.exists(legacy_path):
        paths.append(legacy_path)
    if report_date:
        dated_path = os.path.join("logs", "entry", f"entry_events-{report_date}.log")
        if os.path.exists(dated_path) and dated_path not in paths:
            paths.append(dated_path)
    if not paths:
        return by_id, by_symbol_side
    try:
        for path in paths:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    entry_ts = payload.get("entry_ts")
                    entry_ts_val = None
                    if isinstance(entry_ts, (int, float)):
                        entry_ts_val = float(entry_ts)
                    elif isinstance(entry_ts, str):
                        try:
                            entry_ts_val = datetime.strptime(entry_ts, "%Y-%m-%d %H:%M:%S").timestamp()
                        except Exception:
                            entry_ts_val = None
                    if entry_ts_val is None:
                        continue
                    entry_id = payload.get("entry_order_id")
                    symbol = payload.get("symbol") or ""
                    side = (payload.get("side") or "").upper()
                    engine = payload.get("engine") or "unknown"
                    record = {
                        "entry_ts": entry_ts_val,
                        "entry_order_id": entry_id,
                        "symbol": symbol,
                        "side": side,
                        "engine": engine,
                    }
                    if entry_id:
                        by_id[str(entry_id)] = record
                    if report_date is None or _report_day_str(entry_ts_val) == report_date:
                        key = (symbol, side)
                        by_symbol_side.setdefault(key, []).append(record)
    except Exception:
        return by_id, by_symbol_side
    return by_id, by_symbol_side

def _sync_report_with_api(state: Dict[str, dict], report_date: str) -> bool:
    if executor_mod is None or not hasattr(executor_mod, "exchange"):
        return False
    ex = executor_mod.exchange
    try:
        executor_mod.ensure_ready()
    except Exception as e:
        print(f"[report-api] ensure_ready failed: {e}")
        return False
    try:
        kst = timezone(timedelta(hours=9))
        day = datetime.strptime(report_date, "%Y-%m-%d").replace(tzinfo=kst)
        start_ms = int(day.astimezone(timezone.utc).timestamp() * 1000)
        end_ms = int((day + timedelta(days=1)).astimezone(timezone.utc).timestamp() * 1000)
    except Exception as e:
        print(f"[report-api] bad report_date={report_date} err={e}")
        return False

    log = _get_trade_log(state)
    exit_map = {}
    for tr in log:
        if tr.get("status") != "closed":
            continue
        exit_ts = tr.get("exit_ts")
        if not isinstance(exit_ts, (int, float)):
            continue
        if _report_day_str(exit_ts) != report_date:
            continue
        exit_id = tr.get("exit_order_id")
        symbol = tr.get("symbol")
        if exit_id and symbol:
            exit_map[str(exit_id)] = tr

    entry_map, entry_by_symbol = _load_entry_events_map(report_date)
    order_map = {}
    symbols = {tr.get("symbol") for tr in exit_map.values() if tr.get("symbol")}
    for (sym, _side) in entry_by_symbol.keys():
        if sym:
            symbols.add(sym)
    symbols = sorted(symbols)
    if not symbols:
        try:
            os.makedirs("reports", exist_ok=True)
            report_path = os.path.join("reports", f"{report_date}.csv")
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(
                    "entry_ts,entry_order_id,symbol,side,engine,entry_price,qty,exit_ts,exit_order_id,exit_price,roi,pnl,exit_reason,win,loss\n"
                )
            return True
        except Exception as e:
            print(f"[report-api] write empty failed report_date={report_date} err={e}")
            return False
    for sym in symbols:
        trades = _fetch_my_trades_range(ex, sym, start_ms, end_ms)
        for t in trades:
            oid = _trade_order_id(t)
            if not oid:
                continue
            order_map.setdefault(oid, []).append(t)

    expected_cols = [
        "entry_ts",
        "entry_order_id",
        "symbol",
        "side",
        "engine",
        "entry_price",
        "qty",
        "exit_ts",
        "exit_order_id",
        "exit_price",
        "roi",
        "pnl",
        "exit_reason",
        "win",
        "loss",
    ]
    records = []
    for oid, trades in order_map.items():
        oid_str = str(oid)
        pnl_sum = 0.0
        pnl_found = False
        total_qty = 0.0
        total_notional = 0.0
        latest_ts = None
        side = ""
        symbol = ""
        for t in trades or []:
            info = t.get("info") or {}
            if not symbol:
                symbol = t.get("symbol") or info.get("symbol") or ""
            if not side:
                pos_side = info.get("positionSide")
                if pos_side:
                    side = str(pos_side).upper()
                else:
                    raw_side = t.get("side") or info.get("side")
                    if raw_side:
                        raw = str(raw_side).upper()
                        side = "LONG" if raw == "BUY" else "SHORT" if raw == "SELL" else raw
            price, amount = _trade_price_amount(t)
            if price is not None and amount is not None:
                q = abs(amount)
                total_qty += q
                total_notional += price * q
            rp = _trade_realized_pnl(t)
            if rp is not None:
                pnl_sum += rp
                pnl_found = True
            ts = t.get("timestamp")
            if isinstance(ts, (int, float)):
                ts = int(ts)
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts
        if not pnl_found or abs(pnl_sum) < 1e-12:
            continue
        exit_price = (total_notional / total_qty) if total_qty > 0 else None
        exit_ts_str = ""
        exit_ts_val = None
        if latest_ts is not None:
            exit_ts_val = latest_ts / 1000.0
            exit_ts_str = datetime.fromtimestamp(exit_ts_val).strftime("%Y-%m-%d %H:%M:%S")
        pnl_val = pnl_sum if pnl_found else None
        entry_ts_str = ""
        entry_order_id = ""
        entry_price = ""
        entry_qty = total_qty if total_qty > 0 else ""
        engine = "unknown"
        exit_reason = "api_unmatched"
        tr = exit_map.get(oid_str)
        if tr:
            entry_order_id = str(tr.get("entry_order_id") or "")
            exit_reason = tr.get("exit_reason") or exit_reason
        if entry_order_id:
            entry_event = entry_map.get(entry_order_id)
            if entry_event:
                engine = entry_event.get("engine") or engine
        if (engine == "unknown" or not entry_order_id) and symbol and side:
            candidates = entry_by_symbol.get((symbol, side)) or []
            chosen = None
            if candidates:
                if exit_ts_val:
                    eligible = [c for c in candidates if c.get("entry_ts") and c["entry_ts"] <= exit_ts_val]
                    if eligible:
                        chosen = max(eligible, key=lambda c: c.get("entry_ts") or 0)
                if chosen is None:
                    chosen = max(candidates, key=lambda c: c.get("entry_ts") or 0)
            if chosen:
                if not entry_order_id:
                    entry_order_id = str(chosen.get("entry_order_id") or "")
                if engine == "unknown":
                    engine = chosen.get("engine") or engine
                if not entry_ts_str:
                    try:
                        entry_ts_str = datetime.fromtimestamp(float(chosen.get("entry_ts"))).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
        if entry_order_id and entry_order_id in order_map:
            e_trades = order_map.get(entry_order_id) or []
            total_entry_qty = 0.0
            total_entry_notional = 0.0
            entry_latest_ts = None
            for t in e_trades:
                price, amount = _trade_price_amount(t)
                if price is None or amount is None:
                    continue
                q = abs(amount)
                total_entry_qty += q
                total_entry_notional += price * q
                ts = t.get("timestamp")
                if isinstance(ts, (int, float)):
                    ts = int(ts)
                    if entry_latest_ts is None or ts > entry_latest_ts:
                        entry_latest_ts = ts
            if total_entry_qty > 0:
                entry_price = total_entry_notional / total_entry_qty
                entry_qty = total_entry_qty
            if entry_latest_ts is not None:
                entry_ts_str = datetime.fromtimestamp(entry_latest_ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

        roi_val = None
        if isinstance(pnl_val, (int, float)) and isinstance(entry_price, (int, float)) and isinstance(entry_qty, (int, float)) and entry_price > 0:
            entry_notional = entry_price * entry_qty
            if entry_notional > 0:
                roi_val = (float(pnl_val) / entry_notional) * 100.0
        win = ""
        loss = ""
        if isinstance(pnl_val, (int, float)) or isinstance(roi_val, (int, float)):
            base = pnl_val if isinstance(pnl_val, (int, float)) else roi_val
            if base is not None:
                if base > 0:
                    win, loss = "1", "0"
                elif base < 0:
                    win, loss = "0", "1"
                else:
                    win, loss = "0", "0"
        records.append(
            {
                "entry_ts": entry_ts_str,
                "entry_order_id": entry_order_id,
                "symbol": symbol,
                "side": side,
                "engine": engine,
                "entry_price": entry_price if entry_price != "" else "",
                "qty": entry_qty if entry_qty != "" else "",
                "exit_ts": exit_ts_str,
                "exit_order_id": oid_str,
                "exit_price": exit_price if exit_price is not None else "",
                "roi": roi_val if roi_val is not None else "",
                "pnl": pnl_val if pnl_val is not None else "",
                "exit_reason": exit_reason,
                "win": win,
                "loss": loss,
            }
        )

    try:
        os.makedirs("reports", exist_ok=True)
        report_path = os.path.join("reports", f"{report_date}.csv")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(",".join(expected_cols) + "\n")
            for rec in records:
                line = ",".join(str(rec.get(col, "")) for col in expected_cols)
                f.write(line + "\n")
        return True
    except Exception as e:
        print(f"[report-api] write failed report_date={report_date} err={e}")
        return False

def _place_short_sl_order(
    symbol: str,
    entry_price: float,
    sl_pct: float,
    qty: Optional[float] = None,
) -> Optional[dict]:
    if not LIVE_TRADING:
        return {"status": "skip", "reason": "live_off"}
    if not isinstance(entry_price, (int, float)) or entry_price <= 0:
        return {"status": "skip", "reason": "bad_entry_price"}
    sl_price = entry_price * (1 + (sl_pct / 100.0))
    try:
        amt = None
        if isinstance(qty, (int, float)) and qty > 0:
            amt = qty
        if not isinstance(amt, (int, float)) or amt <= 0:
            # position may not be refreshed immediately after entry
            refresh_positions_cache(force=True)
            for _ in range(8):
                amt = get_short_position_amount(symbol)
                if isinstance(amt, (int, float)) and amt > 0:
                    break
                time.sleep(0.25)
        if not isinstance(amt, (int, float)) or amt <= 0:
            print(f"[short-sl] {symbol} skipped: qty unavailable entry={entry_price} sl={sl_price}")
            return {"status": "skip", "reason": "qty_unavailable"}
        amt = float(exchange.amount_to_precision(symbol, amt))
        if amt <= 0:
            print(f"[short-sl] {symbol} skipped: qty precision=0 entry={entry_price} raw_qty={qty}")
            return {"status": "skip", "reason": "qty_precision_zero"}
        params = {"stopPrice": float(exchange.price_to_precision(symbol, sl_price)), "workingType": "MARK_PRICE"}
        try:
            if is_hedge_mode():
                params["positionSide"] = "SHORT"
            order = exchange.create_order(symbol, "STOP_MARKET", "buy", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[short-sl] {symbol} primary failed: {e}")
        params["closePosition"] = True
        try:
            order = exchange.create_order(symbol, "STOP_MARKET", "buy", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[short-sl] {symbol} retry closePosition failed: {e}")
        params.pop("closePosition", None)
        params["reduceOnly"] = True
        try:
            order = exchange.create_order(symbol, "STOP_MARKET", "buy", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[short-sl] {symbol} retry reduceOnly failed: {e}")
            return {"status": "skip", "reason": "order_failed"}
    except Exception as e:
        print(f"[short-sl] {symbol} failed: {e}")
        return {"status": "skip", "reason": "exception"}

def _place_long_sl_order(
    symbol: str,
    entry_price: float,
    sl_pct: float,
    qty: Optional[float] = None,
) -> Optional[dict]:
    if not LIVE_TRADING:
        return {"status": "skip", "reason": "live_off"}
    if not isinstance(entry_price, (int, float)) or entry_price <= 0:
        return {"status": "skip", "reason": "bad_entry_price"}
    sl_price = entry_price * (1 - (sl_pct / 100.0))
    try:
        amt = None
        if isinstance(qty, (int, float)) and qty > 0:
            amt = qty
        if not isinstance(amt, (int, float)) or amt <= 0:
            refresh_positions_cache(force=True)
            for _ in range(8):
                amt = get_long_position_amount(symbol)
                if isinstance(amt, (int, float)) and amt > 0:
                    break
                time.sleep(0.25)
        if not isinstance(amt, (int, float)) or amt <= 0:
            print(f"[long-sl] {symbol} skipped: qty unavailable entry={entry_price} sl={sl_price}")
            return {"status": "skip", "reason": "qty_unavailable"}
        amt = float(exchange.amount_to_precision(symbol, amt))
        if amt <= 0:
            print(f"[long-sl] {symbol} skipped: qty precision=0 entry={entry_price} raw_qty={qty}")
            return {"status": "skip", "reason": "qty_precision_zero"}
        params = {"stopPrice": float(exchange.price_to_precision(symbol, sl_price)), "workingType": "MARK_PRICE"}
        try:
            if is_hedge_mode():
                params["positionSide"] = "LONG"
            order = exchange.create_order(symbol, "STOP_MARKET", "sell", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[long-sl] {symbol} primary failed: {e}")
        params["closePosition"] = True
        try:
            order = exchange.create_order(symbol, "STOP_MARKET", "sell", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[long-sl] {symbol} retry closePosition failed: {e}")
        params.pop("closePosition", None)
        params["reduceOnly"] = True
        try:
            order = exchange.create_order(symbol, "STOP_MARKET", "sell", amt, None, params)
            return {"status": "ok", "order": order}
        except Exception as e:
            print(f"[long-sl] {symbol} retry reduceOnly failed: {e}")
            return {"status": "skip", "reason": "order_failed"}
    except Exception as e:
        print(f"[long-sl] {symbol} failed: {e}")
        return {"status": "skip", "reason": "exception"}

def _reconcile_long_trades(state: Dict[str, dict], ex, tickers: dict) -> None:
    log = _get_trade_log(state)
    open_longs = [tr for tr in log if tr.get("status") == "open" and tr.get("side") == "LONG"]
    if not open_longs:
        return
    try:
        refresh_positions_cache(force=True)
    except Exception:
        return
    for tr in open_longs:
        symbol = tr.get("symbol")
        if not symbol:
            continue
        try:
            still_open = get_long_position_amount(symbol) > 0
        except Exception:
            continue
        if still_open:
            continue
        print(f"[long-exit] {symbol} position closed detected, canceling open orders")
        meta = tr.get("meta") or {}
        try:
            cancel_conditional_by_side(symbol, "LONG")
        except Exception as e:
            print(f"[long-exit] {symbol} cancel_conditional_by_side failed: {e}")
        try:
            cancel_stop_orders(symbol)
        except Exception as e:
            print(f"[long-exit] {symbol} cancel_stop_orders failed: {e}")
        exit_ts = time.time()
        entry_ts_ms = tr.get("entry_ts_ms")
        realized_pnl = None
        if isinstance(entry_ts_ms, int) and entry_ts_ms > 0:
            realized_pnl = _calc_realized_pnl_from_trades(ex, symbol, entry_ts_ms)
        t = tickers.get(symbol) if isinstance(tickers, dict) else None
        exit_price = None
        if isinstance(t, dict):
            exit_price = t.get("last") or t.get("close") or t.get("bid")
        entry_price = tr.get("entry_price")
        qty = tr.get("qty")
        pnl = realized_pnl
        if pnl is None:
            pnl = None
        if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and isinstance(exit_price, (int, float)):
            pnl = (exit_price - entry_price) * qty if pnl is None else pnl
        _close_trade(
            state,
            side="LONG",
            symbol=symbol,
            exit_ts=exit_ts,
            exit_price=exit_price,
            pnl_usdt=pnl,
            reason="position_closed_detected",
        )
        engine_label = _engine_label_from_reason((meta or {}).get("reason"))
        if meta.get("sl_order_id") and engine_label == "SWAGGY":
            st = state.get(symbol, {})
            if isinstance(st, dict):
                st["swaggy_last_sl_ts"] = exit_ts
                state[symbol] = st
        if not SUPPRESS_RECONCILE_ALERTS:
            _append_report_line(symbol, "LONG", None, pnl, engine_label)
        if (
            send_telegram
            and not SUPPRESS_RECONCILE_ALERTS
            and engine_label != "UNKNOWN"
            and _trade_has_entry(tr)
            and not _recent_auto_exit(state, symbol, exit_ts)
        ):
            pnl_str = f"{pnl:+.3f} USDT" if isinstance(pnl, (int, float)) else "N/A"
            price_str = f"{exit_price:.6g}" if isinstance(exit_price, (int, float)) else "N/A"
            order_block = _format_order_id_block(tr.get("entry_order_id"), tr.get("exit_order_id"))
            order_line = f"{order_block}\n" if order_block else ""
            send_telegram(
                f"{EXIT_ICON} <b>롱 청산</b>\n"
                f"<b>{symbol}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"{order_line}"
                f"청산가={price_str}\n"
                f"손익={pnl_str}"
            )


def _reconcile_short_trades(state: Dict[str, dict], tickers: dict) -> None:
    log = _get_trade_log(state)
    open_shorts = [tr for tr in log if tr.get("status") == "open" and tr.get("side") == "SHORT"]
    if not open_shorts:
        return
    open_longs = {tr.get("symbol") for tr in log if tr.get("status") == "open" and tr.get("side") == "LONG"}
    hedge_mode = False
    try:
        hedge_mode = is_hedge_mode()
    except Exception:
        hedge_mode = False
    try:
        refresh_positions_cache(force=True)
    except Exception:
        pass
    open_trade_index = {}
    for tr in open_shorts:
        sym = tr.get("symbol")
        if sym:
            open_trade_index[sym] = tr
    zero_counts = state.setdefault("short_zero_count", {})
    legacy_zero = state.get("_short_reconcile_zero_count")
    if isinstance(legacy_zero, dict) and not zero_counts:
        zero_counts.update(legacy_zero)
    for tr in open_shorts:
        symbol = tr.get("symbol")
        if not symbol:
            continue
        entry_ts = tr.get("entry_ts")
        now_ts = time.time()
        last_entry_ts = None
        st = state.get(symbol, {}) if isinstance(state, dict) else {}
        if isinstance(st, dict):
            last_entry_ts = st.get("last_entry")
        if isinstance(last_entry_ts, (int, float)):
            if (now_ts - float(last_entry_ts)) < SHORT_RECONCILE_GRACE_SEC:
                continue
        if isinstance(entry_ts, (int, float)):
            if (now_ts - float(entry_ts)) < SHORT_RECONCILE_GRACE_SEC:
                continue
        if (not hedge_mode) and (symbol in open_longs):
            print(f"SHORT_RECONCILE_SKIP_OPEN_LONG sym={symbol}")
            continue
        try:
            amt = get_short_position_amount(symbol)
        except Exception:
            continue
        try:
            long_amt = get_long_position_amount(symbol)
        except Exception:
            long_amt = 0.0
        if (not hedge_mode) and (long_amt > 0):
            zero_counts[symbol] = 0
            print(f"SHORT_RECONCILE_SKIP_LONG_PRESENT sym={symbol} long_amt={long_amt}")
            continue
        if abs(amt) < SHORT_RECONCILE_EPS:
            try:
                refresh_positions_cache(force=True)
            except Exception:
                pass
            try:
                amt = get_short_position_amount(symbol)
            except Exception:
                amt = 0.0
        if abs(amt) < SHORT_RECONCILE_EPS:
            zero_counts[symbol] = int(zero_counts.get(symbol, 0)) + 1
            last_entry_age = None
            if isinstance(last_entry_ts, (int, float)):
                last_entry_age = max(0.0, now_ts - float(last_entry_ts))
            grace_left = None
            if isinstance(last_entry_age, (int, float)):
                grace_left = max(0.0, SHORT_RECONCILE_GRACE_SEC - last_entry_age)
            if zero_counts[symbol] < SHORT_RECONCILE_ZERO_STREAK_N:
                print(
                    "SHORT_RECONCILE_SKIP_ZERO_STREAK "
                    f"sym={symbol} pos=0 zero_count={zero_counts[symbol]}/{SHORT_RECONCILE_ZERO_STREAK_N} "
                    f"grace_left={grace_left} last_entry_age={last_entry_age}"
                )
                st = state.get(symbol, {})
                st["in_pos"] = True
                state[symbol] = st
                continue
            print(
                "SHORT_RECONCILE_DO "
                f"sym={symbol} pos=0 zero_count={zero_counts[symbol]}/{SHORT_RECONCILE_ZERO_STREAK_N} "
                "reason=CONFIRMED_ZERO"
            )
        else:
            zero_counts[symbol] = 0
            st = state.get(symbol, {})
            if isinstance(st, dict):
                st["short_pos_seen_ts"] = now_ts
                state[symbol] = st
        if amt > SHORT_RECONCILE_EPS:
            st = state.get(symbol, {})
            st["in_pos"] = True
            state[symbol] = st
            continue
        age_sec = None
        if isinstance(entry_ts, (int, float)):
            age_sec = max(0.0, now_ts - float(entry_ts))
        last_entry_age = None
        if isinstance(last_entry_ts, (int, float)):
            last_entry_age = max(0.0, now_ts - float(last_entry_ts))
        age_str = f"{age_sec:.1f}" if isinstance(age_sec, (int, float)) else "N/A"
        last_order_status = "N/A"
        last_fill_ts = None
        open_tr = _get_open_trade(state, "SHORT", symbol)
        if isinstance(open_tr, dict):
            meta = open_tr.get("meta") or {}
            last_order_status = meta.get("order_status") or "N/A"
            last_fill_ts = meta.get("last_fill_ts") or open_tr.get("entry_ts_ms")
        print(
            "[SHORT-RECONCILE-CHECK] "
            f"symbol={symbol} since_entry_sec={age_str} pos_amt={float(amt):.6f} "
            f"last_order_status={last_order_status} last_fill_ts={last_fill_ts}"
        )
        try:
            cancel_conditional_by_side(symbol, "SHORT")
        except Exception as e:
            print(f"[short-exit] {symbol} cancel_conditional_by_side failed: {e}")
        cancel_stop_orders(symbol)
        exit_ts = time.time()
        t = tickers.get(symbol) if isinstance(tickers, dict) else None
        exit_price = None
        if isinstance(t, dict):
            exit_price = t.get("last") or t.get("close") or t.get("ask")
        pnl = None
        entry_price = tr.get("entry_price")
        qty = tr.get("qty")
        if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)) and isinstance(exit_price, (int, float)):
            pnl = (entry_price - exit_price) * qty
        exit_reason = "manual_close"
        if isinstance(open_tr, dict):
            meta = open_tr.get("meta") or {}
            if meta.get("sl_order_id"):
                exit_reason = "auto_exit_sl"
        _close_trade(
            state,
            side="SHORT",
            symbol=symbol,
            exit_ts=exit_ts,
            exit_price=exit_price,
            pnl_usdt=pnl,
            reason=exit_reason,
        )
        engine_label = _engine_label_from_reason((tr.get("meta") or {}).get("reason"))
        if exit_reason == "auto_exit_sl" and engine_label == "SWAGGY":
            st = state.get(symbol, {})
            if isinstance(st, dict):
                st["swaggy_last_sl_ts"] = exit_ts
                state[symbol] = st
        st = state.get(symbol, {})
        seen_ts = st.get("short_pos_seen_ts") if isinstance(st, dict) else None
        recent_seen = False
        if isinstance(seen_ts, (int, float)):
            recent_seen = (now_ts - float(seen_ts)) <= SHORT_RECONCILE_SEEN_TTL_SEC
        recent_fill = False
        if isinstance(last_fill_ts, (int, float)) and last_fill_ts > 0:
            ts_val = float(last_fill_ts)
            if ts_val > 10_000_000_000:
                ts_val = ts_val / 1000.0
            if (now_ts - ts_val) <= SHORT_RECONCILE_SEEN_TTL_SEC:
                recent_fill = True
        evidence_short = recent_seen or recent_fill
        if evidence_short and not SUPPRESS_RECONCILE_ALERTS:
            _append_report_line(symbol, "SHORT", None, pnl, engine_label)
        if (
            send_telegram
            and isinstance(open_trade_index.get(symbol), dict)
            and evidence_short
            and not SUPPRESS_RECONCILE_ALERTS
            and engine_label != "UNKNOWN"
            and _trade_has_entry(tr)
            and not _recent_auto_exit(state, symbol, exit_ts)
        ):
            pnl_str = f"{pnl:+.3f} USDT" if isinstance(pnl, (int, float)) else "N/A"
            price_str = f"{exit_price:.6g}" if isinstance(exit_price, (int, float)) else "N/A"
            exit_tag = "SL" if exit_reason == "auto_exit_sl" else "MANUAL"
            order_block = _format_order_id_block(tr.get("entry_order_id"), tr.get("exit_order_id"))
            order_line = f"{order_block}\n" if order_block else ""
            send_telegram(
                f"🔴 <b>숏 청산</b>\n"
                f"<b>{symbol}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{order_line}"
                f"청산가={price_str}\n"
                f"손익={pnl_str}"
            )

def _get_manage_tickers(state: dict, exchange, ttl_sec: float) -> dict:
    now = time.time()
    cached = state.get("_tickers") if isinstance(state, dict) else None
    cached_ts = 0.0
    try:
        cached_ts = float(state.get("_tickers_ts", 0.0) or 0.0)
    except Exception:
        cached_ts = 0.0
    if isinstance(cached, dict) and (now - cached_ts) <= ttl_sec:
        return cached
    try:
        tickers = exchange.fetch_tickers()
        if isinstance(state, dict):
            state["_tickers"] = tickers
            state["_tickers_ts"] = now
        return tickers
    except Exception as e:
        print("[manage] tickers fetch failed:", e)
        return cached if isinstance(cached, dict) else {}


def _run_manage_cycle(state: dict, exchange, cached_long_ex, send_telegram) -> None:
    now = time.time()
    if DB_RECONCILE_ENABLED and dbrecon and dbrec and dbrec.ENABLED:
        last_recon = float(state.get("_db_recon_ts", 0.0) or 0.0)
        if (now - last_recon) >= DB_RECONCILE_SEC:
            try:
                symbols = None
                if DB_RECONCILE_SYMBOLS_RAW:
                    symbols = [s.strip() for s in DB_RECONCILE_SYMBOLS_RAW.split(",") if s.strip()]
                else:
                    try:
                        pos_syms = list_open_position_symbols(force=True)
                        symbols = list((pos_syms.get("long") or set()) | (pos_syms.get("short") or set()))
                    except Exception:
                        symbols = None
                    if not symbols:
                        symbols = list(set(_get_open_symbols(state, "LONG") + _get_open_symbols(state, "SHORT")))
                lookback_sec = DB_RECONCILE_LOOKBACK_SEC if DB_RECONCILE_LOOKBACK_SEC > 0 else None
                since_ts = now - float(lookback_sec) if lookback_sec else state.get("_db_recon_since")
                if symbols:
                    res = dbrecon.sync_exchange_state(exchange, since_ts=since_ts, symbols=symbols)
                    state["_db_recon_ts"] = now
                    state["_db_recon_since"] = now
                    state["_db_recon_stats"] = res
                    last_log = float(state.get("_db_recon_log_ts", 0.0) or 0.0)
                    if (now - last_log) >= max(60.0, DB_RECONCILE_SEC * 2):
                        print(f"[db-reconcile] symbols={len(symbols)} stats={res}")
                        state["_db_recon_log_ts"] = now
            except Exception:
                pass
    exec_backoff_mid = 0.0
    try:
        exec_backoff_mid = float(get_global_backoff_until() or 0.0)
    except Exception:
        exec_backoff_mid = 0.0
    if time.time() < exec_backoff_mid:
        print("[rate-limit] executor backoff active during manage-mode; skip manage this cycle")
        return

    exit_force_refreshed = False
    for sym, st in list(state.items()):
        if not isinstance(st, dict) or not st.get("in_pos"):
            continue
        roi = None
        last_ping = float(st.get("manage_ping_ts", 0.0) or 0.0)
        if (now - last_ping) >= MANAGE_PING_COOLDOWN_SEC:
            try:
                roi = get_short_roi_pct(sym)
            except Exception:
                roi = None
            st["manage_ping_ts"] = now
            state[sym] = st

        last_entry_val = float(st.get("last_entry", 0.0) or 0.0)
        amt = get_short_position_amount(sym)
        if amt <= 0:
            if last_entry_val and (now - last_entry_val) < MANUAL_CLOSE_GRACE_SEC:
                try:
                    refresh_positions_cache(force=True)
                except Exception:
                    pass
                amt = get_short_position_amount(sym)
                if amt > 0:
                    continue
            cancel_stop_orders(sym)
            open_tr = _get_open_trade(state, "SHORT", sym)
            engine_label = _engine_label_from_reason(
                (open_tr.get("meta") or {}).get("reason") if open_tr else None
            )
            exit_reason = "manual_close"
            exit_tag = "MANUAL"
            if isinstance(open_tr, dict):
                meta = open_tr.get("meta") or {}
                if meta.get("sl_order_id"):
                    exit_reason = "auto_exit_sl"
                    exit_tag = "SL"
            _close_trade(
                state,
                side="SHORT",
                symbol=sym,
                exit_ts=now,
                exit_price=None,
                pnl_usdt=None,
                reason=exit_reason,
            )
            _append_report_line(sym, "SHORT", None, None, engine_label)
            last_entry_val = float(st.get("last_entry", 0))
            state[sym] = {
                "in_pos": False,
                "last_ok": False,
                "last_entry": last_entry_val,
                "dca_adds": 0,
                "dca_adds_long": 0,
                "dca_adds_short": 0,
            }
            if exit_reason == "auto_exit_sl" and engine_label == "SWAGGY":
                st = state.get(sym, {})
                if isinstance(st, dict):
                    st["swaggy_last_sl_ts"] = now
                    state[sym] = st
            if (
                isinstance(open_tr, dict)
                and engine_label != "UNKNOWN"
                and _trade_has_entry(open_tr)
                and not _recent_auto_exit(state, sym, now)
            ):
                order_block = _format_order_id_block(open_tr.get("entry_order_id"), open_tr.get("exit_order_id"))
                order_line = f"{order_block}\n" if order_block else ""
                send_telegram(
                    f"🔴 <b>숏 청산</b>\n"
                    f"<b>{sym}</b>\n"
                    f"엔진: {_display_engine_label(engine_label)}\n"
                    f"사유: {exit_tag}\n"
                    f"{order_line}".rstrip()
                )
            time.sleep(0.1)
            continue

        if _should_sample_short_position(sym, st, now):
            raw_pos = None
            update_ts = None
            if executor_mod is not None:
                if hasattr(executor_mod, "_find_position"):
                    try:
                        raw_pos = executor_mod._find_position(sym)
                    except Exception:
                        raw_pos = None
                try:
                    update_ts = float(getattr(executor_mod, "_POS_ALL_CACHE", {}).get("ts", 0.0) or 0.0)
                except Exception:
                    update_ts = None
            latency_hint = None
            if isinstance(update_ts, (int, float)) and update_ts > 0:
                latency_hint = max(0.0, now - update_ts)
            print(
                "[short-pos-sample] "
                f"sym={sym} pos_amt={amt} raw_position={raw_pos} "
                f"update_ts={update_ts} latency_hint={latency_hint}"
            )

        if AUTO_EXIT_ENABLED:
            last_exit = float(st.get("manage_exit_ts", 0.0) or 0.0)
            if (now - last_exit) >= MANAGE_EXIT_COOLDOWN_SEC:
                if last_entry_val and (now - last_entry_val) < AUTO_EXIT_GRACE_SEC:
                    continue
                if not exit_force_refreshed:
                    try:
                        refresh_positions_cache(force=True)
                    except Exception:
                        pass
                    exit_force_refreshed = True
                st["manage_exit_ts"] = now
                state[sym] = st
                pos_detail = None
                try:
                    pos_detail = get_short_position_detail(sym)
                except Exception:
                    pos_detail = None
                profit_unlev = None
                mark_px = None
                entry_px = None
                if pos_detail:
                    entry_px = pos_detail.get("entry")
                    mark_px = pos_detail.get("mark")
                    try:
                        if entry_px and mark_px:
                            profit_unlev = (float(entry_px) - float(mark_px)) / float(entry_px) * 100.0
                    except Exception:
                        profit_unlev = None
                if profit_unlev is not None and profit_unlev >= AUTO_EXIT_SHORT_TP_PCT:
                    open_tr = _get_open_trade(state, "SHORT", sym)
                    engine_label = _engine_label_from_reason(
                        (open_tr.get("meta") or {}).get("reason") if open_tr else None
                    )
                    pnl_usdt = pos_detail.get("pnl") if isinstance(pos_detail, dict) else None
                    try:
                        set_dry_run(False if LIVE_TRADING else True)
                    except Exception:
                        pass
                    res = close_short_market(sym)
                    exit_order_id = _order_id_from_res(res)
                    cancel_stop_orders(sym)
                    last_entry_val = float(st.get("last_entry", 0))
                    state[sym] = {
                        "in_pos": False,
                        "last_ok": False,
                        "last_entry": last_entry_val,
                        "dca_adds": 0,
                        "dca_adds_long": 0,
                        "dca_adds_short": 0,
                    }
                    avg_price = (
                        res.get("order", {}).get("average")
                        or res.get("order", {}).get("price")
                        or res.get("order", {}).get("info", {}).get("avgPrice")
                    )
                    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
                    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
                    roi_leveraged = None
                    try:
                        roi_leveraged = get_short_roi_pct(sym)
                    except Exception:
                        roi_leveraged = None
                    exit_tag = "TP"
                    _close_trade(
                        state,
                        side="SHORT",
                        symbol=sym,
                        exit_ts=now,
                        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
                        pnl_usdt=pnl_usdt,
                        reason="auto_exit_tp",
                        exit_order_id=exit_order_id,
                    )
                    st = state.get(sym, {})
                    if isinstance(st, dict):
                        _set_last_exit_state(st, "SHORT", now, "auto_exit_tp")
                        state[sym] = st
                    _append_report_line(sym, "SHORT", profit_unlev, pnl_usdt, engine_label)
                    order_block = _format_order_id_block(
                        open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
                        exit_order_id,
                    )
                    order_line = f"{order_block}\n" if order_block else ""
                    send_telegram(
                        f"{EXIT_ICON} <b>숏 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {exit_tag}\n"
                        f"{order_line}"
                        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                        f"{'' if pnl_usdt is None else f' 손익={pnl_usdt:+.3f} USDT'}"
                        f"{'' if roi_leveraged is None else f' 레버리지ROI={roi_leveraged:.2f}%'}"
                    )
                    time.sleep(0.15)
                    continue
                if profit_unlev is not None and profit_unlev <= -AUTO_EXIT_SHORT_SL_PCT:
                    open_tr = _get_open_trade(state, "SHORT", sym)
                    engine_label = _engine_label_from_reason(
                        (open_tr.get("meta") or {}).get("reason") if open_tr else None
                    )
                    pnl_usdt = pos_detail.get("pnl") if isinstance(pos_detail, dict) else None
                    try:
                        set_dry_run(False if LIVE_TRADING else True)
                    except Exception:
                        pass
                    res = close_short_market(sym)
                    exit_order_id = _order_id_from_res(res)
                    cancel_stop_orders(sym)
                    last_entry_val = float(st.get("last_entry", 0))
                    state[sym] = {
                        "in_pos": False,
                        "last_ok": False,
                        "last_entry": last_entry_val,
                        "dca_adds": 0,
                        "dca_adds_long": 0,
                        "dca_adds_short": 0,
                    }
                    avg_price = (
                        res.get("order", {}).get("average")
                        or res.get("order", {}).get("price")
                        or res.get("order", {}).get("info", {}).get("avgPrice")
                    )
                    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
                    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
                    exit_tag = "SL"
                    _close_trade(
                        state,
                        side="SHORT",
                        symbol=sym,
                        exit_ts=now,
                        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
                        pnl_usdt=pnl_usdt,
                        reason="auto_exit_sl",
                        exit_order_id=exit_order_id,
                    )
                    st = state.get(sym, {})
                    if isinstance(st, dict):
                        _set_last_exit_state(st, "SHORT", now, "auto_exit_sl")
                        state[sym] = st
                    _append_report_line(sym, "SHORT", profit_unlev, pnl_usdt, engine_label)
                    order_block = _format_order_id_block(
                        open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
                        exit_order_id,
                    )
                    order_line = f"{order_block}\n" if order_block else ""
                    send_telegram(
                        f"🔴 <b>숏 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {exit_tag}\n"
                        f"{order_line}"
                        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                        f"{'' if pnl_usdt is None else f' 손익={pnl_usdt:+.3f} USDT'}"
                    )
                    time.sleep(0.15)
                    continue

        last_eval = float(st.get("manage_eval_ts", 0.0) or 0.0)
        if (now - last_eval) < MANAGE_EVAL_COOLDOWN_SEC:
            continue
        st["manage_eval_ts"] = now
        state[sym] = st

        adds_done = int(st.get("dca_adds_short", st.get("dca_adds", 0)))
        dca_res = dca_short_if_needed(sym, adds_done=adds_done, margin_mode=MARGIN_MODE)
        if dca_res.get("status") not in ("skip", "warn"):
            st["dca_adds_short"] = adds_done + 1
            state[sym] = st
            send_telegram(
                f"➕ <b>DCA</b> {sym} adds {adds_done}->{adds_done+1} mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
            )
            time.sleep(0.1)

    long_syms = set(_get_open_symbols(state, "LONG"))
    try:
        pos_syms = list_open_position_symbols(force=True)
        long_syms |= set(pos_syms.get("long") or [])
    except Exception:
        pass
    for sym in long_syms:
        detail = get_long_position_detail(sym)
        if not detail:
            try:
                refresh_positions_cache(force=True)
            except Exception:
                pass
            detail = get_long_position_detail(sym)
        open_tr = _get_open_trade(state, "LONG", sym)
        entry_ts = float(open_tr.get("entry_ts", 0.0) or 0.0) if open_tr else 0.0
        if entry_ts and (now - entry_ts) < AUTO_EXIT_GRACE_SEC:
            continue
        if not detail:
            try:
                amt = get_long_position_amount(sym)
            except Exception:
                amt = 0.0
            if amt <= 0 and open_tr:
                cancel_stop_orders(sym)
                engine_label = _engine_label_from_reason(
                    (open_tr.get("meta") or {}).get("reason") if open_tr else None
                )
                _close_trade(
                    state,
                    side="LONG",
                    symbol=sym,
                    exit_ts=now,
                    exit_price=None,
                    pnl_usdt=None,
                    reason="manual_close",
                )
                _append_report_line(sym, "LONG", None, None, engine_label)
                st = state.get(sym, {}) if isinstance(state, dict) else {}
                st["in_pos"] = False
                st["last_ok"] = False
                st["dca_adds"] = 0
                st["dca_adds_long"] = 0
                st["dca_adds_short"] = 0
                state[sym] = st
                if engine_label != "UNKNOWN" and _trade_has_entry(open_tr) and not _recent_auto_exit(state, sym, now):
                    order_block = _format_order_id_block(open_tr.get("entry_order_id"), open_tr.get("exit_order_id"))
                    order_line = f"{order_block}\n" if order_block else ""
                    send_telegram(
                        f"{EXIT_ICON} <b>롱 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: MANUAL\n"
                        f"{order_line}".rstrip()
                    )
                time.sleep(0.1)
                continue
            open_tr = _get_open_trade(state, "LONG", sym)
            engine_label = _engine_label_from_reason(
                (open_tr.get("meta") or {}).get("reason") if open_tr else None
            )
            skip_line = f"[long-exit-skip] sym={sym} reason=no_position_detail engine={engine_label}"
            print(skip_line)
            if engine_label == "ATLASFABIO":
                try:
                    os.makedirs(os.path.join("logs", "fabio"), exist_ok=True)
                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                    with open(os.path.join("logs", "fabio", "atlasfabio_funnel.log"), "a", encoding="utf-8") as f:
                        f.write(f"{ts} {skip_line}\n")
                except Exception:
                    pass
            continue
        entry_px = detail.get("entry")
        mark_px = detail.get("mark")
        if not isinstance(entry_px, (int, float)) or not isinstance(mark_px, (int, float)) or entry_px <= 0:
            continue
        profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
        closed = False
        if AUTO_EXIT_ENABLED and profit_unlev >= AUTO_EXIT_LONG_TP_PCT:
            engine_label = _engine_label_from_reason(
                (open_tr.get("meta") or {}).get("reason") if open_tr else None
            )
            try:
                set_dry_run(False if LONG_LIVE_TRADING else True)
            except Exception:
                pass
            res = close_long_market(sym)
            exit_order_id = _order_id_from_res(res)
            cancel_stop_orders(sym)
            avg_price = (
                res.get("order", {}).get("average")
                or res.get("order", {}).get("price")
                or res.get("order", {}).get("info", {}).get("avgPrice")
            )
            filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
            cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
            exit_tag = "TP"
            pnl_long = detail.get("pnl")
            _close_trade(
                state,
                side="LONG",
                symbol=sym,
                exit_ts=now,
                exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
                pnl_usdt=pnl_long,
                reason="auto_exit_tp",
                exit_order_id=exit_order_id,
            )
            st = state.get(sym, {})
            if isinstance(st, dict):
                _set_last_exit_state(st, "LONG", now, "auto_exit_tp")
                state[sym] = st
            _append_report_line(sym, "LONG", profit_unlev, pnl_long, engine_label)
            order_block = _format_order_id_block(
                open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
                exit_order_id,
            )
            order_line = f"{order_block}\n" if order_block else ""
            send_telegram(
                f"{EXIT_ICON} <b>롱 청산</b>\n"
                f"<b>{sym}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{order_line}"
                f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
            )
            time.sleep(0.15)
            closed = True
        elif AUTO_EXIT_ENABLED and profit_unlev <= -AUTO_EXIT_LONG_SL_PCT:
            engine_label = _engine_label_from_reason(
                (open_tr.get("meta") or {}).get("reason") if open_tr else None
            )
            try:
                set_dry_run(False if LONG_LIVE_TRADING else True)
            except Exception:
                pass
            res = close_long_market(sym)
            exit_order_id = _order_id_from_res(res)
            cancel_stop_orders(sym)
            avg_price = (
                res.get("order", {}).get("average")
                or res.get("order", {}).get("price")
                or res.get("order", {}).get("info", {}).get("avgPrice")
            )
            filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
            cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
            exit_tag = "SL"
            pnl_long = detail.get("pnl")
            _close_trade(
                state,
                side="LONG",
                symbol=sym,
                exit_ts=now,
                exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
                pnl_usdt=pnl_long,
                reason="auto_exit_sl",
                exit_order_id=exit_order_id,
            )
            st = state.get(sym, {})
            if isinstance(st, dict):
                _set_last_exit_state(st, "LONG", now, "auto_exit_sl")
                state[sym] = st
            _append_report_line(sym, "LONG", profit_unlev, pnl_long, engine_label)
            order_block = _format_order_id_block(
                open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
                exit_order_id,
            )
            order_line = f"{order_block}\n" if order_block else ""
            send_telegram(
                        f"{EXIT_ICON} <b>롱 청산</b>\n"
                f"<b>{sym}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{order_line}"
                f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
            )
            time.sleep(0.15)
            closed = True
        if not closed:
            st = state.get(sym, {})
            last_eval = float(st.get("manage_eval_long_ts", 0.0) or 0.0)
            if (now - last_eval) >= MANAGE_EVAL_COOLDOWN_SEC:
                st["manage_eval_long_ts"] = now
                state[sym] = st
                adds_done = int(st.get("dca_adds_long", st.get("dca_adds", 0)))
                dca_res = dca_long_if_needed(sym, adds_done=adds_done, margin_mode=MARGIN_MODE)
                if dca_res.get("status") not in ("skip", "warn"):
                    st["dca_adds_long"] = adds_done + 1
                    state[sym] = st
                    send_telegram(
                        f"➕ <b>DCA</b> {sym} LONG adds {adds_done}->{adds_done+1} "
                        f"mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                    )
                    time.sleep(0.1)

    tickers = _get_manage_tickers(state, exchange, MANAGE_TICKER_TTL_SEC)
    _reconcile_long_trades(state, cached_long_ex, tickers)
    _reconcile_short_trades(state, tickers)


def _manage_loop_worker(state: dict, exchange, cached_long_ex, send_telegram) -> None:
    print(f"[manage-loop] started (sleep={MANAGE_LOOP_SLEEP_SEC:.1f}s)")
    while True:
        buf = []
        _set_thread_log_buffer(buf)
        try:
            _run_manage_cycle(state, exchange, cached_long_ex, send_telegram)
        except Exception as e:
            print("[manage-loop] error:", e)
        finally:
            _set_thread_log_buffer(None)
        time.sleep(MANAGE_LOOP_SLEEP_SEC)

def _reload_runtime_settings_from_disk(state: dict) -> None:
    try:
        disk = load_state()
    except Exception:
        return
    if not isinstance(disk, dict):
        return
    keys = [
        "_auto_exit",
        "_auto_exit_long_tp_pct",
        "_auto_exit_long_sl_pct",
        "_auto_exit_short_tp_pct",
        "_auto_exit_short_sl_pct",
        "_live_trading",
        "_long_live",
        "_max_open_positions",
        "_entry_usdt",
        "_dca_enabled",
        "_dca_pct",
        "_dca_first_pct",
        "_dca_second_pct",
        "_dca_third_pct",
        "_exit_cooldown_hours",
        "_atlas_fabio_enabled",
        "_swaggy_atlas_lab_enabled",
        "_div15m_long_enabled",
        "_div15m_short_enabled",
        "_rsi_enabled",
        "_dtfx_enabled",
        "_pumpfade_enabled",
        "_atlas_rs_fail_short_enabled",
        "_chat_id",
        "_manage_ws_mode",
        "_entry_event_offset",
        "_runtime_cfg_ts",
    ]
    for key in keys:
        if key in disk:
            state[key] = disk.get(key)
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT, AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, ATLAS_FABIO_ENABLED, SWAGGY_ATLAS_LAB_ENABLED, DTFX_ENABLED, PUMPFADE_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, DIV15M_LONG_ENABLED, DIV15M_SHORT_ENABLED, RSI_ENABLED
    global USDT_PER_TRADE, CHAT_ID_RUNTIME, MANAGE_WS_MODE, DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC
    if isinstance(state.get("_auto_exit"), bool):
        AUTO_EXIT_ENABLED = bool(state.get("_auto_exit"))
    if isinstance(state.get("_auto_exit_long_tp_pct"), (int, float)):
        AUTO_EXIT_LONG_TP_PCT = float(state.get("_auto_exit_long_tp_pct"))
    if isinstance(state.get("_auto_exit_long_sl_pct"), (int, float)):
        AUTO_EXIT_LONG_SL_PCT = float(state.get("_auto_exit_long_sl_pct"))
    if isinstance(state.get("_auto_exit_short_tp_pct"), (int, float)):
        AUTO_EXIT_SHORT_TP_PCT = float(state.get("_auto_exit_short_tp_pct"))
    if isinstance(state.get("_auto_exit_short_sl_pct"), (int, float)):
        AUTO_EXIT_SHORT_SL_PCT = float(state.get("_auto_exit_short_sl_pct"))
    if isinstance(state.get("_live_trading"), bool):
        LIVE_TRADING = bool(state.get("_live_trading"))
    if isinstance(state.get("_long_live"), bool):
        LONG_LIVE_TRADING = bool(state.get("_long_live"))
    if isinstance(state.get("_max_open_positions"), (int, float)):
        try:
            MAX_OPEN_POSITIONS = int(state.get("_max_open_positions"))
        except Exception:
            pass
    if isinstance(state.get("_entry_usdt"), (int, float)):
        USDT_PER_TRADE = float(state.get("_entry_usdt"))
    if isinstance(state.get("_atlas_fabio_enabled"), bool):
        ATLAS_FABIO_ENABLED = bool(state.get("_atlas_fabio_enabled"))
    if isinstance(state.get("_swaggy_atlas_lab_enabled"), bool):
        SWAGGY_ATLAS_LAB_ENABLED = bool(state.get("_swaggy_atlas_lab_enabled"))
    if isinstance(state.get("_div15m_long_enabled"), bool):
        DIV15M_LONG_ENABLED = bool(state.get("_div15m_long_enabled"))
    if isinstance(state.get("_div15m_short_enabled"), bool):
        DIV15M_SHORT_ENABLED = bool(state.get("_div15m_short_enabled"))
    if isinstance(state.get("_rsi_enabled"), bool):
        RSI_ENABLED = bool(state.get("_rsi_enabled"))
    if isinstance(state.get("_dca_enabled"), bool):
        DCA_ENABLED = bool(state.get("_dca_enabled"))
        if executor_mod:
            executor_mod.DCA_ENABLED = DCA_ENABLED
    if isinstance(state.get("_dca_pct"), (int, float)):
        DCA_PCT = float(state.get("_dca_pct"))
        if executor_mod:
            executor_mod.DCA_PCT = DCA_PCT
    if isinstance(state.get("_dca_first_pct"), (int, float)):
        DCA_FIRST_PCT = float(state.get("_dca_first_pct"))
        if executor_mod:
            executor_mod.DCA_FIRST_PCT = DCA_FIRST_PCT
    if isinstance(state.get("_dca_second_pct"), (int, float)):
        DCA_SECOND_PCT = float(state.get("_dca_second_pct"))
        if executor_mod:
            executor_mod.DCA_SECOND_PCT = DCA_SECOND_PCT
    if isinstance(state.get("_dca_third_pct"), (int, float)):
        DCA_THIRD_PCT = float(state.get("_dca_third_pct"))
        if executor_mod:
            executor_mod.DCA_THIRD_PCT = DCA_THIRD_PCT
    if isinstance(state.get("_exit_cooldown_hours"), (int, float)):
        EXIT_COOLDOWN_HOURS = float(state.get("_exit_cooldown_hours"))
        EXIT_COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
    if isinstance(state.get("_dtfx_enabled"), bool):
        DTFX_ENABLED = bool(state.get("_dtfx_enabled"))
    if isinstance(state.get("_pumpfade_enabled"), bool):
        PUMPFADE_ENABLED = bool(state.get("_pumpfade_enabled"))
    if isinstance(state.get("_atlas_rs_fail_short_enabled"), bool):
        ATLAS_RS_FAIL_SHORT_ENABLED = bool(state.get("_atlas_rs_fail_short_enabled"))
    if state.get("_chat_id"):
        CHAT_ID_RUNTIME = str(state.get("_chat_id"))
    if isinstance(state.get("_manage_ws_mode"), bool):
        MANAGE_WS_MODE = bool(state.get("_manage_ws_mode"))
    _maybe_reload_rsi_config()

_RSI_CONFIG_MTIME: Optional[float] = None
_RSI_CONFIG_LOGGED: bool = False

def _maybe_reload_rsi_config() -> None:
    global _RSI_CONFIG_MTIME, rsi_engine
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "engines", "rsi", "config.py")
        mtime = os.path.getmtime(cfg_path)
    except Exception:
        return
    if _RSI_CONFIG_MTIME is None:
        _RSI_CONFIG_MTIME = mtime
        try:
            import engines.rsi.config as rsi_config
            importlib.reload(rsi_config)
            if rsi_engine:
                rsi_engine.config = rsi_config.RsiConfig()
            print(f"[config] rsi_config loaded mtime={mtime:.0f}")
        except Exception as e:
            print(f"[config] rsi_config load failed: {e}")
        return
    if mtime <= _RSI_CONFIG_MTIME:
        return
    try:
        import engines.rsi.config as rsi_config
        importlib.reload(rsi_config)
        if rsi_engine:
            rsi_engine.config = rsi_config.RsiConfig()
        print(f"[config] rsi_config reloaded mtime={mtime:.0f}")
    except Exception as e:
        print(f"[config] rsi_config reload failed: {e}")
    _RSI_CONFIG_MTIME = mtime

def _load_rsi_config_defaults() -> Optional[object]:
    try:
        import engines.rsi.config as rsi_config
        importlib.reload(rsi_config)
        return rsi_config.RsiConfig()
    except Exception as e:
        print(f"[config] rsi_config load failed: {e}")
        return None

def _read_rsi_config_values() -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(__file__), "engines", "rsi", "config.py")
    try:
        text = open(cfg_path, "r", encoding="utf-8").read()
    except Exception:
        return {}
    values: Dict[str, Any] = {}
    for line in text.splitlines():
        raw = line.strip()
        if raw.startswith("min_quote_volume_usdt"):
            parts = raw.split("=", 1)
            if len(parts) == 2:
                val = parts[1].split("#", 1)[0].strip().replace("_", "")
                try:
                    values["min_quote_volume_usdt"] = float(val)
                except Exception:
                    pass
        elif raw.startswith("universe_top_n"):
            parts = raw.split("=", 1)
            if len(parts) == 2:
                val = parts[1].split("#", 1)[0].strip().replace("_", "")
                if val != "None":
                    try:
                        values["universe_top_n"] = int(val)
                    except Exception:
                        pass
    if "min_quote_volume_usdt" not in values:
        m = re.search(r"min_quote_volume_usdt\\s*=\\s*([0-9_\\.]+)", text)
        if m:
            try:
                values["min_quote_volume_usdt"] = float(m.group(1).replace("_", ""))
            except Exception:
                pass
    if "universe_top_n" not in values:
        m = re.search(r"universe_top_n\\s*=\\s*([0-9_]+|None)", text)
        if m:
            raw = m.group(1)
            if raw != "None":
                try:
                    values["universe_top_n"] = int(raw.replace("_", ""))
                except Exception:
                    pass
    return values

def _save_runtime_settings_only(state: dict) -> None:
    disk = load_state()
    if not isinstance(disk, dict):
        disk = {}
    keys = [
        "_auto_exit",
        "_auto_exit_long_tp_pct",
        "_auto_exit_long_sl_pct",
        "_auto_exit_short_tp_pct",
        "_auto_exit_short_sl_pct",
        "_live_trading",
        "_long_live",
        "_max_open_positions",
        "_entry_usdt",
        "_dca_enabled",
        "_dca_pct",
        "_dca_first_pct",
        "_dca_second_pct",
        "_dca_third_pct",
        "_exit_cooldown_hours",
        "_atlas_fabio_enabled",
        "_swaggy_atlas_lab_enabled",
        "_div15m_long_enabled",
        "_div15m_short_enabled",
        "_rsi_enabled",
        "_dtfx_enabled",
        "_pumpfade_enabled",
        "_atlas_rs_fail_short_enabled",
        "_tg_offset",
        "_tg_queue_offset",
        "_chat_id",
        "_manage_ws_mode",
        "_runtime_cfg_ts",
    ]
    for key in keys:
        if key in state:
            disk[key] = state.get(key)
    save_state(disk)

def _append_report_line(
    symbol: str,
    side: str,
    roi_pct: Optional[float],
    pnl_usdt: Optional[float],
    engine_label: str,
) -> None:
    return


def _update_report_csv(tr: dict) -> None:
    try:
        if REPORT_API_ONLY:
            return
        os.makedirs("reports", exist_ok=True)
        entry_ts = tr.get("entry_ts")
        if not isinstance(entry_ts, (int, float)):
            return
        entry_dt = datetime.fromtimestamp(float(entry_ts))
        date_tag = entry_dt.strftime("%Y-%m-%d")
        report_path = os.path.join("reports", f"{date_tag}.csv")
        entry_ts_str = entry_dt.strftime("%Y-%m-%d %H:%M:%S")
        exit_ts = tr.get("exit_ts")
        exit_ts_str = ""
        if isinstance(exit_ts, (int, float)):
            exit_ts_str = datetime.fromtimestamp(float(exit_ts)).strftime("%Y-%m-%d %H:%M:%S")
        side = tr.get("side") or ""
        symbol = tr.get("symbol") or ""
        entry_price = tr.get("entry_price")
        exit_price = tr.get("exit_price")
        qty = tr.get("qty")
        pnl = tr.get("pnl_usdt")
        roi = tr.get("roi_pct")
        engine = tr.get("engine_label") or ""
        exit_reason = tr.get("exit_reason") or ""
        def _to_float(val: Any) -> Optional[float]:
            try:
                return float(val)
            except Exception:
                return None

        def _calc_win_loss(pnl_val: Any, roi_val: Any) -> tuple[str, str]:
            base = _to_float(pnl_val)
            if base is None:
                base = _to_float(roi_val)
            if base is None:
                return "", ""
            if base > 0:
                return "1", "0"
            if base < 0:
                return "0", "1"
            return "0", "0"

        expected_cols = [
            "entry_ts",
            "entry_order_id",
            "symbol",
            "side",
            "engine",
            "entry_price",
            "qty",
            "exit_ts",
            "exit_order_id",
            "exit_price",
            "roi",
            "pnl",
            "exit_reason",
            "win",
            "loss",
        ]
        records = []
        if os.path.exists(report_path):
            with open(report_path, "r", encoding="utf-8") as f:
                rows = [line.rstrip("\n") for line in f.readlines()]
            if rows:
                header_cols = rows[0].split(",")
                for row in rows[1:]:
                    parts = row.split(",")
                    if len(parts) < len(header_cols):
                        parts += [""] * (len(header_cols) - len(parts))
                    rec = {header_cols[i]: parts[i] for i in range(len(header_cols))}
                    records.append(rec)

        entry_order_id = tr.get("entry_order_id") or ""
        exit_order_id = tr.get("exit_order_id") or ""
        key = f"order:{entry_order_id}" if entry_order_id else f"{entry_ts_str}|{symbol}|{side}"
        updated = False
        for rec in records:
            row_entry_order_id = rec.get("entry_order_id") or ""
            row_key = f"order:{row_entry_order_id}" if row_entry_order_id else f"{rec.get('entry_ts','')}|{rec.get('symbol','')}|{rec.get('side','')}"
            if row_key == key:
                rec.update(
                    {
                        "entry_ts": entry_ts_str,
                        "entry_order_id": entry_order_id,
                        "symbol": symbol,
                        "side": side,
                        "engine": engine,
                        "entry_price": entry_price if entry_price is not None else "",
                        "qty": qty if qty is not None else "",
                        "exit_ts": exit_ts_str,
                        "exit_order_id": exit_order_id,
                        "exit_price": exit_price if exit_price is not None else "",
                        "roi": roi if roi is not None else "",
                        "pnl": pnl if pnl is not None else "",
                        "exit_reason": exit_reason,
                    }
                )
                win, loss = _calc_win_loss(rec.get("pnl"), rec.get("roi"))
                rec["win"] = win
                rec["loss"] = loss
                updated = True
                break
        if not updated:
            win, loss = _calc_win_loss(pnl, roi)
            records.append(
                {
                    "entry_ts": entry_ts_str,
                    "entry_order_id": entry_order_id,
                    "symbol": symbol,
                    "side": side,
                    "engine": engine,
                    "entry_price": entry_price if entry_price is not None else "",
                    "qty": qty if qty is not None else "",
                    "exit_ts": exit_ts_str,
                    "exit_order_id": exit_order_id,
                    "exit_price": exit_price if exit_price is not None else "",
                    "roi": roi if roi is not None else "",
                    "pnl": pnl if pnl is not None else "",
                    "exit_reason": exit_reason,
                    "win": win,
                    "loss": loss,
                }
            )

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(",".join(expected_cols) + "\n")
            for rec in records:
                if not rec.get("win") and not rec.get("loss"):
                    win, loss = _calc_win_loss(rec.get("pnl"), rec.get("roi"))
                    rec["win"] = win
                    rec["loss"] = loss
                line = ",".join(str(rec.get(col, "")) for col in expected_cols)
                f.write(line + "\n")
    except Exception:
        pass


def _append_entry_event(tr: dict) -> None:
    try:
        entry_order_id = tr.get("entry_order_id")
        if not entry_order_id:
            return
        date_tag = time.strftime("%Y-%m-%d")
        dir_path = os.path.join("logs", "entry")
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, f"entry_events-{date_tag}.log")
        payload = {
            "entry_ts": datetime.fromtimestamp(float(tr.get("entry_ts") or 0.0)).strftime("%Y-%m-%d %H:%M:%S"),
            "entry_order_id": entry_order_id,
            "symbol": tr.get("symbol"),
            "side": tr.get("side"),
            "entry_price": tr.get("entry_price"),
            "qty": tr.get("qty"),
            "engine": tr.get("engine_label"),
            "usdt": tr.get("usdt"),
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        if dbrec:
            try:
                ts_val = tr.get("entry_ts")
                if not isinstance(ts_val, (int, float)):
                    ts_val = time.time()
                dbrec.record_engine_signal(
                    symbol=payload.get("symbol") or "",
                    side=payload.get("side") or "",
                    engine=payload.get("engine") or "",
                    reason=(payload.get("engine") or "").lower(),
                    meta=payload,
                    ts=float(ts_val),
                )
            except Exception:
                pass
    except Exception:
        pass

def _sync_short_positions_state(state: dict, symbols: list) -> None:
    now = time.time()
    for sym in symbols:
        st = state.get(sym) or {}
        try:
            amt = get_short_position_amount(sym)
        except Exception:
            amt = 0
        if amt > 0:
            st.setdefault("in_pos", True)
            st.setdefault("dca_adds", 0)
            st.setdefault("dca_adds_long", 0)
            st.setdefault("dca_adds_short", 0)
            st.setdefault("last_entry", now)
            st.setdefault("manage_ping_ts", now - MANAGE_PING_COOLDOWN_SEC)
            state[sym] = st
        else:
            if isinstance(st, dict):
                st["in_pos"] = False
                state[sym] = st

def _sync_positions_state(state: dict, symbols: list) -> None:
    now = time.time()
    for sym in symbols:
        st = state.get(sym) or {}
        if not isinstance(st, dict):
            st = {}
        try:
            short_amt = get_short_position_amount(sym)
        except Exception:
            short_amt = 0
        try:
            long_amt = get_long_position_amount(sym)
        except Exception:
            long_amt = 0
        was_in_pos = bool(st.get("in_pos"))
        if short_amt > 0 or long_amt > 0:
            st.setdefault("in_pos", True)
            st.setdefault("dca_adds", 0)
            st.setdefault("dca_adds_long", 0)
            st.setdefault("dca_adds_short", 0)
            st.setdefault("last_entry", now)
            st.setdefault("manage_ping_ts", now - MANAGE_PING_COOLDOWN_SEC)
            if not was_in_pos:
                try:
                    side = "LONG" if long_amt > 0 else "SHORT" if short_amt > 0 else "UNKNOWN"
                    dbrec and dbrec.record_engine_signal(
                        symbol=sym,
                        side=side,
                        engine="MANUAL",
                        reason="manual_entry",
                        meta={"source": "pos_sync"},
                        ts=now,
                    )
                except Exception:
                    pass
            state[sym] = st
        else:
            if isinstance(st, dict):
                st["in_pos"] = False
                state[sym] = st

def send_telegram(text: str, allow_early: bool = False, chat_id: Optional[str] = None) -> bool:
    return _send_telegram_direct(text, allow_early=allow_early, chat_id=chat_id)

def _safe_telegram_text(text: str, limit: int = 3800) -> str:
    if not isinstance(text, str):
        return ""
    if len(text) <= limit:
        return text
    trimmed = text[:limit]
    last_lt = trimmed.rfind("<")
    last_gt = trimmed.rfind(">")
    if last_lt > last_gt:
        trimmed = trimmed[:last_lt]
    closes = ""
    if "<pre>" in trimmed and "</pre>" not in trimmed.split("<pre>")[-1]:
        closes += "</pre>"
    for tag in ("b", "i", "u", "s", "code"):
        open_tag = f"<{tag}>"
        close_tag = f"</{tag}>"
        diff = trimmed.count(open_tag) - trimmed.count(close_tag)
        if diff > 0:
            closes += close_tag * diff
    if len(trimmed) + len(closes) > limit:
        trimmed = trimmed[: max(0, limit - len(closes))]
    return trimmed + closes

def _send_telegram_direct(text: str, allow_early: bool = False, chat_id: Optional[str] = None) -> bool:
    global CHAT_ID_RUNTIME
    if (time.time() - START_TIME) < TELEGRAM_STARTUP_GRACE_SEC and not allow_early:
        return False
    target_chat_id = chat_id or (CHAT_ID_RUNTIME or CHAT_ID)
    if not BOT_TOKEN or not target_chat_id:
        print("[telegram] BOT_TOKEN or CHAT_ID missing")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target_chat_id,
        "text": _safe_telegram_text(text, 3800),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(2):
        try:
            r = requests.post(url, json=payload, timeout=20)
            if not r.ok:
                print("[telegram] failed:", r.status_code, r.text)
            return r.ok
        except Exception as e:
            if attempt == 1:
                print("[telegram] error:", e)
                return False
            time.sleep(0.8)

def _enqueue_telegram(text: str, allow_early: bool = False, chat_id: Optional[str] = None) -> bool:
    try:
        os.makedirs("logs", exist_ok=True)
        payload = {
            "ts": time.time(),
            "text": text,
            "allow_early": bool(allow_early),
        }
        if chat_id:
            payload["chat_id"] = chat_id
        with open(os.path.join("logs", "telegram_queue.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return True
    except Exception as e:
        print("[telegram] enqueue error:", e)
        return False

def _drain_telegram_queue(state: Dict[str, dict]) -> None:
    queue_path = os.path.join("logs", "telegram_queue.log")
    try:
        if not os.path.exists(queue_path):
            return
        offset = _coerce_state_int(state.get("_tg_queue_offset", 0))
        with open(queue_path, "r", encoding="utf-8") as f:
            f.seek(offset)
            lines = f.readlines()
            new_offset = f.tell()
        if not lines:
            return
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            text = payload.get("text")
            if not text:
                continue
            allow_early = bool(payload.get("allow_early", False))
            chat_id = payload.get("chat_id")
            _send_telegram_direct(text, allow_early=allow_early, chat_id=chat_id)
        state["_tg_queue_offset"] = new_offset
    except Exception as e:
        print("[telegram] queue drain error:", e)

def _coerce_state_int(val: object) -> int:
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            return int(val)
        except Exception:
            return 0
    if isinstance(val, dict):
        for key in ("value", "offset", "id", "last"):
            if key in val:
                return _coerce_state_int(val.get(key))
    return 0

def handle_telegram_commands(state: Dict[str, dict]) -> None:
    """텔레그램으로부터 런타임 명령을 받아 AUTO_EXIT 토글/상태를 제어한다.
    - /auto_exit on|off|status
    - /status
    처리한 마지막 update_id는 state["_tg_offset"]에 저장한다.
    현재 auto-exit 설정은 state["_auto_exit"]에 동기화한다.
    """
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT, AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, ATLAS_FABIO_ENABLED, SWAGGY_ATLAS_LAB_ENABLED, DTFX_ENABLED, PUMPFADE_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, DIV15M_LONG_ENABLED, DIV15M_SHORT_ENABLED, RSI_ENABLED
    global DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT, USDT_PER_TRADE
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC
    if not BOT_TOKEN:
        return
    last_update_id = _coerce_state_int(state.get("_tg_offset", 0))
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {
        "offset": last_update_id + 1,
        "timeout": 10,
        "allowed_updates": ["message", "channel_post", "edited_message", "edited_channel_post"],
        # allowed_updates를 좁히면 불필요한 필드가 줄어든다
        # 하지만 일부 구현에서 오류가 있을 수 있어 기본값 유지도 무방
    }
    state_dirty = False
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=20)
            if not r.ok:
                print("[telegram] getUpdates 실패:", r.status_code, r.text[:120])
                return
            break
        except Exception as e:
            if attempt == 1:
                print("[telegram] cmd error:", e)
                return
            time.sleep(0.8)
    try:
        data = r.json()
        updates = data.get("result", []) if isinstance(data, dict) else []
        new_last_id = last_update_id
        if updates:
            print(f"[telegram] updates 수신: {len(updates)}개 (offset {last_update_id}->{updates[-1].get('update_id')})")
        for u in updates:
            try:
                upd_id = int(u.get("update_id", 0) or 0)
                if upd_id > new_last_id:
                    new_last_id = upd_id
                update_kind = (
                    "message" if u.get("message")
                    else "channel_post" if u.get("channel_post")
                    else "edited_message" if u.get("edited_message")
                    else "edited_channel_post" if u.get("edited_channel_post")
                    else "unknown"
                )
                msg = (
                    u.get("message")
                    or u.get("channel_post")
                    or u.get("edited_message")
                    or u.get("edited_channel_post")
                    or {}
                )
                chat = msg.get("chat") or {}
                chat_id = str(chat.get("id")) if chat.get("id") is not None else None
                text = (msg.get("text") or "").strip()
                if not text or not chat_id:
                    continue
                # 런타임 chat_id 자동 학습 및 저장
                global CHAT_ID_RUNTIME
                if chat_id and chat_id != str(CHAT_ID_RUNTIME or ""):
                    CHAT_ID_RUNTIME = chat_id
                    state["_chat_id"] = chat_id
                reply_chat_id = chat_id
                def _reply(msg: str) -> bool:
                    return send_telegram(msg, allow_early=True, chat_id=reply_chat_id)

                lower = text.lower()
                if "status" in lower:
                    chat_type = (msg.get("chat") or {}).get("type")
                    print(f"[telegram] cmd raw text='{text}' chat_id={chat_id} type={chat_type} kind={update_kind}")
                cmd = lower.split()[0] if lower else ""
                if cmd.startswith("/"):
                    cmd = cmd.split("@")[0]
                responded = False
                if cmd in ("/auto_exit", "auto_exit"):
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        AUTO_EXIT_ENABLED = True
                        state["_auto_exit"] = True
                        state_dirty = True
                        resp = (
                            "✅ auto-exit ON "
                            f"(TP/SL: long {AUTO_EXIT_LONG_TP_PCT:.2f}%/{AUTO_EXIT_LONG_SL_PCT:.2f}%, "
                            f"short {AUTO_EXIT_SHORT_TP_PCT:.2f}%/{AUTO_EXIT_SHORT_SL_PCT:.2f}%)"
                        )
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        AUTO_EXIT_ENABLED = False
                        state["_auto_exit"] = False
                        state_dirty = True
                        resp = "⛔ auto-exit OFF"
                    else:  # status/help
                        resp = (
                            f"ℹ️ auto-exit: {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
                            f"auto-exit-long: TP {AUTO_EXIT_LONG_TP_PCT:.2f}% | SL {AUTO_EXIT_LONG_SL_PCT:.2f}%\n"
                            f"auto-exit-short: TP {AUTO_EXIT_SHORT_TP_PCT:.2f}% | SL {AUTO_EXIT_SHORT_SL_PCT:.2f}%\n"
                            f"live-trading: {'ON' if LIVE_TRADING else 'OFF'}\n"
                            "사용법: /auto_exit on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] auto_exit cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True

                if cmd in ("/live", "live"):
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        LIVE_TRADING = True
                        state["_live_trading"] = True
                        state_dirty = True
                        try:
                            set_dry_run(False)
                        except Exception:
                            pass
                        resp = "🚀 live-trading ON (신호 시 실제 주문 실행)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        LIVE_TRADING = False
                        state["_live_trading"] = False
                        state_dirty = True
                        try:
                            set_dry_run(True)
                        except Exception:
                            pass
                        resp = "🧪 live-trading OFF (알림 전용)"
                    else:
                        resp = f"ℹ️ live-trading 상태: {'ON' if LIVE_TRADING else 'OFF'}\n사용법: /live on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] live cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True

                if ((cmd == "/status" or cmd == "status") or lower.startswith("/status") or lower.startswith("status")) and not responded:
                    print(f"[telegram] status cmd matched: cmd='{cmd}' responded={responded}")
                    _reload_runtime_settings_from_disk(state)
                    try:
                        open_pos = count_open_positions(force=True)
                        if not isinstance(open_pos, int):
                            open_pos = sum(1 for st in state.values() if isinstance(st, dict) and st.get("in_pos"))
                    except Exception as e:
                        open_pos = 0
                        print(f"[telegram] status open_pos error: {e}")
                    try:
                        over_limit = "YES" if open_pos > MAX_OPEN_POSITIONS else "NO"
                        status_msg = (
                            "🤖 상태\n"
                            f"/auto_exit(자동청산): {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
                            f"/long_live(롱실주문): {'ON' if LONG_LIVE_TRADING else 'OFF'}\n"
                            f"/live(숏실주문): {'ON' if LIVE_TRADING else 'OFF'}\n"
                            f"/max_pos(동시진입): {MAX_OPEN_POSITIONS}\n"
                            f"/exit_cd_h(재진입시간h): {EXIT_COOLDOWN_HOURS:.2f}h\n"
                            "--------------\n"
                            f"/entry_usdt(진입비율%): {USDT_PER_TRADE:.2f}%\n"
                            f"/dca(추가진입): {'ON' if DCA_ENABLED else 'OFF'} | /dca_pct: {DCA_PCT:.2f}%\n"
                            f"/dca1: {DCA_FIRST_PCT:.2f}% | /dca2: {DCA_SECOND_PCT:.2f}% | /dca3: {DCA_THIRD_PCT:.2f}%\n"
                            f"/l_exit_tp: {_fmt_pct_safe(AUTO_EXIT_LONG_TP_PCT)} | /l_exit_sl: {_fmt_pct_safe(AUTO_EXIT_LONG_SL_PCT)}\n"
                            f"/s_exit_tp: {_fmt_pct_safe(AUTO_EXIT_SHORT_TP_PCT)} | /s_exit_sl: {_fmt_pct_safe(AUTO_EXIT_SHORT_SL_PCT)}\n"
                            "--------------\n"
                            f"/swaggy_atlas_lab(추가진입): {'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'}\n"
                            f"/atlasfabio(추가진입): {'ON' if ATLAS_FABIO_ENABLED else 'OFF'}\n"
                            f"/dtfx(추가진입): {'ON' if DTFX_ENABLED else 'OFF'}\n\n"
                            f"/atlas_rs_fail_short(추가진입): {'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'}\n"
                            f"/pumpfade(추가진입): {'ON' if PUMPFADE_ENABLED else 'OFF'}\n"
                            f"/rsi(추가진입): {'ON' if RSI_ENABLED else 'OFF'}\n\n"
                            f"/div15m_long(추가진입): {'ON' if DIV15M_LONG_ENABLED else 'OFF'}\n"
                            f"/div15m_short(추가진입): {'ON' if DIV15M_SHORT_ENABLED else 'OFF'}\n"
                            "--------------\n"
                            "/report(리포트): /report today|yesterday|YYYY-MM-DD\n"
                            f"open positions: {open_pos} (over_limit={over_limit})"
                        )
                    except Exception as e:
                        print(f"[telegram] status build error: {e}")
                        _log_error(f"[status-build] {e}")
                        status_msg = "🤖 상태\n(status build error)"
                    try:
                        ok = _reply(status_msg)
                    except Exception as e:
                        ok = False
                        print(f"[telegram] status send error: {e}")
                    print(f"[telegram] status cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True
                if (not responded) and ("status" in lower):
                    print(f"[telegram] status cmd fallback: text='{text}'")
                    try:
                        open_pos = count_open_positions(force=True)
                        if not isinstance(open_pos, int):
                            open_pos = sum(1 for st in state.values() if isinstance(st, dict) and st.get("in_pos"))
                    except Exception as e:
                        open_pos = 0
                        print(f"[telegram] status open_pos error: {e}")
                    over_limit = "YES" if open_pos > MAX_OPEN_POSITIONS else "NO"
                    status_msg = (
                        "🤖 상태\n"
                        f"/auto_exit(자동청산): {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
                        f"/long_live(롱실주문): {'ON' if LONG_LIVE_TRADING else 'OFF'}\n"
                        f"/live(숏실주문): {'ON' if LIVE_TRADING else 'OFF'}\n"
                        f"/max_pos(동시진입): {MAX_OPEN_POSITIONS}\n"
                        f"/exit_cd_h(재진입시간h): {EXIT_COOLDOWN_HOURS:.2f}h\n"
                        "--------------\n"
                        f"/entry_usdt(진입비율%): {USDT_PER_TRADE:.2f}%\n"
                        f"/dca(추가진입): {'ON' if DCA_ENABLED else 'OFF'} | /dca_pct: {DCA_PCT:.2f}%\n"
                        f"/dca1: {DCA_FIRST_PCT:.2f}% | /dca2: {DCA_SECOND_PCT:.2f}% | /dca3: {DCA_THIRD_PCT:.2f}%\n"
                        f"/l_exit_tp: {_fmt_pct_safe(AUTO_EXIT_LONG_TP_PCT)} | /l_exit_sl: {_fmt_pct_safe(AUTO_EXIT_LONG_SL_PCT)}\n"
                        f"/s_exit_tp: {_fmt_pct_safe(AUTO_EXIT_SHORT_TP_PCT)} | /s_exit_sl: {_fmt_pct_safe(AUTO_EXIT_SHORT_SL_PCT)}\n"
                        "--------------\n"
                        f"/swaggy_atlas_lab(추가진입): {'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'}\n"
                        f"/atlasfabio(추가진입): {'ON' if ATLAS_FABIO_ENABLED else 'OFF'}\n"
                        f"/dtfx(추가진입): {'ON' if DTFX_ENABLED else 'OFF'}\n\n"
                        f"/atlas_rs_fail_short(추가진입): {'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'}\n"
                        f"/pumpfade(추가진입): {'ON' if PUMPFADE_ENABLED else 'OFF'}\n"
                        f"/rsi(추가진입): {'ON' if RSI_ENABLED else 'OFF'}\n\n"
                        f"/div15m_long(추가진입): {'ON' if DIV15M_LONG_ENABLED else 'OFF'}\n"
                        f"/div15m_short(추가진입): {'ON' if DIV15M_SHORT_ENABLED else 'OFF'}\n"
                        "--------------\n"
                        "/report(리포트): /report today|yesterday|YYYY-MM-DD\n"
                        f"open positions: {open_pos} (over_limit={over_limit})"
                    )
                    ok = _reply(status_msg)
                    print(f"[telegram] status cmd fallback send={'ok' if ok else 'fail'}")
                    responded = True
                if (cmd in ("/l_exit_tp", "l_exit_tp")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            AUTO_EXIT_LONG_TP_PCT = float(val)
                            state["_auto_exit_long_tp_pct"] = AUTO_EXIT_LONG_TP_PCT
                            state_dirty = True
                            resp = f"✅ long TP set: {AUTO_EXIT_LONG_TP_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /l_exit_tp 3 (예: /l_exit_tp 3.5)"
                    else:
                        resp = f"ℹ️ long TP: {AUTO_EXIT_LONG_TP_PCT:.2f}%\n사용법: /l_exit_tp 3"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] l_exit_tp cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/l_exit_sl", "l_exit_sl")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            AUTO_EXIT_LONG_SL_PCT = float(val)
                            state["_auto_exit_long_sl_pct"] = AUTO_EXIT_LONG_SL_PCT
                            state_dirty = True
                            resp = f"✅ long SL set: {AUTO_EXIT_LONG_SL_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /l_exit_sl 3 (예: /l_exit_sl 3.5)"
                    else:
                        resp = f"ℹ️ long SL: {AUTO_EXIT_LONG_SL_PCT:.2f}%\n사용법: /l_exit_sl 3"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] l_exit_sl cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/s_exit_tp", "s_exit_tp")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            AUTO_EXIT_SHORT_TP_PCT = float(val)
                            state["_auto_exit_short_tp_pct"] = AUTO_EXIT_SHORT_TP_PCT
                            state_dirty = True
                            resp = f"✅ short TP set: {AUTO_EXIT_SHORT_TP_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /s_exit_tp 3 (예: /s_exit_tp 3.5)"
                    else:
                        resp = f"ℹ️ short TP: {AUTO_EXIT_SHORT_TP_PCT:.2f}%\n사용법: /s_exit_tp 3"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] s_exit_tp cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/s_exit_sl", "s_exit_sl")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            AUTO_EXIT_SHORT_SL_PCT = float(val)
                            state["_auto_exit_short_sl_pct"] = AUTO_EXIT_SHORT_SL_PCT
                            state_dirty = True
                            resp = f"✅ short SL set: {AUTO_EXIT_SHORT_SL_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /s_exit_sl 3 (예: /s_exit_sl 3.5)"
                    else:
                        resp = f"ℹ️ short SL: {AUTO_EXIT_SHORT_SL_PCT:.2f}%\n사용법: /s_exit_sl 3"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] s_exit_sl cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/max_pos", "max_pos")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ max_pos: {MAX_OPEN_POSITIONS}\n사용법: /max_pos 12"
                    else:
                        try:
                            val = int(float(arg))
                            if val <= 0:
                                raise ValueError("non-positive")
                            MAX_OPEN_POSITIONS = val
                            state["_max_open_positions"] = MAX_OPEN_POSITIONS
                            state_dirty = True
                            resp = f"✅ max_pos set to {MAX_OPEN_POSITIONS}"
                        except Exception:
                            resp = "⛔ 사용법: /max_pos 12"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] max_pos cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/entry_usdt", "entry_usdt")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ entry_usdt: {USDT_PER_TRADE:.2f}%\n사용법: /entry_usdt 3 (사용가능 금액의 3%)"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            USDT_PER_TRADE = float(val)
                            state["_entry_usdt"] = USDT_PER_TRADE
                            state_dirty = True
                            resp = f"✅ entry_usdt set to {USDT_PER_TRADE:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /entry_usdt 3 (사용가능 금액의 3%)"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] entry_usdt cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dca", "dca")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        DCA_ENABLED = True
                        if executor_mod:
                            executor_mod.DCA_ENABLED = True
                        state["_dca_enabled"] = True
                        state_dirty = True
                        resp = "✅ dca ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        DCA_ENABLED = False
                        if executor_mod:
                            executor_mod.DCA_ENABLED = False
                        state["_dca_enabled"] = False
                        state_dirty = True
                        resp = "⛔ dca OFF"
                    else:
                        resp = f"ℹ️ dca 상태: {'ON' if DCA_ENABLED else 'OFF'}\n사용법: /dca on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dca cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dca_pct", "dca_pct")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ dca_pct: {DCA_PCT:.2f}%\n사용법: /dca_pct 2.5"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            DCA_PCT = float(val)
                            if executor_mod:
                                executor_mod.DCA_PCT = DCA_PCT
                            state["_dca_pct"] = DCA_PCT
                            state_dirty = True
                            resp = f"✅ dca_pct set to {DCA_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /dca_pct 2.5"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dca_pct cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dca1", "dca1")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ dca1: {DCA_FIRST_PCT:.2f}%\n사용법: /dca1 30"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            DCA_FIRST_PCT = float(val)
                            if executor_mod:
                                executor_mod.DCA_FIRST_PCT = DCA_FIRST_PCT
                            state["_dca_first_pct"] = DCA_FIRST_PCT
                            state_dirty = True
                            resp = f"✅ dca1 set to {DCA_FIRST_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /dca1 30"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dca1 cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dca2", "dca2")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ dca2: {DCA_SECOND_PCT:.2f}%\n사용법: /dca2 30"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            DCA_SECOND_PCT = float(val)
                            if executor_mod:
                                executor_mod.DCA_SECOND_PCT = DCA_SECOND_PCT
                            state["_dca_second_pct"] = DCA_SECOND_PCT
                            state_dirty = True
                            resp = f"✅ dca2 set to {DCA_SECOND_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /dca2 30"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dca2 cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dca3", "dca3")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ dca3: {DCA_THIRD_PCT:.2f}%\n사용법: /dca3 30"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            DCA_THIRD_PCT = float(val)
                            if executor_mod:
                                executor_mod.DCA_THIRD_PCT = DCA_THIRD_PCT
                            state["_dca_third_pct"] = DCA_THIRD_PCT
                            state_dirty = True
                            resp = f"✅ dca3 set to {DCA_THIRD_PCT:.2f}%"
                        except Exception:
                            resp = "⛔ 사용법: /dca3 30"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dca3 cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/exit_cd_h", "exit_cd_h")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ exit_cd_h: {EXIT_COOLDOWN_HOURS:.2f}h\n사용법: /exit_cd_h 2"
                    else:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            EXIT_COOLDOWN_HOURS = float(val)
                            EXIT_COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
                            state["_exit_cooldown_hours"] = EXIT_COOLDOWN_HOURS
                            state_dirty = True
                            resp = f"✅ exit_cd_h set to {EXIT_COOLDOWN_HOURS:.2f}h"
                        except Exception:
                            resp = "⛔ 사용법: /exit_cd_h 2"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] exit_cd_h cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/long_live", "long_live")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        LONG_LIVE_TRADING = True
                        state["_long_live"] = True
                        state_dirty = True
                        resp = "🚀 long-live ON (롱 실주문)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        LONG_LIVE_TRADING = False
                        state["_long_live"] = False
                        state_dirty = True
                        resp = "🧪 long-live OFF (롱 알림 전용)"
                    else:
                        resp = f"ℹ️ long-live 상태: {'ON' if LONG_LIVE_TRADING else 'OFF'}\n사용법: /long_live on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] long_live cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/atlasfabio", "atlasfabio")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        ATLAS_FABIO_ENABLED = True
                        state["_atlas_fabio_enabled"] = True
                        state_dirty = True
                        resp = "✅ atlasfabio ON (게이트 결합 엔진)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        ATLAS_FABIO_ENABLED = False
                        state["_atlas_fabio_enabled"] = False
                        state_dirty = True
                        resp = "⛔ atlasfabio OFF"
                    else:
                        resp = f"ℹ️ atlasfabio 상태: {'ON' if ATLAS_FABIO_ENABLED else 'OFF'}\n사용법: /atlasfabio on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] atlasfabio cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/swaggy_atlas_lab", "swaggy_atlas_lab")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        SWAGGY_ATLAS_LAB_ENABLED = True
                        state["_swaggy_atlas_lab_enabled"] = True
                        state_dirty = True
                        resp = "✅ swaggy_atlas_lab ON (hard 모드)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        SWAGGY_ATLAS_LAB_ENABLED = False
                        state["_swaggy_atlas_lab_enabled"] = False
                        state_dirty = True
                        resp = "⛔ swaggy_atlas_lab OFF"
                    else:
                        resp = (
                            f"ℹ️ swaggy_atlas_lab 상태: {'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'}\n"
                            "사용법: /swaggy_atlas_lab on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_atlas_lab cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/div15m_long", "div15m_long")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        DIV15M_LONG_ENABLED = True
                        state["_div15m_long_enabled"] = True
                        state_dirty = True
                        resp = "✅ div15m_long ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        DIV15M_LONG_ENABLED = False
                        state["_div15m_long_enabled"] = False
                        state_dirty = True
                        resp = "⛔ div15m_long OFF"
                    else:
                        resp = f"ℹ️ div15m_long 상태: {'ON' if DIV15M_LONG_ENABLED else 'OFF'}\n사용법: /div15m_long on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] div15m_long cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/div15m_short", "div15m_short")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        DIV15M_SHORT_ENABLED = True
                        state["_div15m_short_enabled"] = True
                        state_dirty = True
                        resp = "✅ div15m_short ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        DIV15M_SHORT_ENABLED = False
                        state["_div15m_short_enabled"] = False
                        state_dirty = True
                        resp = "⛔ div15m_short OFF"
                    else:
                        resp = f"ℹ️ div15m_short 상태: {'ON' if DIV15M_SHORT_ENABLED else 'OFF'}\n사용법: /div15m_short on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] div15m_short cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/rsi", "rsi")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        RSI_ENABLED = True
                        state["_rsi_enabled"] = True
                        state_dirty = True
                        resp = "✅ rsi ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        RSI_ENABLED = False
                        state["_rsi_enabled"] = False
                        state_dirty = True
                        resp = "⛔ rsi OFF"
                    else:
                        resp = f"ℹ️ rsi 상태: {'ON' if RSI_ENABLED else 'OFF'}\n사용법: /rsi on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] rsi cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/dtfx", "dtfx")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        DTFX_ENABLED = True
                        state["_dtfx_enabled"] = True
                        state_dirty = True
                        resp = "✅ dtfx ON (DTFX 엔진)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        DTFX_ENABLED = False
                        state["_dtfx_enabled"] = False
                        state_dirty = True
                        resp = "⛔ dtfx OFF"
                    else:
                        resp = f"ℹ️ dtfx 상태: {'ON' if DTFX_ENABLED else 'OFF'}\n사용법: /dtfx on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] dtfx cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/pumpfade", "pumpfade")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        PUMPFADE_ENABLED = True
                        state["_pumpfade_enabled"] = True
                        state_dirty = True
                        resp = "✅ pumpfade ON (PumpFade 엔진)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        PUMPFADE_ENABLED = False
                        state["_pumpfade_enabled"] = False
                        state_dirty = True
                        resp = "⛔ pumpfade OFF"
                    else:
                        resp = f"ℹ️ pumpfade 상태: {'ON' if PUMPFADE_ENABLED else 'OFF'}\n사용법: /pumpfade on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] pumpfade cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/atlas_rs_fail_short", "atlas_rs_fail_short")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        ATLAS_RS_FAIL_SHORT_ENABLED = True
                        state["_atlas_rs_fail_short_enabled"] = True
                        state_dirty = True
                        resp = "✅ atlas_rs_fail_short ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        ATLAS_RS_FAIL_SHORT_ENABLED = False
                        state["_atlas_rs_fail_short_enabled"] = False
                        state_dirty = True
                        resp = "⛔ atlas_rs_fail_short OFF"
                    else:
                        resp = (
                            f"ℹ️ atlas_rs_fail_short 상태: {'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'}\n"
                            "사용법: /atlas_rs_fail_short on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] atlas_rs_fail_short cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/report", "report")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "today"
                    report_date = None
                    range_start = None
                    range_end = None
                    if arg in ("today", "금일", "금일리포트"):
                        report_date = _kst_now().strftime("%Y-%m-%d")
                    elif arg in ("yesterday", "어제"):
                        report_date = (_kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")
                    elif "~" in arg:
                        start_raw, end_raw = [s.strip() for s in arg.split("~", 1)]
                        range_start = start_raw
                        range_end = end_raw
                    elif arg.endswith("d") and arg[:-1].isdigit():
                        days = int(arg[:-1])
                        end_dt = _kst_now().strftime("%Y-%m-%d")
                        start_dt = (_kst_now() - timedelta(days=max(0, days - 1))).strftime("%Y-%m-%d")
                        range_start = start_dt
                        range_end = end_dt
                    else:
                        report_date = arg
                    use_db = bool(dbrec and dbrec.ENABLED and dbpnl)
                    try:
                        if report_date and not use_db:
                            _sync_report_with_api(state, report_date)
                    except Exception as e:
                        print(f"[report-api] sync failed report_date={report_date} err={e}")
                    try:
                        if use_db:
                            if range_start and range_end:
                                rows = _load_db_daily_rows_range(range_start, range_end) or []
                                print(f"[report] range={range_start}~{range_end} db_rows={len(rows)}")
                            else:
                                rows = _load_db_daily_rows(report_date) or []
                                print(f"[report] date={report_date} db_rows={len(rows)}")
                    except Exception:
                        pass
                    try:
                        if range_start and range_end:
                            report_msg = _build_range_report(state, range_start, range_end, compact=True)
                        else:
                            report_msg = _build_daily_report(state, report_date, compact=True)
                    except Exception as e:
                        report_msg = f"⛔ 리포트 생성 실패: {e}"
                    ok = _reply(report_msg)
                    print(f"[telegram] report cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                    responded = True
            except Exception:
                continue
        if new_last_id != last_update_id:
            state["_tg_offset"] = new_last_id
            state_dirty = True
        if state_dirty:
            try:
                state["_runtime_cfg_ts"] = time.time()
                if MANAGE_WS_MODE:
                    _save_runtime_settings_only(state)
                else:
                    save_state(state)
            except Exception:
                pass
    except Exception as e:
        print("[telegram] cmd error:", e)

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _compute_atlas_swaggy_gate(state: Dict[str, Any]) -> Dict[str, Any]:
    if not atlas_engine or not atlas_swaggy_cfg:
        return {}
    return atlas_engine.compute_swaggy_global(state, cycle_cache, atlas_swaggy_cfg)

def _compute_atlas_swaggy_local(symbol: str, decision, gate: Dict[str, Any], swaggy_cfg) -> Dict[str, Any]:
    if not atlas_engine or not atlas_swaggy_cfg:
        return {
            "trade_allowed": gate.get("trade_allowed", 0),
            "allow_long": gate.get("allow_long", 0),
            "allow_short": gate.get("allow_short", 0),
            "long_size_mult": 1.0,
            "short_size_mult": 1.0,
            "exception_long": 0,
            "exception_reason": "",
            "atlas_long_exception": 0,
            "atlas_long_block_reason": "",
            "atlas_short_quality": False,
            "atlas_short_block_reason": "",
            "rs": None,
            "rs_z": None,
            "corr": None,
            "beta": None,
            "vol_ratio": None,
            "state": gate.get("regime"),
        }
    return atlas_engine.compute_swaggy_local(symbol, decision, gate, cycle_cache, atlas_swaggy_cfg, swaggy_cfg)

def _atlasfabio_gate_long(symbol: str, cfg) -> Optional[Dict[str, Any]]:
    if atlas_engine:
        gate = atlas_engine.evaluate_fabio_gate_long(symbol, cfg)
        if gate is not None:
            return gate
    if atlas_fabio_engine:
        return atlas_fabio_engine.evaluate_gate_long(symbol, cfg)
    return None

def _atlasfabio_gate_short(symbol: str, cfg) -> Optional[Dict[str, Any]]:
    if atlas_engine:
        gate = atlas_engine.evaluate_fabio_gate_short(symbol, cfg)
        if gate is not None:
            return gate
    if atlas_fabio_engine:
        return atlas_fabio_engine.evaluate_gate_short(symbol, cfg)
    return None

def _atlasfabio_gate(symbol: str, cfg) -> Optional[Dict[str, Any]]:
    if atlas_engine:
        gate = atlas_engine.evaluate_fabio_gate(symbol, cfg)
        if gate is not None:
            return gate
    if atlas_fabio_engine:
        return atlas_fabio_engine.evaluate_gate(symbol, cfg)
    return None

def _ema_align_ok(symbol: str, tf: str, limit: int = 120) -> bool:
    df = cycle_cache.get_df(symbol, tf, limit)
    if df.empty or len(df) < 70:
        return False
    df = df.iloc[:-1]
    if len(df) < 70:
        return False
    ema20 = ema(df["close"], 20).iloc[-1]
    ema60 = ema(df["close"], 60).iloc[-1]
    if ema60 == 0:
        return False
    dist = abs(ema20 - ema60) / ema60
    return dist >= EMA_ALIGN_DIST_PCT

exchange = ccxt.binance({
    "apiKey": os.getenv("BINANCE_API_KEY", ""),
    "secret": os.getenv("BINANCE_API_SECRET", ""),
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
    "timeout": 30_000,  # 30초로 증가
    "rateLimit": 200,   # 요청 간격 증가
})
def _fetch_ohlcv_with_stats(symbol: str, tf: str, limit: int) -> Optional[list]:
    try:
        data = exchange.fetch_ohlcv(symbol, tf, limit=limit)
        CURRENT_CYCLE_STATS["rest_calls"] = CURRENT_CYCLE_STATS.get("rest_calls", 0) + 1
        return data
    except Exception as e:
        msg = str(e)
        CURRENT_CYCLE_STATS["rest_fails"] = CURRENT_CYCLE_STATS.get("rest_fails", 0) + 1
        if ("429" in msg) or ("-1003" in msg):
            CURRENT_CYCLE_STATS["rest_429"] = CURRENT_CYCLE_STATS.get("rest_429", 0) + 1
        print(f"[에러] {symbol} {tf} 데이터 가져오기 실패: {e}")
        return None

cycle_cache.set_fetcher(_fetch_ohlcv_with_stats)

ATLASFABIO_LTF_RSI_LONG_MIN = 50.0
ATLASFABIO_MID_RETEST_REASON = "atlas_mid_retest_bypass"
FABIO_LONG_IMPULSE_ATR_FACTOR = 1.2
FABIO_LONG_IMPULSE_ATR_PERIOD = 14
FABIO_LONG_BB_PERIOD = 20
FABIO_LONG_DIST_MAX = 0.8
FABIO_RSI_OVERHEAT = 68.0

USDT_PER_TRADE = 30.0  # 사용가능 USDT 대비 퍼센트
LEVERAGE = 10
MARGIN_MODE = "cross"
MAX_OPEN_POSITIONS = 12
AUTO_EXIT_ENABLED = False  # True이면 TP/SL 기준으로 자동 청산 (기본 OFF)
AUTO_EXIT_LONG_TP_PCT = 3.0
AUTO_EXIT_LONG_SL_PCT = 3.0
AUTO_EXIT_SHORT_TP_PCT = 3.0
AUTO_EXIT_SHORT_SL_PCT = 3.0
LIVE_TRADING = True  # True이면 신호 발생 시 실제 주문 실행

COOLDOWN_SEC = 3600
PER_SYMBOL_SLEEP = 0.12
CYCLE_SLEEP = 10.0
SAME_CYCLE_SLEEP = 10.0
SHORT_RECONCILE_GRACE_SEC = 45.0
SHORT_RECONCILE_EPS = 0.0005
SHORT_RECONCILE_ZERO_STREAK_N = 2
SHORT_RECONCILE_SEEN_TTL_SEC = 3600  # 최근 숏 포지션 실제 존재 TTL

# 옵션 최적화 설정
STRUCTURE_TOP_N: Optional[int] = 20  # 구조용 상위 N개 (거래대금 기준)
EMA_ALIGN_DIST_PCT = 0.003
FABIO_EARLY_ALERT_COOLDOWN_SEC = 900
FABIO_UNIVERSE_TOP_N = 30  # 파비오 엔진: top N (조건별 로테이션)
FABIO_GAIN_TOP_N = 15
FABIO_LOSS_TOP_N = 15
ATLASFABIO_DIST_MAX = 0.03
ATLASFABIO_PULLBACK_VOL_MAX = 1.5
ATLASFABIO_RETEST_TOUCH_TOL = 0.0020
ATLASFABIO_STRONG_SCORE = 85
ATLASFABIO_MID_SCORE = 70
ATLASFABIO_STRONG_DIST_MAX = 0.045
ATLASFABIO_MID_DIST_MAX = 0.008
ATLASFABIO_MID_PULLBACK_VOL_MAX = 1.5
ATLASFABIO_MID_RETEST_TOUCH_TOL = 0.0035
ATLASFABIO_MID_VOL_MULT = 1.05
ATLASFABIO_MIN_STRENGTH = 0.55
ATLASFABIO_MIN_STRENGTH_SOFT = 0.70
MANAGE_EVAL_COOLDOWN_SEC: int = 20  # manage 모드 평가 주기 쿨다운(초) → fetch_positions 빈도 완화
MANAGE_EXIT_COOLDOWN_SEC: int = 5  # auto-exit 전용 최소 평가 주기(초)
MANAGE_PING_COOLDOWN_SEC: int = 7200  # manage 알림 주기(초) - 2시간
MANUAL_CLOSE_GRACE_SEC: int = 60  # 진입 직후 포지션 캐시 오차로 인한 오탐 방지
AUTO_EXIT_GRACE_SEC: int = 30     # 진입 직후 자동청산 금지 구간
EXIT_COOLDOWN_HOURS: float = 2.0  # 청산 후 재진입 쿨다운(시간)
EXIT_COOLDOWN_SEC: int = int(EXIT_COOLDOWN_HOURS * 3600)  # 청산 후 재진입 쿨다운(초)
MANAGE_LOOP_ENABLED: bool = True  # 관리 루프 분리 실행 여부
MANAGE_LOOP_SLEEP_SEC: float = 2.0  # 관리 루프 주기(초)
MANAGE_TICKER_TTL_SEC: float = 5.0  # 관리 루프 티커 캐시 TTL(초)
RUNTIME_CONFIG_RELOAD_SEC: float = 5.0  # 런타임 설정 변경사항 반영 주기
MANAGE_WS_MODE: bool = False  # WS 관리 모듈 사용 시 메인 리컨실/관리 비활성
SUPPRESS_RECONCILE_ALERTS: bool = True  # 리컨실 알림 억제용(기본 ON)
REPORT_API_ONLY: bool = True  # 리포트는 API sync 결과만 사용
DB_RECONCILE_ENABLED: bool = os.getenv("DB_RECONCILE_ENABLED", "0") == "1"
DB_RECONCILE_SEC: float = float(os.getenv("DB_RECONCILE_SEC", "30"))
DB_RECONCILE_LOOKBACK_SEC: float = float(os.getenv("DB_RECONCILE_LOOKBACK_SEC", "3600"))
DB_RECONCILE_SYMBOLS_RAW = os.getenv("DB_RECONCILE_SYMBOLS", "").strip()
DB_REPORT_LOOKBACK_SEC: float = float(os.getenv("DB_REPORT_LOOKBACK_SEC", "259200"))
SHORT_POS_SAMPLE_DIV: int = 20  # 1/N 샘플링
SHORT_POS_SAMPLE_RECENT_SEC: int = 120  # 최근 진입 심볼은 항상 샘플링

FAST_TF_PREFETCH_TOPN = 30
MAX_FAST_SYMBOLS = 30
FAST_LIMIT_CAP = 120
MID_LIMIT_CAP = 200
SLOW_LIMIT_CAP = 300
MID_TF_PREFETCH_EVERY_N = 3
TTL_4H_SEC = 1800
TTL_1D_SEC = 86400

# LONG signal control (used by Fabio/AtlasFabio)
LONG_LIVE_TRADING = True
ATLAS_FABIO_ENABLED = True
ATLAS_FABIO_PAPER = False
SWAGGY_ENABLED = False
SWAGGY_ATLAS_LAB_ENABLED = False
DTFX_ENABLED = True
PUMPFADE_ENABLED = False
ATLAS_RS_FAIL_SHORT_ENABLED = False
DIV15M_LONG_ENABLED = True
DIV15M_SHORT_ENABLED = True
ONLY_DIV15M_SHORT = False
RSI_ENABLED = True

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
STATE_SAVE_LOCK = threading.Lock()

# 청산 보수성 옵션
REQUIRE_CLOSE_ABOVE_FOR_EXIT = True  # True이면 wick 터치만으로는 청산하지 않고 종가가 EMA20 이상이어야 함

# 사이클 단위 데이터 캐시 (symbol, timeframe, limit) -> DataFrame
CURRENT_CYCLE_STATS: Dict[str, dict] = {}
# funding rate TTL 캐시: symbol -> (ts, rate, interval_hours)
FUNDING_TTL_CACHE: Dict[str, tuple] = {}

# 영속(사이클 간) OHLCV TTL 캐시: (symbol, tf, limit) -> (ts, df)
TF_TTL_SECS = {"3m": 60, "5m": 120, "15m": 240, "1h": 300}
PERSISTENT_OHLCV_CACHE: Dict[tuple, tuple] = {}

# 429/-1003 백오프 제어
GLOBAL_BACKOFF_UNTIL: float = 0.0
_BACKOFF_SECS: float = 0.0
# rate-limit 로그 쿨다운
RATE_LIMIT_LOG_TS: float = 0.0
TOTAL_CYCLES: int = 0
TOTAL_ELAPSED: float = 0.0
TOTAL_REST_CALLS: int = 0
TOTAL_429_COUNT: int = 0

FUNDING_INTERVAL_HOURS = 1
FUNDING_BLOCK_PCT = -0.2
FUNDING_TTL_SEC = 300

def prune_ohlcv_cache():
    """TTL 지난 OHLCV 캐시 청소(메모리 보호). 여유 3배를 준다."""
    try:
        now = time.time()
        to_del = []
        for (sym, tf, limit), (ts, _) in list(PERSISTENT_OHLCV_CACHE.items()):
            ttl = TF_TTL_SECS.get(tf, 60) * 3
            if (now - ts) > ttl:
                to_del.append((sym, tf, limit))
        for k in to_del:
            PERSISTENT_OHLCV_CACHE.pop(k, None)
        if to_del:
            CURRENT_CYCLE_STATS.setdefault("cache_pruned", 0)
            CURRENT_CYCLE_STATS["cache_pruned"] += len(to_del)
    except Exception:
        pass

def _prefetch_ohlcv_for_cycle(
    symbols: List[str],
    ex,
    plan: Dict[str, int],
    label: str = "common",
    ttl_by_tf: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    if not symbols or not plan:
        return {"symbols": len(symbols), "tfs": list(plan.keys()), "fetched": 0, "failed": 0, "fresh_hits": {}}
    fetched = 0
    failed = 0
    failed_429 = 0
    fresh_hits: Dict[str, int] = {}
    fetched_by_tf: Dict[str, int] = {}
    ttl_by_tf = ttl_by_tf or {}
    t0 = time.time()
    for sym in symbols:
        for tf, limit in plan.items():
            key = (sym, tf)
            cached_data = cycle_cache.get_raw(sym, tf)
            if cached_data is not None and len(cached_data) >= limit:
                continue
            ttl = ttl_by_tf.get(tf)
            if ttl and cycle_cache.is_fresh(sym, tf, ttl):
                fresh_hits[tf] = fresh_hits.get(tf, 0) + 1
                continue
            try:
                data = ex.fetch_ohlcv(sym, tf, limit=limit)
                cycle_cache.set_raw(sym, tf, data)
                fetched += 1
                fetched_by_tf[tf] = fetched_by_tf.get(tf, 0) + 1
            except Exception as e:
                failed += 1
                msg = str(e)
                if ("429" in msg) or ("-1003" in msg):
                    failed_429 += 1
                print(f"[에러] {sym} {tf} 데이터 가져오기 실패: {e}")
    if fetched > 0:
        elapsed = time.time() - t0
        print(f"[prefetch] {label} fetched={fetched} failed={failed} elapsed={elapsed:.2f}s")
    CURRENT_CYCLE_STATS["rest_calls"] = CURRENT_CYCLE_STATS.get("rest_calls", 0) + fetched
    CURRENT_CYCLE_STATS["rest_fails"] = CURRENT_CYCLE_STATS.get("rest_fails", 0) + failed
    CURRENT_CYCLE_STATS["rest_429"] = CURRENT_CYCLE_STATS.get("rest_429", 0) + failed_429
    return {
        "symbols": len(symbols),
        "tfs": list(plan.keys()),
        "fetched": fetched,
        "failed": failed,
        "fresh_hits": fresh_hits,
        "fetched_by_tf": fetched_by_tf,
    }

def _atlasfabio_ltf_hit(symbol: str, side: str, mode: str = "5m_or_3m") -> tuple[bool, bool, bool]:
    if not rsi_engine:
        return False, False, False
    cfg = rsi_engine.config
    side = (side or "").upper()
    if side == "SHORT":
        r5 = rsi_engine.fetch_rsi(symbol, "5m")
        hit5m = (r5 is not None) and (r5 >= cfg.thresholds["5m"])
        r3, r2, r1 = rsi_engine.fetch_rsi_last3(symbol, "3m")
        hit3m = False
        if r2 is not None and r1 is not None and r2 >= cfg.thresholds["3m"] and r2 > r1:
            hit3m = True
        if r3 is not None and r2 is not None and r3 >= cfg.thresholds["3m"] and r3 > r2:
            hit3m = True
        ltf_ok = hit3m if mode == "3m_only" else (hit5m or hit3m)
        return hit5m, hit3m, ltf_ok

    r5_prev, r5_last = rsi_engine.fetch_rsi_last2(symbol, "5m")
    hit5m = False
    if (
        r5_prev is not None
        and r5_last is not None
        and r5_last >= ATLASFABIO_LTF_RSI_LONG_MIN
        and r5_last > r5_prev
    ):
        hit5m = True
    r3, r2, r1 = rsi_engine.fetch_rsi_last3(symbol, "3m")
    hit3m = False
    if r2 is not None and r1 is not None and r2 >= ATLASFABIO_LTF_RSI_LONG_MIN and r1 > r2:
        hit3m = True
    if r3 is not None and r2 is not None and r3 >= ATLASFABIO_LTF_RSI_LONG_MIN and r2 > r3:
        hit3m = True
    ltf_ok = hit3m if mode == "3m_only" else (hit5m or hit3m)
    return hit5m, hit3m, ltf_ok

def _parse_funding_interval_hours(info: dict) -> Optional[float]:
    for key in ("fundingIntervalHours", "fundingInterval", "intervalHours", "interval"):
        if key in info:
            try:
                val = float(info.get(key))
                if val > 0:
                    return val
            except Exception:
                pass
    return None

def fetch_funding_rate(symbol: str) -> (Optional[float], Optional[float]):
    now = time.time()
    cached = FUNDING_TTL_CACHE.get(symbol)
    if cached and (now - cached[0]) <= FUNDING_TTL_SEC:
        return cached[1], cached[2]
    try:
        data = exchange.fetch_funding_rate(symbol)
    except Exception:
        return None, None
    rate = None
    interval_hours = None
    try:
        rate = data.get("fundingRate")
        info = data.get("info") or {}
        interval_hours = _parse_funding_interval_hours(info)
        if interval_hours is None:
            interval_hours = _parse_funding_interval_hours(data or {})
        if rate is not None:
            rate = float(rate)
    except Exception:
        rate = None
    FUNDING_TTL_CACHE[symbol] = (now, rate, interval_hours)
    return rate, interval_hours

def get_symbols() -> List[str]:
    for i in range(3):
        try:
            markets = exchange.load_markets()
            break
        except Exception as e:
            print(f"[에러] load_markets 실패 시도 {i+1}: {e}")
            time.sleep(3)
    else:
        raise RuntimeError("load_markets 실패(재시도 초과)")
    symbols = []
    for m in markets.values():
        if m.get("swap") and m.get("linear") and m.get("settle") == "USDT" and m.get("active", True):
            symbols.append(m["symbol"])
    return sorted(list(set(symbols)))

def load_state() -> Dict[str, dict]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, dict]) -> None:
    disk = None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            disk = json.load(f)
    except Exception:
        disk = None
    if isinstance(disk, dict):
        disk_ts = disk.get("_runtime_cfg_ts")
        state_ts = state.get("_runtime_cfg_ts")
        if isinstance(disk_ts, (int, float)) and (not isinstance(state_ts, (int, float)) or disk_ts > state_ts):
            runtime_keys = [
                "_auto_exit",
                "_auto_exit_long_tp_pct",
                "_auto_exit_long_sl_pct",
                "_auto_exit_short_tp_pct",
                "_auto_exit_short_sl_pct",
                "_live_trading",
                "_long_live",
                "_max_open_positions",
                "_entry_usdt",
                "_dca_enabled",
                "_dca_pct",
                "_dca_first_pct",
                "_dca_second_pct",
                "_dca_third_pct",
                "_exit_cooldown_hours",
                "_atlas_fabio_enabled",
                "_swaggy_enabled",
                "_swaggy_atlas_lab_enabled",
                "_dtfx_enabled",
                "_pumpfade_enabled",
                "_div15m_long_enabled",
                "_div15m_short_enabled",
                "_rsi_enabled",
                "_runtime_cfg_ts",
            ]
            for key in runtime_keys:
                if key in disk:
                    state[key] = disk.get(key)
    state_snapshot = None
    for _ in range(3):
        try:
            state_snapshot = copy.deepcopy(state)
            break
        except RuntimeError:
            time.sleep(0.01)
    if state_snapshot is None:
        state_snapshot = dict(state)
    base_dir = os.path.dirname(STATE_FILE) or "."
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = f"{STATE_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    with STATE_SAVE_LOCK:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state_snapshot, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_FILE)

def run():
    # 전역 백오프 변수는 run 스코프에서 재할당 하므로 선 선언 필요
    global GLOBAL_BACKOFF_UNTIL, _BACKOFF_SECS, RATE_LIMIT_LOG_TS
    global TOTAL_CYCLES, TOTAL_ELAPSED, TOTAL_REST_CALLS, TOTAL_429_COUNT
    global MANAGE_LOOP_ENABLED, MANAGE_WS_MODE
    _install_error_hooks()
    print("[시작] RSI 스캐너 초기화 중...")
    symbols = get_symbols()
    print(f"[초기화] {len(symbols)}개 심볼 로드됨")
    state = load_state()
    print(f"[초기화] 상태 파일 로드: {len(state)}개 심볼")
    state["_symbols"] = symbols
    global swaggy_engine, swaggy_atlas_lab_engine, atlas_engine, atlas_swaggy_cfg, dtfx_engine, pumpfade_engine, div15m_engine, div15m_short_engine, atlas_rs_fail_short_engine
    swaggy_engine = SwaggyEngine() if SwaggyEngine else None
    swaggy_atlas_lab_engine = SwaggyAtlasLabEngine() if SwaggyAtlasLabEngine else None
    atlas_engine = AtlasEngine() if AtlasEngine else None
    atlas_swaggy_cfg = AtlasSwaggyConfig() if AtlasSwaggyConfig else None
    global rsi_engine
    rsi_engine = RsiEngine() if RsiEngine else None
    _maybe_reload_rsi_config()
    cfg_defaults = _load_rsi_config_defaults()
    if rsi_engine and cfg_defaults:
        rsi_engine.config = cfg_defaults
    div15m_engine = Div15mLongEngine() if Div15mLongEngine else None
    div15m_short_engine = Div15mShortEngine() if Div15mShortEngine else None
    dtfx_engine = DTFXEngine() if DTFXEngine else None
    dtfx_cfg = DTFXConfig() if DTFXConfig else None
    pumpfade_engine = PumpFadeEngine() if PumpFadeEngine else None
    pumpfade_cfg = PumpFadeConfig() if PumpFadeConfig else None
    atlas_rs_fail_short_engine = AtlasRsFailShortEngine() if AtlasRsFailShortEngine else None
    atlas_rs_fail_short_cfg = AtlasRsFailShortConfig() if AtlasRsFailShortConfig else None
    if dtfx_engine and dtfx_cfg and EngineContext:
        try:
            dtfx_engine.on_start(
                EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=print, config=dtfx_cfg)
            )
        except Exception:
            pass
    # state에 저장된 설정 복원 (없으면 기본값 사용)
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT, AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, ATLAS_FABIO_ENABLED, SWAGGY_ATLAS_LAB_ENABLED, DTFX_ENABLED, PUMPFADE_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, DIV15M_LONG_ENABLED, DIV15M_SHORT_ENABLED, ONLY_DIV15M_SHORT, RSI_ENABLED
    global USDT_PER_TRADE, DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC
    # 서버 재시작 시 auto_exit는 마지막 상태를 유지
    AUTO_EXIT_ENABLED = bool(state.get("_auto_exit", AUTO_EXIT_ENABLED))
    state["_auto_exit"] = AUTO_EXIT_ENABLED
    # state.json 우선, 없으면 기본값(3.0)
    AUTO_EXIT_LONG_TP_PCT = float(state.get("_auto_exit_long_tp_pct", 3.0))
    AUTO_EXIT_LONG_SL_PCT = float(state.get("_auto_exit_long_sl_pct", 3.0))
    AUTO_EXIT_SHORT_TP_PCT = float(state.get("_auto_exit_short_tp_pct", 3.0))
    AUTO_EXIT_SHORT_SL_PCT = float(state.get("_auto_exit_short_sl_pct", 3.0))
    state["_auto_exit_long_tp_pct"] = AUTO_EXIT_LONG_TP_PCT
    state["_auto_exit_long_sl_pct"] = AUTO_EXIT_LONG_SL_PCT
    state["_auto_exit_short_tp_pct"] = AUTO_EXIT_SHORT_TP_PCT
    state["_auto_exit_short_sl_pct"] = AUTO_EXIT_SHORT_SL_PCT
    if isinstance(state.get("_long_live"), bool):
        LONG_LIVE_TRADING = bool(state.get("_long_live"))
    else:
        state["_long_live"] = LONG_LIVE_TRADING
    if isinstance(state.get("_live_trading"), bool):
        LIVE_TRADING = bool(state.get("_live_trading"))
    if isinstance(state.get("_div15m_long_enabled"), bool):
        DIV15M_LONG_ENABLED = bool(state.get("_div15m_long_enabled"))
    else:
        state["_div15m_long_enabled"] = DIV15M_LONG_ENABLED
    if isinstance(state.get("_div15m_short_enabled"), bool):
        DIV15M_SHORT_ENABLED = bool(state.get("_div15m_short_enabled"))
    else:
        state["_div15m_short_enabled"] = DIV15M_SHORT_ENABLED
    if isinstance(state.get("_dca_enabled"), bool):
        DCA_ENABLED = bool(state.get("_dca_enabled"))
        if executor_mod:
            executor_mod.DCA_ENABLED = DCA_ENABLED
    else:
        state["_dca_enabled"] = DCA_ENABLED
    if isinstance(state.get("_dca_pct"), (int, float)):
        DCA_PCT = float(state.get("_dca_pct"))
        if executor_mod:
            executor_mod.DCA_PCT = DCA_PCT
    else:
        state["_dca_pct"] = DCA_PCT
    if isinstance(state.get("_dca_first_pct"), (int, float)):
        DCA_FIRST_PCT = float(state.get("_dca_first_pct"))
        if executor_mod:
            executor_mod.DCA_FIRST_PCT = DCA_FIRST_PCT
    else:
        state["_dca_first_pct"] = DCA_FIRST_PCT
    if isinstance(state.get("_dca_second_pct"), (int, float)):
        DCA_SECOND_PCT = float(state.get("_dca_second_pct"))
        if executor_mod:
            executor_mod.DCA_SECOND_PCT = DCA_SECOND_PCT
    else:
        state["_dca_second_pct"] = DCA_SECOND_PCT
    if isinstance(state.get("_dca_third_pct"), (int, float)):
        DCA_THIRD_PCT = float(state.get("_dca_third_pct"))
        if executor_mod:
            executor_mod.DCA_THIRD_PCT = DCA_THIRD_PCT
    else:
        state["_dca_third_pct"] = DCA_THIRD_PCT
    if isinstance(state.get("_exit_cooldown_hours"), (int, float)):
        EXIT_COOLDOWN_HOURS = float(state.get("_exit_cooldown_hours"))
        EXIT_COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
    else:
        state["_exit_cooldown_hours"] = EXIT_COOLDOWN_HOURS
    if isinstance(state.get("_rsi_enabled"), bool):
        RSI_ENABLED = bool(state.get("_rsi_enabled"))
    else:
        state["_rsi_enabled"] = RSI_ENABLED
    if isinstance(state.get("_atlas_fabio_enabled"), bool):
        ATLAS_FABIO_ENABLED = bool(state.get("_atlas_fabio_enabled"))
    else:
        state["_atlas_fabio_enabled"] = ATLAS_FABIO_ENABLED
    if isinstance(state.get("_swaggy_atlas_lab_enabled"), bool):
        SWAGGY_ATLAS_LAB_ENABLED = bool(state.get("_swaggy_atlas_lab_enabled"))
    else:
        state["_swaggy_atlas_lab_enabled"] = SWAGGY_ATLAS_LAB_ENABLED
    if isinstance(state.get("_dtfx_enabled"), bool):
        DTFX_ENABLED = bool(state.get("_dtfx_enabled"))
    else:
        state["_dtfx_enabled"] = DTFX_ENABLED
    if isinstance(state.get("_pumpfade_enabled"), bool):
        PUMPFADE_ENABLED = bool(state.get("_pumpfade_enabled"))
    else:
        state["_pumpfade_enabled"] = PUMPFADE_ENABLED
    if isinstance(state.get("_atlas_rs_fail_short_enabled"), bool):
        ATLAS_RS_FAIL_SHORT_ENABLED = bool(state.get("_atlas_rs_fail_short_enabled"))
    else:
        state["_atlas_rs_fail_short_enabled"] = ATLAS_RS_FAIL_SHORT_ENABLED
    if "--only-div15m-short" in sys.argv:
        ONLY_DIV15M_SHORT = True
        DIV15M_LONG_ENABLED = False
        DIV15M_SHORT_ENABLED = True
        RSI_ENABLED = False
        DTFX_ENABLED = False
        PUMPFADE_ENABLED = False
        ATLAS_RS_FAIL_SHORT_ENABLED = False
        ATLAS_FABIO_ENABLED = False
        state["_div15m_long_enabled"] = False
        state["_div15m_short_enabled"] = True
        state["_rsi_enabled"] = False
        state["_dtfx_enabled"] = False
        state["_pumpfade_enabled"] = False
        state["_atlas_rs_fail_short_enabled"] = False
        state["_atlas_fabio_enabled"] = False
        print("[모드] ONLY_DIV15M_SHORT 활성화: div15m_short만 스캔")
    if isinstance(state.get("_entry_usdt"), (int, float)):
        try:
            val = float(state.get("_entry_usdt"))
            if val > 0:
                USDT_PER_TRADE = val
        except Exception:
            pass
    else:
        state["_entry_usdt"] = USDT_PER_TRADE
    if isinstance(state.get("_max_open_positions"), (int, float)):
        try:
            val = int(state.get("_max_open_positions"))
            if val > 0:
                MAX_OPEN_POSITIONS = val
        except Exception:
            pass
    else:
        state["_max_open_positions"] = MAX_OPEN_POSITIONS
    if isinstance(state.get("_manage_ws_mode"), bool):
        MANAGE_WS_MODE = bool(state.get("_manage_ws_mode"))
    else:
        state["_manage_ws_mode"] = MANAGE_WS_MODE
    if "--no-manage-loop" in sys.argv:
        MANAGE_LOOP_ENABLED = False
    try:
        set_dry_run(not LIVE_TRADING)
    except Exception:
        pass
    # WebSocket 시작 (관리 대상만)
    ws_on = False
    if ws_manager and ws_manager.is_available():
        try:
            ws_on = ws_manager.start()
            print(f"[WS] manager started: {ws_on}")
        except Exception as e:
            print("[WS] start failed:", e)
    # runtime chat id 복원
    try:
        if state.get("_chat_id"):
            global CHAT_ID_RUNTIME
            CHAT_ID_RUNTIME = str(state.get("_chat_id"))
            print(f"[telegram] runtime CHAT_ID set to {CHAT_ID_RUNTIME}")
    except Exception:
        pass
    send_telegram(
        "✅ RSI 스캐너 시작\n"
        f"auto-exit: {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
        f"live-trading: {'ON' if LIVE_TRADING else 'OFF'}\n"
        "명령: /auto_exit on|off|status, /l_exit_tp n, /l_exit_sl n, /s_exit_tp n, /s_exit_sl n, /live on|off|status, /long_live on|off|status, /entry_usdt pct, /dca on|off|status, /dca_pct n, /dca1 n, /dca2 n, /dca3 n, /exit_cd_h n, /atlasfabio on|off|status, /swaggy_atlas_lab on|off|status, /div15m_long on|off|status, /div15m_short on|off|status, /rsi on|off|status, /dtfx on|off|status, /pumpfade on|off|status, /atlas_rs_fail_short on|off|status, /max_pos n, /report today|yesterday, /status"
    )
    print("[시작] 메인 루프 시작")
    manage_thread = None
    if MANAGE_WS_MODE:
        MANAGE_LOOP_ENABLED = False
        print("[manage] ws mode active: skip manage-loop in main")
    if MANAGE_LOOP_ENABLED:
        manage_cached_long_ex = CachedExchange(exchange)
        manage_thread = threading.Thread(
            target=_manage_loop_worker,
            args=(state, exchange, manage_cached_long_ex, send_telegram),
            daemon=True,
        )
        manage_thread.start()

    cycle_count = 0
    realtime_count = 0
    last_cfg_reload_ts = 0.0
    while True:
        now = time.time()  # manage 모드 등 선행 로직에서 사용
        # 전역 백오프(OHLCV/티커) + executor 백오프 통합 체크
        exec_backoff = 0.0
        try:
            exec_backoff = float(get_global_backoff_until() or 0.0)
        except Exception:
            exec_backoff = 0.0
        combined_backoff = max(float(GLOBAL_BACKOFF_UNTIL or 0.0), exec_backoff)
        if now < combined_backoff:
            sleep_sec = max(0.0, combined_backoff - now)
            print(f"[rate-limit] global backoff active: sleep {sleep_sec:.1f}s")
            time.sleep(sleep_sec)
            continue
        if (now - last_cfg_reload_ts) >= RUNTIME_CONFIG_RELOAD_SEC:
            _reload_runtime_settings_from_disk(state)
            last_cfg_reload_ts = now
        try:
            set_dry_run(not LIVE_TRADING)
        except Exception:
            pass

        # cycle ts debug (BTC 15m, prev candle only)
        cycle_ts = None
        last_open_ts = None
        prev_open_ts = None
        server_ms = None
        try:
            server_ms = int(exchange.milliseconds())
        except Exception:
            server_ms = int(time.time() * 1000)
        try:
            ohlcv = exchange.fetch_ohlcv("BTC/USDT:USDT", "15m", limit=3)
            if ohlcv and len(ohlcv) >= 2:
                last_open_ts = int(ohlcv[-1][0])
                prev_open_ts = int(ohlcv[-2][0])
                cycle_ts = prev_open_ts
        except Exception:
            cycle_ts = None
        last_cycle_raw = state.get("_last_cycle_ts", 0)
        try:
            last_cycle_ts = int(float(last_cycle_raw))
        except Exception:
            last_cycle_ts = 0
            state["_last_cycle_ts"] = 0
        state["_current_cycle_ts"] = cycle_ts
        def _fmt_ms_kst(ms: Optional[int]) -> str:
            if not ms:
                return "N/A"
            return _ts_to_kst_str(ms / 1000.0)
        server_kst = _fmt_ms_kst(server_ms)
        last_open_kst = _fmt_ms_kst(last_open_ts)
        prev_open_kst = _fmt_ms_kst(prev_open_ts)
        cycle_kst = _fmt_ms_kst(cycle_ts)
        print(
            f"[CYCLE-TS] server={server_kst} last_open={last_open_kst} "
            f"prev_open={prev_open_kst} cycle_ts={cycle_kst}"
        )
        print(f"[CYCLE] cycle_ts={cycle_kst} last_cycle_ts={_fmt_ms_kst(last_cycle_ts)}")
        heavy_scan = bool(cycle_ts and cycle_ts != last_cycle_ts)
        if not heavy_scan:
            print(f"[CYCLE] same cycle_ts={cycle_kst} -> realtime only")

        # 사이클 캐시/통계 초기화
        try:
            cycle_cache.clear_cycle_cache(keep_raw=True)
        except Exception:
            pass
        try:
            cycle_cache.drop_raw_by_tf(["3m", "5m"])
            if heavy_scan and (cycle_count + 1) % MID_TF_PREFETCH_EVERY_N == 0:
                cycle_cache.drop_raw_by_tf(["15m"])
        except Exception:
            pass
        try:
            CURRENT_CYCLE_STATS.clear()
        except Exception:
            pass
        if rsi_engine:
            rsi_engine.set_cycle_stats(CURRENT_CYCLE_STATS)
        if heavy_scan:
            cycle_count += 1
            if cycle_ts and cycle_ts > last_cycle_ts:
                state["_last_cycle_ts"] = cycle_ts
            cycle_label = cycle_kst if cycle_kst != "N/A" else str(cycle_count)
            print(f"\n[사이클 {cycle_label}] 시작 (heavy_scan=Y)")
        else:
            realtime_count += 1
            base_cycle = int(state.get("_last_cycle_ts", 0)) or cycle_ts or 0
            base_label = _fmt_ms_kst(base_cycle)
            if base_label == "N/A":
                base_label = str(cycle_count)
            cycle_label = f"{base_label}-RT{realtime_count}"
            print(f"\n[사이클 {cycle_label}] (동일 캔들) realtime only (heavy_scan=N)")
        cycle_start = time.time()
        run_rsi_short = bool(RSI_ENABLED and (not ONLY_DIV15M_SHORT))
        run_div15m_long = bool((not ONLY_DIV15M_SHORT) and DIV15M_LONG_ENABLED and div15m_engine)
        run_div15m_short = bool(div15m_short_engine and (DIV15M_SHORT_ENABLED or ONLY_DIV15M_SHORT))
        # 기존 Manage/SYNC는 Universe 생성 후로 이동하여 universe 심볼도 포함

        # --- Universe build ---
        try:
            tickers = exchange.fetch_tickers()
            state["_tickers"] = tickers
            state["_tickers_ts"] = time.time()
            # 성공 시 백오프 완화
            if _BACKOFF_SECS > 0:
                _BACKOFF_SECS = max(0.0, _BACKOFF_SECS - 1.0)
        except ccxt.RequestTimeout:
            print("[에러] 타임아웃 - 3초 대기")
            time.sleep(3)
            continue
        except ccxt.NetworkError as e:
            print(f"[에러] 네트워크 오류: {e}")
            # 네트워크 오류 중 레이트리밋 케이스 백오프
            if ("429" in str(e)) or ("-1003" in str(e)):
                _BACKOFF_SECS = 5.0 if _BACKOFF_SECS <= 0 else min(_BACKOFF_SECS * 1.5, 30.0)
                GLOBAL_BACKOFF_UNTIL = time.time() + _BACKOFF_SECS
                print(f"[rate-limit] tickers 백오프 {_BACKOFF_SECS:.1f}s 적용")
            time.sleep(5)
            continue
        except Exception as e:
            msg = str(e)
            # 일반 예외에서도 레이트리밋 메시지면 백오프
            if ("429" in msg) or ("-1003" in msg):
                _BACKOFF_SECS = 5.0 if _BACKOFF_SECS <= 0 else min(_BACKOFF_SECS * 1.5, 30.0)
                GLOBAL_BACKOFF_UNTIL = time.time() + _BACKOFF_SECS
                print(f"[rate-limit] tickers 백오프 {_BACKOFF_SECS:.1f}s 적용 (Exception)")
                time.sleep(5)
                continue
            print(f"[에러] 기타 오류: {e}")
            time.sleep(10)
            continue

        universe_momentum = []
        universe_structure = []
        pct_all_map = {}
        qv_all_map = {}
        qv_map = {}
        anchors = ("BTC/USDT:USDT", "ETH/USDT:USDT")

        _maybe_reload_rsi_config()
        cfg_defaults = _load_rsi_config_defaults()
        if rsi_engine and cfg_defaults:
            rsi_engine.config = cfg_defaults
        rsi_cfg = rsi_engine.config if rsi_engine else cfg_defaults
        global _RSI_CONFIG_LOGGED
        cfg_vals = _read_rsi_config_values()
        if not _RSI_CONFIG_LOGGED:
            cfg_path = os.path.join(os.path.dirname(__file__), "engines", "rsi", "config.py")
            print(f"[config] rsi_config path={cfg_path} parsed={cfg_vals} engine={__file__}")
            _RSI_CONFIG_LOGGED = True
        shared_min_qv = cfg_vals.get("min_quote_volume_usdt")
        if not isinstance(shared_min_qv, (int, float)):
            shared_min_qv = rsi_cfg.min_quote_volume_usdt if rsi_cfg else 30_000_000.0
        shared_top_n = cfg_vals.get("universe_top_n")
        if not isinstance(shared_top_n, int):
            shared_top_n = rsi_cfg.universe_top_n if rsi_cfg else 40
        for s in symbols:
            t = tickers.get(s)
            if not t:
                continue
            pct = t.get("percentage")
            qv = t.get("quoteVolume")
            if pct is None or qv is None:
                continue
            try:
                pct = float(pct)
                qv = float(qv)
            except Exception:
                continue
            pct_all_map[s] = pct
            qv_all_map[s] = qv
            if qv >= shared_min_qv:
                qv_map[s] = qv
        shared_universe = []
        if build_universe_from_tickers:
            shared_universe = build_universe_from_tickers(
                tickers,
                symbols=symbols,
                min_quote_volume_usdt=shared_min_qv,
                top_n=shared_top_n,
                anchors=anchors,
            )
        else:
            shared_universe = [s for s, _ in sorted(pct_all_map.items(), key=lambda x: abs(x[1]), reverse=True)]
            shared_universe = [s for s in shared_universe if qv_all_map.get(s, 0) >= shared_min_qv]
            shared_universe = [s for s in anchors] + [s for s in shared_universe if s not in anchors]
            if shared_top_n:
                shared_universe = shared_universe[:shared_top_n]
        state["_universe"] = list(shared_universe)
        if rsi_engine:
            ctx = EngineContext(
                exchange=exchange,
                state=state,
                now_ts=time.time(),
                logger=print,
                config=rsi_engine.config,
            )
            universe_momentum = rsi_engine.build_universe(ctx)
        else:
            universe_momentum = list(shared_universe)
        shared_universe_len = len(shared_universe)
        rsi_universe_len = len(universe_momentum)
        div15m_universe = list(shared_universe)
        div15m_universe_len = len(div15m_universe)
        div15m_short_universe = list(shared_universe)
        div15m_short_universe_len = len(div15m_short_universe)
        fabio_universe = []
        fabio_label = "realtime_only"
        fabio_dir_hint = {}
        if heavy_scan and dtfx_cfg and build_universe_from_tickers:
            fabio_label = "dtfx_universe"
            anchors = []
            for s in dtfx_cfg.anchor_symbols or []:
                anchors.append(s if "/" in s else f"{s}/USDT:USDT")
            dtfx_min_qv = max(dtfx_cfg.min_quote_volume_usdt, dtfx_cfg.low_liquidity_qv_usdt)
            fabio_universe = build_universe_from_tickers(
                tickers,
                symbols=symbols,
                min_quote_volume_usdt=dtfx_min_qv,
                top_n=dtfx_cfg.universe_top_n,
                anchors=tuple(anchors),
            )
            state["_fabio_universe"] = fabio_universe
            state["_fabio_label"] = fabio_label
            state["_fabio_dir_hint"] = fabio_dir_hint

        swaggy_universe = []
        swaggy_cfg = None
        swaggy_atlas_lab_cfg = None
        swaggy_atlas_lab_atlas_cfg = None
        if (SWAGGY_ENABLED or SWAGGY_ATLAS_LAB_ENABLED) and swaggy_engine and SwaggyConfig and EngineContext:
            swaggy_cfg = SwaggyConfig()
            ctx = EngineContext(
                exchange=exchange,
                state=state,
                now_ts=time.time(),
                logger=print,
                config=swaggy_cfg,
            )
            swaggy_universe = swaggy_engine.build_universe(ctx)
            state["_swaggy_universe"] = swaggy_universe
        elif SWAGGY_ATLAS_LAB_ENABLED:
            dtfx_cfg = dtfx_cfg if dtfx_cfg else DTFXConfig()
            dtfx_min_qv = max(dtfx_cfg.min_quote_volume_usdt, dtfx_cfg.low_liquidity_qv_usdt)
            anchors = []
            for s in dtfx_cfg.anchor_symbols or []:
                anchors.append(s if "/" in s else f"{s}/USDT:USDT")
            swaggy_universe = build_universe_from_tickers(
                tickers,
                symbols=symbols,
                min_quote_volume_usdt=dtfx_min_qv,
                top_n=dtfx_cfg.universe_top_n,
                anchors=tuple(anchors),
            )
            state["_swaggy_universe"] = swaggy_universe
        if SWAGGY_ATLAS_LAB_ENABLED and SwaggyAtlasLabConfig and SwaggyAtlasLabAtlasConfig:
            swaggy_atlas_lab_cfg = SwaggyAtlasLabConfig()
            swaggy_atlas_lab_atlas_cfg = SwaggyAtlasLabAtlasConfig()

            structure_candidates = sorted(qv_map.keys(), key=lambda x: qv_map.get(x, 0.0), reverse=True)
            if STRUCTURE_TOP_N:
                structure_candidates = structure_candidates[:STRUCTURE_TOP_N]
            _prefetch_ohlcv_for_cycle(
                structure_candidates,
                exchange,
                {"15m": 120, "4h": 120},
                label="structure-pre",
                ttl_by_tf={"4h": TTL_4H_SEC},
            )
            for s in structure_candidates:
                if _ema_align_ok(s, "15m", 120) or _ema_align_ok(s, "4h", 120):
                    universe_structure.append(s)

        dtfx_universe = []
        if DTFX_ENABLED and dtfx_engine and dtfx_cfg and EngineContext:
            ctx = EngineContext(
                exchange=exchange,
                state=state,
                now_ts=time.time(),
                logger=print,
                config=dtfx_cfg,
            )
            dtfx_universe = dtfx_engine.build_universe(ctx)
            state["_dtfx_universe"] = dtfx_universe

        pumpfade_universe = []
        if PUMPFADE_ENABLED and pumpfade_engine and pumpfade_cfg and EngineContext:
            ctx = EngineContext(
                exchange=exchange,
                state=state,
                now_ts=time.time(),
                logger=print,
                config=pumpfade_cfg,
            )
            pumpfade_universe = pumpfade_engine.build_universe(ctx)
            state["_pumpfade_universe"] = pumpfade_universe

        atlas_rs_fail_short_universe = []
        if ATLAS_RS_FAIL_SHORT_ENABLED and atlas_rs_fail_short_engine and atlas_rs_fail_short_cfg and EngineContext:
            ctx = EngineContext(
                exchange=exchange,
                state=state,
                now_ts=time.time(),
                logger=print,
                config=atlas_rs_fail_short_cfg,
            )
            atlas_rs_fail_short_universe = atlas_rs_fail_short_engine.build_universe(ctx)
            state["_atlas_rs_fail_short_universe"] = atlas_rs_fail_short_universe

        if heavy_scan:
            universe_union = list(
                set(
                    universe_momentum
                    + universe_structure
                    + fabio_universe
                    + (swaggy_universe or [])
                    + (dtfx_universe or [])
                    + (pumpfade_universe or [])
                    + (atlas_rs_fail_short_universe or [])
                )
            )
        else:
            if not fabio_universe:
                fabio_universe = list(state.get("_fabio_universe") or [])
                fabio_label = str(state.get("_fabio_label") or "realtime_only")
                fabio_dir_hint = dict(state.get("_fabio_dir_hint") or {})
            universe_union = list(
                set(
                    universe_momentum
                    + universe_structure
                    + fabio_universe
                    + (swaggy_universe or [])
                    + (dtfx_universe or [])
                    + (pumpfade_universe or [])
                    + (atlas_rs_fail_short_universe or [])
                )
            )
        if heavy_scan:
            try:
                os.makedirs("logs", exist_ok=True)
                atlas_line = (
                    f"[atlasfabio-universe] total={len(fabio_universe)} label={fabio_label}"
                )
                with open(os.path.join("logs", "fabio", "atlasfabio_universe.log"), "a", encoding="utf-8") as f:
                    f.write(atlas_line + "\n")
            except Exception:
                pass

        cached_ex = CachedExchange(exchange)
        cached_long_ex = CachedExchange(exchange)

        atlas_cfg = atlas_fabio_engine.Config() if (heavy_scan and ATLAS_FABIO_ENABLED and atlas_fabio_engine) else None
        fabio_cfg_atlas = None
        fabio_cfg_atlas_mid = None
        if atlas_cfg and fabio_entry_engine:
            fabio_cfg_atlas = fabio_entry_engine.Config()
            fabio_cfg_atlas.dist_to_ema20_max = ATLASFABIO_STRONG_DIST_MAX
            fabio_cfg_atlas.long_dist_to_ema20_max = ATLASFABIO_STRONG_DIST_MAX
            fabio_cfg_atlas.pullback_vol_ratio_max = ATLASFABIO_PULLBACK_VOL_MAX
            fabio_cfg_atlas.retest_touch_tol = ATLASFABIO_RETEST_TOUCH_TOL

            fabio_cfg_atlas_mid = fabio_entry_engine.Config()
            fabio_cfg_atlas_mid.timeframe_ltf = "5m"
            fabio_cfg_atlas_mid.dist_to_ema20_max = ATLASFABIO_MID_DIST_MAX
            fabio_cfg_atlas_mid.long_dist_to_ema20_max = ATLASFABIO_MID_DIST_MAX
            fabio_cfg_atlas_mid.pullback_vol_ratio_max = ATLASFABIO_MID_PULLBACK_VOL_MAX
            fabio_cfg_atlas_mid.retest_touch_tol = ATLASFABIO_MID_RETEST_TOUCH_TOL
            fabio_cfg_atlas_mid.trigger_vol_ratio_min = 1.05
        fabio_universe_len = len(fabio_universe)
        swaggy_universe_len = len(swaggy_universe) if swaggy_universe else 0
        swaggy_atlas_lab_universe_len = swaggy_universe_len if SWAGGY_ATLAS_LAB_ENABLED else 0
        dtfx_universe_len = len(dtfx_universe) if dtfx_universe else 0
        pumpfade_universe_len = len(pumpfade_universe) if pumpfade_universe else 0
        atlas_rs_fail_short_universe_len = len(atlas_rs_fail_short_universe) if atlas_rs_fail_short_universe else 0
        universe_structure_len = len(universe_structure)
        universe_union_len = len(universe_union)
        rsi_ran = bool(run_rsi_short and universe_momentum)
        div15m_long_ran = bool(run_div15m_long and div15m_universe)
        div15m_short_ran = bool(run_div15m_short and div15m_short_universe)
        atlasfabio_ran = bool(heavy_scan and ATLAS_FABIO_ENABLED and atlas_cfg and fabio_cfg_atlas)
        swaggy_ran = bool(heavy_scan and SWAGGY_ENABLED and swaggy_cfg and swaggy_engine)
        swaggy_atlas_lab_ran = bool(
            heavy_scan
            and SWAGGY_ATLAS_LAB_ENABLED
            and swaggy_atlas_lab_cfg
            and swaggy_atlas_lab_atlas_cfg
            and swaggy_atlas_lab_engine
            and swaggy_universe
        )
        dtfx_ran = bool(DTFX_ENABLED and dtfx_engine and dtfx_cfg and dtfx_universe)
        pumpfade_ran = bool(PUMPFADE_ENABLED and pumpfade_engine and pumpfade_cfg and pumpfade_universe)
        atlas_rs_fail_short_ran = bool(
            ATLAS_RS_FAIL_SHORT_ENABLED
            and atlas_rs_fail_short_engine
            and atlas_rs_fail_short_cfg
            and atlas_rs_fail_short_universe
        )
        # 공통 OHLCV 프리패치 (Tiered + TTL)
        watch_symbols = [
            s
            for s, st in state.items()
            if isinstance(st, dict) and st.get("in_pos")
        ]
        prefetch_symbols = list(set(universe_union + watch_symbols))
        top_candidates = list(universe_momentum[:FAST_TF_PREFETCH_TOPN])
        in_pos_symbols = [s for s, st in state.items() if isinstance(st, dict) and st.get("in_pos")]
        fast_symbols_ordered = []
        for s in in_pos_symbols:
            if s not in fast_symbols_ordered:
                fast_symbols_ordered.append(s)
        for s in top_candidates:
            if s not in fast_symbols_ordered:
                fast_symbols_ordered.append(s)
        fast_symbols = fast_symbols_ordered[:MAX_FAST_SYMBOLS]
        slow_symbols_ordered = []
        for s in in_pos_symbols:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in universe_momentum[:30]:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in universe_structure[:30]:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in fabio_universe:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in swaggy_universe or []:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in pumpfade_universe or []:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        for s in atlas_rs_fail_short_universe or []:
            if s not in slow_symbols_ordered:
                slow_symbols_ordered.append(s)
        slow_symbols = slow_symbols_ordered

        fast_plan: Dict[str, int] = {}
        mid_plan: Dict[str, int] = {}
        slow_plan: Dict[str, int] = {}

        # FAST TF (3m/5m) - 후보만
        fast_plan["3m"] = max(fast_plan.get("3m", 0), 60)
        fast_plan["5m"] = max(fast_plan.get("5m", 0), 120)
        fast_plan["3m"] = min(fast_plan.get("3m", 0), FAST_LIMIT_CAP)
        fast_plan["5m"] = min(fast_plan.get("5m", 0), FAST_LIMIT_CAP)

        # MID TF (15m) - 2~3 사이클마다
        mid_plan["15m"] = max(mid_plan.get("15m", 0), 60)
        if fabio_cfg_atlas:
            mid_plan["15m"] = max(mid_plan.get("15m", 0), int(fabio_cfg_atlas.limit))
        if swaggy_cfg:
            mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
            mid_plan["1h"] = max(mid_plan.get("1h", 0), int(swaggy_cfg.vp_lookback_1h))
        if swaggy_atlas_lab_cfg:
            mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
            mid_plan["1h"] = max(mid_plan.get("1h", 0), int(swaggy_atlas_lab_cfg.vp_lookback_1h))
        if pumpfade_cfg:
            mid_plan["15m"] = max(mid_plan.get("15m", 0), int(max(80, pumpfade_cfg.lookback_hh + 10)))
            mid_plan["1h"] = max(mid_plan.get("1h", 0), 40)
        if atlas_rs_fail_short_cfg:
            mid_plan["15m"] = max(mid_plan.get("15m", 0), int(atlas_rs_fail_short_cfg.ltf_limit))
        if atlas_cfg:
            mid_plan["1h"] = max(mid_plan.get("1h", 0), int(atlas_cfg.htf_limit))
        mid_plan["15m"] = min(mid_plan.get("15m", 0), MID_LIMIT_CAP)
        if "1h" in mid_plan:
            mid_plan["1h"] = min(mid_plan.get("1h", 0), MID_LIMIT_CAP)

        # SLOW TF (4h/1d) - TTL
        if fabio_cfg_atlas:
            slow_plan["4h"] = max(slow_plan.get("4h", 0), int(fabio_cfg_atlas.limit))
        if atlas_cfg:
            slow_plan["1d"] = max(slow_plan.get("1d", 0), int(getattr(atlas_cfg, "d1_limit", 90)))
        if swaggy_cfg:
            slow_plan["4h"] = max(slow_plan.get("4h", 0), 200)
        if swaggy_atlas_lab_cfg:
            slow_plan["4h"] = max(slow_plan.get("4h", 0), 200)
        if "4h" in slow_plan:
            slow_plan["4h"] = min(slow_plan.get("4h", 0), SLOW_LIMIT_CAP)
        if "1d" in slow_plan:
            slow_plan["1d"] = min(slow_plan.get("1d", 0), SLOW_LIMIT_CAP)

        # cycle ts debug moved earlier (after prefetch plan)

        do_prefetch = bool(heavy_scan)
        print(f"[prefetch] cycle={cycle_label}")
        fast_momentum = [s for s in fast_symbols if s in universe_momentum]
        fast_structure = [s for s in fast_symbols if s in universe_structure]
        if do_prefetch:
            print(f"[prefetch-fast] total={len(fast_symbols)} {fast_symbols}")
            print(f"[prefetch-fast] momentum={len(fast_momentum)} {fast_momentum}")
            print(f"[prefetch-fast] structure={len(fast_structure)} {fast_structure}")
        fast_stats = {"symbols": len(fast_symbols), "tfs": list(fast_plan.keys()), "fetched": 0, "failed": 0, "fresh_hits": {}}
        mid_skipped = len(slow_symbols)
        mid_stats = {"symbols": len(slow_symbols), "tfs": list(mid_plan.keys()), "fetched": 0, "failed": 0, "fresh_hits": {}}
        slow_stats = {"symbols": len(slow_symbols), "tfs": list(slow_plan.keys()), "fetched": 0, "failed": 0, "fresh_hits": {}, "fetched_by_tf": {}}
        if do_prefetch:
            mid_skipped = 0
            force_mid = bool(cycle_ts and cycle_ts != last_cycle_ts)
            if force_mid or cycle_count % MID_TF_PREFETCH_EVERY_N == 0:
                mid_stats = _prefetch_ohlcv_for_cycle(slow_symbols, exchange, mid_plan, label="mid")
            else:
                mid_skipped = len(slow_symbols)
            slow_stats = _prefetch_ohlcv_for_cycle(
                slow_symbols,
                exchange,
                slow_plan,
                label="slow",
                ttl_by_tf={"4h": TTL_4H_SEC, "1d": TTL_1D_SEC},
            )
            atlas_pass_symbols = []
            atlas_fail_count = 0
            if heavy_scan and ATLAS_FABIO_ENABLED and atlas_fabio_engine:
                for sym in fabio_universe:
                    d = fabio_dir_hint.get(sym) if isinstance(fabio_dir_hint, dict) else None
                    if d == "LONG":
                        gate = _atlasfabio_gate_long(sym, atlas_cfg)
                    elif d == "SHORT":
                        gate = _atlasfabio_gate_short(sym, atlas_cfg)
                    else:
                        continue
                    if gate.get("status") != "ok":
                        continue
                    trade_allowed = bool(gate.get("trade_allowed"))
                    allow_long = bool(gate.get("allow_long"))
                    allow_short = bool(gate.get("allow_short"))
                    if not trade_allowed:
                        atlas_fail_count += 1
                        continue
                    if d == "LONG" and not allow_long:
                        atlas_fail_count += 1
                        continue
                    if d == "SHORT" and not allow_short:
                        atlas_fail_count += 1
                        continue
                    atlas_pass_symbols.append(sym)
                print(
                    f"[atlas-prefetch] pass={len(atlas_pass_symbols)} fail={atlas_fail_count} total={len(fabio_universe)}"
                )
            if atlas_pass_symbols:
                fast_symbols_ordered = []
                for s in in_pos_symbols:
                    if s not in fast_symbols_ordered:
                        fast_symbols_ordered.append(s)
                for s in atlas_pass_symbols:
                    if s not in fast_symbols_ordered:
                        fast_symbols_ordered.append(s)
                for s in top_candidates:
                    if s not in fast_symbols_ordered:
                        fast_symbols_ordered.append(s)
                fast_symbols = fast_symbols_ordered[:MAX_FAST_SYMBOLS]
            fast_stats = _prefetch_ohlcv_for_cycle(fast_symbols, exchange, fast_plan, label="fast")

            print(
                "[prefetch] fast_tf: symbols=%d tfs=%s fetched=%d failed=%d"
                % (fast_stats["symbols"], fast_stats["tfs"], fast_stats["fetched"], fast_stats["failed"])
            )
            print(
                "[prefetch] mid_tf: symbols=%d tfs=%s skipped=%d fetched=%d failed=%d"
                % (mid_stats["symbols"], mid_stats["tfs"], mid_skipped, mid_stats["fetched"], mid_stats["failed"])
            )
            h4_hit = slow_stats["fresh_hits"].get("4h", 0)
            d1_hit = slow_stats["fresh_hits"].get("1d", 0)
            h4_miss = slow_stats.get("fetched_by_tf", {}).get("4h", 0)
            d1_miss = slow_stats.get("fetched_by_tf", {}).get("1d", 0)
            print(
                "[prefetch] slow_tf: 4h_hit=%d miss=%d | 1d_hit=%d miss=%d"
                % (h4_hit, h4_miss, d1_hit, d1_miss)
            )
        else:
            print("[prefetch] skip heavy scan (realtime only)")

        now = time.time()

        # 전체 포지션 캐시 1회 갱신 (레이트리밋 완화)
        try:
            refresh_positions_cache(force=True)
        except Exception as e:
            print("[positions] cache refresh failed:", e)
        active_positions_total = count_open_positions(force=True)
        if not isinstance(active_positions_total, int):
            active_positions_total = sum(
                1 for st in state.values() if isinstance(st, dict) and st.get("in_pos")
            )

        # --- SYNC from exchange: manage mode baseline (universe 포함) ---
        sync_syms = list(set(list(state.keys()) + list(universe_union)))
        _sync_positions_state(state, sync_syms)

        # 관리 대상(실포지션)만 WebSocket 감시 목록에 반영
        if ws_manager and ws_manager.is_running():
            try:
                manage_syms = [s for s, st in state.items() if isinstance(st, dict) and st.get("in_pos")]
                ws_manager.set_watch_symbols(manage_syms)
            except Exception as e:
                print("[WS] set_watch_symbols error:", e)



        if not MANAGE_LOOP_ENABLED:
            short_buf = []
            _set_thread_log_buffer(short_buf)
            _run_manage_cycle(state, exchange, cached_long_ex, send_telegram)
            _set_thread_log_buffer(None)

        # --- ENTRY scan ---
        scanned = 0
        pass_counts = {"1h": 0, "15m": 0, "5m": 0}
        pass_tally = {i: 0 for i in range(7)}  # 0~6개 조건 충족 카운트
        div15m_cfg = div15m_engine.config if div15m_engine else None
        div15m_limit = 0
        if div15m_cfg:
            div15m_limit = max(
                int(div15m_cfg.LOOKBACK_BARS + div15m_cfg.PIVOT_L * 2 + 10),
                int(div15m_cfg.RSI_LEN + div15m_cfg.WARMUP_BARS + 10),
                int(div15m_cfg.EMA_REGIME_LEN + 5),
            )
        div15m_short_cfg = div15m_short_engine.config if div15m_short_engine else None
        div15m_short_limit = 0
        if div15m_short_cfg:
            div15m_short_limit = max(
                int(div15m_short_cfg.LOOKBACK_BARS + div15m_short_cfg.PIVOT_L * 2 + 10),
                int(div15m_short_cfg.RSI_LEN + div15m_short_cfg.WARMUP_BARS + 10),
                int(div15m_short_cfg.EMA_REGIME_LEN + 5),
            )

        def _div15m_log(msg: str) -> None:
            date_tag = time.strftime("%Y%m%d")
            _append_entry_log(f"div15m_long/div15m_live_{date_tag}.log", msg)
        def _div15m_short_log(msg: str) -> None:
            date_tag = time.strftime("%Y%m%d")
            _append_entry_log(f"div15m_short/div15m_live_{date_tag}.log", msg)

        def _div15m_short_csv(event) -> None:
            if event is None:
                return
            date_tag = time.strftime("%Y%m%d")
            path = os.path.join("logs", "div15m_short", f"div15m_short_signals_{date_tag}.csv")
            header = [
                "ts",
                "symbol",
                "event",
                "entry_px",
                "p1_idx",
                "p2_idx",
                "high1",
                "high2",
                "rsi1",
                "rsi2",
                "score",
                "reasons",
            ]
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                need_header = not os.path.exists(path)
                with open(path, "a", encoding="utf-8") as f:
                    if need_header:
                        f.write(",".join(header) + "\n")
                    f.write(
                        f"{event.ts},{event.symbol},{event.event},{event.entry_px:.6g},"
                        f"{event.p1_idx},{event.p2_idx},{event.high1:.6g},{event.high2:.6g},"
                        f"{event.rsi1:.6g},{event.rsi2:.6g},{event.score:.4f},{event.reasons}\n"
                    )
            except Exception:
                pass
        active_positions = int(active_positions_total)
        pos_limit_logged = False
        atlasfabio_result = {}
        atlasfabio_thread = None
        swaggy_result = {}
        swaggy_thread = None
        swaggy_atlas_lab_result = {}
        swaggy_atlas_lab_thread = None
        dtfx_result = {}
        dtfx_thread = None
        pumpfade_result = {}
        pumpfade_thread = None
        atlas_rs_fail_short_result = {}
        atlas_rs_fail_short_thread = None
        if heavy_scan and ATLAS_FABIO_ENABLED and fabio_cfg_atlas and atlas_cfg:
            atlasfabio_thread = threading.Thread(
                target=lambda: atlasfabio_result.update(
                    _run_atlas_fabio_cycle(
                        fabio_universe,
                        cached_ex,
                        state,
                        fabio_cfg_atlas,
                        fabio_cfg_atlas_mid,
                        atlas_cfg,
                        active_positions,
                        fabio_dir_hint,
                        send_telegram,
                    )
                ),
                daemon=True,
            )
            atlasfabio_thread.start()
        elif ATLAS_FABIO_ENABLED and fabio_cfg_atlas:
            pass
        if SWAGGY_ENABLED and swaggy_cfg and swaggy_engine:
            swaggy_thread = threading.Thread(
                target=lambda: swaggy_result.update(
                    _run_swaggy_cycle(
                        swaggy_engine,
                        swaggy_universe,
                        cached_ex,
                        state,
                        swaggy_cfg,
                        active_positions,
                        send_telegram,
                    )
                ),
                daemon=True,
            )
            swaggy_thread.start()
        if SWAGGY_ATLAS_LAB_ENABLED and swaggy_atlas_lab_cfg and swaggy_atlas_lab_atlas_cfg and swaggy_atlas_lab_engine:
            swaggy_atlas_lab_thread = threading.Thread(
                target=lambda: swaggy_atlas_lab_result.update(
                    _run_swaggy_atlas_lab_cycle(
                        swaggy_atlas_lab_engine,
                        swaggy_universe,
                        cached_ex,
                        state,
                        swaggy_atlas_lab_cfg,
                        swaggy_atlas_lab_atlas_cfg,
                        active_positions,
                        send_telegram,
                    )
                ),
                daemon=True,
            )
            swaggy_atlas_lab_thread.start()
        if DTFX_ENABLED and dtfx_cfg and dtfx_engine:
            dtfx_thread = threading.Thread(
                target=lambda: dtfx_result.update(
                    _run_dtfx_cycle(
                        dtfx_engine,
                        dtfx_universe,
                        cached_ex,
                        state,
                        dtfx_cfg,
                        active_positions,
                        send_telegram,
                    )
                ),
                daemon=True,
            )
            dtfx_thread.start()
        if PUMPFADE_ENABLED and pumpfade_cfg and pumpfade_engine:
            pumpfade_thread = threading.Thread(
                target=lambda: pumpfade_result.update(
                    _run_pumpfade_cycle(
                        pumpfade_engine,
                        pumpfade_universe,
                        state,
                        send_telegram,
                        pumpfade_cfg,
                    )
                ),
                daemon=True,
            )
            pumpfade_thread.start()
        if ATLAS_RS_FAIL_SHORT_ENABLED and atlas_rs_fail_short_cfg and atlas_rs_fail_short_engine:
            atlas_rs_fail_short_thread = threading.Thread(
                target=lambda: atlas_rs_fail_short_result.update(
                    _run_atlas_rs_fail_short_cycle(
                        atlas_rs_fail_short_engine,
                        atlas_rs_fail_short_universe,
                        state,
                        send_telegram,
                        atlas_rs_fail_short_cfg,
                    )
                ),
                daemon=True,
            )
            atlas_rs_fail_short_thread.start()
        universe_total = len(universe_momentum)
        for idx, symbol in enumerate(universe_momentum, start=1):
            if not run_rsi_short:
                break
            scanned += 1
            # 최소 로그 모드: 진행률 출력 생략
            st = state.get(symbol, {"in_pos": False, "last_ok": False, "last_entry": 0})
            in_pos = bool(st.get("in_pos", False))
            last_ok = bool(st.get("last_ok", False))
            last_entry = _get_last_entry_ts_by_side(st, "SHORT") or 0.0

            # 진입 이전에 실포지션 존재 시 즉시 차단
            if not in_pos:
                try:
                    existing_amt = get_short_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    st["in_pos"] = True
                    st.setdefault("dca_adds", 0)
                    st.setdefault("dca_adds_long", 0)
                    st.setdefault("dca_adds_short", 0)
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

            if in_pos:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            cur_total = count_open_positions(force=True)
            if not isinstance(cur_total, int):
                cur_total = active_positions
            if cur_total >= MAX_OPEN_POSITIONS:
                if not pos_limit_logged:
                    print(f"[제한] 동시 포지션 {cur_total}/{MAX_OPEN_POSITIONS} → 신규 진입 스킵")
                    pos_limit_logged = True
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if _exit_cooldown_blocked(state, symbol, "rsi", "SHORT"):
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if isinstance(last_entry, (int, float)) and now - float(last_entry) < COOLDOWN_SEC:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            fr_rate, fr_interval = fetch_funding_rate(symbol)
            if fr_interval == FUNDING_INTERVAL_HOURS and isinstance(fr_rate, (int, float)):
                if fr_rate < (FUNDING_BLOCK_PCT / 100.0):
                    print(f"[제한] 펀딩비 {fr_rate*100:.3f}%/h → 숏 금지 ({symbol})")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

            if not rsi_engine:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            scan_result = rsi_engine.scan_symbol(symbol, pass_counts, logger=lambda *args, **kwargs: None)
            if not scan_result:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            rsis = scan_result.rsis
            rsis_prev = scan_result.rsis_prev
            r3m_val = scan_result.r3m_val
            rsi5m_downturn = scan_result.rsi5m_downturn
            rsi3m_downturn = scan_result.rsi3m_downturn
            vol_ok = scan_result.vol_ok
            struct_ok = scan_result.struct_ok
            struct_metrics = scan_result.struct_metrics
            vol_cur = scan_result.vol_cur
            vol_avg = scan_result.vol_avg
            spike_ready = scan_result.spike_ready
            struct_ready = scan_result.struct_ready
            pass_count = scan_result.pass_count
            miss_count = scan_result.miss_count
            trigger_ok = scan_result.trigger_ok
            impulse_block = scan_result.impulse_block
            ready_entry = scan_result.ready_entry
            pass_tally[min(max(pass_count, 0), 6)] += 1
            vol_ratio = (float(vol_cur) / float(vol_avg)) if (vol_cur and vol_avg and vol_avg > 0) else None
            struct_lower_highs = struct_metrics.get("lower_highs") if isinstance(struct_metrics, dict) else None
            struct_wick_reject = struct_metrics.get("wick_reject") if isinstance(struct_metrics, dict) else None
            wick_ratio_1 = struct_metrics.get("upper_wick_ratio_1") if isinstance(struct_metrics, dict) else None
            wick_ratio_2 = struct_metrics.get("upper_wick_ratio_2") if isinstance(struct_metrics, dict) else None
            thr = rsi_engine.config.thresholds if rsi_engine and hasattr(rsi_engine, "config") else {}
            detail_line = (
                "[{ts}] [rsi-detail] idx={idx}/{total} sym={sym} "
                "rsi1h={r1h} rsi15={r15} rsi5={r5} rsi5_prev={r5p} "
                "rsi3={r3} rsi3_prev={r3p} rsi3_prev2={r3p2} "
                "thr1h={t1h} thr15={t15} thr5={t5} thr3={t3} "
                "down5={d5} down3={d3} ok_tf={oktf} trigger={trig} "
                "vol_ok={vok} vol_cur={vcur} vol_avg={vavg} vol_ratio={vr} "
                "struct_ok={sok} lower_highs={lh} wick_reject={wr} wick1={w1} wick2={w2} "
                "impulse_block={imp} spike_ready={spk} struct_ready={str} ready={ready} "
                "pass={pc} miss={mc}"
            ).format(
                ts=_now_kst_str(),
                idx=idx,
                total=universe_total,
                sym=symbol,
                r1h=_fmt_float(rsis.get("1h"), 2),
                r15=_fmt_float(rsis.get("15m"), 2),
                r5=_fmt_float(rsis.get("5m"), 2),
                r5p=_fmt_float(rsis_prev.get("5m"), 2),
                r3=_fmt_float(rsis.get("3m"), 2),
                r3p=_fmt_float(rsis_prev.get("3m"), 2),
                r3p2=_fmt_float(r3m_val, 2),
                t1h=_fmt_float(thr.get("1h"), 2) if isinstance(thr, dict) else "N/A",
                t15=_fmt_float(thr.get("15m"), 2) if isinstance(thr, dict) else "N/A",
                t5=_fmt_float(thr.get("5m"), 2) if isinstance(thr, dict) else "N/A",
                t3=_fmt_float(thr.get("3m"), 2) if isinstance(thr, dict) else "N/A",
                d5="Y" if rsi5m_downturn else "N",
                d3="Y" if rsi3m_downturn else "N",
                oktf="Y" if scan_result.ok_tf else "N",
                trig="Y" if trigger_ok else "N",
                vok="Y" if vol_ok else "N",
                vcur=_fmt_float(vol_cur, 2),
                vavg=_fmt_float(vol_avg, 2),
                vr=_fmt_float(vol_ratio, 2),
                sok="Y" if struct_ok else "N",
                lh="Y" if struct_lower_highs else "N",
                wr="Y" if struct_wick_reject else "N",
                w1=_fmt_float(wick_ratio_1, 3),
                w2=_fmt_float(wick_ratio_2, 3),
                imp="Y" if impulse_block else "N",
                spk="Y" if spike_ready else "N",
                str="Y" if struct_ready else "N",
                ready="Y" if ready_entry else "N",
                pc=pass_count,
                mc=miss_count,
            )
            _append_rsi_detail_log(detail_line)

            # update state for edge detection
            state.setdefault(symbol, {})
            state[symbol].setdefault("in_pos", False)
            state[symbol].setdefault("last_entry", last_entry)
            # state[symbol]["last_ok"]는 아래에서 ready_entry 기준으로 갱신

            # edge 상태 업데이트: ready_entry 저장 (다음 사이클에서 상승 에지 감지용)
            state[symbol]["last_ok"] = ready_entry

            if ready_entry and (not last_ok):
                # ensure no actual position
                if get_short_position_amount(symbol) > 0:
                    state[symbol]["in_pos"] = True
                    send_telegram(f"⏭️ <b>SKIP</b> (position exists)\n<b>{symbol}</b>")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

                order_info = "(알림 전용)"
                entry_price_disp = None
                if LIVE_TRADING:
                    guard_key = _entry_guard_key(state, symbol, "SHORT")
                    if not _entry_guard_acquire(state, symbol, key=guard_key, engine="rsi", side="SHORT"):
                        print(f"[entry] 숏 중복 차단 ({symbol})")
                        time.sleep(PER_SYMBOL_SLEEP)
                        continue
                    if _entry_seen_blocked(state, symbol, "SHORT", "rsi"):
                        time.sleep(PER_SYMBOL_SLEEP)
                        continue
                    try:
                        res = short_market(symbol, usdt_amount=_resolve_entry_usdt(), leverage=LEVERAGE, margin_mode=MARGIN_MODE)
                        entry_order_id = _order_id_from_res(res)
                        # 알림용 간략 정보
                        fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                        qty = res.get("amount") or res.get("order", {}).get("amount")
                        order_info = (
                            f"entry_price={fill_price} qty={qty} usdt={_resolve_entry_usdt()}"
                        )
                        entry_price_disp = fill_price
                        state[symbol]["in_pos"] = True
                        active_positions += 1
                        if res.get("order") or res.get("status") in ("ok", "dry_run"):
                            _entry_seen_mark(state, symbol, "SHORT", "rsi")
                            _log_trade_entry(
                                state,
                                side="SHORT",
                                symbol=symbol,
                                entry_ts=time.time(),
                                entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                                qty=qty if isinstance(qty, (int, float)) else None,
                                usdt=_resolve_entry_usdt(),
                                entry_order_id=entry_order_id,
                                meta={"reason": "short_entry"},
                            )
                            date_tag = time.strftime("%Y%m%d")
                            _append_entry_log(
                                f"rsi/rsi_entries_{date_tag}.log",
                                "engine=rsi side=SHORT symbol=%s price=%s qty=%s usdt=%s "
                                "rsi1h=%s rsi15=%s rsi5=%s rsi3=%s "
                                "down5=%s down3=%s vol=%s struct=%s spike=%s structr=%s"
                                % (
                                    symbol,
                                    f"{fill_price:.6g}" if isinstance(fill_price, (int, float)) else "N/A",
                                    f"{qty:.6g}" if isinstance(qty, (int, float)) else "N/A",
                                    f"{_resolve_entry_usdt():.2f}",
                                    f"{rsis.get('1h', 0):.2f}" if isinstance(rsis.get("1h"), (int, float)) else "N/A",
                                    f"{rsis.get('15m', 0):.2f}" if isinstance(rsis.get("15m"), (int, float)) else "N/A",
                                    f"{rsis.get('5m', 0):.2f}" if isinstance(rsis.get("5m"), (int, float)) else "N/A",
                                    f"{rsis.get('3m', 0):.2f}" if isinstance(rsis.get("3m"), (int, float)) else "N/A",
                                    "Y" if rsi5m_downturn else "N",
                                    "Y" if rsi3m_downturn else "N",
                                    "Y" if vol_ok else "N",
                                    "Y" if struct_ok else "N",
                                    "Y" if spike_ready else "N",
                                    "Y" if struct_ready else "N",
                                ),
                            )
                    except Exception as e:
                        order_info = f"order_error: {e}"
                    finally:
                        _entry_guard_release(state, symbol, key=guard_key)

                _set_last_entry_state(state[symbol], "SHORT", time.time())

                reason_parts = ["RSI222"]
                if spike_ready and struct_ready:
                    reason_parts.append("SPIKE+STRUCT")
                elif spike_ready:
                    reason_parts.append("SPIKE")
                elif struct_ready:
                    reason_parts.append("STRUCT")
                if rsi3m_downturn:
                    reason_parts.append("RSI 꺾임")
                if struct_ok:
                    reason_parts.append("구조 거절")
                if vol_ok:
                    reason_parts.append("거래량 급증")
                reason = ", ".join(reason_parts)
                rsi_line = (
                    f"RSI: 1h {rsis.get('1h',0):.2f} | 15m {rsis.get('15m',0):.2f} | "
                    f"5m {rsis.get('5m',0):.2f} | 3m {rsis.get('3m',0):.2f}"
                )
                reason_full = reason if reason else "조건 충족"
                reason_full = f"{reason_full} | {rsi_line}"
                _send_entry_alert(
                    send_telegram,
                    side="SHORT",
                    symbol=symbol,
                    engine="RSI",
                    entry_price=entry_price_disp,
                    usdt=_resolve_entry_usdt(),
                    reason=reason_full,
                    live=LIVE_TRADING,
                    order_info=order_info,
                    entry_order_id=entry_order_id,
                    sl=_fmt_price_safe(entry_price_disp, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                    tp=None,
                )

        if not run_div15m_long:
            print("[모드] DIV15M_LONG 비활성: 롱 다이버전스 스캔 스킵")
        for symbol in div15m_universe:
            if not run_div15m_long:
                break
            st = state.get(symbol, {"in_pos": False, "last_entry": 0})
            in_pos = bool(st.get("in_pos", False))
            last_entry = _get_last_entry_ts_by_side(st, "LONG") or 0.0

            if not in_pos:
                try:
                    existing_amt = get_long_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    st["in_pos"] = True
                    st.setdefault("dca_adds", 0)
                    st.setdefault("dca_adds_long", 0)
                    st.setdefault("dca_adds_short", 0)
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

            if in_pos:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            cur_total = count_open_positions(force=True)
            if not isinstance(cur_total, int):
                cur_total = active_positions
            if cur_total >= MAX_OPEN_POSITIONS:
                _append_entry_gate_log(
                    "div15m",
                    symbol,
                    f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side=LONG",
                    side="LONG",
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _exit_cooldown_blocked(state, symbol, "div15m", "LONG"):
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if isinstance(last_entry, (int, float)) and now - float(last_entry) < COOLDOWN_SEC:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if not div15m_engine or not div15m_cfg:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            df_15m = cycle_cache.get_df(symbol, "15m", limit=div15m_limit)
            if df_15m.empty or len(df_15m) < div15m_limit:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            df_15m = df_15m.iloc[:-1]
            if df_15m.empty:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            idx = len(df_15m) - 1
            close_15m = df_15m["close"]
            rsi_15m = div15m_engine._rsi_wilder(close_15m, div15m_cfg.RSI_LEN)
            ema_fast = div15m_engine._ema(close_15m, div15m_cfg.EMA_FAST)
            ema_slow = div15m_engine._ema(close_15m, div15m_cfg.EMA_SLOW)
            ema_regime = div15m_engine._ema(close_15m, div15m_cfg.EMA_REGIME_LEN)

            event = div15m_engine.process_candidate(
                symbol,
                df_15m,
                idx,
                rsi_15m,
                ema_fast,
                ema_slow,
                ema_regime,
                _div15m_log,
            )
            div15m_engine.on_bar(symbol, df_15m, idx, rsi_15m, ema_fast, ema_slow, _div15m_log)
            if not event or event.event != "ENTRY_READY":
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if get_long_position_amount(symbol) > 0:
                st["in_pos"] = True
                state[symbol] = st
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            order_info = "(알림 전용)"
            entry_price_disp = None
            entry_order_id = None
            if LONG_LIVE_TRADING:
                guard_key = _entry_guard_key(state, symbol, "LONG")
                if not _entry_guard_acquire(state, symbol, key=guard_key, engine="div15m", side="LONG"):
                    print(f"[entry] 롱 중복 차단 ({symbol})")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                if _entry_seen_blocked(state, symbol, "LONG", "div15m"):
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                try:
                    res = long_market(
                        symbol,
                        usdt_amount=_resolve_entry_usdt(),
                        leverage=LEVERAGE,
                        margin_mode=MARGIN_MODE,
                    )
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    order_info = (
                        f"entry_price={fill_price} qty={qty} usdt={_resolve_entry_usdt()}"
                    )
                    entry_price_disp = fill_price
                    st["in_pos"] = True
                    _set_last_entry_state(st, "LONG", time.time())
                    state[symbol] = st
                    if res.get("order") or res.get("status") in ("ok", "dry_run"):
                        _entry_seen_mark(state, symbol, "LONG", "div15m")
                    _log_trade_entry(
                        state,
                        side="LONG",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=_resolve_entry_usdt(),
                        entry_order_id=entry_order_id,
                        meta={"reason": "div15m_long"},
                    )
                    date_tag = time.strftime("%Y%m%d")
                    _append_entry_log(
                        f"div15m_long/div15m_entries_{date_tag}.log",
                        "engine=div15m_long side=LONG symbol=%s price=%s qty=%s usdt=%s p1=%s p2=%s score=%.4f"
                        % (
                            symbol,
                            f"{fill_price:.6g}" if isinstance(fill_price, (int, float)) else "N/A",
                            f"{qty:.6g}" if isinstance(qty, (int, float)) else "N/A",
                            f"{_resolve_entry_usdt():.2f}",
                            event.p1_idx,
                            event.p2_idx,
                            float(event.score or 0.0),
                        ),
                    )
                    active_positions += 1
                except Exception as e:
                    order_info = f"order_error: {e}"
                finally:
                    _entry_guard_release(state, symbol, key=guard_key)

            _set_last_entry_state(state[symbol], "LONG", time.time())

            _send_entry_alert(
                send_telegram,
                side="LONG",
                symbol=symbol,
                engine="DIV15M_LONG",
                entry_price=entry_price_disp,
                usdt=_resolve_entry_usdt(),
                reason=f"trigger={event.reasons or 'ENTRY_READY'}",
                live=LONG_LIVE_TRADING,
                order_info=order_info,
                entry_order_id=entry_order_id,
                sl=_fmt_price_safe(entry_price_disp, AUTO_EXIT_LONG_SL_PCT, side="LONG"),
                tp=None,
            )

        if not run_div15m_short:
            print("[모드] DIV15M_SHORT 비활성: 숏 다이버전스 스캔 스킵")
        div15m_short_bucket = state.setdefault("_div15m_short", {})
        for symbol in div15m_short_universe:
            if not run_div15m_short:
                break

            st = div15m_short_bucket.get(symbol, {"in_pos": False, "last_entry": 0})
            in_pos = bool(st.get("in_pos", False))
            last_entry = _get_last_entry_ts_by_side(st, "SHORT") or 0.0

            if not in_pos:
                try:
                    existing_amt = get_short_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    st["in_pos"] = True
                    div15m_short_bucket[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

            if in_pos:
                try:
                    existing_amt = get_short_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                st["in_pos"] = False
                div15m_short_bucket[symbol] = st
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            cur_total = count_open_positions(force=True)
            if not isinstance(cur_total, int):
                cur_total = active_positions
            if cur_total >= MAX_OPEN_POSITIONS:
                _append_entry_gate_log(
                    "div15m_short",
                    symbol,
                    f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side=SHORT",
                    side="SHORT",
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _exit_cooldown_blocked(state, symbol, "div15m_short", "SHORT"):
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if isinstance(last_entry, (int, float)) and now - float(last_entry) < COOLDOWN_SEC:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            if not div15m_short_engine or not div15m_short_cfg:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            df_15m = cycle_cache.get_df(symbol, "15m", limit=div15m_short_limit)
            if df_15m.empty or len(df_15m) < div15m_short_limit:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            df_15m = df_15m.iloc[:-1]
            if df_15m.empty:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            idx = len(df_15m) - 1
            close_15m = df_15m["close"]
            rsi_15m = div15m_short_engine._rsi_wilder(close_15m, div15m_short_cfg.RSI_LEN)
            ema_fast = div15m_short_engine._ema(close_15m, div15m_short_cfg.EMA_FAST)
            ema_slow = div15m_short_engine._ema(close_15m, div15m_short_cfg.EMA_SLOW)
            ema_regime = div15m_short_engine._ema(close_15m, div15m_short_cfg.EMA_REGIME_LEN)
            macd_hist = div15m_short_engine._macd_hist(
                close_15m,
                div15m_short_cfg.MACD_FAST,
                div15m_short_cfg.MACD_SLOW,
                div15m_short_cfg.MACD_SIGNAL,
            )

            event = div15m_short_engine.process_candidate(
                symbol,
                df_15m,
                idx,
                rsi_15m,
                ema_fast,
                ema_slow,
                ema_regime,
                _div15m_short_log,
                macd_hist,
            )
            div15m_short_engine.on_bar(symbol, df_15m, idx, rsi_15m, ema_fast, ema_slow, _div15m_short_log)
            if not event or event.event != "ENTRY_READY":
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            date_tag = time.strftime("%Y%m%d")
            _append_entry_log(
                f"div15m_short/div15m_entries_{date_tag}.log",
                "engine=div15m_short side=SHORT symbol=%s price=%s qty=%s usdt=%s p1=%s p2=%s score=%.4f"
                % (
                    symbol,
                    f"{event.entry_px:.6g}" if isinstance(event.entry_px, (int, float)) else "N/A",
                    "N/A",
                    f"{_resolve_entry_usdt():.2f}",
                    event.p1_idx,
                    event.p2_idx,
                    float(event.score or 0.0),
                ),
            )
            _div15m_short_csv(event)
            order_info = "(알림 전용)"
            entry_price_disp = None
            if LIVE_TRADING:
                guard_key = _entry_guard_key(state, symbol, "SHORT")
                if not _entry_guard_acquire(state, symbol, key=guard_key, engine="div15m_short", side="SHORT"):
                    print(f"[entry] 숏 중복 차단 ({symbol})")
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                if _entry_seen_blocked(state, symbol, "SHORT", "div15m_short"):
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                try:
                    res = short_market(
                        symbol,
                        usdt_amount=_resolve_entry_usdt(),
                        leverage=LEVERAGE,
                        margin_mode=MARGIN_MODE,
                    )
                    entry_order_id = _order_id_from_res(res)
                    fill_price = res.get("last") or res.get("order", {}).get("average") or res.get("order", {}).get("price")
                    qty = res.get("amount") or res.get("order", {}).get("amount")
                    order_info = (
                        f"entry_price={fill_price} qty={qty} usdt={_resolve_entry_usdt()}"
                    )
                    entry_price_disp = fill_price
                    if res.get("order") or res.get("status") in ("ok", "dry_run"):
                        _entry_seen_mark(state, symbol, "SHORT", "div15m_short")
                    st["in_pos"] = True
                    _log_trade_entry(
                        state,
                        side="SHORT",
                        symbol=symbol,
                        entry_ts=time.time(),
                        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=_resolve_entry_usdt(),
                        entry_order_id=entry_order_id,
                        meta={"reason": "div15m_short"},
                    )
                    _append_entry_log(
                        f"div15m_short/div15m_entries_{date_tag}.log",
                        "engine=div15m_short side=SHORT symbol=%s price=%s qty=%s usdt=%s p1=%s p2=%s score=%.4f"
                        % (
                            symbol,
                            f"{fill_price:.6g}" if isinstance(fill_price, (int, float)) else "N/A",
                            f"{qty:.6g}" if isinstance(qty, (int, float)) else "N/A",
                            f"{_resolve_entry_usdt():.2f}",
                            event.p1_idx,
                            event.p2_idx,
                            float(event.score or 0.0),
                        ),
                    )
                    active_positions += 1
                except Exception as e:
                    order_info = f"order_error: {e}"
                finally:
                    _entry_guard_release(state, symbol, key=guard_key)

            _set_last_entry_state(st, "SHORT", time.time())
            div15m_short_bucket[symbol] = st
            _send_entry_alert(
                send_telegram,
                side="SHORT",
                symbol=symbol,
                engine="DIV15M_SHORT",
                entry_price=entry_price_disp,
                usdt=_resolve_entry_usdt(),
                reason=f"trigger={event.reasons or 'ENTRY_READY'}",
                live=LIVE_TRADING,
                order_info=order_info,
                entry_order_id=entry_order_id,
                sl=_fmt_price_safe(entry_price_disp, AUTO_EXIT_SHORT_SL_PCT, side="SHORT"),
                tp=None,
            )
            time.sleep(PER_SYMBOL_SLEEP)
        if int(time.time()) % 30 == 0:
            _reload_runtime_settings_from_disk(state)
            save_state(state)

            time.sleep(PER_SYMBOL_SLEEP)

        if atlasfabio_thread:
            atlasfabio_thread.join()
        if swaggy_thread:
            swaggy_thread.join()
        if swaggy_atlas_lab_thread:
            swaggy_atlas_lab_thread.join()
        if dtfx_thread:
            dtfx_thread.join()
        if pumpfade_thread:
            pumpfade_thread.join()
        if atlas_rs_fail_short_thread:
            atlas_rs_fail_short_thread.join()
        _set_thread_log_buffer(None)

        if (not MANAGE_LOOP_ENABLED) and (not MANAGE_WS_MODE):
            _reconcile_long_trades(state, cached_long_ex, tickers)
            _reconcile_short_trades(state, tickers)
        _reload_runtime_settings_from_disk(state)
        save_state(state)
        _prune_trade_log(state)
        prune_ohlcv_cache()
        tally_msg = " | ".join(f"{k}개:{v}" for k, v in sorted(pass_tally.items()))
        cache_msg = " ".join(
            [
                f"hit[{tf}]={CURRENT_CYCLE_STATS.get('cache_hits_by_tf',{}).get(tf,0)}" for tf in ["1h","15m","5m","3m"]
            ] + [
                f"miss[{tf}]={CURRENT_CYCLE_STATS.get('cache_miss_by_tf',{}).get(tf,0)}" for tf in ["1h","15m","5m","3m"]
            ]
        )
        elapsed = time.time() - cycle_start
        try:
            CURRENT_CYCLE_STATS["elapsed"] = elapsed
        except Exception:
            pass
        print_section("사이클 요약")
        anchors_disp = ",".join(anchors)
        print(
            f"[universe] rule=qVol>={int(shared_min_qv):,} sort=abs(pct) topN={shared_top_n} anchors={anchors_disp} "
            f"shared={shared_universe_len} rsi={rsi_universe_len} struct={universe_structure_len} "
            f"dtfx={dtfx_universe_len} "
            f"pumpfade={pumpfade_universe_len} arsf={atlas_rs_fail_short_universe_len} union={universe_union_len}"
        )
        print(
            "[engines] rsi=%s(%d) div15m_long=%s(%d) div15m_short=%s(%d) atlasfabio=%s(%d) "
            "swaggy_atlas_lab=%s(%d) dtfx=%s(%d) pumpfade=%s(%d) arsf=%s(%d)"
            % (
                "ON" if rsi_ran else "OFF",
                rsi_universe_len,
                "ON" if div15m_long_ran else "OFF",
                div15m_universe_len,
                "ON" if div15m_short_ran else "OFF",
                div15m_short_universe_len,
                "ON" if atlasfabio_ran else "OFF",
                fabio_universe_len,
                "ON" if swaggy_atlas_lab_ran else "OFF",
                swaggy_atlas_lab_universe_len,
                "ON" if dtfx_ran else "OFF",
                dtfx_universe_len,
                "ON" if pumpfade_ran else "OFF",
                pumpfade_universe_len,
                "ON" if atlas_rs_fail_short_ran else "OFF",
                atlas_rs_fail_short_universe_len,
            )
        )
        print(f"[cycle] heavy_scan={'Y' if heavy_scan else 'N'} elapsed={elapsed:.2f}s")
        rest_calls = int(CURRENT_CYCLE_STATS.get("rest_calls", 0) or 0)
        rest_fails = int(CURRENT_CYCLE_STATS.get("rest_fails", 0) or 0)
        rest_429 = int(CURRENT_CYCLE_STATS.get("rest_429", 0) or 0)
        TOTAL_CYCLES += 1
        TOTAL_ELAPSED += elapsed
        TOTAL_REST_CALLS += rest_calls
        TOTAL_429_COUNT += rest_429
        avg_cycle = (TOTAL_ELAPSED / TOTAL_CYCLES) if TOTAL_CYCLES > 0 else 0.0
        avg_rest = (TOTAL_REST_CALLS / TOTAL_CYCLES) if TOTAL_CYCLES > 0 else 0.0
        avg_429 = (TOTAL_429_COUNT / TOTAL_CYCLES) if TOTAL_CYCLES > 0 else 0.0
        # daily report at 09:30 KST for previous day
        now_kst = _kst_now()
        today_kst = now_kst.strftime("%Y-%m-%d")
        last_report_date = state.get("_daily_report_date")
        if (now_kst.hour > 9 or (now_kst.hour == 9 and now_kst.minute >= 30)) and last_report_date != today_kst:
            report_guard = os.path.join("reports", f"daily_report_{today_kst}.done")
            try:
                os.makedirs("reports", exist_ok=True)
                fd = os.open(report_guard, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                report_guard = None
            except Exception:
                report_guard = None
            if not report_guard:
                time.sleep(CYCLE_SLEEP)
                continue
            report_date = (now_kst - timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                use_db = bool(dbrec and dbrec.ENABLED and dbpnl)
                if not use_db:
                    _sync_report_with_api(state, report_date)
            except Exception as e:
                print(f"[report-api] daily sync failed report_date={report_date} err={e}")
            report_msg = _build_daily_report(state, report_date, compact=True)
            send_telegram(report_msg)
            state["_daily_report_date"] = today_kst
            _reload_runtime_settings_from_disk(state)
            save_state(state)
        time.sleep(CYCLE_SLEEP)

if __name__ == "__main__":
    run()
