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
from contextlib import contextmanager, nullcontext
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
import accounts_db
from account_context import AccountContext, AccountSettings
from telegram_client import TelegramClient
import manage_queue

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
from executor import AccountExecutor
from env_loader import load_env
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
    from engines.swaggy_atlas_lab_v2.swaggy_signal import SwaggySignalEngine as SwaggyAtlasLabV2Engine
    from engines.swaggy_atlas_lab_v2.config import SwaggyConfig as SwaggyAtlasLabV2Config
    from engines.swaggy_atlas_lab_v2.config import AtlasConfig as SwaggyAtlasLabV2AtlasConfig
    from engines.swaggy_no_atlas.engine import SwaggyNoAtlasEngine
    from engines.swaggy_no_atlas.config import SwaggyNoAtlasConfig
    from engines.swaggy_atlas_lab.atlas_eval import (
        evaluate_global_gate as lab_evaluate_global_gate,
        evaluate_local as lab_evaluate_local,
    )
    from engines.swaggy_atlas_lab_v2.atlas_eval import (
        evaluate_global_gate as lab_v2_evaluate_global_gate,
        evaluate_local as lab_v2_evaluate_local,
    )
    from engines.swaggy_atlas_lab.policy import apply_policy as lab_apply_policy
    from engines.swaggy_atlas_lab.policy import AtlasMode as SwaggyAtlasLabMode
    from engines.swaggy_atlas_lab_v2.policy import apply_policy as lab_v2_apply_policy
    from engines.swaggy_atlas_lab_v2.policy import AtlasMode as SwaggyAtlasLabV2Mode
    from engines.atlas.atlas_engine import AtlasEngine, AtlasSwaggyConfig
    from engines.rsi.engine import RsiEngine
    from engines.universe import build_universe_from_tickers
    from engines.dtfx.engine import DTFXEngine, DTFXConfig
    from engines.dtfx.core.logger import write_dtfx_log
    from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine
    from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig
    from engines.swaggy_atlas_lab.indicators import atr
except Exception as _import_err:
    SwaggyEngine = None
    SwaggyConfig = None
    EngineContext = None
    format_cut_top = None
    format_zone_stats = None
    SwaggyAtlasLabEngine = None
    SwaggyAtlasLabConfig = None
    SwaggyAtlasLabAtlasConfig = None
    SwaggyAtlasLabV2Engine = None
    SwaggyAtlasLabV2Config = None
    SwaggyAtlasLabV2AtlasConfig = None
    SwaggyNoAtlasEngine = None
    SwaggyNoAtlasConfig = None
    lab_evaluate_global_gate = None
    lab_evaluate_local = None
    lab_apply_policy = None
    SwaggyAtlasLabMode = None
    lab_v2_evaluate_global_gate = None
    lab_v2_evaluate_local = None
    lab_v2_apply_policy = None
    SwaggyAtlasLabV2Mode = None
    _IMPORT_ERROR = str(_import_err)
    try:
        print(f"[import-error] { _IMPORT_ERROR }")
    except Exception:
        pass

if "Div15mLongEngine" not in globals():
    Div15mLongEngine = None
if "Div15mShortEngine" not in globals():
    Div15mShortEngine = None

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
    close_long_market_qty,
    close_short_market_qty,
    place_long_sl_px,
    place_short_sl_px,
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
    BASE_ENTRY_USDT,
    set_dry_run,
    get_global_backoff_until,
    refresh_positions_cache,
    get_available_usdt,
    exchange,
)

_EXEC_SHORT_MARKET = short_market
_EXEC_SHORT_LIMIT = short_limit
_EXEC_LONG_MARKET = long_market
_EXEC_CLOSE_SHORT_MARKET = close_short_market
_EXEC_CLOSE_LONG_MARKET = close_long_market
_EXEC_CLOSE_LONG_MARKET_QTY = close_long_market_qty
_EXEC_CLOSE_SHORT_MARKET_QTY = close_short_market_qty
_EXEC_PLACE_LONG_SL_PX = place_long_sl_px
_EXEC_PLACE_SHORT_SL_PX = place_short_sl_px
_EXEC_CANCEL_OPEN_ORDERS = cancel_open_orders
_EXEC_CANCEL_STOP_ORDERS = cancel_stop_orders
_EXEC_CANCEL_CONDITIONAL_BY_SIDE = cancel_conditional_by_side
_EXEC_DCA_SHORT_IF_NEEDED = dca_short_if_needed
_EXEC_DCA_LONG_IF_NEEDED = dca_long_if_needed

swaggy_engine = None
swaggy_atlas_lab_engine = None
swaggy_atlas_lab_v2_engine = None
swaggy_no_atlas_engine = None
atlas_engine = None
atlas_swaggy_cfg = None
rsi_engine = None
dtfx_engine = None
atlas_rs_fail_short_engine = None
div15m_engine = None
div15m_short_engine = None

load_env()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
# 런타임 전송용 chat_id (state에서 복원/업데이트)
CHAT_ID_RUNTIME = CHAT_ID
START_TIME = time.time()
TELEGRAM_STARTUP_GRACE_SEC = 15.0
EXIT_ICON = os.getenv("EXIT_ICON", "😄😄😄")
EXIT_SL_ICON = os.getenv("EXIT_SL_ICON", "🚨🚨🚨")
MIN_LISTING_AGE_DAYS = float(os.getenv("MIN_LISTING_AGE_DAYS", "14"))
MANAGE_QUEUE_PENDING_TTL_SEC = float(os.getenv("MANAGE_QUEUE_PENDING_TTL_SEC", "600"))
MANUAL_ALERT_TTL_SEC = float(os.getenv("MANUAL_ALERT_TTL_SEC", "3600"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "20"))
USDT_PER_TRADE = float(os.getenv("ENTRY_USDT_PCT", "8.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
MARGIN_MODE = os.getenv("MARGIN_MODE", "cross").lower().strip()
POS_CHECK_MIN_SEC = float(os.getenv("POS_CHECK_MIN_SEC", "20"))
ENGINE_WRITE_STATE = os.getenv("ENGINE_WRITE_STATE", "1") == "1"
LOG_LONG_EXIT = os.getenv("LOG_LONG_EXIT", "0") == "1"
LIVE_TRADING = True
LONG_LIVE_TRADING = True
ACCOUNT_REFRESH_SEC = float(os.getenv("ACCOUNT_REFRESH_SEC", "60"))
_LAST_ACCOUNT_REFRESH_TS = 0.0

ADMIN_ACCOUNT_CONTEXT: Optional[AccountContext] = None
FOLLOWER_CONTEXTS: List[AccountContext] = []
_LAST_ENTRY_BROADCAST = {"ts": 0.0, "symbol": None, "side": None, "line": None}
_ENTRY_BROADCAST_TTL_SEC = float(os.getenv("ENTRY_BROADCAST_TTL_SEC", "120"))

BROADCAST_RETRY_MAX = int(os.getenv("BROADCAST_RETRY_MAX", "2"))
BROADCAST_RETRY_BASE_SEC = float(os.getenv("BROADCAST_RETRY_BASE_SEC", "0.7"))
BROADCAST_RETRY_BACKOFF = float(os.getenv("BROADCAST_RETRY_BACKOFF", "1.7"))
BROADCAST_NOTIFY = os.getenv("BROADCAST_NOTIFY", "1") == "1"
BROADCAST_NOTIFY_ACTIONS = os.getenv(
    "BROADCAST_NOTIFY_ACTIONS",
    "long_market,short_market,close_long_market,close_short_market",
)

def _parse_action_allowlist(val: str) -> set:
    if not isinstance(val, str):
        return set()
    items = [s.strip() for s in val.split(",") if s.strip()]
    return set(items)

def _infer_symbol_from_args(args, kwargs) -> Optional[str]:
    if "symbol" in kwargs:
        return kwargs.get("symbol")
    if args:
        return args[0]
    return None

def _extract_status(res: object) -> str:
    if isinstance(res, dict) and "status" in res:
        try:
            return str(res.get("status") or "ok")
        except Exception:
            return "ok"
    return "ok"

def _call_with_retry(fn):
    last_err = None
    delay = BROADCAST_RETRY_BASE_SEC
    for attempt in range(BROADCAST_RETRY_MAX + 1):
        try:
            res = fn()
            status = _extract_status(res)
            ok = status in ("ok", "dry_run", "skip")
            return {"ok": ok, "status": status, "res": res, "err": None, "attempts": attempt + 1}
        except Exception as e:
            last_err = e
            if attempt < BROADCAST_RETRY_MAX:
                time.sleep(delay)
                delay *= BROADCAST_RETRY_BACKOFF
    return {"ok": False, "status": "error", "res": None, "err": str(last_err), "attempts": BROADCAST_RETRY_MAX + 1}

def _refresh_admin_settings_from_db(state: Optional[dict] = None) -> None:
    global MAX_OPEN_POSITIONS, USDT_PER_TRADE, LEVERAGE, MARGIN_MODE
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT
    global AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT, DCA_ENABLED, DCA_PCT
    global DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT, EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC
    global LIVE_TRADING
    try:
        admin_id = accounts_db.get_account_id_by_name("admin")
    except Exception:
        admin_id = None
    if not admin_id:
        return
    try:
        settings = accounts_db.get_account_settings(admin_id)
    except Exception:
        return
    if not settings:
        return
    try:
        entry_pct = settings.get("entry_pct")
        if isinstance(entry_pct, (int, float)) and entry_pct > 0:
            USDT_PER_TRADE = float(entry_pct)
            if isinstance(state, dict):
                state["_entry_usdt"] = USDT_PER_TRADE
        max_pos = int(settings.get("max_positions") or 0)
        if max_pos > 0:
            MAX_OPEN_POSITIONS = max_pos
            if isinstance(state, dict):
                state["_max_open_positions"] = max_pos
        leverage = settings.get("leverage")
        if isinstance(leverage, (int, float)) and int(leverage) > 0:
            LEVERAGE = int(leverage)
        margin_mode = settings.get("margin_mode")
        if isinstance(margin_mode, str) and margin_mode.strip():
            MARGIN_MODE = margin_mode.strip()
        auto_exit = settings.get("auto_exit")
        if isinstance(auto_exit, (int, float, bool)):
            AUTO_EXIT_ENABLED = bool(auto_exit)
            if isinstance(state, dict):
                state["_auto_exit"] = AUTO_EXIT_ENABLED
        long_tp = settings.get("long_tp_pct")
        if isinstance(long_tp, (int, float)):
            AUTO_EXIT_LONG_TP_PCT = float(long_tp)
            if isinstance(state, dict):
                state["_auto_exit_long_tp_pct"] = AUTO_EXIT_LONG_TP_PCT
        long_sl = settings.get("long_sl_pct")
        if isinstance(long_sl, (int, float)):
            AUTO_EXIT_LONG_SL_PCT = float(long_sl)
            if isinstance(state, dict):
                state["_auto_exit_long_sl_pct"] = AUTO_EXIT_LONG_SL_PCT
        short_tp = settings.get("short_tp_pct")
        if isinstance(short_tp, (int, float)):
            AUTO_EXIT_SHORT_TP_PCT = float(short_tp)
            if isinstance(state, dict):
                state["_auto_exit_short_tp_pct"] = AUTO_EXIT_SHORT_TP_PCT
        short_sl = settings.get("short_sl_pct")
        if isinstance(short_sl, (int, float)):
            AUTO_EXIT_SHORT_SL_PCT = float(short_sl)
            if isinstance(state, dict):
                state["_auto_exit_short_sl_pct"] = AUTO_EXIT_SHORT_SL_PCT
        dca_enabled = settings.get("dca_enabled")
        if isinstance(dca_enabled, (int, float, bool)):
            DCA_ENABLED = bool(dca_enabled)
            if isinstance(state, dict):
                state["_dca_enabled"] = DCA_ENABLED
        dca_pct = settings.get("dca_pct")
        if isinstance(dca_pct, (int, float)):
            DCA_PCT = float(dca_pct)
            if isinstance(state, dict):
                state["_dca_pct"] = DCA_PCT
        dca1 = settings.get("dca1_pct")
        if isinstance(dca1, (int, float)):
            DCA_FIRST_PCT = float(dca1)
            if isinstance(state, dict):
                state["_dca_first_pct"] = DCA_FIRST_PCT
        dca2 = settings.get("dca2_pct")
        if isinstance(dca2, (int, float)):
            DCA_SECOND_PCT = float(dca2)
            if isinstance(state, dict):
                state["_dca_second_pct"] = DCA_SECOND_PCT
        dca3 = settings.get("dca3_pct")
        if isinstance(dca3, (int, float)):
            DCA_THIRD_PCT = float(dca3)
            if isinstance(state, dict):
                state["_dca_third_pct"] = DCA_THIRD_PCT
        exit_cd = settings.get("exit_cooldown_h")
        if isinstance(exit_cd, (int, float)):
            EXIT_COOLDOWN_HOURS = float(exit_cd)
            EXIT_COOLDOWN_SEC = int(float(exit_cd) * 3600)
            if isinstance(state, dict):
                state["_exit_cooldown_hours"] = EXIT_COOLDOWN_HOURS
        dry_run = settings.get("dry_run")
        if isinstance(dry_run, (int, float, bool)):
            LIVE_TRADING = not bool(dry_run)
            if isinstance(state, dict):
                state["_live_trading"] = LIVE_TRADING
    except Exception:
        return

def _send_broadcast_summary(action_name: str, meta: dict, results: list) -> None:
    if not BROADCAST_NOTIFY:
        return
    allowlist = _parse_action_allowlist(BROADCAST_NOTIFY_ACTIONS)
    if allowlist and action_name not in allowlist:
        return
    try:
        symbol = meta.get("symbol") or ""
        title = f"📣 <b>Broadcast</b> {action_name}"
        if symbol:
            title += f" <b>{symbol}</b>"
        lines = [title]
        for r in results:
            acct = r.get("acct") or "unknown"
            status = r.get("status") or "unknown"
            ok = r.get("ok")
            attempts = r.get("attempts")
            err = r.get("err")
            if status == "skip_no_pos":
                line = f"- {acct}: SKIP (no position)"
            elif ok:
                line = f"- {acct}: OK (status={status}"
                if attempts and attempts > 1:
                    line += f", tries={attempts}"
                line += ")"
            else:
                line = f"- {acct}: FAIL (status={status}"
                if attempts and attempts > 1:
                    line += f", tries={attempts}"
                line += ")"
                if err:
                    line += f" err={err}"
            lines.append(line)
        send_telegram("\n".join(lines))
    except Exception:
        return

def _broadcast_followers(action_name: str, follower_calls: list, meta: dict) -> list:
    results = []
    for item in follower_calls:
        acct = item.get("acct")
        if item.get("skip") == "no_pos":
            results.append({"acct": acct.name if acct else "unknown", "ok": True, "status": "skip_no_pos", "attempts": 0})
            continue
        fn = item.get("fn")
        if not fn:
            results.append({"acct": acct.name if acct else "unknown", "ok": False, "status": "error", "attempts": 0, "err": "no_fn"})
            continue
        out = _call_with_retry(fn)
        _record_follower_entry(action_name, acct, out, meta)
        out["acct"] = acct.name if acct else "unknown"
        results.append(out)
    if results:
        _send_broadcast_summary(action_name, meta, results)
    return results

def _record_follower_entry(action_name: str, acct: Optional[AccountContext], out: dict, meta: dict) -> None:
    if not acct or not isinstance(out, dict):
        return
    if action_name not in ("long_market", "short_market"):
        return
    status = str(out.get("status") or "")
    if status not in ("ok", "dry_run"):
        return
    res = out.get("res") if isinstance(out.get("res"), dict) else {}
    symbol = res.get("symbol") or (meta.get("symbol") if isinstance(meta, dict) else None)
    if not symbol:
        return
    side = "LONG" if action_name == "long_market" else "SHORT"
    entry_ts = time.time()
    entry_price = res.get("last")
    if entry_price is None:
        order = res.get("order") if isinstance(res.get("order"), dict) else {}
        entry_price = order.get("average") or order.get("price")
    qty = res.get("amount")
    if qty is None:
        order = res.get("order") if isinstance(res.get("order"), dict) else {}
        qty = order.get("amount")
    usdt = res.get("usdt")
    entry_order_id = _order_id_from_res(res)
    follower_state = load_state_from(acct.state_path)
    _log_trade_entry(
        follower_state,
        side=side,
        symbol=symbol,
        entry_ts=entry_ts,
        entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
        qty=qty if isinstance(qty, (int, float)) else None,
        usdt=usdt if isinstance(usdt, (int, float)) else None,
        entry_order_id=entry_order_id,
        meta={"reason": "broadcast_follow", "engine": "BROADCAST"},
    )
    save_state_to(follower_state, acct.state_path)

def _status_to_kr_label(status: str, ok: bool) -> str:
    if status in ("skip_no_pos", "skip"):
        return "스킵"
    if status == "dry_run":
        return "성공"
    if ok:
        return "성공"
    return "실패"

def _set_last_entry_broadcast(symbol: str, side: str, admin_status: str, admin_ok: bool, follower_results: list) -> None:
    global _LAST_ENTRY_BROADCAST
    if not symbol:
        return
    admin_label = ADMIN_ACCOUNT_CONTEXT.name if ADMIN_ACCOUNT_CONTEXT else "admin"
    entries = []
    admin_suffix = "" if admin_ok else "(실패)"
    entries.append(f"{admin_label}{admin_suffix}")
    for r in follower_results or []:
        acct = r.get("acct") or "unknown"
        status = r.get("status") or "unknown"
        ok = bool(r.get("ok"))
        label = _status_to_kr_label(status, ok)
        if label == "성공":
            entries.append(f"{acct}(성공)")
        elif label == "스킵":
            entries.append(f"{acct}(스킵)")
        else:
            entries.append(f"{acct}(실패)")
    _LAST_ENTRY_BROADCAST = {
        "ts": time.time(),
        "symbol": symbol,
        "side": (side or "").upper(),
        "line": "진입 여부 : " + ", ".join(entries),
    }

def _consume_entry_broadcast_line(symbol: str, side: str) -> Optional[str]:
    global _LAST_ENTRY_BROADCAST
    try:
        if not _LAST_ENTRY_BROADCAST.get("line"):
            return None
        if (time.time() - float(_LAST_ENTRY_BROADCAST.get("ts") or 0.0)) > _ENTRY_BROADCAST_TTL_SEC:
            return None
        if _LAST_ENTRY_BROADCAST.get("symbol") != symbol:
            return None
        if _LAST_ENTRY_BROADCAST.get("side") != (side or "").upper():
            return None
        line = _LAST_ENTRY_BROADCAST.get("line")
        _LAST_ENTRY_BROADCAST = {"ts": 0.0, "symbol": None, "side": None, "line": None}
        return line
    except Exception:
        return None

def _realtime_only_required() -> bool:
    # realtime-only에서 의미 있는 엔진들만 체크
    if LOSS_HEDGE_ENGINE_ENABLED:
        return True
    if RSI_ENABLED:
        return True
    if DTFX_ENABLED:
        return True
    if DIV15M_LONG_ENABLED or DIV15M_SHORT_ENABLED or ONLY_DIV15M_SHORT:
        return True
    return False

def _broadcast_exec(action_name: str, exec_fn, *args, **kwargs):
    res = exec_fn(*args, **kwargs)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            follower_fn = getattr(acct.executor, action_name)
            follower_calls.append({"acct": acct, "fn": lambda f=follower_fn: f(*args, **kwargs)})
        except Exception:
            follower_calls.append({"acct": acct, "fn": None})
    _broadcast_followers(action_name, follower_calls, {"symbol": _infer_symbol_from_args(args, kwargs)})
    return res

def _resolve_entry_usdt_for_executor(executor: AccountExecutor, pct: Optional[float]) -> float:
    global _LAST_ENTRY_PCT_USED, _LAST_ENTRY_USDT_USED, _LAST_ENTRY_PCT_TS
    prev_pct = _LAST_ENTRY_PCT_USED
    prev_usdt = _LAST_ENTRY_USDT_USED
    prev_ts = _LAST_ENTRY_PCT_TS
    with executor.activate():
        val = _resolve_entry_usdt(pct)
    _LAST_ENTRY_PCT_USED = prev_pct
    _LAST_ENTRY_USDT_USED = prev_usdt
    _LAST_ENTRY_PCT_TS = prev_ts
    return val

def short_market(symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = LEVERAGE, margin_mode: str = "isolated") -> dict:
    if not _admin_is_active():
        _set_last_entry_broadcast(symbol, "SHORT", "skip", False, [])
        return {"status": "skip", "reason": "admin_inactive", "symbol": symbol}
    try:
        res = _EXEC_SHORT_MARKET(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)
    except Exception:
        _set_last_entry_broadcast(symbol, "SHORT", "error", False, [])
        raise
    followers = FOLLOWER_CONTEXTS
    if not followers:
        admin_status = _extract_status(res)
        admin_ok = admin_status in ("ok", "dry_run", "skip")
        _set_last_entry_broadcast(symbol, "SHORT", admin_status, admin_ok, [])
        return res
    use_pct = _calc_effective_entry_pct(usdt_amount)
    follower_calls = []
    active_names = _active_account_names()
    for acct in followers:
        if active_names and str(acct.name) not in active_names:
            follower_calls.append({"acct": acct, "skip": "inactive"})
            continue
        follower_usdt = usdt_amount
        if use_pct is not None:
            follower_usdt = _resolve_entry_usdt_for_executor(acct.executor, use_pct)
        follower_calls.append({
            "acct": acct,
            "fn": lambda a=acct, u=follower_usdt: a.executor.short_market(symbol, usdt_amount=u, leverage=leverage, margin_mode=margin_mode),
        })
    results = _broadcast_followers("short_market", follower_calls, {"symbol": symbol})
    admin_status = _extract_status(res)
    admin_ok = admin_status in ("ok", "dry_run", "skip")
    _set_last_entry_broadcast(symbol, "SHORT", admin_status, admin_ok, results)
    return res

def short_limit(*args, **kwargs) -> dict:
    try:
        res = _EXEC_SHORT_LIMIT(*args, **kwargs)
    except Exception:
        sym_err = args[0] if len(args) >= 1 else kwargs.get("symbol")
        _set_last_entry_broadcast(sym_err, "SHORT", "error", False, [])
        raise
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    sym = args[0] if len(args) >= 1 else kwargs.get("symbol")
    usdt_amount = kwargs.get("usdt_amount")
    if usdt_amount is None and len(args) >= 3:
        usdt_amount = args[2]
    use_pct = _calc_effective_entry_pct(usdt_amount)
    follower_calls = []
    active_names = _active_account_names()
    for acct in followers:
        if active_names and str(acct.name) not in active_names:
            follower_calls.append({"acct": acct, "skip": "inactive"})
            continue
        follower_usdt = usdt_amount
        if use_pct is not None:
            follower_usdt = _resolve_entry_usdt_for_executor(acct.executor, use_pct)
        price = args[1] if len(args) >= 2 else kwargs.get("price")
        if sym is None or price is None:
            follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.short_limit(*args, **kwargs)})
            continue
        new_kwargs = dict(kwargs)
        new_kwargs.pop("symbol", None)
        new_kwargs.pop("price", None)
        new_kwargs["usdt_amount"] = follower_usdt if follower_usdt is not None else new_kwargs.get("usdt_amount")
        follower_calls.append({"acct": acct, "fn": lambda a=acct, s=sym, p=price, kw=new_kwargs: a.executor.short_limit(s, p, **kw)})
    results = _broadcast_followers("short_limit", follower_calls, {"symbol": sym or _infer_symbol_from_args(args, kwargs)})
    admin_status = _extract_status(res)
    admin_ok = admin_status in ("ok", "dry_run", "skip")
    _set_last_entry_broadcast(sym or _infer_symbol_from_args(args, kwargs), "SHORT", admin_status, admin_ok, results)
    return res

def long_market(symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = LEVERAGE, margin_mode: str = "isolated") -> dict:
    if not _admin_is_active():
        _set_last_entry_broadcast(symbol, "LONG", "skip", False, [])
        return {"status": "skip", "reason": "admin_inactive", "symbol": symbol}
    try:
        res = _EXEC_LONG_MARKET(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)
    except Exception:
        _set_last_entry_broadcast(symbol, "LONG", "error", False, [])
        raise
    followers = FOLLOWER_CONTEXTS
    if not followers:
        admin_status = _extract_status(res)
        admin_ok = admin_status in ("ok", "dry_run", "skip")
        _set_last_entry_broadcast(symbol, "LONG", admin_status, admin_ok, [])
        return res
    use_pct = _calc_effective_entry_pct(usdt_amount)
    follower_calls = []
    active_names = _active_account_names()
    for acct in followers:
        if active_names and str(acct.name) not in active_names:
            follower_calls.append({"acct": acct, "skip": "inactive"})
            continue
        follower_usdt = usdt_amount
        if use_pct is not None:
            follower_usdt = _resolve_entry_usdt_for_executor(acct.executor, use_pct)
        follower_calls.append({
            "acct": acct,
            "fn": lambda a=acct, u=follower_usdt: a.executor.long_market(symbol, usdt_amount=u, leverage=leverage, margin_mode=margin_mode),
        })
    results = _broadcast_followers("long_market", follower_calls, {"symbol": symbol})
    admin_status = _extract_status(res)
    admin_ok = admin_status in ("ok", "dry_run", "skip")
    _set_last_entry_broadcast(symbol, "LONG", admin_status, admin_ok, results)
    return res

def close_short_market(symbol: str) -> dict:
    res = _EXEC_CLOSE_SHORT_MARKET(symbol)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_short_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_short_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.close_short_market(symbol)})
    _broadcast_followers("close_short_market", follower_calls, {"symbol": symbol})
    return res

def close_long_market(symbol: str) -> dict:
    res = _EXEC_CLOSE_LONG_MARKET(symbol)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_long_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_long_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.close_long_market(symbol)})
    _broadcast_followers("close_long_market", follower_calls, {"symbol": symbol})
    return res

def close_long_market_qty(symbol: str, qty: float) -> dict:
    res = _EXEC_CLOSE_LONG_MARKET_QTY(symbol, qty)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_long_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_long_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.close_long_market_qty(symbol, qty)})
    _broadcast_followers("close_long_market_qty", follower_calls, {"symbol": symbol})
    return res

def close_short_market_qty(symbol: str, qty: float) -> dict:
    res = _EXEC_CLOSE_SHORT_MARKET_QTY(symbol, qty)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_short_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_short_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.close_short_market_qty(symbol, qty)})
    _broadcast_followers("close_short_market_qty", follower_calls, {"symbol": symbol})
    return res

def place_long_sl_px(symbol: str, stop_price: float, qty: Optional[float] = None) -> dict:
    res = _EXEC_PLACE_LONG_SL_PX(symbol, stop_price, qty=qty)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_long_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_long_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.place_long_sl_px(symbol, stop_price, qty=qty)})
    _broadcast_followers("place_long_sl_px", follower_calls, {"symbol": symbol})
    return res

def place_short_sl_px(symbol: str, stop_price: float, qty: Optional[float] = None) -> dict:
    res = _EXEC_PLACE_SHORT_SL_PX(symbol, stop_price, qty=qty)
    followers = FOLLOWER_CONTEXTS
    if not followers:
        return res
    follower_calls = []
    for acct in followers:
        try:
            amt = acct.executor.get_short_position_amount(symbol)
            if isinstance(amt, (int, float)) and amt <= 0:
                try:
                    acct.executor.refresh_positions_cache(force=True)
                    amt = acct.executor.get_short_position_amount(symbol)
                except Exception:
                    amt = amt
            if isinstance(amt, (int, float)) and amt <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
        except Exception:
            pass
        follower_calls.append({"acct": acct, "fn": lambda a=acct: a.executor.place_short_sl_px(symbol, stop_price, qty=qty)})
    _broadcast_followers("place_short_sl_px", follower_calls, {"symbol": symbol})
    return res

def cancel_open_orders(symbol: str) -> dict:
    return _broadcast_exec("cancel_open_orders", _EXEC_CANCEL_OPEN_ORDERS, symbol)

def cancel_stop_orders(symbol: str) -> dict:
    return _broadcast_exec("cancel_stop_orders", _EXEC_CANCEL_STOP_ORDERS, symbol)

def cancel_conditional_by_side(symbol: str, side: str) -> dict:
    return _broadcast_exec("cancel_conditional_by_side", _EXEC_CANCEL_CONDITIONAL_BY_SIDE, symbol, side)

def dca_short_if_needed(symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
    return _broadcast_exec("dca_short_if_needed", _EXEC_DCA_SHORT_IF_NEEDED, symbol, adds_done, margin_mode)

def dca_long_if_needed(symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
    return _broadcast_exec("dca_long_if_needed", _EXEC_DCA_LONG_IF_NEEDED, symbol, adds_done, margin_mode)
AUTO_EXIT_ENABLED: bool = True
AUTO_EXIT_LONG_TP_PCT: float = 3.0
AUTO_EXIT_LONG_SL_PCT: float = 3.0
AUTO_EXIT_SHORT_TP_PCT: float = 3.0
AUTO_EXIT_SHORT_SL_PCT: float = 3.0
ENGINE_EXIT_OVERRIDES: dict = {}
SWAGGY_ENABLED = False
SWAGGY_ATLAS_LAB_ENABLED = False
SWAGGY_ATLAS_LAB_V2_ENABLED = False
SWAGGY_NO_ATLAS_ENABLED = False
SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN = -0.7
SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG = float(os.getenv("SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG", "-2.5"))
SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED = True
SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK = int(os.getenv("SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK", "3"))
SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS = int(os.getenv("SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS", "9"))
SWAGGY_NO_ATLAS_STRUCTURE_MIN_STRENGTH = float(os.getenv("SWAGGY_NO_ATLAS_STRUCTURE_MIN_STRENGTH", "0.45"))
SWAGGY_NO_ATLAS_USE_WICK_BREAK = str(os.getenv("SWAGGY_NO_ATLAS_USE_WICK_BREAK", "false")).strip().lower() in ("1", "true", "yes", "on")
SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG = float(os.getenv("SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG", "0.08"))
SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK = float(os.getenv("SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK", "0.02"))
SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN = float(os.getenv("SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN", "0.20"))
SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS = int(os.getenv("SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS", "9"))
SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR = float(os.getenv("SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR", "0.03"))
SWAGGY_NO_ATLAS_EXPANSION_MULT = float(os.getenv("SWAGGY_NO_ATLAS_EXPANSION_MULT", "1.2"))
SWAGGY_NO_ATLAS_DELAY_WAIT_BARS = int(os.getenv("SWAGGY_NO_ATLAS_DELAY_WAIT_BARS", "2"))
SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT = float(os.getenv("SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT", "0.0015"))
SWAGGY_NO_ATLAS_DELAY_VOL_MULT = float(os.getenv("SWAGGY_NO_ATLAS_DELAY_VOL_MULT", "1.2"))
SWAGGY_NO_ATLAS_DELAY_VOL_MA = int(os.getenv("SWAGGY_NO_ATLAS_DELAY_VOL_MA", "20"))
SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK = int(os.getenv("SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK", "3"))
SWAGGY_NO_ATLAS_DEBUG_SYMBOLS = os.getenv("SWAGGY_NO_ATLAS_DEBUG_SYMBOLS", "").strip()
ADV_TREND_ENABLED = os.getenv("ADV_TREND_ENABLED", "0") == "1"
ADV_TREND_MIN_QV = float(os.getenv("ADV_TREND_MIN_QV", "30000000"))
ADV_TREND_UNIVERSE_TOP_N = int(os.getenv("ADV_TREND_UNIVERSE_TOP_N", "50"))
ADV_TREND_RISK_PCT = float(os.getenv("ADV_TREND_RISK_PCT", "1.0"))
ADV_TREND_MAX_NOTIONAL_MULT = float(os.getenv("ADV_TREND_MAX_NOTIONAL_MULT", "10.0"))
ADV_TREND_MIN_STOP_ATR = float(os.getenv("ADV_TREND_MIN_STOP_ATR", "0.5"))
ADV_TREND_ADX_MIN = float(os.getenv("ADV_TREND_ADX_MIN", "25"))
ADV_TREND_MFI_LONG_MAX = float(os.getenv("ADV_TREND_MFI_LONG_MAX", "80"))
ADV_TREND_MFI_SHORT_MIN = float(os.getenv("ADV_TREND_MFI_SHORT_MIN", "20"))
ADV_TREND_ADX_LEN = int(os.getenv("ADV_TREND_ADX_LEN", "14"))
ADV_TREND_MFI_LEN = int(os.getenv("ADV_TREND_MFI_LEN", "14"))
ADV_TREND_EMA_LEN = int(os.getenv("ADV_TREND_EMA_LEN", "200"))
ADV_TREND_SUPER_ATR_LEN = int(os.getenv("ADV_TREND_SUPER_ATR_LEN", "10"))
ADV_TREND_SUPER_MULT = float(os.getenv("ADV_TREND_SUPER_MULT", "3.0"))
ADV_TREND_TP1_R_MULT = float(os.getenv("ADV_TREND_TP1_R_MULT", "1.5"))
ADV_TREND_TP1_FRACTION = float(os.getenv("ADV_TREND_TP1_FRACTION", "0.5"))
ADV_TREND_PULLBACK_RSI_LONG = float(os.getenv("ADV_TREND_PULLBACK_RSI_LONG", "45"))
ADV_TREND_PULLBACK_RSI_SHORT = float(os.getenv("ADV_TREND_PULLBACK_RSI_SHORT", "55"))
ADV_TREND_PULLBACK_WAIT_BARS = int(os.getenv("ADV_TREND_PULLBACK_WAIT_BARS", "4"))
ADV_TREND_PULLBACK_PIVOT = int(os.getenv("ADV_TREND_PULLBACK_PIVOT", "3"))
LOSS_HEDGE_ENGINE_ENABLED = False
LOSS_HEDGE_INTERVAL_MIN = 15
SWAGGY_ATLAS_LAB_OFF_WINDOWS = os.getenv("SWAGGY_ATLAS_LAB_OFF_WINDOWS", "").strip()
SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS = os.getenv("SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS", "").strip()
SWAGGY_NO_ATLAS_OFF_WINDOWS = os.getenv("SWAGGY_NO_ATLAS_OFF_WINDOWS", "").strip()
SWAGGY_D1_OVEREXT_ATR_MULT = 1.2
SATURDAY_TRADE_ENABLED = str(os.getenv("SATURDAY_TRADE_ENABLED", "1")).strip().lower() not in ("0", "false", "off", "no")
DTFX_ENABLED = True
ATLAS_RS_FAIL_SHORT_ENABLED = False
DIV15M_LONG_ENABLED = False
DIV15M_SHORT_ENABLED = False
ONLY_DIV15M_SHORT = False
RSI_ENABLED = True
MANAGE_EXIT_COOLDOWN_SEC: int = 5
MANAGE_PING_COOLDOWN_SEC: int = 7200
MANAGE_EVAL_COOLDOWN_SEC: int = 3
MANUAL_CLOSE_GRACE_SEC: int = 60
AUTO_EXIT_GRACE_SEC: int = 30
COOLDOWN_SEC: int = int(float(os.getenv("COOLDOWN_SEC", "1800")))
# Re-entry cooldown is managed by COOLDOWN_SEC only.
EXIT_COOLDOWN_HOURS: float = COOLDOWN_SEC / 3600.0
EXIT_COOLDOWN_SEC: int = COOLDOWN_SEC
_DISK_STATE_CACHE = {"ts": 0.0, "data": {}}
_DB_EXIT_CACHE = {"ts": 0.0, "data": {}}
MANAGE_LOOP_ENABLED: bool = True
MANAGE_LOOP_SLEEP_SEC: float = 2.0
MANAGE_TICKER_TTL_SEC: float = 5.0
RUNTIME_CONFIG_RELOAD_SEC: float = 5.0
MANAGE_WS_MODE: bool = False
SUPPRESS_RECONCILE_ALERTS: bool = True
REPORT_API_ONLY: bool = True
DB_RECONCILE_ENABLED: bool = os.getenv("DB_RECONCILE_ENABLED", "0") == "1"
DB_RECONCILE_SEC: float = float(os.getenv("DB_RECONCILE_SEC", "30"))
DB_RECONCILE_LOOKBACK_SEC: float = float(os.getenv("DB_RECONCILE_LOOKBACK_SEC", "3600"))
DB_RECONCILE_SYMBOLS_RAW = os.getenv("DB_RECONCILE_SYMBOLS", "").strip()
DB_REPORT_LOOKBACK_SEC: float = float(os.getenv("DB_REPORT_LOOKBACK_SEC", "259200"))
SHORT_POS_SAMPLE_DIV: int = 20
SHORT_POS_SAMPLE_RECENT_SEC: int = 120
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
STATE_SAVE_LOCK = threading.Lock()
SHORT_RECONCILE_GRACE_SEC: int = 60
SHORT_RECONCILE_EPS: float = 1e-8
SHORT_RECONCILE_ZERO_STREAK_N: int = 3
SHORT_RECONCILE_SEEN_TTL_SEC: int = 180
FAST_TF_PREFETCH_TOPN = 30
MAX_FAST_SYMBOLS = 30
FAST_LIMIT_CAP = 120
MID_LIMIT_CAP = 200
SLOW_LIMIT_CAP = 300
MID_TF_PREFETCH_EVERY_N = 3
TTL_4H_SEC = 1800
TTL_1D_SEC = 86400
FUNDING_INTERVAL_HOURS = 1
FUNDING_BLOCK_PCT = -0.2
FUNDING_TTL_SEC = 300
STRUCTURE_TOP_N = 30
PER_SYMBOL_SLEEP = 0.05
CYCLE_SLEEP = float(os.getenv("CYCLE_SLEEP", "1.0"))
REALTIME_CYCLE_SLEEP = float(os.getenv("REALTIME_CYCLE_SLEEP", "300"))
CURRENT_CYCLE_STATS: Dict[str, dict] = {}
FUNDING_TTL_CACHE: Dict[str, tuple] = {}
TF_TTL_SECS = {"3m": 60, "5m": 120, "15m": 240, "1h": 300}
PERSISTENT_OHLCV_CACHE: Dict[tuple, tuple] = {}
GLOBAL_BACKOFF_UNTIL: float = 0.0
_BACKOFF_SECS: float = 0.0
RATE_LIMIT_LOG_TS: float = 0.0
TOTAL_CYCLES: int = 0
TOTAL_ELAPSED: float = 0.0
TOTAL_REST_CALLS: int = 0
TOTAL_429_COUNT: int = 0

def prune_ohlcv_cache():
    """TTL 지난 OHLCV 캐시 청소."""
    try:
        now = time.time()
        to_del = []
        for (sym, tf, limit), (ts, _) in list(PERSISTENT_OHLCV_CACHE.items()):
            ttl = TF_TTL_SECS.get(tf, 60) * 3
            if (now - ts) > ttl:
                to_del.append((sym, tf, limit))
        for key in to_del:
            PERSISTENT_OHLCV_CACHE.pop(key, None)
    except Exception:
        pass

def print_section(title: str) -> None:
    print(f"[{title}]")
    print("---")

_PRINT_ORIG = builtins.print
_THREAD_LOG = threading.local()
_THREAD_TG = threading.local()
_STATE_FILE_OVERRIDE = None
_ENTRY_LOCK_MUTEX = threading.Lock()
_LAST_CYCLE_TS_MEM = 0
_ENTRY_SEEN_MUTEX = threading.Lock()
_ACCOUNT_ACTIVE_CACHE = {"ts": 0.0, "admin_active": True}

def _admin_is_active(ttl_sec: float = 5.0) -> bool:
    now = time.time()
    cached = _ACCOUNT_ACTIVE_CACHE
    if (now - float(cached.get("ts") or 0.0)) <= ttl_sec:
        return bool(cached.get("admin_active", True))
    active = True
    try:
        accounts = accounts_db.list_active_accounts()
        active = any(str(a.get("name")) == "admin" for a in (accounts or []))
    except Exception:
        active = True
    cached["ts"] = now
    cached["admin_active"] = bool(active)
    return bool(active)

def _active_account_names(ttl_sec: float = 5.0) -> set:
    now = time.time()
    cached = _ACCOUNT_ACTIVE_CACHE
    last = float(cached.get("names_ts") or 0.0)
    if (now - last) <= ttl_sec and isinstance(cached.get("names"), set):
        return cached.get("names")
    names = set()
    try:
        accounts = accounts_db.list_active_accounts()
        for a in accounts or []:
            if a.get("name"):
                names.add(str(a.get("name")))
    except Exception:
        names = set()
    cached["names"] = names
    cached["names_ts"] = now
    return names

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

def _prefetch_ohlcv_for_cycle(
    symbols: list,
    ex,
    plan: Dict[str, int],
    label: str = "prefetch",
    ttl_by_tf: Optional[dict] = None,
) -> dict:
    stats = {"symbols": len(symbols or []), "tfs": list(plan.keys()), "fetched": 0, "failed": 0, "fresh_hits": {}, "fetched_by_tf": {}}
    if not symbols or not plan:
        return stats
    ttl_by_tf = ttl_by_tf or {}

    def _inc_map(key: str, tf: str, amount: int = 1) -> None:
        bucket = CURRENT_CYCLE_STATS.setdefault(key, {})
        bucket[tf] = int(bucket.get(tf, 0) or 0) + amount

    for symbol in symbols:
        for tf, limit in plan.items():
            if not isinstance(limit, int) or limit <= 0:
                continue
            ttl = ttl_by_tf.get(tf)
            if isinstance(ttl, int) and ttl > 0 and cycle_cache.is_fresh(symbol, tf, ttl):
                stats["fresh_hits"][tf] = int(stats["fresh_hits"].get(tf, 0) or 0) + 1
                _inc_map("cache_hits_by_tf", tf)
                continue
            try:
                CURRENT_CYCLE_STATS["rest_calls"] = int(CURRENT_CYCLE_STATS.get("rest_calls", 0) or 0) + 1
                data = ex.fetch_ohlcv(symbol, tf, limit=limit)
                if data:
                    cycle_cache.set_raw(symbol, tf, data)
                    stats["fetched"] += 1
                    stats["fetched_by_tf"][tf] = int(stats["fetched_by_tf"].get(tf, 0) or 0) + 1
                    _inc_map("cache_miss_by_tf", tf)
                else:
                    stats["failed"] += 1
                    CURRENT_CYCLE_STATS["rest_fails"] = int(CURRENT_CYCLE_STATS.get("rest_fails", 0) or 0) + 1
            except Exception:
                stats["failed"] += 1
                CURRENT_CYCLE_STATS["rest_fails"] = int(CURRENT_CYCLE_STATS.get("rest_fails", 0) or 0) + 1
    return stats

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
_LAST_ENTRY_PCT_USED = None
_LAST_ENTRY_USDT_USED = None
_LAST_ENTRY_PCT_TS = 0.0

def _resolve_entry_usdt(pct: Optional[float] = None) -> float:
    """entry_usdt는 사용가능 USDT 대비 퍼센트로 해석한다."""
    global _ENTRY_PCT_WARN_TS, _LAST_ENTRY_PCT_USED, _LAST_ENTRY_USDT_USED, _LAST_ENTRY_PCT_TS
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
    usdt_val = max(0.0, float(avail) * (effective_pct / 100.0))
    _LAST_ENTRY_PCT_USED = pct_val
    _LAST_ENTRY_USDT_USED = usdt_val
    _LAST_ENTRY_PCT_TS = time.time()
    return usdt_val


def _calc_effective_entry_pct(usdt_amount: Optional[float]) -> Optional[float]:
    try:
        usdt_val = float(usdt_amount)
    except Exception:
        usdt_val = None
    try:
        avail = get_available_usdt()
    except Exception:
        avail = None
    if isinstance(usdt_val, (int, float)) and usdt_val > 0 and isinstance(avail, (int, float)) and avail > 0:
        pct = (float(usdt_val) / float(avail)) * 100.0
        if pct > 0:
            return pct
    if _LAST_ENTRY_PCT_USED is not None:
        if (time.time() - float(_LAST_ENTRY_PCT_TS or 0.0)) <= 5.0:
            return float(_LAST_ENTRY_PCT_USED)
    try:
        val = float(USDT_PER_TRADE)
    except Exception:
        val = None
    if isinstance(val, (int, float)) and val > 0:
        return val
    return None


def _log_entry_usdt_debug(symbol: str, engine: str, usdt: float) -> None:
    try:
        avail = get_available_usdt()
    except Exception:
        avail = None
    entry_pct = None
    try:
        entry_pct = float(USDT_PER_TRADE)
    except Exception:
        entry_pct = None
    if not isinstance(avail, (int, float)):
        avail = None
    if not isinstance(entry_pct, (int, float)):
        entry_pct = None
    try:
        usdt_val = float(usdt) if usdt is not None else None
    except Exception:
        usdt_val = None
    print(
        "[entry_usdt] "
        f"symbol={symbol} engine={engine} "
        f"available={avail} pct={entry_pct} calc_usdt={usdt_val}"
    )


def _fetch_ohlcv_with_retry(exchange, symbol: str, tf: str, limit: int):
    global GLOBAL_BACKOFF_UNTIL, _BACKOFF_SECS, TOTAL_429_COUNT, RATE_LIMIT_LOG_TS
    max_retries = 5
    base_wait = max(1.0, (getattr(exchange, "rateLimit", 0) or 0) / 1000.0)
    for attempt in range(max_retries):
        try:
            return exchange.fetch_ohlcv(symbol, tf, limit=limit)
        except (ccxt.DDoSProtection, ccxt.RateLimitExceeded) as e:
            _BACKOFF_SECS = 5.0 if _BACKOFF_SECS <= 0 else min(_BACKOFF_SECS * 1.5, 30.0)
            GLOBAL_BACKOFF_UNTIL = time.time() + _BACKOFF_SECS
            try:
                TOTAL_429_COUNT += 1
            except Exception:
                pass
            wait_s = min(base_wait * (2 ** attempt), 60.0)
            now = time.time()
            if now - RATE_LIMIT_LOG_TS > 5.0:
                print(f"[rate-limit] ohlcv {symbol} {tf} retry={attempt+1}/{max_retries} wait={wait_s:.1f}s err={e}")
                RATE_LIMIT_LOG_TS = now
            time.sleep(wait_s)
        except Exception as e:
            msg = str(e)
            if ("429" in msg) or ("-1003" in msg):
                _BACKOFF_SECS = 5.0 if _BACKOFF_SECS <= 0 else min(_BACKOFF_SECS * 1.5, 30.0)
                GLOBAL_BACKOFF_UNTIL = time.time() + _BACKOFF_SECS
                wait_s = min(base_wait * (2 ** attempt), 60.0)
                now = time.time()
                if now - RATE_LIMIT_LOG_TS > 5.0:
                    print(f"[rate-limit] ohlcv {symbol} {tf} retry={attempt+1}/{max_retries} wait={wait_s:.1f}s err={msg}")
                    RATE_LIMIT_LOG_TS = now
                time.sleep(wait_s)
                continue
            print(f"[ohlcv] fetch error sym={symbol} tf={tf} err={e}")
            return []
    return []


def _ema_align_ok(symbol: str, tf: str, limit: int) -> bool:
    df = cycle_cache.get_df(symbol, tf, limit=limit)
    if df is None or df.empty or len(df) < max(50, int(limit)):
        return False
    try:
        close = df["close"].astype(float).values
    except Exception:
        return False
    if len(close) < 50:
        return False
    ema_fast = pd.Series(close).ewm(span=20).mean().iloc[-1]
    ema_slow = pd.Series(close).ewm(span=50).mean().iloc[-1]
    return float(ema_fast) >= float(ema_slow)


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

def _fmt_tp_price_safe(entry_price: Any, tp_pct: Any, side: str = "LONG") -> str:
    try:
        entry = float(entry_price)
        pct = float(tp_pct) / 100.0
        if entry <= 0 or pct <= 0:
            return "N/A"
        if side.upper() == "SHORT":
            price = entry * (1.0 - pct)
        else:
            price = entry * (1.0 + pct)
        return f"{price:.6g}"
    except Exception:
        return "N/A"

def _fmt_entry_price(val: Any) -> str:
    try:
        return f"{float(val):.6g}"
    except Exception:
        return "N/A"

def _kst_today_start_ts(now_ts: Optional[float] = None) -> float:
    base = float(now_ts if now_ts is not None else time.time())
    kst = base + 9 * 3600
    day_start_kst = int(kst // 86400) * 86400
    # Binance 기준: KST 09:00 ~ 다음날 08:59:59
    return (day_start_kst - 9 * 3600) + 9 * 3600

def _fetch_realized_pnl_since(ex, since_ts: float) -> tuple[Optional[float], int, Optional[str]]:
    start_ms = int(float(since_ts) * 1000)
    now_ms = int(time.time() * 1000)
    total = 0.0
    count = 0
    income_types = ("REALIZED_PNL", "FUNDING_FEE", "COMMISSION")
    try:
        for income_type in income_types:
            since = start_ms
            while True:
                params = {
                    "incomeType": income_type,
                    "startTime": since,
                    "endTime": now_ms,
                    "limit": 1000,
                }
                if hasattr(ex, "fapiPrivateGetIncome"):
                    batch = ex.fapiPrivateGetIncome(params)
                else:
                    batch = ex.fapiPrivate_get_income(params)
                if not batch:
                    break
                last_ts = None
                for row in batch:
                    if not isinstance(row, dict):
                        continue
                    if row.get("incomeType") != income_type:
                        continue
                    asset = row.get("asset")
                    if asset and str(asset).upper() not in ("USDT",):
                        continue
                    income = row.get("income")
                    try:
                        val = float(income)
                    except Exception:
                        val = None
                    if val is not None:
                        total += val
                        count += 1
                    ts = row.get("time")
                    try:
                        ts_ms = int(float(ts))
                        if last_ts is None or ts_ms > last_ts:
                            last_ts = ts_ms
                    except Exception:
                        pass
                if last_ts is None:
                    break
                since = last_ts + 1
                if last_ts >= now_ms:
                    break
    except Exception as e:
        return None, 0, str(e)[:80]
    return total, count, None

def _fetch_last_price(symbol: str) -> Optional[float]:
    try:
        ticker = exchange.fetch_ticker(symbol)
        if isinstance(ticker, dict):
            last = ticker.get("last") or ticker.get("info", {}).get("lastPrice")
            if isinstance(last, (int, float)):
                return float(last)
    except Exception:
        return None
    return None

def _coerce_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        return float(str(val).strip())
    except Exception:
        return None

def _fmt_exit_pct(entry_price: Any, target_price: Any, side: str, kind: str) -> str:
    entry = _coerce_float(entry_price)
    target = _coerce_float(target_price)
    if entry is None or target is None or entry <= 0:
        return "n/a"
    side_key = (side or "").upper()
    kind_key = (kind or "").upper()
    if side_key == "SHORT":
        pct = (entry - target) / entry * 100.0 if kind_key == "TP" else (target - entry) / entry * 100.0
    else:
        pct = (target - entry) / entry * 100.0
    return f"{pct:+.2f}%"

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
    state: Optional[dict] = None,
    label_tag: Optional[str] = None,
) -> None:
    if not send_alert:
        return
    if isinstance(engine, str) and engine.strip().upper() == "MANUAL" and not MANAGE_WS_MODE:
        return
    side_key = (side or "").upper()
    icon = "🟢" if side_key == "LONG" else "🔴" if side_key == "SHORT" else "⚪"
    side_label = "롱" if side_key == "LONG" else "숏" if side_key == "SHORT" else side_key
    header = f"{icon} <b>{side_label} 시그널</b>"
    if label_tag:
        if label_tag.startswith("MULTI_ENTRY:"):
            try:
                num = int(label_tag.split(":", 1)[1])
                header = f"{header} (추가진입 +{num}번째)"
            except Exception:
                header = f"{header} (추가진입)"
        else:
            header = f"{header} ({label_tag})"
    lines = [header, f"<b>{symbol}</b>"]
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
    sl_disp = sl if sl else None
    tp_disp = tp if tp else None
    if (sl_disp is None or tp_disp is None) and entry_price is not None:
        tp_pct, sl_pct = _get_engine_exit_thresholds(engine, side_key)
        if sl_disp is None:
            sl_disp = _fmt_price_safe(entry_price, sl_pct, side=side_key)
        if tp_disp is None:
            tp_disp = _fmt_tp_price_safe(entry_price, tp_pct, side=side_key)
    if not sl_disp:
        sl_disp = "N/A"
    if not tp_disp:
        tp_disp = "N/A"
    sl_pct_disp = _fmt_exit_pct(entry_price, sl_disp, side_key, "SL")
    tp_pct_disp = _fmt_exit_pct(entry_price, tp_disp, side_key, "TP")
    lines.append(f"손절가={sl_disp} ({sl_pct_disp}) 익절가={tp_disp} ({tp_pct_disp})")
    lines.append(f"엔진: {_display_engine_label(engine)}")
    reason_disp = reason if (reason and str(reason).strip()) else "N/A"
    lines.append(f"사유: {reason_disp}")
    entry_status_line = _consume_entry_broadcast_line(symbol, side_key)
    if entry_status_line:
        lines.append(entry_status_line)
    send_alert("\n".join(lines))
    if isinstance(state, dict):
        try:
            _mark_entry_alerted(
                state,
                symbol,
                side_key,
                engine=engine,
                reason=reason,
                entry_order_id=entry_order_id,
            )
        except Exception:
            pass
    if dbrec:
        try:
            dbrec.record_event(
                symbol=symbol,
                side=side_key,
                event_type="ALERT_ENTRY",
                source="engine_runner",
                qty=None,
                avg_entry=entry_price,
                price=None,
                meta={
                    "engine": engine,
                    "reason": reason_disp,
                    "entry_price": entry_price,
                    "entry_usdt": usdt,
                    "entry_order_id": entry_order_id,
                    "sl": sl_disp,
                    "tp": tp_disp,
                    "live": live,
                    "order_info": order_info,
                },
            )
        except Exception:
            pass


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

def _append_swaggy_atlas_lab_v2_log(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    path = os.path.join("swaggy_atlas_lab_v2", f"swaggy_atlas_lab_v2-{date_tag}.log")
    _append_log_lines(path, [f"{ts} {line}"])

def _append_swaggy_no_atlas_log(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    path = os.path.join("swaggy_no_atlas", f"swaggy_no_atlas-{date_tag}.log")
    _append_log_lines(path, [f"{ts} {line}"])

def _append_adv_trend_log(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    path = os.path.join("advanced_trend_follower", f"advanced_trend_follower-{date_tag}.log")
    _append_log_lines(path, [f"{ts} {line}"])

def _iso_kst(ts: Optional[float] = None) -> str:
    tz = timezone(timedelta(hours=9))
    try:
        if ts is None:
            dt = datetime.now(tz=tz)
        else:
            dt = datetime.fromtimestamp(float(ts), tz=tz)
        return dt.isoformat(timespec="seconds")
    except Exception:
        return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

def _fmt_entry_time(tr: Optional[dict]) -> Optional[str]:
    if not isinstance(tr, dict):
        return None
    ts = tr.get("entry_ts")
    if not isinstance(ts, (int, float)):
        ts_ms = tr.get("entry_ts_ms")
        if isinstance(ts_ms, (int, float)) and ts_ms > 0:
            ts = float(ts_ms) / 1000.0
    if isinstance(ts, (int, float)) and ts > 0:
        return _iso_kst(float(ts))
    return None

def _parse_time_windows(text: str) -> List[tuple[int, int]]:
    windows: List[tuple[int, int]] = []
    for part in (text or "").split(","):
        part = part.strip()
        if not part or "-" not in part:
            continue
        start_raw, end_raw = part.split("-", 1)
        start_raw = start_raw.strip()
        end_raw = end_raw.strip()
        try:
            if ":" in start_raw:
                sh, sm = start_raw.split(":", 1)
            else:
                sh, sm = start_raw, "0"
            if ":" in end_raw:
                eh, em = end_raw.split(":", 1)
            else:
                eh, em = end_raw, "0"
            sh_i = int(sh)
            sm_i = int(sm)
            eh_i = int(eh)
            em_i = int(em)
        except Exception:
            continue
        if not (0 <= sh_i <= 23 and 0 <= eh_i <= 23 and 0 <= sm_i <= 59 and 0 <= em_i <= 59):
            continue
        windows.append((sh_i * 60 + sm_i, eh_i * 60 + em_i))
    return windows


def _is_in_off_window(text: str, now_ts: Optional[float] = None) -> bool:
    if not text:
        return False
    windows = _parse_time_windows(text)
    if not windows:
        return False
    try:
        tz = timezone(timedelta(hours=9))
        ts_val = time.time() if now_ts is None else float(now_ts)
        dt = datetime.fromtimestamp(ts_val, tz=tz)
    except Exception:
        return False
    cur_min = dt.hour * 60 + dt.minute
    for start_min, end_min in windows:
        if start_min == end_min:
            continue
        if start_min < end_min:
            if start_min <= cur_min < end_min:
                return True
        else:
            if cur_min >= start_min or cur_min < end_min:
                return True
    return False


def _is_saturday_kst(now_ts: Optional[float] = None) -> bool:
    try:
        ts_val = time.time() if now_ts is None else float(now_ts)
        tz = timezone(timedelta(hours=9))
        dt = datetime.fromtimestamp(ts_val, tz=tz)
        return dt.weekday() == 5
    except Exception:
        return False


def _log_off_window_status(state: dict, now_ts: float, tag: str = "skip") -> None:
    try:
        last_ts = _coerce_state_float(state.get("_off_window_skip_ts", 0.0))
    except Exception:
        last_ts = 0.0
    if (now_ts - last_ts) < 60:
        return
    msgs = []
    if SWAGGY_ATLAS_LAB_ENABLED and _is_in_off_window(SWAGGY_ATLAS_LAB_OFF_WINDOWS, now_ts):
        msgs.append(f"SWAGGY_ATLAS_LAB windows={SWAGGY_ATLAS_LAB_OFF_WINDOWS}")
    if SWAGGY_ATLAS_LAB_V2_ENABLED and _is_in_off_window(SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, now_ts):
        msgs.append(f"SWAGGY_ATLAS_LAB_V2 windows={SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS}")
    if SWAGGY_NO_ATLAS_ENABLED and _is_in_off_window(SWAGGY_NO_ATLAS_OFF_WINDOWS, now_ts):
        msgs.append(f"SWAGGY_NO_ATLAS windows={SWAGGY_NO_ATLAS_OFF_WINDOWS}")
    if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now_ts):
        msgs.append("SATURDAY_OFF")
    if not msgs:
        return
    print(f"[off-window] {tag} now={_now_kst_str()} " + " | ".join(msgs))
    state["_off_window_skip_ts"] = now_ts

def _append_swaggy_trade_json(payload: Dict[str, Any]) -> None:
    try:
        path = os.path.join("logs", "swaggy_trades.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    except Exception:
        pass

def _run_swaggy_atlas_lab_cycle(
    swaggy_atlas_lab_engine,
    swaggy_universe,
    cached_ex,
    state,
    swaggy_cfg,
    atlas_cfg,
    active_positions_total,
    send_alert,
    cycle_id: Optional[int] = None,
):
    def _fmt(v: Any) -> str:
        if v is None or v == "":
            return "N/A"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, float):
            return f"{v:.6g}"
        return str(v)

    def _entry_quality_bucket(
        atlas_pass_hard: Optional[bool],
        confirm_pass: Optional[int],
        overext_dist_at_entry: Optional[float],
        trigger_combo: str,
        trigger_parts: Dict[str, float],
        strength_total: float,
        strength_min_req: float,
    ) -> tuple[str, List[str]]:
        reasons: List[str] = []
        if atlas_pass_hard:
            reasons.append("ATLAS_HARD_OK")
        if confirm_pass:
            reasons.append("CONFIRM_OK")
        if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry <= 1.0:
            reasons.append("LOW_OVEREXT")
        strong_combo = False
        combo_set = {c.strip().upper() for c in (trigger_combo or "").split("+") if c.strip()}
        if combo_set == {"SWEEP", "RECLAIM"}:
            strong_combo = True
        rej_strength = trigger_parts.get("REJECTION")
        if rej_strength is not None and rej_strength >= 0.56:
            strong_combo = True
        if strong_combo:
            reasons.append("TRIGGER_COMBO_STRONG")
        if strength_total >= (strength_min_req + 0.08):
            reasons.append("STRENGTH_ABOVE_MIN")

        if (
            atlas_pass_hard
            and confirm_pass
            and isinstance(overext_dist_at_entry, (int, float))
            and overext_dist_at_entry <= 1.0
            and strong_combo
            and strength_total >= (strength_min_req + 0.08)
        ):
            return "A", reasons
        if (
            atlas_pass_hard
            and confirm_pass
            and isinstance(overext_dist_at_entry, (int, float))
            and overext_dist_at_entry <= 1.35
        ):
            return "B", reasons
        fail_reasons = []
        if not atlas_pass_hard:
            fail_reasons.append("ATLAS_HARD_FAIL")
        if not confirm_pass:
            fail_reasons.append("CONFIRM_FAIL")
        if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry > 1.35:
            fail_reasons.append("HIGH_OVEREXT")
        if not strong_combo:
            fail_reasons.append("WEAK_TRIGGER")
        if strength_total < (strength_min_req + 0.08):
            fail_reasons.append("LOW_STRENGTH")
        return "C", reasons + fail_reasons

    result = {"long_hits": 0, "short_hits": 0, "scanned": 0, "phase_changes": 0}
    if (not SWAGGY_ATLAS_LAB_ENABLED) or (not swaggy_atlas_lab_engine) or (not swaggy_cfg) or (not atlas_cfg):
        return result
    if not swaggy_universe:
        return result

    now_ts = time.time()
    if _is_in_off_window(SWAGGY_ATLAS_LAB_OFF_WINDOWS, now_ts):
        print(f"[off-window] SWAGGY_ATLAS_LAB now={_now_kst_str()} windows={SWAGGY_ATLAS_LAB_OFF_WINDOWS}")
        _append_swaggy_atlas_lab_log("SWAGGY_ATLAS_LAB_SKIP reason=OFF_WINDOW")
        return result
    if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now_ts):
        print(f"[off-sat] SWAGGY_ATLAS_LAB now={_now_kst_str()} saturday=ON")
        _append_swaggy_atlas_lab_log("SWAGGY_ATLAS_LAB_SKIP reason=SATURDAY_OFF")
        return result
    hedge_mode = False
    try:
        hedge_mode = is_hedge_mode()
    except Exception:
        hedge_mode = False
    btc_df = cycle_cache.get_df(atlas_cfg.ref_symbol, swaggy_cfg.tf_mtf, limit=200)
    if btc_df is None:
        btc_df = pd.DataFrame()

    ltf_limit = max(int(swaggy_cfg.ltf_limit), 120)
    mtf_limit = 200
    htf_limit = max(int(swaggy_cfg.vp_lookback_1h), 120)
    htf2_limit = 200
    d1_limit = 120

    for symbol in swaggy_universe:
        result["scanned"] += 1
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if _both_sides_open(st) and not hedge_mode:
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
            st["in_pos_long"] = bool(long_amt > 0)
            st["in_pos_short"] = bool(short_amt > 0)
            st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
            now_seen = time.time()
            if long_amt > 0:
                _set_last_entry_state(st, "LONG", now_seen)
            if short_amt > 0:
                _set_last_entry_state(st, "SHORT", now_seen)
            state[symbol] = st
            if (long_amt > 0) and (short_amt > 0):
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        df_5m = cycle_cache.get_df(symbol, swaggy_cfg.tf_ltf, limit=ltf_limit)
        df_15m = cycle_cache.get_df(symbol, swaggy_cfg.tf_mtf, limit=mtf_limit)
        df_1h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf, limit=htf_limit)
        df_4h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf2, limit=htf2_limit)
        df_1d = cycle_cache.get_df(symbol, swaggy_cfg.tf_d1, limit=d1_limit)
        df_3m = cycle_cache.get_df(symbol, "3m", limit=30)
        if not df_5m.empty and len(df_5m) > 1:
            df_5m = df_5m.iloc[:-1]
        if not df_15m.empty and len(df_15m) > 1:
            df_15m = df_15m.iloc[:-1]
        if not df_1h.empty and len(df_1h) > 1:
            df_1h = df_1h.iloc[:-1]
        if not df_4h.empty and len(df_4h) > 1:
            df_4h = df_4h.iloc[:-1]
        if not df_1d.empty and len(df_1d) > 1:
            df_1d = df_1d.iloc[:-1]
        if not df_3m.empty and len(df_3m) > 1:
            df_3m = df_3m.iloc[:-1]
        if df_5m.empty or df_15m.empty or df_1h.empty or df_4h.empty or df_3m.empty:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        last_close = float(df_5m.iloc[-1]["close"])

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

        debug = signal.debug if isinstance(signal.debug, dict) else {}
        event_list = debug.get("events") if isinstance(debug.get("events"), list) else []
        if event_list:
            for event in event_list:
                if not isinstance(event, dict):
                    continue
                payload = {
                    "ts": _iso_kst(),
                    "event": event.get("event") or "SWAGGY_EVENT",
                    "engine": "SWAGGY_ATLAS_LAB",
                    "mode": "live" if LIVE_TRADING or LONG_LIVE_TRADING else "paper",
                    "symbol": symbol,
                    "side": event.get("side") or signal.side,
                    "ltf": swaggy_cfg.tf_ltf,
                    "mtf": swaggy_cfg.tf_mtf,
                    "htf": swaggy_cfg.tf_htf,
                    "htf2": swaggy_cfg.tf_htf2,
                    "cycle_id": cycle_id,
                    "range_id": event.get("range_id") or debug.get("touch_key"),
                }
                payload.update(event)
                _append_swaggy_trade_json(payload)

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

        atlas_metrics = atlas.metrics if atlas and isinstance(atlas.metrics, dict) else {}
        entry_quality, entry_quality_reasons = _entry_quality_bucket(
            bool(atlas.pass_hard) if atlas and atlas.pass_hard is not None else False,
            int(debug.get("confirm_pass") or 0),
            debug.get("overext_dist_at_entry"),
            str(debug.get("trigger_combo") or ""),
            debug.get("trigger_parts") if isinstance(debug.get("trigger_parts"), dict) else {},
            float(debug.get("strength_total") or 0.0),
            float(debug.get("strength_min_req") or 0.0),
        )
        entry_line = (
            "SWAGGY_ATLAS_LAB_ENTRY sym=%s side=%s sw_strength=%.3f sw_reasons=%s "
            "final_usdt=%.2f atlas_pass=%s atlas_mult=%s atlas_reasons=%s policy_action=%s "
            "level_score=%s touch_count=%s level_age=%s trigger_combo=%s confirm_pass=%s confirm_fail=%s "
            "overext_dist_at_touch=%s overext_dist_at_entry=%s "
            "atlas_rs_z=%s atlas_corr=%s atlas_beta=%s atlas_vol_ratio=%s atlas_pass_soft=%s atlas_pass_hard=%s "
            "entry_quality=%s"
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
                _fmt(debug.get("level_score")),
                _fmt(debug.get("touch_count")),
                _fmt(debug.get("level_age_sec")),
                _fmt(debug.get("trigger_combo")),
                _fmt(debug.get("confirm_pass")),
                _fmt(debug.get("confirm_fail")),
                _fmt(debug.get("overext_dist_at_touch")),
                _fmt(debug.get("overext_dist_at_entry")),
                _fmt(atlas_metrics.get("rs_z")),
                _fmt(atlas_metrics.get("corr")),
                _fmt(atlas_metrics.get("beta")),
                _fmt(atlas_metrics.get("vol_ratio")),
                _fmt(atlas_metrics.get("pass_soft")),
                _fmt(atlas_metrics.get("pass_hard")),
                _fmt(entry_quality),
            )
        )
        _append_swaggy_atlas_lab_log(entry_line)
        if not policy.allow:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if swaggy_cfg:
            beta_val = atlas_metrics.get("beta")
            if (
                swaggy_cfg.skip_beta_mid
                and isinstance(beta_val, (int, float))
                and 1.0 <= float(beta_val) < 1.5
            ):
                _append_swaggy_atlas_lab_log(
                    f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=SKIP_BETA_MID beta={beta_val:.4g}"
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            overext_val = debug.get("overext_dist_at_entry")
            if isinstance(overext_val, (int, float)):
                overext_val = abs(float(overext_val))
            if (
                swaggy_cfg.skip_overext_mid
                and isinstance(overext_val, (int, float))
                and 1.1 <= overext_val < 1.4
            ):
                _append_swaggy_atlas_lab_log(
                    f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=SKIP_OVEREXT_MID overext={overext_val:.4g}"
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            body_ratio = None
            confirm_metrics = debug.get("confirm_metrics")
            if isinstance(confirm_metrics, dict):
                body_ratio = confirm_metrics.get("body_ratio")
            if (
                swaggy_cfg.skip_confirm_body
                and isinstance(body_ratio, (int, float))
                and float(body_ratio) < 0.60
            ):
                _append_swaggy_atlas_lab_log(
                    f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=SKIP_CONFIRM_BODY body_ratio={body_ratio:.4g}"
                )
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
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab", side=side)
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_usdt = _resolve_entry_usdt(policy.final_usdt)
        try:
            live = LONG_LIVE_TRADING if side == "LONG" else LIVE_TRADING
            req_id = _enqueue_entry_request(
                state,
                symbol=symbol,
                side=side,
                engine="SWAGGY_ATLAS_LAB",
                reason="swaggy_atlas_lab",
                usdt=entry_usdt,
                live=live,
                alert_reason="SWAGGY_ATLAS_LAB",
            )
            if req_id:
                trade_payload = {
                    "ts": _iso_kst(),
                    "event": "SWAGGY_TRADE",
                    "engine": "SWAGGY_ATLAS_LAB",
                    "mode": "live" if live else "paper",
                    "symbol": symbol,
                    "side": side,
                    "ltf": swaggy_cfg.tf_ltf,
                    "mtf": swaggy_cfg.tf_mtf,
                    "htf": swaggy_cfg.tf_htf,
                    "htf2": swaggy_cfg.tf_htf2,
                    "cycle_id": cycle_id,
                    "range_id": debug.get("touch_key"),
                    "entry_ts": _iso_kst(),
                    "entry_price": entry_px,
                    "atr14_ltf": debug.get("atr14_ltf"),
                    "atr14_htf": debug.get("atr14_htf"),
                    "ema20_ltf": debug.get("ema20_ltf"),
                    "ema20_htf": debug.get("ema20_htf"),
                    "level_type": debug.get("level_type"),
                    "level_price": debug.get("level_price"),
                    "level_score": debug.get("level_score"),
                    "touch_count": debug.get("touch_count"),
                    "level_age_bars": debug.get("level_age_bars"),
                    "touch_pct": debug.get("touch_pct"),
                    "touch_atr_mult": debug.get("touch_atr_mult"),
                    "touch_pass": int(debug.get("touch_pass") or 0),
                    "touch_fail_reason": debug.get("touch_fail_reason"),
                    "trigger_combo": debug.get("trigger_combo"),
                    "trigger_strength_best": debug.get("trigger_strength_best"),
                    "trigger_strength_min": debug.get("trigger_strength_min"),
                    "trigger_strength_avg": debug.get("trigger_strength_avg"),
                    "trigger_strength_used": debug.get("trigger_strength_used"),
                    "trigger_parts": debug.get("trigger_parts"),
                    "strength_total": debug.get("strength_total"),
                    "strength_min_req": debug.get("strength_min_req"),
                    "trigger_threshold_used": debug.get("trigger_threshold_used"),
                    "use_trigger_min": debug.get("use_trigger_min"),
                    "confirm_pass": int(debug.get("confirm_pass") or 0),
                    "confirm_fail_reason": debug.get("confirm_fail"),
                "confirm_metrics": debug.get("confirm_metrics"),
                "confirm_body_ratio": (debug.get("confirm_metrics") or {}).get("body_ratio")
                if isinstance(debug.get("confirm_metrics"), dict)
                else None,
                    "overext_ema_len": debug.get("overext_ema_len"),
                    "overext_atr_mult": debug.get("overext_atr_mult"),
                    "overext_dist_at_touch": debug.get("overext_dist_at_touch"),
                    "overext_dist_at_entry": debug.get("overext_dist_at_entry"),
                    "overext_blocked": 0,
                    "overext_state": "OK",
                    "atlas_pass_soft": atlas_metrics.get("pass_soft"),
                    "atlas_pass_hard": atlas_metrics.get("pass_hard"),
                    "atlas_state": gate.get("reason") if isinstance(gate, dict) else None,
                    "atlas_regime": atlas_metrics.get("regime") or (gate.get("regime") if isinstance(gate, dict) else None),
                    "atlas_rs": atlas_metrics.get("rs"),
                    "atlas_rs_z": atlas_metrics.get("rs_z"),
                    "atlas_corr": atlas_metrics.get("corr"),
                    "atlas_beta": atlas_metrics.get("beta"),
                    "atlas_vol_ratio": atlas_metrics.get("vol_ratio"),
                    "atlas_block_reason": policy.policy_action if policy and not policy.allow else None,
                    "entry_quality_bucket": entry_quality,
                    "entry_quality_reasons": entry_quality_reasons,
                }
                _append_swaggy_trade_json(trade_payload)
                if side == "LONG":
                    result["long_hits"] += 1
                else:
                    result["short_hits"] += 1
        except Exception as e:
            _append_swaggy_atlas_lab_log(f"SWAGGY_ATLAS_LAB_SKIP sym={symbol} reason=QUEUE_ERROR {e}")
        finally:
            _entry_guard_release(state, symbol, key=guard_key)
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab", side=side)

        time.sleep(PER_SYMBOL_SLEEP)
    return result

def _run_swaggy_atlas_lab_v2_cycle(
    swaggy_atlas_lab_engine,
    swaggy_universe,
    cached_ex,
    state,
    swaggy_cfg,
    atlas_cfg,
    active_positions_total,
    send_alert,
    cycle_id: Optional[int] = None,
):
    def _fmt(v: Any) -> str:
        if v is None or v == "":
            return "N/A"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, float):
            return f"{v:.6g}"
        return str(v)

    def _entry_quality_bucket(
        atlas_pass_hard: Optional[bool],
        confirm_pass: Optional[int],
        overext_dist_at_entry: Optional[float],
        trigger_combo: str,
        trigger_parts: Dict[str, float],
        strength_total: float,
        strength_min_req: float,
    ) -> tuple[str, List[str]]:
        reasons: List[str] = []
        if atlas_pass_hard:
            reasons.append("ATLAS_HARD_OK")
        if confirm_pass:
            reasons.append("CONFIRM_OK")
        if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry <= 1.0:
            reasons.append("LOW_OVEREXT")
        strong_combo = False
        combo_set = {c.strip().upper() for c in (trigger_combo or "").split("+") if c.strip()}
        if combo_set == {"SWEEP", "RECLAIM"}:
            strong_combo = True
        rej_strength = trigger_parts.get("REJECTION")
        if rej_strength is not None and rej_strength >= 0.56:
            strong_combo = True
        if strong_combo:
            reasons.append("TRIGGER_COMBO_STRONG")
        if strength_total >= (strength_min_req + 0.08):
            reasons.append("STRENGTH_ABOVE_MIN")

        if (
            atlas_pass_hard
            and confirm_pass
            and isinstance(overext_dist_at_entry, (int, float))
            and overext_dist_at_entry <= 1.0
            and strong_combo
            and strength_total >= (strength_min_req + 0.08)
        ):
            return "A", reasons
        if (
            atlas_pass_hard
            and confirm_pass
            and isinstance(overext_dist_at_entry, (int, float))
            and overext_dist_at_entry <= 1.35
        ):
            return "B", reasons
        fail_reasons = []
        if not atlas_pass_hard:
            fail_reasons.append("ATLAS_HARD_FAIL")
        if not confirm_pass:
            fail_reasons.append("CONFIRM_FAIL")
        if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry > 1.35:
            fail_reasons.append("HIGH_OVEREXT")
        if not strong_combo:
            fail_reasons.append("WEAK_TRIGGER")
        if strength_total < (strength_min_req + 0.08):
            fail_reasons.append("LOW_STRENGTH")
        return "C", reasons + fail_reasons

    result = {"long_hits": 0, "short_hits": 0}
    if (not SWAGGY_ATLAS_LAB_V2_ENABLED) or (not swaggy_atlas_lab_engine) or (not swaggy_cfg) or (not atlas_cfg):
        return result
    if not swaggy_universe:
        return result

    now_ts = time.time()
    if _is_in_off_window(SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, now_ts):
        print(f"[off-window] SWAGGY_ATLAS_LAB_V2 now={_now_kst_str()} windows={SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS}")
        _append_swaggy_atlas_lab_v2_log("SWAGGY_ATLAS_LAB_V2_SKIP reason=OFF_WINDOW")
        return result
    if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now_ts):
        print(f"[off-sat] SWAGGY_ATLAS_LAB_V2 now={_now_kst_str()} saturday=ON")
        _append_swaggy_atlas_lab_v2_log("SWAGGY_ATLAS_LAB_V2_SKIP reason=SATURDAY_OFF")
        return result
    hedge_mode = False
    try:
        hedge_mode = is_hedge_mode()
    except Exception:
        hedge_mode = False
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
        if _both_sides_open(st) and not hedge_mode:
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
            st["in_pos_long"] = bool(long_amt > 0)
            st["in_pos_short"] = bool(short_amt > 0)
            st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
            now_seen = time.time()
            if long_amt > 0:
                _set_last_entry_state(st, "LONG", now_seen)
            if short_amt > 0:
                _set_last_entry_state(st, "SHORT", now_seen)
            state[symbol] = st
            if (long_amt > 0) and (short_amt > 0):
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        df_5m = cycle_cache.get_df(symbol, swaggy_cfg.tf_ltf, limit=ltf_limit)
        df_15m = cycle_cache.get_df(symbol, swaggy_cfg.tf_mtf, limit=mtf_limit)
        df_1h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf, limit=htf_limit)
        df_4h = cycle_cache.get_df(symbol, swaggy_cfg.tf_htf2, limit=htf2_limit)
        df_1d = cycle_cache.get_df(symbol, swaggy_cfg.tf_d1, limit=d1_limit)
        df_3m = cycle_cache.get_df(symbol, "3m", limit=30)
        if not df_5m.empty and len(df_5m) > 1:
            df_5m = df_5m.iloc[:-1]
        if not df_15m.empty and len(df_15m) > 1:
            df_15m = df_15m.iloc[:-1]
        if not df_1h.empty and len(df_1h) > 1:
            df_1h = df_1h.iloc[:-1]
        if not df_4h.empty and len(df_4h) > 1:
            df_4h = df_4h.iloc[:-1]
        if not df_1d.empty and len(df_1d) > 1:
            df_1d = df_1d.iloc[:-1]
        if not df_3m.empty and len(df_3m) > 1:
            df_3m = df_3m.iloc[:-1]
        if df_5m.empty or df_15m.empty or df_1h.empty or df_4h.empty or df_3m.empty:
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        last_close = float(df_5m.iloc[-1]["close"])

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
            result["phase_changes"] = int(result.get("phase_changes", 0) or 0) + 1
            _append_swaggy_atlas_lab_v2_log(
                "SWAGGY_ATLAS_LAB_V2_PHASE sym=%s side=%s prev=%s now=%s reasons=%s"
                % (symbol, signal.side, prev_phase, new_phase, ",".join(signal.reasons or []))
            )

        debug = signal.debug if isinstance(signal.debug, dict) else {}
        event_list = debug.get("events") if isinstance(debug.get("events"), list) else []
        if event_list:
            for ev in event_list:
                if not isinstance(ev, dict):
                    continue
                evt_name = ev.get("event")
                if not evt_name:
                    continue
                try:
                    payload = {
                        "ts": _iso_kst(),
                        "event": evt_name,
                        "engine": "SWAGGY_ATLAS_LAB_V2",
                        "mode": "live",
                        "symbol": symbol,
                        "side": signal.side,
                        "ltf": swaggy_cfg.tf_ltf,
                        "mtf": swaggy_cfg.tf_mtf,
                        "htf": swaggy_cfg.tf_htf,
                        "htf2": swaggy_cfg.tf_htf2,
                        "cycle_id": cycle_id,
                        "range_id": ev.get("range_id"),
                        "level_score": ev.get("level_score"),
                        "touch_count": ev.get("touch_count"),
                        "level_age_bars": ev.get("level_age_bars"),
                        "trigger_combo": ev.get("trigger_combo"),
                        "trigger_parts": ev.get("trigger_parts"),
                        "trigger_strength_best": ev.get("trigger_strength_best"),
                        "trigger_strength_min": ev.get("trigger_strength_min"),
                        "trigger_strength_avg": ev.get("trigger_strength_avg"),
                        "trigger_strength_used": ev.get("trigger_strength_used"),
                        "strength_total": ev.get("strength_total"),
                        "strength_min_req": ev.get("strength_min_req"),
                        "trigger_threshold_used": ev.get("trigger_threshold_used"),
                        "use_trigger_min": ev.get("use_trigger_min"),
                        "confirm_pass": int(ev.get("confirm_pass") or 0),
                        "confirm_fail_reason": ev.get("confirm_fail"),
                        "confirm_metrics": ev.get("confirm_metrics"),
                        "overext_ema_len": ev.get("overext_ema_len"),
                        "overext_atr_mult": ev.get("overext_atr_mult"),
                        "overext_dist_at_touch": ev.get("overext_dist_at_touch"),
                        "overext_dist_at_entry": ev.get("overext_dist_at_entry"),
                    }
                    _append_swaggy_trade_json(payload)
                except Exception:
                    pass

        if not signal or not signal.entry_ok:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if signal.side not in ("LONG", "SHORT"):
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        side = signal.side
        if _exit_cooldown_blocked(state, symbol, "swaggy_atlas_lab_v2", side):
            _append_swaggy_atlas_lab_v2_log(
                f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=EXIT_COOLDOWN side={side}"
            )
            _append_entry_gate_log("swaggy_atlas_lab_v2", symbol, "exit_cooldown", side=side)
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if side == "LONG" and _has_open_side(st, "LONG"):
            _append_swaggy_atlas_lab_v2_log(
                f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=ALREADY_IN_POSITION side=LONG"
            )
            _append_entry_gate_log("swaggy_atlas_lab_v2", symbol, "already_in_position", side="LONG")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if side == "SHORT" and _has_open_side(st, "SHORT"):
            _append_swaggy_atlas_lab_v2_log(
                f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=ALREADY_IN_POSITION side=SHORT"
            )
            _append_entry_gate_log("swaggy_atlas_lab_v2", symbol, "already_in_position", side="SHORT")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_usdt = _resolve_entry_usdt()
        entry_px = last_close

        atlas = {}
        gate = {}
        if lab_v2_evaluate_global_gate and lab_v2_evaluate_local and not btc_df.empty:
            gate = lab_v2_evaluate_global_gate(btc_df, atlas_cfg)
            atlas = lab_v2_evaluate_local(symbol, side, df_15m, btc_df, gate, atlas_cfg)
        policy = lab_v2_apply_policy(SwaggyAtlasLabV2Mode.HARD, USDT_PER_TRADE, atlas) if lab_v2_apply_policy else None
        if policy and policy.atlas_reasons:
            try:
                _append_swaggy_atlas_lab_v2_log(
                    "SWAGGY_ATLAS_LAB_V2_POLICY sym=%s side=%s action=%s atlas_reasons=%s"
                    % (
                        symbol,
                        side,
                        policy.policy_action,
                        ",".join(policy.atlas_reasons or []),
                    )
                )
            except Exception:
                pass
        if policy and not policy.allow:
            _append_swaggy_atlas_lab_v2_log(
                f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=ATLAS_POLICY block={policy.policy_action}"
            )
            _append_entry_gate_log(
                "swaggy_atlas_lab_v2",
                symbol,
                f"atlas_policy block={policy.policy_action}",
                side=side,
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        trigger_combo = _fmt(debug.get("trigger_combo"))
        trigger_parts = debug.get("trigger_parts") if isinstance(debug.get("trigger_parts"), dict) else {}
        strength_total = float(debug.get("strength_total") or 0.0)
        strength_min_req = float(debug.get("strength_min_req") or 0.0)
        atlas_pass_hard = False
        atlas_metrics = atlas if isinstance(atlas, dict) else (getattr(atlas, "metrics", {}) or {})
        if isinstance(atlas, dict):
            atlas_pass_hard = bool(atlas.get("pass_hard"))
        else:
            atlas_pass_hard = bool(getattr(atlas, "pass_hard", False))
        entry_quality, entry_quality_reasons = _entry_quality_bucket(
            atlas_pass_hard,
            int(debug.get("confirm_pass") or 0),
            debug.get("overext_dist_at_entry"),
            trigger_combo,
            trigger_parts,
            strength_total,
            strength_min_req,
        )

        guard_key = f"swaggy_atlas_lab_v2|{symbol}|{side}"
        lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="swaggy_atlas_lab_v2", side=side)
        if not lock_ok:
            _append_swaggy_atlas_lab_v2_log(
                f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=ENTRY_LOCK owner={lock_owner} age_s={lock_age:.1f}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if not _entry_guard_acquire(state, symbol, key=guard_key, engine="swaggy_atlas_lab_v2", side=side):
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab_v2", side=side)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        try:
            req_id = _enqueue_entry_request(
                state,
                symbol=symbol,
                side=side,
                engine="SWAGGY_ATLAS_LAB_V2",
                reason="swaggy_atlas_lab_v2",
                usdt=entry_usdt,
                live=LIVE_TRADING,
                alert_reason="SWAGGY_ATLAS_LAB_V2",
                entry_price_hint=entry_px,
            )
            if not req_id:
                _append_swaggy_atlas_lab_v2_log(
                    f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=QUEUE_REJECT side={side}"
                )
                _append_entry_gate_log("swaggy_atlas_lab_v2", symbol, "queue_reject", side=side)
            if req_id:
                _append_swaggy_atlas_lab_v2_log(
                    "SWAGGY_ATLAS_LAB_V2_ENTRY sym=%s side=%s sw_strength=%.3f sw_reasons=%s "
                    "final_usdt=%.2f level_score=%s touch_count=%s level_age=%s trigger_combo=%s confirm_pass=%s confirm_fail=%s "
                    "overext_dist_at_touch=%s overext_dist_at_entry=%s entry_quality=%s"
                    % (
                        symbol,
                        side,
                        float(signal.strength or 0.0),
                        ",".join(signal.reasons or []),
                        float(entry_usdt or 0.0),
                        _fmt(debug.get("level_score")),
                        _fmt(debug.get("touch_count")),
                        _fmt(debug.get("level_age_sec")),
                        _fmt(debug.get("trigger_combo")),
                        _fmt(debug.get("confirm_pass")),
                        _fmt(debug.get("confirm_fail")),
                        _fmt(debug.get("overext_dist_at_touch")),
                        _fmt(debug.get("overext_dist_at_entry")),
                        _fmt(entry_quality),
                    )
                )
                trade_payload = {
                    "ts": _iso_kst(),
                    "event": "SWAGGY_TRADE",
                    "engine": "SWAGGY_ATLAS_LAB_V2",
                    "mode": "live",
                    "symbol": symbol,
                    "side": side,
                    "ltf": swaggy_cfg.tf_ltf,
                    "mtf": swaggy_cfg.tf_mtf,
                    "htf": swaggy_cfg.tf_htf,
                    "htf2": swaggy_cfg.tf_htf2,
                    "cycle_id": cycle_id,
                    "range_id": debug.get("range_id"),
                    "entry_ts": _iso_kst(),
                    "entry_price": entry_px,
                    "atr14_ltf": debug.get("atr14_ltf"),
                    "atr14_htf": debug.get("atr14_htf"),
                    "ema20_ltf": debug.get("ema20_ltf"),
                    "ema20_htf": debug.get("ema20_htf"),
                    "level_type": debug.get("level_type"),
                    "level_price": debug.get("level_price"),
                    "level_score": debug.get("level_score"),
                    "touch_count": debug.get("touch_count"),
                    "level_age_bars": debug.get("level_age_bars"),
                    "touch_pct": debug.get("touch_pct"),
                    "touch_atr_mult": debug.get("touch_atr_mult"),
                    "touch_pass": int(debug.get("touch_pass") or 0),
                    "touch_fail_reason": debug.get("touch_fail_reason"),
                    "trigger_combo": debug.get("trigger_combo"),
                    "trigger_strength_best": debug.get("trigger_strength_best"),
                    "trigger_strength_min": debug.get("trigger_strength_min"),
                    "trigger_strength_avg": debug.get("trigger_strength_avg"),
                    "trigger_strength_used": debug.get("trigger_strength_used"),
                    "trigger_parts": debug.get("trigger_parts"),
                    "strength_total": debug.get("strength_total"),
                    "strength_min_req": debug.get("strength_min_req"),
                    "trigger_threshold_used": debug.get("trigger_threshold_used"),
                    "use_trigger_min": debug.get("use_trigger_min"),
                    "confirm_pass": int(debug.get("confirm_pass") or 0),
                    "confirm_fail_reason": debug.get("confirm_fail"),
                    "confirm_metrics": debug.get("confirm_metrics"),
                    "confirm_body_ratio": (debug.get("confirm_metrics") or {}).get("body_ratio")
                    if isinstance(debug.get("confirm_metrics"), dict)
                    else None,
                    "overext_ema_len": debug.get("overext_ema_len"),
                    "overext_atr_mult": debug.get("overext_atr_mult"),
                    "overext_dist_at_touch": debug.get("overext_dist_at_touch"),
                    "overext_dist_at_entry": debug.get("overext_dist_at_entry"),
                    "overext_blocked": 0,
                    "overext_state": "OK",
                    "atlas_pass_soft": atlas_metrics.get("pass_soft"),
                    "atlas_pass_hard": atlas_metrics.get("pass_hard") if isinstance(atlas, dict) else atlas_pass_hard,
                    "atlas_state": gate.get("reason") if isinstance(gate, dict) else None,
                    "atlas_regime": atlas_metrics.get("regime") or (gate.get("regime") if isinstance(gate, dict) else None),
                    "atlas_rs": atlas_metrics.get("rs"),
                    "atlas_rs_z": atlas_metrics.get("rs_z"),
                    "atlas_corr": atlas_metrics.get("corr"),
                    "atlas_beta": atlas_metrics.get("beta"),
                    "atlas_vol_ratio": atlas_metrics.get("vol_ratio"),
                    "atlas_block_reason": policy.policy_action if policy and not policy.allow else None,
                    "entry_quality_bucket": entry_quality,
                    "entry_quality_reasons": entry_quality_reasons,
                }
                _append_swaggy_trade_json(trade_payload)
                if side == "LONG":
                    result["long_hits"] += 1
                else:
                    result["short_hits"] += 1
        except Exception as e:
            _append_swaggy_atlas_lab_v2_log(f"SWAGGY_ATLAS_LAB_V2_SKIP sym={symbol} reason=QUEUE_ERROR {e}")
        finally:
            _entry_guard_release(state, symbol, key=guard_key)
            _entry_lock_release(state, symbol, owner="swaggy_atlas_lab_v2", side=side)

        time.sleep(PER_SYMBOL_SLEEP)
    return result

def _run_swaggy_no_atlas_cycle(
    swaggy_no_atlas_engine,
    swaggy_universe,
    cached_ex,
    state,
    swaggy_cfg,
    active_positions_total,
    send_alert,
    cycle_id: Optional[int] = None,
):
    def _fmt(v: Any) -> str:
        if v is None or v == "":
            return "N/A"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, float):
            return f"{v:.6g}"
        return str(v)

    result = {"long_hits": 0, "short_hits": 0}
    if (not SWAGGY_NO_ATLAS_ENABLED) or (not swaggy_no_atlas_engine) or (not swaggy_cfg):
        return result
    if not swaggy_universe:
        return result
    try:
        refresh_positions_cache(force=True)
    except Exception:
        pass

    now_ts = time.time()
    debug_syms = {
        s.strip()
        for s in (SWAGGY_NO_ATLAS_DEBUG_SYMBOLS or "").split(",")
        if s.strip()
    }
    if _is_in_off_window(SWAGGY_NO_ATLAS_OFF_WINDOWS, now_ts):
        print(f"[off-window] SWAGGY_NO_ATLAS now={_now_kst_str()} windows={SWAGGY_NO_ATLAS_OFF_WINDOWS}")
        _append_swaggy_no_atlas_log("SWAGGY_NO_ATLAS_SKIP reason=OFF_WINDOW")
        return result
    if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now_ts):
        print(f"[off-sat] SWAGGY_NO_ATLAS now={_now_kst_str()} saturday=ON")
        _append_swaggy_no_atlas_log("SWAGGY_NO_ATLAS_SKIP reason=SATURDAY_OFF")
        return result
    hedge_mode = False
    try:
        hedge_mode = is_hedge_mode()
    except Exception:
        hedge_mode = False
    ltf_limit = max(int(swaggy_cfg.ltf_limit), 120)
    mtf_limit = 200
    htf_limit = max(int(swaggy_cfg.vp_lookback_1h), 120)
    htf2_limit = 200
    d1_limit = 120

    for symbol in swaggy_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if _both_sides_open(st) and not hedge_mode:
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
            st["in_pos_long"] = bool(long_amt > 0)
            st["in_pos_short"] = bool(short_amt > 0)
            st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
            now_seen = time.time()
            if long_amt > 0:
                _set_last_entry_state(st, "LONG", now_seen)
            if short_amt > 0:
                _set_last_entry_state(st, "SHORT", now_seen)
            state[symbol] = st
            if (long_amt > 0) and (short_amt > 0):
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

        prev_phase = swaggy_no_atlas_engine._state.get(symbol, {}).get("phase")
        signal = swaggy_no_atlas_engine.evaluate_symbol(
            symbol,
            df_4h,
            df_1h,
            df_15m,
            df_5m,
            df_3m,
            df_1d if isinstance(df_1d, pd.DataFrame) else pd.DataFrame(),
            now_ts,
        )
        new_phase = swaggy_no_atlas_engine._state.get(symbol, {}).get("phase")
        if prev_phase != new_phase and new_phase:
            _append_swaggy_no_atlas_log(
                "SWAGGY_NO_ATLAS_PHASE sym=%s prev=%s now=%s reasons=%s"
                % (symbol, prev_phase, new_phase, ",".join(signal.reasons or []))
            )

        debug = signal.debug if isinstance(signal.debug, dict) else {}
        event_list = debug.get("events") if isinstance(debug.get("events"), list) else []
        if event_list:
            for event in event_list:
                if not isinstance(event, dict):
                    continue
                payload = {
                    "ts": _iso_kst(),
                    "event": event.get("event") or "SWAGGY_EVENT",
                    "engine": "SWAGGY_NO_ATLAS",
                    "mode": "live" if LIVE_TRADING or LONG_LIVE_TRADING else "paper",
                    "symbol": symbol,
                    "side": event.get("side") or signal.side,
                    "ltf": swaggy_cfg.tf_ltf,
                    "mtf": swaggy_cfg.tf_mtf,
                    "htf": swaggy_cfg.tf_htf,
                    "htf2": swaggy_cfg.tf_htf2,
                    "cycle_id": cycle_id,
                    "range_id": event.get("range_id") or debug.get("touch_key"),
                }
                payload.update(event)
                _append_swaggy_trade_json(payload)

        struct_ctx = None
        entry_from_structure = False

        entry_px = signal.entry_px
        if signal.entry_ok and signal.side and entry_px is None:
            try:
                entry_px = float(df_5m.iloc[-1]["close"])
            except Exception:
                entry_px = None
            if entry_px is not None:
                _append_swaggy_no_atlas_log(
                    "SWAGGY_NO_ATLAS_ENTRY_PX_FALLBACK sym=%s side=%s entry_px=%s"
                    % (symbol, signal.side, _fmt(entry_px))
                )
        if entry_from_structure:
            side = str(struct_ctx.get("side") or "").upper() if struct_ctx else ""
            last = df_5m.iloc[-1]
            entry_px = float(last["close"])
            struct_debug = struct_ctx.get("debug") if isinstance(struct_ctx, dict) else None
            if isinstance(struct_debug, dict):
                debug = struct_debug
        else:
            side = signal.side.upper()
            # entry_px fallback already applied above
            if entry_px is None:
                entry_px = signal.entry_px
        if symbol in debug_syms or _symbol_in_pos_any(st):
            try:
                live_amt_dbg = (
                    executor_mod.get_long_position_amount(symbol)
                    if side == "LONG"
                    else executor_mod.get_short_position_amount(symbol)
                ) if executor_mod else None
            except Exception:
                live_amt_dbg = None
            if _is_in_pos_side(st, side) and not _get_open_trade(state, side, symbol):
                _get_open_trade_or_backfill(state, symbol, side, now_ts=now_ts)
            _append_swaggy_no_atlas_log(
                "SWAGGY_NO_ATLAS_ENTRY_READY sym=%s side=%s entry_px=%s in_pos_flag=%s live_amt=%s open_tr=%s"
                % (
                    symbol,
                    side,
                    _fmt(entry_px),
                    int(bool(_is_in_pos_side(st, side))),
                    _fmt(live_amt_dbg) if live_amt_dbg is not None else "N/A",
                    int(bool(_get_open_trade(state, side, symbol))),
                )
            )
        in_pos_same = _is_in_pos_side(st, side)
        if not in_pos_same:
            try:
                open_tr = _get_open_trade(state, side, symbol)
                if isinstance(open_tr, dict):
                    in_pos_same = True
            except Exception:
                pass
        if not in_pos_same and isinstance(st.get("_no_atlas_weak"), dict):
            st.pop("_no_atlas_weak", None)
            state[symbol] = st
        live_amt_same = None
        if not in_pos_same:
            try:
                live_amt = (
                    executor_mod.get_long_position_amount(symbol)
                    if side == "LONG"
                    else executor_mod.get_short_position_amount(symbol)
                )
                live_amt_same = live_amt
                if isinstance(live_amt, (int, float)) and live_amt > 0:
                    in_pos_same = True
                    if side == "LONG":
                        st["in_pos_long"] = True
                    else:
                        st["in_pos_short"] = True
                    st["in_pos"] = True
                    state[symbol] = st
            except Exception:
                pass
        last_entry = _get_last_entry_ts_by_side(st, side)
        if not in_pos_same:
            if isinstance(last_entry, (int, float)) and (now_ts - float(last_entry)) < COOLDOWN_SEC:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _exit_cooldown_blocked(state, symbol, "swaggy_no_atlas", side, ttl_sec=COOLDOWN_SEC):
                time.sleep(PER_SYMBOL_SLEEP)
                continue
        if in_pos_same:
            weak_ctx = st.get("_no_atlas_weak")
            if isinstance(weak_ctx, dict) and str(weak_ctx.get("side") or "").upper() == side:
                confirm_fail = ""
                try:
                    confirm_fail = str(debug.get("confirm_fail") or "")
                except Exception:
                    confirm_fail = ""
                if confirm_fail:
                    _append_swaggy_no_atlas_log(
                        "SWAGGY_NO_ATLAS_WEAK_EXIT sym=%s side=%s reason=CONFIRM_FAIL confirm_fail=%s"
                        % (symbol, side, confirm_fail)
                    )
                    if side == "LONG":
                        close_long_market(symbol)
                    else:
                        close_short_market(symbol)
                    _set_last_exit_state(st, side, now_ts, "weak_confirm_fail")
                    st.pop("_no_atlas_weak", None)
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                entry_idx = weak_ctx.get("entry_idx")
                entry_px = weak_ctx.get("entry_px")
                entry_atr = weak_ctx.get("entry_atr")
                cur_idx = len(df_5m) - 1
                bars_elapsed = None
                try:
                    bars_elapsed = int(cur_idx) - int(entry_idx) + 1
                except Exception:
                    bars_elapsed = None
                mfe_abs = None
                if isinstance(entry_idx, int) and isinstance(entry_px, (int, float)):
                    try:
                        window = df_5m.iloc[int(entry_idx) : int(cur_idx) + 1]
                        if side == "LONG":
                            mfe_abs = float(window["high"].max()) - float(entry_px)
                        else:
                            mfe_abs = float(entry_px) - float(window["low"].min())
                    except Exception:
                        mfe_abs = None
                if (
                    isinstance(bars_elapsed, int)
                    and bars_elapsed >= int(SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS)
                    and isinstance(entry_atr, (int, float))
                    and isinstance(mfe_abs, (int, float))
                    and mfe_abs < float(entry_atr) * float(SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR)
                ):
                    _append_swaggy_no_atlas_log(
                        "SWAGGY_NO_ATLAS_WEAK_EXIT sym=%s side=%s reason=NO_PROGRESS bars=%s mfe_abs=%s entry_atr=%s"
                        % (
                            symbol,
                            side,
                            _fmt(bars_elapsed),
                            _fmt(mfe_abs),
                            _fmt(entry_atr),
                        )
                    )
                    if side == "LONG":
                        close_long_market(symbol)
                    else:
                        close_short_market(symbol)
                    _set_last_exit_state(st, side, now_ts, "weak_no_progress")
                    st.pop("_no_atlas_weak", None)
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
            _append_swaggy_no_atlas_log(
                f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=IN_POS side={side}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if swaggy_cfg:
            overext_val = debug.get("overext_dist_at_entry")
            if isinstance(overext_val, (int, float)):
                overext_val = abs(float(overext_val))
            if (
                swaggy_cfg.skip_overext_mid
                and isinstance(overext_val, (int, float))
                and 1.1 <= overext_val < 1.4
            ):
                _append_swaggy_no_atlas_log(
                    f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=SKIP_OVEREXT_MID overext={overext_val:.4g}"
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        break_margin = None
        entry_quality = "NA"
        entry_atr = None
        entry_sw_ok = bool(signal.entry_ok)
        entry_sw_strength = float(signal.strength or 0.0)
        entry_sw_reasons = list(signal.reasons or [])
        wait_ctx = st.get("_no_atlas_structure_wait")
        if isinstance(wait_ctx, dict):
            wait_side = str(wait_ctx.get("side") or "").upper()
            if signal.entry_ok and signal.side and signal.side.upper() != wait_side:
                _append_swaggy_no_atlas_log(
                    "STRUCTURE_BREAK_FAIL sym=%s side=%s reason=REVERSE_SIGNAL"
                    % (symbol, wait_side)
                )
                st.pop("_no_atlas_structure_wait", None)
                state[symbol] = st
            else:
                cur_idx = len(df_5m) - 1
                deadline_idx = wait_ctx.get("deadline_idx")
                if isinstance(deadline_idx, (int, float)) and cur_idx > int(deadline_idx):
                    _append_swaggy_no_atlas_log(
                        "STRUCTURE_BREAK_FAIL sym=%s side=%s reason=TIMEOUT"
                        % (symbol, wait_side)
                    )
                    st.pop("_no_atlas_structure_wait", None)
                    state[symbol] = st
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
                structure_level = wait_ctx.get("structure_level")
                entry_atr = wait_ctx.get("entry_atr")
                try:
                    structure_level = float(structure_level)
                except Exception:
                    structure_level = None
                try:
                    entry_atr = float(entry_atr)
                except Exception:
                    entry_atr = None
                if isinstance(structure_level, (int, float)) and isinstance(entry_atr, (int, float)) and entry_atr > 0:
                    close_now = float(df_5m.iloc[-1]["close"])
                    if wait_side == "LONG":
                        break_margin = (close_now - float(structure_level)) / float(entry_atr)
                    else:
                        break_margin = (float(structure_level) - close_now) / float(entry_atr)
                    if break_margin >= float(SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG):
                        entry_quality = "STRONG"
                    elif break_margin >= float(SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK):
                        entry_quality = "WEAK"
                if entry_quality != "NA":
                    _append_swaggy_no_atlas_log(
                        "STRUCTURE_BREAK_PASS sym=%s side=%s quality=%s margin=%.4f"
                        % (symbol, wait_side, entry_quality, float(break_margin or 0.0))
                    )
                    st.pop("_no_atlas_structure_wait", None)
                    state[symbol] = st
                    entry_px = close_now
                else:
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
        if signal.entry_ok and side and isinstance(entry_px, (int, float)) and entry_quality == "NA":
            touch_count = 0
            try:
                touch_count = int(debug.get("touch_count") or 0)
            except Exception:
                touch_count = 0
            lookback = 5 if touch_count >= 2 else 3
            ready_idx = len(df_5m) - 1
            start_idx = ready_idx - lookback
            if start_idx < 0 or start_idx >= ready_idx:
                _append_swaggy_no_atlas_log(
                    "STRUCTURE_SKIP sym=%s side=%s reason=INSUFFICIENT_BARS" % (symbol, side)
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            window = df_5m.iloc[start_idx:ready_idx]
            structure_level = None
            if side == "LONG":
                try:
                    structure_level = float(window["high"].max())
                except Exception:
                    structure_level = None
            elif side == "SHORT":
                try:
                    structure_level = float(window["low"].min())
                except Exception:
                    structure_level = None
            try:
                cur_atr = float(atr(df_5m, 14))
            except Exception:
                cur_atr = None
            if not isinstance(cur_atr, (int, float)) or cur_atr <= 0:
                _append_swaggy_no_atlas_log(
                    "STRUCTURE_SKIP sym=%s side=%s reason=BAD_ATR" % (symbol, side)
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            last = df_5m.iloc[-1]
            close_now = float(last["close"])
            if isinstance(structure_level, (int, float)):
                if side == "LONG":
                    break_margin = (close_now - float(structure_level)) / float(cur_atr)
                else:
                    break_margin = (float(structure_level) - close_now) / float(cur_atr)
            if not isinstance(break_margin, (int, float)):
                _append_swaggy_no_atlas_log(
                    "STRUCTURE_SKIP sym=%s side=%s reason=BAD_MARGIN" % (symbol, side)
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            deadline_idx = ready_idx + int(SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS)
            st["_no_atlas_structure_wait"] = {
                "side": side,
                "ready_idx": ready_idx,
                "deadline_idx": deadline_idx,
                "structure_level": structure_level,
                "entry_atr": cur_atr,
            }
            state[symbol] = st
            _append_swaggy_no_atlas_log(
                "STRUCTURE_WAIT_START sym=%s side=%s level=%s atr=%s deadline_idx=%s lookback=%d"
                % (symbol, side, _fmt(structure_level), _fmt(cur_atr), _fmt(deadline_idx), int(lookback))
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        overext_signed = _swaggy_no_atlas_overext_dist(df_5m, side, swaggy_cfg)
        if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED and isinstance(overext_signed, (int, float)):
            overext_thresh = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN
            if entry_quality == "STRONG":
                overext_thresh = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG
            if overext_signed < overext_thresh:
                _append_swaggy_no_atlas_log(
                    f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=SKIP_OVEREXT_DEEP overext={overext_signed:.4g}"
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        delay_ctx = st.get("_no_atlas_entry_delay")
        delay_passed = False
        if isinstance(delay_ctx, dict):
            delay_side = str(delay_ctx.get("side") or "").upper()
            deadline_idx = delay_ctx.get("deadline_idx")
            ready_idx = delay_ctx.get("ready_idx")
            ready_px = delay_ctx.get("entry_px")
            cur_idx = len(df_5m) - 1
            if isinstance(deadline_idx, (int, float)) and cur_idx > int(deadline_idx):
                _append_swaggy_no_atlas_log(
                    "ENTRY_DELAY_FAIL sym=%s side=%s reason=TIMEOUT"
                    % (symbol, delay_side)
                )
                st.pop("_no_atlas_entry_delay", None)
                state[symbol] = st
            else:
                close_now = float(df_5m.iloc[-1]["close"])
                low_now = float(df_5m.iloc[-1]["low"])
                high_now = float(df_5m.iloc[-1]["high"])
                close_ok = isinstance(ready_px, (int, float)) and close_now > float(ready_px) * (
                    1.0 + float(SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT)
                ) if delay_side == "LONG" else isinstance(ready_px, (int, float)) and close_now < float(ready_px) * (
                    1.0 - float(SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT)
                )
                strict_delay = not bool(delay_ctx.get("sw_ok"))
                sweep_ok = False
                vol_ok = False
                if not strict_delay:
                    try:
                        lookback = max(1, int(SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK))
                        if delay_side == "LONG":
                            prev_low = float(df_5m["low"].iloc[-lookback - 1 : -1].min())
                            sweep_ok = low_now < prev_low and close_now > float(ready_px or close_now)
                        else:
                            prev_high = float(df_5m["high"].iloc[-lookback - 1 : -1].max())
                            sweep_ok = high_now > prev_high and close_now < float(ready_px or close_now)
                    except Exception:
                        sweep_ok = False
                    try:
                        vol_ma = float(df_5m["volume"].iloc[-SWAGGY_NO_ATLAS_DELAY_VOL_MA : -1].mean())
                        vol_now = float(df_5m.iloc[-1]["volume"])
                        if vol_ma > 0:
                            vol_ok = vol_now >= vol_ma * float(SWAGGY_NO_ATLAS_DELAY_VOL_MULT)
                    except Exception:
                        vol_ok = False
                if strict_delay:
                    streak = int(delay_ctx.get("close_streak") or 0)
                    streak = streak + 1 if close_ok else 0
                    delay_ctx["close_streak"] = streak
                    st["_no_atlas_entry_delay"] = delay_ctx
                    pass_ok = streak >= 2
                else:
                    pass_ok = close_ok or sweep_ok or vol_ok
                if pass_ok:
                    _append_swaggy_no_atlas_log(
                        "ENTRY_DELAY_PASS sym=%s side=%s pass=Y close=%s sweep=%s vol=%s"
                        % (
                            symbol,
                            delay_side,
                            int(bool(close_ok)),
                            int(bool(sweep_ok)),
                            int(bool(vol_ok)),
                        )
                    )
                    st.pop("_no_atlas_entry_delay", None)
                    state[symbol] = st
                    entry_px = close_now
                    delay_passed = True
                    entry_quality = str(delay_ctx.get("entry_quality") or entry_quality or "STRONG")
                    break_margin = delay_ctx.get("break_margin", break_margin)
                    entry_atr = delay_ctx.get("entry_atr", entry_atr)
                    side = delay_side or side
                    entry_sw_ok = bool(delay_ctx.get("sw_ok"))
                    entry_sw_strength = float(delay_ctx.get("sw_strength") or entry_sw_strength)
                    entry_sw_reasons = list(delay_ctx.get("sw_reasons") or entry_sw_reasons)
                else:
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue

        if entry_quality == "STRONG" and any(
            reason in ("REGIME_BLOCK", "WEAK_SIGNAL") for reason in (entry_sw_reasons or [])
        ):
            _append_swaggy_no_atlas_log(
                "SWAGGY_NO_ATLAS_SKIP sym=%s reason=STRONG_BLOCK_REASONS sw_reasons=%s"
                % (symbol, ",".join(entry_sw_reasons or []))
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if not entry_sw_ok:
            reasons_set = set(entry_sw_reasons or [])
            if reasons_set and "CHASE" not in reasons_set:
                _append_swaggy_no_atlas_log(
                    "SWAGGY_NO_ATLAS_SKIP sym=%s reason=SW_OKN_REASON sw_reasons=%s"
                    % (symbol, ",".join(entry_sw_reasons or []))
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if not isinstance(break_margin, (int, float)):
                _append_swaggy_no_atlas_log(
                    "SWAGGY_NO_ATLAS_SKIP sym=%s reason=SW_OKN_NO_MARGIN" % (symbol,)
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if float(break_margin) < float(SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN):
                _append_swaggy_no_atlas_log(
                    "SWAGGY_NO_ATLAS_SKIP sym=%s reason=SW_OKN_MARGIN margin=%.4f"
                    % (symbol, float(break_margin))
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        if entry_quality == "STRONG" and SWAGGY_NO_ATLAS_DELAY_WAIT_BARS > 0 and not delay_passed and not entry_sw_ok:
            delay_ctx = st.get("_no_atlas_entry_delay")
            if not isinstance(delay_ctx, dict):
                cur_idx = len(df_5m) - 1
                st["_no_atlas_entry_delay"] = {
                    "side": side,
                    "ready_idx": cur_idx,
                    "deadline_idx": cur_idx + int(SWAGGY_NO_ATLAS_DELAY_WAIT_BARS),
                    "entry_px": entry_px,
                    "entry_quality": entry_quality,
                    "break_margin": break_margin,
                    "entry_atr": entry_atr,
                    "sw_ok": entry_sw_ok,
                    "sw_strength": entry_sw_strength,
                    "sw_reasons": entry_sw_reasons,
                    "close_streak": 0,
                }
                state[symbol] = st
                _append_swaggy_no_atlas_log(
                    "ENTRY_DELAY_START sym=%s side=%s entry_px=%s wait_bars=%d"
                    % (symbol, side, _fmt(entry_px), int(SWAGGY_NO_ATLAS_DELAY_WAIT_BARS))
                )
                time.sleep(PER_SYMBOL_SLEEP)
                continue

        entry_ok_effective = bool(entry_sw_ok) or entry_quality != "NA" or delay_passed
        if not entry_ok_effective or not signal.side or entry_px is None:
            if symbol in debug_syms or _symbol_in_pos_any(st):
                phase_info = None
                if isinstance(swaggy_no_atlas_engine, object):
                    try:
                        phase_info = (swaggy_no_atlas_engine._state or {}).get(symbol)
                    except Exception:
                        phase_info = None
                cooldown_until = None
                last_signal_ts = None
                if isinstance(phase_info, dict):
                    cooldown_until = phase_info.get("cooldown_until")
                    last_signal_ts = phase_info.get("last_signal_ts")
                cooldown_until_fmt = None
                last_signal_fmt = None
                try:
                    if isinstance(cooldown_until, (int, float)):
                        cooldown_until_fmt = _iso_kst(float(cooldown_until))
                except Exception:
                    cooldown_until_fmt = None
                try:
                    if isinstance(last_signal_ts, (int, float)):
                        last_signal_fmt = _iso_kst(float(last_signal_ts))
                except Exception:
                    last_signal_fmt = None
                _append_swaggy_no_atlas_log(
                    "SWAGGY_NO_ATLAS_SKIP sym=%s reason=ENTRY_NOT_READY entry_ok=%s side=%s entry_px=%s "
                    "phase=%s reasons=%s confirm_pass=%s confirm_fail=%s cooldown_until=%s last_signal=%s"
                    % (
                        symbol,
                        int(bool(entry_ok_effective)),
                        signal.side,
                        _fmt(debug.get("entry_px") or entry_px),
                        new_phase or prev_phase,
                        ",".join(signal.reasons or []),
                        _fmt(debug.get("confirm_pass")),
                        _fmt(debug.get("confirm_fail")),
                        cooldown_until_fmt or "N/A",
                        last_signal_fmt or "N/A",
                    )
                )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_quality_reasons = ["NO_ATLAS"]
        entry_usdt = _resolve_entry_usdt(USDT_PER_TRADE)
        if entry_quality == "STRONG" and any(
            reason == "EXPANSION_BAR" for reason in (entry_sw_reasons or [])
        ):
            entry_usdt = float(entry_usdt or 0.0) * float(SWAGGY_NO_ATLAS_EXPANSION_MULT)
        if entry_from_structure and isinstance(struct_ctx, dict):
            strength_val = float(struct_ctx.get("strength") or 0.0)
            reasons_val = struct_ctx.get("reasons")
        else:
            strength_val = float(entry_sw_strength)
            reasons_val = entry_sw_reasons
        entry_line = (
            "SWAGGY_NO_ATLAS_ENTRY sym=%s side=%s sw_strength=%.3f sw_reasons=%s "
            "final_usdt=%.2f "
            "level_score=%s touch_count=%s level_age=%s trigger_combo=%s confirm_pass=%s confirm_fail=%s "
            "overext_dist_at_touch=%s overext_dist_at_entry=%s entry_quality=%s break_margin=%s"
            % (
                symbol,
                side,
                float(strength_val),
                ",".join(reasons_val or []),
                float(entry_usdt or 0.0),
                _fmt(debug.get("level_score")),
                _fmt(debug.get("touch_count")),
                _fmt(debug.get("level_age_sec")),
                _fmt(debug.get("trigger_combo")),
                _fmt(debug.get("confirm_pass")),
                _fmt(debug.get("confirm_fail")),
                _fmt(debug.get("overext_dist_at_touch")),
                _fmt(debug.get("overext_dist_at_entry")),
                _fmt(entry_quality),
                _fmt(break_margin),
            )
        )
        _append_swaggy_no_atlas_log(entry_line)
        if entry_quality == "WEAK":
            try:
                ready_idx = len(df_5m) - 1
            except Exception:
                ready_idx = None
            entry_atr = None
            try:
                entry_atr = float(atr(df_5m, 14))
            except Exception:
                entry_atr = None
            st["_no_atlas_weak"] = {
                "side": side,
                "entry_ts": now_ts,
                "entry_idx": ready_idx,
                "entry_px": entry_px,
                "entry_atr": entry_atr,
                "break_margin": break_margin,
            }
            state[symbol] = st
        body_ratio = None
        confirm_metrics = debug.get("confirm_metrics")
        if isinstance(confirm_metrics, dict):
            body_ratio = confirm_metrics.get("body_ratio")
        if (
            swaggy_cfg.skip_confirm_body
            and isinstance(body_ratio, (int, float))
            and float(body_ratio) < 0.60
        ):
            _append_swaggy_no_atlas_log(
                f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=SKIP_CONFIRM_BODY body_ratio={body_ratio:.4g}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        cur_total = count_open_positions(force=True)
        if not isinstance(cur_total, int):
            cur_total = active_positions_total
        if cur_total >= MAX_OPEN_POSITIONS:
            _append_entry_gate_log(
                "swaggy_no_atlas",
                symbol,
                f"포지션제한={cur_total}/{MAX_OPEN_POSITIONS} side={side} in_pos_same={in_pos_same} live_amt={live_amt_same}",
                side=side,
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        lock_ok, lock_owner, lock_age = _entry_lock_acquire(state, symbol, owner="swaggy_no_atlas", side=side)
        if not lock_ok:
            _append_swaggy_no_atlas_log(
                f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=ENTRY_LOCK owner={lock_owner} age_s={lock_age:.1f}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        guard_key = _entry_guard_key(state, symbol, side)
        if not _entry_guard_acquire(state, symbol, key=guard_key, engine="swaggy_no_atlas", side=side):
            _entry_lock_release(state, symbol, owner="swaggy_no_atlas", side=side)
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        try:
            live = LONG_LIVE_TRADING if side == "LONG" else LIVE_TRADING
            _append_swaggy_no_atlas_log(
                "SWAGGY_NO_ATLAS_ENQUEUE sym=%s side=%s in_pos_same=%s in_pos_flag=%s live_amt=%s open_tr=%s reason=%s"
                % (
                    symbol,
                    side,
                    int(bool(in_pos_same)),
                    int(bool(_is_in_pos_side(st, side))),
                    _fmt(
                        executor_mod.get_long_position_amount(symbol)
                        if side == "LONG"
                        else executor_mod.get_short_position_amount(symbol)
                    )
                    if executor_mod
                    else "N/A",
                    int(bool(_get_open_trade(state, side, symbol))),
                    "swaggy_no_atlas",
                )
            )
            req_id = _enqueue_entry_request(
                state,
                symbol=symbol,
                side=side,
                engine="SWAGGY_NO_ATLAS",
                reason="swaggy_no_atlas",
                usdt=entry_usdt,
                live=live,
                alert_reason="SWAGGY_NO_ATLAS",
                alert_tag=None,
                allow_over_max=False,
            )
            if req_id:
                trade_payload = {
                    "ts": _iso_kst(),
                    "event": "SWAGGY_TRADE",
                    "engine": "SWAGGY_NO_ATLAS",
                    "mode": "live" if live else "paper",
                    "symbol": symbol,
                    "side": side,
                    "ltf": swaggy_cfg.tf_ltf,
                    "mtf": swaggy_cfg.tf_mtf,
                    "htf": swaggy_cfg.tf_htf,
                    "htf2": swaggy_cfg.tf_htf2,
                    "cycle_id": cycle_id,
                    "range_id": debug.get("touch_key"),
                    "entry_ts": _iso_kst(),
                    "entry_price": signal.entry_px,
                    "atr14_ltf": debug.get("atr14_ltf"),
                    "atr14_htf": debug.get("atr14_htf"),
                    "ema20_ltf": debug.get("ema20_ltf"),
                    "ema20_htf": debug.get("ema20_htf"),
                    "level_type": debug.get("level_type"),
                    "level_price": debug.get("level_price"),
                    "level_score": debug.get("level_score"),
                    "touch_count": debug.get("touch_count"),
                    "level_age_bars": debug.get("level_age_bars"),
                    "touch_pct": debug.get("touch_pct"),
                    "touch_atr_mult": debug.get("touch_atr_mult"),
                    "touch_pass": int(debug.get("touch_pass") or 0),
                    "touch_fail_reason": debug.get("touch_fail_reason"),
                    "trigger_combo": debug.get("trigger_combo"),
                    "trigger_strength_best": debug.get("trigger_strength_best"),
                    "trigger_strength_min": debug.get("trigger_strength_min"),
                    "trigger_strength_avg": debug.get("trigger_strength_avg"),
                    "trigger_strength_used": debug.get("trigger_strength_used"),
                    "trigger_parts": debug.get("trigger_parts"),
                    "strength_total": debug.get("strength_total"),
                    "strength_min_req": debug.get("strength_min_req"),
                    "trigger_threshold_used": debug.get("trigger_threshold_used"),
                    "use_trigger_min": debug.get("use_trigger_min"),
                    "confirm_pass": int(debug.get("confirm_pass") or 0),
                    "confirm_fail_reason": debug.get("confirm_fail"),
                    "confirm_metrics": debug.get("confirm_metrics"),
                    "confirm_body_ratio": (debug.get("confirm_metrics") or {}).get("body_ratio")
                    if isinstance(debug.get("confirm_metrics"), dict)
                    else None,
                    "overext_ema_len": debug.get("overext_ema_len"),
                    "overext_atr_mult": debug.get("overext_atr_mult"),
                    "overext_dist_at_touch": debug.get("overext_dist_at_touch"),
                    "overext_dist_at_entry": debug.get("overext_dist_at_entry"),
                    "overext_blocked": 0,
                    "overext_state": "OK",
                    "atlas_pass_soft": None,
                    "atlas_pass_hard": None,
                    "atlas_state": None,
                    "atlas_regime": None,
                    "atlas_rs": None,
                    "atlas_rs_z": None,
                    "atlas_corr": None,
                    "atlas_beta": None,
                    "atlas_vol_ratio": None,
                    "atlas_block_reason": None,
                    "entry_quality_bucket": entry_quality,
                    "entry_quality_reasons": entry_quality_reasons,
                }
                _append_swaggy_trade_json(trade_payload)
                if side == "LONG":
                    result["long_hits"] += 1
                else:
                    result["short_hits"] += 1
        except Exception as e:
            _append_swaggy_no_atlas_log(f"SWAGGY_NO_ATLAS_SKIP sym={symbol} reason=QUEUE_ERROR {e}")
        finally:
            _entry_guard_release(state, symbol, key=guard_key)
            _entry_lock_release(state, symbol, owner="swaggy_no_atlas", side=side)

        time.sleep(PER_SYMBOL_SLEEP)
    return result

def _run_adv_trend_cycle(
    adv_universe,
    cached_ex,
    state,
    send_alert,
    cycle_id: Optional[int] = None,
):
    result = {"long_hits": 0, "short_hits": 0}
    if not ADV_TREND_ENABLED or not adv_universe:
        return result
    try:
        refresh_positions_cache(force=True)
    except Exception:
        pass
    open_total = count_open_positions(force=True)
    if not isinstance(open_total, int):
        open_total = _count_open_positions_state(state)

    now_ts = time.time()
    if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now_ts):
        _append_adv_trend_log("ADV_TREND_SKIP reason=SATURDAY_OFF")
        return result

    hedge_mode = False
    try:
        hedge_mode = is_hedge_mode()
    except Exception:
        hedge_mode = False

    ltf_limit = max(ADV_TREND_EMA_LEN, 200)
    htf_limit = max(ADV_TREND_EMA_LEN, 220)

    for symbol in adv_universe:
        st = state.get(symbol, {"in_pos": False, "last_entry": 0})
        if _both_sides_open(st) and not hedge_mode:
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
            st["in_pos_long"] = bool(long_amt > 0)
            st["in_pos_short"] = bool(short_amt > 0)
            st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
            now_seen = time.time()
            if long_amt > 0:
                _set_last_entry_state(st, "LONG", now_seen)
            if short_amt > 0:
                _set_last_entry_state(st, "SHORT", now_seen)
            st.pop("adv_trend_pending", None)
            state[symbol] = st
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if isinstance(open_total, int) and open_total >= MAX_OPEN_POSITIONS:
            _append_adv_trend_log(
                f"ADV_TREND_SKIP sym={symbol} reason=MAX_POS open={open_total} max={MAX_OPEN_POSITIONS}"
            )
            break

        df_15m = cycle_cache.get_df(symbol, "15m", limit=ltf_limit)
        df_4h = cycle_cache.get_df(symbol, "4h", limit=htf_limit)
        if df_15m.empty or df_4h.empty:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if len(df_15m) < 20:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        signal = _adv_eval_15m(df_15m, df_4h)
        if not signal:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        ema200 = signal["ema200"]
        mfi_val = signal["mfi"]
        adx_val = signal["adx"]
        st_px = signal["st_px"]
        trend_dir = signal["trend_dir"]
        close_px = signal["close_px"]
        atr_val = signal["atr"]
        signal_ts = signal.get("ts")
        try:
            rsi_val = float(_adv_rsi(df_15m["close"], 14).iloc[-1])
        except Exception:
            rsi_val = None
        try:
            atr14 = float(_adv_atr(df_15m, 14).iloc[-1])
        except Exception:
            atr14 = None
        try:
            ema7 = float(ema(df_15m["close"], 7).iloc[-1])
            ema20 = float(ema(df_15m["close"], 20).iloc[-1])
        except Exception:
            ema7 = None
            ema20 = None

        close_series = df_15m["close"].iloc[-20:]
        bb_mid = close_series.mean()
        bb_std = close_series.std(ddof=0)
        bb_upper = bb_mid + (2.0 * bb_std)
        bb_lower = bb_mid - (2.0 * bb_std)

        long_ok = (
            close_px > ema200
            and mfi_val < ADV_TREND_MFI_LONG_MAX
            and adx_val > ADV_TREND_ADX_MIN
            and trend_dir == 1
        )
        short_ok = (
            close_px < ema200
            and mfi_val > ADV_TREND_MFI_SHORT_MIN
            and adx_val > ADV_TREND_ADX_MIN
            and trend_dir == -1
        )
        if long_ok and isinstance(rsi_val, (int, float)) and rsi_val >= 70:
            long_ok = False
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=RSI_OVERHEAT side=LONG rsi={rsi_val:.2f}")
        if short_ok and isinstance(rsi_val, (int, float)) and rsi_val <= 30:
            short_ok = False
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=RSI_OVERHEAT side=SHORT rsi={rsi_val:.2f}")
        if long_ok and isinstance(ema7, (int, float)) and isinstance(ema20, (int, float)) and ema20 > 0:
            if ema7 > (ema20 * 1.03):
                long_ok = False
                _append_adv_trend_log(
                    f"ADV_TREND_SKIP sym={symbol} reason=EMA_GAP side=LONG ema7={ema7:.6g} ema20={ema20:.6g}"
                )
        if short_ok and isinstance(ema7, (int, float)) and isinstance(ema20, (int, float)) and ema20 > 0:
            if ema7 < (ema20 * 0.97):
                short_ok = False
                _append_adv_trend_log(
                    f"ADV_TREND_SKIP sym={symbol} reason=EMA_GAP side=SHORT ema7={ema7:.6g} ema20={ema20:.6g}"
                )
        if (long_ok or short_ok) and isinstance(atr14, (int, float)) and atr14 > 0:
            dist = abs(close_px - st_px)
            if dist > (atr14 * 2.5):
                _append_adv_trend_log(
                    f"ADV_TREND_SKIP sym={symbol} reason=ATR_DIST side={'LONG' if long_ok else 'SHORT'} "
                    f"dist={dist:.6g} atr14={atr14:.6g}"
                )
                long_ok = False
                short_ok = False
        if long_ok and isinstance(bb_upper, (int, float)) and close_px > bb_upper:
            long_ok = False
            _append_adv_trend_log(
                f"ADV_TREND_SKIP sym={symbol} reason=BB_OVERHEAT side=LONG close={close_px:.6g} bb_upper={bb_upper:.6g}"
            )
        if short_ok and isinstance(bb_lower, (int, float)) and close_px < bb_lower:
            short_ok = False
            _append_adv_trend_log(
                f"ADV_TREND_SKIP sym={symbol} reason=BB_OVERHEAT side=SHORT close={close_px:.6g} bb_lower={bb_lower:.6g}"
            )
        if not long_ok and not short_ok:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if long_ok and short_ok:
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        side = "LONG" if long_ok else "SHORT"

        if _exit_cooldown_blocked(state, symbol, "advanced_trend_follower", side):
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=EXIT_COOLDOWN side={side}")
            _append_entry_gate_log("advanced_trend_follower", symbol, "exit_cooldown", side=side)
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if not _entry_guard_acquire(state, symbol, ttl_sec=5.0, key=f"adv_trend:{symbol}", engine="advanced_trend_follower", side=side):
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        risk_per_unit = abs(close_px - st_px)
        if risk_per_unit <= 0 or atr_val <= 0:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=BAD_STOP side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if risk_per_unit < (atr_val * ADV_TREND_MIN_STOP_ATR):
            _append_adv_trend_log(
                f"ADV_TREND_SKIP sym={symbol} reason=MIN_STOP side={side} dist={risk_per_unit:.6g} atr={atr_val:.6g}"
            )
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if side == "LONG" and st_px >= close_px:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=STOP_ABOVE side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if side == "SHORT" and st_px <= close_px:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=STOP_BELOW side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        avail = get_available_usdt()
        if not isinstance(avail, (int, float)) or avail <= 0:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=NO_BALANCE side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        risk_usdt = float(avail) * (ADV_TREND_RISK_PCT / 100.0)
        qty = risk_usdt / risk_per_unit
        if qty <= 0:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=QTY_ZERO side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        notional = qty * close_px
        entry_pct = None
        try:
            entry_pct = float(USDT_PER_TRADE)
        except Exception:
            entry_pct = None
        if isinstance(entry_pct, (int, float)) and entry_pct > 0:
            entry_cap = float(avail) * (entry_pct / 100.0)
            if entry_cap > 0:
                notional = min(notional, entry_cap)
        max_notional = float(avail) * float(ADV_TREND_MAX_NOTIONAL_MULT)
        if max_notional > 0 and notional > max_notional:
            notional = max_notional
            qty = notional / close_px
        if notional <= 0:
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=NOTIONAL_ZERO side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        if not _admin_is_active():
            _append_adv_trend_log(f"ADV_TREND_SKIP sym={symbol} reason=ADMIN_INACTIVE side={side}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue
        if side == "LONG":
            res = _EXEC_LONG_MARKET(symbol, usdt_amount=notional, leverage=LEVERAGE, margin_mode=MARGIN_MODE)
        else:
            res = _EXEC_SHORT_MARKET(symbol, usdt_amount=notional, leverage=LEVERAGE, margin_mode=MARGIN_MODE)

        admin_status = _extract_status(res)
        admin_ok = admin_status in ("ok", "dry_run", "skip")
        if not admin_ok:
            _append_adv_trend_log(f"ADV_TREND_ENTRY_FAIL sym={symbol} side={side} status={admin_status}")
            time.sleep(PER_SYMBOL_SLEEP)
            continue

        entry_order_id = _order_id_from_res(res)
        entry_px = res.get("last")
        if entry_px is None:
            order = res.get("order") if isinstance(res.get("order"), dict) else {}
            entry_px = order.get("average") or order.get("price")
        if not isinstance(entry_px, (int, float)) or entry_px <= 0:
            entry_px = close_px

        if side == "LONG":
            sl_res = _EXEC_PLACE_LONG_SL_PX(symbol, st_px)
        else:
            sl_res = _EXEC_PLACE_SHORT_SL_PX(symbol, st_px)
        sl_order_id = _order_id_from_res(sl_res) if isinstance(sl_res, dict) else None

        tp1_px = None
        if side == "LONG":
            tp1_px = entry_px + (ADV_TREND_TP1_R_MULT * (entry_px - st_px))
        else:
            tp1_px = entry_px - (ADV_TREND_TP1_R_MULT * (st_px - entry_px))

        meta = {
            "reason": "advanced_trend_follower",
            "engine": "ADVANCED_TREND_FOLLOWER",
            "sl_price": st_px,
            "tp1_price": tp1_px,
            "tp2_mode": "SUPER_TREND",
            "tp1_done": False,
            "sl_order_id": sl_order_id,
        }
        qty_fill = res.get("amount") or (res.get("order") or {}).get("amount") if isinstance(res, dict) else None
        _log_trade_entry(
            state,
            side=side,
            symbol=symbol,
            entry_ts=time.time(),
            entry_price=entry_px,
            qty=qty_fill if isinstance(qty_fill, (int, float)) else None,
            usdt=notional,
            entry_order_id=entry_order_id,
            meta=meta,
        )
        if isinstance(open_total, int):
            open_total += 1

        follower_calls = []
        for acct in FOLLOWER_CONTEXTS:
            try:
                avail_f = acct.executor.get_available_usdt()
            except Exception:
                avail_f = None
            if not isinstance(avail_f, (int, float)) or avail_f <= 0:
                follower_calls.append({"acct": acct, "skip": "no_balance"})
                continue
            risk_usdt_f = float(avail_f) * (ADV_TREND_RISK_PCT / 100.0)
            qty_f = risk_usdt_f / risk_per_unit
            if qty_f <= 0:
                follower_calls.append({"acct": acct, "skip": "qty_zero"})
                continue
            notional_f = qty_f * close_px
            entry_pct_f = None
            try:
                entry_pct_f = float(getattr(acct.settings, "entry_pct", USDT_PER_TRADE))
            except Exception:
                entry_pct_f = None
            if isinstance(entry_pct_f, (int, float)) and entry_pct_f > 0:
                entry_cap_f = float(avail_f) * (entry_pct_f / 100.0)
                if entry_cap_f > 0:
                    notional_f = min(notional_f, entry_cap_f)
            max_notional_f = float(avail_f) * float(ADV_TREND_MAX_NOTIONAL_MULT)
            if max_notional_f > 0 and notional_f > max_notional_f:
                notional_f = max_notional_f
            if notional_f <= 0:
                follower_calls.append({"acct": acct, "skip": "notional_zero"})
                continue
            if side == "LONG":
                follower_calls.append({
                    "acct": acct,
                    "fn": lambda a=acct, u=notional_f: a.executor.long_market(symbol, usdt_amount=u, leverage=LEVERAGE, margin_mode=MARGIN_MODE),
                })
            else:
                follower_calls.append({
                    "acct": acct,
                    "fn": lambda a=acct, u=notional_f: a.executor.short_market(symbol, usdt_amount=u, leverage=LEVERAGE, margin_mode=MARGIN_MODE),
                })
        action_name = "long_market" if side == "LONG" else "short_market"
        results = _broadcast_followers(action_name, follower_calls, {"symbol": symbol})
        _set_last_entry_broadcast(symbol, side, admin_status, admin_ok, results)

        for acct in FOLLOWER_CONTEXTS:
            try:
                if side == "LONG":
                    acct.executor.place_long_sl_px(symbol, st_px)
                else:
                    acct.executor.place_short_sl_px(symbol, st_px)
            except Exception:
                continue

        _append_adv_trend_log(
            "ADV_TREND_ENTRY "
            f"sym={symbol} side={side} entry={entry_px:.6g} sl={st_px:.6g} tp1={tp1_px:.6g} "
            f"close={close_px:.6g} ema200={ema200:.6g} mfi={mfi_val:.4g} adx={adx_val:.4g} "
            f"st_dir={trend_dir} bb_upper={bb_upper:.6g} bb_lower={bb_lower:.6g}"
        )
        _send_entry_alert(
            send_alert,
            side=side,
            symbol=symbol,
            engine="ADVANCED_TREND_FOLLOWER",
            entry_price=entry_px,
            usdt=notional,
            reason=_display_engine_label("ADVANCED_TREND_FOLLOWER"),
            sl=f"{st_px:.6g}",
            tp=f"{tp1_px:.6g}",
            entry_order_id=entry_order_id,
            extras=["기준: SL=SuperTrend, TP1=1.5R"],
            state=state,
        )

        if side == "LONG":
            result["long_hits"] += 1
        else:
            result["short_hits"] += 1

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


def _symbol_in_pos_any(st: Optional[dict]) -> bool:
    if not isinstance(st, dict):
        return False
    if ("in_pos_long" in st) or ("in_pos_short" in st):
        return bool(st.get("in_pos_long")) or bool(st.get("in_pos_short"))
    return bool(st.get("in_pos"))


def _is_in_pos_side(st: Optional[dict], side: str) -> bool:
    if not isinstance(st, dict):
        return False
    side_key = (side or "").upper()
    if side_key == "LONG":
        if "in_pos_long" in st:
            return bool(st.get("in_pos_long"))
    elif side_key == "SHORT":
        if "in_pos_short" in st:
            return bool(st.get("in_pos_short"))
    return bool(st.get("in_pos"))


def _both_sides_open(st: Optional[dict]) -> bool:
    if not isinstance(st, dict):
        return False
    if ("in_pos_long" in st) or ("in_pos_short" in st):
        return bool(st.get("in_pos_long")) and bool(st.get("in_pos_short"))
    return False


def _has_open_side(st: Optional[dict], side: str) -> bool:
    return _is_in_pos_side(st, side)


def _set_in_pos_side(st: Dict[str, Any], side: str, val: bool) -> None:
    side_key = (side or "").upper()
    if side_key == "LONG":
        st["in_pos_long"] = bool(val)
    elif side_key == "SHORT":
        st["in_pos_short"] = bool(val)
    st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))


def _count_open_positions_state(state: Dict[str, dict]) -> int:
    total = 0
    for st in state.values():
        if not isinstance(st, dict):
            continue
        if ("in_pos_long" in st) or ("in_pos_short" in st):
            total += 1 if st.get("in_pos_long") else 0
            total += 1 if st.get("in_pos_short") else 0
        elif st.get("in_pos"):
            total += 1
    return total



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
        ttl_sec = COOLDOWN_SEC
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    last_exit_ts = _get_last_exit_ts_by_side(st, side)
    if not isinstance(last_exit_ts, (int, float)):
        last_exit_ts = None
    db_ts = None
    if dbrec and dbrec.ENABLED:
        now_db = time.time()
        cache = _DB_EXIT_CACHE
        data = cache.get("data") if isinstance(cache.get("data"), dict) else {}
        if (now_db - float(cache.get("ts") or 0.0)) > 5.0 or not isinstance(data, dict):
            try:
                db_path = getattr(dbrec, "DB_PATH", "") or os.path.join(os.path.dirname(__file__), "logs", "trades.db")
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                rows = cur.execute(
                    """
                    SELECT symbol, side, MAX(ts) as last_ts
                    FROM events
                    WHERE event_type = 'EXIT'
                    GROUP BY symbol, side
                    """
                ).fetchall()
                data = {}
                for sym, side_val, ts_val in rows:
                    if not sym or not side_val or ts_val is None:
                        continue
                    data[(sym, str(side_val).upper())] = float(ts_val)
                conn.close()
            except Exception:
                data = {}
            cache["data"] = data if isinstance(data, dict) else {}
            cache["ts"] = now_db
        key = (symbol, side)
        db_ts = data.get(key) if isinstance(data, dict) else None
        if isinstance(db_ts, (int, float)):
            _set_last_exit_state(st, side, float(db_ts), "db_exit")
            state[symbol] = st
            last_exit_ts = float(db_ts)
    if MANAGE_WS_MODE:
        now_disk = time.time()
        cache = _DISK_STATE_CACHE
        data = cache.get("data") if isinstance(cache.get("data"), dict) else {}
        if (now_disk - float(cache.get("ts") or 0.0)) > 5.0 or not isinstance(data, dict):
            try:
                data = load_state()
            except Exception:
                data = {}
            cache["data"] = data if isinstance(data, dict) else {}
            cache["ts"] = now_disk
        disk_st = data.get(symbol) if isinstance(data, dict) else None
        if isinstance(disk_st, dict):
            disk_exit_ts = _get_last_exit_ts_by_side(disk_st, side)
            disk_exit_reason = _get_last_exit_reason_by_side(disk_st, side)
            if isinstance(disk_exit_ts, (int, float)) and (
                last_exit_ts is None or float(disk_exit_ts) > float(last_exit_ts)
            ):
                _set_last_exit_state(st, side, float(disk_exit_ts), str(disk_exit_reason or ""))
                state[symbol] = st
                last_exit_ts = float(disk_exit_ts)
    if not isinstance(last_exit_ts, (int, float)):
        return False
    now = time.time()
    if (now - float(last_exit_ts)) < ttl_sec:
        db_note = ""
        if isinstance(db_ts, (int, float)) and float(db_ts) == float(last_exit_ts):
            try:
                db_note = f" db_ts={_iso_kst(float(db_ts))}"
            except Exception:
                db_note = ""
        _append_entry_gate_log(
            engine,
            symbol,
            f"청산쿨다운={int(ttl_sec)}s side={side}{db_note}",
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

def _manual_alert_key(symbol: str, side: str) -> str:
    return f"{symbol}|{side.upper()}"

def _get_manual_alerted(state: Dict[str, dict]) -> Dict[str, float]:
    cache = state.get("_manual_alerted")
    if not isinstance(cache, dict):
        cache = {}
        state["_manual_alerted"] = cache
    return cache

def _manual_alert_info(state: Dict[str, dict], symbol: str, side: str) -> Optional[dict]:
    cache = _get_manual_alerted(state)
    info = cache.get(_manual_alert_key(symbol, side))
    return info if isinstance(info, dict) else None

def _mark_manual_alerted(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    entry_price: Optional[float] = None,
    qty: Optional[float] = None,
) -> None:
    cache = _get_manual_alerted(state)
    cache[_manual_alert_key(symbol, side)] = {
        "ts": time.time(),
        "entry": entry_price,
        "qty": qty,
    }

def _clear_manual_alerted(state: Dict[str, dict], symbol: str, side: str) -> None:
    cache = _get_manual_alerted(state)
    cache.pop(_manual_alert_key(symbol, side), None)

def _entry_alert_key(symbol: str, side: str) -> str:
    return f"{symbol}|{side.upper()}"

def _get_entry_alerted(state: Dict[str, dict]) -> Dict[str, dict]:
    cache = state.get("_entry_alerted")
    if not isinstance(cache, dict):
        cache = {}
        state["_entry_alerted"] = cache
    return cache

def _entry_alert_info(state: Dict[str, dict], symbol: str, side: str) -> Optional[dict]:
    cache = _get_entry_alerted(state)
    info = cache.get(_entry_alert_key(symbol, side))
    return info if isinstance(info, dict) else None

def _mark_entry_alerted(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    engine: Optional[str] = None,
    reason: Optional[str] = None,
    entry_order_id: Optional[str] = None,
) -> None:
    cache = _get_entry_alerted(state)
    now_ts = time.time()
    cache[_entry_alert_key(symbol, side)] = {
        "ts": now_ts,
        "engine": engine,
        "reason": reason,
        "entry_order_id": entry_order_id,
    }
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    if not isinstance(st, dict):
        st = {}
    suffix = "long" if side.upper() == "LONG" else "short"
    st[f"entry_alerted_{suffix}"] = True
    st[f"entry_alerted_{suffix}_ts"] = now_ts
    if engine:
        st[f"entry_alerted_{suffix}_engine"] = str(engine)
    if reason:
        st[f"entry_alerted_{suffix}_reason"] = str(reason)
    if entry_order_id:
        st[f"entry_alerted_{suffix}_order_id"] = entry_order_id
    state[symbol] = st

def _clear_entry_alerted(state: Dict[str, dict], symbol: str, side: str) -> None:
    cache = _get_entry_alerted(state)
    cache.pop(_entry_alert_key(symbol, side), None)
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    if not isinstance(st, dict):
        return
    suffix = "long" if side.upper() == "LONG" else "short"
    st.pop(f"entry_alerted_{suffix}", None)
    st.pop(f"entry_alerted_{suffix}_ts", None)
    st.pop(f"entry_alerted_{suffix}_engine", None)
    st.pop(f"entry_alerted_{suffix}_reason", None)
    st.pop(f"entry_alerted_{suffix}_order_id", None)
    state[symbol] = st

def _dca_alert_key(symbol: str, side: str, adds_done: int) -> str:
    return f"{symbol}|{side.upper()}|{adds_done}"

def _get_dca_alerted(state: Dict[str, dict]) -> Dict[str, float]:
    cache = state.get("_dca_alerted")
    if not isinstance(cache, dict):
        cache = {}
        state["_dca_alerted"] = cache
    return cache

def _dca_alerted(state: Dict[str, dict], symbol: str, side: str, adds_done: int) -> bool:
    cache = _get_dca_alerted(state)
    return _dca_alert_key(symbol, side, adds_done) in cache

def _mark_dca_alerted(state: Dict[str, dict], symbol: str, side: str, adds_done: int) -> None:
    cache = _get_dca_alerted(state)
    cache[_dca_alert_key(symbol, side, adds_done)] = time.time()

def _clear_dca_alerted(state: Dict[str, dict], symbol: str, side: str) -> None:
    cache = _get_dca_alerted(state)
    prefix = f"{symbol}|{side.upper()}|"
    for key in list(cache.keys()):
        if str(key).startswith(prefix):
            cache.pop(key, None)


def _manage_pending_key(symbol: str, side: str) -> str:
    return f"{symbol}|{side.upper()}"

def _get_manage_pending(state: Dict[str, dict]) -> Dict[str, dict]:
    pending = state.get("_manage_pending")
    if not isinstance(pending, dict):
        pending = {}
        state["_manage_pending"] = pending
    return pending

def _mark_manage_pending(state: Dict[str, dict], symbol: str, side: str, req_id: str) -> None:
    pending = _get_manage_pending(state)
    pending[_manage_pending_key(symbol, side)] = {"id": req_id, "ts": time.time()}

def _clear_manage_pending(state: Dict[str, dict], symbol: str, side: str) -> None:
    pending = _get_manage_pending(state)
    pending.pop(_manage_pending_key(symbol, side), None)

def _is_manage_pending(state: Dict[str, dict], symbol: str, side: str) -> bool:
    pending = _get_manage_pending(state)
    key = _manage_pending_key(symbol, side)
    item = pending.get(key)
    if not isinstance(item, dict):
        return False
    ts = item.get("ts")
    if isinstance(ts, (int, float)) and (time.time() - float(ts)) <= MANAGE_QUEUE_PENDING_TTL_SEC:
        return True
    pending.pop(key, None)
    return False

def _enqueue_entry_request(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    engine: str,
    reason: str,
    usdt: float,
    live: bool,
    alert_reason: Optional[str] = None,
    alert_tag: Optional[str] = None,
    entry_price_hint: Optional[float] = None,
    size_mult: Optional[float] = None,
    notify: bool = False,
    allow_over_max: bool = False,
) -> Optional[str]:
    if _is_manage_pending(state, symbol, side):
        _append_entry_gate_log(engine.lower(), symbol, f"pending_request side={side}", side=side)
        return None
    cur_total = None
    try:
        cur_total = count_open_positions(force=True)
    except Exception:
        cur_total = None
    if not isinstance(cur_total, int):
        cur_total = _count_open_positions_state(state)
    if not allow_over_max and isinstance(cur_total, int) and cur_total >= MAX_OPEN_POSITIONS:
        _append_entry_gate_log(engine.lower(), symbol, f"pos_limit={cur_total}/{MAX_OPEN_POSITIONS}", side=side)
        return None
    _log_entry_usdt_debug(symbol, engine, usdt)
    payload = {
        "type": "entry",
        "symbol": symbol,
        "side": side.upper(),
        "engine": engine,
        "reason": reason,
        "alert_reason": alert_reason,
        "alert_tag": alert_tag,
        "usdt": float(usdt),
        "leverage": LEVERAGE,
        "margin_mode": MARGIN_MODE,
        "live": bool(live),
        "entry_price_hint": entry_price_hint,
        "size_mult": size_mult,
    }
    req_id = manage_queue.enqueue_request(payload)
    _mark_manage_pending(state, symbol, side, req_id)
    if notify:
        price_text = _fmt_float(entry_price_hint, 6) if isinstance(entry_price_hint, (int, float)) else "N/A"
        send_telegram(
            f"📝 <b>QUEUE</b>\n"
            f"<b>{symbol}</b>\n"
            f"{side} entry={price_text}"
        )
    return req_id

def _entry_lock_acquire(
    state: Dict[str, dict],
    symbol: str,
    owner: str,
    ttl_sec: float = 10.0,
    side: Optional[str] = None,
) -> tuple[bool, Optional[str], Optional[float]]:
    lock = _get_entry_lock(state)
    now = time.time()
    key = f"{symbol}|{(side or '').upper()}" if side else symbol
    with _ENTRY_LOCK_MUTEX:
        cur = lock.get(key)
        if isinstance(cur, dict):
            expires = float(cur.get("expires", 0.0) or 0.0)
            if now < expires and cur.get("owner"):
                held_by = str(cur.get("owner"))
                _append_entry_gate_log(owner, symbol, f"entry_lock_held_by={held_by}", side=side)
                return False, held_by, expires - now
        lock[key] = {"owner": owner, "expires": now + ttl_sec, "ts": now}
    return True, None, None


def _entry_lock_release(state: Dict[str, dict], symbol: str, owner: Optional[str] = None, side: Optional[str] = None) -> None:
    lock = _get_entry_lock(state)
    key = f"{symbol}|{(side or '').upper()}" if side else symbol
    with _ENTRY_LOCK_MUTEX:
        cur = lock.get(key)
        if not isinstance(cur, dict):
            return
        if owner and cur.get("owner") != owner:
            return
        lock.pop(key, None)

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
        _set_in_pos_side(st, side, True)
        state[symbol] = st
    except Exception:
        pass
    if entry_order_id:
        st = state.get(symbol) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        st[f"entry_order_id_{side.lower()}"] = entry_order_id
        state[symbol] = st

def _append_trade_log_only(
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
    log.append(tr)
    try:
        st = state.get(symbol) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        _set_last_entry_state(st, side, float(entry_ts))
        _set_in_pos_side(st, side, True)
        if entry_order_id:
            st[f"entry_order_id_{side.lower()}"] = entry_order_id
        state[symbol] = st
    except Exception:
        pass

def _sync_trade_log_from_db(state: Dict[str, dict], symbol: str, side: str) -> None:
    if _get_open_trade(state, side, symbol):
        return
    if not dbrec:
        return
    try:
        dbrec._get_conn()
    except Exception:
        pass
    if not dbrec.ENABLED:
        return
    try:
        db_path = getattr(dbrec, "DB_PATH", "") or os.path.join(os.path.dirname(__file__), "logs", "trades.db")
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            """
            SELECT ts, source, qty, avg_entry, meta_json
            FROM events
            WHERE symbol = ? AND side = ? AND event_type = 'ENTRY'
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, side),
        ).fetchone()
        conn.close()
    except Exception:
        row = None
    if not row:
        return
    try:
        ts_val = float(row[0]) if row[0] is not None else None
    except Exception:
        ts_val = None
    if ts_val is None:
        return
    source = row[1] or ""
    qty = row[2]
    avg_entry = row[3]
    meta = None
    entry_order_id = None
    if row[4]:
        try:
            meta = json.loads(row[4])
        except Exception:
            meta = None
    if isinstance(meta, dict):
        entry_order_id = meta.get("entry_order_id")
        if not meta.get("reason") and meta.get("engine"):
            meta["reason"] = _reason_from_engine_label(meta.get("engine"), side)
    if not meta:
        meta = {"reason": source} if source else {}
    _append_trade_log_only(
        state,
        side=side,
        symbol=symbol,
        entry_ts=ts_val,
        entry_price=avg_entry if isinstance(avg_entry, (int, float)) else None,
        qty=qty if isinstance(qty, (int, float)) else None,
        usdt=None,
        entry_order_id=entry_order_id,
        meta=meta,
    )

def _get_open_trade(state: Dict[str, dict], side: str, symbol: str) -> Optional[dict]:
    log = _get_trade_log(state)
    for tr in reversed(log):
        if tr.get("status") == "open" and tr.get("side") == side and tr.get("symbol") == symbol:
            return tr
    return None

def _get_open_trade_or_backfill(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    now_ts: Optional[float] = None,
) -> Optional[dict]:
    tr = _get_open_trade(state, side, symbol)
    if tr:
        return tr
    return _backfill_open_trade_from_db(state, symbol, side, now_ts=now_ts)

def _db_latest_position_snapshot(symbol: str, side: str) -> Optional[dict]:
    if not dbrec:
        return None
    try:
        # ensure .env has been loaded and dbrec.ENABLED refreshed
        dbrec._get_conn()
    except Exception:
        pass
    if not dbrec.ENABLED:
        return None
    try:
        db_path = getattr(dbrec, "DB_PATH", "") or os.path.join(os.path.dirname(__file__), "logs", "trades.db")
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """
            SELECT ts, side, qty, avg_entry
            FROM positions
            WHERE symbol = ?
            ORDER BY ts DESC
            LIMIT 20
            """,
            (symbol,),
        ).fetchall()
        conn.close()
    except Exception:
        return None
    want_side = (side or "").upper()
    for row in rows or []:
        try:
            ts_val = float(row[0]) if row[0] is not None else None
        except Exception:
            ts_val = None
        row_side = str(row[1] or "").upper()
        try:
            qty_val = float(row[2]) if row[2] is not None else None
        except Exception:
            qty_val = None
        try:
            avg_entry = float(row[3]) if row[3] is not None else None
        except Exception:
            avg_entry = None
        if ts_val is None or qty_val is None or qty_val == 0:
            continue
        if want_side == "LONG":
            if row_side != "LONG":
                continue
            if qty_val <= 0:
                continue
            return {"ts": ts_val, "qty": float(qty_val), "avg_entry": avg_entry}
        if want_side == "SHORT":
            if row_side != "SHORT":
                continue
            if qty_val >= 0 and row_side not in ("SHORT", "BOTH", ""):
                continue
            return {"ts": ts_val, "qty": float(abs(qty_val)), "avg_entry": avg_entry}
    return None

def _find_entry_event_for_backfill(
    symbol: str,
    side: str,
    ref_ts: Optional[float] = None,
    now_ts: Optional[float] = None,
    window_sec: float = 7 * 24 * 3600,
) -> Optional[dict]:
    now_ts = float(now_ts if isinstance(now_ts, (int, float)) else time.time())
    since_ts = now_ts - float(window_sec)
    _, by_symbol = _load_entry_events_map(since_ts=since_ts)
    recs = by_symbol.get((symbol, (side or "").upper())) if isinstance(by_symbol, dict) else None
    if not recs:
        return None
    if isinstance(ref_ts, (int, float)):
        best = None
        best_gap = None
        for rec in recs:
            ts_val = rec.get("entry_ts")
            if not isinstance(ts_val, (int, float)):
                continue
            gap = abs(float(ts_val) - float(ref_ts))
            if best_gap is None or gap < best_gap:
                best_gap = gap
                best = rec
        return best
    return max(recs, key=lambda r: float(r.get("entry_ts") or 0.0))

def _backfill_open_trade_from_db(
    state: Dict[str, dict],
    symbol: str,
    side: str,
    now_ts: Optional[float] = None,
) -> Optional[dict]:
    if _get_open_trade(state, side, symbol):
        return _get_open_trade(state, side, symbol)
    pos = _db_latest_position_snapshot(symbol, side)
    if not isinstance(pos, dict):
        return None
    ref_ts = pos.get("ts")
    rec = _find_entry_event_for_backfill(symbol, side, ref_ts=ref_ts, now_ts=now_ts)
    entry_ts = None
    entry_order_id = None
    engine_label = None
    entry_price = pos.get("avg_entry")
    if isinstance(rec, dict):
        entry_ts = rec.get("entry_ts")
        entry_order_id = rec.get("entry_order_id")
        engine_label = str(rec.get("engine") or "").upper() or None
        if isinstance(rec.get("entry_price"), (int, float)):
            entry_price = float(rec.get("entry_price"))
    if not isinstance(entry_ts, (int, float)):
        entry_ts = float(ref_ts) if isinstance(ref_ts, (int, float)) else time.time()
    qty = pos.get("qty")
    try:
        qty = float(qty) if qty is not None else None
    except Exception:
        qty = None
    reason = _reason_from_engine_label(engine_label, side) if engine_label else None
    if not reason:
        reason = "manual_entry"
    meta = {"reason": reason}
    if engine_label:
        meta["engine"] = engine_label
    _log_trade_entry(
        state,
        side=side,
        symbol=symbol,
        entry_ts=float(entry_ts),
        entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
        qty=qty if isinstance(qty, (int, float)) else None,
        usdt=None,
        entry_order_id=entry_order_id,
        meta=meta,
    )
    return _get_open_trade(state, side, symbol)

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
    if key in ("swaggy_long", "swaggy_short"):
        return "SWAGGY"
    if key == "swaggy_atlas_lab":
        return "SWAGGY_ATLAS_LAB"
    if key == "swaggy_atlas_lab_v2":
        return "SWAGGY_ATLAS_LAB_V2"
    if key == "swaggy_no_atlas":
        return "SWAGGY_NO_ATLAS"
    if key == "loss_hedge_engine":
        return "LOSS_HEDGE_ENGINE"
    if key in ("dtfx_long", "dtfx_short"):
        return "DTFX"
    if key == "atlas_rs_fail_short":
        return "ATLAS_RS_FAIL_SHORT"
    if key == "short_entry":
        return "RSI"
    if key == "long_entry":
        return "SCALP"
    if key in ("manual", "manual_entry"):
        return "MANUAL"
    if key in ("manual_admin", "admin_manual"):
        return "MANUAL_ADMIN"
    if key in ("관리자수동진입", "admin_manual_entry"):
        return "관리자수동진입"
    if key in ("advanced_trend_follower", "adv_trend"):
        return "ADVANCED_TREND_FOLLOWER"
    return "UNKNOWN"

def _reason_from_engine_label(engine_label: Optional[str], side: str) -> Optional[str]:
    label = (engine_label or "").upper()
    if label == "SWAGGY_ATLAS_LAB":
        return "swaggy_atlas_lab"
    if label == "SWAGGY_ATLAS_LAB_V2":
        return "swaggy_atlas_lab_v2"
    if label == "SWAGGY_NO_ATLAS":
        return "swaggy_no_atlas"
    if label == "LOSS_HEDGE_ENGINE":
        return "loss_hedge_engine"
    if label == "SWAGGY":
        return "swaggy_long" if side == "LONG" else "swaggy_short"
    if label == "ATLAS_RS_FAIL_SHORT":
        return "atlas_rs_fail_short"
    if label == "DTFX":
        return "dtfx_long" if side == "LONG" else "dtfx_short"
    if label == "RSI":
        return "short_entry"
    if label == "SCALP":
        return "long_entry"
    if label == "MANUAL":
        return "manual_entry"
    if label == "ADVANCED_TREND_FOLLOWER":
        return "advanced_trend_follower"
    return None

def _display_engine_label(label: Optional[str]) -> str:
    name = (label or "").strip() or "UNKNOWN"
    overrides = {
        "ATLAS_RS_FAIL_SHORT": "아틀라스 숏",
        "SWAGGY_ATLAS_LAB": "스웨기랩",
        "SWAGGY_ATLAS_LAB_V2": "스웨기랩v2",
        "SWAGGY_NO_ATLAS": "스웨기 단독",
        "LOSS_HEDGE_ENGINE": "손실방지엔진",
        "ADVANCED_TREND_FOLLOWER": "트리플체크",
    }
    return overrides.get(name, name)

def _is_engine_enabled(engine: str) -> bool:
    key = (engine or "").upper()
    if key in ("SWAGGY_ATLAS_LAB", "SWAGGY_ATLAS_LAB_V2", "SWAGGY", "SWAGGY_NO_ATLAS"):
        if key == "SWAGGY_ATLAS_LAB":
            return SWAGGY_ATLAS_LAB_ENABLED
        if key == "SWAGGY_ATLAS_LAB_V2":
            return SWAGGY_ATLAS_LAB_V2_ENABLED
        if key == "SWAGGY_NO_ATLAS":
            return SWAGGY_NO_ATLAS_ENABLED
        return SWAGGY_ENABLED
    if key == "LOSS_HEDGE_ENGINE":
        return LOSS_HEDGE_ENGINE_ENABLED
    if key == "DTFX":
        return DTFX_ENABLED
    if key == "ATLAS_RS_FAIL_SHORT":
        return ATLAS_RS_FAIL_SHORT_ENABLED
    if key == "ADVANCED_TREND_FOLLOWER":
        return ADV_TREND_ENABLED
    if key in ("RSI", "SCALP"):
        return RSI_ENABLED
    if key in ("MANUAL", "UNKNOWN", ""):
        return True
    return True

def _normalize_engine_key(engine: str) -> str:
    return (engine or "").strip().upper()

def _format_engine_exit_overrides() -> str:
    if not ENGINE_EXIT_OVERRIDES:
        return "none"
    parts = []
    for eng, val in ENGINE_EXIT_OVERRIDES.items():
        if not isinstance(val, dict):
            continue
        for side, cfg in val.items():
            if not isinstance(cfg, dict):
                continue
            tp = cfg.get("tp")
            sl = cfg.get("sl")
            parts.append(f"{eng}:{side} tp={_fmt_pct_safe(tp)} sl={_fmt_pct_safe(sl)}")
    return ", ".join(parts) if parts else "none"

def _get_engine_exit_thresholds(engine_label: Optional[str], side: str) -> tuple[float, float]:
    side_key = (side or "").upper()
    tp = AUTO_EXIT_SHORT_TP_PCT if side_key == "SHORT" else AUTO_EXIT_LONG_TP_PCT
    sl = AUTO_EXIT_SHORT_SL_PCT if side_key == "SHORT" else AUTO_EXIT_LONG_SL_PCT
    label_key = (engine_label or "").upper()
    overrides_map = globals().get("ENGINE_EXIT_OVERRIDES") or {}
    overrides = overrides_map.get(label_key) or overrides_map.get(label_key.lower())
    if isinstance(overrides, dict):
        side_override = overrides.get(side_key) or overrides.get(side_key.lower())
        if isinstance(side_override, dict):
            if isinstance(side_override.get("tp"), (int, float)):
                tp = float(side_override.get("tp"))
            if isinstance(side_override.get("sl"), (int, float)):
                sl = float(side_override.get("sl"))
    return float(tp), float(sl)


def _swaggy_no_atlas_overext_dist(df_ltf: pd.DataFrame, side: str, cfg) -> Optional[float]:
    if df_ltf.empty or len(df_ltf) < int(getattr(cfg, "overext_ema_len", 20)) + 2:
        return None
    ema_len = int(getattr(cfg, "overext_ema_len", 20))
    atr_len = int(getattr(cfg, "touch_atr_len", 14))
    ema_series = ema(df_ltf["close"], ema_len)
    if ema_series.empty:
        return None
    ema_val = float(ema_series.iloc[-1])
    last_price = float(df_ltf["close"].iloc[-1])
    atr_val = atr(df_ltf, atr_len)
    if atr_val <= 0:
        return None
    side = (side or "").upper()
    if side == "SHORT":
        return (ema_val - last_price) / atr_val
    return (last_price - ema_val) / atr_val


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
    _set_in_pos_side(st, side, False)
    suffix = "long" if side == "LONG" else "short"
    st.pop(f"manual_entry_alerted_{suffix}", None)
    st.pop(f"manual_qty_{suffix}", None)
    st.pop(f"manual_dca_adds_{suffix}", None)
    _clear_entry_alerted(state, symbol, side)
    state[symbol] = st
    _clear_dca_alerted(state, symbol, side)
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
        try:
            report_start_ts = calendar.timegm(datetime.strptime(report_date, "%Y-%m-%d").timetuple())
        except Exception:
            report_start_ts = None
        if dbrecon and executor_mod and hasattr(executor_mod, "exchange"):
            lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
            since_ts = time.time() - float(lookback)
            if isinstance(report_start_ts, (int, float)):
                since_ts = min(since_ts, float(report_start_ts))
            dbrecon.sync_income(executor_mod.exchange, since_ts=since_ts)
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
                if isinstance(report_start_ts, (int, float)):
                    since_ts = min(since_ts, float(report_start_ts))
                dbrecon.sync_exchange_state(executor_mod.exchange, since_ts=since_ts, symbols=norm_syms)
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        out_path = os.path.join("reports", "db_pnl_last3d.csv")
        daily_path = os.path.join("reports", "db_pnl_daily_last3d.csv")
        lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
        if isinstance(report_start_ts, (int, float)):
            lookback = max(lookback, time.time() - float(report_start_ts))
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
        range_start_ts = calendar.timegm(start_dt.timetuple())
        if dbrecon and executor_mod and hasattr(executor_mod, "exchange"):
            lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
            since_ts = time.time() - float(lookback)
            if isinstance(range_start_ts, (int, float)):
                since_ts = min(since_ts, float(range_start_ts))
            dbrecon.sync_income(executor_mod.exchange, since_ts=since_ts)
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
                if isinstance(range_start_ts, (int, float)):
                    since_ts = min(since_ts, float(range_start_ts))
                dbrecon.sync_exchange_state(executor_mod.exchange, since_ts=since_ts, symbols=norm_syms)
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        out_path = os.path.join("reports", "db_pnl_last3d.csv")
        daily_path = os.path.join("reports", "db_pnl_daily_last3d.csv")
        lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
        if isinstance(range_start_ts, (int, float)):
            lookback = max(lookback, time.time() - float(range_start_ts))
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
    short_line = _format_total(totals["SHORT"])
    long_line = _format_total(totals["LONG"])
    lines = [
        f"📊 일일 리포트 (KST, {report_label})",
        f"- <b>총 진입={entry_count} 총 청산={total_exits} 총 수익={total_pnl_str} 승률={total_win_rate:.1f}% 승={total_wins} 패={total_losses}</b>",
        f"- SHORT {short_line}",
        f"- LONG {long_line}",
        "",
        "🔴 SHORT",
    ]
    if not compact:
        lines.append("")
    lines.extend(engine_lines("SHORT"))
    lines.append("🔎 SHORT SYMBOLS")
    lines.append("<pre>")
    lines.extend(symbol_lines("SHORT"))
    lines.append("</pre>")
    lines.append("🟢 LONG")
    if not compact:
        lines.append("")
    lines.extend(engine_lines("LONG"))
    lines.append("🔎 LONG SYMBOLS")
    lines.append("<pre>")
    lines.extend(symbol_lines("LONG"))
    lines.append("</pre>")
    return "\n".join(lines)

def _count_unique_entries(entry_by_symbol: dict) -> int:
    seen = set()
    total = 0
    for (sym, side), entries in (entry_by_symbol or {}).items():
        if not entries:
            continue
        for rec in entries:
            order_id = rec.get("entry_order_id")
            if order_id:
                key = ("order", str(order_id))
            else:
                price = rec.get("entry_price")
                ts = rec.get("entry_ts")
                bucket = int(float(ts) // 1800) if isinstance(ts, (int, float)) else 0
                if isinstance(price, (int, float)):
                    key = ("price", sym, side, round(float(price), 8), bucket)
                else:
                    key = ("ts", sym, side, bucket)
            if key in seen:
                continue
            seen.add(key)
            total += 1
    return total

def _build_daily_report(state: Dict[str, dict], report_date: str, compact: bool = False) -> str:
    db_rows = _load_db_daily_rows(report_date)
    records = None if db_rows else _read_report_csv_records(report_date)
    try:
        report_start_ts = calendar.timegm(datetime.strptime(report_date, "%Y-%m-%d").timetuple())
    except Exception:
        report_start_ts = None
    report_end_ts = report_start_ts + 86400.0 if isinstance(report_start_ts, (int, float)) else None
    lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
    since_ts = (float(report_start_ts) - lookback) if isinstance(report_start_ts, (int, float)) else None
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
        _, entry_by_symbol = _load_entry_events_map(report_date, since_ts=report_start_ts, end_ts=report_end_ts)
    except Exception:
        entry_by_symbol = {}
    entry_by_symbol_all = {}
    try:
        _, entry_by_symbol_all = _load_entry_events_map(None, since_ts=since_ts, end_ts=report_end_ts)
    except Exception:
        entry_by_symbol_all = {}

    entry_engine_map: Dict[Tuple[str, str], Tuple[float, str]] = {}
    entry_event_index: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    entry_event_index: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    for (sym, side), records_by_sym in entry_by_symbol_all.items():
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

    entry_count = _count_unique_entries(entry_by_symbol) if entry_by_symbol else 0
    rows_out = symbol_trade_rows if use_trade_rows else symbol_rows
    return _format_report_output(totals, engine_totals, rows_out, report_date, compact, entry_count)

def _build_range_report(state: Dict[str, dict], start_date: str, end_date: str, compact: bool = False) -> str:
    db_rows = _load_db_daily_rows_range(start_date, end_date)
    if not db_rows:
        return _build_daily_report(state, start_date, compact=compact)
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except Exception:
        return _build_daily_report(state, start_date, compact=compact)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    range_start_ts = calendar.timegm(start_dt.timetuple())
    range_end_ts = calendar.timegm((end_dt + timedelta(days=1)).timetuple())
    lookback = max(86400.0, float(DB_REPORT_LOOKBACK_SEC or 0.0))
    since_ts = float(range_start_ts) - lookback
    entry_by_symbol: dict = {}
    entry_count = 0
    entry_engine_map: Dict[Tuple[str, str], Tuple[float, str]] = {}
    entry_event_index: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    entry_by_symbol_all = {}
    try:
        _, entry_by_symbol_all = _load_entry_events_map(None, since_ts=since_ts, end_ts=range_end_ts)
    except Exception:
        entry_by_symbol_all = {}
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
            _, entry_by_symbol = _load_entry_events_map(day, since_ts=range_start_ts, end_ts=range_end_ts)
            entry_count += _count_unique_entries(entry_by_symbol)
            cur += timedelta(days=1)
    except Exception:
        entry_count = 0

    for (sym, side), records_by_sym in entry_by_symbol_all.items():
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
        if _both_sides_open(st):
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
                    _set_in_pos_side(st, "LONG", True)
                    time.sleep(PER_SYMBOL_SLEEP)
                    continue
            if side == "SHORT":
                try:
                    existing_amt = get_short_position_amount(symbol)
                except Exception:
                    existing_amt = 0.0
                if existing_amt > 0:
                    _set_in_pos_side(st, "SHORT", True)
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
                _entry_lock_release(state, symbol, owner="dtfx", side=side)
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if _entry_seen_blocked(state, symbol, side, "dtfx"):
                _entry_guard_release(state, symbol, key=guard_key)
                _entry_lock_release(state, symbol, owner="dtfx", side=side)
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            try:
                if side == "LONG":
                    req_id = _enqueue_entry_request(
                        state,
                        symbol=symbol,
                        side="LONG",
                        engine="DTFX",
                        reason="dtfx_long",
                        usdt=_resolve_entry_usdt(),
                        live=LONG_LIVE_TRADING,
                        alert_reason=f"event={sig.pattern}",
                    )
                    if req_id:
                        result["long_hits"] += 1
                        active_positions_total += 1
                else:
                    req_id = _enqueue_entry_request(
                        state,
                        symbol=symbol,
                        side="SHORT",
                        engine="DTFX",
                        reason="dtfx_short",
                        usdt=_resolve_entry_usdt(),
                        live=LIVE_TRADING,
                        alert_reason=f"event={sig.pattern}",
                    )
                    if req_id:
                        result["short_hits"] += 1
                        active_positions_total += 1
            except Exception as e:
                print(f"[dtfx] queue error {symbol} side={side}: {e}")
            finally:
                _entry_guard_release(state, symbol, key=guard_key)
                _entry_lock_release(state, symbol, owner="dtfx", side=side)
        time.sleep(PER_SYMBOL_SLEEP)
    _set_thread_log_buffer(None)
    result["log"] = buf
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
        if _is_in_pos_side(st, "SHORT"):
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
                req_id = _enqueue_entry_request(
                    state,
                    symbol=symbol,
                    side="SHORT",
                    engine="ATLAS_RS_FAIL_SHORT",
                    reason="atlas_rs_fail_short",
                    usdt=entry_usdt,
                    live=LIVE_TRADING,
                    alert_reason="PULLBACK_FAIL",
                    entry_price_hint=entry_price,
                    size_mult=size_mult,
                )
                if req_id and isinstance(bucket, dict):
                    rec = bucket.get(symbol)
                    if isinstance(rec, dict):
                        rec["last_entry_ts"] = time.time()
                        bucket[symbol] = rec
            except Exception as e:
                print(f"[atlas-rs-fail-short] queue error {symbol}: {e}")
            finally:
                _entry_guard_release(state, symbol, key=guard_key)
        finally:
            _entry_lock_release(state, symbol, owner="atlas_rs_fail_short", side="SHORT")
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

def _load_db_entry_events_map(
    report_date: Optional[str] = None,
    since_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
    include_alerts: bool = False,
    include_engine_signals: bool = False,
) -> tuple[dict, dict]:
    by_id: dict = {}
    by_symbol_side: dict = {}
    if not dbrec or not dbrec.ENABLED:
        return by_id, by_symbol_side
    try:
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        if include_alerts:
            clauses = ["event_type IN ('ENTRY','ALERT_ENTRY')"]
        else:
            clauses = ["event_type = 'ENTRY'"]
        params: list = []
        if report_date:
            clauses.append("date(datetime(ts, 'unixepoch')) = ?")
            params.append(report_date)
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(float(since_ts))
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(float(end_ts))
        where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = cur.execute(
            f"""
            SELECT ts, symbol, side, source, avg_entry, meta_json
            FROM events
            {where_sql}
            """,
            tuple(params),
        ).fetchall()
        conn.close()
        for row in rows:
            try:
                ts_val = float(row[0]) if row[0] is not None else None
            except Exception:
                ts_val = None
            if ts_val is None:
                continue
            symbol = row[1] or ""
            side = (row[2] or "").upper()
            if not symbol or side not in ("LONG", "SHORT"):
                continue
            engine = row[3] or "unknown"
            entry_price = None
            try:
                entry_price = float(row[4]) if row[4] is not None else None
            except Exception:
                entry_price = None
            meta = None
            raw_meta = row[5]
            if raw_meta:
                try:
                    meta = json.loads(raw_meta)
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                meta_engine = meta.get("engine") or meta.get("source_engine")
                if meta_engine:
                    engine = meta_engine
                meta_entry_price = meta.get("entry_price") or meta.get("avg_entry")
                if entry_price is None and isinstance(meta_entry_price, (int, float)):
                    entry_price = float(meta_entry_price)
                entry_order_id = meta.get("entry_order_id")
            else:
                entry_order_id = None
            record = {
                "entry_ts": ts_val,
                "entry_order_id": entry_order_id,
                "symbol": symbol,
                "side": side,
                "engine": engine,
                "entry_price": entry_price,
                "record_type": "event",
                "event_type": "ENTRY",
                "source": row[3],
                "meta": meta if isinstance(meta, dict) else None,
            }
            if entry_order_id:
                by_id[str(entry_order_id)] = record
            key = (symbol, side)
            by_symbol_side.setdefault(key, []).append(record)
        if include_engine_signals:
            try:
                clauses = []
                params = []
                if report_date:
                    clauses.append("date(datetime(ts, 'unixepoch')) = ?")
                    params.append(report_date)
                if since_ts is not None:
                    clauses.append("ts >= ?")
                    params.append(float(since_ts))
                if end_ts is not None:
                    clauses.append("ts <= ?")
                    params.append(float(end_ts))
                where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
                sig_rows = cur.execute(
                    f"""
                    SELECT ts, symbol, side, engine, reason
                    FROM engine_signals
                    {where_sql}
                    """,
                    tuple(params),
                ).fetchall()
                for row in sig_rows:
                    try:
                        ts_val = float(row[0]) if row[0] is not None else None
                    except Exception:
                        ts_val = None
                    if ts_val is None:
                        continue
                    symbol = row[1] or ""
                    side = (row[2] or "").upper()
                    if not symbol or side not in ("LONG", "SHORT"):
                        continue
                    engine = row[3] or "unknown"
                    record = {
                        "entry_ts": ts_val,
                        "entry_order_id": None,
                        "symbol": symbol,
                        "side": side,
                        "engine": engine,
                        "entry_price": None,
                        "record_type": "engine_signal",
                        "event_type": None,
                        "source": row[4],
                        "meta": None,
                    }
                    key = (symbol, side)
                    by_symbol_side.setdefault(key, []).append(record)
            except Exception:
                pass
        return by_id, by_symbol_side
    except Exception:
        return by_id, by_symbol_side

def _load_entry_events_map(
    report_date: Optional[str] = None,
    since_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
    include_alerts: bool = False,
    include_engine_signals: bool = False,
) -> tuple[dict, dict]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if dbrec and dbrec.ENABLED:
        by_id, by_symbol_side = _load_db_entry_events_map(
            report_date=report_date,
            since_ts=since_ts,
            end_ts=end_ts,
            include_alerts=include_alerts,
            include_engine_signals=include_engine_signals,
        )
        if by_id or by_symbol_side:
            return by_id, by_symbol_side
    by_id = {}
    by_symbol_side = {}
    paths: list[str] = []
    entry_dir = os.path.join(base_dir, "logs", "entry")
    if os.path.isdir(entry_dir):
        try:
            for name in sorted(os.listdir(entry_dir)):
                if name.startswith("entry_events-") and name.endswith(".log"):
                    paths.append(os.path.join(entry_dir, name))
        except Exception:
            pass
    legacy_path = os.path.join(base_dir, "logs", "entry_events.log")
    if os.path.exists(legacy_path):
        paths.append(legacy_path)
    if report_date:
        dated_path = os.path.join(base_dir, "logs", "entry", f"entry_events-{report_date}.log")
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
                        "entry_price": payload.get("entry_price"),
                    }
                    if entry_id:
                        by_id[str(entry_id)] = record
                    if report_date is None or _report_day_str(entry_ts_val) == report_date:
                        key = (symbol, side)
                        by_symbol_side.setdefault(key, []).append(record)
    except Exception:
        return by_id, by_symbol_side
    return by_id, by_symbol_side

def _load_swaggy_trade_engine_map(needed: set) -> dict:
    if not needed:
        return {}
    out = {}
    path = os.path.join("logs", "swaggy_trades.jsonl")
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if rec.get("event") != "SWAGGY_TRADE":
                    continue
                sym = rec.get("symbol")
                side = (rec.get("side") or "").upper()
                if not sym or side not in ("LONG", "SHORT"):
                    continue
                key = (sym, side)
                if key not in needed:
                    continue
                ts_val = None
                ts = rec.get("entry_ts") or rec.get("ts")
                if isinstance(ts, (int, float)):
                    ts_val = float(ts)
                elif isinstance(ts, str):
                    try:
                        ts_val = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        ts_val = None
                if ts_val is None:
                    continue
                cur = out.get(key)
                if not cur or ts_val > float(cur.get("ts") or 0.0):
                    out[key] = {"engine": rec.get("engine"), "ts": ts_val}
    except Exception:
        return out
    return out

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
        if LOG_LONG_EXIT:
            print(f"[long-exit] {symbol} position closed detected, canceling open orders")
        meta = tr.get("meta") or {}
        try:
            cancel_conditional_by_side(symbol, "LONG")
        except Exception as e:
            if LOG_LONG_EXIT:
                print(f"[long-exit] {symbol} cancel_conditional_by_side failed: {e}")
        try:
            cancel_stop_orders(symbol)
            cancel_open_orders(symbol)
        except Exception as e:
            if LOG_LONG_EXIT:
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
            entry_time = _fmt_entry_time(tr)
            entry_line = f"진입시간={entry_time}\n" if entry_time else ""
            send_telegram(
                f"{EXIT_ICON} <b>롱 청산</b>\n"
                f"<b>{symbol}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"{entry_line}"
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
                if isinstance(st, dict):
                    _set_in_pos_side(st, "SHORT", True)
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
            if isinstance(st, dict):
                _set_in_pos_side(st, "SHORT", True)
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
            entry_time = _fmt_entry_time(tr)
            entry_line = f"진입시간={entry_time}\n" if entry_time else ""
            icon = EXIT_SL_ICON if exit_tag == "SL" else EXIT_ICON
            send_telegram(
                f"{icon} <b>숏 청산</b>\n"
                f"<b>{symbol}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{entry_line}"
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

def _process_manage_queue(state: dict, send_telegram) -> None:
    if not MANAGE_WS_MODE and isinstance(state, dict) and state.get("_manage_ws_mode"):
        return
    offset = state.get("_manage_queue_offset", 0)
    try:
        offset = int(offset or 0)
    except Exception:
        offset = 0
    offset, reqs = manage_queue.read_requests_from_offset(offset)
    state["_manage_queue_offset"] = offset
    if not reqs:
        return
    status = state.setdefault("_manage_queue_status", {})
    now = time.time()
    for req in reqs:
        if not isinstance(req, dict):
            continue
        if req.get("type") != "entry":
            continue
        engine = str(req.get("engine") or "").upper()
        allowed_engines = {
            "SWAGGY_ATLAS_LAB",
            "SWAGGY_ATLAS_LAB_V2",
            "SWAGGY_NO_ATLAS",
            "ATLAS_RS_FAIL_SHORT",
            "DTFX",
            "RSI",
            "MANUAL",
            "UNKNOWN",
        }
        if engine and engine not in allowed_engines:
            status_key = str(req.get("id") or "").strip() or f"skip:{engine}"
            status[status_key] = {"status": "failed", "ts": now, "reason": "engine_removed"}
            print(f"[manage-queue] skip engine={engine} symbol={req.get('symbol')}")
            continue
        req_id = str(req.get("id") or "").strip()
        if not req_id:
            continue
        st = status.get(req_id)
        if isinstance(st, dict) and st.get("status") in ("done", "executing"):
            continue
        status[req_id] = {"status": "executing", "ts": now}
        symbol = str(req.get("symbol") or "")
        side = str(req.get("side") or "").upper()
        if engine in ("SWAGGY_NO_ATLAS", "SWAGGY_ATLAS_LAB", "SWAGGY_ATLAS_LAB_V2"):
            if not SATURDAY_TRADE_ENABLED and _is_saturday_kst(now):
                _append_entry_gate_log(
                    engine.lower() if engine else "unknown",
                    symbol,
                    "off_window=saturday",
                    side=side,
                )
                status[req_id] = {"status": "failed", "ts": time.time(), "reason": "off_window_saturday"}
                continue
            if engine == "SWAGGY_NO_ATLAS" and _is_in_off_window(SWAGGY_NO_ATLAS_OFF_WINDOWS, now):
                _append_entry_gate_log(
                    engine.lower() if engine else "unknown",
                    symbol,
                    f"off_window={SWAGGY_NO_ATLAS_OFF_WINDOWS}",
                    side=side,
                )
                status[req_id] = {"status": "failed", "ts": time.time(), "reason": "off_window"}
                continue
            if engine == "SWAGGY_ATLAS_LAB" and _is_in_off_window(SWAGGY_ATLAS_LAB_OFF_WINDOWS, now):
                _append_entry_gate_log(
                    engine.lower() if engine else "unknown",
                    symbol,
                    f"off_window={SWAGGY_ATLAS_LAB_OFF_WINDOWS}",
                    side=side,
                )
                status[req_id] = {"status": "failed", "ts": time.time(), "reason": "off_window"}
                continue
            if engine == "SWAGGY_ATLAS_LAB_V2" and _is_in_off_window(SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, now):
                _append_entry_gate_log(
                    engine.lower() if engine else "unknown",
                    symbol,
                    f"off_window={SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS}",
                    side=side,
                )
                status[req_id] = {"status": "failed", "ts": time.time(), "reason": "off_window"}
                continue
        ok = _execute_manage_entry_request(state, req, send_telegram)
        status[req_id] = {"status": "done" if ok else "failed", "ts": time.time()}

def _record_position_event(
    symbol: str,
    side: str,
    event_type: str,
    source: str,
    qty: Optional[float],
    avg_entry: Optional[float],
    price: Optional[float],
    meta: Optional[dict],
) -> None:
    if not dbrec or not dbrec.ENABLED:
        return
    try:
        dbrec.record_event(
            symbol=symbol,
            side=side,
            event_type=event_type,
            source=source,
            qty=qty,
            avg_entry=avg_entry,
            price=price,
            meta=meta,
            ts=time.time(),
        )
    except Exception:
        pass

def _detect_position_events(state: dict, send_telegram) -> None:
    try:
        pos_syms = list_open_position_symbols(force=True)
    except Exception:
        return
    def _recent_entry_event(symbol: str, side: str, now_ts: float, window_sec: float = 180.0) -> Optional[dict]:
        report_date = _report_day_str(now_ts)
        _by_id, by_symbol = _load_entry_events_map(report_date)
        recs = by_symbol.get((symbol, side)) or []
        if not recs:
            return None
        recs = sorted(recs, key=lambda r: float(r.get("entry_ts") or 0.0), reverse=True)
        for rec in recs:
            ts_val = rec.get("entry_ts")
            if not isinstance(ts_val, (int, float)):
                continue
            if (now_ts - float(ts_val)) > window_sec:
                continue
            engine = str(rec.get("engine") or "").upper()
            if engine and engine not in ("UNKNOWN", "MANUAL"):
                return rec
        return None
    snap = state.setdefault("_pos_snapshot", {})
    now = time.time()
    def _handle(symbol: str, side: str, detail: Optional[dict]) -> None:
        open_tr = _get_open_trade(state, side, symbol)
        managed = isinstance(open_tr, dict)
        managed_engine = (open_tr.get("meta") or {}).get("engine") if managed else None
        changed = False
        key = f"{symbol}|{side}"
        prev = snap.get(key) if isinstance(snap, dict) else None
        prev_qty = prev.get("qty") if isinstance(prev, dict) else None
        prev_entry = prev.get("entry") if isinstance(prev, dict) else None
        qty = None
        avg_entry = None
        mark = None
        if isinstance(detail, dict):
            qty = detail.get("qty") or detail.get("amount")
            avg_entry = detail.get("entry")
            mark = detail.get("mark")
        try:
            qty = float(qty) if qty is not None else None
        except Exception:
            qty = None
        if side == "SHORT" and isinstance(qty, (int, float)):
            qty = abs(qty)
        try:
            avg_entry = float(avg_entry) if avg_entry is not None else None
        except Exception:
            avg_entry = None
        if isinstance(qty, (int, float)) and qty <= 0:
            qty = None
        if prev_qty is None and qty is not None:
            if not managed:
                recent = _recent_entry_event(symbol, side, now)
                if isinstance(recent, dict):
                    engine_label = str(recent.get("engine") or "").upper()
                    reason = _reason_from_engine_label(engine_label, side)
                    entry_price = avg_entry if isinstance(avg_entry, (int, float)) else recent.get("entry_price")
                    _log_trade_entry(
                        state,
                        side=side,
                        symbol=symbol,
                        entry_ts=float(recent.get("entry_ts") or now),
                        entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=None,
                        entry_order_id=recent.get("entry_order_id"),
                        meta={"reason": reason, "engine": engine_label} if reason else {"engine": engine_label},
                    )
                    return
                backfilled = _get_open_trade_or_backfill(state, symbol, side, now_ts=now)
                if isinstance(backfilled, dict):
                    return
                _record_position_event(symbol, side, "ENTRY", "MANUAL", qty, avg_entry, mark, {"source": "pos_snapshot"})
                changed = True
                _send_entry_alert(
                    send_telegram,
                    side=side,
                    symbol=symbol,
                    engine="MANUAL",
                    entry_price=avg_entry,
                    usdt=None,
                    reason="manual_entry",
                    live=True,
                    order_info="(pos-snap)",
                    entry_order_id=None,
                    sl=_fmt_price_safe(
                        avg_entry,
                        AUTO_EXIT_SHORT_SL_PCT if side == "SHORT" else AUTO_EXIT_LONG_SL_PCT,
                        side=side,
                    ),
                    tp=None,
                    state=state,
                )
        elif prev_qty is not None and qty is not None:
            if isinstance(prev_qty, (int, float)) and isinstance(qty, (int, float)) and qty > prev_qty * 1.0001:
                if not managed:
                    _record_position_event(symbol, side, "DCA", "MANUAL", qty, avg_entry, mark, {"source": "pos_snapshot"})
                    changed = True
                    label = "DCA" if DCA_ENABLED else "ADD"
                    send_telegram(
                        f"➕ <b>{label}</b> {symbol} {side} adds mark={mark} entry={avg_entry} qty={qty}"
                    )
        elif prev_qty is not None and qty is None:
            source = managed_engine or ("AUTO" if managed else "MANUAL")
            cancel_stop_orders(symbol)
            _record_position_event(symbol, side, "EXIT", source, prev_qty, prev_entry, mark, {"source": "pos_snapshot"})
            changed = True
        if isinstance(snap, dict):
            if qty is not None:
                snap[key] = {"qty": qty, "entry": avg_entry, "ts": now}
            else:
                snap.pop(key, None)
        if changed:
            try:
                save_state(state)
            except Exception:
                pass
    for sym in pos_syms.get("short") or []:
        detail = None
        try:
            detail = get_short_position_detail(sym)
        except Exception:
            detail = None
        _handle(sym, "SHORT", detail)
    for sym in pos_syms.get("long") or []:
        detail = None
        try:
            detail = get_long_position_detail(sym)
        except Exception:
            detail = None
        _handle(sym, "LONG", detail)

def _detect_manual_positions(state: dict, send_telegram) -> None:
    try:
        pos_syms = list_open_position_symbols(force=True)
    except Exception:
        return
    for side_key, side_label in (("short", "SHORT"), ("long", "LONG")):
        syms = pos_syms.get(side_key) or set()
        for sym in syms:
            if _get_open_trade(state, side_label, sym):
                continue
            detail = get_short_position_detail(sym) if side_label == "SHORT" else get_long_position_detail(sym)
            if not isinstance(detail, dict):
                continue
            qty = detail.get("qty") or detail.get("amount")
            entry = detail.get("entry")
            mark = detail.get("mark")
            try:
                qty = float(qty)
            except Exception:
                qty = None
            if side_label == "SHORT" and isinstance(qty, (int, float)):
                qty = abs(qty)
            st = state.get(sym) if isinstance(state.get(sym), dict) else {}
            if not isinstance(st, dict):
                st = {}
            prev_qty = st.get(f"manual_qty_{side_key}")
            info = _manual_alert_info(state, sym, side_label)
            if info:
                prev_entry = info.get("entry")
                prev_qty = info.get("qty")
                prev_ts = info.get("ts")
                same_entry = False
                if isinstance(prev_entry, (int, float)) and isinstance(entry, (int, float)) and entry > 0:
                    same_entry = abs(float(prev_entry) - float(entry)) / float(entry) <= 0.001
                elif prev_entry is None or entry is None:
                    same_entry = True
                same_qty = False
                if isinstance(prev_qty, (int, float)) and isinstance(qty, (int, float)) and qty > 0:
                    same_qty = abs(float(prev_qty) - float(qty)) / float(qty) <= 0.01
                elif prev_qty is None or qty is None:
                    same_qty = True
                ttl_ok = False
                if isinstance(prev_ts, (int, float)):
                    ttl_ok = (time.time() - float(prev_ts)) <= MANUAL_ALERT_TTL_SEC
                if same_entry and same_qty and ttl_ok:
                    _set_in_pos_side(st, side_label, True)
                    st[f"manual_qty_{side_key}"] = qty
                    st.setdefault(f"manual_dca_adds_{side_key}", 0)
                    state[sym] = st
                    continue
            _log_trade_entry(
                state,
                side=side_label,
                symbol=sym,
                entry_ts=time.time(),
                entry_price=entry if isinstance(entry, (int, float)) else None,
                qty=qty if isinstance(qty, (int, float)) else None,
                usdt=None,
                entry_order_id=None,
                meta={"reason": "manual_entry"},
            )
            _send_entry_alert(
                send_telegram,
                side=side_label,
                symbol=sym,
                engine="MANUAL",
                entry_price=entry if isinstance(entry, (int, float)) else None,
                usdt=None,
                reason="manual_entry",
                live=True,
                order_info="(manual)",
                entry_order_id=None,
                sl=_fmt_price_safe(entry, AUTO_EXIT_SHORT_SL_PCT if side_label == "SHORT" else AUTO_EXIT_LONG_SL_PCT, side=side_label),
                tp=None,
                state=state,
            )
            _mark_manual_alerted(state, sym, side_label, entry_price=entry, qty=qty)
            try:
                save_state(state)
            except Exception:
                pass
            _set_in_pos_side(st, side_label, True)
            st[f"manual_qty_{side_key}"] = qty
            st[f"manual_dca_adds_{side_key}"] = 0
            _set_last_entry_state(st, side_label, time.time())
            state[sym] = st
            continue
            if isinstance(qty, (int, float)) and isinstance(prev_qty, (int, float)) and qty > prev_qty * 1.0001:
                adds_key = f"manual_dca_adds_{side_key}"
                adds_done = int(st.get(adds_key, 0) or 0)
                st[adds_key] = adds_done + 1
                st[f"manual_qty_{side_key}"] = qty
                state[sym] = st
                send_telegram(
                    f"➕ <b>DCA</b> {sym} adds {adds_done}->{adds_done+1} "
                    f"mark={mark} entry={entry} qty={qty}"
                )
                try:
                    save_state(state)
                except Exception:
                    pass

def _execute_manage_entry_request(state: dict, req: dict, send_telegram) -> bool:
    symbol = req.get("symbol")
    side = str(req.get("side") or "").upper()
    if not symbol or side not in ("LONG", "SHORT"):
        return False
    engine = str(req.get("engine") or "").upper()
    if engine and not _is_engine_enabled(engine):
        _clear_manage_pending(state, symbol, side)
        return False
    try:
        refresh_positions_cache(force=True)
    except Exception:
        pass
    cur_total = None
    try:
        cur_total = count_open_positions(force=True)
    except Exception:
        cur_total = None
    if not isinstance(cur_total, int):
        cur_total = _count_open_positions_state(state)
    alert_tag = req.get("alert_tag")
    reason = str(req.get("reason") or "").lower()
    is_multi_req = False
    if isinstance(alert_tag, str) and alert_tag.startswith("MULTI_ENTRY"):
        is_multi_req = True
    if "multi" in reason:
        is_multi_req = True
    if engine == "SWAGGY_NO_ATLAS" or is_multi_req:
        _append_swaggy_no_atlas_log(
            f"SWAGGY_NO_ATLAS_MANAGE_REQ sym={symbol} side={side} "
            f"multi={int(is_multi_req)} reason={req.get('reason')} tag={alert_tag or ''}"
        )
    if isinstance(cur_total, int) and cur_total >= MAX_OPEN_POSITIONS and not is_multi_req:
        _clear_manage_pending(state, symbol, side)
        _append_entry_gate_log(engine.lower() if engine else "unknown", symbol, f"pos_limit={cur_total}/{MAX_OPEN_POSITIONS}", side=side)
        return False
    try:
        if side == "SHORT" and get_short_position_amount(symbol) > 0 and not is_multi_req:
            _clear_manage_pending(state, symbol, side)
            _append_entry_gate_log(engine.lower() if engine else "unknown", symbol, "already_in_position", side=side)
            if engine == "SWAGGY_NO_ATLAS":
                _append_swaggy_no_atlas_log(
                    f"SWAGGY_NO_ATLAS_MANAGE_SKIP sym={symbol} side={side} reason=ALREADY_IN_POSITION"
                )
            return True
        if side == "LONG" and get_long_position_amount(symbol) > 0 and not is_multi_req:
            _clear_manage_pending(state, symbol, side)
            _append_entry_gate_log(engine.lower() if engine else "unknown", symbol, "already_in_position", side=side)
            if engine == "SWAGGY_NO_ATLAS":
                _append_swaggy_no_atlas_log(
                    f"SWAGGY_NO_ATLAS_MANAGE_SKIP sym={symbol} side={side} reason=ALREADY_IN_POSITION"
                )
            return True
    except Exception:
        pass
    live = bool(req.get("live"))
    try:
        set_dry_run(False if live else True)
    except Exception:
        pass
    usdt = float(req.get("usdt") or 0.0)
    res = {}
    try:
        if side == "SHORT":
            res = short_market(symbol, usdt_amount=usdt, leverage=LEVERAGE, margin_mode=MARGIN_MODE)
        else:
            res = long_market(symbol, usdt_amount=usdt, leverage=LEVERAGE, margin_mode=MARGIN_MODE)
    except Exception as e:
        print(f"[manage-queue] execute failed sym={symbol} side={side} err={e}")
        _clear_manage_pending(state, symbol, side)
        return False
    entry_order_id = _order_id_from_res(res)
    fill_price = res.get("last") or (res.get("order") or {}).get("average") or (res.get("order") or {}).get("price")
    qty = res.get("amount") or (res.get("order") or {}).get("amount")
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    if not isinstance(st, dict):
        st = {}
    _set_in_pos_side(st, side, True)
    st.setdefault("dca_adds", 0)
    st.setdefault("dca_adds_long", 0)
    st.setdefault("dca_adds_short", 0)
    _set_last_entry_state(st, side, time.time())
    state[symbol] = st
    _log_trade_entry(
        state,
        side=side,
        symbol=symbol,
        entry_ts=time.time(),
        entry_price=fill_price if isinstance(fill_price, (int, float)) else None,
        qty=qty if isinstance(qty, (int, float)) else None,
        usdt=usdt,
        entry_order_id=entry_order_id,
        meta={"reason": req.get("reason"), "engine": req.get("engine")},
    )
    _record_position_event(
        symbol,
        side,
        "ENTRY",
        str(req.get("engine") or "AUTO"),
        qty if isinstance(qty, (int, float)) else None,
        fill_price if isinstance(fill_price, (int, float)) else req.get("entry_price_hint"),
        fill_price if isinstance(fill_price, (int, float)) else req.get("entry_price_hint"),
        {"source": "manage_queue", "reason": req.get("reason")},
    )
    try:
        _sync_trade_log_from_db(state, symbol, side)
    except Exception:
        pass
    try:
        _entry_seen_mark(state, symbol, side, str(req.get("engine") or "unknown"))
    except Exception:
        pass
    _send_entry_alert(
        (lambda text: send_telegram(text, allow_early=True)),
        side=side,
        symbol=symbol,
        engine=req.get("engine") or "UNKNOWN",
        entry_price=fill_price if isinstance(fill_price, (int, float)) else req.get("entry_price_hint"),
        usdt=usdt,
        reason=req.get("alert_reason") or req.get("reason") or "",
        live=live,
        order_info=None,
        entry_order_id=entry_order_id,
        label_tag=str(alert_tag) if isinstance(alert_tag, str) and alert_tag.startswith("MULTI_ENTRY") else None,
        sl=_fmt_price_safe(
            fill_price if isinstance(fill_price, (int, float)) else None,
            _get_engine_exit_thresholds(_engine_label_from_reason(req.get("reason")), side)[1],
            side=side,
        ),
        tp=None,
        state=state,
    )
    _clear_manage_pending(state, symbol, side)
    return True


def _adv_place_be_stop_all(symbol: str, side: str, entry_px: float) -> None:
    cancel_stop_orders(symbol)
    if side == "LONG":
        detail = get_long_position_detail(symbol) or {}
        be_px = detail.get("entry") if isinstance(detail.get("entry"), (int, float)) else entry_px
        _EXEC_PLACE_LONG_SL_PX(symbol, be_px)
        for acct in FOLLOWER_CONTEXTS:
            try:
                detail_f = acct.executor.get_long_position_detail(symbol) or {}
                be_px_f = detail_f.get("entry") if isinstance(detail_f.get("entry"), (int, float)) else be_px
                acct.executor.place_long_sl_px(symbol, be_px_f)
            except Exception:
                continue
    else:
        detail = get_short_position_detail(symbol) or {}
        be_px = detail.get("entry") if isinstance(detail.get("entry"), (int, float)) else entry_px
        _EXEC_PLACE_SHORT_SL_PX(symbol, be_px)
        for acct in FOLLOWER_CONTEXTS:
            try:
                detail_f = acct.executor.get_short_position_detail(symbol) or {}
                be_px_f = detail_f.get("entry") if isinstance(detail_f.get("entry"), (int, float)) else be_px
                acct.executor.place_short_sl_px(symbol, be_px_f)
            except Exception:
                continue


def _adv_partial_close(symbol: str, side: str, fraction: float) -> None:
    try:
        fraction = float(fraction)
    except Exception:
        return
    if fraction <= 0:
        return
    if side == "LONG":
        amt = get_long_position_amount(symbol)
        if isinstance(amt, (int, float)) and amt > 0:
            _EXEC_CLOSE_LONG_MARKET_QTY(symbol, amt * fraction)
        follower_calls = []
        for acct in FOLLOWER_CONTEXTS:
            try:
                amt_f = acct.executor.get_long_position_amount(symbol)
            except Exception:
                amt_f = None
            if not isinstance(amt_f, (int, float)) or amt_f <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
            follower_calls.append({
                "acct": acct,
                "fn": lambda a=acct, q=amt_f * fraction: a.executor.close_long_market_qty(symbol, q),
            })
        _broadcast_followers("close_long_market_qty", follower_calls, {"symbol": symbol})
    else:
        amt = get_short_position_amount(symbol)
        if isinstance(amt, (int, float)) and amt > 0:
            _EXEC_CLOSE_SHORT_MARKET_QTY(symbol, amt * fraction)
        follower_calls = []
        for acct in FOLLOWER_CONTEXTS:
            try:
                amt_f = acct.executor.get_short_position_amount(symbol)
            except Exception:
                amt_f = None
            if not isinstance(amt_f, (int, float)) or amt_f <= 0:
                follower_calls.append({"acct": acct, "skip": "no_pos"})
                continue
            follower_calls.append({
                "acct": acct,
                "fn": lambda a=acct, q=amt_f * fraction: a.executor.close_short_market_qty(symbol, q),
            })
        _broadcast_followers("close_short_market_qty", follower_calls, {"symbol": symbol})


def _adv_latest_supertrend(symbol: str) -> Optional[tuple]:
    df = cycle_cache.get_df(symbol, "15m", limit=max(ADV_TREND_EMA_LEN, 200))
    if df.empty or len(df) < ADV_TREND_SUPER_ATR_LEN + 5:
        return None
    try:
        st_line, st_trend = _adv_supertrend(df, ADV_TREND_SUPER_ATR_LEN, ADV_TREND_SUPER_MULT)
        last_ts = int(df["ts"].iloc[-1])
        return float(st_line.iloc[-1]), int(st_trend.iloc[-1]), last_ts
    except Exception:
        return None


def _manage_adv_trend_positions(state: dict, send_telegram) -> None:
    open_trades = [
        tr
        for tr in (_get_trade_log(state) or [])
        if tr.get("status") == "open"
        and _engine_label_from_reason((tr.get("meta") or {}).get("reason")) == "ADVANCED_TREND_FOLLOWER"
    ]
    if not open_trades:
        return
    now = time.time()
    for tr in open_trades:
        symbol = tr.get("symbol")
        side = tr.get("side")
        if not symbol or side not in ("LONG", "SHORT"):
            continue
        meta = tr.get("meta") if isinstance(tr.get("meta"), dict) else {}
        entry_px = tr.get("entry_price")
        tp1_px = meta.get("tp1_price")
        be_done = bool(meta.get("be_done"))
        amt = None
        detail = None
        if side == "LONG":
            try:
                detail = get_long_position_detail(symbol)
                amt = detail.get("qty") if isinstance(detail, dict) else None
            except Exception:
                detail = None
            if not isinstance(amt, (int, float)):
                try:
                    amt = get_long_position_amount(symbol)
                except Exception:
                    amt = None
        else:
            try:
                detail = get_short_position_detail(symbol)
                amt = detail.get("qty") if isinstance(detail, dict) else None
            except Exception:
                detail = None
            if not isinstance(amt, (int, float)):
                try:
                    amt = get_short_position_amount(symbol)
                except Exception:
                    amt = None
        if not isinstance(amt, (int, float)) or amt <= 0:
            cancel_stop_orders(symbol)
            engine_label = "ADVANCED_TREND_FOLLOWER"
            _close_trade(
                state,
                side=side,
                symbol=symbol,
                exit_ts=now,
                exit_price=None,
                pnl_usdt=None,
                reason="auto_exit_sl",
            )
            _append_report_line(symbol, side, None, None, engine_label)
            icon = EXIT_SL_ICON
            send_telegram(
                f"{icon} <b>{'롱' if side == 'LONG' else '숏'} 청산</b>\n"
                f"<b>{symbol}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {_display_engine_label(engine_label)}"
            )
            continue

        mark_px = None
        if isinstance(detail, dict):
            mark_px = detail.get("mark")
        if not isinstance(mark_px, (int, float)):
            mark_px = None

        if not isinstance(tp1_px, (int, float)) and isinstance(entry_px, (int, float)):
            st_info = _adv_latest_supertrend(symbol)
            if st_info:
                st_px = float(st_info[0])
                if side == "LONG":
                    tp1_px = entry_px + (ADV_TREND_TP1_R_MULT * (entry_px - st_px))
                else:
                    tp1_px = entry_px - (ADV_TREND_TP1_R_MULT * (st_px - entry_px))
                meta["tp1_price"] = tp1_px
                meta.setdefault("sl_price", st_px)
                tr["meta"] = meta

        if not be_done and isinstance(tp1_px, (int, float)) and isinstance(mark_px, (int, float)):
            tp1_hit = (mark_px >= tp1_px) if side == "LONG" else (mark_px <= tp1_px)
            if tp1_hit:
                if isinstance(entry_px, (int, float)):
                    _adv_place_be_stop_all(symbol, side, entry_px)
                meta["be_done"] = True
                meta["be_ts"] = now
                tr["meta"] = meta
                _append_adv_trend_log(f"ADV_TREND_BE_MOVE sym={symbol} side={side} px={tp1_px:.6g}")

        st_info = _adv_latest_supertrend(symbol)
        if st_info:
            _st_px, trend_dir, last_ts = st_info
            st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
            last_seen = st.get("adv_trend_last_st_ts")
            if last_seen != last_ts:
                st["adv_trend_last_st_ts"] = last_ts
                state[symbol] = st
                if (side == "LONG" and trend_dir == -1) or (side == "SHORT" and trend_dir == 1):
                    if side == "LONG":
                        res = close_long_market(symbol)
                    else:
                        res = close_short_market(symbol)
                    exit_order_id = _order_id_from_res(res)
                    cancel_stop_orders(symbol)
                    cancel_open_orders(symbol)
                    engine_label = "ADVANCED_TREND_FOLLOWER"
                    _close_trade(
                        state,
                        side=side,
                        symbol=symbol,
                        exit_ts=now,
                        exit_price=mark_px,
                        pnl_usdt=detail.get("pnl") if isinstance(detail, dict) else None,
                        reason="tp2_supertrend",
                        exit_order_id=exit_order_id,
                    )
                    _append_report_line(symbol, side, None, detail.get("pnl") if isinstance(detail, dict) else None, engine_label)
                    send_telegram(
                        f"{EXIT_ICON} <b>{'롱' if side == 'LONG' else '숏'} 청산</b>\n"
                        f"<b>{symbol}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {_display_engine_label(engine_label)}"
                    )
                    continue


def _run_manage_cycle(state: dict, exchange, cached_long_ex, send_telegram) -> None:
    now = time.time()
    if DB_RECONCILE_ENABLED and dbrecon and dbrec and dbrec.ENABLED:
        last_recon = _coerce_state_float(state.get("_db_recon_ts", 0.0))
        if not isinstance(last_recon, (int, float)):
            last_recon = 0.0
            state["_db_recon_ts"] = 0.0
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

    _process_manage_queue(state, send_telegram)
    _detect_position_events(state, send_telegram)
    _manage_adv_trend_positions(state, send_telegram)

    exit_force_refreshed = False
    for sym, st in list(state.items()):
        if not isinstance(st, dict) or not _symbol_in_pos_any(st):
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
                entry_time = _fmt_entry_time(open_tr)
                entry_line = f"진입시간={entry_time}\n" if entry_time else ""
                icon = EXIT_SL_ICON if exit_tag == "SL" else EXIT_ICON
                send_telegram(
                    f"{icon} <b>숏 청산</b>\n"
                    f"<b>{sym}</b>\n"
                    f"엔진: {_display_engine_label(engine_label)}\n"
                    f"사유: {exit_tag}\n"
                    f"{entry_line}".rstrip()
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

        open_tr = _get_open_trade(state, "SHORT", sym)
        engine_label = _engine_label_from_reason(
            (open_tr.get("meta") or {}).get("reason") if open_tr else None
        )
        if engine_label == "ADVANCED_TREND_FOLLOWER":
            continue

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
                if open_tr is None:
                    open_tr = _get_open_trade(state, "SHORT", sym)
                if engine_label is None:
                    engine_label = _engine_label_from_reason(
                        (open_tr.get("meta") or {}).get("reason") if open_tr else None
                    )
                tp_pct, sl_pct = _get_engine_exit_thresholds(engine_label, "SHORT")
                if profit_unlev is not None and profit_unlev >= tp_pct:
                    pnl_usdt = pos_detail.get("pnl") if isinstance(pos_detail, dict) else None
                    try:
                        set_dry_run(False if LIVE_TRADING else True)
                    except Exception:
                        pass
                    res = close_short_market(sym)
                    exit_order_id = _order_id_from_res(res)
                    cancel_stop_orders(sym)
                    if engine_label == "ADVANCED_TREND_FOLLOWER":
                        cancel_open_orders(sym)
                    if engine_label == "ADVANCED_TREND_FOLLOWER":
                        cancel_open_orders(sym)
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
                    entry_time = _fmt_entry_time(open_tr)
                    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
                    icon = EXIT_ICON if exit_tag == "TP" else EXIT_SL_ICON
                    send_telegram(
                        f"{icon} <b>숏 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {exit_tag}\n"
                        f"{entry_line}"
                        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                        f"{'' if pnl_usdt is None else f' 손익={pnl_usdt:+.3f} USDT'}"
                        f"{'' if roi_leveraged is None else f' 레버리지ROI={roi_leveraged:.2f}%'}"
                    )
                    time.sleep(0.15)
                    continue
                if profit_unlev is not None and profit_unlev <= -sl_pct:
                    pnl_usdt = pos_detail.get("pnl") if isinstance(pos_detail, dict) else None
                    try:
                        set_dry_run(False if LIVE_TRADING else True)
                    except Exception:
                        pass
                    res = close_short_market(sym)
                    exit_order_id = _order_id_from_res(res)
                    cancel_stop_orders(sym)
                    if engine_label == "ADVANCED_TREND_FOLLOWER":
                        cancel_open_orders(sym)
                    if engine_label == "ADVANCED_TREND_FOLLOWER":
                        cancel_open_orders(sym)
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
                    entry_time = _fmt_entry_time(open_tr)
                    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
                    icon = EXIT_ICON if exit_tag == "TP" else EXIT_SL_ICON
                    send_telegram(
                        f"{icon} <b>숏 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {exit_tag}\n"
                        f"{entry_line}"
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
        open_tr = _get_open_trade(state, "SHORT", sym)
        engine_label = _engine_label_from_reason(
            (open_tr.get("meta") or {}).get("reason") if open_tr else None
        )
        dca_res = dca_short_if_needed(sym, adds_done=adds_done, margin_mode=MARGIN_MODE)
        if dca_res.get("status") not in ("skip", "warn"):
            if not _dca_alerted(state, sym, "SHORT", adds_done + 1):
                st["dca_adds_short"] = adds_done + 1
                state[sym] = st
                qty = dca_res.get("amount") or (dca_res.get("order") or {}).get("amount")
                _record_position_event(
                    sym,
                    "SHORT",
                    "DCA",
                    engine_label or "AUTO",
                    qty if isinstance(qty, (int, float)) else None,
                    dca_res.get("entry") if isinstance(dca_res.get("entry"), (int, float)) else None,
                    dca_res.get("mark") if isinstance(dca_res.get("mark"), (int, float)) else None,
                    {
                        "engine": engine_label or "AUTO",
                        "adds_done": adds_done + 1,
                        "dca_usdt": dca_res.get("dca_usdt"),
                        "source": "manage_dca",
                    },
                )
                send_telegram(
                    f"➕ <b>DCA</b> {sym} adds {adds_done}->{adds_done+1} mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                )
                _mark_dca_alerted(state, sym, "SHORT", adds_done + 1)
                try:
                    save_state(state)
                except Exception:
                    pass
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
                exit_reason = "manual_close"
                entry_px = open_tr.get("entry_price") if isinstance(open_tr, dict) else None
                tp_pct, sl_pct = _get_engine_exit_thresholds(engine_label, "LONG")
                meta = open_tr.get("meta") if isinstance(open_tr, dict) else None
                sl_price_meta = None
                if isinstance(meta, dict):
                    try:
                        sl_price_meta = float(meta.get("sl_price"))
                    except Exception:
                        sl_price_meta = None
                mark_px = _fetch_last_price(sym)
                if mark_px is None and isinstance(sl_price_meta, (int, float)):
                    mark_px = float(sl_price_meta)
                if isinstance(entry_px, (int, float)) and isinstance(mark_px, (int, float)) and entry_px > 0:
                    profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
                    if isinstance(tp_pct, (int, float)) and profit_unlev >= float(tp_pct):
                        exit_reason = "auto_exit_tp"
                    elif isinstance(sl_pct, (int, float)) and float(sl_pct) > 0 and profit_unlev <= -float(sl_pct):
                        exit_reason = "auto_exit_sl"
                    elif isinstance(sl_price_meta, (int, float)) and mark_px <= float(sl_price_meta):
                        exit_reason = "auto_exit_sl"
                _close_trade(
                    state,
                    side="LONG",
                    symbol=sym,
                    exit_ts=now,
                    exit_price=mark_px if isinstance(mark_px, (int, float)) else None,
                    pnl_usdt=None,
                    reason=exit_reason,
                )
                _append_report_line(sym, "LONG", None, None, engine_label)
                st = state.get(sym, {}) if isinstance(state, dict) else {}
                _set_in_pos_side(st, "LONG", False)
                st["last_ok"] = False
                st["dca_adds"] = 0
                st["dca_adds_long"] = 0
                st["dca_adds_short"] = 0
                state[sym] = st
                if engine_label != "UNKNOWN" and _trade_has_entry(open_tr) and not _recent_auto_exit(state, sym, now):
                    entry_time = _fmt_entry_time(open_tr)
                    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
                    exit_tag = "SL" if exit_reason == "auto_exit_sl" else "TP" if exit_reason == "auto_exit_tp" else "MANUAL"
                    icon = EXIT_SL_ICON if exit_tag == "SL" else EXIT_ICON
                    send_telegram(
                        f"{icon} <b>롱 청산</b>\n"
                        f"<b>{sym}</b>\n"
                        f"엔진: {_display_engine_label(engine_label)}\n"
                        f"사유: {exit_tag}\n"
                        f"{entry_line}".rstrip()
                    )
                time.sleep(0.1)
                continue
            open_tr = _get_open_trade(state, "LONG", sym)
            engine_label = _engine_label_from_reason(
                (open_tr.get("meta") or {}).get("reason") if open_tr else None
            )
            skip_line = f"[long-exit-skip] sym={sym} reason=no_position_detail engine={engine_label}"
            print(skip_line)
            continue
        entry_px = detail.get("entry")
        mark_px = detail.get("mark")
        if not isinstance(entry_px, (int, float)) or not isinstance(mark_px, (int, float)) or entry_px <= 0:
            continue
        engine_label = _engine_label_from_reason(
            (open_tr.get("meta") or {}).get("reason") if open_tr else None
        )
        profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
        if engine_label == "ADVANCED_TREND_FOLLOWER":
            continue
        closed = False
        tp_pct, sl_pct = _get_engine_exit_thresholds(engine_label, "LONG")
        if AUTO_EXIT_ENABLED and profit_unlev >= tp_pct:
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
            entry_time = _fmt_entry_time(open_tr)
            entry_line = f"진입시간={entry_time}\n" if entry_time else ""
            send_telegram(
                f"{EXIT_ICON} <b>롱 청산</b>\n"
                f"<b>{sym}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{entry_line}"
                f"체결가={avg_price} 수량={filled} 비용={cost}\n"
                f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
                f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
            )
            time.sleep(0.15)
            closed = True
        elif AUTO_EXIT_ENABLED and profit_unlev <= -sl_pct:
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
            entry_time = _fmt_entry_time(open_tr)
            entry_line = f"진입시간={entry_time}\n" if entry_time else ""
            send_telegram(
                f"{EXIT_SL_ICON} <b>롱 청산</b>\n"
                f"<b>{sym}</b>\n"
                f"엔진: {_display_engine_label(engine_label)}\n"
                f"사유: {exit_tag}\n"
                f"{entry_line}"
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
                open_tr = _get_open_trade(state, "LONG", sym)
                engine_label = _engine_label_from_reason(
                    (open_tr.get("meta") or {}).get("reason") if open_tr else None
                )
                dca_res = dca_long_if_needed(sym, adds_done=adds_done, margin_mode=MARGIN_MODE)
                if dca_res.get("status") not in ("skip", "warn"):
                    if not _dca_alerted(state, sym, "LONG", adds_done + 1):
                        st["dca_adds_long"] = adds_done + 1
                        state[sym] = st
                        qty = dca_res.get("amount") or (dca_res.get("order") or {}).get("amount")
                        _record_position_event(
                            sym,
                            "LONG",
                            "DCA",
                            engine_label or "AUTO",
                            qty if isinstance(qty, (int, float)) else None,
                            dca_res.get("entry") if isinstance(dca_res.get("entry"), (int, float)) else None,
                            dca_res.get("mark") if isinstance(dca_res.get("mark"), (int, float)) else None,
                            {
                                "engine": engine_label or "AUTO",
                                "adds_done": adds_done + 1,
                                "dca_usdt": dca_res.get("dca_usdt"),
                                "source": "manage_dca",
                            },
                        )
                        send_telegram(
                            f"➕ <b>DCA</b> {sym} LONG adds {adds_done}->{adds_done+1} "
                            f"mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                        )
                        _mark_dca_alerted(state, sym, "LONG", adds_done + 1)
                        try:
                            save_state(state)
                        except Exception:
                            pass
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


def _reload_runtime_settings_from_disk(state: dict, state_path: Optional[str] = None, skip_keys: Optional[set] = None) -> None:
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT, AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT
    global ENGINE_EXIT_OVERRIDES
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, SWAGGY_ATLAS_LAB_ENABLED
    global SWAGGY_ATLAS_LAB_V2_ENABLED, SWAGGY_NO_ATLAS_ENABLED, SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN
    global SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED, SWAGGY_D1_OVEREXT_ATR_MULT
    global SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK, SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS
    global SWAGGY_NO_ATLAS_USE_WICK_BREAK
    global SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG, SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS, SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR
    global SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG
    global SWAGGY_NO_ATLAS_EXPANSION_MULT
    global SWAGGY_NO_ATLAS_DELAY_WAIT_BARS, SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT
    global SWAGGY_NO_ATLAS_DELAY_VOL_MULT, SWAGGY_NO_ATLAS_DELAY_VOL_MA, SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK
    global SWAGGY_ATLAS_LAB_OFF_WINDOWS, SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, SWAGGY_NO_ATLAS_OFF_WINDOWS
    global ADV_TREND_ENABLED, ADV_TREND_MIN_QV, ADV_TREND_UNIVERSE_TOP_N, ADV_TREND_RISK_PCT
    global ADV_TREND_MAX_NOTIONAL_MULT, ADV_TREND_MIN_STOP_ATR, ADV_TREND_ADX_MIN
    global ADV_TREND_MFI_LONG_MAX, ADV_TREND_MFI_SHORT_MIN
    global SATURDAY_TRADE_ENABLED, DTFX_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, DIV15M_LONG_ENABLED, DIV15M_SHORT_ENABLED
    global RSI_ENABLED, LOSS_HEDGE_ENGINE_ENABLED, LOSS_HEDGE_INTERVAL_MIN
    global USDT_PER_TRADE, CHAT_ID_RUNTIME, MANAGE_WS_MODE, DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC, COOLDOWN_SEC
    try:
        disk = load_state_from(state_path) if state_path else load_state()
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
        "_engine_exit_overrides",
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
        "_sat_trade",
        "_swaggy_atlas_lab_enabled",
        "_swaggy_atlas_lab_v2_enabled",
        "_swaggy_no_atlas_enabled",
        "_swaggy_atlas_lab_off_windows",
        "_swaggy_atlas_lab_v2_off_windows",
        "_swaggy_no_atlas_off_windows",
        "_swaggy_no_atlas_overext_min",
        "_swaggy_no_atlas_overext_min_strong",
        "_swaggy_no_atlas_overext_min_enabled",
        "_swaggy_d1_overext_atr_mult",
        "_swaggy_no_atlas_structure_lookback",
        "_swaggy_no_atlas_structure_wait_bars",
        "_swaggy_no_atlas_use_wick_break",
        "_swaggy_no_atlas_break_margin_strong",
        "_swaggy_no_atlas_break_margin_weak",
        "_swaggy_no_atlas_sw_okn_min_margin",
        "_swaggy_no_atlas_weak_timeout_bars",
        "_swaggy_no_atlas_weak_no_progress_mfe_atr",
        "_swaggy_no_atlas_expansion_mult",
        "_swaggy_no_atlas_delay_wait_bars",
        "_swaggy_no_atlas_delay_close_pct",
        "_swaggy_no_atlas_delay_vol_mult",
        "_swaggy_no_atlas_delay_vol_ma",
        "_swaggy_no_atlas_delay_sweep_lookback",
        "_adv_trend_enabled",
        "_adv_trend_min_qv",
        "_adv_trend_universe_top_n",
        "_adv_trend_risk_pct",
        "_adv_trend_max_notional_mult",
        "_adv_trend_min_stop_atr",
        "_adv_trend_adx_min",
        "_adv_trend_mfi_long_max",
        "_adv_trend_mfi_short_min",
        "_loss_hedge_engine_enabled",
        "_loss_hedge_interval_min",
        "_rsi_enabled",
        "_dtfx_enabled",
        "_atlas_rs_fail_short_enabled",
        "_div15m_long_enabled",
        "_div15m_short_enabled",
        "_chat_id",
        "_manage_ws_mode",
        "_entry_event_offset",
        "_runtime_cfg_ts",
    ]
    for key in keys:
        if skip_keys and key in skip_keys:
            continue
        if key in disk:
            state[key] = disk.get(key)
    if isinstance(state.get("_atlas_rs_fail_short_enabled"), dict):
        state["_atlas_rs_fail_short_enabled"] = False
    if isinstance(state.get("_atlas_rs_fail_short_universe"), dict):
        state["_atlas_rs_fail_short_universe"] = []
    if isinstance(state.get("_swaggy_no_atlas_enabled"), dict):
        state["_swaggy_no_atlas_enabled"] = False
    if isinstance(state.get("_swaggy_no_atlas_overext_min"), dict):
        state["_swaggy_no_atlas_overext_min"] = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN
    if isinstance(state.get("_swaggy_no_atlas_overext_min_strong"), dict):
        state["_swaggy_no_atlas_overext_min_strong"] = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG
    if isinstance(state.get("_swaggy_no_atlas_overext_min_enabled"), dict):
        state["_swaggy_no_atlas_overext_min_enabled"] = SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED
    if isinstance(state.get("_swaggy_d1_overext_atr_mult"), dict):
        state["_swaggy_d1_overext_atr_mult"] = SWAGGY_D1_OVEREXT_ATR_MULT
    if isinstance(state.get("_swaggy_no_atlas_structure_lookback"), dict):
        state["_swaggy_no_atlas_structure_lookback"] = SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK
    if isinstance(state.get("_swaggy_no_atlas_structure_wait_bars"), dict):
        state["_swaggy_no_atlas_structure_wait_bars"] = SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS
    if isinstance(state.get("_swaggy_no_atlas_use_wick_break"), dict):
        state["_swaggy_no_atlas_use_wick_break"] = SWAGGY_NO_ATLAS_USE_WICK_BREAK
    if isinstance(state.get("_swaggy_no_atlas_break_margin_strong"), dict):
        state["_swaggy_no_atlas_break_margin_strong"] = SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG
    if isinstance(state.get("_swaggy_no_atlas_break_margin_weak"), dict):
        state["_swaggy_no_atlas_break_margin_weak"] = SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK
    if isinstance(state.get("_swaggy_no_atlas_sw_okn_min_margin"), dict):
        state["_swaggy_no_atlas_sw_okn_min_margin"] = SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    if isinstance(state.get("_swaggy_no_atlas_weak_timeout_bars"), dict):
        state["_swaggy_no_atlas_weak_timeout_bars"] = SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS
    if isinstance(state.get("_swaggy_no_atlas_weak_no_progress_mfe_atr"), dict):
        state["_swaggy_no_atlas_weak_no_progress_mfe_atr"] = SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR
    if isinstance(state.get("_swaggy_no_atlas_expansion_mult"), dict):
        state["_swaggy_no_atlas_expansion_mult"] = SWAGGY_NO_ATLAS_EXPANSION_MULT
    if isinstance(state.get("_swaggy_no_atlas_delay_wait_bars"), dict):
        state["_swaggy_no_atlas_delay_wait_bars"] = SWAGGY_NO_ATLAS_DELAY_WAIT_BARS
    if isinstance(state.get("_swaggy_no_atlas_delay_close_pct"), dict):
        state["_swaggy_no_atlas_delay_close_pct"] = SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT
    if isinstance(state.get("_swaggy_no_atlas_delay_vol_mult"), dict):
        state["_swaggy_no_atlas_delay_vol_mult"] = SWAGGY_NO_ATLAS_DELAY_VOL_MULT
    if isinstance(state.get("_swaggy_no_atlas_delay_vol_ma"), dict):
        state["_swaggy_no_atlas_delay_vol_ma"] = SWAGGY_NO_ATLAS_DELAY_VOL_MA
    if isinstance(state.get("_swaggy_no_atlas_delay_sweep_lookback"), dict):
        state["_swaggy_no_atlas_delay_sweep_lookback"] = SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK
    if isinstance(state.get("_swaggy_no_atlas_structure_min_strength"), dict):
        state["_swaggy_no_atlas_structure_min_strength"] = SWAGGY_NO_ATLAS_STRUCTURE_MIN_STRENGTH
    if (not skip_keys or "_auto_exit" not in skip_keys) and isinstance(state.get("_auto_exit"), bool):
        AUTO_EXIT_ENABLED = bool(state.get("_auto_exit"))
    if (not skip_keys or "_auto_exit_long_tp_pct" not in skip_keys) and isinstance(state.get("_auto_exit_long_tp_pct"), (int, float)):
        AUTO_EXIT_LONG_TP_PCT = float(state.get("_auto_exit_long_tp_pct"))
    if (not skip_keys or "_auto_exit_long_sl_pct" not in skip_keys) and isinstance(state.get("_auto_exit_long_sl_pct"), (int, float)):
        AUTO_EXIT_LONG_SL_PCT = float(state.get("_auto_exit_long_sl_pct"))
    if (not skip_keys or "_auto_exit_short_tp_pct" not in skip_keys) and isinstance(state.get("_auto_exit_short_tp_pct"), (int, float)):
        AUTO_EXIT_SHORT_TP_PCT = float(state.get("_auto_exit_short_tp_pct"))
    if (not skip_keys or "_auto_exit_short_sl_pct" not in skip_keys) and isinstance(state.get("_auto_exit_short_sl_pct"), (int, float)):
        AUTO_EXIT_SHORT_SL_PCT = float(state.get("_auto_exit_short_sl_pct"))
    if (not skip_keys or "_engine_exit_overrides" not in skip_keys) and isinstance(state.get("_engine_exit_overrides"), dict):
        ENGINE_EXIT_OVERRIDES = dict(state.get("_engine_exit_overrides"))
    if (not skip_keys or "_live_trading" not in skip_keys) and isinstance(state.get("_live_trading"), bool):
        LIVE_TRADING = bool(state.get("_live_trading"))
    if (not skip_keys or "_long_live" not in skip_keys) and isinstance(state.get("_long_live"), bool):
        LONG_LIVE_TRADING = bool(state.get("_long_live"))
    if (not skip_keys or "_max_open_positions" not in skip_keys) and isinstance(state.get("_max_open_positions"), (int, float)):
        try:
            MAX_OPEN_POSITIONS = int(state.get("_max_open_positions"))
        except Exception:
            pass
    if (not skip_keys or "_entry_usdt" not in skip_keys) and isinstance(state.get("_entry_usdt"), (int, float)):
        USDT_PER_TRADE = float(state.get("_entry_usdt"))
    if (not skip_keys or "_swaggy_atlas_lab_enabled" not in skip_keys) and isinstance(state.get("_swaggy_atlas_lab_enabled"), bool):
        SWAGGY_ATLAS_LAB_ENABLED = bool(state.get("_swaggy_atlas_lab_enabled"))
    if (not skip_keys or "_swaggy_atlas_lab_v2_enabled" not in skip_keys) and isinstance(state.get("_swaggy_atlas_lab_v2_enabled"), bool):
        SWAGGY_ATLAS_LAB_V2_ENABLED = bool(state.get("_swaggy_atlas_lab_v2_enabled"))
    if (not skip_keys or "_swaggy_no_atlas_enabled" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_enabled"), bool):
        SWAGGY_NO_ATLAS_ENABLED = bool(state.get("_swaggy_no_atlas_enabled"))
    if (not skip_keys or "_adv_trend_enabled" not in skip_keys) and isinstance(state.get("_adv_trend_enabled"), bool):
        ADV_TREND_ENABLED = bool(state.get("_adv_trend_enabled"))
    if (not skip_keys or "_adv_trend_min_qv" not in skip_keys) and isinstance(state.get("_adv_trend_min_qv"), (int, float)):
        ADV_TREND_MIN_QV = float(state.get("_adv_trend_min_qv"))
    if (not skip_keys or "_adv_trend_universe_top_n" not in skip_keys) and isinstance(state.get("_adv_trend_universe_top_n"), (int, float)):
        try:
            ADV_TREND_UNIVERSE_TOP_N = int(state.get("_adv_trend_universe_top_n"))
        except Exception:
            pass
    if (not skip_keys or "_adv_trend_risk_pct" not in skip_keys) and isinstance(state.get("_adv_trend_risk_pct"), (int, float)):
        ADV_TREND_RISK_PCT = float(state.get("_adv_trend_risk_pct"))
    if (not skip_keys or "_adv_trend_max_notional_mult" not in skip_keys) and isinstance(state.get("_adv_trend_max_notional_mult"), (int, float)):
        ADV_TREND_MAX_NOTIONAL_MULT = float(state.get("_adv_trend_max_notional_mult"))
    if (not skip_keys or "_adv_trend_min_stop_atr" not in skip_keys) and isinstance(state.get("_adv_trend_min_stop_atr"), (int, float)):
        ADV_TREND_MIN_STOP_ATR = float(state.get("_adv_trend_min_stop_atr"))
    if (not skip_keys or "_adv_trend_adx_min" not in skip_keys) and isinstance(state.get("_adv_trend_adx_min"), (int, float)):
        ADV_TREND_ADX_MIN = float(state.get("_adv_trend_adx_min"))
    if (not skip_keys or "_adv_trend_mfi_long_max" not in skip_keys) and isinstance(state.get("_adv_trend_mfi_long_max"), (int, float)):
        ADV_TREND_MFI_LONG_MAX = float(state.get("_adv_trend_mfi_long_max"))
    if (not skip_keys or "_adv_trend_mfi_short_min" not in skip_keys) and isinstance(state.get("_adv_trend_mfi_short_min"), (int, float)):
        ADV_TREND_MFI_SHORT_MIN = float(state.get("_adv_trend_mfi_short_min"))
    if (not skip_keys or "_loss_hedge_engine_enabled" not in skip_keys) and isinstance(state.get("_loss_hedge_engine_enabled"), bool):
        LOSS_HEDGE_ENGINE_ENABLED = bool(state.get("_loss_hedge_engine_enabled"))
    if (not skip_keys or "_loss_hedge_interval_min" not in skip_keys) and isinstance(state.get("_loss_hedge_interval_min"), (int, float)):
        try:
            LOSS_HEDGE_INTERVAL_MIN = max(0, int(state.get("_loss_hedge_interval_min")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_atlas_lab_off_windows" not in skip_keys) and isinstance(state.get("_swaggy_atlas_lab_off_windows"), str):
        SWAGGY_ATLAS_LAB_OFF_WINDOWS = str(state.get("_swaggy_atlas_lab_off_windows") or "")
    if (not skip_keys or "_swaggy_atlas_lab_v2_off_windows" not in skip_keys) and isinstance(state.get("_swaggy_atlas_lab_v2_off_windows"), str):
        SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS = str(state.get("_swaggy_atlas_lab_v2_off_windows") or "")
    if (not skip_keys or "_swaggy_no_atlas_off_windows" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_off_windows"), str):
        SWAGGY_NO_ATLAS_OFF_WINDOWS = str(state.get("_swaggy_no_atlas_off_windows") or "")
    if (not skip_keys or "_swaggy_no_atlas_overext_min" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_overext_min"), (int, float)):
        SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN = float(state.get("_swaggy_no_atlas_overext_min"))
    if (not skip_keys or "_swaggy_no_atlas_overext_min_strong" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_overext_min_strong"), (int, float)):
        SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG = float(state.get("_swaggy_no_atlas_overext_min_strong"))
    if (not skip_keys or "_swaggy_no_atlas_overext_min_enabled" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_overext_min_enabled"), bool):
        SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED = bool(state.get("_swaggy_no_atlas_overext_min_enabled"))
    if (not skip_keys or "_swaggy_d1_overext_atr_mult" not in skip_keys) and isinstance(state.get("_swaggy_d1_overext_atr_mult"), (int, float)):
        SWAGGY_D1_OVEREXT_ATR_MULT = float(state.get("_swaggy_d1_overext_atr_mult"))
    if (not skip_keys or "_swaggy_no_atlas_structure_lookback" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_structure_lookback"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK = max(1, int(state.get("_swaggy_no_atlas_structure_lookback")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_structure_wait_bars" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_structure_wait_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS = max(1, int(state.get("_swaggy_no_atlas_structure_wait_bars")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_use_wick_break" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_use_wick_break"), bool):
        SWAGGY_NO_ATLAS_USE_WICK_BREAK = bool(state.get("_swaggy_no_atlas_use_wick_break"))
    if (not skip_keys or "_swaggy_no_atlas_break_margin_strong" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_break_margin_strong"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG = float(state.get("_swaggy_no_atlas_break_margin_strong"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_break_margin_weak" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_break_margin_weak"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK = float(state.get("_swaggy_no_atlas_break_margin_weak"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_sw_okn_min_margin" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_sw_okn_min_margin"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN = float(state.get("_swaggy_no_atlas_sw_okn_min_margin"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_weak_timeout_bars" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_weak_timeout_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS = max(1, int(state.get("_swaggy_no_atlas_weak_timeout_bars")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_weak_no_progress_mfe_atr" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_weak_no_progress_mfe_atr"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR = float(state.get("_swaggy_no_atlas_weak_no_progress_mfe_atr"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_expansion_mult" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_expansion_mult"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_EXPANSION_MULT = float(state.get("_swaggy_no_atlas_expansion_mult"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_delay_wait_bars" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_delay_wait_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_WAIT_BARS = max(1, int(state.get("_swaggy_no_atlas_delay_wait_bars")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_delay_close_pct" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_delay_close_pct"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT = float(state.get("_swaggy_no_atlas_delay_close_pct"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_delay_vol_mult" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_delay_vol_mult"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_VOL_MULT = float(state.get("_swaggy_no_atlas_delay_vol_mult"))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_delay_vol_ma" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_delay_vol_ma"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_VOL_MA = max(1, int(state.get("_swaggy_no_atlas_delay_vol_ma")))
        except Exception:
            pass
    if (not skip_keys or "_swaggy_no_atlas_delay_sweep_lookback" not in skip_keys) and isinstance(state.get("_swaggy_no_atlas_delay_sweep_lookback"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK = max(1, int(state.get("_swaggy_no_atlas_delay_sweep_lookback")))
        except Exception:
            pass
    if (not skip_keys or "_rsi_enabled" not in skip_keys) and isinstance(state.get("_rsi_enabled"), bool):
        RSI_ENABLED = bool(state.get("_rsi_enabled"))
    if (not skip_keys or "_dca_enabled" not in skip_keys) and isinstance(state.get("_dca_enabled"), bool):
        DCA_ENABLED = bool(state.get("_dca_enabled"))
        if executor_mod:
            executor_mod.DCA_ENABLED = DCA_ENABLED
    if (not skip_keys or "_dca_pct" not in skip_keys) and isinstance(state.get("_dca_pct"), (int, float)):
        DCA_PCT = float(state.get("_dca_pct"))
        if executor_mod:
            executor_mod.DCA_PCT = DCA_PCT
    if (not skip_keys or "_dca_first_pct" not in skip_keys) and isinstance(state.get("_dca_first_pct"), (int, float)):
        DCA_FIRST_PCT = float(state.get("_dca_first_pct"))
        if executor_mod:
            executor_mod.DCA_FIRST_PCT = DCA_FIRST_PCT
    if (not skip_keys or "_dca_second_pct" not in skip_keys) and isinstance(state.get("_dca_second_pct"), (int, float)):
        DCA_SECOND_PCT = float(state.get("_dca_second_pct"))
        if executor_mod:
            executor_mod.DCA_SECOND_PCT = DCA_SECOND_PCT
    if (not skip_keys or "_dca_third_pct" not in skip_keys) and isinstance(state.get("_dca_third_pct"), (int, float)):
        DCA_THIRD_PCT = float(state.get("_dca_third_pct"))
        if executor_mod:
            executor_mod.DCA_THIRD_PCT = DCA_THIRD_PCT
    if (not skip_keys or "_sat_trade" not in skip_keys) and isinstance(state.get("_sat_trade"), bool):
        SATURDAY_TRADE_ENABLED = bool(state.get("_sat_trade"))
    if (not skip_keys or "_exit_cooldown_hours" not in skip_keys) and isinstance(state.get("_exit_cooldown_hours"), (int, float)):
        EXIT_COOLDOWN_HOURS = float(state.get("_exit_cooldown_hours"))
        COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
        EXIT_COOLDOWN_SEC = COOLDOWN_SEC
    if (not skip_keys or "_dtfx_enabled" not in skip_keys) and isinstance(state.get("_dtfx_enabled"), bool):
        DTFX_ENABLED = bool(state.get("_dtfx_enabled"))
    if (not skip_keys or "_atlas_rs_fail_short_enabled" not in skip_keys) and isinstance(state.get("_atlas_rs_fail_short_enabled"), bool):
        ATLAS_RS_FAIL_SHORT_ENABLED = bool(state.get("_atlas_rs_fail_short_enabled"))
    if (not skip_keys or "_div15m_long_enabled" not in skip_keys) and isinstance(state.get("_div15m_long_enabled"), bool):
        DIV15M_LONG_ENABLED = bool(state.get("_div15m_long_enabled"))
    if (not skip_keys or "_div15m_short_enabled" not in skip_keys) and isinstance(state.get("_div15m_short_enabled"), bool):
        DIV15M_SHORT_ENABLED = bool(state.get("_div15m_short_enabled"))
    if state.get("_chat_id"):
        CHAT_ID_RUNTIME = str(state.get("_chat_id"))
    if (not skip_keys or "_manage_ws_mode" not in skip_keys) and isinstance(state.get("_manage_ws_mode"), bool):
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
        "_engine_exit_overrides",
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
        "_sat_trade",
        "_swaggy_atlas_lab_enabled",
        "_swaggy_atlas_lab_v2_enabled",
        "_swaggy_no_atlas_enabled",
        "_swaggy_no_atlas_overext_min",
        "_swaggy_d1_overext_atr_mult",
        "_loss_hedge_engine_enabled",
        "_loss_hedge_interval_min",
        "_rsi_enabled",
        "_dtfx_enabled",
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
    _sync_admin_settings_from_state(state)


def _sync_admin_settings_from_state(state: dict) -> None:
    if not isinstance(state, dict):
        return
    try:
        admin_id = accounts_db.get_account_id_by_name("admin")
    except Exception:
        admin_id = None
    if not admin_id:
        return
    mapping = {
        "_entry_usdt": ("entry_pct", float),
        "_auto_exit": ("auto_exit", int),
        "_max_open_positions": ("max_positions", int),
        "_exit_cooldown_hours": ("exit_cooldown_h", float),
        "_auto_exit_long_tp_pct": ("long_tp_pct", float),
        "_auto_exit_long_sl_pct": ("long_sl_pct", float),
        "_auto_exit_short_tp_pct": ("short_tp_pct", float),
        "_auto_exit_short_sl_pct": ("short_sl_pct", float),
        "_dca_enabled": ("dca_enabled", int),
        "_dca_pct": ("dca_pct", float),
        "_dca_first_pct": ("dca1_pct", float),
        "_dca_second_pct": ("dca2_pct", float),
        "_dca_third_pct": ("dca3_pct", float),
    }
    for state_key, (db_key, cast) in mapping.items():
        if state_key not in state:
            continue
        val = state.get(state_key)
        if val is None:
            continue
        try:
            if cast is int:
                val = int(bool(val)) if isinstance(val, bool) else int(val)
            else:
                val = cast(val)
        except Exception:
            continue
        try:
            accounts_db.update_account_setting(admin_id, db_key, val)
        except Exception:
            pass
    if "_live_trading" in state:
        try:
            dry_run_val = 0 if bool(state.get("_live_trading")) else 1
            accounts_db.update_account_setting(admin_id, "dry_run", int(dry_run_val))
        except Exception:
            pass

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
        entry_ts = tr.get("entry_ts")
        if not isinstance(entry_ts, (int, float)):
            entry_ts = time.time()
        date_tag = datetime.fromtimestamp(float(entry_ts), tz=timezone.utc).strftime("%Y-%m-%d")
        dir_path = os.path.join("logs", "entry")
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, f"entry_events-{date_tag}.log")
        payload = {
            "entry_ts": datetime.fromtimestamp(float(entry_ts)).strftime("%Y-%m-%d %H:%M:%S"),
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
                dbrec.record_engine_signal(
                    symbol=payload.get("symbol") or "",
                    side=payload.get("side") or "",
                    engine=payload.get("engine") or "",
                    reason=(payload.get("engine") or "").lower(),
                    meta=payload,
                    ts=float(entry_ts),
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
            st["in_pos_short"] = True
            st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
            st.setdefault("dca_adds", 0)
            st.setdefault("dca_adds_long", 0)
            st.setdefault("dca_adds_short", 0)
            st.setdefault("last_entry", now)
            st.setdefault("manage_ping_ts", now - MANAGE_PING_COOLDOWN_SEC)
            state[sym] = st
        else:
            if isinstance(st, dict):
                st["in_pos_short"] = False
                st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
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
        in_long = bool(long_amt > 0)
        in_short = bool(short_amt > 0)
        st["in_pos_long"] = in_long
        st["in_pos_short"] = in_short
        st["in_pos"] = bool(in_long or in_short)
        if in_short or in_long:
            st.setdefault("dca_adds", 0)
            st.setdefault("dca_adds_long", 0)
            st.setdefault("dca_adds_short", 0)
            st.setdefault("last_entry", now)
            st.setdefault("manage_ping_ts", now - MANAGE_PING_COOLDOWN_SEC)
            state[sym] = st
        else:
            if isinstance(st, dict):
                st["in_pos_long"] = False
                st["in_pos_short"] = False
                st["in_pos"] = False
                state[sym] = st

def send_telegram(text: str, allow_early: bool = False, chat_id: Optional[str] = None) -> bool:
    client = getattr(_THREAD_TG, "client", None)
    if client:
        try:
            return client.send(text, allow_early=allow_early, chat_id=chat_id)
        except Exception:
            return False
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


def _coerce_state_float(val: object) -> float:
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except Exception:
            return 0.0
    if isinstance(val, dict):
        for key in ("value", "offset", "id", "last"):
            if key in val:
                return _coerce_state_float(val.get(key))
    return 0.0

def handle_telegram_commands(state: Dict[str, dict]) -> None:
    """텔레그램으로부터 런타임 명령을 받아 AUTO_EXIT 토글/상태를 제어한다.
    - /auto_exit on|off|status
    - /status
    처리한 마지막 update_id는 state["_tg_offset"]에 저장한다.
    현재 auto-exit 설정은 state["_auto_exit"]에 동기화한다.
    """
    global AUTO_EXIT_ENABLED, AUTO_EXIT_LONG_TP_PCT, AUTO_EXIT_LONG_SL_PCT, AUTO_EXIT_SHORT_TP_PCT, AUTO_EXIT_SHORT_SL_PCT
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, SWAGGY_ATLAS_LAB_ENABLED, SWAGGY_ATLAS_LAB_V2_ENABLED, SWAGGY_NO_ATLAS_ENABLED, ADV_TREND_ENABLED, SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN, SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED, SWAGGY_D1_OVEREXT_ATR_MULT, SWAGGY_ATLAS_LAB_OFF_WINDOWS, SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, SWAGGY_NO_ATLAS_OFF_WINDOWS, SATURDAY_TRADE_ENABLED, DTFX_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, RSI_ENABLED, LOSS_HEDGE_ENGINE_ENABLED, LOSS_HEDGE_INTERVAL_MIN
    global DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT, USDT_PER_TRADE
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC, COOLDOWN_SEC
    if not BOT_TOKEN:
        return
    # manage_ws 프로세스에서 contexts가 비어있는 경우가 있어 주기적으로 리프레시
    _maybe_refresh_accounts(reason="telegram_periodic")
    if not ACCOUNT_CONTEXTS:
        _reload_account_contexts(reason="telegram_boot")
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
                def _build_positions_message() -> str:
                    try:
                        refresh_positions_cache(force=True)
                    except Exception:
                        pass
                    print("[positions] build message start", flush=True)
                    pos_cache = getattr(executor_mod, "_POS_ALL_CACHE", {}) if executor_mod else {}
                    pos_map = pos_cache.get("positions_by_symbol") if isinstance(pos_cache, dict) else None
                    pos_map = pos_map or {}
                    open_trades = [tr for tr in (_get_trade_log(state) or []) if tr.get("status") == "open"]
                    engine_by_key = {}
                    entry_by_id, entry_by_symbol = _load_entry_events_map(
                        None, include_alerts=True, include_engine_signals=True
                    )
                    position_keys = set()
                    for sym, positions in pos_map.items():
                        if not positions:
                            continue
                        for p in positions:
                            try:
                                size = executor_mod._position_size_abs(p)
                            except Exception:
                                size = 0.0
                            if not size:
                                continue
                            side = "LONG" if executor_mod._is_long_position(p) else "SHORT" if executor_mod._is_short_position(p) else "UNKNOWN"
                            if side in ("LONG", "SHORT"):
                                position_keys.add((sym, side))
                    swaggy_trade_map = _load_swaggy_trade_engine_map(position_keys)
                    try:
                        print(f"[positions] entry_by_id={len(entry_by_id)} entry_by_symbol={len(entry_by_symbol)}", flush=True)
                    except Exception:
                        pass
                    entry_by_symbol_norm = {}
                    entry_by_symbol_flat = {}
                    def _pick_entry_record(recs: list) -> Optional[dict]:
                        if not recs:
                            return None
                        def _is_event(rec: dict) -> bool:
                            return (rec.get("record_type") or "") == "event" or rec.get("event_type") in ("ENTRY", "ALERT_ENTRY")
                        def _ts_val(rec: dict) -> float:
                            try:
                                return float(rec.get("entry_ts") or 0.0)
                            except Exception:
                                return 0.0
                        def _is_manual_pos_snapshot(rec: dict) -> bool:
                            eng = str(rec.get("engine") or "").strip().upper()
                            if eng not in ("MANUAL", "MANUAL_ENTRY"):
                                return False
                            meta = rec.get("meta") or {}
                            src = (rec.get("source") or "").strip().lower()
                            meta_src = str(meta.get("source") or "").strip().lower()
                            return src == "pos_snapshot" or meta_src == "pos_snapshot"
                        def _rank(rec: dict) -> tuple:
                            # Prefer non-pos_snapshot records even if older; then by ts; then event vs signal.
                            return (
                                0 if _is_manual_pos_snapshot(rec) else 1,
                                _ts_val(rec),
                                1 if _is_event(rec) else 0,
                            )
                        try:
                            return max(recs, key=_rank)
                        except Exception:
                            return recs[-1]
                    if isinstance(entry_by_symbol, dict):
                        for (sym_key, side_key), recs in entry_by_symbol.items():
                            if not sym_key or not side_key or not recs:
                                continue
                            sym_norm = str(sym_key).replace("/USDT:USDT", "").replace("/USDT", "")
                            rec = _pick_entry_record(recs)
                            if rec is None:
                                continue
                            entry_by_symbol_norm[(sym_norm, str(side_key).upper())] = rec
                            entry_by_symbol_flat.setdefault(sym_norm, []).append(rec)
                    if entry_by_symbol_flat:
                        for sym_norm, recs in list(entry_by_symbol_flat.items()):
                            rec = _pick_entry_record(recs)
                            if rec is None:
                                continue
                            entry_by_symbol_flat[sym_norm] = rec
                    for tr in open_trades:
                        sym = tr.get("symbol")
                        side = (tr.get("side") or "").upper()
                        if not sym or not side:
                            continue
                        engine = tr.get("engine_label") or (tr.get("meta") or {}).get("engine") or (tr.get("meta") or {}).get("reason")
                        if engine:
                            eng_norm = str(engine).strip()
                            eng_upper = eng_norm.upper()
                            if eng_upper not in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                                engine_by_key[(sym, side)] = eng_norm
                                sym_norm = str(sym).replace("/USDT:USDT", "").replace("/USDT", "")
                                engine_by_key[(sym_norm, side)] = eng_norm
                    rows = []
                    sym_col = 0
                    eng_col = 0
                    fallback_positions = []
                    if not pos_map:
                        try:
                            pos_syms = list_open_position_symbols(force=True)
                        except Exception:
                            pos_syms = {}
                        for sym in pos_syms.get("long") or []:
                            fallback_positions.append((sym, "LONG"))
                        for sym in pos_syms.get("short") or []:
                            fallback_positions.append((sym, "SHORT"))
                    if pos_map:
                        for sym, positions in pos_map.items():
                            if not positions:
                                continue
                            for p in positions:
                                try:
                                    size = executor_mod._position_size_abs(p)
                                except Exception:
                                    size = 0.0
                                if not size:
                                    continue
                                side = "LONG" if executor_mod._is_long_position(p) else "SHORT" if executor_mod._is_short_position(p) else "UNKNOWN"
                                sym_norm = (sym or "").replace("/USDT:USDT", "").replace("/USDT", "")
                                engine = engine_by_key.get((sym, side)) or engine_by_key.get((sym_norm, side))
                                if isinstance(engine, str) and engine.strip().upper() in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                                    engine = None
                                if not engine:
                                    st = state.get(sym, {}) if isinstance(state, dict) else {}
                                    entry_order_id = None
                                    if isinstance(st, dict):
                                        entry_order_id = st.get(f"entry_order_id_{side.lower()}")
                                    debug_info = {
                                        "sym": sym,
                                        "side": side,
                                        "sym_norm": sym_norm,
                                        "state_entry_id": entry_order_id,
                                        "engine_by_key": engine_by_key.get((sym, side)) or engine_by_key.get((sym_norm, side)),
                                    }
                                    if entry_order_id and isinstance(entry_by_id, dict):
                                        rec = entry_by_id.get(str(entry_order_id))
                                        engine = rec.get("engine") if isinstance(rec, dict) else None
                                        if engine:
                                            debug_info["engine_by_id"] = engine
                                    if not engine and isinstance(entry_by_symbol, dict):
                                        recs = entry_by_symbol.get((sym, side)) or []
                                        rec = _pick_entry_record(recs)
                                        if isinstance(rec, dict):
                                            engine = rec.get("engine")
                                            if engine:
                                                debug_info["engine_by_sym"] = engine
                                    if not engine and entry_by_symbol_norm:
                                        rec = entry_by_symbol_norm.get((sym_norm, side))
                                        if isinstance(rec, dict):
                                            engine = rec.get("engine")
                                            if engine:
                                                debug_info["engine_by_sym_norm"] = engine
                                    if not engine and entry_by_symbol_flat:
                                        rec = entry_by_symbol_flat.get(sym_norm)
                                        if isinstance(rec, dict):
                                            engine = rec.get("engine")
                                            if engine:
                                                debug_info["engine_by_sym_flat"] = engine
                                    if not engine:
                                        print(f"[positions-map] unresolved {debug_info}")
                                if not engine:
                                    print(f"[positions-map] unresolved {debug_info}", flush=True)
                                print(f"[positions-map] sym={sym} side={side} engine={engine}", flush=True)
                                if not engine or str(engine).strip().upper() in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                                    rec = swaggy_trade_map.get((sym, side))
                                    if isinstance(rec, dict):
                                        swaggy_engine = rec.get("engine")
                                        if swaggy_engine:
                                            engine = swaggy_engine
                                engine = engine or "UNKNOWN"
                                base = (sym or "").replace("/USDT:USDT", "")
                                sym_col = max(sym_col, len(base))
                                eng_col = max(eng_col, len(str(engine)))
                                tp_pct, sl_pct = _get_engine_exit_thresholds(engine, side)
                                rows.append((base, side, engine, tp_pct, sl_pct))
                    else:
                        for sym, side in fallback_positions:
                            sym_norm = (sym or "").replace("/USDT:USDT", "").replace("/USDT", "")
                            engine = engine_by_key.get((sym, side)) or engine_by_key.get((sym_norm, side))
                            if isinstance(engine, str) and engine.strip().upper() in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                                engine = None
                            if not engine:
                                st = state.get(sym, {}) if isinstance(state, dict) else {}
                                entry_order_id = None
                                if isinstance(st, dict):
                                    entry_order_id = st.get(f"entry_order_id_{side.lower()}")
                                debug_info = {
                                    "sym": sym,
                                    "side": side,
                                    "sym_norm": sym_norm,
                                    "state_entry_id": entry_order_id,
                                    "engine_by_key": engine_by_key.get((sym, side)) or engine_by_key.get((sym_norm, side)),
                                }
                                if entry_order_id and isinstance(entry_by_id, dict):
                                    rec = entry_by_id.get(str(entry_order_id))
                                    engine = rec.get("engine") if isinstance(rec, dict) else None
                                    if engine:
                                        debug_info["engine_by_id"] = engine
                                if not engine and isinstance(entry_by_symbol, dict):
                                    recs = entry_by_symbol.get((sym, side)) or []
                                    rec = _pick_entry_record(recs)
                                    if isinstance(rec, dict):
                                        engine = rec.get("engine")
                                        if engine:
                                            debug_info["engine_by_sym"] = engine
                                if not engine and entry_by_symbol_norm:
                                    rec = entry_by_symbol_norm.get((sym_norm, side))
                                    if isinstance(rec, dict):
                                        engine = rec.get("engine")
                                        if engine:
                                            debug_info["engine_by_sym_norm"] = engine
                                if not engine and entry_by_symbol_flat:
                                    rec = entry_by_symbol_flat.get(sym_norm)
                                    if isinstance(rec, dict):
                                        engine = rec.get("engine")
                                        if engine:
                                            debug_info["engine_by_sym_flat"] = engine
                                if not engine:
                                    print(f"[positions-map] unresolved {debug_info}")
                            if not engine:
                                print(f"[positions-map] unresolved {debug_info}", flush=True)
                            print(f"[positions-map] sym={sym} side={side} engine={engine}", flush=True)
                            if not engine or str(engine).strip().upper() in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                                rec = swaggy_trade_map.get((sym, side))
                                if isinstance(rec, dict):
                                    swaggy_engine = rec.get("engine")
                                    if swaggy_engine:
                                        engine = swaggy_engine
                            engine = engine or "UNKNOWN"
                            base = (sym or "").replace("/USDT:USDT", "")
                            sym_col = max(sym_col, len(base))
                            eng_col = max(eng_col, len(str(engine)))
                            tp_pct, sl_pct = _get_engine_exit_thresholds(engine, side)
                            rows.append((base, side, engine, tp_pct, sl_pct))
                    lines = [f"ℹ️ positions total={len(rows)} pid={os.getpid()}"]
                    if not rows:
                        lines.append("none")
                    else:
                        for base, side, engine, tp_pct, sl_pct in sorted(rows, key=lambda r: (r[2], r[0], r[1])):
                            tp_str = f"{tp_pct:.2f}%" if isinstance(tp_pct, (int, float)) else "N/A"
                            sl_str = f"{sl_pct:.2f}%" if isinstance(sl_pct, (int, float)) else "N/A"
                            lines.append(
                                f"{base.ljust(sym_col)}  {side.ljust(5)}  {str(engine).ljust(eng_col)}  tp={tp_str} sl={sl_str}"
                            )
                    return "\n".join(lines)

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
                if (cmd in ("/sat_trade", "sat_trade")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        SATURDAY_TRADE_ENABLED = True
                        state["_sat_trade"] = True
                        state_dirty = True
                        resp = "✅ saturday trade ON (토요일 진입 허용)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        SATURDAY_TRADE_ENABLED = False
                        state["_sat_trade"] = False
                        state_dirty = True
                        resp = "⛔ saturday trade OFF (토요일 진입 차단)"
                    else:
                        resp = f"ℹ️ saturday trade 상태: {'ON' if SATURDAY_TRADE_ENABLED else 'OFF'}\n사용법: /sat_trade on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] sat_trade cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True

                if cmd in ("/engine_exit", "engine_exit") and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "list"):
                        resp = f"ℹ️ engine-exit: {_format_engine_exit_overrides()}\n사용법: /engine_exit ENGINE SIDE tp sl"
                    else:
                        if len(parts) < 3:
                            resp = "⛔ 사용법: /engine_exit ENGINE SIDE tp sl | /engine_exit ENGINE off"
                        else:
                            engine_key = _normalize_engine_key(parts[1])
                            side_key = parts[2].upper()
                            if side_key == "OFF":
                                if ENGINE_EXIT_OVERRIDES.pop(engine_key, None) is not None:
                                    state["_engine_exit_overrides"] = ENGINE_EXIT_OVERRIDES
                                    state_dirty = True
                                resp = f"✅ engine-exit cleared: {engine_key}"
                            elif side_key not in ("LONG", "SHORT"):
                                resp = "⛔ SIDE는 LONG 또는 SHORT (또는 off)만 가능"
                            else:
                                if len(parts) >= 4 and parts[3].lower() == "off":
                                    entry = ENGINE_EXIT_OVERRIDES.get(engine_key) if isinstance(ENGINE_EXIT_OVERRIDES, dict) else None
                                    if isinstance(entry, dict):
                                        entry.pop(side_key, None)
                                        if not entry:
                                            ENGINE_EXIT_OVERRIDES.pop(engine_key, None)
                                        else:
                                            ENGINE_EXIT_OVERRIDES[engine_key] = entry
                                        state["_engine_exit_overrides"] = ENGINE_EXIT_OVERRIDES
                                        state_dirty = True
                                    resp = f"✅ engine-exit cleared: {engine_key} {side_key}"
                                elif len(parts) < 5:
                                    resp = "⛔ 사용법: /engine_exit ENGINE SIDE tp sl (예: /engine_exit SWAGGY_ATLAS_LAB SHORT 2 6)"
                                else:
                                    tp_raw = parts[3]
                                    sl_raw = parts[4]
                                    try:
                                        tp = float(tp_raw)
                                        sl = float(sl_raw)
                                        if tp <= 0 or sl <= 0:
                                            raise ValueError("non-positive")
                                        entry = ENGINE_EXIT_OVERRIDES.get(engine_key) if isinstance(ENGINE_EXIT_OVERRIDES, dict) else None
                                        if not isinstance(entry, dict):
                                            entry = {}
                                        entry[side_key] = {"tp": tp, "sl": sl}
                                        ENGINE_EXIT_OVERRIDES[engine_key] = entry
                                        state["_engine_exit_overrides"] = ENGINE_EXIT_OVERRIDES
                                        state_dirty = True
                                        resp = f"✅ engine-exit set: {engine_key} {side_key} tp={tp:.2f}% sl={sl:.2f}%"
                                    except Exception:
                                        resp = "⛔ TP/SL은 0보다 큰 숫자여야 함"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] engine_exit cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True

                if cmd in ("/positions", "/pos", "positions", "pos") and not responded:
                    ok = _reply(_build_positions_message())
                    print(f"[telegram] positions cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True

                if cmd in ("/admin_follow", "admin_follow"):
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        state["_admin_follow_enabled"] = True
                        state_dirty = True
                        resp = "✅ admin_follow ON (관리자 셋팅 적용)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        state["_admin_follow_enabled"] = False
                        state_dirty = True
                        resp = "⛔ admin_follow OFF (관리자 셋팅 적용 안함)"
                    else:
                        cur = state.get("_admin_follow_enabled")
                        if cur is None:
                            cur = True
                        resp = f"ℹ️ admin_follow: {'ON' if cur else 'OFF'}\n사용법: /admin_follow on|off|status"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] admin_follow cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True

                if cmd in ("/reload_accounts", "reload_accounts") and not responded:
                    info = _reload_account_contexts(reason="telegram")
                    if info.get("ok"):
                        resp = f"✅ accounts reloaded\n{info.get('summary')}"
                    else:
                        resp = f"⛔ accounts reload failed\n{info.get('summary')}"
                    ok = _reply(resp)
                    print(f"[telegram] reload_accounts cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True

                if cmd in ("/accounts", "accounts") and not responded:
                    db_path = getattr(accounts_db, "DB_PATH", "unknown")
                    summary = _format_account_summary(ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT)
                    resp = f"ℹ️ accounts\n{summary}\ndb={db_path}"
                    ok = _reply(resp)
                    print(f"[telegram] accounts cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True

                if cmd in ("/pnl_today", "pnl_today", "/today_pnl", "today_pnl") and not responded:
                    _maybe_refresh_accounts(reason="pnl_today")
                    contexts = ACCOUNT_CONTEXTS or ([ADMIN_ACCOUNT_CONTEXT] if ADMIN_ACCOUNT_CONTEXT else [])
                    if not contexts:
                        resp = "⛔ 계정을 찾을 수 없습니다."
                    else:
                        start_ts = _kst_today_start_ts()
                        date_kst = time.strftime("%Y-%m-%d", time.gmtime(start_ts + 9 * 3600))
                        lines = [f"📊 오늘 실현손익 (KST {date_kst})"]
                        for acct in contexts:
                            if not acct:
                                continue
                            try:
                                with acct.executor.activate():
                                    total, cnt, err = _fetch_realized_pnl_since(exchange, start_ts)
                            except Exception as e:
                                total, cnt, err = None, 0, str(e)[:80]
                            if err:
                                lines.append(f"- {acct.name}: N/A (err={err})")
                            else:
                                lines.append(f"- {acct.name}: {total:+.4f} USDT (trades={cnt})")
                        resp = "\n".join(lines)
                    ok = _reply(resp)
                    print(f"[telegram] pnl_today cmd 처리 send={'ok' if ok else 'fail'}")
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
                        try:
                            admin_id = accounts_db.get_account_id_by_name("admin")
                            if admin_id:
                                accounts_db.update_account_setting(admin_id, "dry_run", 0)
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
                        try:
                            admin_id = accounts_db.get_account_id_by_name("admin")
                            if admin_id:
                                accounts_db.update_account_setting(admin_id, "dry_run", 1)
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
                            open_pos = _count_open_positions_state(state)
                    except Exception as e:
                        open_pos = 0
                        print(f"[telegram] status open_pos error: {e}")
                    try:
                        over_limit = "YES" if open_pos > MAX_OPEN_POSITIONS else "NO"
                        status_msg = (
                            "🤖 상태\n"
                            f"/auto_exit(자동청산): {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
                            f"/sat_trade(토요일진입): {'ON' if SATURDAY_TRADE_ENABLED else 'OFF'}\n"
                            f"/long_live(롱실주문): {'ON' if LONG_LIVE_TRADING else 'OFF'}\n"
                            f"/live(숏실주문): {'ON' if LIVE_TRADING else 'OFF'}\n"
                            f"/max_pos(동시진입): {MAX_OPEN_POSITIONS}\n"
                            f"/exit_cd_h(재진입시간h): {EXIT_COOLDOWN_HOURS:.2f}h\n"
                            "--------------\n"
                            f"/entry_usdt(진입비율%): {USDT_PER_TRADE:.2f}%\n"
                            f"/dca(추가진입): {'ON' if DCA_ENABLED else 'OFF'} | /dca_pct: {DCA_PCT:.2f}%\n"
                            f"/dca1: {DCA_FIRST_PCT:.2f}% | /dca2: {DCA_SECOND_PCT:.2f}% | /dca3: {DCA_THIRD_PCT:.2f}%\n"
                            f"/swaggy_no_atlas_overext: {SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN:.2f} "
                            f"({ 'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF' })\n"
                            f"/swaggy_no_atlas_overext_on: {'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF'}\n"
                            f"/swaggy_d1_overext: {SWAGGY_D1_OVEREXT_ATR_MULT:.2f}\n"
                            f"/loss_hedge_interval: {LOSS_HEDGE_INTERVAL_MIN}분\n"
                            f"/swaggy_atlas_lab_off: {SWAGGY_ATLAS_LAB_OFF_WINDOWS or 'NONE'}\n"
                            f"/swaggy_atlas_lab_v2_off: {SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS or 'NONE'}\n"
                            f"/swaggy_no_atlas_off: {SWAGGY_NO_ATLAS_OFF_WINDOWS or 'NONE'}\n"
                            f"/l_exit_tp: {_fmt_pct_safe(AUTO_EXIT_LONG_TP_PCT)} | /l_exit_sl: {_fmt_pct_safe(AUTO_EXIT_LONG_SL_PCT)}\n"
                            f"/s_exit_tp: {_fmt_pct_safe(AUTO_EXIT_SHORT_TP_PCT)} | /s_exit_sl: {_fmt_pct_safe(AUTO_EXIT_SHORT_SL_PCT)}\n"
                            f"/engine_exit: {_format_engine_exit_overrides()}\n"
                            "--------------\n"
                            f"엔진요약: swaggy_lab={'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'} "
                            f"swaggy_lab_v2={'ON' if SWAGGY_ATLAS_LAB_V2_ENABLED else 'OFF'} "
                            f"no_atlas={'ON' if SWAGGY_NO_ATLAS_ENABLED else 'OFF'} "
                            f"adv_trend={'ON' if ADV_TREND_ENABLED else 'OFF'} "
                            f"loss_hedge={'ON' if LOSS_HEDGE_ENGINE_ENABLED else 'OFF'} "
                            f"dtfx={'ON' if DTFX_ENABLED else 'OFF'} "
                            f"arsf={'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'} "
                            f"rsi={'ON' if RSI_ENABLED else 'OFF'} "
                            ""
                            "--------------\n"
                            f"/swaggy_atlas_lab(추가진입): {'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'}\n"
                            f"/swaggy_atlas_lab_v2(추가진입): {'ON' if SWAGGY_ATLAS_LAB_V2_ENABLED else 'OFF'}\n"
                            f"/swaggy_no_atlas(추가진입): {'ON' if SWAGGY_NO_ATLAS_ENABLED else 'OFF'}\n"
                            f"/adv_trend(추가진입): {'ON' if ADV_TREND_ENABLED else 'OFF'}\n"
                            f"/loss_hedge_engine(손실방지): {'ON' if LOSS_HEDGE_ENGINE_ENABLED else 'OFF'}\n"
                            f"/dtfx(추가진입): {'ON' if DTFX_ENABLED else 'OFF'}\n\n"
                            f"/atlas_rs_fail_short(추가진입): {'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'}\n"
                            f"/rsi(추가진입): {'ON' if RSI_ENABLED else 'OFF'}\n\n"
                            ""
                            "--------------\n"
                            "/report(리포트): /report today|yesterday|YYYY-MM-DD\n"
                            f"open positions: {open_pos} (over_limit={over_limit})\n"
                            "--------------\n"
                            f"{_build_positions_message()}"
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
                            open_pos = _count_open_positions_state(state)
                    except Exception as e:
                        open_pos = 0
                        print(f"[telegram] status open_pos error: {e}")
                    over_limit = "YES" if open_pos > MAX_OPEN_POSITIONS else "NO"
                    status_msg = (
                        "🤖 상태\n"
                        f"/auto_exit(자동청산): {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
                        f"/sat_trade(토요일진입): {'ON' if SATURDAY_TRADE_ENABLED else 'OFF'}\n"
                        f"/long_live(롱실주문): {'ON' if LONG_LIVE_TRADING else 'OFF'}\n"
                        f"/live(숏실주문): {'ON' if LIVE_TRADING else 'OFF'}\n"
                        f"/max_pos(동시진입): {MAX_OPEN_POSITIONS}\n"
                        f"/exit_cd_h(재진입시간h): {EXIT_COOLDOWN_HOURS:.2f}h\n"
                        "--------------\n"
                        f"/entry_usdt(진입비율%): {USDT_PER_TRADE:.2f}%\n"
                        f"/dca(추가진입): {'ON' if DCA_ENABLED else 'OFF'} | /dca_pct: {DCA_PCT:.2f}%\n"
                        f"/dca1: {DCA_FIRST_PCT:.2f}% | /dca2: {DCA_SECOND_PCT:.2f}% | /dca3: {DCA_THIRD_PCT:.2f}%\n"
                        f"/swaggy_no_atlas_overext: {SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN:.2f} "
                        f"({ 'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF' })\n"
                        f"/swaggy_no_atlas_overext_on: {'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF'}\n"
                        f"/swaggy_d1_overext: {SWAGGY_D1_OVEREXT_ATR_MULT:.2f}\n"
                        f"/loss_hedge_interval: {LOSS_HEDGE_INTERVAL_MIN}분\n"
                        f"/swaggy_atlas_lab_off: {SWAGGY_ATLAS_LAB_OFF_WINDOWS or 'NONE'}\n"
                        f"/swaggy_atlas_lab_v2_off: {SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS or 'NONE'}\n"
                        f"/swaggy_no_atlas_off: {SWAGGY_NO_ATLAS_OFF_WINDOWS or 'NONE'}\n"
                        f"/l_exit_tp: {_fmt_pct_safe(AUTO_EXIT_LONG_TP_PCT)} | /l_exit_sl: {_fmt_pct_safe(AUTO_EXIT_LONG_SL_PCT)}\n"
                        f"/s_exit_tp: {_fmt_pct_safe(AUTO_EXIT_SHORT_TP_PCT)} | /s_exit_sl: {_fmt_pct_safe(AUTO_EXIT_SHORT_SL_PCT)}\n"
                        f"/engine_exit: {_format_engine_exit_overrides()}\n"
                        "--------------\n"
                        f"엔진요약: swaggy_lab={'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'} "
                        f"swaggy_lab_v2={'ON' if SWAGGY_ATLAS_LAB_V2_ENABLED else 'OFF'} "
                        f"no_atlas={'ON' if SWAGGY_NO_ATLAS_ENABLED else 'OFF'} "
                        f"loss_hedge={'ON' if LOSS_HEDGE_ENGINE_ENABLED else 'OFF'} "
                        f"dtfx={'ON' if DTFX_ENABLED else 'OFF'} "
                        f"arsf={'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'} "
                        f"rsi={'ON' if RSI_ENABLED else 'OFF'} "
                        ""
                        "--------------\n"
                        f"/swaggy_atlas_lab(추가진입): {'ON' if SWAGGY_ATLAS_LAB_ENABLED else 'OFF'}\n"
                        f"/swaggy_atlas_lab_v2(추가진입): {'ON' if SWAGGY_ATLAS_LAB_V2_ENABLED else 'OFF'}\n"
                        f"/swaggy_no_atlas(추가진입): {'ON' if SWAGGY_NO_ATLAS_ENABLED else 'OFF'}\n"
                        f"/loss_hedge_engine(손실방지): {'ON' if LOSS_HEDGE_ENGINE_ENABLED else 'OFF'}\n"
                        f"/dtfx(추가진입): {'ON' if DTFX_ENABLED else 'OFF'}\n\n"
                        f"/atlas_rs_fail_short(추가진입): {'ON' if ATLAS_RS_FAIL_SHORT_ENABLED else 'OFF'}\n"
                        f"/rsi(추가진입): {'ON' if RSI_ENABLED else 'OFF'}\n\n"
                        ""
                        "--------------\n"
                        "/report(리포트): /report today|yesterday|YYYY-MM-DD\n"
                        f"open positions: {open_pos} (over_limit={over_limit})\n"
                        "--------------\n"
                        f"{_build_positions_message()}"
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
                            try:
                                admin_id = accounts_db.get_account_id_by_name("admin")
                                if admin_id:
                                    accounts_db.update_account_setting(admin_id, "entry_pct", float(USDT_PER_TRADE))
                            except Exception:
                                pass
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
                            COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
                            EXIT_COOLDOWN_SEC = COOLDOWN_SEC
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
                if (cmd in ("/max_pos", "max_pos")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ max_pos: {MAX_OPEN_POSITIONS}\n사용법: /max_pos 21"
                    else:
                        try:
                            val = int(float(arg))
                            if val <= 0:
                                raise ValueError("non-positive")
                            MAX_OPEN_POSITIONS = val
                            state["_max_open_positions"] = MAX_OPEN_POSITIONS
                            state_dirty = True
                            try:
                                admin_id = accounts_db.get_account_id_by_name("admin")
                                if admin_id:
                                    accounts_db.update_account_setting(admin_id, "max_positions", int(MAX_OPEN_POSITIONS))
                            except Exception:
                                pass
                            resp = f"✅ max_pos set to {MAX_OPEN_POSITIONS}"
                        except Exception:
                            resp = "⛔ 사용법: /max_pos 21"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] max_pos cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
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
                if (cmd in ("/swaggy_atlas_lab_off", "swaggy_atlas_lab_off")) and not responded:
                    raw_arg = text.split(maxsplit=1)[1] if len(text.split(maxsplit=1)) > 1 else ""
                    arg = (raw_arg or "").strip()
                    if not arg or arg.lower() in ("off", "0", "false", "clear", "none", "disable", "disabled"):
                        SWAGGY_ATLAS_LAB_OFF_WINDOWS = ""
                        state["_swaggy_atlas_lab_off_windows"] = ""
                        resp = "✅ swaggy_atlas_lab_off cleared"
                    else:
                        SWAGGY_ATLAS_LAB_OFF_WINDOWS = arg
                        state["_swaggy_atlas_lab_off_windows"] = arg
                        resp = f"✅ swaggy_atlas_lab_off set: {arg}"
                    state_dirty = True
                    ok = _reply(resp)
                    print(f"[telegram] swaggy_atlas_lab_off cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True
                if (cmd in ("/swaggy_atlas_lab_v2", "swaggy_atlas_lab_v2")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        SWAGGY_ATLAS_LAB_V2_ENABLED = True
                        state["_swaggy_atlas_lab_v2_enabled"] = True
                        state_dirty = True
                        resp = "✅ swaggy_atlas_lab_v2 ON (hard 모드)"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        SWAGGY_ATLAS_LAB_V2_ENABLED = False
                        state["_swaggy_atlas_lab_v2_enabled"] = False
                        state_dirty = True
                        resp = "⛔ swaggy_atlas_lab_v2 OFF"
                    else:
                        resp = (
                            f"ℹ️ swaggy_atlas_lab_v2 상태: {'ON' if SWAGGY_ATLAS_LAB_V2_ENABLED else 'OFF'}\n"
                            "사용법: /swaggy_atlas_lab_v2 on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_atlas_lab_v2 cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/swaggy_atlas_lab_v2_off", "swaggy_atlas_lab_v2_off")) and not responded:
                    raw_arg = text.split(maxsplit=1)[1] if len(text.split(maxsplit=1)) > 1 else ""
                    arg = (raw_arg or "").strip()
                    if not arg or arg.lower() in ("off", "0", "false", "clear", "none", "disable", "disabled"):
                        SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS = ""
                        state["_swaggy_atlas_lab_v2_off_windows"] = ""
                        resp = "✅ swaggy_atlas_lab_v2_off cleared"
                    else:
                        SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS = arg
                        state["_swaggy_atlas_lab_v2_off_windows"] = arg
                        resp = f"✅ swaggy_atlas_lab_v2_off set: {arg}"
                    state_dirty = True
                    ok = _reply(resp)
                    print(f"[telegram] swaggy_atlas_lab_v2_off cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True
                if (cmd in ("/swaggy_no_atlas", "swaggy_no_atlas")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        SWAGGY_NO_ATLAS_ENABLED = True
                        state["_swaggy_no_atlas_enabled"] = True
                        state_dirty = True
                        resp = "✅ swaggy_no_atlas ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        SWAGGY_NO_ATLAS_ENABLED = False
                        state["_swaggy_no_atlas_enabled"] = False
                        state_dirty = True
                        resp = "⛔ swaggy_no_atlas OFF"
                    else:
                        resp = (
                            f"ℹ️ swaggy_no_atlas 상태: {'ON' if SWAGGY_NO_ATLAS_ENABLED else 'OFF'}\n"
                            "사용법: /swaggy_no_atlas on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_no_atlas cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/adv_trend", "adv_trend")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        ADV_TREND_ENABLED = True
                        state["_adv_trend_enabled"] = True
                        state_dirty = True
                        resp = "✅ adv_trend ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        ADV_TREND_ENABLED = False
                        state["_adv_trend_enabled"] = False
                        state_dirty = True
                        resp = "⛔ adv_trend OFF"
                    else:
                        resp = (
                            f"ℹ️ adv_trend 상태: {'ON' if ADV_TREND_ENABLED else 'OFF'}\n"
                            "사용법: /adv_trend on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] adv_trend cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/loss_hedge_engine", "loss_hedge_engine")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        LOSS_HEDGE_ENGINE_ENABLED = True
                        state["_loss_hedge_engine_enabled"] = True
                        state_dirty = True
                        resp = "✅ 손실방지엔진 ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        LOSS_HEDGE_ENGINE_ENABLED = False
                        state["_loss_hedge_engine_enabled"] = False
                        state_dirty = True
                        resp = "⛔ 손실방지엔진 OFF"
                    else:
                        resp = (
                            f"ℹ️ 손실방지엔진 상태: {'ON' if LOSS_HEDGE_ENGINE_ENABLED else 'OFF'}\n"
                            "사용법: /loss_hedge_engine on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] loss_hedge_engine cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/loss_hedge_interval", "loss_hedge_interval")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("status", "help"):
                        resp = f"ℹ️ loss_hedge_interval: {LOSS_HEDGE_INTERVAL_MIN}분\n사용법: /loss_hedge_interval 60"
                    else:
                        try:
                            val = int(float(arg))
                            if val < 0:
                                raise ValueError("negative")
                            LOSS_HEDGE_INTERVAL_MIN = val
                            state["_loss_hedge_interval_min"] = LOSS_HEDGE_INTERVAL_MIN
                            state_dirty = True
                            resp = f"✅ loss_hedge_interval set to {LOSS_HEDGE_INTERVAL_MIN}분"
                        except Exception:
                            resp = "⛔ 사용법: /loss_hedge_interval 60"
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] loss_hedge_interval cmd 처리 ({arg}) send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/swaggy_no_atlas_off", "swaggy_no_atlas_off")) and not responded:
                    raw_arg = text.split(maxsplit=1)[1] if len(text.split(maxsplit=1)) > 1 else ""
                    arg = (raw_arg or "").strip()
                    if not arg or arg.lower() in ("off", "0", "false", "clear", "none", "disable", "disabled"):
                        SWAGGY_NO_ATLAS_OFF_WINDOWS = ""
                        state["_swaggy_no_atlas_off_windows"] = ""
                        resp = "✅ swaggy_no_atlas_off cleared"
                    else:
                        SWAGGY_NO_ATLAS_OFF_WINDOWS = arg
                        state["_swaggy_no_atlas_off_windows"] = arg
                        resp = f"✅ swaggy_no_atlas_off set: {arg}"
                    state_dirty = True
                    ok = _reply(resp)
                    print(f"[telegram] swaggy_no_atlas_off cmd 처리 send={'ok' if ok else 'fail'}")
                    responded = True
                if (cmd in ("/swaggy_no_atlas_overext", "swaggy_no_atlas_overext")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN = float(val)
                            state["_swaggy_no_atlas_overext_min"] = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN
                            state_dirty = True
                            resp = f"✅ swaggy_no_atlas_overext set: {SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN:.2f}"
                        except Exception:
                            resp = "⛔ 사용법: /swaggy_no_atlas_overext -0.70"
                    else:
                        resp = (
                            f"ℹ️ swaggy_no_atlas_overext: {SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN:.2f} "
                            f"({ 'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF' })\n"
                            "사용법: /swaggy_no_atlas_overext -0.70"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_no_atlas_overext cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/swaggy_no_atlas_overext_on", "swaggy_no_atlas_overext_on")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else "status"
                    resp = None
                    if arg in ("on", "1", "true", "enable", "enabled"):
                        SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED = True
                        state["_swaggy_no_atlas_overext_min_enabled"] = True
                        state_dirty = True
                        resp = "✅ swaggy_no_atlas_overext_on: ON"
                    elif arg in ("off", "0", "false", "disable", "disabled"):
                        SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED = False
                        state["_swaggy_no_atlas_overext_min_enabled"] = False
                        state_dirty = True
                        resp = "⛔ swaggy_no_atlas_overext_on: OFF"
                    else:
                        resp = (
                            f"ℹ️ swaggy_no_atlas_overext_on: {'ON' if SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED else 'OFF'}\n"
                            "사용법: /swaggy_no_atlas_overext_on on|off|status"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_no_atlas_overext_on cmd 처리 send={'ok' if ok else 'fail'}")
                        responded = True
                if (cmd in ("/swaggy_d1_overext", "swaggy_d1_overext")) and not responded:
                    parts = lower.split()
                    arg = parts[1] if len(parts) >= 2 else ""
                    resp = None
                    if arg:
                        try:
                            val = float(arg)
                            if val <= 0:
                                raise ValueError("non-positive")
                            SWAGGY_D1_OVEREXT_ATR_MULT = float(val)
                            state["_swaggy_d1_overext_atr_mult"] = SWAGGY_D1_OVEREXT_ATR_MULT
                            state_dirty = True
                            resp = f"✅ swaggy_d1_overext set: {SWAGGY_D1_OVEREXT_ATR_MULT:.2f}"
                        except Exception:
                            resp = "⛔ 사용법: /swaggy_d1_overext 1.2"
                    else:
                        resp = (
                            f"ℹ️ swaggy_d1_overext: {SWAGGY_D1_OVEREXT_ATR_MULT:.2f}\n"
                            "사용법: /swaggy_d1_overext 1.2"
                        )
                    if resp:
                        ok = _reply(resp)
                        print(f"[telegram] swaggy_d1_overext cmd 처리 send={'ok' if ok else 'fail'}")
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
                _sync_admin_settings_from_state(state)
            except Exception:
                pass
    except Exception as e:
        print("[telegram] cmd error:", e)

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _adv_atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()

def _adv_mfi(df: pd.DataFrame, length: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"]
    tp_diff = tp.diff()
    pos_mf = mf.where(tp_diff > 0, 0.0)
    neg_mf = mf.where(tp_diff < 0, 0.0).abs()
    pos_sum = pos_mf.rolling(length).sum()
    neg_sum = neg_mf.rolling(length).sum()
    ratio = pos_sum / neg_sum.replace(0, float("nan"))
    mfi = 100 - (100 / (1 + ratio))
    return mfi.fillna(0.0)

def _adv_adx(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float("nan"))) * 100
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx.fillna(0.0)

def _adv_supertrend(df: pd.DataFrame, atr_len: int, mult: float) -> tuple[pd.Series, pd.Series]:
    atr = _adv_atr(df, atr_len)
    hl2 = (df["high"] + df["low"]) / 2.0
    upper = hl2 + (mult * atr)
    lower = hl2 - (mult * atr)
    final_upper = upper.copy()
    final_lower = lower.copy()
    trend = pd.Series(index=df.index, dtype="int")
    for i in range(len(df)):
        if i == 0:
            trend.iloc[i] = 1
            continue
        if upper.iloc[i] < final_upper.iloc[i - 1] or df["close"].iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]
        if lower.iloc[i] > final_lower.iloc[i - 1] or df["close"].iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]
        if trend.iloc[i - 1] == 1:
            trend.iloc[i] = -1 if df["close"].iloc[i] < final_lower.iloc[i] else 1
        else:
            trend.iloc[i] = 1 if df["close"].iloc[i] > final_upper.iloc[i] else -1
    st_line = pd.Series(index=df.index, dtype="float")
    for i in range(len(df)):
        st_line.iloc[i] = final_lower.iloc[i] if trend.iloc[i] == 1 else final_upper.iloc[i]
    return st_line, trend

def _adv_rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)

def _adv_find_pivots(series: pd.Series, lookback: int, mode: str) -> list:
    if series.empty or lookback <= 0:
        return []
    pivots = []
    n = len(series)
    end = n - lookback
    for i in range(lookback, end):
        window = series.iloc[i - lookback : i + lookback + 1]
        if window.empty:
            continue
        val = series.iloc[i]
        if mode == "low":
            if val == window.min():
                pivots.append(i)
        else:
            if val == window.max():
                pivots.append(i)
    return pivots

def _adv_divergence(df: pd.DataFrame, rsi_series: pd.Series, side: str, lookback: int) -> bool:
    if df.empty or rsi_series.empty:
        return False
    side_key = (side or "").upper()
    if side_key == "LONG":
        pivots = _adv_find_pivots(df["low"], lookback, "low")
    else:
        pivots = _adv_find_pivots(df["high"], lookback, "high")
    if len(pivots) < 2:
        return False
    i1, i2 = pivots[-2], pivots[-1]
    if side_key == "LONG":
        price_ok = df["low"].iloc[i2] < df["low"].iloc[i1]
        rsi_ok = rsi_series.iloc[i2] > rsi_series.iloc[i1]
    else:
        price_ok = df["high"].iloc[i2] > df["high"].iloc[i1]
        rsi_ok = rsi_series.iloc[i2] < rsi_series.iloc[i1]
    return bool(price_ok and rsi_ok)

def _adv_eval_15m(df_15m: pd.DataFrame, df_4h: pd.DataFrame) -> Optional[dict]:
    if df_15m.empty or df_4h.empty:
        return None
    if len(df_15m) < max(ADV_TREND_SUPER_ATR_LEN + 5, ADV_TREND_ADX_LEN + 5, ADV_TREND_MFI_LEN + 5):
        return None
    if len(df_4h) < ADV_TREND_EMA_LEN:
        return None
    try:
        ema200 = ema(df_4h["close"], ADV_TREND_EMA_LEN).iloc[-1]
        mfi_val = _adv_mfi(df_15m, ADV_TREND_MFI_LEN).iloc[-1]
        adx_val = _adv_adx(df_15m, ADV_TREND_ADX_LEN).iloc[-1]
        st_line, st_trend = _adv_supertrend(df_15m, ADV_TREND_SUPER_ATR_LEN, ADV_TREND_SUPER_MULT)
        st_px = float(st_line.iloc[-1])
        trend_dir = int(st_trend.iloc[-1])
        close_px = float(df_15m["close"].iloc[-1])
        atr_val = float(_adv_atr(df_15m, ADV_TREND_SUPER_ATR_LEN).iloc[-1])
    except Exception:
        return None
    return {
        "ema200": float(ema200),
        "mfi": float(mfi_val),
        "adx": float(adx_val),
        "st_px": st_px,
        "trend_dir": trend_dir,
        "close_px": close_px,
        "atr": atr_val,
        "ts": int(df_15m["ts"].iloc[-1]) if "ts" in df_15m.columns else None,
    }

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

def _parse_onboard_date_ms(info: dict) -> Optional[int]:
    if not isinstance(info, dict):
        return None
    for key in ("onboardDate", "listingDate", "listDate", "onboardTime"):
        if key in info:
            try:
                val = int(info.get(key))
            except Exception:
                val = None
            if isinstance(val, int) and val > 0:
                return val
    return None

def _is_symbol_old_enough(market: dict, min_days: float) -> bool:
    info = market.get("info") if isinstance(market, dict) else None
    onboard_ms = _parse_onboard_date_ms(info or {})
    if not onboard_ms:
        return True
    age_days = (time.time() * 1000.0 - float(onboard_ms)) / (1000.0 * 60 * 60 * 24)
    return age_days >= min_days

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
    skipped_new = 0
    for m in markets.values():
        if m.get("swap") and m.get("linear") and m.get("settle") == "USDT" and m.get("active", True):
            if not _is_symbol_old_enough(m, MIN_LISTING_AGE_DAYS):
                skipped_new += 1
                continue
            symbols.append(m["symbol"])
    if skipped_new:
        print(f"[초기화] 신규 상장 필터: {skipped_new}개 제외 (상장 {MIN_LISTING_AGE_DAYS:.0f}일 미만)")
    return sorted(list(set(symbols)))

def load_state() -> Dict[str, dict]:
    path = _STATE_FILE_OVERRIDE or STATE_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, dict]) -> None:
    path = _STATE_FILE_OVERRIDE or STATE_FILE
    if not ENGINE_WRITE_STATE:
        return
    disk = None
    try:
        with open(path, "r", encoding="utf-8") as f:
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
                "_engine_exit_overrides",
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
                "_swaggy_enabled",
                "_swaggy_atlas_lab_enabled",
                "_swaggy_atlas_lab_v2_enabled",
                "_swaggy_no_atlas_enabled",
                "_swaggy_atlas_lab_off_windows",
                "_swaggy_atlas_lab_v2_off_windows",
                "_swaggy_no_atlas_off_windows",
                "_swaggy_no_atlas_overext_min",
                "_swaggy_d1_overext_atr_mult",
                "_loss_hedge_engine_enabled",
                "_loss_hedge_interval_min",
                "_dtfx_enabled",
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
    base_dir = os.path.dirname(path) or "."
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with STATE_SAVE_LOCK:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state_snapshot, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)


def load_state_from(path: str) -> Dict[str, dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state_to(state: Dict[str, dict], path: str) -> None:
    disk = None
    try:
        with open(path, "r", encoding="utf-8") as f:
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
                "_swaggy_enabled",
                "_swaggy_atlas_lab_enabled",
                "_dtfx_enabled",
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
    base_dir = os.path.dirname(path) or "."
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with STATE_SAVE_LOCK:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state_snapshot, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)

ACCOUNT_CONTEXTS: List[AccountContext] = []


def _coerce_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(int(val))
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return False


def _build_account_contexts() -> List[AccountContext]:
    accounts_db.ensure_default_account("admin")
    accounts = accounts_db.list_active_accounts()
    contexts: List[AccountContext] = []
    position_mode = os.getenv("POSITION_MODE", "hedge").lower().strip()
    admin_account_id = None
    for acct in accounts:
        if str(acct.get("name")) == "admin":
            admin_account_id = int(acct["id"])
            break
    admin_settings_raw = accounts_db.get_account_settings(admin_account_id) if admin_account_id else {}
    for acct in accounts:
        account_id = int(acct["id"])
        # 모든 계정이 관리자 기준 설정을 사용하도록 고정
        settings_raw = admin_settings_raw
        defaults = accounts_db.DEFAULT_SETTINGS
        def _get_setting(key: str, default_val: Any) -> Any:
            if key in settings_raw and settings_raw.get(key) is not None:
                return settings_raw.get(key)
            return default_val
        settings = AccountSettings(
            entry_pct=float(_get_setting("entry_pct", defaults["entry_pct"])),
            dry_run=_coerce_bool(_get_setting("dry_run", defaults["dry_run"])),
            auto_exit=_coerce_bool(_get_setting("auto_exit", defaults["auto_exit"])),
            max_positions=int(_get_setting("max_positions", defaults["max_positions"])),
            leverage=int(_get_setting("leverage", defaults["leverage"])),
            margin_mode=str(_get_setting("margin_mode", defaults["margin_mode"])),
            exit_cooldown_h=float(_get_setting("exit_cooldown_h", defaults["exit_cooldown_h"])),
            long_tp_pct=float(_get_setting("long_tp_pct", defaults["long_tp_pct"])),
            long_sl_pct=float(_get_setting("long_sl_pct", defaults["long_sl_pct"])),
            short_tp_pct=float(_get_setting("short_tp_pct", defaults["short_tp_pct"])),
            short_sl_pct=float(_get_setting("short_sl_pct", defaults["short_sl_pct"])),
            dca_enabled=_coerce_bool(_get_setting("dca_enabled", defaults["dca_enabled"])),
            dca_pct=float(_get_setting("dca_pct", defaults["dca_pct"])),
            dca1_pct=float(_get_setting("dca1_pct", defaults["dca1_pct"])),
            dca2_pct=float(_get_setting("dca2_pct", defaults["dca2_pct"])),
            dca3_pct=float(_get_setting("dca3_pct", defaults["dca3_pct"])),
        )
        bot = accounts_db.get_telegram_bot(account_id)
        telegram = None
        if bot and bot.get("bot_token") and bot.get("chat_id"):
            telegram = TelegramClient(bot_token=str(bot.get("bot_token")), chat_id=str(bot.get("chat_id")))
        state_path = STATE_FILE if str(acct.get("name")) == "admin" else f"state_{account_id}.json"
        executor = AccountExecutor(
            api_key=str(acct.get("api_key") or ""),
            api_secret=str(acct.get("api_secret") or ""),
            dry_run=settings.dry_run,
            position_mode=position_mode,
            default_leverage=settings.leverage,
            base_entry_usdt=BASE_ENTRY_USDT,
            dca_enabled=settings.dca_enabled,
            dca_pct=settings.dca_pct,
            dca_first_pct=settings.dca1_pct,
            dca_second_pct=settings.dca2_pct,
            dca_third_pct=settings.dca3_pct,
            account_name=str(acct.get("name") or account_id),
        )
        contexts.append(
            AccountContext(
                account_id=account_id,
                name=str(acct.get("name") or account_id),
                api_key=str(acct.get("api_key") or ""),
                api_secret=str(acct.get("api_secret") or ""),
                settings=settings,
                executor=executor,
                telegram=telegram,
                state_path=state_path,
                meta={"db": acct},
            )
        )
    return contexts


def _pick_admin_context(
    contexts: List[AccountContext],
    prev_admin_name: Optional[str] = None,
) -> Optional[AccountContext]:
    if not contexts:
        return None
    for ctx in contexts:
        if str(ctx.name) == "admin":
            return ctx
    if prev_admin_name:
        for ctx in contexts:
            if str(ctx.name) == str(prev_admin_name):
                return ctx
    return contexts[0]


def _format_account_summary(contexts: List[AccountContext], admin_ctx: Optional[AccountContext]) -> str:
    names = [str(c.name) for c in contexts]
    admin_name = admin_ctx.name if admin_ctx else "none"
    follower_names = [str(c.name) for c in contexts if c is not admin_ctx] if contexts else []
    base = f"active={len(contexts)} admin={admin_name} followers={len(follower_names)}"
    if names:
        base += " names=" + ",".join(names)
    return base


def _reload_account_contexts(reason: str = "runtime") -> Dict[str, Any]:
    global ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT, FOLLOWER_CONTEXTS
    global _LAST_ACCOUNT_REFRESH_TS
    prev_admin_name = ADMIN_ACCOUNT_CONTEXT.name if ADMIN_ACCOUNT_CONTEXT else None
    try:
        new_contexts = _build_account_contexts()
    except Exception as e:
        print(f"[accounts] reload failed ({reason}): {e}")
        return {
            "ok": False,
            "reason": reason,
            "error": str(e),
            "summary": _format_account_summary(ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT),
        }
    if not new_contexts:
        print(f"[accounts] reload empty ({reason}); keeping current")
        return {
            "ok": False,
            "reason": reason,
            "error": "empty",
            "summary": _format_account_summary(ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT),
        }
    new_admin = _pick_admin_context(new_contexts, prev_admin_name=prev_admin_name)
    new_followers = [c for c in new_contexts if c is not new_admin]
    ACCOUNT_CONTEXTS = new_contexts
    ADMIN_ACCOUNT_CONTEXT = new_admin
    FOLLOWER_CONTEXTS = new_followers
    _LAST_ACCOUNT_REFRESH_TS = time.time()
    db_path = getattr(accounts_db, "DB_PATH", "unknown")
    summary = _format_account_summary(ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT)
    print(f"[accounts] reload ok ({reason}) db={db_path} {summary}")
    if len(ACCOUNT_CONTEXTS) > 1 and not FOLLOWER_CONTEXTS:
        print(f"[accounts] warning: followers empty with multiple accounts ({summary})")
    return {
        "ok": True,
        "reason": reason,
        "summary": summary,
        "db_path": db_path,
    }


def _maybe_refresh_accounts(now_ts: Optional[float] = None, reason: str = "periodic") -> None:
    global _LAST_ACCOUNT_REFRESH_TS
    if now_ts is None:
        now_ts = time.time()
    if ACCOUNT_REFRESH_SEC <= 0:
        return
    if (now_ts - float(_LAST_ACCOUNT_REFRESH_TS or 0.0)) < ACCOUNT_REFRESH_SEC:
        return
    _reload_account_contexts(reason=reason)


@contextmanager
def _use_account_context(account_ctx: Optional[AccountContext]):
    prev_client = getattr(_THREAD_TG, "client", None)
    prev_state_path = _STATE_FILE_OVERRIDE
    if account_ctx and account_ctx.telegram:
        _THREAD_TG.client = account_ctx.telegram
    else:
        _THREAD_TG.client = None
    if not account_ctx:
        try:
            yield
        finally:
            _THREAD_TG.client = prev_client
        return
    settings = account_ctx.settings
    exec_exchange_prev = None
    if executor_mod:
        exec_exchange_prev = getattr(executor_mod, "exchange", None)
    globals_backup = {
        "USDT_PER_TRADE": USDT_PER_TRADE,
        "LEVERAGE": LEVERAGE,
        "MARGIN_MODE": MARGIN_MODE,
        "MAX_OPEN_POSITIONS": MAX_OPEN_POSITIONS,
        "AUTO_EXIT_ENABLED": AUTO_EXIT_ENABLED,
        "AUTO_EXIT_LONG_TP_PCT": AUTO_EXIT_LONG_TP_PCT,
        "AUTO_EXIT_LONG_SL_PCT": AUTO_EXIT_LONG_SL_PCT,
        "AUTO_EXIT_SHORT_TP_PCT": AUTO_EXIT_SHORT_TP_PCT,
        "AUTO_EXIT_SHORT_SL_PCT": AUTO_EXIT_SHORT_SL_PCT,
        "DCA_ENABLED": DCA_ENABLED,
        "DCA_PCT": DCA_PCT,
        "DCA_FIRST_PCT": DCA_FIRST_PCT,
        "DCA_SECOND_PCT": DCA_SECOND_PCT,
        "DCA_THIRD_PCT": DCA_THIRD_PCT,
        "EXIT_COOLDOWN_HOURS": EXIT_COOLDOWN_HOURS,
        "EXIT_COOLDOWN_SEC": EXIT_COOLDOWN_SEC,
        "LIVE_TRADING": LIVE_TRADING,
        "BOT_TOKEN": BOT_TOKEN,
        "CHAT_ID": CHAT_ID,
        "CHAT_ID_RUNTIME": CHAT_ID_RUNTIME,
    }
    try:
        globals()["_STATE_FILE_OVERRIDE"] = account_ctx.state_path
        globals()["USDT_PER_TRADE"] = float(settings.entry_pct)
        globals()["LEVERAGE"] = int(settings.leverage)
        globals()["MARGIN_MODE"] = str(settings.margin_mode)
        globals()["MAX_OPEN_POSITIONS"] = int(settings.max_positions)
        globals()["AUTO_EXIT_ENABLED"] = bool(settings.auto_exit)
        globals()["AUTO_EXIT_LONG_TP_PCT"] = float(settings.long_tp_pct)
        globals()["AUTO_EXIT_LONG_SL_PCT"] = float(settings.long_sl_pct)
        globals()["AUTO_EXIT_SHORT_TP_PCT"] = float(settings.short_tp_pct)
        globals()["AUTO_EXIT_SHORT_SL_PCT"] = float(settings.short_sl_pct)
        globals()["DCA_ENABLED"] = bool(settings.dca_enabled)
        globals()["DCA_PCT"] = float(settings.dca_pct)
        globals()["DCA_FIRST_PCT"] = float(settings.dca1_pct)
        globals()["DCA_SECOND_PCT"] = float(settings.dca2_pct)
        globals()["DCA_THIRD_PCT"] = float(settings.dca3_pct)
        globals()["EXIT_COOLDOWN_HOURS"] = float(settings.exit_cooldown_h)
        globals()["EXIT_COOLDOWN_SEC"] = int(float(settings.exit_cooldown_h) * 3600)
        globals()["LIVE_TRADING"] = not bool(settings.dry_run)
        if account_ctx.telegram:
            globals()["BOT_TOKEN"] = account_ctx.telegram.bot_token
            globals()["CHAT_ID"] = account_ctx.telegram.chat_id
            globals()["CHAT_ID_RUNTIME"] = account_ctx.telegram.chat_id
        else:
            globals()["BOT_TOKEN"] = ""
            globals()["CHAT_ID"] = ""
            globals()["CHAT_ID_RUNTIME"] = ""
        if executor_mod:
            executor_mod.exchange = account_ctx.executor.ctx.exchange
            executor_mod.DCA_ENABLED = bool(settings.dca_enabled)
            executor_mod.DCA_PCT = float(settings.dca_pct)
            executor_mod.DCA_FIRST_PCT = float(settings.dca1_pct)
            executor_mod.DCA_SECOND_PCT = float(settings.dca2_pct)
            executor_mod.DCA_THIRD_PCT = float(settings.dca3_pct)
        yield
    finally:
        for key, value in globals_backup.items():
            globals()[key] = value
        if executor_mod:
            executor_mod.exchange = exec_exchange_prev
        globals()["_STATE_FILE_OVERRIDE"] = prev_state_path
        _THREAD_TG.client = prev_client

def run():
    # 전역 백오프 변수는 run 스코프에서 재할당 하므로 선 선언 필요
    global GLOBAL_BACKOFF_UNTIL, _BACKOFF_SECS, RATE_LIMIT_LOG_TS, _LAST_ACCOUNT_REFRESH_TS
    global TOTAL_CYCLES, TOTAL_ELAPSED, TOTAL_REST_CALLS, TOTAL_429_COUNT
    global MANAGE_LOOP_ENABLED, MANAGE_WS_MODE
    _install_error_hooks()
    print("[시작] RSI 스캐너 초기화 중...")
    symbols = get_symbols()
    print(f"[초기화] {len(symbols)}개 심볼 로드됨")
    state = load_state()
    global ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT, FOLLOWER_CONTEXTS
    try:
        ACCOUNT_CONTEXTS = _build_account_contexts()
        if ACCOUNT_CONTEXTS:
            db_path = getattr(accounts_db, "DB_PATH", "unknown")
            print(f"[accounts] active={len(ACCOUNT_CONTEXTS)} default=admin db={db_path}")
            print(f"[accounts] names={','.join([str(a.name) for a in ACCOUNT_CONTEXTS])}")
        else:
            print("[accounts] no active accounts in DB (fallback to env default)")
    except Exception as e:
        db_path = getattr(accounts_db, "DB_PATH", "unknown")
        print(f"[accounts] load failed: {e} db={db_path}")
        ACCOUNT_CONTEXTS = []
    if ACCOUNT_CONTEXTS:
        ADMIN_ACCOUNT_CONTEXT = _pick_admin_context(ACCOUNT_CONTEXTS)
        FOLLOWER_CONTEXTS = [a for a in ACCOUNT_CONTEXTS if a is not ADMIN_ACCOUNT_CONTEXT]
        if FOLLOWER_CONTEXTS:
            print(f"[accounts] followers={len(FOLLOWER_CONTEXTS)}")
        elif len(ACCOUNT_CONTEXTS) > 1:
            print(f"[accounts] warning: followers empty with multiple accounts ({_format_account_summary(ACCOUNT_CONTEXTS, ADMIN_ACCOUNT_CONTEXT)})")
        with _use_account_context(ADMIN_ACCOUNT_CONTEXT):
            state = load_state()
        state["_symbols"] = symbols
    else:
        ADMIN_ACCOUNT_CONTEXT = None
        FOLLOWER_CONTEXTS = []
        state["_symbols"] = symbols
    print(f"[초기화] 상태 파일 로드: {len(state)}개 심볼")
    state["_symbols"] = symbols
    state["_startup_ts"] = time.time()
    try:
        cycle_cache.set_fetcher(lambda sym, tf, limit: _fetch_ohlcv_with_retry(exchange, sym, tf, limit))
    except Exception:
        pass
    global swaggy_engine, swaggy_atlas_lab_engine, swaggy_atlas_lab_v2_engine, swaggy_no_atlas_engine
    global atlas_engine, atlas_swaggy_cfg, dtfx_engine, div15m_engine, div15m_short_engine, atlas_rs_fail_short_engine
    swaggy_engine = SwaggyEngine() if SwaggyEngine else None
    swaggy_atlas_lab_engine = SwaggyAtlasLabEngine() if SwaggyAtlasLabEngine else None
    swaggy_atlas_lab_v2_engine = SwaggyAtlasLabV2Engine() if SwaggyAtlasLabV2Engine else None
    swaggy_no_atlas_engine = SwaggyNoAtlasEngine() if SwaggyNoAtlasEngine else None
    atlas_engine = None
    atlas_swaggy_cfg = None
    global rsi_engine
    rsi_engine = RsiEngine() if RsiEngine else None
    _maybe_reload_rsi_config()
    cfg_defaults = _load_rsi_config_defaults()
    if rsi_engine and cfg_defaults:
        rsi_engine.config = cfg_defaults
    div15m_engine = Div15mLongEngine() if Div15mLongEngine else None
    div15m_short_engine = Div15mShortEngine() if Div15mShortEngine else None
    dtfx_engine = DTFXEngine() if DTFXEngine else None
    dtfx_cfg = DTFXConfig(tf_ltf="5m") if DTFXConfig else None
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
    global LIVE_TRADING, LONG_LIVE_TRADING, MAX_OPEN_POSITIONS, SWAGGY_ATLAS_LAB_ENABLED, SWAGGY_ATLAS_LAB_V2_ENABLED, SWAGGY_NO_ATLAS_ENABLED, ADV_TREND_ENABLED, SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN, SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG, SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED, SWAGGY_D1_OVEREXT_ATR_MULT, SATURDAY_TRADE_ENABLED, DTFX_ENABLED, ATLAS_RS_FAIL_SHORT_ENABLED, DIV15M_LONG_ENABLED, DIV15M_SHORT_ENABLED, ONLY_DIV15M_SHORT, RSI_ENABLED, LOSS_HEDGE_ENGINE_ENABLED, LOSS_HEDGE_INTERVAL_MIN
    global SWAGGY_ATLAS_LAB_OFF_WINDOWS, SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS, SWAGGY_NO_ATLAS_OFF_WINDOWS
    global SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK, SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS, SWAGGY_NO_ATLAS_USE_WICK_BREAK
    global SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG, SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK
    global SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    global SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS, SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR
    global SWAGGY_NO_ATLAS_EXPANSION_MULT
    global SWAGGY_NO_ATLAS_DELAY_WAIT_BARS, SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT
    global SWAGGY_NO_ATLAS_DELAY_VOL_MULT, SWAGGY_NO_ATLAS_DELAY_VOL_MA, SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK
    global USDT_PER_TRADE, DCA_ENABLED, DCA_PCT, DCA_FIRST_PCT, DCA_SECOND_PCT, DCA_THIRD_PCT
    global EXIT_COOLDOWN_HOURS, EXIT_COOLDOWN_SEC, COOLDOWN_SEC
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
    if isinstance(state.get("_sat_trade"), bool):
        SATURDAY_TRADE_ENABLED = bool(state.get("_sat_trade"))
    else:
        state["_sat_trade"] = SATURDAY_TRADE_ENABLED
    if isinstance(state.get("_exit_cooldown_hours"), (int, float)):
        EXIT_COOLDOWN_HOURS = float(state.get("_exit_cooldown_hours"))
        COOLDOWN_SEC = int(EXIT_COOLDOWN_HOURS * 3600)
        EXIT_COOLDOWN_SEC = COOLDOWN_SEC
    else:
        state["_exit_cooldown_hours"] = EXIT_COOLDOWN_HOURS
    if isinstance(state.get("_rsi_enabled"), bool):
        RSI_ENABLED = bool(state.get("_rsi_enabled"))
    else:
        state["_rsi_enabled"] = RSI_ENABLED
    if isinstance(state.get("_swaggy_atlas_lab_enabled"), bool):
        SWAGGY_ATLAS_LAB_ENABLED = bool(state.get("_swaggy_atlas_lab_enabled"))
    else:
        state["_swaggy_atlas_lab_enabled"] = SWAGGY_ATLAS_LAB_ENABLED
    if isinstance(state.get("_swaggy_atlas_lab_v2_enabled"), bool):
        SWAGGY_ATLAS_LAB_V2_ENABLED = bool(state.get("_swaggy_atlas_lab_v2_enabled"))
    else:
        state["_swaggy_atlas_lab_v2_enabled"] = SWAGGY_ATLAS_LAB_V2_ENABLED
    if isinstance(state.get("_swaggy_atlas_lab_off_windows"), str):
        SWAGGY_ATLAS_LAB_OFF_WINDOWS = str(state.get("_swaggy_atlas_lab_off_windows") or "")
    else:
        state["_swaggy_atlas_lab_off_windows"] = SWAGGY_ATLAS_LAB_OFF_WINDOWS
    if isinstance(state.get("_swaggy_atlas_lab_v2_off_windows"), str):
        SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS = str(state.get("_swaggy_atlas_lab_v2_off_windows") or "")
    else:
        state["_swaggy_atlas_lab_v2_off_windows"] = SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS
    if isinstance(state.get("_swaggy_no_atlas_enabled"), bool):
        SWAGGY_NO_ATLAS_ENABLED = bool(state.get("_swaggy_no_atlas_enabled"))
    else:
        state["_swaggy_no_atlas_enabled"] = SWAGGY_NO_ATLAS_ENABLED
    if isinstance(state.get("_swaggy_no_atlas_off_windows"), str):
        SWAGGY_NO_ATLAS_OFF_WINDOWS = str(state.get("_swaggy_no_atlas_off_windows") or "")
    else:
        state["_swaggy_no_atlas_off_windows"] = SWAGGY_NO_ATLAS_OFF_WINDOWS
    if isinstance(state.get("_swaggy_no_atlas_overext_min"), (int, float)):
        SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN = float(state.get("_swaggy_no_atlas_overext_min"))
    else:
        state["_swaggy_no_atlas_overext_min"] = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN
    if isinstance(state.get("_swaggy_no_atlas_overext_min_strong"), (int, float)):
        SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG = float(state.get("_swaggy_no_atlas_overext_min_strong"))
    else:
        state["_swaggy_no_atlas_overext_min_strong"] = SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN_STRONG
    if isinstance(state.get("_swaggy_no_atlas_overext_min_enabled"), bool):
        SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED = bool(state.get("_swaggy_no_atlas_overext_min_enabled"))
    else:
        state["_swaggy_no_atlas_overext_min_enabled"] = SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED
    if isinstance(state.get("_swaggy_no_atlas_structure_lookback"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK = max(1, int(state.get("_swaggy_no_atlas_structure_lookback")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_structure_lookback"] = SWAGGY_NO_ATLAS_STRUCTURE_LOOKBACK
    if isinstance(state.get("_swaggy_no_atlas_structure_wait_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS = max(1, int(state.get("_swaggy_no_atlas_structure_wait_bars")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_structure_wait_bars"] = SWAGGY_NO_ATLAS_STRUCTURE_WAIT_BARS
    if isinstance(state.get("_swaggy_no_atlas_use_wick_break"), bool):
        SWAGGY_NO_ATLAS_USE_WICK_BREAK = bool(state.get("_swaggy_no_atlas_use_wick_break"))
    else:
        state["_swaggy_no_atlas_use_wick_break"] = SWAGGY_NO_ATLAS_USE_WICK_BREAK
    if isinstance(state.get("_swaggy_no_atlas_break_margin_strong"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG = float(state.get("_swaggy_no_atlas_break_margin_strong"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_break_margin_strong"] = SWAGGY_NO_ATLAS_BREAK_MARGIN_STRONG
    if isinstance(state.get("_swaggy_no_atlas_break_margin_weak"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK = float(state.get("_swaggy_no_atlas_break_margin_weak"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_break_margin_weak"] = SWAGGY_NO_ATLAS_BREAK_MARGIN_WEAK
    if isinstance(state.get("_swaggy_no_atlas_sw_okn_min_margin"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN = float(state.get("_swaggy_no_atlas_sw_okn_min_margin"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_sw_okn_min_margin"] = SWAGGY_NO_ATLAS_SW_OKN_MIN_MARGIN
    if isinstance(state.get("_swaggy_no_atlas_weak_timeout_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS = max(1, int(state.get("_swaggy_no_atlas_weak_timeout_bars")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_weak_timeout_bars"] = SWAGGY_NO_ATLAS_WEAK_TIMEOUT_BARS
    if isinstance(state.get("_swaggy_no_atlas_weak_no_progress_mfe_atr"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR = float(state.get("_swaggy_no_atlas_weak_no_progress_mfe_atr"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_weak_no_progress_mfe_atr"] = SWAGGY_NO_ATLAS_WEAK_NO_PROGRESS_MFE_ATR
    if isinstance(state.get("_swaggy_no_atlas_expansion_mult"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_EXPANSION_MULT = float(state.get("_swaggy_no_atlas_expansion_mult"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_expansion_mult"] = SWAGGY_NO_ATLAS_EXPANSION_MULT
    if isinstance(state.get("_swaggy_no_atlas_delay_wait_bars"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_WAIT_BARS = max(1, int(state.get("_swaggy_no_atlas_delay_wait_bars")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_delay_wait_bars"] = SWAGGY_NO_ATLAS_DELAY_WAIT_BARS
    if isinstance(state.get("_swaggy_no_atlas_delay_close_pct"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT = float(state.get("_swaggy_no_atlas_delay_close_pct"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_delay_close_pct"] = SWAGGY_NO_ATLAS_DELAY_CLOSE_PCT
    if isinstance(state.get("_swaggy_no_atlas_delay_vol_mult"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_VOL_MULT = float(state.get("_swaggy_no_atlas_delay_vol_mult"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_delay_vol_mult"] = SWAGGY_NO_ATLAS_DELAY_VOL_MULT
    if isinstance(state.get("_swaggy_no_atlas_delay_vol_ma"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_VOL_MA = max(1, int(state.get("_swaggy_no_atlas_delay_vol_ma")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_delay_vol_ma"] = SWAGGY_NO_ATLAS_DELAY_VOL_MA
    if isinstance(state.get("_swaggy_no_atlas_delay_sweep_lookback"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK = max(1, int(state.get("_swaggy_no_atlas_delay_sweep_lookback")))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_delay_sweep_lookback"] = SWAGGY_NO_ATLAS_DELAY_SWEEP_LOOKBACK
    if isinstance(state.get("_swaggy_no_atlas_structure_min_strength"), (int, float)):
        try:
            SWAGGY_NO_ATLAS_STRUCTURE_MIN_STRENGTH = float(state.get("_swaggy_no_atlas_structure_min_strength"))
        except Exception:
            pass
    else:
        state["_swaggy_no_atlas_structure_min_strength"] = SWAGGY_NO_ATLAS_STRUCTURE_MIN_STRENGTH
    if isinstance(state.get("_swaggy_d1_overext_atr_mult"), (int, float)):
        SWAGGY_D1_OVEREXT_ATR_MULT = float(state.get("_swaggy_d1_overext_atr_mult"))
    else:
        state["_swaggy_d1_overext_atr_mult"] = SWAGGY_D1_OVEREXT_ATR_MULT
    if isinstance(state.get("_loss_hedge_engine_enabled"), bool):
        LOSS_HEDGE_ENGINE_ENABLED = bool(state.get("_loss_hedge_engine_enabled"))
    else:
        state["_loss_hedge_engine_enabled"] = LOSS_HEDGE_ENGINE_ENABLED
    if isinstance(state.get("_loss_hedge_interval_min"), (int, float)):
        try:
            LOSS_HEDGE_INTERVAL_MIN = max(0, int(state.get("_loss_hedge_interval_min")))
        except Exception:
            pass
    else:
        state["_loss_hedge_interval_min"] = LOSS_HEDGE_INTERVAL_MIN
    if isinstance(state.get("_dtfx_enabled"), bool):
        DTFX_ENABLED = bool(state.get("_dtfx_enabled"))
    else:
        state["_dtfx_enabled"] = DTFX_ENABLED
    if isinstance(state.get("_swaggy_no_atlas_enabled"), dict):
        state["_swaggy_no_atlas_enabled"] = False
    if isinstance(state.get("_swaggy_atlas_lab_v2_enabled"), dict):
        state["_swaggy_atlas_lab_v2_enabled"] = False
    if isinstance(state.get("_loss_hedge_engine_enabled"), dict):
        state["_loss_hedge_engine_enabled"] = False
    if isinstance(state.get("_atlas_rs_fail_short_enabled"), dict):
        state["_atlas_rs_fail_short_enabled"] = False
    if isinstance(state.get("_atlas_rs_fail_short_universe"), dict):
        state["_atlas_rs_fail_short_universe"] = []
    if isinstance(state.get("_atlas_rs_fail_short_enabled"), bool):
        ATLAS_RS_FAIL_SHORT_ENABLED = bool(state.get("_atlas_rs_fail_short_enabled"))
    else:
        state["_atlas_rs_fail_short_enabled"] = ATLAS_RS_FAIL_SHORT_ENABLED
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
    startup_msg = (
        "✅ RSI 스캐너 시작\n"
        f"auto-exit: {'ON' if AUTO_EXIT_ENABLED else 'OFF'}\n"
        f"live-trading: {'ON' if LIVE_TRADING else 'OFF'}\n"
        "명령: /auto_exit on|off|status, /sat_trade on|off|status, /l_exit_tp n, /l_exit_sl n, /s_exit_tp n, /s_exit_sl n, /engine_exit ENGINE SIDE tp sl, /live on|off|status, /long_live on|off|status, /entry_usdt pct, /dca on|off|status, /dca_pct n, /dca1 n, /dca2 n, /dca3 n, /swaggy_no_atlas_overext n, /swaggy_no_atlas_overext_on on|off|status, /swaggy_d1_overext n, /swaggy_atlas_lab_off windows, /swaggy_atlas_lab_v2_off windows, /swaggy_no_atlas_off windows, /exit_cd_h n, /swaggy_atlas_lab on|off|status, /swaggy_atlas_lab_v2 on|off|status, /swaggy_no_atlas on|off|status, /adv_trend on|off|status, /loss_hedge_engine on|off|status, /loss_hedge_interval n, /rsi on|off|status, /dtfx on|off|status, /atlas_rs_fail_short on|off|status, /max_pos n, /report today|yesterday, /status, /accounts, /reload_accounts"
    )
    if ADMIN_ACCOUNT_CONTEXT:
        with (ADMIN_ACCOUNT_CONTEXT.executor.activate() if ADMIN_ACCOUNT_CONTEXT else nullcontext()):
            with _use_account_context(ADMIN_ACCOUNT_CONTEXT):
                send_telegram(startup_msg)
    else:
        send_telegram(startup_msg)
    print("[시작] 메인 루프 시작")
    manage_thread = None
    # admin-only scan 모드에서는 multi-account라도 manage-loop를 유지
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
    global _LAST_CYCLE_TS_MEM
    while True:
        now_refresh = time.time()
        if ACCOUNT_REFRESH_SEC > 0 and (now_refresh - _LAST_ACCOUNT_REFRESH_TS) >= ACCOUNT_REFRESH_SEC:
            _reload_account_contexts(reason="periodic")
            _LAST_ACCOUNT_REFRESH_TS = now_refresh
        active_accounts = [ADMIN_ACCOUNT_CONTEXT] if ADMIN_ACCOUNT_CONTEXT else [None]
        for account_ctx in active_accounts:
            account_state = state
            with (account_ctx.executor.activate() if account_ctx else nullcontext()):
                with _use_account_context(account_ctx):
                    state = account_state
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
                        skip_keys = None
                        if account_ctx is not None:
                            skip_keys = {
                                "_auto_exit",
                                "_auto_exit_long_tp_pct",
                                "_auto_exit_long_sl_pct",
                                "_auto_exit_short_tp_pct",
                                "_auto_exit_short_sl_pct",
                                "_live_trading",
                                "_long_live",
                                "_entry_usdt",
                                "_dca_enabled",
                                "_dca_pct",
                                "_dca_first_pct",
                                "_dca_second_pct",
                                "_dca_third_pct",
                                "_exit_cooldown_hours",
                            }
                        if account_ctx and str(getattr(account_ctx, "name", "")) == "admin":
                            _refresh_admin_settings_from_db(state)
                        _reload_runtime_settings_from_disk(state, skip_keys=skip_keys)
                        last_cfg_reload_ts = now
                    try:
                        set_dry_run(not LIVE_TRADING)
                    except Exception:
                        pass
                    verified = None
                    try:
                        last_pos_check = _coerce_state_float(state.get("_last_pos_check_ts", 0.0))
                    except Exception:
                        last_pos_check = 0.0
                    force_pos = (now - last_pos_check) >= POS_CHECK_MIN_SEC
                    try:
                        verified = count_open_positions(force=force_pos)
                        if force_pos:
                            state["_last_pos_check_ts"] = now
                    except Exception:
                        verified = None
                    if isinstance(verified, int):
                        state["_active_positions_total"] = int(verified)
                    active_positions_total_est = (
                        verified
                        if isinstance(verified, int)
                        else _count_open_positions_state(state)
                    )
                    if isinstance(active_positions_total_est, int) and active_positions_total_est >= MAX_OPEN_POSITIONS:
                        state["_pos_limit_reached"] = True
                        last_warn = _coerce_state_float(state.get("_pos_limit_skip_ts", 0.0))
                        if (now - last_warn) >= 60:
                            print(f"[제한] 동시 포지션 {active_positions_total_est}/{MAX_OPEN_POSITIONS} → 조회 스킵")
                            state["_pos_limit_skip_ts"] = now
                            _log_off_window_status(state, now, tag="pos_limit")
                        time.sleep(1)
                        continue
                    state["_pos_limit_reached"] = False

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
                    last_cycle_ts = _coerce_state_int(last_cycle_raw)
                    if not last_cycle_ts and isinstance(_LAST_CYCLE_TS_MEM, int) and _LAST_CYCLE_TS_MEM:
                        last_cycle_ts = _LAST_CYCLE_TS_MEM
                    if not isinstance(last_cycle_raw, (int, float, str)):
                        state["_last_cycle_ts"] = last_cycle_ts
                    state["_current_cycle_ts"] = cycle_ts
                    def _fmt_ms_kst(ms: Optional[int]) -> str:
                        if not ms:
                            return "N/A"
                        return _ts_to_kst_str(ms / 1000.0)
                    server_kst = _fmt_ms_kst(server_ms)
                    last_open_kst = _fmt_ms_kst(last_open_ts)
                    prev_open_kst = _fmt_ms_kst(prev_open_ts)
                    cycle_kst = _fmt_ms_kst(cycle_ts)
                    heavy_scan = bool(cycle_ts and cycle_ts != last_cycle_ts)
                    last_cycle_kst = _fmt_ms_kst(last_cycle_ts)
                    if heavy_scan:
                        print(
                            f"[CYCLE] server={server_kst} last_open={last_open_kst} prev_open={prev_open_kst} "
                            f"cycle_ts={cycle_kst} last_cycle_ts={last_cycle_kst} mode=heavy-scan"
                        )
                    else:
                        if _realtime_only_required():
                            print(
                                f"[CYCLE] server={server_kst} last_open={last_open_kst} prev_open={prev_open_kst} "
                                f"cycle_ts={cycle_kst} last_cycle_ts={last_cycle_kst} mode=realtime-only"
                            )
                        else:
                            print(
                                f"[CYCLE] server={server_kst} last_open={last_open_kst} prev_open={prev_open_kst} "
                                f"cycle_ts={cycle_kst} last_cycle_ts={last_cycle_kst} mode=realtime-only skip=heavy-only"
                            )
                    if not heavy_scan:
                        if not _realtime_only_required():
                            time.sleep(REALTIME_CYCLE_SLEEP)
                            continue

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
                            _LAST_CYCLE_TS_MEM = cycle_ts
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
                        shared_top_n = rsi_cfg.universe_top_n if rsi_cfg else 50
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
                    swaggy_universe = []
                    adv_trend_universe = []
                    swaggy_cfg = None
                    swaggy_atlas_lab_cfg = None
                    swaggy_atlas_lab_atlas_cfg = None
                    swaggy_atlas_lab_v2_cfg = None
                    swaggy_atlas_lab_v2_atlas_cfg = None
                    if (
                        (SWAGGY_ENABLED or SWAGGY_ATLAS_LAB_ENABLED or SWAGGY_ATLAS_LAB_V2_ENABLED or SWAGGY_NO_ATLAS_ENABLED)
                        and swaggy_engine
                        and SwaggyConfig
                        and EngineContext
                    ):
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
                    elif SWAGGY_ATLAS_LAB_ENABLED or SWAGGY_ATLAS_LAB_V2_ENABLED or SWAGGY_NO_ATLAS_ENABLED:
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
                    if SWAGGY_NO_ATLAS_ENABLED and (swaggy_cfg is None):
                        if SwaggyNoAtlasConfig:
                            swaggy_cfg = SwaggyNoAtlasConfig()
                    if SWAGGY_ATLAS_LAB_ENABLED and SwaggyAtlasLabConfig and SwaggyAtlasLabAtlasConfig:
                        swaggy_atlas_lab_cfg = SwaggyAtlasLabConfig()
                        swaggy_atlas_lab_atlas_cfg = SwaggyAtlasLabAtlasConfig()
                    if SWAGGY_ATLAS_LAB_V2_ENABLED and SwaggyAtlasLabV2Config and SwaggyAtlasLabV2AtlasConfig:
                        swaggy_atlas_lab_v2_cfg = SwaggyAtlasLabV2Config()
                        swaggy_atlas_lab_v2_atlas_cfg = SwaggyAtlasLabV2AtlasConfig()

                    if ADV_TREND_ENABLED:
                        adv_trend_universe = list(shared_universe)
                        state["_adv_trend_universe"] = adv_trend_universe

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
                                + (swaggy_universe or [])
                                + (adv_trend_universe or [])
                                + (dtfx_universe or [])
                                + (atlas_rs_fail_short_universe or [])
                            )
                        )
                    else:
                        universe_union = list(
                            set(
                                universe_momentum
                                + universe_structure
                                + (swaggy_universe or [])
                                + (adv_trend_universe or [])
                                + (dtfx_universe or [])
                                + (atlas_rs_fail_short_universe or [])
                            )
                        )

                    cached_ex = CachedExchange(exchange)
                    cached_long_ex = CachedExchange(exchange)

                    atlas_cfg = None
                    swaggy_universe_len = len(swaggy_universe) if swaggy_universe else 0
                    swaggy_atlas_lab_universe_len = swaggy_universe_len if SWAGGY_ATLAS_LAB_ENABLED else 0
                    swaggy_atlas_lab_v2_universe_len = swaggy_universe_len if SWAGGY_ATLAS_LAB_V2_ENABLED else 0
                    adv_trend_universe_len = len(adv_trend_universe) if adv_trend_universe else 0
                    dtfx_universe_len = len(dtfx_universe) if dtfx_universe else 0
                    atlas_rs_fail_short_universe_len = len(atlas_rs_fail_short_universe) if atlas_rs_fail_short_universe else 0
                    universe_structure_len = len(universe_structure)
                    universe_union_len = len(universe_union)
                    rsi_ran = bool(run_rsi_short and universe_momentum)
                    div15m_long_ran = bool(run_div15m_long and div15m_universe)
                    div15m_short_ran = bool(run_div15m_short and div15m_short_universe)
                    swaggy_ran = bool(heavy_scan and SWAGGY_ENABLED and swaggy_cfg and swaggy_engine)
                    swaggy_atlas_lab_ran = bool(
                        heavy_scan
                        and SWAGGY_ATLAS_LAB_ENABLED
                        and swaggy_atlas_lab_cfg
                        and swaggy_atlas_lab_atlas_cfg
                        and swaggy_atlas_lab_engine
                        and swaggy_universe
                    )
                    swaggy_atlas_lab_v2_ran = bool(
                        heavy_scan
                        and SWAGGY_ATLAS_LAB_V2_ENABLED
                        and swaggy_atlas_lab_v2_cfg
                        and swaggy_atlas_lab_v2_atlas_cfg
                        and swaggy_atlas_lab_v2_engine
                        and swaggy_universe
                    )
                    adv_trend_ran = bool(heavy_scan and ADV_TREND_ENABLED and adv_trend_universe)
                    dtfx_ran = bool(DTFX_ENABLED and dtfx_engine and dtfx_cfg and dtfx_universe)
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
                    for s in swaggy_universe or []:
                        if s not in slow_symbols_ordered:
                            slow_symbols_ordered.append(s)
                    for s in adv_trend_universe or []:
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
                    if swaggy_cfg:
                        mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
                        mid_plan["1h"] = max(mid_plan.get("1h", 0), int(swaggy_cfg.vp_lookback_1h))
                    if swaggy_atlas_lab_cfg:
                        mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
                        mid_plan["1h"] = max(mid_plan.get("1h", 0), int(swaggy_atlas_lab_cfg.vp_lookback_1h))
                    if swaggy_atlas_lab_v2_cfg:
                        mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
                        mid_plan["1h"] = max(mid_plan.get("1h", 0), int(swaggy_atlas_lab_v2_cfg.vp_lookback_1h))
                    if ADV_TREND_ENABLED:
                        mid_plan["15m"] = max(mid_plan.get("15m", 0), 200)
                    if atlas_rs_fail_short_cfg:
                        mid_plan["15m"] = max(mid_plan.get("15m", 0), int(atlas_rs_fail_short_cfg.ltf_limit))
                    if atlas_cfg:
                        mid_plan["1h"] = max(mid_plan.get("1h", 0), int(atlas_cfg.htf_limit))
                    mid_plan["15m"] = min(mid_plan.get("15m", 0), MID_LIMIT_CAP)
                    if "1h" in mid_plan:
                        mid_plan["1h"] = min(mid_plan.get("1h", 0), MID_LIMIT_CAP)

                    # SLOW TF (4h/1d) - TTL
                    if atlas_cfg:
                        slow_plan["1d"] = max(slow_plan.get("1d", 0), int(getattr(atlas_cfg, "d1_limit", 90)))
                    if swaggy_cfg:
                        slow_plan["4h"] = max(slow_plan.get("4h", 0), 200)
                    if swaggy_atlas_lab_cfg:
                        slow_plan["4h"] = max(slow_plan.get("4h", 0), 200)
                    if swaggy_atlas_lab_v2_cfg:
                        slow_plan["4h"] = max(slow_plan.get("4h", 0), 200)
                    if ADV_TREND_ENABLED:
                        slow_plan["4h"] = max(slow_plan.get("4h", 0), 220)
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



                    if not MANAGE_LOOP_ENABLED and (not MANAGE_WS_MODE) and (not state.get("_manage_ws_mode")):
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
                    swaggy_result = {}
                    swaggy_thread = None
                    swaggy_atlas_lab_result = {}
                    swaggy_atlas_lab_thread = None
                    swaggy_atlas_lab_v2_result = {}
                    swaggy_atlas_lab_v2_thread = None
                    swaggy_no_atlas_result = {}
                    swaggy_no_atlas_thread = None
                    adv_trend_result = {}
                    adv_trend_thread = None
                    dtfx_result = {}
                    dtfx_thread = None
                    atlas_rs_fail_short_result = {}
                    atlas_rs_fail_short_thread = None
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
                    if SWAGGY_ATLAS_LAB_V2_ENABLED and swaggy_atlas_lab_v2_cfg and swaggy_atlas_lab_v2_atlas_cfg and swaggy_atlas_lab_v2_engine:
                        swaggy_atlas_lab_v2_thread = threading.Thread(
                            target=lambda: swaggy_atlas_lab_v2_result.update(
                                _run_swaggy_atlas_lab_v2_cycle(
                                    swaggy_atlas_lab_v2_engine,
                                    swaggy_universe,
                                    cached_ex,
                                    state,
                                    swaggy_atlas_lab_v2_cfg,
                                    swaggy_atlas_lab_v2_atlas_cfg,
                                    active_positions,
                                    send_telegram,
                                )
                            ),
                            daemon=True,
                        )
                        swaggy_atlas_lab_v2_thread.start()
                    if SWAGGY_NO_ATLAS_ENABLED and swaggy_cfg and swaggy_no_atlas_engine:
                        swaggy_no_atlas_thread = threading.Thread(
                            target=lambda: swaggy_no_atlas_result.update(
                                _run_swaggy_no_atlas_cycle(
                                    swaggy_no_atlas_engine,
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
                        swaggy_no_atlas_thread.start()
                    if ADV_TREND_ENABLED:
                        adv_trend_thread = threading.Thread(
                            target=lambda: adv_trend_result.update(
                                _run_adv_trend_cycle(
                                    adv_trend_universe,
                                    cached_ex,
                                    state,
                                    send_telegram,
                                )
                            ),
                            daemon=True,
                        )
                        adv_trend_thread.start()
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

                    if swaggy_thread:
                        swaggy_thread.join()
                    if swaggy_atlas_lab_thread:
                        swaggy_atlas_lab_thread.join()
                    if swaggy_atlas_lab_v2_thread:
                        swaggy_atlas_lab_v2_thread.join()
                    if swaggy_no_atlas_thread:
                        swaggy_no_atlas_thread.join()
                    if adv_trend_thread:
                        adv_trend_thread.join()
                    if dtfx_thread:
                        dtfx_thread.join()
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
                        f"adv_trend={adv_trend_universe_len} dtfx={dtfx_universe_len} "
                        f"arsf={atlas_rs_fail_short_universe_len} union={universe_union_len}"
                    )
                    print(
                        "[engines] rsi=%s(%d) div15m_long=%s(%d) div15m_short=%s(%d) "
                        "adv_trend=%s(%d) swaggy_atlas_lab=%s(%d) swaggy_atlas_lab_v2=%s(%d) dtfx=%s(%d) arsf=%s(%d)"
                        % (
                            "ON" if rsi_ran else "OFF",
                            rsi_universe_len,
                            "ON" if div15m_long_ran else "OFF",
                            div15m_universe_len,
                            "ON" if div15m_short_ran else "OFF",
                            div15m_short_universe_len,
                            "ON" if adv_trend_ran else "OFF",
                            adv_trend_universe_len,
                            "ON" if swaggy_atlas_lab_ran else "OFF",
                            swaggy_atlas_lab_universe_len,
                            "ON" if swaggy_atlas_lab_v2_ran else "OFF",
                            swaggy_atlas_lab_v2_universe_len,
                            "ON" if dtfx_ran else "OFF",
                            dtfx_universe_len,
                            "ON" if atlas_rs_fail_short_ran else "OFF",
                            atlas_rs_fail_short_universe_len,
                        )
                    )
                    if swaggy_atlas_lab_v2_ran:
                        print(
                            "[swaggy_v2] scanned=%d phase_changes=%d hits_long=%d hits_short=%d"
                            % (
                                int(swaggy_atlas_lab_v2_result.get("scanned", 0) or 0),
                                int(swaggy_atlas_lab_v2_result.get("phase_changes", 0) or 0),
                                int(swaggy_atlas_lab_v2_result.get("long_hits", 0) or 0),
                                int(swaggy_atlas_lab_v2_result.get("short_hits", 0) or 0),
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
                    cycle_sleep = CYCLE_SLEEP if heavy_scan else REALTIME_CYCLE_SLEEP
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
                            time.sleep(cycle_sleep)
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
                    time.sleep(cycle_sleep)

if __name__ == "__main__":
    run()
