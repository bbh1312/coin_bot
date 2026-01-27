from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

STATE_FILE = os.path.join(ROOT_DIR, "state.json")
LOG_DIR = os.path.join(ROOT_DIR, "logs", "curr_position_swaggy")
LOSS_HEDGE_ENGINE_KEY = "LOSS_HEDGE_ENGINE"
LOSS_HEDGE_MIN_PCT = -10.0
DEFAULT_TP_PCT = 3.0
DEFAULT_SL_PCT = 30.0
DEFAULT_LEVERAGE = int(os.getenv("LEVERAGE", "10"))
DEFAULT_MARGIN_MODE = os.getenv("MARGIN_MODE", "cross").lower().strip()

from env_loader import load_env
import cycle_cache
from atlas_test.data_feed import fetch_ohlcv
from atlas_test.notifier_telegram import send_message
from engines.curr_position_swaggy.config import CurrPositionSwaggyConfig
from engines.swaggy_atlas_lab_v2.swaggy_signal import SwaggySignalEngine as SwaggyAtlasLabV2Engine
from engines.swaggy_atlas_lab_v2.config import SwaggyConfig as SwaggyAtlasLabV2Config
from engines.swaggy_atlas_lab_v2.config import AtlasConfig as SwaggyAtlasLabV2AtlasConfig
from engines.swaggy_atlas_lab_v2.atlas_eval import evaluate_global_gate, evaluate_local
from engines.swaggy_atlas_lab_v2.policy import apply_policy, AtlasMode
from executor import (
    exchange,
    list_open_position_symbols,
    get_long_position_detail,
    get_short_position_detail,
    get_available_usdt,
)


def _fmt_kst_now() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S KST")

def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    base_dir = os.path.dirname(STATE_FILE) or "."
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = f"{STATE_FILE}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=True)
    os.replace(tmp_path, STATE_FILE)


def _append_log(msg: str) -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        day = datetime.now(tz=timezone(timedelta(hours=9))).strftime("%Y%m%d")
        path = os.path.join(LOG_DIR, f"curr_position_swaggy_{day}.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        return


def _log_kst(msg: str) -> None:
    line = f"{_fmt_kst_now()} {msg}"
    print(line)
    _append_log(line)


def _state_bool(state: dict, key: str, default: bool = False) -> bool:
    val = state.get(key)
    if isinstance(val, bool):
        return val
    return default


def _state_number(state: dict, key: str, default: float) -> float:
    val = state.get(key)
    if isinstance(val, (int, float)):
        return float(val)
    return float(default)


def _loss_hedge_enabled(state: dict) -> bool:
    return _state_bool(state, "_loss_hedge_engine_enabled", False)


def _live_enabled(state: dict, side: str) -> bool:
    side_key = (side or "").upper()
    if side_key == "SHORT":
        return _state_bool(state, "_live_trading", True)
    return _state_bool(state, "_long_live", True)


def _resolve_entry_usdt(entry_pct: float) -> float:
    if entry_pct <= 0:
        return 0.0
    avail = None
    try:
        avail = get_available_usdt()
    except Exception:
        avail = None
    if not isinstance(avail, (int, float)) or avail <= 0:
        if os.getenv("DRY_RUN", "1") == "1":
            try:
                avail = float(os.getenv("SIM_USDT_BALANCE", "1000"))
            except Exception:
                avail = 1000.0
        else:
            return 0.0
    pct = min(float(entry_pct), 100.0)
    return max(0.0, float(avail) * (pct / 100.0))


def _engine_tp_sl(state: dict, side: str) -> Tuple[float, float]:
    tp = DEFAULT_TP_PCT
    sl = DEFAULT_SL_PCT
    overrides = state.get("_engine_exit_overrides")
    if not isinstance(overrides, dict):
        return tp, sl
    key = LOSS_HEDGE_ENGINE_KEY
    entry = overrides.get(key) or overrides.get(key.lower())
    if not isinstance(entry, dict):
        return tp, sl
    side_key = (side or "").upper()
    side_cfg = entry.get(side_key) or entry.get(side_key.lower())
    if isinstance(side_cfg, dict):
        if isinstance(side_cfg.get("tp"), (int, float)):
            tp = float(side_cfg.get("tp"))
        if isinstance(side_cfg.get("sl"), (int, float)):
            sl = float(side_cfg.get("sl"))
    return tp, sl


def _profit_unlev_pct(detail: dict, side: str) -> Optional[float]:
    if not isinstance(detail, dict):
        return None
    entry = detail.get("entry")
    mark = detail.get("mark")
    try:
        entry = float(entry)
        mark = float(mark)
    except Exception:
        return None
    if entry <= 0:
        return None
    side_key = (side or "").upper()
    if side_key == "SHORT":
        return (entry - mark) / entry * 100.0
    return (mark - entry) / entry * 100.0


def _append_trade_log_entry(
    state: dict,
    side: str,
    symbol: str,
    entry_price: Optional[float],
    qty: Optional[float],
    usdt: Optional[float],
    entry_order_id: Optional[str],
) -> None:
    log = state.get("_trade_log")
    if not isinstance(log, list):
        log = []
        state["_trade_log"] = log
    entry_ts = time.time()
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
        "meta": {"reason": "loss_hedge_engine"},
        "engine_label": LOSS_HEDGE_ENGINE_KEY,
    }
    log.append(tr)
    st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
    if not isinstance(st, dict):
        st = {}
    if (side or "").upper() == "LONG":
        st["in_pos_long"] = True
    elif (side or "").upper() == "SHORT":
        st["in_pos_short"] = True
    st["in_pos"] = bool(st.get("in_pos_long") or st.get("in_pos_short"))
    st["last_entry"] = entry_ts
    state[symbol] = st


def _build_entry_message(
    symbol: str,
    side: str,
    entry_px: Optional[float],
    usdt: float,
    loss_pct: Optional[float],
    tp_pct: float,
    sl_pct: float,
    live: bool,
) -> str:
    sym = symbol.replace("/USDT:USDT", "")
    loss_str = f"{loss_pct:.2f}%" if isinstance(loss_pct, (int, float)) else "n/a"
    entry_str = f"{float(entry_px):.6g}" if isinstance(entry_px, (int, float)) else "n/a"
    live_str = "LIVE" if live else "ALERT_ONLY"
    return (
        f"🛡️ 손실방지엔진 진입\n"
        f"{sym}\n"
        f"방향: {_side_kr(side)} ({live_str})\n"
        f"진입가≈{entry_str} (USDT {usdt:.2f})\n"
        f"TP={tp_pct:.2f}% SL={sl_pct:.2f}%\n"
        f"기존포지션 손실={loss_str}"
    )

def _sleep_to_boundary(interval_sec: int) -> None:
    now = time.time()
    next_ts = (int(now // interval_sec) + 1) * interval_sec
    time.sleep(max(1, next_ts - now))


def _build_pos_side_map() -> Dict[str, str]:
    pos = list_open_position_symbols(force=True)
    longs = pos.get("long") or set()
    shorts = pos.get("short") or set()
    symbols = set(longs) | set(shorts)
    out: Dict[str, str] = {}
    for sym in symbols:
        has_long = sym in longs
        has_short = sym in shorts
        if has_long and has_short:
            out[sym] = "LONG+SHORT"
        elif has_long:
            out[sym] = "LONG"
        elif has_short:
            out[sym] = "SHORT"
    return out


def _side_kr(side: str) -> str:
    side = (side or "").upper()
    if side == "LONG":
        return "롱"
    if side == "SHORT":
        return "숏"
    if side == "LONG+SHORT":
        return "롱+숏"
    return side or "N/A"


def _fmt_roi(val: object) -> str:
    if isinstance(val, (int, float)):
        return f"{val:.2f}%"
    return "n/a"


def _pos_roi_text(symbol: str, pos_side: str) -> str:
    pos_side = (pos_side or "").upper()
    if pos_side == "LONG":
        roi = (get_long_position_detail(symbol) or {}).get("roi")
        return _fmt_roi(roi)
    if pos_side == "SHORT":
        roi = (get_short_position_detail(symbol) or {}).get("roi")
        return _fmt_roi(roi)
    if pos_side == "LONG+SHORT":
        roi_l = (get_long_position_detail(symbol) or {}).get("roi")
        roi_s = (get_short_position_detail(symbol) or {}).get("roi")
        return f"롱 {_fmt_roi(roi_l)} / 숏 {_fmt_roi(roi_s)}"
    return "n/a"


def _fmt_entry_px(entry_px: object) -> str:
    if isinstance(entry_px, (int, float)):
        return f"{float(entry_px):.6g}"
    return "n/a"


def _build_message(entries: list[dict]) -> str:
    lines = [
        "[손실방지엔진] 15m 진입 판단",
        f"시간: {_fmt_kst_now()}",
        "----",
    ]
    for item in entries:
        sym = item["symbol"].replace("/USDT:USDT", "")
        pos_side = item["pos_side"]
        roi_text = _pos_roi_text(item["symbol"], pos_side)
        entry_px = item.get("entry_px")
        entry_px_str = f"{float(entry_px):.6g}" if isinstance(entry_px, (int, float)) else "n/a"
        candle_ts = item.get("candle_ts")
        candle_str = "n/a"
        if isinstance(candle_ts, (int, float)) and candle_ts > 0:
            try:
                kst = timezone(timedelta(hours=9))
                candle_dt = datetime.fromtimestamp(float(candle_ts) / 1000.0, tz=kst)
                candle_str = candle_dt.strftime("%Y-%m-%d %H:%M KST")
            except Exception:
                candle_str = "n/a"
        lines.append(
            f"{sym} / { _side_kr(pos_side) } / ROI: {roi_text} : "
            f"{_side_kr(item['side'])} 판단(진입가: {entry_px_str}, 캔들: {candle_str})"
        )
    return "\n".join(lines)


def _set_cache(symbol: str, tf: str, data: list) -> bool:
    if not data:
        return False
    formatted = [[c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"]] for c in data]
    cycle_cache.set_raw(symbol, tf, formatted)
    return True


def _fetch_and_cache(symbol: str, tf: str, limit: int) -> bool:
    ohlcv = fetch_ohlcv(exchange, symbol, tf, limit, drop_last=True)
    return _set_cache(symbol, tf, ohlcv)


def main() -> None:
    load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN_CURR_POSITION", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_CURR_POSITION", "").strip()
    if not token or not chat_id:
        print("[curr-pos-swaggy] missing TELEGRAM_BOT_TOKEN_CURR_POSITION or TELEGRAM_CHAT_ID_CURR_POSITION")
        _append_log("[curr-pos-swaggy] missing TELEGRAM_BOT_TOKEN_CURR_POSITION or TELEGRAM_CHAT_ID_CURR_POSITION")
        return

    cfg = CurrPositionSwaggyConfig()
    swaggy_cfg = SwaggyAtlasLabV2Config()
    swaggy_engine = SwaggyAtlasLabV2Engine(swaggy_cfg)
    atlas_cfg = SwaggyAtlasLabV2AtlasConfig()
    # Match SwaggyLab v2 live engine exception_min_score
    atlas_cfg.exception_min_score = 2
    ltf_limit = max(int(swaggy_cfg.ltf_limit), 120)
    mtf_limit = 200
    htf_limit = max(int(swaggy_cfg.vp_lookback_1h), 120)
    htf2_limit = 200
    d1_limit = 120
    tf_3m_limit = 30
    exchange.load_markets()

    initial_state = _load_state()
    initial_interval = int(_state_number(initial_state, "_loss_hedge_interval_min", 15.0))
    if initial_interval > 0:
        cfg.interval_sec = max(60, initial_interval * 60)
        print(f"[curr-pos-swaggy] start ({initial_interval}m)")
        _append_log(f"[curr-pos-swaggy] start ({initial_interval}m)")
    else:
        print("[curr-pos-swaggy] start (15m)")
        _append_log("[curr-pos-swaggy] start (15m)")
    while True:
        cycle_ts = _fmt_kst_now()
        state = _load_state()
        engine_on = _loss_hedge_enabled(state)
        entry_pct = _state_number(state, "_entry_usdt", float(os.getenv("ENTRY_USDT_PCT", "8.0")))
        interval_min = int(_state_number(state, "_loss_hedge_interval_min", 15.0))
        if interval_min > 0:
            cfg.interval_sec = max(60, interval_min * 60)
        pos_map = _build_pos_side_map()
        if not pos_map:
            print(f"[curr-pos-swaggy] cycle {cycle_ts} 결과: 없음 (포지션 없음)")
            _append_log(f"[curr-pos-swaggy] cycle {cycle_ts} 결과: 없음 (포지션 없음)")
            _sleep_to_boundary(cfg.interval_sec)
            continue
        btc_15m_limit = max(120, int(getattr(atlas_cfg, "corr_m_slow", 96)) + 5)
        _fetch_and_cache(atlas_cfg.ref_symbol, "15m", btc_15m_limit)
        entries: list[dict] = []
        for sym, pos_side in sorted(pos_map.items()):
            if not _fetch_and_cache(sym, swaggy_cfg.tf_ltf, ltf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_mtf, mtf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_htf, htf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_htf2, htf2_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_d1, d1_limit):
                continue
            if not _fetch_and_cache(sym, "3m", tf_3m_limit):
                continue

            df_5m = cycle_cache.get_df(sym, swaggy_cfg.tf_ltf, limit=ltf_limit, force=True)
            df_15m = cycle_cache.get_df(sym, swaggy_cfg.tf_mtf, limit=mtf_limit, force=True)
            df_1h = cycle_cache.get_df(sym, swaggy_cfg.tf_htf, limit=htf_limit, force=True)
            df_4h = cycle_cache.get_df(sym, swaggy_cfg.tf_htf2, limit=htf2_limit, force=True)
            df_1d = cycle_cache.get_df(sym, swaggy_cfg.tf_d1, limit=d1_limit, force=True)
            df_3m = cycle_cache.get_df(sym, "3m", limit=tf_3m_limit, force=True)
            if df_5m.empty or df_15m.empty or df_1h.empty or df_4h.empty or df_3m.empty:
                continue
            candle_ts = None
            try:
                candle_ts = df_15m.iloc[-1]["ts"]
            except Exception:
                candle_ts = None

            prev_phase = None
            try:
                prev_phase = (swaggy_engine._state.get(sym) or {}).get("phase")
            except Exception:
                prev_phase = None
            signal = swaggy_engine.evaluate_symbol(
                sym,
                df_4h,
                df_1h,
                df_15m,
                df_5m,
                df_3m,
                df_1d,
                time.time(),
            )
            new_phase = None
            try:
                new_phase = (swaggy_engine._state.get(sym) or {}).get("phase")
            except Exception:
                new_phase = None
            if signal:
                reasons = ",".join(signal.reasons or []) if isinstance(signal.reasons, list) else str(signal.reasons or "")
                side_disp = (signal.side or "").upper() if signal.side else "N/A"
                if prev_phase != new_phase or signal.entry_ok:
                    _log_kst(
                        f"SWAGGY_ATLAS_LAB_V2_PHASE sym={sym} side={side_disp} prev={prev_phase} now={new_phase} reasons={reasons}"
                    )
            if signal and signal.entry_ok:
                btc_df = cycle_cache.get_df(atlas_cfg.ref_symbol, "15m", limit=btc_15m_limit, force=True)
                if btc_df.empty:
                    continue
                atlas_gate = evaluate_global_gate(btc_df, atlas_cfg)
                atlas_decision = evaluate_local(sym, (signal.side or "").upper(), df_15m, btc_df, atlas_gate, atlas_cfg)
                policy = apply_policy(AtlasMode.HARD, float(entry_pct or 0.0), atlas_decision)
                if not policy.allow:
                    _log_kst(
                        "SWAGGY_ATLAS_LAB_V2_POLICY sym=%s side=%s action=%s atlas_reasons=%s"
                        % (
                            sym,
                            (signal.side or "").upper() if signal.side else "N/A",
                            policy.policy_action,
                            ",".join(policy.atlas_reasons or []),
                        )
                    )
                    _log_kst(
                        f"SWAGGY_ATLAS_LAB_V2_SKIP sym={sym} reason=ATLAS_POLICY block={policy.policy_action}"
                    )
                    continue
                atlas_reasons = []
                atlas_action = None
                if policy:
                    atlas_action = policy.policy_action
                    atlas_reasons = policy.atlas_reasons or []
                    _log_kst(
                        "SWAGGY_ATLAS_LAB_V2_POLICY sym=%s side=%s action=%s atlas_reasons=%s"
                        % (
                            sym,
                            (signal.side or "").upper() if signal.side else "N/A",
                            atlas_action,
                            ",".join(atlas_reasons or []),
                        )
                    )
                    _log_kst(
                        "SWAGGY_ATLAS_LAB_V2_PASS sym=%s side=%s action=%s atlas_reasons=%s"
                        % (
                            sym,
                            (signal.side or "").upper() if signal.side else "N/A",
                            atlas_action,
                            ",".join(atlas_reasons or []),
                        )
                    )
                entries.append(
                    {
                        "symbol": sym,
                        "pos_side": pos_side,
                        "side": (signal.side or "").upper() if signal.side else "N/A",
                        "entry_px": signal.entry_px,
                        "candle_ts": candle_ts,
                        "atlas_action": atlas_action,
                        "atlas_reasons": atlas_reasons,
                    }
                )
                if not engine_on:
                    continue
                signal_side = (signal.side or "").upper()
                opp_side = "SHORT" if pos_side == "LONG" else "LONG"
                live = _live_enabled(state, opp_side)
                loss_pct = None
                # Alert-only: always notify when engine is ON and signal is ready.
                if pos_side in ("LONG", "SHORT"):
                    pos_detail = (
                        get_long_position_detail(sym) if pos_side == "LONG" else get_short_position_detail(sym)
                    )
                    loss_pct = _profit_unlev_pct(pos_detail, pos_side)
                entry_usdt = _resolve_entry_usdt(entry_pct)
                # live_ready reserved for future live execution (alerts always sent)
                live_ready = False
                if live:
                    if interval_min > 0 and int(time.time() // 60) % interval_min != 0:
                        live_ready = False
                    elif pos_side not in ("LONG", "SHORT"):
                        live_ready = False
                    elif signal_side != opp_side:
                        live_ready = False
                    elif not isinstance(loss_pct, (int, float)) or loss_pct > LOSS_HEDGE_MIN_PCT:
                        live_ready = False
                    elif entry_usdt <= 0:
                        live_ready = False
                    else:
                        live_ready = True
                entry_price = signal.entry_px
                tp_pct, sl_pct = _engine_tp_sl(state, opp_side)
                msg = _build_entry_message(
                    symbol=sym,
                    side=opp_side,
                    entry_px=entry_price if isinstance(entry_price, (int, float)) else None,
                    usdt=entry_usdt,
                    loss_pct=loss_pct,
                    tp_pct=tp_pct,
                    sl_pct=sl_pct,
                    live=live,
                )
                ok = send_message(token, chat_id, msg)
                debug = signal.debug if hasattr(signal, "debug") else {}
                _log_kst(
                    "SWAGGY_ATLAS_LAB_V2_ENTRY sym=%s side=%s sw_strength=%.3f sw_reasons=%s "
                    "final_usdt=%.2f level_score=%s touch_count=%s level_age=%s trigger_combo=%s confirm_pass=%s confirm_fail=%s "
                    "overext_dist_at_touch=%s overext_dist_at_entry=%s"
                    % (
                        sym,
                        (signal.side or "").upper() if signal.side else "N/A",
                        float(signal.strength or 0.0),
                        ",".join(signal.reasons or []),
                        float(entry_usdt or 0.0),
                        debug.get("level_score"),
                        debug.get("touch_count"),
                        debug.get("level_age_sec"),
                        debug.get("trigger_combo"),
                        debug.get("confirm_pass"),
                        debug.get("confirm_fail"),
                        debug.get("overext_dist_at_touch"),
                        debug.get("overext_dist_at_entry"),
                    )
                )
                log_line = (
                    f"[loss-hedge] alert sent={ok} sym={sym} pos_side={pos_side} "
                    f"signal_side={signal_side} opp_side={opp_side} "
                    f"entry_px={_fmt_entry_px(entry_price)} usdt={entry_usdt:.2f} "
                    f"loss_pct={_fmt_roi(loss_pct)} live={live} "
                    f"atlas_action={atlas_action or 'N/A'} "
                    f"atlas_reasons={','.join(atlas_reasons or []) or 'N/A'}"
                )
                print(log_line)
                _append_log(log_line)
        if not entries:
            print(f"[curr-pos-swaggy] cycle {cycle_ts} 결과: 없음")
            _sleep_to_boundary(cfg.interval_sec)
            continue
        lines = [f"[curr-pos-swaggy] cycle {cycle_ts} 결과:"]
        for item in entries:
            sym = item["symbol"].replace("/USDT:USDT", "")
            atlas_action = item.get("atlas_action") or "N/A"
            atlas_reasons = ",".join(item.get("atlas_reasons") or []) or "N/A"
            lines.append(
                f"- {sym} / {item['pos_side']} / entry={_fmt_entry_px(item.get('entry_px'))} "
                f"/ atlas={atlas_action} ({atlas_reasons})"
            )
        print("\n".join(lines))
        _append_log("\n".join(lines))
        msg = _build_message(entries)
        ok = send_message(token, chat_id, msg)
        print(f"[curr-pos-swaggy] sent={ok} count={len(entries)}")
        _append_log(f"[curr-pos-swaggy] sent={ok} count={len(entries)}")
        _sleep_to_boundary(cfg.interval_sec)


if __name__ == "__main__":
    main()
