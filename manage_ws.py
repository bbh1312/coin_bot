"""manage_ws.py
WebSocket 기반 관리 모듈(테스트용).

특징
- ws_manager 5m kline 캐시를 이용해 빠르게 TP 판단
- 기존 관리 모듈과 별개로 실행(엔진/메인 루프 무수정)
"""
import time
import os
import json
from datetime import datetime
from typing import Optional

from env_loader import load_env
load_env()

import ws_manager
import executor as executor_mod
import engine_runner as er
import db_reconcile as dbrecon

MANAGE_WS_WRITE_STATE = os.getenv("MANAGE_WS_WRITE_STATE", "1") == "1"
MANAGE_WS_SAVE_RUNTIME = os.getenv("MANAGE_WS_SAVE_RUNTIME", "1") == "1"

_ENTRY_EVENTS_CACHE = {"ts": 0.0, "mtime": 0.0, "map": {}}
_ENTRY_EVENTS_BY_SYMBOL_CACHE = {"ts": 0.0, "mtime": 0.0, "map": {}}
_ENTRY_EVENTS_TTL_SEC = 5.0

def _is_startup_position(state: dict, symbol: str) -> bool:
    if not symbol or not isinstance(state, dict):
        return False
    startup = state.get("_startup_pos_syms")
    if isinstance(startup, (list, set, tuple)):
        return symbol in startup
    return False


def _consume_startup_position(state: dict, symbol: str) -> bool:
    if not symbol or not isinstance(state, dict):
        return False
    startup = state.get("_startup_pos_syms")
    if isinstance(startup, set):
        if symbol in startup:
            startup.discard(symbol)
            state["_startup_pos_syms"] = startup
            return True
        return False
    if isinstance(startup, list):
        if symbol in startup:
            startup = [s for s in startup if s != symbol]
            state["_startup_pos_syms"] = startup
            return True
        return False
    return False

def _entry_alerted_in_state(st: dict, side: str) -> bool:
    if not isinstance(st, dict):
        return False
    suffix = "long" if side == "LONG" else "short"
    return bool(st.get(f"entry_alerted_{suffix}"))


def _coerce_state_int(val) -> int:
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


def _get_entry_event_engine(entry_order_id: Optional[str]) -> Optional[str]:
    if not entry_order_id:
        return None
    base_dir = os.path.dirname(os.path.abspath(__file__))
    date_tag = time.strftime("%Y-%m-%d")
    path = os.path.join(base_dir, "logs", "entry", f"entry_events-{date_tag}.log")
    if not os.path.exists(path):
        path = os.path.join(base_dir, "logs", "entry_events.log")
    try:
        mtime = os.path.getmtime(path) if os.path.exists(path) else 0.0
    except Exception:
        mtime = 0.0
    now = time.time()
    cached = _ENTRY_EVENTS_CACHE
    if (
        cached.get("map")
        and (now - float(cached.get("ts", 0.0) or 0.0)) <= _ENTRY_EVENTS_TTL_SEC
        and float(cached.get("mtime", 0.0) or 0.0) == float(mtime or 0.0)
    ):
        return cached["map"].get(str(entry_order_id), {}).get("engine")
    entry_map, _ = er._load_entry_events_map(None)
    _ENTRY_EVENTS_CACHE["ts"] = now
    _ENTRY_EVENTS_CACHE["mtime"] = float(mtime or 0.0)
    _ENTRY_EVENTS_CACHE["map"] = entry_map
    return entry_map.get(str(entry_order_id), {}).get("engine")


def _get_entry_events_by_symbol(now_ts: Optional[float] = None) -> dict:
    date_tag = time.strftime("%Y-%m-%d")
    path = os.path.join("logs", "entry", f"entry_events-{date_tag}.log")
    legacy = os.path.join("logs", "entry_events.log")
    use_path = path if os.path.exists(path) else legacy
    try:
        mtime = os.path.getmtime(use_path) if os.path.exists(use_path) else 0.0
    except Exception:
        mtime = 0.0
    now = now_ts if isinstance(now_ts, (int, float)) else time.time()
    cached = _ENTRY_EVENTS_BY_SYMBOL_CACHE
    if (
        cached.get("map")
        and (now - float(cached.get("ts", 0.0) or 0.0)) <= _ENTRY_EVENTS_TTL_SEC
        and float(cached.get("mtime", 0.0) or 0.0) == float(mtime or 0.0)
    ):
        return cached["map"]
    _, by_symbol = er._load_entry_events_map(None, include_alerts=True, include_engine_signals=True)
    _ENTRY_EVENTS_BY_SYMBOL_CACHE["ts"] = now
    _ENTRY_EVENTS_BY_SYMBOL_CACHE["mtime"] = float(mtime or 0.0)
    _ENTRY_EVENTS_BY_SYMBOL_CACHE["map"] = by_symbol
    return by_symbol


def _load_entry_events_map_local() -> tuple[dict, dict]:
    by_id = {}
    by_symbol = {}
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
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
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
                    if symbol and side:
                        by_symbol.setdefault((symbol, side), []).append(record)
        except Exception:
            continue
    return by_id, by_symbol


def _get_recent_entry_event(symbol: str, side: str, now_ts: Optional[float] = None, window_sec: float = 2592000.0) -> Optional[dict]:
    if not symbol or not side:
        return None
    by_symbol = _get_entry_events_by_symbol(now_ts)
    key = (symbol, (side or "").upper())
    recs = by_symbol.get(key) if isinstance(by_symbol, dict) else None
    if not recs:
        return None
    now_ts = now_ts if isinstance(now_ts, (int, float)) else time.time()
    best = None
    best_ts = 0.0
    for rec in recs:
        if not isinstance(rec, dict):
            continue
        ts_val = rec.get("entry_ts")
        if not isinstance(ts_val, (int, float)):
            continue
        if (now_ts - float(ts_val)) > window_sec:
            continue
        eng = str(rec.get("engine") or "").upper()
        if not eng or eng in ("UNKNOWN", "MANUAL"):
            continue
        if float(ts_val) >= best_ts:
            best = rec
            best_ts = float(ts_val)
    return best


def _last_ws_close(symbol: str):
    df = ws_manager.get_5m_df(symbol, limit=2)
    if df is None or df.empty:
        return None
    try:
        return float(df.iloc[-1].get("close"))
    except Exception:
        return None


def _update_watch_symbols() -> list:
    try:
        symbols = executor_mod.list_open_position_symbols(force=True)
        watch = list((symbols.get("long") or set()) | (symbols.get("short") or set()))
    except Exception:
        watch = []
    if ws_manager and ws_manager.is_running():
        try:
            ws_manager.set_watch_symbols(watch)
        except Exception:
            pass
    return watch


def _format_symbol_list(symbols: list) -> str:
    if not symbols:
        return "[]"
    return "[" + ", ".join(sorted(symbols)) + "]"


def _diff_symbols(prev: list, cur: list) -> tuple[list, list]:
    prev_set = set(prev or [])
    cur_set = set(cur or [])
    added = sorted(list(cur_set - prev_set))
    removed = sorted(list(prev_set - cur_set))
    return added, removed


def _get_tickers(cache: dict) -> dict:
    now = time.time()
    last_ts = cache.get("ts", 0.0)
    if (now - last_ts) <= 5.0 and isinstance(cache.get("tickers"), dict):
        return cache.get("tickers")
    try:
        tickers = executor_mod.exchange.fetch_tickers()
        cache["tickers"] = tickers
        cache["ts"] = now
        return tickers
    except Exception as e:
        print("[manage-ws] tickers fetch failed:", e)
        return cache.get("tickers") or {}


def _force_in_pos_from_api(state: dict, api_set: set) -> None:
    if not api_set:
        return
    now = time.time()
    for sym in api_set:
        if not isinstance(sym, str) or "/" not in sym:
            continue
        st = state.get(sym)
        if not isinstance(st, dict):
            st = {}
        try:
            long_amt = executor_mod.get_long_position_amount(sym)
        except Exception:
            long_amt = 0.0
        try:
            short_amt = executor_mod.get_short_position_amount(sym)
        except Exception:
            short_amt = 0.0
        if long_amt > 0:
            er._set_in_pos_side(st, "LONG", True)
        if short_amt > 0:
            er._set_in_pos_side(st, "SHORT", True)
        if long_amt <= 0 and short_amt <= 0:
            st["in_pos_long"] = False
            st["in_pos_short"] = False
            st["in_pos"] = False
        st.setdefault("dca_adds", 0)
        st.setdefault("dca_adds_long", 0)
        st.setdefault("dca_adds_short", 0)
        st.setdefault("last_entry", now)
        st.setdefault("manage_ping_ts", now - er.MANAGE_PING_COOLDOWN_SEC)
        state[sym] = st


def _drain_entry_events(state) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    path = os.path.join("logs", "entry", f"entry_events-{date_tag}.log")
    legacy_path = os.path.join("logs", "entry_events.log")
    try:
        if not os.path.exists(path) and not os.path.exists(legacy_path):
            return
        use_path = path if os.path.exists(path) else legacy_path
        offset_key = "_entry_event_offset" if use_path == path else "_entry_event_offset_legacy"
        offset = _coerce_state_int(state.get(offset_key, 0))
        with open(use_path, "r", encoding="utf-8") as f:
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
            entry_ts = payload.get("entry_ts")
            entry_ts_val = None
            if isinstance(entry_ts, (int, float)):
                entry_ts_val = float(entry_ts)
            elif isinstance(entry_ts, str):
                try:
                    entry_ts_val = time.mktime(time.strptime(entry_ts, "%Y-%m-%d %H:%M:%S"))
                except Exception:
                    entry_ts_val = None
            if entry_ts_val is None:
                continue
            payload_engine = payload.get("engine") or "UNKNOWN"
            tr = {
                "entry_ts": entry_ts_val,
                "entry_order_id": payload.get("entry_order_id"),
                "symbol": payload.get("symbol"),
                "side": payload.get("side"),
                "entry_price": payload.get("entry_price"),
                "qty": payload.get("qty"),
                "engine_label": payload_engine,
                "exit_ts": None,
                "exit_price": None,
                "pnl_usdt": None,
                "exit_reason": "",
                "roi_pct": None,
            }
            er._update_report_csv(tr)
            sym = tr.get("symbol")
            side = (tr.get("side") or "").upper()
            if sym and side:
                st = state.get(sym, {})
                if not isinstance(st, dict):
                    st = {}
                st[f"entry_order_id_{side.lower()}"] = tr.get("entry_order_id")
                state[sym] = st
                open_tr = er._get_open_trade(state, side, sym)
                if open_tr:
                    cur_label = _trade_engine_label(open_tr)
                    if payload_engine != "UNKNOWN" and cur_label in ("UNKNOWN", "MANUAL"):
                        open_tr["engine_label"] = payload_engine
                        reason = _reason_from_engine_label(payload_engine, side)
                        if reason:
                            meta = open_tr.get("meta") if isinstance(open_tr.get("meta"), dict) else {}
                            meta["reason"] = reason
                            open_tr["meta"] = meta
                else:
                    log = er._get_trade_log(state)
                    reason = _reason_from_engine_label(payload_engine, side)
                    log.append(
                        {
                            "side": side,
                            "symbol": sym,
                            "entry_ts": float(entry_ts_val),
                            "entry_ts_ms": int(float(entry_ts_val) * 1000),
                            "entry_price": payload.get("entry_price"),
                            "qty": payload.get("qty"),
                            "usdt": None,
                            "entry_order_id": payload.get("entry_order_id"),
                            "status": "open",
                            "meta": {"reason": reason} if reason else {},
                            "engine_label": payload_engine,
                        }
                    )
        state[offset_key] = new_offset
    except Exception as e:
        print("[manage-ws] entry_events drain error:", e)


def _reason_from_engine_label(engine_label: Optional[str], side: str) -> Optional[str]:
    label = (engine_label or "").upper()
    if label == "SWAGGY_ATLAS_LAB":
        return "swaggy_atlas_lab"
    if label == "SWAGGY_NO_ATLAS":
        return "swaggy_no_atlas"
    if label == "SWAGGY":
        return "swaggy_long" if side == "LONG" else "swaggy_short"
    if label == "ATLAS_RS_FAIL_SHORT":
        return "atlas_rs_fail_short"
    if label == "RSI":
        return "short_entry"
    if label == "SCALP":
        return "long_entry"
    if label == "MANUAL":
        return "manual_entry"
    return None


def _trade_engine_label(tr: Optional[dict]) -> str:
    if isinstance(tr, dict):
        label = tr.get("engine_label")
        if isinstance(label, str) and label:
            if label not in ("UNKNOWN", "MANUAL"):
                return label
        reason = (tr.get("meta") or {}).get("reason")
        base_label = er._engine_label_from_reason(reason)
        if base_label not in ("UNKNOWN", "MANUAL"):
            return base_label
        entry_order_id = tr.get("entry_order_id")
        entry_engine = _get_entry_event_engine(str(entry_order_id) if entry_order_id else None)
        if entry_engine and str(entry_engine).lower() != "unknown":
            return str(entry_engine)
        return base_label
    return "UNKNOWN"


def _find_entry_event_for_trade(symbol: str, side: str, entry_ts: Optional[float] = None, now_ts: Optional[float] = None, window_sec: float = 2592000.0) -> Optional[dict]:
    if not symbol or not side:
        return None
    now_ts = now_ts if isinstance(now_ts, (int, float)) else time.time()
    by_symbol = _get_entry_events_by_symbol(now_ts)
    key = (symbol, (side or "").upper())
    recs = by_symbol.get(key) if isinstance(by_symbol, dict) else None
    if not recs:
        _, by_symbol = er._load_entry_events_map(None)
        _ENTRY_EVENTS_BY_SYMBOL_CACHE["ts"] = now_ts
        _ENTRY_EVENTS_BY_SYMBOL_CACHE["mtime"] = _ENTRY_EVENTS_BY_SYMBOL_CACHE.get("mtime", 0.0)
        _ENTRY_EVENTS_BY_SYMBOL_CACHE["map"] = by_symbol
        recs = by_symbol.get(key) if isinstance(by_symbol, dict) else None
    if not recs:
        return None
    best = None
    best_gap = None
    entry_ts_val = float(entry_ts) if isinstance(entry_ts, (int, float)) else None
    for rec in recs:
        if not isinstance(rec, dict):
            continue
        rec_ts = rec.get("entry_ts")
        if not isinstance(rec_ts, (int, float)):
            continue
        eng = str(rec.get("engine") or "").upper()
        if not eng or eng in ("UNKNOWN", "MANUAL"):
            continue
        if entry_ts_val is not None:
            gap = abs(float(rec_ts) - entry_ts_val)
            if gap > window_sec:
                continue
            if best_gap is None or gap < best_gap:
                best = rec
                best_gap = gap
        else:
            if (now_ts - float(rec_ts)) > 7 * 24 * 3600:
                continue
            if best is None or float(rec_ts) > float(best.get("entry_ts") or 0.0):
                best = rec
    return best


def _backfill_engine_labels_from_entry_events(state: dict, window_sec: float = 2592000.0) -> None:
    log = er._get_trade_log(state)
    if not isinstance(log, list):
        return
    entry_map, by_symbol = _load_entry_events_map_local()
    now_ts = time.time()
    for tr in log:
        if not isinstance(tr, dict):
            continue
        if tr.get("status") != "open":
            continue
        cur_label = _trade_engine_label(tr)
        if cur_label not in ("UNKNOWN", "MANUAL"):
            continue
        symbol = tr.get("symbol")
        side = (tr.get("side") or "").upper()
        if not symbol or side not in ("LONG", "SHORT"):
            continue
        entry_order_id = tr.get("entry_order_id")
        rec = entry_map.get(str(entry_order_id)) if entry_order_id else None
        if not isinstance(rec, dict):
            recs = by_symbol.get((symbol, side)) if isinstance(by_symbol, dict) else None
            best = None
            best_gap = None
            entry_ts = tr.get("entry_ts")
            entry_ts_val = float(entry_ts) if isinstance(entry_ts, (int, float)) else None
            if recs:
                for cand in recs:
                    if not isinstance(cand, dict):
                        continue
                    eng = str(cand.get("engine") or "").upper()
                    if not eng or eng in ("UNKNOWN", "MANUAL"):
                        continue
                    cand_ts = cand.get("entry_ts")
                    if not isinstance(cand_ts, (int, float)):
                        continue
                    if entry_ts_val is not None:
                        gap = abs(float(cand_ts) - entry_ts_val)
                        if gap > window_sec:
                            continue
                        if best_gap is None or gap < best_gap:
                            best = cand
                            best_gap = gap
                    else:
                        if (now_ts - float(cand_ts)) > 7 * 24 * 3600:
                            continue
                        if best is None or float(cand_ts) > float(best.get("entry_ts") or 0.0):
                            best = cand
            rec = best
        if not isinstance(rec, dict):
            continue
        eng = str(rec.get("engine") or "").upper()
        if not eng or eng in ("UNKNOWN", "MANUAL"):
            continue
        tr["engine_label"] = eng
        meta = tr.get("meta") if isinstance(tr.get("meta"), dict) else {}
        reason = _reason_from_engine_label(eng, side)
        if reason:
            meta["reason"] = reason
        meta["engine"] = eng
        tr["meta"] = meta
        if not tr.get("entry_order_id") and rec.get("entry_order_id"):
            tr["entry_order_id"] = rec.get("entry_order_id")
            st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
            if not isinstance(st, dict):
                st = {}
            st[f"entry_order_id_{side.lower()}"] = rec.get("entry_order_id")
            state[symbol] = st


def _maybe_update_open_trade_engine(state: dict, symbol: str, side: str, now_ts: float) -> None:
    open_tr = er._get_open_trade(state, side, symbol)
    if not isinstance(open_tr, dict):
        return
    cur_label = _trade_engine_label(open_tr)
    if cur_label not in ("UNKNOWN", "MANUAL"):
        return
    rec = _find_entry_event_for_trade(symbol, side, entry_ts=open_tr.get("entry_ts"), now_ts=now_ts)
    if not isinstance(rec, dict):
        return
    eng = str(rec.get("engine") or "").upper()
    if not eng or eng in ("UNKNOWN", "MANUAL"):
        return
    open_tr["engine_label"] = eng
    meta = open_tr.get("meta") if isinstance(open_tr.get("meta"), dict) else {}
    reason = _reason_from_engine_label(eng, side)
    if reason:
        meta["reason"] = reason
    meta["engine"] = eng
    open_tr["meta"] = meta
    if not open_tr.get("entry_order_id") and rec.get("entry_order_id"):
        open_tr["entry_order_id"] = rec.get("entry_order_id")
        st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
        if not isinstance(st, dict):
            st = {}
        st[f"entry_order_id_{side.lower()}"] = rec.get("entry_order_id")
        state[symbol] = st


def _backfill_engine_from_recent_event(state: dict, symbol: str, side: str, now_ts: float) -> bool:
    rec = _get_recent_entry_event(symbol, side, now_ts=now_ts, window_sec=600.0)
    if not isinstance(rec, dict):
        return False
    eng = str(rec.get("engine") or "").upper()
    if not eng or eng in ("UNKNOWN", "MANUAL"):
        return False
    open_tr = er._get_open_trade(state, side, symbol)
    if not isinstance(open_tr, dict):
        return False
    open_tr["engine_label"] = eng
    meta = open_tr.get("meta") if isinstance(open_tr.get("meta"), dict) else {}
    reason = _reason_from_engine_label(eng, side)
    if reason:
        meta["reason"] = reason
    meta["engine"] = eng
    open_tr["meta"] = meta
    if not open_tr.get("entry_order_id") and rec.get("entry_order_id"):
        open_tr["entry_order_id"] = rec.get("entry_order_id")
        st = state.get(symbol) if isinstance(state.get(symbol), dict) else {}
        if not isinstance(st, dict):
            st = {}
        st[f"entry_order_id_{side.lower()}"] = rec.get("entry_order_id")
        state[symbol] = st
    return True


def _mark_existing_manual_entries(state: dict, pos_syms: dict, now_ts: float) -> None:
    if not isinstance(pos_syms, dict):
        return
    for sym in pos_syms.get("long") or []:
        st = state.get(sym, {}) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        open_tr = er._get_open_trade(state, "LONG", sym)
        eng_label = _trade_engine_label(open_tr)
        st.pop("manual_entry_pending_long_ts", None)
        if eng_label == "MANUAL":
            st["manual_entry_alerted_long"] = True
        state[sym] = st
    for sym in pos_syms.get("short") or []:
        st = state.get(sym, {}) if isinstance(state, dict) else {}
        if not isinstance(st, dict):
            st = {}
        open_tr = er._get_open_trade(state, "SHORT", sym)
        eng_label = _trade_engine_label(open_tr)
        st.pop("manual_entry_pending_short_ts", None)
        if eng_label == "MANUAL":
            st["manual_entry_alerted_short"] = True
        state[sym] = st


def _within_auto_exit_grace(state: dict, symbol: str, side: str, now_ts: float) -> bool:
    open_tr = er._get_open_trade(state, side, symbol)
    entry_ts = None
    if isinstance(open_tr, dict):
        entry_ts = open_tr.get("entry_ts")
        if entry_ts is None:
            entry_ts_ms = open_tr.get("entry_ts_ms")
            if isinstance(entry_ts_ms, (int, float)):
                entry_ts = float(entry_ts_ms) / 1000.0
    if entry_ts is None:
        st = state.get(symbol, {}) if isinstance(state, dict) else {}
        if isinstance(st, dict):
            entry_ts = st.get("last_entry")
    if isinstance(entry_ts, (int, float)):
        return (now_ts - float(entry_ts)) < er.AUTO_EXIT_GRACE_SEC
    return False


def _recent_auto_exit_disk(symbol: str, now_ts: float) -> bool:
    try:
        disk = er.load_state()
    except Exception:
        return False
    st = disk.get(symbol, {}) if isinstance(disk, dict) else {}
    if not isinstance(st, dict):
        return False
    last_exit_ts = st.get("last_exit_ts")
    last_exit_reason = st.get("last_exit_reason")
    if not isinstance(last_exit_ts, (int, float)):
        return False
    if last_exit_reason not in ("auto_exit_tp", "auto_exit_sl"):
        return False
    return (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC


def _manual_close_long(state, symbol, now_ts, report_ok: bool = True, mark_px: Optional[float] = None):
    open_tr = er._get_open_trade(state, "LONG", symbol)
    if not open_tr:
        return
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    engine_label = _trade_engine_label(open_tr)
    st = state.get(symbol, {})
    seen_ts = st.get("long_pos_seen_ts") if isinstance(st, dict) else None
    # WS 감지 기반 수동 청산은 최근 확인 여부와 무관하게 알림
    exit_reason = "manual_close"
    entry_px = open_tr.get("entry_price") if isinstance(open_tr, dict) else None
    tp_pct, sl_pct = er._get_engine_exit_thresholds(_trade_engine_label(open_tr), "LONG")
    if isinstance(entry_px, (int, float)) and isinstance(mark_px, (int, float)) and entry_px > 0:
        profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
        if isinstance(tp_pct, (int, float)) and profit_unlev >= float(tp_pct):
            exit_reason = "auto_exit_tp"
        elif isinstance(sl_pct, (int, float)) and float(sl_pct) > 0 and profit_unlev <= -float(sl_pct):
            exit_reason = "auto_exit_sl"
    last_exit_ts = st.get("last_exit_ts") if isinstance(st, dict) else None
    last_exit_reason = st.get("last_exit_reason") if isinstance(st, dict) else None
    if (
        isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl")
    ):
        return
    if (
        exit_reason == "manual_close"
        and isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl", "manual_close")
    ):
        return
    if exit_reason == "manual_close" and _recent_auto_exit_disk(symbol, now_ts):
        return
    er._close_trade(
        state,
        side="LONG",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=mark_px if isinstance(mark_px, (int, float)) else None,
        pnl_usdt=None,
        reason=exit_reason,
    )
    try:
        er._record_position_event(
            symbol=symbol,
            side="LONG",
            event_type="EXIT",
            source="MANUAL",
            qty=open_tr.get("qty") if isinstance(open_tr, dict) else None,
            avg_entry=open_tr.get("entry_price") if isinstance(open_tr, dict) else None,
            price=None,
            meta={"source": "manage_ws"},
        )
    except Exception:
        pass
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    er._set_in_pos_side(st, "LONG", False)
    st["last_ok"] = False
    st["dca_adds"] = 0
    st["dca_adds_long"] = 0
    st["dca_adds_short"] = 0
    st.pop("manual_entry_pending_long_ts", None)
    st.pop("manual_entry_alerted_long", None)
    st.pop("manual_entry_alerted_long_ts", None)
    st.pop("manual_entry_alerted_long_reason", None)
    st["last_exit_ts"] = now_ts
    st["last_exit_reason"] = exit_reason
    state[symbol] = st
    if report_ok:
        er._update_report_csv(open_tr)
    print(f"[manage-ws] long_manual_close sym={symbol} engine={engine_label}")
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    reason_label = "TP" if exit_reason == "auto_exit_tp" else "SL" if exit_reason == "auto_exit_sl" else "MANUAL"
    icon = er.EXIT_SL_ICON if reason_label == "SL" else er.EXIT_ICON
    er.send_telegram(
        f"{icon} <b>롱 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: {reason_label}\n"
        f"{entry_line}".rstrip()
    )


def _manual_close_short(state, symbol, now_ts, report_ok: bool = True, mark_px: Optional[float] = None):
    open_tr = er._get_open_trade(state, "SHORT", symbol)
    if not open_tr:
        return
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    engine_label = _trade_engine_label(open_tr)
    st = state.get(symbol, {})
    seen_ts = st.get("short_pos_seen_ts") if isinstance(st, dict) else None
    # WS 감지 기반 수동 청산은 최근 확인 여부와 무관하게 알림
    exit_reason = "manual_close"
    entry_px = open_tr.get("entry_price") if isinstance(open_tr, dict) else None
    tp_pct, sl_pct = er._get_engine_exit_thresholds(_trade_engine_label(open_tr), "SHORT")
    if isinstance(entry_px, (int, float)) and isinstance(mark_px, (int, float)) and entry_px > 0:
        profit_unlev = (float(entry_px) - float(mark_px)) / float(entry_px) * 100.0
        if isinstance(tp_pct, (int, float)) and profit_unlev >= float(tp_pct):
            exit_reason = "auto_exit_tp"
        elif isinstance(sl_pct, (int, float)) and float(sl_pct) > 0 and profit_unlev <= -float(sl_pct):
            exit_reason = "auto_exit_sl"
    last_exit_ts = st.get("last_exit_ts") if isinstance(st, dict) else None
    last_exit_reason = st.get("last_exit_reason") if isinstance(st, dict) else None
    if (
        isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl")
    ):
        return
    if (
        exit_reason == "manual_close"
        and isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl", "manual_close")
    ):
        return
    if exit_reason == "manual_close" and _recent_auto_exit_disk(symbol, now_ts):
        return
    er._close_trade(
        state,
        side="SHORT",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=mark_px if isinstance(mark_px, (int, float)) else None,
        pnl_usdt=None,
        reason=exit_reason,
    )
    try:
        er._record_position_event(
            symbol=symbol,
            side="SHORT",
            event_type="EXIT",
            source="MANUAL",
            qty=open_tr.get("qty") if isinstance(open_tr, dict) else None,
            avg_entry=open_tr.get("entry_price") if isinstance(open_tr, dict) else None,
            price=None,
            meta={"source": "manage_ws"},
        )
    except Exception:
        pass
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    er._set_in_pos_side(st, "SHORT", False)
    st["last_ok"] = False
    st["dca_adds"] = 0
    st["dca_adds_long"] = 0
    st["dca_adds_short"] = 0
    st.pop("manual_entry_pending_short_ts", None)
    st.pop("manual_entry_alerted_short", None)
    st.pop("manual_entry_alerted_short_ts", None)
    st.pop("manual_entry_alerted_short_reason", None)
    st["last_exit_ts"] = now_ts
    st["last_exit_reason"] = exit_reason
    state[symbol] = st
    if report_ok:
        er._update_report_csv(open_tr)
    print(f"[manage-ws] short_manual_close sym={symbol} engine={engine_label}")
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    reason_label = "TP" if exit_reason == "auto_exit_tp" else "SL" if exit_reason == "auto_exit_sl" else "MANUAL"
    icon = er.EXIT_SL_ICON if reason_label == "SL" else er.EXIT_ICON
    er.send_telegram(
        f"{icon} <b>숏 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: {reason_label}\n"
        f"{entry_line}".rstrip()
    )


def _handle_long_tp(state, symbol, detail, mark_px, now_ts) -> bool:
    if _within_auto_exit_grace(state, symbol, "LONG", now_ts):
        return False
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return False
    profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
    tp_pct, _ = er._get_engine_exit_thresholds(_trade_engine_label(er._get_open_trade(state, "LONG", symbol)), "LONG")
    if profit_unlev < tp_pct:
        return False
    open_tr = er._get_open_trade(state, "LONG", symbol)
    engine_label = _trade_engine_label(open_tr)
    try:
        executor_mod.set_dry_run(False if er.LONG_LIVE_TRADING else True)
    except Exception:
        pass
    res = executor_mod.close_long_market(symbol)
    executor_mod.cancel_stop_orders(symbol)
    exit_order_id = None
    if isinstance(res, dict):
        exit_order_id = res.get("order_id") or er._extract_order_id(res.get("order"))
    avg_price = (
        res.get("order", {}).get("average")
        or res.get("order", {}).get("price")
        or res.get("order", {}).get("info", {}).get("avgPrice")
    )
    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
    pnl_long = detail.get("pnl")
    er._close_trade(
        state,
        side="LONG",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
        pnl_usdt=pnl_long,
        reason="auto_exit_tp",
        exit_order_id=exit_order_id,
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    if isinstance(st, dict):
        st["last_exit_ts"] = now_ts
        st["last_exit_reason"] = "auto_exit_tp"
        state[symbol] = st
    er._append_report_line(symbol, "LONG", profit_unlev, pnl_long, engine_label)
    print(f"[manage-ws] long_tp_exit sym={symbol} roi={profit_unlev:.2f}% pnl={pnl_long}")
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    er.send_telegram(
        f"{er.EXIT_ICON} <b>롱 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: TP\n"
        f"{entry_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
    )
    return True


def _handle_short_tp(state, symbol, detail, mark_px, now_ts) -> bool:
    if _within_auto_exit_grace(state, symbol, "SHORT", now_ts):
        return False
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return False
    profit_unlev = (float(entry_px) - float(mark_px)) / float(entry_px) * 100.0
    tp_pct, _ = er._get_engine_exit_thresholds(_trade_engine_label(er._get_open_trade(state, "SHORT", symbol)), "SHORT")
    if profit_unlev < tp_pct:
        return False
    open_tr = er._get_open_trade(state, "SHORT", symbol)
    engine_label = _trade_engine_label(open_tr)
    try:
        executor_mod.set_dry_run(False if er.LIVE_TRADING else True)
    except Exception:
        pass
    res = executor_mod.close_short_market(symbol)
    executor_mod.cancel_stop_orders(symbol)
    exit_order_id = None
    if isinstance(res, dict):
        exit_order_id = res.get("order_id") or er._extract_order_id(res.get("order"))
    avg_price = (
        res.get("order", {}).get("average")
        or res.get("order", {}).get("price")
        or res.get("order", {}).get("info", {}).get("avgPrice")
    )
    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
    pnl_short = detail.get("pnl")
    er._close_trade(
        state,
        side="SHORT",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
        pnl_usdt=pnl_short,
        reason="auto_exit_tp",
        exit_order_id=exit_order_id,
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    if isinstance(st, dict):
        st["last_exit_ts"] = now_ts
        st["last_exit_reason"] = "auto_exit_tp"
        state[symbol] = st
    er._append_report_line(symbol, "SHORT", profit_unlev, pnl_short, engine_label)
    print(f"[manage-ws] short_tp_exit sym={symbol} roi={profit_unlev:.2f}% pnl={pnl_short}")
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    er.send_telegram(
        f"{er.EXIT_ICON} <b>숏 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: TP\n"
        f"{entry_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_short is None else f' 손익={pnl_short:+.3f} USDT'}"
    )
    return True


def _handle_long_sl(state, symbol, detail, mark_px, now_ts) -> bool:
    if _within_auto_exit_grace(state, symbol, "LONG", now_ts):
        return False
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return False
    profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
    _, sl_pct = er._get_engine_exit_thresholds(_trade_engine_label(er._get_open_trade(state, "LONG", symbol)), "LONG")
    if profit_unlev > -sl_pct:
        return False
    open_tr = er._get_open_trade(state, "LONG", symbol)
    engine_label = _trade_engine_label(open_tr)
    try:
        executor_mod.set_dry_run(False if er.LONG_LIVE_TRADING else True)
    except Exception:
        pass
    res = executor_mod.close_long_market(symbol)
    executor_mod.cancel_stop_orders(symbol)
    exit_order_id = None
    if isinstance(res, dict):
        exit_order_id = res.get("order_id") or er._extract_order_id(res.get("order"))
    avg_price = (
        res.get("order", {}).get("average")
        or res.get("order", {}).get("price")
        or res.get("order", {}).get("info", {}).get("avgPrice")
    )
    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
    pnl_long = detail.get("pnl")
    er._close_trade(
        state,
        side="LONG",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
        pnl_usdt=pnl_long,
        reason="auto_exit_sl",
        exit_order_id=exit_order_id,
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    if isinstance(st, dict):
        st["last_exit_ts"] = now_ts
        st["last_exit_reason"] = "auto_exit_sl"
        state[symbol] = st
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    er.send_telegram(
        f"{er.EXIT_SL_ICON} <b>롱 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: SL\n"
        f"{entry_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
    )
    return True


def _handle_short_sl(state, symbol, detail, mark_px, now_ts) -> bool:
    if _within_auto_exit_grace(state, symbol, "SHORT", now_ts):
        return False
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return False
    profit_unlev = (float(entry_px) - float(mark_px)) / float(entry_px) * 100.0
    _, sl_pct = er._get_engine_exit_thresholds(_trade_engine_label(er._get_open_trade(state, "SHORT", symbol)), "SHORT")
    if profit_unlev > -sl_pct:
        return False
    open_tr = er._get_open_trade(state, "SHORT", symbol)
    engine_label = _trade_engine_label(open_tr)
    try:
        executor_mod.set_dry_run(False if er.LIVE_TRADING else True)
    except Exception:
        pass
    res = executor_mod.close_short_market(symbol)
    executor_mod.cancel_stop_orders(symbol)
    exit_order_id = None
    if isinstance(res, dict):
        exit_order_id = res.get("order_id") or er._extract_order_id(res.get("order"))
    avg_price = (
        res.get("order", {}).get("average")
        or res.get("order", {}).get("price")
        or res.get("order", {}).get("info", {}).get("avgPrice")
    )
    filled = res.get("order", {}).get("filled") or res.get("order", {}).get("amount")
    cost = res.get("order", {}).get("cost") or res.get("order", {}).get("info", {}).get("cumQuote")
    pnl_short = detail.get("pnl")
    er._close_trade(
        state,
        side="SHORT",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=avg_price if isinstance(avg_price, (int, float)) else mark_px,
        pnl_usdt=pnl_short,
        reason="auto_exit_sl",
        exit_order_id=exit_order_id,
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    if isinstance(st, dict):
        st["last_exit_ts"] = now_ts
        st["last_exit_reason"] = "auto_exit_sl"
        state[symbol] = st
    entry_time = er._fmt_entry_time(open_tr)
    entry_line = f"진입시간={entry_time}\n" if entry_time else ""
    er.send_telegram(
        f"{er.EXIT_SL_ICON} <b>숏 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {er._display_engine_label(engine_label)}\n"
        f"사유: SL\n"
        f"{entry_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_short is None else f' 손익={pnl_short:+.3f} USDT'}"
    )
    return True


def main():
    er.MANAGE_WS_MODE = True
    er.SUPPRESS_RECONCILE_ALERTS = True
    er._install_error_hooks()
    state = er.load_state()
    er._reload_runtime_settings_from_disk(state)
    state["_manage_ws_mode"] = True
    if ws_manager and ws_manager.is_available():
        ws_manager.start()
    else:
        print("[manage-ws] ws_manager unavailable")
    # Startup sync: ensure state/db reflect current positions without alert spam.
    try:
        executor_mod.refresh_positions_cache(force=True)
    except Exception:
        pass
    pos_syms = {"long": [], "short": []}
    try:
        pos_syms = executor_mod.list_open_position_symbols(force=True)
        api_syms = set()
        api_syms.update(pos_syms.get("long") or [])
        api_syms.update(pos_syms.get("short") or [])
        api_syms = {s for s in api_syms if isinstance(s, str) and "/" in s}
        for sym, st in list(state.items()):
            if not isinstance(st, dict):
                continue
            if st.get("in_pos") and sym not in api_syms:
                st["in_pos_long"] = False
                st["in_pos_short"] = False
                st["in_pos"] = False
                state[sym] = st
        if api_syms:
            state["_startup_pos_syms"] = sorted(api_syms)
            now_ts = time.time()
            for sym in pos_syms.get("long") or []:
                st = state.get(sym, {}) if isinstance(state, dict) else {}
                if not isinstance(st, dict):
                    st = {}
                st["entry_alerted_long"] = True
                st["entry_alerted_long_ts"] = now_ts
                state[sym] = st
            for sym in pos_syms.get("short") or []:
                st = state.get(sym, {}) if isinstance(state, dict) else {}
                if not isinstance(st, dict):
                    st = {}
                st["entry_alerted_short"] = True
                st["entry_alerted_short_ts"] = now_ts
                state[sym] = st
            er._sync_positions_state(state, list(api_syms))
            _force_in_pos_from_api(state, api_syms)
            try:
                dbrecon.sync_exchange_state(executor_mod.exchange, symbols=list(api_syms))
            except Exception:
                pass
            try:
                state["_active_positions_total"] = int(
                    len(pos_syms.get("long") or []) + len(pos_syms.get("short") or [])
                )
            except Exception:
                pass
    except Exception:
        pass
    try:
        tickers_cache = {"ts": 0.0, "tickers": {}}
        cached_long_ex = er.CachedExchange(executor_mod.exchange)
        tickers = _get_tickers(tickers_cache)
        er._reconcile_long_trades(state, cached_long_ex, tickers)
        er._reconcile_short_trades(state, tickers)
        er._detect_position_events(state, lambda *args, **kwargs: None)
        _backfill_engine_labels_from_entry_events(state)
        _mark_existing_manual_entries(state, pos_syms, time.time())
        if MANAGE_WS_WRITE_STATE:
            er.save_state(state)
        print("[manage-ws] startup sync complete")
    except Exception as e:
        print(f"[manage-ws] startup sync failed: {e}")
    last_sync_ts = 0.0
    last_cfg_save_ts = 0.0
    last_watch_ts = 0.0
    last_reconcile_ts = 0.0
    last_state_save_ts = 0.0
    watch_syms = []
    first_watch = True
    last_amt = {}
    last_pos_log_ts = 0.0
    tickers_cache = {"ts": 0.0, "tickers": {}}
    cached_long_ex = er.CachedExchange(executor_mod.exchange)
    while True:
        _drain_entry_events(state)
        try:
            er._process_manage_queue(state, er.send_telegram)
        except Exception:
            pass
        er.handle_telegram_commands(state)
        if (time.time() - last_cfg_save_ts) >= 2.0:
            try:
                er._reload_runtime_settings_from_disk(state)
                if MANAGE_WS_SAVE_RUNTIME:
                    er._save_runtime_settings_only(state)
            except Exception:
                pass
            last_cfg_save_ts = time.time()
        if (time.time() - last_watch_ts) >= 5.0:
            new_watch_syms = _update_watch_symbols()
            try:
                active_count = int(executor_mod.count_open_positions(force=True))
            except Exception:
                active_count = len(new_watch_syms)
            state["_active_positions_total"] = int(active_count)
            state["_pos_limit_reached"] = bool(active_count >= er.MAX_OPEN_POSITIONS)
            try:
                api_set = set(new_watch_syms or [])
                for sym, st in list(state.items()):
                    if not isinstance(st, dict):
                        continue
                    if st.get("in_pos") and sym not in api_set:
                        st["in_pos_long"] = False
                        st["in_pos_short"] = False
                        st["in_pos"] = False
                        state[sym] = st
                if api_set:
                    er._sync_positions_state(state, list(api_set))
                    _force_in_pos_from_api(state, api_set)
                if api_set:
                    missing = sorted(
                        api_set
                        - {s for s, st in state.items() if isinstance(st, dict) and er._symbol_in_pos_any(st)}
                    )
                    if missing:
                        print(f"[manage-ws] api_inpos_missing_in_state={len(missing)} {missing}")
                    else:
                        longs = set((symbols.get("long") or set()))
                        shorts = set((symbols.get("short") or set()))
                        both = longs & shorts
                        total = len(longs) + len(shorts)
                        print(
                            "[manage-ws] api_inpos_sync_ok "
                            f"total={total} unique={len(api_set)} long={len(longs)} short={len(shorts)} both={len(both)}"
                        )
            except Exception:
                pass
            if new_watch_syms != watch_syms:
                if first_watch:
                    print(f"[manage-ws] watch_symbols={_format_symbol_list(new_watch_syms)}")
                    first_watch = False
                else:
                    added, removed = _diff_symbols(watch_syms, new_watch_syms)
                    if added:
                        print(f"[manage-ws] watch_add={_format_symbol_list(added)}")
                    if removed:
                        print(f"[manage-ws] watch_remove={_format_symbol_list(removed)}")
                        now_ts = time.time()
                        for sym in removed:
                            prev_long_amt = float(last_amt.get((sym, "long"), 0.0) or 0.0)
                            prev_short_amt = float(last_amt.get((sym, "short"), 0.0) or 0.0)
                            if prev_long_amt > 0:
                                _manual_close_long(state, sym, now_ts, report_ok=True, mark_px=_last_ws_close(sym))
                            if prev_short_amt > 0:
                                _manual_close_short(state, sym, now_ts, report_ok=True, mark_px=_last_ws_close(sym))
                watch_syms = new_watch_syms
            last_watch_ts = time.time()
            now_ts = last_watch_ts
            if (now_ts - last_pos_log_ts) >= 30.0:
                try:
                    pos_syms = executor_mod.list_open_position_symbols(force=True)
                except Exception:
                    pos_syms = {"long": set(), "short": set()}
                longs = set(pos_syms.get("long") or set())
                shorts = set(pos_syms.get("short") or set())
                both = longs & shorts
                total = len(longs) + len(shorts)
                unique = len(longs | shorts)
                print(
                    f"[manage-ws] pos_summary total={total} unique={unique} "
                    f"long={len(longs)} short={len(shorts)} both={len(both)}"
                )
                last_pos_log_ts = now_ts
        if (time.time() - last_state_save_ts) >= 5.0:
            try:
                if MANAGE_WS_WRITE_STATE:
                    er.save_state(state)
            except Exception:
                pass
            last_state_save_ts = time.time()
        now_ts = time.time()
        if (now_ts - last_sync_ts) >= 5.0:
            try:
                executor_mod.refresh_positions_cache(force=True)
            except Exception:
                pass
            last_sync_ts = now_ts
        if (now_ts - last_reconcile_ts) >= 10.0:
            tickers = _get_tickers(tickers_cache)
            er._reconcile_long_trades(state, cached_long_ex, tickers)
            er._reconcile_short_trades(state, tickers)
            last_reconcile_ts = now_ts
        for symbol in watch_syms:
            if not isinstance(symbol, str) or "/" not in symbol:
                continue
            long_amt = executor_mod.get_long_position_amount(symbol)
            short_amt = executor_mod.get_short_position_amount(symbol)
            st = state.get(symbol, {}) if isinstance(state, dict) else {}
            prev_long_amt = float(last_amt.get((symbol, "long"), 0.0) or 0.0)
            prev_short_amt = float(last_amt.get((symbol, "short"), 0.0) or 0.0)
            if long_amt > 0:
                st["zero_long_count_ws"] = 0
            if short_amt > 0:
                st["zero_short_count_ws"] = 0
            _maybe_update_open_trade_engine(state, symbol, "LONG", now_ts)
            _maybe_update_open_trade_engine(state, symbol, "SHORT", now_ts)
            open_long = er._get_open_trade(state, "LONG", symbol)
            if isinstance(open_long, dict):
                eng_label = _trade_engine_label(open_long)
                pending_key = "manual_entry_pending_long_ts"
                alert_key = "manual_entry_alerted_long"
                if long_amt <= 0:
                    st.pop(pending_key, None)
                    state[symbol] = st
                elif eng_label == "MANUAL" and not st.get(alert_key):
                    if _entry_alerted_in_state(st, "LONG"):
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "entry_alerted"
                        st.pop(pending_key, None)
                        state[symbol] = st
                        continue
                    if _backfill_engine_from_recent_event(state, symbol, "LONG", now_ts):
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "engine_backfill"
                        st.pop(pending_key, None)
                        state[symbol] = st
                        continue
                    if _is_startup_position(state, symbol):
                        _consume_startup_position(state, symbol)
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "startup_sync"
                        st.pop(pending_key, None)
                    else:
                        pending_ts = st.get(pending_key)
                        if not isinstance(pending_ts, (int, float)):
                            st[pending_key] = now_ts
                        elif (now_ts - float(pending_ts)) >= 10.0:
                            entry_price = open_long.get("entry_price")
                            sl_pct = er.AUTO_EXIT_LONG_SL_PCT
                            sl_price = er._fmt_price_safe(entry_price, sl_pct, side="LONG")
                            er._send_entry_alert(
                                er.send_telegram,
                                side="LONG",
                                symbol=symbol,
                                engine="MANUAL",
                                entry_price=entry_price,
                                usdt=None,
                                reason="manual_entry",
                                live=True,
                                order_info="(manage-ws)",
                                entry_order_id=open_long.get("entry_order_id"),
                                sl=sl_price,
                                tp=None,
                                state=state,
                            )
                            st[alert_key] = True
                            st[f"{alert_key}_ts"] = now_ts
                            st[f"{alert_key}_reason"] = "sent"
                else:
                    st.pop(pending_key, None)
                state[symbol] = st
            open_short = er._get_open_trade(state, "SHORT", symbol)
            if isinstance(open_short, dict):
                eng_label = _trade_engine_label(open_short)
                pending_key = "manual_entry_pending_short_ts"
                alert_key = "manual_entry_alerted_short"
                if short_amt <= 0:
                    st.pop(pending_key, None)
                    state[symbol] = st
                elif eng_label == "MANUAL" and not st.get(alert_key):
                    if _entry_alerted_in_state(st, "SHORT"):
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "entry_alerted"
                        st.pop(pending_key, None)
                        state[symbol] = st
                        continue
                    if _backfill_engine_from_recent_event(state, symbol, "SHORT", now_ts):
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "engine_backfill"
                        st.pop(pending_key, None)
                        state[symbol] = st
                        continue
                    if _is_startup_position(state, symbol):
                        _consume_startup_position(state, symbol)
                        st[alert_key] = True
                        st[f"{alert_key}_ts"] = now_ts
                        st[f"{alert_key}_reason"] = "startup_sync"
                        st.pop(pending_key, None)
                    else:
                        pending_ts = st.get(pending_key)
                        if not isinstance(pending_ts, (int, float)):
                            st[pending_key] = now_ts
                        elif (now_ts - float(pending_ts)) >= 10.0:
                            entry_price = open_short.get("entry_price")
                            sl_pct = er.AUTO_EXIT_SHORT_SL_PCT
                            sl_price = er._fmt_price_safe(entry_price, sl_pct, side="SHORT")
                            er._send_entry_alert(
                                er.send_telegram,
                                side="SHORT",
                                symbol=symbol,
                                engine="MANUAL",
                                entry_price=entry_price,
                                usdt=None,
                                reason="manual_entry",
                                live=True,
                                order_info="(manage-ws)",
                                entry_order_id=open_short.get("entry_order_id"),
                                sl=sl_price,
                                tp=None,
                                state=state,
                            )
                            st[alert_key] = True
                            st[f"{alert_key}_ts"] = now_ts
                            st[f"{alert_key}_reason"] = "sent"
                else:
                    st.pop(pending_key, None)
                state[symbol] = st
            if long_amt > 0:
                open_tr = er._get_open_trade(state, "LONG", symbol)
                if not open_tr:
                    detail = executor_mod.get_long_position_detail(symbol) or {}
                    entry_price = detail.get("entry") if isinstance(detail, dict) else None
                    qty = detail.get("qty") if isinstance(detail, dict) else None
                    meta = {"reason": "manual_entry"}
                    entry_order_id = None
                    recent = _get_recent_entry_event(symbol, "LONG", now_ts=now_ts)
                    if isinstance(recent, dict):
                        eng = str(recent.get("engine") or "").upper()
                        reason = _reason_from_engine_label(eng, "LONG")
                        if reason:
                            meta["reason"] = reason
                        meta["engine"] = eng
                        entry_order_id = recent.get("entry_order_id")
                    if "engine" not in meta:
                        alerted_eng = str(st.get("entry_alerted_long_engine") or "").upper()
                        alerted_ts = st.get("entry_alerted_long_ts")
                        if alerted_eng and alerted_eng not in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                            if isinstance(alerted_ts, (int, float)) and (now_ts - float(alerted_ts)) <= 600.0:
                                meta["engine"] = alerted_eng
                                reason = _reason_from_engine_label(alerted_eng, "LONG")
                                if reason:
                                    meta["reason"] = reason
                                entry_order_id = st.get("entry_alerted_long_order_id") or entry_order_id
                    er._log_trade_entry(
                        state,
                        side="LONG",
                        symbol=symbol,
                        entry_ts=now_ts,
                        entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=None,
                        entry_order_id=entry_order_id,
                        meta=meta,
                    )
                    er._set_in_pos_side(st, "LONG", True)
                    st["last_entry"] = now_ts
                    engine_label = str(meta.get("engine") or "MANUAL").upper()
                    if engine_label in ("", "UNKNOWN"):
                        engine_label = "MANUAL"
                    alert_key = "manual_entry_alerted_long"
                    pending_key = "manual_entry_pending_long_ts"
                    if engine_label == "MANUAL" and not st.get(alert_key):
                        if _entry_alerted_in_state(st, "LONG"):
                            st[alert_key] = True
                            st[f"{alert_key}_ts"] = now_ts
                            st[f"{alert_key}_reason"] = "entry_alerted"
                            st.pop(pending_key, None)
                        else:
                            if _consume_startup_position(state, symbol):
                                st[alert_key] = True
                                st[f"{alert_key}_ts"] = now_ts
                                st[f"{alert_key}_reason"] = "startup_sync"
                                st.pop(pending_key, None)
                            else:
                                pending_ts = st.get(pending_key)
                                if not isinstance(pending_ts, (int, float)):
                                    st[pending_key] = now_ts
                                elif (now_ts - float(pending_ts)) >= 10.0:
                                    sl_pct = er.AUTO_EXIT_LONG_SL_PCT
                                    sl_price = er._fmt_price_safe(entry_price, sl_pct, side="LONG")
                                    er._send_entry_alert(
                                        er.send_telegram,
                                        side="LONG",
                                        symbol=symbol,
                                        engine=engine_label,
                                        entry_price=entry_price,
                                        usdt=None,
                                        reason="manual_entry",
                                        live=True,
                                        order_info="(manage-ws)",
                                        entry_order_id=entry_order_id,
                                        sl=sl_price,
                                        tp=None,
                                        state=state,
                                    )
                                    st[alert_key] = True
                                    st[f"{alert_key}_ts"] = now_ts
                                    st[f"{alert_key}_reason"] = "sent"
                    else:
                        st.pop(pending_key, None)
                    state[symbol] = st
            if short_amt > 0:
                open_tr = er._get_open_trade(state, "SHORT", symbol)
                entry_price = None
                qty = None
                meta = {"reason": "manual_entry"}
                entry_order_id = None
                if not open_tr:
                    detail = executor_mod.get_short_position_detail(symbol) or {}
                    entry_price = detail.get("entry") if isinstance(detail, dict) else None
                    qty = detail.get("qty") if isinstance(detail, dict) else None
                    recent = _get_recent_entry_event(symbol, "SHORT", now_ts=now_ts)
                    if isinstance(recent, dict):
                        eng = str(recent.get("engine") or "").upper()
                        reason = _reason_from_engine_label(eng, "SHORT")
                        if reason:
                            meta["reason"] = reason
                        meta["engine"] = eng
                        entry_order_id = recent.get("entry_order_id")
                    if "engine" not in meta:
                        alerted_eng = str(st.get("entry_alerted_short_engine") or "").upper()
                        alerted_ts = st.get("entry_alerted_short_ts")
                        if alerted_eng and alerted_eng not in ("UNKNOWN", "MANUAL", "MANUAL_ENTRY"):
                            if isinstance(alerted_ts, (int, float)) and (now_ts - float(alerted_ts)) <= 600.0:
                                meta["engine"] = alerted_eng
                                reason = _reason_from_engine_label(alerted_eng, "SHORT")
                                if reason:
                                    meta["reason"] = reason
                                entry_order_id = st.get("entry_alerted_short_order_id") or entry_order_id
                    er._log_trade_entry(
                        state,
                        side="SHORT",
                        symbol=symbol,
                        entry_ts=now_ts,
                        entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
                        qty=qty if isinstance(qty, (int, float)) else None,
                        usdt=None,
                        entry_order_id=entry_order_id,
                        meta=meta,
                    )
                    er._set_in_pos_side(st, "SHORT", True)
                    st["last_entry"] = now_ts
                    engine_label = str(meta.get("engine") or "MANUAL").upper()
                    if engine_label in ("", "UNKNOWN"):
                        engine_label = "MANUAL"
                    alert_key = "manual_entry_alerted_short"
                    pending_key = "manual_entry_pending_short_ts"
                    if engine_label == "MANUAL" and not st.get(alert_key):
                        if _entry_alerted_in_state(st, "SHORT"):
                            st[alert_key] = True
                            st[f"{alert_key}_ts"] = now_ts
                            st[f"{alert_key}_reason"] = "entry_alerted"
                            st.pop(pending_key, None)
                        else:
                            if _consume_startup_position(state, symbol):
                                st[alert_key] = True
                                st[f"{alert_key}_ts"] = now_ts
                                st[f"{alert_key}_reason"] = "startup_sync"
                                st.pop(pending_key, None)
                            else:
                                pending_ts = st.get(pending_key)
                                if not isinstance(pending_ts, (int, float)):
                                    st[pending_key] = now_ts
                                elif (now_ts - float(pending_ts)) >= 10.0:
                                    sl_pct = er.AUTO_EXIT_SHORT_SL_PCT
                                    sl_price = er._fmt_price_safe(entry_price, sl_pct, side="SHORT")
                                    er._send_entry_alert(
                                        er.send_telegram,
                                        side="SHORT",
                                        symbol=symbol,
                                        engine=engine_label,
                                        entry_price=entry_price,
                                        usdt=None,
                                        reason="manual_entry",
                                        live=True,
                                        order_info="(manage-ws)",
                                        entry_order_id=entry_order_id,
                                        sl=sl_price,
                                        tp=None,
                                        state=state,
                                    )
                                    st[alert_key] = True
                                    st[f"{alert_key}_ts"] = now_ts
                                    st[f"{alert_key}_reason"] = "sent"
                    else:
                        st.pop(pending_key, None)
                    state[symbol] = st
            if long_amt > 0:
                st["long_pos_seen_ts"] = now_ts
            if short_amt > 0:
                st["short_pos_seen_ts"] = now_ts
            if prev_long_amt > 0 and long_amt <= 0:
                try:
                    executor_mod.refresh_positions_cache(force=True)
                    if executor_mod.get_long_position_amount(symbol) > 0:
                        long_amt = executor_mod.get_long_position_amount(symbol)
                except Exception:
                    pass
                if long_amt <= 0:
                    zero_cnt = int(st.get("zero_long_count_ws", 0)) + 1
                    st["zero_long_count_ws"] = zero_cnt
                    if zero_cnt >= 2:
                        _manual_close_long(state, symbol, now_ts, report_ok=True, mark_px=_last_ws_close(symbol))
            if prev_short_amt > 0 and short_amt <= 0:
                try:
                    executor_mod.refresh_positions_cache(force=True)
                    if executor_mod.get_short_position_amount(symbol) > 0:
                        short_amt = executor_mod.get_short_position_amount(symbol)
                except Exception:
                    pass
                if short_amt <= 0:
                    zero_cnt = int(st.get("zero_short_count_ws", 0)) + 1
                    st["zero_short_count_ws"] = zero_cnt
                    if zero_cnt >= 2:
                        _manual_close_short(state, symbol, now_ts, report_ok=True, mark_px=_last_ws_close(symbol))
            state[symbol] = st
            last_amt[(symbol, "long")] = float(long_amt or 0.0)
            last_amt[(symbol, "short")] = float(short_amt or 0.0)
            if not er.AUTO_EXIT_ENABLED:
                continue
            mark_px = _last_ws_close(symbol)
            if long_amt > 0:
                detail = executor_mod.get_long_position_detail(symbol) or {}
                if mark_px is None:
                    mark_px = detail.get("mark")
                if isinstance(mark_px, (int, float)):
                    closed = _handle_long_tp(state, symbol, detail, mark_px, now_ts)
                    if not closed:
                        _handle_long_sl(state, symbol, detail, mark_px, now_ts)
                st = state.get(symbol, {})
                last_eval = float(st.get("manage_eval_long_ts", 0.0) or 0.0)
                if (now_ts - last_eval) >= er.MANAGE_EVAL_COOLDOWN_SEC:
                    st["manage_eval_long_ts"] = now_ts
                    state[symbol] = st
                    adds_done = int(st.get("dca_adds_long", st.get("dca_adds", 0)))
                    dca_res = executor_mod.dca_long_if_needed(
                        symbol, adds_done=adds_done, margin_mode=er.MARGIN_MODE
                    )
                    if dca_res.get("status") not in ("skip", "warn"):
                        st["dca_adds_long"] = adds_done + 1
                        state[symbol] = st
                        er.send_telegram(
                            f"➕ <b>DCA</b> {symbol} LONG adds {adds_done}->{adds_done+1} "
                            f"mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                        )
                # TODO(manage-ws): 추가 롱 청산 조건(현재 주석 처리)
                # - SL/보호 조건
                # - 추가 리스크 기반 exit
            if short_amt > 0:
                detail = executor_mod.get_short_position_detail(symbol) or {}
                if mark_px is None:
                    mark_px = detail.get("mark")
                if isinstance(mark_px, (int, float)):
                    closed = _handle_short_tp(state, symbol, detail, mark_px, now_ts)
                    if not closed:
                        _handle_short_sl(state, symbol, detail, mark_px, now_ts)
                # TODO(manage-ws): 추가 숏 청산 조건(현재 주석 처리)
                # - SL/보호 조건
                # - 추가 리스크 기반 exit
                st = state.get(symbol, {})
                last_eval = float(st.get("manage_eval_ts", 0.0) or 0.0)
                if (now_ts - last_eval) >= er.MANAGE_EVAL_COOLDOWN_SEC:
                    st["manage_eval_ts"] = now_ts
                    state[symbol] = st
                    adds_done = int(st.get("dca_adds_short", st.get("dca_adds", 0)))
                    dca_res = executor_mod.dca_short_if_needed(
                        symbol, adds_done=adds_done, margin_mode=er.MARGIN_MODE
                    )
                    if dca_res.get("status") not in ("skip", "warn"):
                        st["dca_adds_short"] = adds_done + 1
                        state[symbol] = st
                        er.send_telegram(
                            f"➕ <b>DCA</b> {symbol} adds {adds_done}->{adds_done+1} "
                            f"mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                        )
        time.sleep(1.0)


if __name__ == "__main__":
    main()
