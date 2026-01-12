"""manage_ws.py
WebSocket 기반 관리 모듈(테스트용).

특징
- ws_manager 5m kline 캐시를 이용해 빠르게 TP 판단
- 기존 관리 모듈과 별개로 실행(엔진/메인 루프 무수정)
"""
import time
import os
import json

import ws_manager
import executor as executor_mod
import engine_runner as er


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


def _drain_entry_events(state) -> None:
    path = os.path.join("logs", "entry_events.log")
    try:
        if not os.path.exists(path):
            return
        offset = int(state.get("_entry_event_offset", 0) or 0)
        with open(path, "r", encoding="utf-8") as f:
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
            if not isinstance(entry_ts, (int, float)):
                continue
            tr = {
                "entry_ts": float(entry_ts),
                "entry_order_id": payload.get("entry_order_id"),
                "symbol": payload.get("symbol"),
                "side": payload.get("side"),
                "entry_price": payload.get("entry_price"),
                "qty": payload.get("qty"),
                "engine_label": payload.get("engine") or "UNKNOWN",
                "exit_ts": None,
                "exit_price": None,
                "pnl_usdt": None,
                "exit_reason": "",
                "roi_pct": None,
            }
            er._update_report_csv(tr)
            sym = tr.get("symbol")
            side = (tr.get("side") or "").lower()
            if sym and side:
                st = state.get(sym, {})
                if not isinstance(st, dict):
                    st = {}
                st[f"entry_order_id_{side}"] = tr.get("entry_order_id")
                state[sym] = st
        state["_entry_event_offset"] = new_offset
    except Exception as e:
        print("[manage-ws] entry_events drain error:", e)


def _manual_close_long(state, symbol, now_ts, report_ok: bool = True):
    open_tr = er._get_open_trade(state, "LONG", symbol)
    if not open_tr:
        return
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    last_exit_ts = st.get("last_exit_ts") if isinstance(st, dict) else None
    last_exit_reason = st.get("last_exit_reason") if isinstance(st, dict) else None
    if (
        isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl")
    ):
        return
    engine_label = er._engine_label_from_reason((open_tr.get("meta") or {}).get("reason"))
    if engine_label == "UNKNOWN":
        return
    if not open_tr.get("entry_ts") and not open_tr.get("entry_ts_ms") and not open_tr.get("entry_price"):
        return
    st = state.get(symbol, {})
    seen_ts = st.get("long_pos_seen_ts") if isinstance(st, dict) else None
    recent_seen = False
    if isinstance(seen_ts, (int, float)):
        recent_seen = (now_ts - float(seen_ts)) <= er.SHORT_RECONCILE_SEEN_TTL_SEC
    last_fill_ts = open_tr.get("entry_ts_ms") or (open_tr.get("meta") or {}).get("last_fill_ts")
    recent_fill = False
    if isinstance(last_fill_ts, (int, float)) and last_fill_ts > 0:
        ts_val = float(last_fill_ts)
        if ts_val > 10_000_000_000:
            ts_val = ts_val / 1000.0
        if (now_ts - ts_val) <= er.SHORT_RECONCILE_SEEN_TTL_SEC:
            recent_fill = True
    if not (recent_seen or recent_fill):
        return
    er._close_trade(
        state,
        side="LONG",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=None,
        pnl_usdt=None,
        reason="manual_close",
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    st["in_pos"] = False
    st["last_ok"] = False
    st["dca_adds"] = 0
    state[symbol] = st
    if report_ok:
        er._update_report_csv(open_tr)
    print(f"[manage-ws] long_manual_close sym={symbol} engine={engine_label}")
    order_block = er._format_order_id_block(open_tr.get("entry_order_id"), open_tr.get("exit_order_id"))
    order_line = f"{order_block}\n" if order_block else ""
    er.send_telegram(
        f"🔴 <b>롱 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {engine_label}\n"
        f"사유: MANUAL\n"
        f"{order_line}".rstrip()
    )


def _manual_close_short(state, symbol, now_ts, report_ok: bool = True):
    open_tr = er._get_open_trade(state, "SHORT", symbol)
    if not open_tr:
        return
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    last_exit_ts = st.get("last_exit_ts") if isinstance(st, dict) else None
    last_exit_reason = st.get("last_exit_reason") if isinstance(st, dict) else None
    if (
        isinstance(last_exit_ts, (int, float))
        and (now_ts - float(last_exit_ts)) <= er.MANUAL_CLOSE_GRACE_SEC
        and last_exit_reason in ("auto_exit_tp", "auto_exit_sl")
    ):
        return
    engine_label = er._engine_label_from_reason((open_tr.get("meta") or {}).get("reason"))
    if engine_label == "UNKNOWN":
        return
    if not open_tr.get("entry_ts") and not open_tr.get("entry_ts_ms") and not open_tr.get("entry_price"):
        return
    st = state.get(symbol, {})
    seen_ts = st.get("short_pos_seen_ts") if isinstance(st, dict) else None
    recent_seen = False
    if isinstance(seen_ts, (int, float)):
        recent_seen = (now_ts - float(seen_ts)) <= er.SHORT_RECONCILE_SEEN_TTL_SEC
    last_fill_ts = open_tr.get("entry_ts_ms") or (open_tr.get("meta") or {}).get("last_fill_ts")
    recent_fill = False
    if isinstance(last_fill_ts, (int, float)) and last_fill_ts > 0:
        ts_val = float(last_fill_ts)
        if ts_val > 10_000_000_000:
            ts_val = ts_val / 1000.0
        if (now_ts - ts_val) <= er.SHORT_RECONCILE_SEEN_TTL_SEC:
            recent_fill = True
    if not (recent_seen or recent_fill):
        return
    er._close_trade(
        state,
        side="SHORT",
        symbol=symbol,
        exit_ts=now_ts,
        exit_price=None,
        pnl_usdt=None,
        reason="manual_close",
    )
    st = state.get(symbol, {}) if isinstance(state, dict) else {}
    st["in_pos"] = False
    st["last_ok"] = False
    st["dca_adds"] = 0
    state[symbol] = st
    if report_ok:
        er._update_report_csv(open_tr)
    print(f"[manage-ws] short_manual_close sym={symbol} engine={engine_label}")
    order_block = er._format_order_id_block(open_tr.get("entry_order_id"), open_tr.get("exit_order_id"))
    order_line = f"{order_block}\n" if order_block else ""
    er.send_telegram(
        f"🔴 <b>숏 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {engine_label}\n"
        f"사유: MANUAL\n"
        f"{order_line}".rstrip()
    )


def _handle_long_tp(state, symbol, detail, mark_px, now_ts):
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return
    profit_unlev = (float(mark_px) - float(entry_px)) / float(entry_px) * 100.0
    if profit_unlev < er.AUTO_EXIT_LONG_TP_PCT:
        return
    open_tr = er._get_open_trade(state, "LONG", symbol)
    engine_label = er._engine_label_from_reason(
        (open_tr.get("meta") or {}).get("reason") if open_tr else None
    )
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
    order_block = er._format_order_id_block(
        open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
        exit_order_id,
    )
    order_line = f"{order_block}\n" if order_block else ""
    er.send_telegram(
        f"🟢 <b>롱 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {engine_label}\n"
        f"사유: TP\n"
        f"{order_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_long is None else f' 손익={pnl_long:+.3f} USDT'}"
    )


def _handle_short_tp(state, symbol, detail, mark_px, now_ts):
    entry_px = detail.get("entry")
    if not isinstance(entry_px, (int, float)) or entry_px <= 0:
        return
    profit_unlev = (float(entry_px) - float(mark_px)) / float(entry_px) * 100.0
    if profit_unlev < er.AUTO_EXIT_SHORT_TP_PCT:
        return
    open_tr = er._get_open_trade(state, "SHORT", symbol)
    engine_label = er._engine_label_from_reason(
        (open_tr.get("meta") or {}).get("reason") if open_tr else None
    )
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
    order_block = er._format_order_id_block(
        open_tr.get("entry_order_id") if isinstance(open_tr, dict) else None,
        exit_order_id,
    )
    order_line = f"{order_block}\n" if order_block else ""
    er.send_telegram(
        f"✅ <b>숏 청산</b>\n"
        f"<b>{symbol}</b>\n"
        f"엔진: {engine_label}\n"
        f"사유: TP\n"
        f"{order_line}"
        f"체결가={avg_price} 수량={filled} 비용={cost}\n"
        f"진입가={entry_px} 현재가={mark_px} 수익률={profit_unlev:.2f}%"
        f"{'' if pnl_short is None else f' 손익={pnl_short:+.3f} USDT'}"
    )


def main():
    er.MANAGE_WS_MODE = True
    er.SUPPRESS_RECONCILE_ALERTS = False
    er._install_error_hooks()
    state = er.load_state()
    er._reload_runtime_settings_from_disk(state)
    state["_manage_ws_mode"] = True
    if ws_manager and ws_manager.is_available():
        ws_manager.start()
    else:
        print("[manage-ws] ws_manager unavailable")
    last_sync_ts = 0.0
    last_cfg_save_ts = 0.0
    last_watch_ts = 0.0
    last_reconcile_ts = 0.0
    watch_syms = []
    first_watch = True
    last_amt = {}
    tickers_cache = {"ts": 0.0, "tickers": {}}
    cached_long_ex = er.CachedExchange(executor_mod.exchange)
    while True:
        _drain_entry_events(state)
        er.handle_telegram_commands(state)
        if (time.time() - last_cfg_save_ts) >= 2.0:
            try:
                er._save_runtime_settings_only(state)
            except Exception:
                pass
            last_cfg_save_ts = time.time()
        if (time.time() - last_watch_ts) >= 5.0:
            new_watch_syms = _update_watch_symbols()
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
                watch_syms = new_watch_syms
            last_watch_ts = time.time()
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
            long_amt = executor_mod.get_long_position_amount(symbol)
            short_amt = executor_mod.get_short_position_amount(symbol)
            st = state.get(symbol, {}) if isinstance(state, dict) else {}
            prev_long_amt = float(last_amt.get((symbol, "long"), 0.0) or 0.0)
            prev_short_amt = float(last_amt.get((symbol, "short"), 0.0) or 0.0)
            if long_amt > 0:
                st["long_pos_seen_ts"] = now_ts
            if short_amt > 0:
                st["short_pos_seen_ts"] = now_ts
            if prev_long_amt > 0 and long_amt <= 0:
                _manual_close_long(state, symbol, now_ts, report_ok=True)
            if prev_short_amt > 0 and short_amt <= 0:
                _manual_close_short(state, symbol, now_ts, report_ok=True)
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
                    _handle_long_tp(state, symbol, detail, mark_px, now_ts)
                # TODO(manage-ws): 추가 롱 청산 조건(현재 주석 처리)
                # - SL/보호 조건
                # - 추가 리스크 기반 exit
            if short_amt > 0:
                detail = executor_mod.get_short_position_detail(symbol) or {}
                if mark_px is None:
                    mark_px = detail.get("mark")
                if isinstance(mark_px, (int, float)):
                    _handle_short_tp(state, symbol, detail, mark_px, now_ts)
                # TODO(manage-ws): 추가 숏 청산 조건(현재 주석 처리)
                # - SL/보호 조건
                # - 추가 리스크 기반 exit
                st = state.get(symbol, {})
                last_eval = float(st.get("manage_eval_ts", 0.0) or 0.0)
                if (now_ts - last_eval) >= er.MANAGE_EVAL_COOLDOWN_SEC:
                    st["manage_eval_ts"] = now_ts
                    state[symbol] = st
                    adds_done = int(st.get("dca_adds", 0))
                    dca_res = executor_mod.dca_short_if_needed(
                        symbol, adds_done=adds_done, margin_mode=er.MARGIN_MODE
                    )
                    if dca_res.get("status") not in ("skip", "warn"):
                        st["dca_adds"] = adds_done + 1
                        state[symbol] = st
                        er.send_telegram(
                            f"➕ <b>DCA</b> {symbol} adds {adds_done}->{adds_done+1} "
                            f"mark={dca_res.get('mark')} entry={dca_res.get('entry')} usdt={dca_res.get('dca_usdt')}"
                        )
        time.sleep(1.0)


if __name__ == "__main__":
    main()
