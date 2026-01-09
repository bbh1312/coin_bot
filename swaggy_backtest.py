#!/usr/bin/env python3
import argparse
import os
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Any, Dict, List

import ccxt
import pandas as pd

from engines.swaggy import run as swaggy_run
from engines.swaggy.event_writer import JsonlWriter
from engines.swaggy.logs import format_cut_top, format_zone_stats
from engines.swaggy.swaggy_engine import SwaggyEngine, SwaggyConfig


def _parse_ts(text: str) -> int:
    if text.isdigit():
        return int(text)
    t = text.strip().replace("T", " ")
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _fetch_ohlcv_all(ex, symbol: str, tf: str, start_ms: int, end_ms: int, limit: int = 1000) -> List[list]:
    out: List[list] = []
    since = start_ms
    while True:
        batch = ex.fetch_ohlcv(symbol, tf, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if ts > end_ms:
                return out
            out.append(row)
        last_ts = int(batch[-1][0])
        if last_ts <= since:
            break
        since = last_ts + 1
        if last_ts >= end_ms:
            break
        time.sleep(ex.rateLimit / 1000.0)
    return out


def _to_df(rows: List[list]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df


def _slice_df(df: pd.DataFrame, ts_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["ts"] <= ts_ms]


def _print_header(symbol: str, start_ms: int, end_ms: int) -> None:
    start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    print(f"[swaggy-backtest] sym={symbol} start={start} end={end}")


def run_backtest(symbol: str, start_ms: int, end_ms: int, cfg: SwaggyConfig) -> None:
    ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    data_5m = _fetch_ohlcv_all(ex, symbol, cfg.tf_ltf, start_ms, end_ms)
    data_15m = _fetch_ohlcv_all(ex, symbol, cfg.tf_mtf, start_ms, end_ms)
    data_1h = _fetch_ohlcv_all(ex, symbol, cfg.tf_htf, start_ms, end_ms)
    data_4h = _fetch_ohlcv_all(ex, symbol, cfg.tf_htf2, start_ms, end_ms)

    df_5m = _to_df(data_5m)
    df_15m = _to_df(data_15m)
    df_1h = _to_df(data_1h)
    df_4h = _to_df(data_4h)
    if df_5m.empty:
        print("[swaggy-backtest] empty data")
        return

    engine = SwaggyEngine(cfg)
    engine.begin_cycle()
    state: Dict[str, Any] = {}
    entry_count = 0
    cycle = 0
    summary_every = int(os.getenv("SWAGGY_SUMMARY_EVERY", "50"))
    window_eval = int(os.getenv("SWAGGY_WINDOW_EVAL", "500"))
    window_reason = int(os.getenv("SWAGGY_WINDOW_REASON", "2000"))
    cut_mode = swaggy_run._cut_mode()
    debug_on = os.getenv("SWAGGY_DEBUG", "0").strip() == "1"
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(
        swaggy_run.ROOT_DIR,
        "logs",
        "swaggy",
        f"swaggy_backtest_{symbol.replace('/', '').replace(':', '')}_{date_tag}.jsonl",
    )
    writer = JsonlWriter(log_path)
    reason_window = deque(maxlen=window_reason)
    gate_fail_window = deque(maxlen=window_reason)
    gate_stage_window = deque(maxlen=window_reason)
    ready_fail_window = deque(maxlen=window_reason)
    trigger_window = deque(maxlen=window_eval)
    strength_window = deque(maxlen=window_eval)
    funnel_window = deque(maxlen=window_reason)
    for idx in range(30, len(df_5m)):
        cycle += 1
        cur = df_5m.iloc[idx]
        ts_ms = int(cur["ts"])
        now_ts = ts_ms / 1000.0
        d5 = df_5m.iloc[: idx + 1]
        d15 = _slice_df(df_15m, ts_ms)
        d1h = _slice_df(df_1h, ts_ms)
        d4h = _slice_df(df_4h, ts_ms)
        print(f"[swaggy-scan] mode=replay tf={cfg.tf_ltf} cycle={cycle} ts={ts_ms} syms=1 evaluated=1")
        decision = engine.evaluate_symbol(
            sym=symbol,
            candles_4h=d4h,
            candles_1h=d1h,
            candles_15m=d15,
            candles_5m=d5,
            position_state=state,
            engine_config=cfg,
            now_ts=now_ts,
        )
        reason = swaggy_run._decision_reason(decision)
        dbg = decision.debug or {}
        trigger_kind = dbg.get("trigger", "-")
        strength = dbg.get("strength", 0.0) if isinstance(dbg.get("strength"), (int, float)) else 0.0
        dist_pct = dbg.get("dist_pct", 0.0) if isinstance(dbg.get("dist_pct"), (int, float)) else 0.0
        regime = dbg.get("regime", "-")
        in_lvn_gap = 1 if decision.filters.get("lvn_gap") == 1 else 0
        trigger_debug = (dbg.get("trigger_debug") or {}) if isinstance(dbg.get("trigger_debug"), dict) else {}
        if debug_on and trigger_debug:
            print(
                "[swaggy-trigger-debug] "
                f"sym={symbol} side={decision.side} px={swaggy_run.last_price(cur):.6g} level={trigger_debug.get('level', '-')}\n"
                f"trigger_candidate="
                f"reclaim={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection={trigger_debug.get('rejection_ok', 0)} "
                f"sweep={trigger_debug.get('sweep_ok', 0)} "
                f"retest={trigger_debug.get('retest_ok', 0)}\n"
                f"candle_ref={trigger_debug.get('candle_ref', '-')}"
                f" c1_close={trigger_debug.get('c1_close')} c2_close={trigger_debug.get('c2_close')} "
                f"c3_close={trigger_debug.get('c3_close')}\n"
                f"reclaim_ok={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection_ok={trigger_debug.get('rejection_ok', 0)} "
                f"sweep_ok={trigger_debug.get('sweep_ok', 0)}\n"
                f"wick_ratio={trigger_debug.get('wick_ratio')} vol={trigger_debug.get('vol')} "
                f"vol_sma={trigger_debug.get('vol_sma')}"
            )
        funnel = swaggy_run._funnel_counts(decision, trigger_kind)
        print(
            "[swaggy-funnel] "
            f"total={funnel['total']} in_pos={funnel['in_pos']} cooldown={funnel['cooldown']} "
            f"no_level={funnel['no_level']} no_touch={funnel['no_touch']} "
            f"touched={funnel['touched']} no_trigger={funnel['no_trigger']} "
            f"trigger={funnel['trigger']} filter_fail={funnel['filter_fail']} "
            f"filters_pass={funnel['filters_pass']} entry_ready={funnel['entry_ready']}"
        )
        reason_window.append(reason)
        gate_fail_window.append(decision.gate_fail or reason)
        gate_stage_window.append(decision.gate_stage or "")
        if not decision.entry_ready and decision.gate_fail:
            ready_fail_window.append(decision.gate_fail)
        funnel_window.append(funnel)
        if trigger_kind in {"SWEEP", "RECLAIM", "REJECTION", "RETEST"}:
            trigger_window.append(trigger_kind)
            strength_window.append(float(strength))
        if decision.entry_ready:
            entry_count += 1
            level = dbg.get("level", "-")
            bucket = swaggy_run._strength_bucket(float(strength))
            print(
                f"[swaggy-entry] sym={symbol} side={decision.side} px={swaggy_run.last_price(cur):.6g} level={level}\n"
                f"trigger={trigger_kind} strength={strength:.2f} bucket={bucket} "
                f"dist_pct={swaggy_run._fmt_dist(dist_pct)} regime={regime} in_lvn_gap={in_lvn_gap}\n"
                f"entry={decision.entry_px:.6g} sl={decision.sl_px:.6g} "
                f"tp1={decision.tp1_px} tp2={decision.tp2_px} reason=ENTRY_READY"
            )
            writer.write(swaggy_run._entry_event(
                symbol,
                cfg,
                "replay",
                cycle,
                decision,
                trigger_kind,
                strength,
                bucket,
                dist_pct,
                regime,
                in_lvn_gap,
                ts_ms,
            ))
        elif debug_on or swaggy_run._should_log_cut(cut_mode, decision):
            extra = ""
            if reason == "NO_TRIGGER" and isinstance(decision.evidence, dict):
                if decision.evidence.get("no_trigger_log_sample"):
                    extra = " " + swaggy_run._fmt_no_trigger(decision.evidence)
            if reason == "SWEEP_OBSERVE" and isinstance(decision.evidence, dict):
                if decision.evidence.get("sweep_log_sample"):
                    extra = " " + swaggy_run._fmt_sweep_observe(decision.evidence)
            print(
                f"[swaggy-cut] sym={symbol} side={decision.side} px={swaggy_run.last_price(cur):.6g} "
                f"level={dbg.get('level', '-')}\n"
                f"trigger={trigger_kind} strength={strength:.2f} dist_pct={swaggy_run._fmt_dist(dist_pct)}\n"
                f"filters: dist={decision.filters.get('dist', 0)} lvn_gap={decision.filters.get('lvn_gap', 0)} "
                f"expansion={decision.filters.get('expansion', 0)} cooldown={decision.filters.get('cooldown', 0)} "
                f"regime={decision.filters.get('regime', 0)}\n"
                f"reason={reason} gate_stage={decision.gate_stage} gate_fail={decision.gate_fail} "
                f"gate_detail={decision.gate_fail_detail}{extra}"
            )
            if swaggy_run._should_write_cut(cut_mode, decision):
                writer.write(swaggy_run._cut_event(
                    symbol,
                    cfg,
                    "replay",
                    cycle,
                    decision,
                    trigger_kind,
                    strength,
                    dist_pct,
                    regime,
                    in_lvn_gap,
                    ts_ms,
                ))
        elif debug_on:
            print(
                swaggy_run.format_debug(
                    symbol,
                    swaggy_run.last_price(cur),
                    dbg.get("regime", "-"),
                    float(dbg.get("level", 0.0) or 0.0),
                    dbg.get("vp", {}) if isinstance(dbg.get("vp"), dict) else {},
                    dbg.get("trigger", "-"),
                    dbg.get("side", "-"),
                    float(strength or 0.0),
                    float(dist_pct or 0.0),
                    1 if decision.reason_codes == ["LVN_GAP"] else 0,
                    1 if decision.reason_codes == ["EXPANSION_BAR"] else 0,
                    f"ENTRY_READY={decision.entry_ready}",
                    reason,
                )
            )
        state.update(engine._state.get(symbol, {}))
        if cycle % summary_every == 0:
            swaggy_run._print_summary(
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
            )
            writer.write(swaggy_run._summary_event(
                cfg,
                "replay",
                cycle,
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
                ts_ms,
            ))
    if entry_count == 0:
        print(format_cut_top(engine.get_trigger_counts(), engine.get_filter_counts(), engine.get_gate_fail_counts(), engine.get_gate_stage_counts()))
    print(format_zone_stats(engine.get_stats(), engine.get_ready_fail_counts()))


def main() -> None:
    parser = argparse.ArgumentParser(description="Swaggy backtest runner")
    parser.add_argument("--symbol", action="append", help="repeatable, e.g. --symbol JASMY/USDT:USDT")
    parser.add_argument("--symbols", help="comma-separated symbols")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD or epoch ms")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD or epoch ms")
    args = parser.parse_args()

    start_ms = _parse_ts(args.start)
    end_ms = _parse_ts(args.end)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    symbols: List[str] = []
    if args.symbol:
        symbols.extend([s.strip() for s in args.symbol if s and s.strip()])
    if args.symbols:
        symbols.extend([s.strip() for s in args.symbols.split(",") if s.strip()])
    if not symbols:
        raise SystemExit("symbol(s) required")

    cfg = SwaggyConfig()
    for symbol in symbols:
        _print_header(symbol, start_ms, end_ms)
        run_backtest(symbol, start_ms, end_ms, cfg)


if __name__ == "__main__":
    main()
