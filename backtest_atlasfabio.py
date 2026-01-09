import argparse
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import ccxt

import cycle_cache
import engine_runner
from engines.fabio import fabio_entry_engine, atlas_fabio_engine
from engines.rsi.engine import RsiEngine


def _parse_datetime(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("datetime value is empty")
    if raw.isdigit():
        num = int(raw)
        if num > 1_000_000_000_000:
            return num
        return num * 1000
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        dt = datetime.strptime(raw, "%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _tf_to_ms(tf: str) -> int:
    tf = (tf or "").lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60 * 1000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60 * 1000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60 * 1000
    return 0


def _warmup_start_ms(start_ms: int, tf: str, min_bars: int) -> int:
    tf_ms = _tf_to_ms(tf)
    if tf_ms <= 0:
        return start_ms
    lookback = max(0, int(min_bars) - 1)
    return max(0, start_ms - (lookback * tf_ms))


def _fetch_ohlcv_range(ex, symbol: str, tf: str, since_ms: int, end_ms: int) -> List[list]:
    out: List[list] = []
    tf_ms = _tf_to_ms(tf)
    since = since_ms
    while True:
        batch = ex.fetch_ohlcv(symbol, tf, since=since, limit=1000)
        if not batch:
            break
        out.extend(batch)
        last_ts = batch[-1][0]
        if last_ts >= end_ms:
            break
        if len(batch) < 2:
            break
        since = last_ts + max(tf_ms, 1)
        time.sleep(0.05)
    return [row for row in out if row[0] <= end_ms]


def _get_available_range(ex, symbol: str, tf: str) -> Tuple[Optional[int], Optional[int]]:
    try:
        first = ex.fetch_ohlcv(symbol, tf, since=0, limit=1)
    except Exception:
        first = []
    try:
        last = ex.fetch_ohlcv(symbol, tf, limit=1)
    except Exception:
        last = []
    start_ts = first[0][0] if first else None
    end_ts = last[0][0] if last else None
    return start_ts, end_ts


def _build_dir_hint(
    symbols: List[str],
    data_by_tf: Dict[Tuple[str, str], List[list]],
    ltf: str,
    mode: str,
    long_syms: Optional[List[str]] = None,
    short_syms: Optional[List[str]] = None,
) -> Dict[str, str]:
    hint: Dict[str, str] = {}
    mode = (mode or "auto").lower()
    long_set = set(long_syms or [])
    short_set = set(short_syms or [])
    if long_set or short_set:
        for sym in symbols:
            if sym in long_set:
                hint[sym] = "LONG"
            elif sym in short_set:
                hint[sym] = "SHORT"
        return hint
    if mode == "long":
        return {s: "LONG" for s in symbols}
    if mode == "short":
        return {s: "SHORT" for s in symbols}
    for sym in symbols:
        data = data_by_tf.get((sym, ltf)) or []
        if len(data) < 2:
            hint[sym] = "LONG"
            print(f"[backtest] {sym} dir_hint fallback=LONG (insufficient {ltf} data)")
            continue
        first = float(data[0][4])
        last = float(data[-1][4])
        hint[sym] = "LONG" if last >= first else "SHORT"
    return hint


def _min_required_bars(atlas_cfg, fabio_cfg) -> Dict[str, int]:
    min_map: Dict[str, int] = {}
    min_map[atlas_cfg.htf_tf] = max(atlas_cfg.supertrend_period + 2, 20) + 1
    min_map[atlas_cfg.ltf_tf] = max(atlas_cfg.atr_period + atlas_cfg.atr_sma_period, 70) + 1
    rsi_min = max(100, int(getattr(fabio_cfg, "rsi_len", 14)) + 2)
    atr_min = max(100, int(getattr(fabio_cfg, "atr_len", 14)) + 2)
    vol_min = max(50, int(getattr(fabio_cfg, "vol_sma_len", 20)) + 1)
    htf_min = max(61, vol_min)
    ltf_min = max(61, rsi_min, atr_min, vol_min, int(getattr(fabio_cfg, "long_retest_window", 12)) + 2)
    min_map[fabio_cfg.timeframe_htf] = max(min_map.get(fabio_cfg.timeframe_htf, 0), htf_min)
    min_map[fabio_cfg.timeframe_ltf] = max(min_map.get(fabio_cfg.timeframe_ltf, 0), ltf_min)
    if fabio_cfg.short_timeframe_primary:
        min_map[fabio_cfg.short_timeframe_primary] = max(
            min_map.get(fabio_cfg.short_timeframe_primary, 0), max(rsi_min, atr_min, vol_min, 20)
        )
    if fabio_cfg.short_timeframe_secondary:
        min_map[fabio_cfg.short_timeframe_secondary] = max(
            min_map.get(fabio_cfg.short_timeframe_secondary, 0), rsi_min
        )
    return min_map


def _build_atlasfabio_configs():
    atlas_cfg = atlas_fabio_engine.Config()
    fabio_cfg = fabio_entry_engine.Config()
    fabio_cfg_mid = fabio_entry_engine.Config()

    fabio_cfg.dist_to_ema20_max = engine_runner.ATLASFABIO_STRONG_DIST_MAX
    fabio_cfg.long_dist_to_ema20_max = engine_runner.ATLASFABIO_STRONG_DIST_MAX
    fabio_cfg.pullback_vol_ratio_max = engine_runner.ATLASFABIO_PULLBACK_VOL_MAX
    fabio_cfg.retest_touch_tol = engine_runner.ATLASFABIO_RETEST_TOUCH_TOL

    fabio_cfg_mid.timeframe_ltf = "5m"
    fabio_cfg_mid.dist_to_ema20_max = engine_runner.ATLASFABIO_MID_DIST_MAX
    fabio_cfg_mid.long_dist_to_ema20_max = engine_runner.ATLASFABIO_MID_DIST_MAX
    fabio_cfg_mid.pullback_vol_ratio_max = engine_runner.ATLASFABIO_MID_PULLBACK_VOL_MAX
    fabio_cfg_mid.retest_touch_tol = engine_runner.ATLASFABIO_MID_RETEST_TOUCH_TOL
    fabio_cfg_mid.trigger_vol_ratio_min = engine_runner.ATLASFABIO_MID_VOL_MULT

    return atlas_cfg, fabio_cfg, fabio_cfg_mid


def _run_backtest_for_symbol(
    symbol: str,
    data_by_tf: Dict[Tuple[str, str], List[list]],
    dir_hint: Dict[str, str],
    start_ms: int,
    end_ms: int,
    log_path: str,
    exchange,
) -> None:
    atlas_cfg, fabio_cfg, fabio_cfg_mid = _build_atlasfabio_configs()
    ltf = fabio_cfg.timeframe_ltf
    ltf_data = data_by_tf.get((symbol, ltf)) or []
    if not ltf_data:
        print(f"[backtest] {symbol} skip: missing {ltf} data")
        return

    min_required = _min_required_bars(atlas_cfg, fabio_cfg)
    tfs = list({atlas_cfg.htf_tf, atlas_cfg.ltf_tf, fabio_cfg.timeframe_htf, fabio_cfg.timeframe_ltf,
                fabio_cfg.short_timeframe_primary, fabio_cfg.short_timeframe_secondary})
    tfs = [tf for tf in tfs if tf]

    idx_by_tf: Dict[str, int] = {tf: 0 for tf in tfs}
    state: Dict[str, dict] = {
        "_atlasfabio_funnel_log_path": log_path,
        "_atlasfabio_confirm_entry_mode": "confirm_only",
        "_atlasfabio_backtest_mode": True,
    }
    cached_ex = engine_runner.CachedExchange(exchange)

    cycles_run = 0
    for row in ltf_data:
        ts = int(row[0])
        if ts < start_ms or ts > end_ms:
            continue
        for tf in tfs:
            data = data_by_tf.get((symbol, tf)) or []
            idx = idx_by_tf.get(tf, 0)
            while idx < len(data) and int(data[idx][0]) <= ts:
                idx += 1
            idx_by_tf[tf] = idx
            cycle_cache.set_raw(symbol, tf, data[:idx])

        if any(idx_by_tf.get(tf, 0) < min_required.get(tf, 0) for tf in tfs):
            continue

        cycle_cache.clear_cycle_cache(keep_raw=True)
        engine_runner.CURRENT_CYCLE_STATS = {}

        engine_runner._run_atlas_fabio_cycle(
            universe_structure=[symbol],
            cached_ex=cached_ex,
            state=state,
            fabio_cfg=fabio_cfg,
            fabio_cfg_mid=fabio_cfg_mid,
            atlas_cfg=atlas_cfg,
            active_positions_total=0,
            dir_hint=dir_hint,
            send_alert=lambda _: None,
        )
        cycles_run += 1
    if cycles_run == 0:
        print(f"[backtest] {symbol} skip: insufficient warmup data in range")


def main() -> None:
    parser = argparse.ArgumentParser(description="AtlasFabio backtest runner")
    parser.add_argument("--symbols", required=True, help="comma-separated symbols")
    parser.add_argument("--start", required=True, help="start datetime (YYYY-MM-DD or ISO)")
    parser.add_argument("--end", required=True, help="end datetime (YYYY-MM-DD or ISO)")
    parser.add_argument("--direction", default="auto", choices=("auto", "long", "short"))
    parser.add_argument("--long", default="", help="comma-separated long symbols override")
    parser.add_argument("--short", default="", help="comma-separated short symbols override")
    parser.add_argument("--log-file", default="", help="backtest funnel log path")
    parser.add_argument("--auto-range", action="store_true", help="auto clamp to available data range")
    args = parser.parse_args()
    if engine_runner.rsi_engine is None:
        engine_runner.rsi_engine = RsiEngine()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("symbols is required")
    start_ms = _parse_datetime(args.start)
    end_ms = _parse_datetime(args.end)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    log_path = args.log_file.strip() or os.path.join("logs", "fabio", "atlasfabio_funnel_backtest.log")
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[backtest] start={args.start} end={args.end} symbols={symbols}\n")

    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )

    cycle_cache.set_fetcher(None)

    engine_runner.ATLAS_FABIO_ENABLED = True
    engine_runner.ATLAS_FABIO_PAPER = True
    engine_runner.LIVE_TRADING = False
    engine_runner.LONG_LIVE_TRADING = False
    engine_runner.count_open_positions = lambda force=False: 0

    atlas_cfg, fabio_cfg, _ = _build_atlasfabio_configs()
    tfs = list({atlas_cfg.htf_tf, atlas_cfg.ltf_tf, fabio_cfg.timeframe_htf, fabio_cfg.timeframe_ltf,
                fabio_cfg.short_timeframe_primary, fabio_cfg.short_timeframe_secondary})
    tfs = [tf for tf in tfs if tf]

    min_required = _min_required_bars(atlas_cfg, fabio_cfg)
    data_by_tf: Dict[Tuple[str, str], List[list]] = {}
    for sym in symbols:
        for tf in tfs:
            fetch_start = _warmup_start_ms(start_ms, tf, min_required.get(tf, 0))
            print(f"[backtest] fetching {sym} {tf}")
            data_by_tf[(sym, tf)] = _fetch_ohlcv_range(exchange, sym, tf, fetch_start, end_ms)
        ltf = fabio_cfg.timeframe_ltf
        ltf_data = data_by_tf.get((sym, ltf)) or []
        if not ltf_data:
            avail_start, avail_end = _get_available_range(exchange, sym, ltf)
            if avail_start and avail_end:
                start_str = datetime.fromtimestamp(avail_start / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                end_str = datetime.fromtimestamp(avail_end / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                print(f"[backtest] {sym} {ltf} available={start_str}~{end_str}")
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(f"[backtest] {sym} {ltf} available={start_str}~{end_str}\n")
                if args.auto_range:
                    new_start = max(start_ms, avail_start)
                    new_end = min(end_ms, avail_end)
                    if new_end > new_start:
                        print(f"[backtest] {sym} auto-range applied")
                        for tf in tfs:
                            fetch_start = _warmup_start_ms(new_start, tf, min_required.get(tf, 0))
                            data_by_tf[(sym, tf)] = _fetch_ohlcv_range(
                                exchange, sym, tf, fetch_start, new_end
                            )
                    else:
                        print(f"[backtest] {sym} auto-range skipped: no overlap")
            else:
                print(f"[backtest] {sym} {ltf} available=unknown")
        for tf in tfs:
            data = data_by_tf.get((sym, tf)) or []
            req = min_required.get(tf, 0)
            print(f"[backtest] {sym} {tf} bars={len(data)} min_required={req}")

    dir_hint = _build_dir_hint(
        symbols,
        data_by_tf,
        fabio_cfg.timeframe_ltf,
        args.direction,
        long_syms=[s.strip() for s in args.long.split(",") if s.strip()],
        short_syms=[s.strip() for s in args.short.split(",") if s.strip()],
    )

    for sym in symbols:
        hint = dir_hint.get(sym)
        if not hint:
            print(f"[backtest] {sym} skip: dir_hint missing")
            continue
        _run_backtest_for_symbol(
            sym,
            data_by_tf,
            dir_hint={sym: hint},
            start_ms=start_ms,
            end_ms=end_ms,
            log_path=log_path,
            exchange=exchange,
        )


if __name__ == "__main__":
    main()
