#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import ccxt
import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import (
    calc_warmup_window,
    load_common_universe,
    format_backtest_summary,
    print_time_summaries,
)
from engines.top_fail_short_v1.engine import (
    TopFailShortV1Config,
    top_fail_short_entry_signal,
)


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _read_common_warmup(path: str) -> List[list]:
    if not path or not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty or "ts" not in df.columns:
            return []
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return []


def _parse_ts_arg(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone(timedelta(hours=9)))
            return int(dt.astimezone(timezone.utc).timestamp() * 1000)
        except Exception:
            continue
    return None


def _has_gaps(rows: List[list], tf_ms: int, start_ms: int, end_ms: int) -> bool:
    if not rows:
        return True
    rows_sorted = sorted(rows, key=lambda r: int(r[0]))
    if int(rows_sorted[0][0]) > start_ms + tf_ms:
        return True
    if int(rows_sorted[-1][0]) < end_ms - tf_ms:
        return True
    prev = int(rows_sorted[0][0])
    for row in rows_sorted[1:]:
        ts = int(row[0])
        if ts - prev > tf_ms:
            return True
        prev = ts
    return False


def _fetch_ohlcv_rest(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> List[list]:
    out: List[list] = []
    since = start_ms
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    last_ts = None
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if ts > end_ms:
                continue
            if last_ts is None or ts > last_ts:
                out.append(row)
                last_ts = ts
        new_last = int(batch[-1][0])
        if last_ts is None or new_last == last_ts:
            since = new_last + tf_ms
        else:
            since = last_ts + tf_ms
    return out


def _ohlcv_cache_path(root_dir: str, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe = _sanitize_symbol(symbol)
    return os.path.join(
        root_dir,
        "logs",
        "top_fail_short_v1",
        "ohlcv_cache",
        f"{safe}_{timeframe}_{start_ms}_{end_ms}.csv",
    )


def _read_ohlcv_cache(path: str) -> List[list]:
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty:
            return []
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return []


def _read_snapshot(path: str) -> List[list]:
    return _read_common_warmup(path)


def _write_ohlcv_cache(path: str, rows: List[list]) -> None:
    if not rows:
        return
    _ensure_dir(os.path.dirname(path))
    try:
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df.to_csv(path, index=False)
    except Exception:
        return


def _fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
    use_common_warmup: bool = False,
    common_warmup_dir: str = "",
    cache_only: bool = False,
    fill_missing: bool = False,
    snapshot_dir: str = "",
    snapshot_only: bool = False,
    common_only: bool = False,
) -> List[list]:
    if common_only:
        use_common_warmup = True
        cache_only = True
        snapshot_dir = ""
        snapshot_only = False
    if snapshot_dir:
        safe = _sanitize_symbol(symbol)
        snap_path = os.path.join(snapshot_dir, f"{safe}_{timeframe}.csv")
        snap_rows = _read_snapshot(snap_path)
        if snap_rows:
            if snapshot_only:
                return snap_rows
            filtered = [r for r in snap_rows if start_ms <= int(r[0]) <= end_ms]
            if not fill_missing or cache_only:
                return filtered
            tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
            if not _has_gaps(filtered, tf_ms, start_ms, end_ms):
                return filtered
        if snapshot_only:
            return []
    if use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_common_warmup(warmup_path)
        if warmup_rows:
            filtered = [r for r in warmup_rows if start_ms <= int(r[0]) <= end_ms]
            if not fill_missing or cache_only:
                return filtered
            tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
            if not _has_gaps(filtered, tf_ms, start_ms, end_ms):
                return filtered
            rest_rows = _fetch_ohlcv_rest(exchange, symbol, timeframe, start_ms, end_ms, limit=limit)
            if rest_rows:
                cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
                _write_ohlcv_cache(cache_path, rest_rows)
                return rest_rows
        if cache_only:
            return []
    if common_only:
        return []
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        if not fill_missing or cache_only:
            return cached
        tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
        if not _has_gaps(cached, tf_ms, start_ms, end_ms):
            return cached
        rest_rows = _fetch_ohlcv_rest(exchange, symbol, timeframe, start_ms, end_ms, limit=limit)
        if rest_rows:
            _write_ohlcv_cache(cache_path, rest_rows)
            return rest_rows
    if cache_only:
        return []
    out = _fetch_ohlcv_rest(exchange, symbol, timeframe, start_ms, end_ms, limit=limit)
    _write_ohlcv_cache(cache_path, out)
    return out


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _upper_wick_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    upper = high - max(open_, close)
    return upper / rng


def _ts_kst(ts_ms: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def run_backtest() -> None:
    parser = argparse.ArgumentParser("top_fail_short_v1 backtest")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--use-live-cache", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")

    parser.add_argument("--ltf-tf", type=str, default="3m")
    parser.add_argument("--mtf-tf", type=str, default="15m")
    parser.add_argument("--htf-tf", type=str, default="1h")

    parser.add_argument("--universe-24h-change", type=float, default=20.0)
    parser.add_argument("--universe-7d-mult", type=float, default=3.0)
    parser.add_argument("--min-quote-vol-24h", type=float, default=0.0)

    parser.add_argument("--stall-high-lookback", type=int, default=6)
    parser.add_argument("--stall-wick-min", type=float, default=0.5)
    parser.add_argument("--stall-min-count", type=int, default=2)
    parser.add_argument("--mtf-require-weak-close", action="store_true", default=False)

    parser.add_argument("--ema-len", type=int, default=20)
    parser.add_argument("--swing-lookback", type=int, default=30)
    parser.add_argument("--atr-len", type=int, default=14)
    parser.add_argument("--wash-atr-mult", type=float, default=1.3)
    parser.add_argument("--vol-spike-mult", type=float, default=1.8)
    parser.add_argument("--vol-sma-len", type=int, default=20)
    parser.add_argument("--retest-ema-tol", type=float, default=0.1)
    parser.add_argument("--start", type=str, default="")
    parser.add_argument("--end", type=str, default="")
    parser.add_argument("--fill-missing", action="store_true", default=False)
    parser.add_argument("--snapshot-dir", type=str, default="")
    parser.add_argument("--snapshot-only", action="store_true", default=False)
    parser.add_argument("--limit-entry", action="store_true", default=True)
    parser.add_argument("--limit-offset-atr", type=float, default=0.15)
    parser.add_argument("--retest-max-depth-atr", type=float, default=1.15)
    parser.add_argument("--retest-wait-next-high", action="store_true", default=False)
    parser.add_argument("--fail-wick-max", type=float, default=0.30)
    parser.add_argument("--fail-require-ema", action="store_true", default=False)
    parser.add_argument("--stop-atr-mult", type=float, default=0.35)
    parser.add_argument("--min-hold-bars", type=int, default=3)
    parser.add_argument("--tp-min-pct", type=float, default=0.015)
    parser.add_argument("--tp-r-mult", type=float, default=0.9)
    parser.add_argument("--max-wait-bars", type=int, default=60)
    parser.add_argument("--cooldown-bars", type=int, default=20)

    args = parser.parse_args()

    cfg = TopFailShortV1Config()
    cfg.tf_ltf = str(args.ltf_tf).strip()
    cfg.tf_mtf = str(args.mtf_tf).strip()
    cfg.tf_htf = str(args.htf_tf).strip()
    cfg.universe_24h_change = float(args.universe_24h_change)
    cfg.universe_7d_mult = float(args.universe_7d_mult)
    cfg.min_quote_vol_24h = float(args.min_quote_vol_24h)
    cfg.stall_high_lookback = int(args.stall_high_lookback)
    cfg.stall_wick_min = float(args.stall_wick_min)
    cfg.stall_min_count = int(args.stall_min_count)
    cfg.mtf_require_weak_close = bool(args.mtf_require_weak_close)
    cfg.ema_len = int(args.ema_len)
    cfg.swing_lookback = int(args.swing_lookback)
    cfg.atr_len = int(args.atr_len)
    cfg.wash_atr_mult = float(args.wash_atr_mult)
    cfg.vol_spike_mult = float(args.vol_spike_mult)
    cfg.vol_sma_len = int(args.vol_sma_len)
    cfg.retest_ema_tol = float(args.retest_ema_tol)
    cfg.limit_entry = bool(args.limit_entry)
    cfg.limit_offset_atr = float(args.limit_offset_atr)
    cfg.retest_max_depth_atr = float(args.retest_max_depth_atr)
    cfg.retest_wait_next_high = bool(args.retest_wait_next_high)
    cfg.fail_wick_max = float(args.fail_wick_max)
    cfg.fail_require_ema = bool(args.fail_require_ema)
    cfg.stop_atr_mult = float(args.stop_atr_mult)
    cfg.min_hold_bars = int(args.min_hold_bars)
    cfg.tp_min_pct = float(args.tp_min_pct)
    cfg.tp_r_mult = float(args.tp_r_mult)
    cfg.max_wait_bars = int(args.max_wait_bars)
    cfg.cooldown_bars = int(args.cooldown_bars)

    exchange = ccxt.binance({"enableRateLimit": True})
    end_ms = _parse_ts_arg(args.end) or int(datetime.now(timezone.utc).timestamp() * 1000)

    ltf_tf = cfg.tf_ltf
    mtf_tf = cfg.tf_mtf
    htf_tf = cfg.tf_htf

    min_ltf = max(cfg.swing_lookback + 5, cfg.vol_sma_len + 5, cfg.atr_len + 5, cfg.ema_len + 5, 80)
    min_mtf = max(cfg.ema_len + 5, 60)
    min_htf = max(cfg.stall_high_lookback + 5, 180)

    start_arg_ms = _parse_ts_arg(args.start)
    if start_arg_ms is None:
        start_ms, eval_start_ms, _, _ = calc_warmup_window(
            args.days,
            end_ms,
            {ltf_tf: min_ltf, mtf_tf: min_mtf, htf_tf: min_htf},
        )
    else:
        eval_start_ms = start_arg_ms
        warmup_minutes = max(min_ltf * 3, min_mtf * 15, min_htf * 60)
        start_ms = eval_start_ms - int(warmup_minutes * 60 * 1000)

    universe = load_common_universe(args.universe, exchange, args.cache_only)
    base_dir = os.path.join(ROOT_DIR, "logs", "top_fail_short_v1", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(base_dir, f"backtest_{date_tag}.log")

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    use_common = bool(args.use_live_cache or args.common_only)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    if args.common_only:
        args.cache_only = True

    data_by_sym: Dict[str, pd.DataFrame] = {}
    data_by_sym_mtf: Dict[str, pd.DataFrame] = {}
    data_by_sym_htf: Dict[str, pd.DataFrame] = {}

    for sym in universe:
        rows_ltf = _fetch_ohlcv_all(
            exchange,
            sym,
            ltf_tf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
            fill_missing=args.fill_missing,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        rows_mtf = _fetch_ohlcv_all(
            exchange,
            sym,
            mtf_tf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
            fill_missing=args.fill_missing,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        rows_htf = _fetch_ohlcv_all(
            exchange,
            sym,
            htf_tf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
            fill_missing=args.fill_missing,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        if not rows_ltf or not rows_mtf or not rows_htf:
            continue
        df_ltf = pd.DataFrame(rows_ltf, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_mtf = pd.DataFrame(rows_mtf, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_htf = pd.DataFrame(rows_htf, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if len(df_ltf) < min_ltf or len(df_mtf) < min_mtf or len(df_htf) < min_htf:
            continue
        data_by_sym[sym] = df_ltf
        data_by_sym_mtf[sym] = df_mtf
        data_by_sym_htf[sym] = df_htf

    if not data_by_sym:
        print("[BACKTEST] no_data")
        return

    stats = {
        "entries": 0,
        "exits": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
        "net_sum": 0.0,
        "tp_sum": 0.0,
        "sl_sum": 0.0,
        "net_sum_usdt": 0.0,
        "tp_sum_usdt": 0.0,
        "sl_sum_usdt": 0.0,
    }
    entry_usdt = 10.0
    trades_out: List[dict] = []

    universe_ok_count = 0
    top_stall_count = 0
    washdown_armed_count = 0
    entry_window_count = 0
    retest_seen_count = 0
    entry_count = 0
    top_state: Dict[str, dict] = {}

    for sym, df in data_by_sym.items():
        df_mtf = data_by_sym_mtf.get(sym)
        df_htf = data_by_sym_htf.get(sym)
        if df_mtf is None or df_htf is None:
            continue

        df_ltf_sig = df.iloc[:-1] if args.use_confirmed else df
        df_mtf_sig = df_mtf.iloc[:-1] if args.use_confirmed else df_mtf
        df_htf_sig = df_htf.iloc[:-1] if args.use_confirmed else df_htf

        ts = df_ltf_sig["ts"].astype(int).to_numpy()

        sym_stats = {
            "entries": 0,
            "exits": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "mfe_sum": 0.0,
            "mae_sum": 0.0,
            "hold_sum": 0.0,
            "net_sum": 0.0,
            "tp_sum": 0.0,
            "sl_sum": 0.0,
            "net_sum_usdt": 0.0,
            "tp_sum_usdt": 0.0,
            "sl_sum_usdt": 0.0,
        }

        trade = None
        cooldown = 0
        top_stall = False
        washdown_armed = False
        entry_window = False
        entry_window_start = None
        retest_active = False
        retest_high = None
        retest_touch_idx = None
        retest_touch_high = None
        break_level = None

        for i in range(1, len(df)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx < 1:
                continue
            ts_ms = int(ts[sig_idx])
            if ts_ms < eval_start_ms:
                continue

            if trade:
                high = float(df.at[i, "high"])
                low = float(df.at[i, "low"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))

                if high >= trade["sl_price"]:
                    exit_px = trade["sl_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    stats["sl_sum"] += pnl_pct
                    stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    stats["sl_sum_usdt"] += pnl_pct * entry_usdt
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += pnl_pct
                    sym_stats["sl_sum"] += pnl_pct
                    sym_stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    sym_stats["sl_sum_usdt"] += pnl_pct * entry_usdt
                    stats["losses"] += 1
                    sym_stats["losses"] += 1
                    trades_out.append(
                        {
                            "symbol": sym,
                            "entry_ts": int(trade["entry_ts"]),
                            "exit_ts": int(ts[i]),
                            "pnl_pct": pnl_pct * 100.0,
                            "result": "LOSS",
                            "reason": "SL",
                            "entry_type": trade.get("entry_type"),
                            "limit_filled": trade.get("limit_filled"),
                            "retest_depth_atr": trade.get("retest_depth_atr"),
                        }
                    )
                    trade = None
                    cooldown = cfg.cooldown_bars
                    continue

                if trade["hold_bars"] >= int(cfg.min_hold_bars) and low <= trade["tp_price"]:
                    exit_px = trade["tp_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    stats["tp_sum"] += pnl_pct
                    stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    stats["tp_sum_usdt"] += pnl_pct * entry_usdt
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += pnl_pct
                    sym_stats["tp_sum"] += pnl_pct
                    sym_stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    sym_stats["tp_sum_usdt"] += pnl_pct * entry_usdt
                    stats["wins"] += 1
                    sym_stats["wins"] += 1
                    trades_out.append(
                        {
                            "symbol": sym,
                            "entry_ts": int(trade["entry_ts"]),
                            "exit_ts": int(ts[i]),
                            "pnl_pct": pnl_pct * 100.0,
                            "result": "WIN",
                            "reason": "TP",
                            "entry_type": trade.get("entry_type"),
                            "limit_filled": trade.get("limit_filled"),
                            "retest_depth_atr": trade.get("retest_depth_atr"),
                        }
                    )
                    trade = None
                    cooldown = cfg.cooldown_bars
                continue

            if cooldown > 0:
                cooldown -= 1
                continue

            sym_state = top_state.setdefault(sym, {})
            entry_info, reason, meta = top_fail_short_entry_signal(
                df_ltf_sig,
                df_mtf_sig,
                df_htf_sig,
                sig_idx,
                cfg,
                sym_state,
                True,
            )
            if meta.get("universe_ok"):
                universe_ok_count += 1
            if meta.get("stall_ok"):
                top_stall_count += 1
            if meta.get("armed_ok"):
                washdown_armed_count += 1
            if meta.get("entry_window"):
                entry_window_count += 1
            if meta.get("retest_touch"):
                retest_seen_count += 1

            if not entry_info:
                continue

            entry_px = float(entry_info["entry_px"])
            entry_type = entry_info.get("entry_type") or "market"
            limit_filled = True if entry_type == "limit" else False
            trade = {
                "entry_px": entry_px,
                "sl_price": float(entry_info["sl_price"]),
                "tp_price": float(entry_info["tp_price"]),
                "mfe": 0.0,
                "mae": 0.0,
                "hold_bars": 0,
                "entry_ts": int(entry_info["entry_ts"]),
                "entry_type": entry_type,
                "limit_filled": limit_filled,
                "retest_depth_atr": entry_info.get("retest_depth_atr"),
            }
            stats["entries"] += 1
            sym_stats["entries"] += 1
            entry_count += 1
            sym_state["cooldown_left"] = int(cfg.cooldown_bars)
            sym_state["entry_window_start"] = None
            sym_state["break_level"] = None
            sym_state["retest_high"] = None
            sym_state["retest_wait"] = False
            sym_state["retest_touch_high"] = None

        trades = sym_stats["trades"]
        if trades > 0:
            sym_stats["winrate"] = (sym_stats["wins"] / trades) * 100.0
            sym_stats["avg_mfe"] = sym_stats["mfe_sum"] / trades
            sym_stats["avg_mae"] = sym_stats["mae_sum"] / trades
            sym_stats["avg_hold"] = sym_stats["hold_sum"] / trades
            line = format_backtest_summary(sym, sym_stats)
            print(line)
            _log(line)

    print("[BACKTEST] TRADES(KST) symbol result pnl_pct entry_ts exit_ts entry_type limit_filled retest_depth_atr")
    for t in trades_out:
        print(
            f"[BACKTEST] TRADE {t['symbol']} result={t['result']} pnl_pct={t['pnl_pct']:.2f}% "
            f"entry_ts={_ts_kst(t['entry_ts'])} exit_ts={_ts_kst(t['exit_ts'])} "
            f"entry_type={t.get('entry_type','-')} limit_filled={t.get('limit_filled','-')} "
            f"retest_depth_atr={t.get('retest_depth_atr','-')}"
        )

    print(format_backtest_summary(None, stats))
    print(
        f"[BACKTEST] FUNNEL universe_ok={universe_ok_count} top_stall={top_stall_count} "
        f"washdown_armed={washdown_armed_count} entry_window={entry_window_count} "
        f"retest_seen={retest_seen_count} entries={entry_count}"
    )
    print_time_summaries(trades_out, _log)


if __name__ == "__main__":
    run_backtest()
