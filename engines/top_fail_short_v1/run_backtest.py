#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone
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
) -> List[list]:
    if use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_common_warmup(warmup_path)
        if warmup_rows:
            return [r for r in warmup_rows if start_ms <= int(r[0]) <= end_ms]
        if cache_only:
            return []
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        return cached
    if cache_only:
        return []
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
    parser.add_argument("--common-warmup-dir", type=str, default="")

    parser.add_argument("--ltf-tf", type=str, default="3m")
    parser.add_argument("--mtf-tf", type=str, default="15m")
    parser.add_argument("--htf-tf", type=str, default="1h")

    parser.add_argument("--universe-24h-change", type=float, default=20.0)
    parser.add_argument("--universe-7d-mult", type=float, default=3.0)
    parser.add_argument("--min-quote-vol-24h", type=float, default=0.0)

    parser.add_argument("--stall-high-lookback", type=int, default=6)
    parser.add_argument("--stall-wick-min", type=float, default=0.45)
    parser.add_argument("--stall-min-count", type=int, default=2)

    parser.add_argument("--ema-len", type=int, default=20)
    parser.add_argument("--swing-lookback", type=int, default=30)
    parser.add_argument("--atr-len", type=int, default=14)
    parser.add_argument("--wash-atr-mult", type=float, default=1.3)
    parser.add_argument("--vol-spike-mult", type=float, default=1.8)
    parser.add_argument("--vol-sma-len", type=int, default=20)
    parser.add_argument("--retest-ema-tol", type=float, default=0.1)
    parser.add_argument("--limit-entry", action="store_true", default=True)
    parser.add_argument("--limit-offset-atr", type=float, default=0.05)
    parser.add_argument("--retest-max-depth-atr", type=float, default=1.15)
    parser.add_argument("--retest-wait-next-high", action="store_true", default=False)
    parser.add_argument("--fail-wick-max", type=float, default=0.45)
    parser.add_argument("--fail-require-ema", action="store_true", default=False)
    parser.add_argument("--stop-atr-mult", type=float, default=0.35)
    parser.add_argument("--min-hold-bars", type=int, default=3)
    parser.add_argument("--tp-min-pct", type=float, default=0.015)
    parser.add_argument("--max-wait-bars", type=int, default=60)
    parser.add_argument("--cooldown-bars", type=int, default=20)

    args = parser.parse_args()

    exchange = ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    ltf_tf = str(args.ltf_tf).strip()
    mtf_tf = str(args.mtf_tf).strip()
    htf_tf = str(args.htf_tf).strip()

    min_ltf = max(args.swing_lookback + 5, args.vol_sma_len + 5, args.atr_len + 5, args.ema_len + 5, 80)
    min_mtf = max(args.ema_len + 5, 60)
    min_htf = max(args.stall_high_lookback + 5, 180)

    start_ms, eval_start_ms, _, _ = calc_warmup_window(
        args.days,
        end_ms,
        {ltf_tf: min_ltf, mtf_tf: min_mtf, htf_tf: min_htf},
    )

    universe = load_common_universe(args.universe, exchange, args.cache_only)
    base_dir = os.path.join(ROOT_DIR, "logs", "top_fail_short_v1", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(base_dir, f"backtest_{date_tag}.log")

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    use_common = bool(args.use_live_cache)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")

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

    for sym, df in data_by_sym.items():
        df_mtf = data_by_sym_mtf.get(sym)
        df_htf = data_by_sym_htf.get(sym)
        if df_mtf is None or df_htf is None:
            continue

        ts = df["ts"].astype(int).to_numpy()
        ts_mtf = df_mtf["ts"].astype(int).to_numpy()
        ts_htf = df_htf["ts"].astype(int).to_numpy()

        ema_ltf = _ema(df["close"].astype(float), args.ema_len)
        ema_mtf = _ema(df_mtf["close"].astype(float), args.ema_len)
        atr_ltf = (df["high"] - df["low"]).astype(float).rolling(args.atr_len).mean()
        vol_sma = df["volume"].astype(float).rolling(args.vol_sma_len).mean()
        swing_low_prev = df["low"].astype(float).rolling(args.swing_lookback).min().shift(1)

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
                    cooldown = args.cooldown_bars
                    continue

                if trade["hold_bars"] >= int(args.min_hold_bars) and low <= trade["tp_price"]:
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
                    cooldown = args.cooldown_bars
                continue

            if cooldown > 0:
                cooldown -= 1
                continue

            idx_htf = int(np.searchsorted(ts_htf, ts_ms, side="right") - 1)
            idx_mtf = int(np.searchsorted(ts_mtf, ts_ms, side="right") - 1)
            if idx_htf <= 0 or idx_mtf <= 0:
                continue

            close_htf = float(df_htf["close"].iloc[idx_htf])
            close_prev_htf = float(df_htf["close"].iloc[idx_htf - 1])
            high_prev_htf = float(df_htf["high"].iloc[idx_htf - 1])
            high_htf = float(df_htf["high"].iloc[idx_htf])

            u1 = False
            u2 = False
            if idx_htf >= 24:
                c_24h = float(df_htf["close"].iloc[idx_htf - 24])
                if c_24h > 0:
                    u1 = ((close_htf - c_24h) / c_24h * 100.0) >= float(args.universe_24h_change)
            if idx_htf >= 168:
                low_7d = float(df_htf["low"].iloc[idx_htf - 168: idx_htf + 1].min())
                if low_7d > 0:
                    u2 = close_htf >= low_7d * float(args.universe_7d_mult)
            if not (u1 or u2):
                top_stall = False
                washdown_armed = False
                entry_window = False
                retest_active = False
                continue
            universe_ok_count += 1

            if float(args.min_quote_vol_24h) > 0 and idx_htf >= 24:
                qv = (df_htf["close"].iloc[idx_htf - 23: idx_htf + 1].astype(float) * df_htf["volume"].iloc[idx_htf - 23: idx_htf + 1].astype(float)).sum()
                if qv < float(args.min_quote_vol_24h):
                    top_stall = False
                    washdown_armed = False
                    entry_window = False
                    retest_active = False
                    continue

            expanding = close_htf > high_prev_htf
            conds = 0
            if close_htf <= close_prev_htf:
                conds += 1
            if _upper_wick_ratio(df_htf.iloc[idx_htf]) >= float(args.stall_wick_min):
                conds += 1
            if idx_htf >= args.stall_high_lookback:
                prev_hi = float(df_htf["high"].iloc[idx_htf - args.stall_high_lookback: idx_htf].max())
                if high_htf <= prev_hi:
                    conds += 1
            top_stall = (not expanding) and conds >= int(args.stall_min_count)
            if top_stall:
                top_stall_count += 1

            if top_stall:
                cur_close_mtf = float(df_mtf["close"].iloc[idx_mtf])
                cur_ema_mtf = float(ema_mtf.iloc[idx_mtf])
                washdown_armed = bool(cur_close_mtf < cur_ema_mtf)
            else:
                washdown_armed = False
            if washdown_armed:
                washdown_armed_count += 1

            if washdown_armed and not entry_window:
                swing_low = swing_low_prev.iloc[sig_idx]
                atr_now = atr_ltf.iloc[sig_idx]
                vol_ma = vol_sma.iloc[sig_idx]
                if np.isnan(swing_low) or np.isnan(atr_now) or np.isnan(vol_ma):
                    continue
                conds = 0
                if float(df["low"].iloc[sig_idx]) < float(swing_low):
                    conds += 1
                if float(df["high"].iloc[sig_idx] - df["low"].iloc[sig_idx]) >= float(args.wash_atr_mult) * float(atr_now):
                    conds += 1
                if float(df["volume"].iloc[sig_idx]) >= float(args.vol_spike_mult) * float(vol_ma):
                    conds += 1
                if conds >= 1 and float(df["low"].iloc[sig_idx]) < float(swing_low):
                    entry_window = True
                    entry_window_start = sig_idx
                    retest_active = False
                    retest_high = None
                    retest_touch_idx = None
                    retest_touch_high = None
                    break_level = float(swing_low)
                    entry_window_count += 1

            if entry_window:
                if entry_window_start is not None and (sig_idx - entry_window_start) > int(args.max_wait_bars):
                    entry_window = False
                    retest_active = False
                    retest_touch_idx = None
                    retest_touch_high = None
                    continue
                atr_now = atr_ltf.iloc[sig_idx]
                ema_now = ema_ltf.iloc[sig_idx]
                if np.isnan(atr_now) or np.isnan(ema_now):
                    continue
                if break_level is None:
                    continue
                retest_touch = float(df["high"].iloc[sig_idx]) >= float(break_level) - float(args.retest_ema_tol) * float(atr_now)
                if retest_touch:
                    retest_active = True
                    retest_high = float(df["high"].iloc[sig_idx]) if retest_high is None else max(retest_high, float(df["high"].iloc[sig_idx]))
                    retest_seen_count += 1
                    if retest_touch_idx is None:
                        retest_touch_idx = sig_idx
                        retest_touch_high = float(df["high"].iloc[sig_idx])
                if retest_active:
                    low_now = float(df["low"].iloc[sig_idx])
                    close_now = float(df["close"].iloc[sig_idx])
                    open_now = float(df["open"].iloc[sig_idx])
                    upper_wick = _upper_wick_ratio(df.iloc[sig_idx])
                    prev_low = float(df["low"].iloc[sig_idx - 1])
                    fail_candle = close_now < open_now and upper_wick <= float(args.fail_wick_max) and close_now < float(break_level)
                    if bool(args.fail_require_ema):
                        fail_candle = fail_candle and close_now < float(ema_now)
                    else:
                        fail_candle = fail_candle and (low_now < prev_low)
                    if bool(args.retest_wait_next_high) and retest_touch_idx is not None:
                        if sig_idx <= retest_touch_idx:
                            fail_candle = False
                        elif retest_touch_high is not None and float(df["high"].iloc[sig_idx]) > float(retest_touch_high):
                            fail_candle = False
                    if fail_candle:
                        if break_level is not None:
                            retest_depth = (float(retest_high) - float(break_level)) / float(atr_now) if atr_now > 0 else 0.0
                            if retest_depth > float(args.retest_max_depth_atr):
                                entry_window = False
                                retest_active = False
                                retest_touch_idx = None
                                retest_touch_high = None
                                continue
                        entry_px = close_now
                        entry_type = "market"
                        limit_filled = False
                        if bool(args.limit_entry):
                            offset = float(args.limit_offset_atr)
                            if retest_seen_count >= 5 and entry_count == 0:
                                offset *= 1.2
                            entry_limit = float(break_level) - offset * float(atr_now)
                            entry_type = "limit"
                            if low_now <= entry_limit:
                                entry_px = entry_limit
                                limit_filled = True
                            else:
                                entry_window = False
                                retest_active = False
                                retest_touch_idx = None
                                retest_touch_high = None
                                continue
                        stop_high = float(retest_high) if retest_high is not None else float(df["high"].iloc[sig_idx])
                        sl_candidate = stop_high
                        ema_stop = float(ema_now) + 0.3 * float(atr_now)
                        sl_price = max(sl_candidate, ema_stop)
                        r = sl_price - entry_px
                        if r <= 0:
                            entry_window = False
                            retest_active = False
                            retest_touch_idx = None
                            retest_touch_high = None
                            continue
                        tp_price = entry_px - max(0.7 * r, float(args.tp_min_pct) * entry_px)
                        trade = {
                            "entry_px": entry_px,
                            "sl_price": sl_price,
                            "tp_price": tp_price,
                            "mfe": 0.0,
                            "mae": 0.0,
                            "hold_bars": 0,
                            "entry_ts": ts_ms,
                            "entry_type": entry_type,
                            "limit_filled": limit_filled,
                            "retest_depth_atr": (float(retest_high) - float(break_level)) / float(atr_now) if break_level is not None and atr_now > 0 else 0.0,
                        }
                        stats["entries"] += 1
                        sym_stats["entries"] += 1
                        entry_count += 1
                        entry_window = False
                        retest_active = False
                        retest_touch_idx = None
                        retest_touch_high = None
                        entry_count += 1
                        entry_window = False
                        retest_active = False

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
