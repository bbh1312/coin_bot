#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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
from engines.top_fail_short_v1.engine import TopFailShortConfig


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


def _runup_pct(df: pd.DataFrame, idx: int, lookback: int) -> Optional[float]:
    if idx - lookback < 0:
        return None
    base = float(df["close"].iloc[idx - lookback])
    if base <= 0:
        return None
    return (float(df["close"].iloc[idx]) - base) / base * 100.0


def _high_touch_count(df: pd.DataFrame, idx: int, lookback: int, band_pct: float) -> int:
    start = max(0, idx - lookback + 1)
    window = df.iloc[start : idx + 1]
    if window.empty:
        return 0
    top = float(window["high"].max())
    if top <= 0:
        return 0
    band = top * (band_pct / 100.0)
    lo = top - band
    hi = top + band
    return int(((window["high"] >= lo) & (window["high"] <= hi)).sum())


def _upper_wick_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    upper = high - max(open_, close)
    return upper / rng


def _close_pos(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    return (close - low) / rng


def _avg_range(df: pd.DataFrame, idx: int, lookback: int) -> Optional[float]:
    start = max(0, idx - lookback + 1)
    window = df.iloc[start : idx + 1]
    if window.empty:
        return None
    ranges = (window["high"] - window["low"]).astype(float)
    return float(ranges.mean())


def _body_size(row: pd.Series) -> float:
    return abs(float(row["close"]) - float(row["open"]))


def _avg_body(df: pd.DataFrame, idx: int, lookback: int) -> Optional[float]:
    start = max(0, idx - lookback + 1)
    window = df.iloc[start : idx + 1]
    if window.empty:
        return None
    bodies = (window["close"] - window["open"]).abs().astype(float)
    return float(bodies.mean())


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
    parser.add_argument("--tf", type=str, default="3m")
    parser.add_argument("--runup-bars", type=int, default=40)
    parser.add_argument("--runup-min-pct", type=float, default=4.0)
    parser.add_argument("--runup-top-pct", type=float, default=10.0)
    parser.add_argument("--touch-lookback", type=int, default=40)
    parser.add_argument("--touch-band-pct", type=float, default=0.15)
    parser.add_argument("--touch-min", type=int, default=3)
    parser.add_argument("--fake-break-lookback", type=int, default=6)
    parser.add_argument("--range-avg-lookback", type=int, default=10)
    parser.add_argument("--range-mult", type=float, default=1.2)
    parser.add_argument("--upper-wick-min", type=float, default=0.45)
    parser.add_argument("--close-pos-max", type=float, default=0.35)
    parser.add_argument("--entry-mode", type=str, default="break")
    parser.add_argument("--entry-retrace-min", type=float, default=0.382)
    parser.add_argument("--entry-retrace-max", type=float, default=0.5)
    parser.add_argument("--sl-pad-pct", type=float, default=0.1)
    parser.add_argument("--tp1-r", type=float, default=0.5)
    parser.add_argument("--tp2-r", type=float, default=1.0)
    parser.add_argument("--tp1-frac", type=float, default=0.4)
    parser.add_argument("--tp2-frac", type=float, default=0.3)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--cooldown-bars", type=int, default=5)
    parser.add_argument("--htf-tf", type=str, default="15m")
    parser.add_argument("--htf-last-bull-window", type=int, default=3)
    parser.add_argument("--htf-body-lookback", type=int, default=20)
    parser.add_argument("--htf-body-min-ratio", type=float, default=0.35)
    parser.add_argument("--htf-high-lookback", type=int, default=10)
    parser.add_argument("--htf-high-distance-max", type=float, default=1.0)
    parser.add_argument("--htf-distance-soft-max", type=float, default=1.6)
    parser.add_argument("--htf-atr-len", type=int, default=14)
    parser.add_argument("--htf-atr-mult", type=float, default=0.8)
    parser.add_argument("--htf-distance-min", type=float, default=1.0)
    parser.add_argument("--ltf-strong-wick-min", type=float, default=0.55)
    parser.add_argument("--ltf-strong-close-pos-max", type=float, default=0.25)
    parser.add_argument("--htf-confirmed-only", action="store_true", default=True)
    parser.add_argument("--htf-gate-mode", type=str, default="confirm")
    parser.add_argument("--print-ready-counts", action="store_true")
    args = parser.parse_args()

    cfg = TopFailShortConfig(
        tf=args.tf,
        runup_bars=args.runup_bars,
        runup_min_pct=args.runup_min_pct,
        runup_top_pct=args.runup_top_pct,
        touch_lookback=args.touch_lookback,
        touch_band_pct=args.touch_band_pct,
        touch_min=args.touch_min,
        fake_break_lookback=args.fake_break_lookback,
        range_avg_lookback=args.range_avg_lookback,
        range_mult=args.range_mult,
        upper_wick_min=args.upper_wick_min,
        close_pos_max=args.close_pos_max,
        entry_mode=args.entry_mode,
        entry_retrace_min=args.entry_retrace_min,
        entry_retrace_max=args.entry_retrace_max,
        sl_pad_pct=args.sl_pad_pct,
        tp1_r=args.tp1_r,
        tp2_r=args.tp2_r,
        tp1_frac=args.tp1_frac,
        tp2_frac=args.tp2_frac,
        max_retries=args.max_retries,
        cooldown_bars=args.cooldown_bars,
    )

    exchange = ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    tf_ms = int(exchange.parse_timeframe(cfg.tf) * 1000)
    htf_tf = str(args.htf_tf).strip()
    htf_ms = int(exchange.parse_timeframe(htf_tf) * 1000)
    min_tf = max(cfg.runup_bars + 10, cfg.touch_lookback + 10, cfg.fake_break_lookback + 10, 120)
    min_htf = max(args.htf_high_lookback + 5, args.htf_body_lookback + 5, 60)
    start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
        args.days,
        end_ms,
        {cfg.tf: min_tf, htf_tf: min_htf},
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
    data_by_sym_htf: Dict[str, pd.DataFrame] = {}
    for sym in universe:
        rows = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf,
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
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if len(df) < min_tf or not rows_htf:
            continue
        df_htf = pd.DataFrame(rows_htf, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if len(df_htf) < min_htf:
            continue
        data_by_sym[sym] = df
        data_by_sym_htf[sym] = df_htf

    if not data_by_sym:
        print("[BACKTEST] no_data")
        return

    # build runup map by timestamp
    runup_map: Dict[int, List[float]] = {}
    v1_ready_count = 0
    htf_confirm_count = 0
    pass_htf_alignment = 0
    pass_htf_body = 0
    pass_htf_distance = 0
    final_ready_count = 0

    for sym, df in data_by_sym.items():
        for i in range(cfg.runup_bars, len(df)):
            ts = int(df["ts"].iloc[i])
            ru = _runup_pct(df, i, cfg.runup_bars)
            if ru is None:
                continue
            runup_map.setdefault(ts, []).append(float(ru))

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
    stats_by_symbol: Dict[str, Dict[str, float]] = {}
    trades_out: List[dict] = []

    for sym, df in data_by_sym.items():
        ts = df["ts"].astype(int).to_numpy()
        ema20 = _ema(df["close"].astype(float), 20)
        df_htf = data_by_sym_htf.get(sym)
        if df_htf is None or df_htf.empty:
            continue
        ts_htf = df_htf["ts"].astype(int).to_numpy()
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
        retries = 0
        cooldown = 0
        invalidated = False
        trade = None

        for i in range(1, len(df)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx < 1:
                continue
            ts_ms = int(ts[sig_idx])
            if ts_ms < eval_start_ms:
                continue
            if invalidated:
                continue
            if cooldown > 0:
                cooldown -= 1
                continue

            if trade:
                high = float(df.at[i, "high"])
                low = float(df.at[i, "low"])
                close = float(df.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))

                # SL
                if high >= trade["sl_price"]:
                    exit_px = trade["sl_price"]
                    pnl_pct = trade["realized"] + (trade["entry_px"] - exit_px) / trade["entry_px"] * trade["remaining"]
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
                        }
                    )
                    trade = None
                    retries += 1
                    cooldown = cfg.cooldown_bars
                    if retries >= cfg.max_retries:
                        invalidated = True
                    continue

                # TP1
                if (not trade["tp1_done"]) and low <= trade["tp1_price"]:
                    trade["tp1_done"] = True
                    pnl_pct = (trade["entry_px"] - trade["tp1_price"]) / trade["entry_px"] * cfg.tp1_frac
                    trade["realized"] += pnl_pct
                    trade["remaining"] -= cfg.tp1_frac

                # TP2
                if (not trade["tp2_done"]) and low <= trade["tp2_price"]:
                    trade["tp2_done"] = True
                    pnl_pct = (trade["entry_px"] - trade["tp2_price"]) / trade["entry_px"] * cfg.tp2_frac
                    trade["realized"] += pnl_pct
                    trade["remaining"] -= cfg.tp2_frac

                # TP3: EMA20 touch
                ema_now = float(ema20.iloc[i]) if not np.isnan(ema20.iloc[i]) else None
                if ema_now is not None and low <= ema_now and trade["remaining"] > 0:
                    exit_px = ema_now
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"] * trade["remaining"]
                    total_pnl = trade["realized"] + pnl_pct
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += total_pnl
                    stats["tp_sum"] += total_pnl
                    stats["net_sum_usdt"] += total_pnl * entry_usdt
                    stats["tp_sum_usdt"] += total_pnl * entry_usdt
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += total_pnl
                    sym_stats["tp_sum"] += total_pnl
                    sym_stats["net_sum_usdt"] += total_pnl * entry_usdt
                    sym_stats["tp_sum_usdt"] += total_pnl * entry_usdt
                    if total_pnl > 0:
                        stats["wins"] += 1
                        sym_stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                        sym_stats["losses"] += 1
                    trades_out.append(
                        {
                            "symbol": sym,
                            "entry_ts": int(trade["entry_ts"]),
                            "exit_ts": int(ts[i]),
                            "pnl_pct": total_pnl * 100.0,
                            "result": "WIN" if total_pnl > 0 else "LOSS",
                            "reason": "TP",
                        }
                    )
                    trade = None
                    cooldown = cfg.cooldown_bars
                continue

            # HTF 15m confirm filter
            idx_htf = int(np.searchsorted(ts_htf, ts_ms, side="right") - 1)
            if idx_htf <= 0:
                continue
            pass_htf_alignment += 1
            window = max(1, int(args.htf_last_bull_window))
            start_htf = max(0, idx_htf - window + 1)
            last_bull_idx = None
            for j in range(idx_htf, start_htf - 1, -1):
                if float(df_htf["close"].iloc[j]) > float(df_htf["open"].iloc[j]):
                    last_bull_idx = j
                    break
            if last_bull_idx is None:
                continue
            confirm_idx = last_bull_idx + 1
            use_current = not bool(args.htf_confirmed_only)
            if confirm_idx > idx_htf:
                continue
            confirm_row = df_htf.iloc[confirm_idx]
            confirm_ok = float(confirm_row["close"]) < float(confirm_row["open"])
            if not confirm_ok and use_current:
                cur_htf = df_htf.iloc[idx_htf]
                if float(cur_htf["close"]) < float(cur_htf["open"]):
                    confirm_row = cur_htf
                    confirm_idx = idx_htf
                    confirm_ok = True
            if not confirm_ok:
                continue
            avg_body = _avg_body(df_htf, confirm_idx, args.htf_body_lookback)
            if avg_body is None or avg_body <= 0:
                continue
            confirm_body = _body_size(confirm_row)
            confirm_body_ratio = confirm_body / avg_body
            if confirm_body_ratio < float(args.htf_body_min_ratio):
                continue
            pass_htf_body += 1
            confirm_ts = int(df_htf["ts"].iloc[confirm_idx])
            if (not use_current) and ts_ms <= confirm_ts:
                continue
            high_start = max(0, idx_htf - max(1, int(args.htf_high_lookback)) + 1)
            htf_high = float(df_htf["high"].iloc[high_start: idx_htf + 1].max())
            if htf_high <= 0:
                continue
            cur_close = float(df["close"].iloc[sig_idx])
            htf_high_dist = abs((htf_high - cur_close) / htf_high) * 100.0
            atr_htf = _avg_range(df_htf, idx_htf, args.htf_atr_len)
            atr_pct = 0.0
            if isinstance(atr_htf, (int, float)) and atr_htf > 0 and cur_close > 0:
                atr_pct = (float(args.htf_atr_mult) * float(atr_htf) / cur_close) * 100.0
            dist_cap = max(float(args.htf_distance_min), float(args.htf_high_distance_max), atr_pct)
            htf_confirm_count += 1
            htf_confirm_ok = True
            htf_absorption = _upper_wick_ratio(confirm_row) >= cfg.upper_wick_min and _close_pos(confirm_row) <= cfg.close_pos_max

            runup = _runup_pct(df, sig_idx, cfg.runup_bars)
            if runup is None or runup < cfg.runup_min_pct:
                continue
            ru_list = runup_map.get(ts_ms, [])
            if not ru_list:
                continue
            threshold = float(np.percentile(ru_list, 100.0 - cfg.runup_top_pct))
            if runup < threshold:
                continue

            touch_cnt = _high_touch_count(df, sig_idx, cfg.touch_lookback, cfg.touch_band_pct)
            v1_ready = touch_cnt >= cfg.touch_min
            if not v1_ready and touch_cnt >= 2:
                v1_ready = True
            if not v1_ready:
                continue
            v1_ready_count += 1

            # fake break fail
            fb_start = max(0, sig_idx - cfg.fake_break_lookback)
            prev_max = float(df["high"].iloc[fb_start:sig_idx].max()) if sig_idx > fb_start else float(df["high"].iloc[sig_idx])
            cur = df.iloc[sig_idx]
            fake_break = float(cur["high"]) > prev_max and float(cur["close"]) < prev_max
            avg_rng = _avg_range(df, sig_idx, cfg.range_avg_lookback)
            if avg_rng is None:
                continue
            range_now = float(cur["high"] - cur["low"])
            range_ok = range_now >= avg_rng * cfg.range_mult
            fake_break_flag = bool(fake_break and range_ok)

            upper_wick = _upper_wick_ratio(cur)
            close_pos = _close_pos(cur)
            absorption_flag = upper_wick >= cfg.upper_wick_min and close_pos <= cfg.close_pos_max

            if not (fake_break_flag or absorption_flag):
                continue
            # soft distance gate
            hard_ok = htf_high_dist <= dist_cap
            soft_limit = max(dist_cap, float(args.htf_distance_soft_max))
            soft_ok = htf_high_dist <= soft_limit and (
                fake_break_flag
                or (upper_wick >= float(args.ltf_strong_wick_min) and close_pos <= float(args.ltf_strong_close_pos_max))
            )
            if not (hard_ok or soft_ok):
                continue
            pass_htf_distance += 1
            gate_mode = str(args.htf_gate_mode).strip().lower()
            if gate_mode in ("confirm_or_absorption", "confirm-or-absorption", "confirm_or_absorb"):
                htf_gate_ok = htf_confirm_ok or htf_absorption
            else:
                htf_gate_ok = htf_confirm_ok
            if not htf_gate_ok:
                continue
            final_ready_count += 1

            # invalidate if price successfully re-breaks higher
            if float(cur["high"]) > prev_max and float(cur["close"]) > prev_max:
                invalidated = True
                continue

            # Entry
            trigger_low = float(cur["low"])
            trigger_high = float(cur["high"])
            if cfg.entry_mode == "break":
                entry_px = trigger_low - 0.0
                next_low = float(df.iloc[i]["low"])
                if next_low > entry_px:
                    continue
            else:
                retr = max(cfg.entry_retrace_min, min(cfg.entry_retrace_max, 0.5))
                entry_px = trigger_low + (trigger_high - trigger_low) * retr
                next_high = float(df.iloc[i]["high"])
                if next_high < entry_px:
                    continue

            sl_price = trigger_high * (1.0 + cfg.sl_pad_pct / 100.0)
            r = sl_price - entry_px
            if r <= 0:
                continue
            tp1 = entry_px - cfg.tp1_r * r
            tp2 = entry_px - cfg.tp2_r * r

            trade = {
                "entry_px": entry_px,
                "sl_price": sl_price,
                "tp1_price": tp1,
                "tp2_price": tp2,
                "tp1_done": False,
                "tp2_done": False,
                "realized": 0.0,
                "remaining": 1.0,
                "mfe": 0.0,
                "mae": 0.0,
                "hold_bars": 0,
                "entry_ts": ts_ms,
            }
            stats["entries"] += 1
            sym_stats["entries"] += 1
            _log(
                "TOP_FAIL_ENTRY "
                f"sym={sym} runup_pct={runup:.2f} high_touch_count={touch_cnt} "
                f"fake_break={int(fake_break_flag)} absorption={int(absorption_flag)} "
                f"upper_wick_ratio={upper_wick:.2f} entry={entry_px:.6g} sl={sl_price:.6g} "
                f"htf_confirm_time={_ts_kst(confirm_ts)} htf_confirm_body_ratio={confirm_body_ratio:.2f} "
                f"htf_high_distance={htf_high_dist:.2f}"
            )

        trades = sym_stats["trades"]
        if trades > 0:
            sym_stats["winrate"] = (sym_stats["wins"] / trades) * 100.0
            sym_stats["avg_mfe"] = sym_stats["mfe_sum"] / trades
            sym_stats["avg_mae"] = sym_stats["mae_sum"] / trades
            sym_stats["avg_hold"] = sym_stats["hold_sum"] / trades
            line = format_backtest_summary(sym, sym_stats)
            print(line)
            _log(line)
        stats_by_symbol[sym] = sym_stats

    trades = stats["trades"]
    winrate = (stats["wins"] / trades) * 100.0 if trades > 0 else 0.0
    avg_mfe = stats["mfe_sum"] / trades if trades > 0 else 0.0
    avg_mae = stats["mae_sum"] / trades if trades > 0 else 0.0
    avg_hold = stats["hold_sum"] / trades if trades > 0 else 0.0
    print(format_backtest_summary(None, stats))
    print_time_summaries(trades_out, _log)
    if args.print_ready_counts:
        print(
            f"[BACKTEST] READY_COUNTS v1_ready={v1_ready_count} htf_confirm={htf_confirm_count} "
            f"pass_htf_alignment={pass_htf_alignment} pass_htf_body={pass_htf_body} "
            f"pass_htf_distance={pass_htf_distance} final_ready={final_ready_count}"
        )
    print(f"[BACKTEST] WARMUP auto days={warmup_days} minutes={warmup_minutes} eval_days={args.days}")


if __name__ == "__main__":
    run_backtest()
