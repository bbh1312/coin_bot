#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
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
)
from engines.fakeout_short_v1.engine import FakeoutShortV1Config


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _ohlcv_cache_path(root_dir: str, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe = _sanitize_symbol(symbol)
    return os.path.join(
        root_dir,
        "logs",
        "fakeout_short_v1",
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
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df.to_csv(path, index=False)


def _fetch_ohlcv_all(
    exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    cache_only: bool,
    use_common_warmup: bool,
    common_warmup_dir: str,
    common_only: bool,
) -> List[list]:
    if common_only and use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_ohlcv_cache(warmup_path)
        if warmup_rows:
            return [r for r in warmup_rows if int(r[0]) <= end_ms]
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        return cached
    if cache_only or exchange is None:
        return []
    rows = []
    since = start_ms
    limit = 1500
    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not batch:
            break
        for r in batch:
            if r[0] > end_ms:
                break
            rows.append(r)
        if batch[-1][0] >= end_ms:
            break
        since = batch[-1][0] + 1
        time.sleep(exchange.rateLimit / 1000.0)
    _write_ohlcv_cache(cache_path, rows)
    return rows


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    out = 100 - (100 / (1 + rs))
    return out.fillna(0.0)


def mfi(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    typical = (high + low + close) / 3.0
    raw = typical * volume
    direction = typical.diff()
    pos = raw.where(direction > 0, 0.0)
    neg = raw.where(direction < 0, 0.0).abs()
    pos_sum = pos.rolling(length).sum()
    neg_sum = neg.rolling(length).sum().replace(0, float("nan"))
    mfr = pos_sum / neg_sum
    out = 100 - (100 / (1 + mfr))
    return out.fillna(0.0)


def obv(series: pd.Series, volume: pd.Series) -> pd.Series:
    delta = series.diff()
    direction = delta.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (direction * volume).fillna(0.0).cumsum()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def bbands(series: pd.Series, length: int, std_mult: float) -> Tuple[pd.Series, pd.Series, pd.Series]:
    mid = series.rolling(length).mean()
    std = series.rolling(length).std(ddof=0)
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    return upper, mid, lower


def _upper_wick_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    upper = high - max(open_, close)
    return upper / rng


def _body_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    return abs(close - open_) / rng


def _tf_minutes(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    raise ValueError(f"Unsupported tf: {tf}")


def _is_shooting_star(row: pd.Series, wick_min: float, body_max: float) -> bool:
    return _upper_wick_ratio(row) >= float(wick_min) and _body_ratio(row) <= float(body_max)


def _is_bear_engulf(prev: pd.Series, cur: pd.Series) -> bool:
    return (
        float(prev["close"]) > float(prev["open"])
        and float(cur["close"]) < float(cur["open"])
        and float(cur["open"]) >= float(prev["close"])
        and float(cur["close"]) <= float(prev["open"])
    )


def _is_upper_wick_dom(cur: pd.Series, ratio: float) -> bool:
    open_ = float(cur["open"])
    close = float(cur["close"])
    high = float(cur["high"])
    low = float(cur["low"])
    body = abs(close - open_)
    upper = high - max(open_, close)
    if body <= 0:
        return False
    return upper >= body * ratio and high > low


def _find_last_two_swing_highs(highs: pd.Series, lookback: int, window: int) -> Optional[Tuple[int, int]]:
    if len(highs) < lookback + window * 2 + 1:
        return None
    start = max(0, len(highs) - lookback)
    idxs = []
    for i in range(start + window, len(highs) - window):
        cur = highs.iloc[i]
        if all(cur > highs.iloc[i - window:i]) and all(cur > highs.iloc[i + 1:i + 1 + window]):
            idxs.append(i)
    if len(idxs) < 2:
        return None
    return idxs[-2], idxs[-1]


def _divergence_ok(df: pd.DataFrame, rsi_series: pd.Series, cfg: FakeoutShortV1Config) -> bool:
    highs = df["high"].astype(float)
    pair = _find_last_two_swing_highs(highs, cfg.div_lookback, cfg.div_swing_window)
    if not pair:
        return False
    i1, i2 = pair
    if highs.iloc[i2] <= highs.iloc[i1]:
        return False
    r1 = float(rsi_series.iloc[i1])
    r2 = float(rsi_series.iloc[i2])
    return r2 <= r1 - float(cfg.div_rsi_delta)


def _obv_divergence_ok(df: pd.DataFrame, obv_series: pd.Series, cfg: FakeoutShortV1Config) -> bool:
    highs = df["high"].astype(float)
    pair = _find_last_two_swing_highs(highs, cfg.div_lookback, cfg.div_swing_window)
    if not pair:
        return False
    i1, i2 = pair
    if highs.iloc[i2] <= highs.iloc[i1]:
        return False
    o1 = float(obv_series.iloc[i1])
    o2 = float(obv_series.iloc[i2])
    return o2 < o1


def run_backtest() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)

    parser.add_argument("--change-24h-min-pct", type=float, default=10.0)
    parser.add_argument("--trend-rsi-len", type=int, default=14)
    parser.add_argument("--trend-rsi-min", type=float, default=60.0)

    parser.add_argument("--rsi-len", type=int, default=14)
    parser.add_argument("--ema20-len", type=int, default=20)
    parser.add_argument("--ema120-len", type=int, default=120)
    parser.add_argument("--obv-ema-len", type=int, default=20)
    parser.add_argument("--vol-sma-len", type=int, default=20)
    parser.add_argument("--scan-ignore-ema20", action="store_true")

    parser.add_argument("--hod-fail-min", type=int, default=6)
    parser.add_argument("--near-hod-band", type=float, default=0.0015)
    parser.add_argument("--touch-band", type=float, default=0.0015)
    parser.add_argument("--touch-break", type=float, default=0.0003)
    parser.add_argument("--touch-max", type=int, default=4)

    parser.add_argument("--ext-ema7-max", type=float, default=0.007)
    parser.add_argument("--ema120-floor", type=float, default=1.0)
    parser.add_argument("--vol-expand-min", type=float, default=1.1)
    parser.add_argument("--uptrend-block-min-hits", type=int, default=2)
    parser.add_argument("--uptrend-consec-bull-min", type=int, default=3)
    parser.add_argument("--ema20-slope-min-mult", type=float, default=0.0002)
    parser.add_argument("--disable-block-uptrend", action="store_true")
    parser.add_argument("--disable-block-hod-fatigue", action="store_true")

    parser.add_argument("--retest-band", type=float, default=0.0025)
    parser.add_argument("--retest-break", type=float, default=0.0008)
    parser.add_argument("--break-low-lookback", type=int, default=5)
    parser.add_argument("--use-break-low", action="store_true")
    parser.add_argument("--ignore-retest-touch", action="store_true")
    parser.add_argument("--entry-simple", action="store_true")
    parser.add_argument("--entry-log", action="store_true", help="Print 3m entry checks when env_on and not blocked.")

    parser.add_argument("--tf-exec", type=str, default="3m")
    parser.add_argument("--ema7-len", type=int, default=7)
    parser.add_argument("--ema20-len-3m", type=int, default=20)
    parser.add_argument("--atr-len", type=int, default=14)

    parser.add_argument("--sl-atr-mult", type=float, default=0.3)
    parser.add_argument("--tp-r-mult", type=float, default=1.5)
    parser.add_argument("--block-log-path", type=str, default="logs/fakeout_short_v1/block_log.jsonl")
    parser.add_argument("--env-log", action="store_true", help="Print ENV snapshot each 15m close.")

    args = parser.parse_args()

    cfg = FakeoutShortV1Config(
        change_24h_min_pct=args.change_24h_min_pct,
        trend_rsi_len=args.trend_rsi_len,
        trend_rsi_min=args.trend_rsi_min,
        rsi_len=args.rsi_len,
        ema20_len=args.ema20_len,
        ema120_len=args.ema120_len,
        obv_ema_len=args.obv_ema_len,
        vol_sma_len=args.vol_sma_len,
        hod_fail_min=args.hod_fail_min,
        near_hod_band=args.near_hod_band,
        touch_band=args.touch_band,
        touch_break=args.touch_break,
        touch_max=args.touch_max,
        ext_ema7_max=args.ext_ema7_max,
        ema120_floor=args.ema120_floor,
        vol_expand_min=args.vol_expand_min,
        retest_band=args.retest_band,
        retest_break=args.retest_break,
        break_low_lookback=args.break_low_lookback,
        ema7_len=args.ema7_len,
        ema20_len_3m=args.ema20_len_3m,
        atr_len=args.atr_len,
        sl_atr_mult=args.sl_atr_mult,
        tp_r_mult=args.tp_r_mult,
    )
    cfg.tf_exec = args.tf_exec

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    bars_24h = int((24 * 60) / _tf_minutes(cfg.tf_scan))
    min_main = max(cfg.ema120_len + 5, cfg.vol_sma_len + 5, 200)
    min_exec = max(cfg.ema20_len_3m + 5, cfg.atr_len + 5, 300)
    min_scan = max(cfg.ema120_len + 5, bars_24h + 5, 200)

    start_ms, eval_start_ms, _, _ = calc_warmup_window(
        args.days,
        end_ms,
        {cfg.tf_main: min_main, cfg.tf_exec: min_exec, cfg.tf_scan: min_scan},
    )

    universe = load_common_universe(args.universe, exchange, args.cache_only)
    if args.top_n and args.top_n > 0:
        universe = universe[: int(args.top_n)]

    use_common = bool(args.common_only)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    if args.common_only:
        args.cache_only = True

    data_main: Dict[str, pd.DataFrame] = {}
    data_exec: Dict[str, pd.DataFrame] = {}
    data_scan: Dict[str, pd.DataFrame] = {}

    btc_symbol = "BTC/USDT:USDT"
    btc_alt_symbol = "BTC/USDT"
    btc_1m = None
    btc_5m = None
    btc_1h = None

    # preload BTC filters
    btc_rows_1m = _fetch_ohlcv_all(
        exchange, btc_symbol, "1m", start_ms, end_ms,
        cache_only=args.cache_only, use_common_warmup=use_common,
        common_warmup_dir=common_dir, common_only=args.common_only,
    )
    if not btc_rows_1m:
        btc_rows_1m = _fetch_ohlcv_all(
            exchange, btc_alt_symbol, "1m", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
    btc_rows_5m = _fetch_ohlcv_all(
        exchange, btc_symbol, "5m", start_ms, end_ms,
        cache_only=args.cache_only, use_common_warmup=use_common,
        common_warmup_dir=common_dir, common_only=args.common_only,
    )
    if not btc_rows_5m:
        btc_rows_5m = _fetch_ohlcv_all(
            exchange, btc_alt_symbol, "5m", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
    btc_rows_1h = _fetch_ohlcv_all(
        exchange, btc_symbol, "1h", start_ms, end_ms,
        cache_only=args.cache_only, use_common_warmup=use_common,
        common_warmup_dir=common_dir, common_only=args.common_only,
    )
    if not btc_rows_1h:
        btc_rows_1h = _fetch_ohlcv_all(
            exchange, btc_alt_symbol, "1h", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
    if btc_rows_1m:
        btc_1m = pd.DataFrame(btc_rows_1m, columns=["ts","open","high","low","close","volume"])
    if btc_rows_5m:
        btc_5m = pd.DataFrame(btc_rows_5m, columns=["ts","open","high","low","close","volume"])
    if btc_rows_1h:
        btc_1h = pd.DataFrame(btc_rows_1h, columns=["ts","open","high","low","close","volume"])

    for sym in universe:
        rows_scan = _fetch_ohlcv_all(
            exchange, sym, cfg.tf_scan, start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        rows_main = _fetch_ohlcv_all(
            exchange, sym, cfg.tf_main, start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        rows_exec = _fetch_ohlcv_all(
            exchange, sym, cfg.tf_exec, start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        if rows_scan and rows_main and rows_exec:
            df_scan = pd.DataFrame(rows_scan, columns=["ts", "open", "high", "low", "close", "volume"])
            df_main = pd.DataFrame(rows_main, columns=["ts", "open", "high", "low", "close", "volume"])
            df_exec = pd.DataFrame(rows_exec, columns=["ts", "open", "high", "low", "close", "volume"])
            data_scan[sym] = df_scan
            data_main[sym] = df_main
            data_exec[sym] = df_exec

    if not data_main:
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
    }

    trades_out: List[dict] = []
    block_log_path = Path(args.block_log_path) if args.block_log_path else None
    block_log_fh = None
    if block_log_path:
        block_log_path.parent.mkdir(parents=True, exist_ok=True)
        block_log_fh = block_log_path.open("a", encoding="utf-8")
    block_counts = {
        "UPTREND_15M": 0,
        "EXTENSION_3M": 0,
        "BELOW_EMA120_15M": 0,
        "VOLUME_EXPAND_15M": 0,
        "HOD_FATIGUE": 0,
        "BREAKOUT_3M": 0,
    }
    env_on_bars = 0
    entry_ready_bars = 0

    for sym, df_main in data_main.items():
        df_scan = data_scan.get(sym)
        df_exec = data_exec.get(sym)
        if df_scan is None or df_exec is None:
            continue

        df_main_sig = df_main.iloc[:-1] if args.use_confirmed else df_main
        df_exec_sig = df_exec.iloc[:-1] if args.use_confirmed else df_exec
        df_scan_sig = df_scan.iloc[:-1] if args.use_confirmed else df_scan

        ts_main = df_main_sig["ts"].astype(int).to_numpy()
        ts_exec = df_exec_sig["ts"].astype(int).to_numpy()
        ts_scan = df_scan_sig["ts"].astype(int).to_numpy()

        rsi_main = rsi(df_main_sig["close"].astype(float), cfg.rsi_len)
        rsi_sma5 = rsi_main.rolling(5).mean()
        rsi_scan = rsi(df_scan_sig["close"].astype(float), cfg.trend_rsi_len)
        ema20_main = ema(df_main_sig["close"].astype(float), cfg.ema20_len)
        ema120_main = ema(df_main_sig["close"].astype(float), cfg.ema120_len)
        ema20_scan = ema(df_scan_sig["close"].astype(float), cfg.ema20_len)
        obv_main = obv(df_main_sig["close"].astype(float), df_main_sig["volume"].astype(float))
        obv_ema = ema(obv_main.astype(float), cfg.obv_ema_len)
        obv_slope = obv_ema.diff()
        vol_sma = df_main_sig["volume"].astype(float).rolling(cfg.vol_sma_len).mean()

        ema7_exec = ema(df_exec_sig["close"].astype(float), cfg.ema7_len)
        ema20_exec = ema(df_exec_sig["close"].astype(float), cfg.ema20_len_3m)
        atr_exec = atr(df_exec_sig, cfg.atr_len)

        trade = None
        main_idx = 0
        cur_session_start_ms = None
        hod_price = None
        hod_ts = None
        hod_fail_count = 0
        touch_count = 0
        in_hod_zone = False
        env_on = False
        block_flags_15m: List[str] = []
        retest_lower = None
        retest_upper = None
        require_break_low = False
        armed = False
        armed_ttl = 0

        for idx_exec in range(1, len(df_exec_sig)):
            exec_ts = int(ts_exec[idx_exec])
            if exec_ts < eval_start_ms:
                continue

            # update 15m environment up to this exec bar
            while main_idx < len(df_main_sig) and ts_main[main_idx] <= exec_ts:
                ts_ms = int(ts_main[main_idx])
                dt_kst = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + pd.Timedelta(hours=9)
                day_start = dt_kst.replace(hour=9, minute=0, second=0, microsecond=0)
                if dt_kst < day_start:
                    day_start = day_start - pd.Timedelta(days=1)
                day_start_ms = int((day_start - pd.Timedelta(hours=9)).timestamp() * 1000)
                if cur_session_start_ms != day_start_ms:
                    cur_session_start_ms = day_start_ms
                    hod_price = None
                    hod_ts = None
                    hod_fail_count = 0
                    touch_count = 0
                    in_hod_zone = False
                    armed = False
                    armed_ttl = 0

                bar = df_main_sig.iloc[main_idx]
                high_15m = float(bar["high"])
                low_15m = float(bar["low"])
                close_15m = float(bar["close"])
                open_15m = float(bar["open"])
                volume_15m = float(bar["volume"])

                if hod_price is None or high_15m > hod_price:
                    hod_price = high_15m
                    hod_ts = ts_ms
                    hod_fail_count = 0
                    armed = False
                    armed_ttl = 0
                else:
                    hod_fail_count += 1

                # touch count (edge-based to avoid overcounting)
                if hod_price is not None:
                    touch_low = hod_price * (1.0 - float(cfg.touch_band))
                    touch_high = hod_price * (1.0 + float(cfg.touch_break))
                    in_zone = touch_low <= close_15m <= touch_high
                    if in_zone and not in_hod_zone:
                        touch_count += 1
                    in_hod_zone = in_zone

                # ENV snapshot
                near_hod_attempt = False
                if hod_price is not None and hod_fail_count > 0:
                    start_idx = max(0, main_idx - hod_fail_count + 1)
                    recent_high = float(df_main_sig["high"].iloc[start_idx:main_idx + 1].max())
                    near_hod_attempt = recent_high >= hod_price * (1.0 - float(cfg.near_hod_band))

                rsi_weak = False
                if main_idx >= 3:
                    r0 = float(rsi_main.iloc[main_idx])
                    r1 = float(rsi_main.iloc[main_idx - 1])
                    r2 = float(rsi_main.iloc[main_idx - 2])
                    r3 = float(rsi_main.iloc[main_idx - 3])
                    if r0 < r1 < r2 < r3:
                        rsi_weak = True
                rsi_sma_val = float(rsi_sma5.iloc[main_idx]) if not np.isnan(rsi_sma5.iloc[main_idx]) else None
                if rsi_sma_val is not None and float(rsi_main.iloc[main_idx]) < rsi_sma_val:
                    rsi_weak = True

                obv_weak = False
                if not np.isnan(obv_slope.iloc[main_idx]) and float(obv_slope.iloc[main_idx]) < 0:
                    obv_weak = True

                # scan filters
                idx_scan = int(np.searchsorted(ts_scan, ts_ms, side="right") - 1)
                scan_ok = False
                scan_reason = []
                close_scan = None
                ema_scan = None
                rsi_scan_val = None
                change_24h = None
                if idx_scan > 0 and idx_scan >= bars_24h:
                    close_scan = float(df_scan_sig["close"].iloc[idx_scan])
                    ema_scan = float(ema20_scan.iloc[idx_scan])
                    rsi_scan_val = float(rsi_scan.iloc[idx_scan])
                    ema_ok = close_scan > ema_scan or args.scan_ignore_ema20
                    if not ema_ok:
                        scan_reason.append("EMA20")
                    if rsi_scan_val < float(cfg.trend_rsi_min):
                        scan_reason.append("RSI")
                    close_24h = float(df_scan_sig["close"].iloc[idx_scan - bars_24h])
                    if close_24h > 0:
                        change_24h = (close_scan - close_24h) / close_24h * 100.0
                        if change_24h < float(cfg.change_24h_min_pct):
                            scan_reason.append("CHANGE24H")
                        if ema_ok and rsi_scan_val >= float(cfg.trend_rsi_min) and change_24h >= float(cfg.change_24h_min_pct):
                            scan_ok = True
                    else:
                        scan_reason.append("CLOSE24H")
                else:
                    scan_reason.append("IDX")

                env_on = (
                    scan_ok
                    and hod_fail_count >= int(cfg.hod_fail_min)
                    and near_hod_attempt
                    and rsi_weak
                    and obv_weak
                )
                block_flags_15m = []
                uptrend_hits = 0
                uptrend_subflags = []
                consec_bull_ok = False
                hhhl_ok = False
                ema20_slope_ok = False
                if main_idx >= 2:
                    prev = df_main_sig.iloc[main_idx - 1]
                    prev2 = df_main_sig.iloc[main_idx - 2]
                    consec_bull_ok = (
                        close_15m > open_15m
                        and float(prev["close"]) > float(prev["open"])
                        and float(prev2["close"]) > float(prev2["open"])
                    )
                if consec_bull_ok:
                    uptrend_hits += 1
                    uptrend_subflags.append("CONSEC_BULL")
                if main_idx >= 1:
                    prev = df_main_sig.iloc[main_idx - 1]
                    hhhl_ok = high_15m > float(prev["high"]) and low_15m > float(prev["low"])
                if hhhl_ok:
                    uptrend_hits += 1
                    uptrend_subflags.append("HHHL")
                ema20_now = float(ema20_main.iloc[main_idx])
                ema20_prev = float(ema20_main.iloc[main_idx - 1]) if main_idx >= 1 else ema20_now
                ema20_slope = ema20_now - ema20_prev
                slope_min = float(args.ema20_slope_min_mult) * close_15m
                ema20_slope_ok = close_15m > ema20_now and ema20_slope > slope_min
                if ema20_slope_ok:
                    uptrend_hits += 1
                    uptrend_subflags.append("ABOVE_EMA20_SLOPEUP")
                if not args.disable_block_uptrend and uptrend_hits >= int(args.uptrend_block_min_hits):
                    block_flags_15m.append("UPTREND_15M")
                ema120_now = float(ema120_main.iloc[main_idx])
                require_break_low = False
                if ema120_now > 0 and close_15m < ema120_now * float(cfg.ema120_floor):
                    require_break_low = True
                vol_ma = float(vol_sma.iloc[main_idx])
                if vol_ma > 0 and (volume_15m / vol_ma) >= float(cfg.vol_expand_min):
                    require_break_low = True
                if not args.disable_block_hod_fatigue and touch_count >= int(cfg.touch_max):
                    require_break_low = True

                if hod_price is not None:
                    retest_lower = hod_price * (1.0 - float(cfg.retest_band))
                    retest_upper = hod_price * (1.0 + float(cfg.retest_break))

                main_idx += 1

                if args.env_log:
                    vol_ma = float(vol_sma.iloc[main_idx - 1])
                    vol_ratio = (volume_15m / vol_ma) if vol_ma > 0 else None
                    print(
                        "[BACKTEST][ENV] "
                        f"{sym} { _ts_kst(ts_ms) } "
                        f"scan_ok={scan_ok} scan_reason={scan_reason} idx_scan={idx_scan} bars_24h={bars_24h} "
                        f"close_scan={close_scan} ema20={ema_scan} rsi_scan={rsi_scan_val} change_24h={change_24h} "
                        f"hod={hod_price} hod_fail={hod_fail_count} near_hod={near_hod_attempt} "
                        f"rsi_weak={rsi_weak} obv_weak={obv_weak} "
                        f"vol_ratio={vol_ratio} touch_count={touch_count} env_on={env_on} "
                        f"uptrend_subflags={uptrend_subflags} consec_bull_15m={consec_bull_ok} "
                        f"hhhl={hhhl_ok} close15_gt_ema20={(close_15m > ema20_now)} "
                        f"ema20_slope={ema20_slope}"
                    )

            if not env_on or retest_lower is None or retest_upper is None:
                continue

            exec_open = float(df_exec_sig["open"].iloc[idx_exec])
            exec_close = float(df_exec_sig["close"].iloc[idx_exec])
            exec_high = float(df_exec_sig["high"].iloc[idx_exec])
            exec_low = float(df_exec_sig["low"].iloc[idx_exec])

            # extension block (3m)
            block_flags = list(block_flags_15m)
            ema7 = float(ema7_exec.iloc[idx_exec])
            if ema7 > 0:
                dist_ema7 = (ema7 - exec_close) / ema7
                if dist_ema7 >= float(cfg.ext_ema7_max):
                    block_flags.append("EXTENSION_3M")

            if env_on:
                env_on_bars += 1
                if block_flags:
                    for reason in set(block_flags):
                        block_counts[reason] += 1
                    if block_log_fh:
                        block_event = {
                            "ts_kst": _ts_kst(exec_ts),
                            "symbol": sym,
                            "hod": hod_price,
                            "hod_fail_count": hod_fail_count,
                            "touch_count": touch_count,
                            "retest_lower": retest_lower,
                            "retest_upper": retest_upper,
                            "block_reasons": block_flags,
                            "uptrend_subflags": uptrend_subflags,
                            "consec_bull_15m": consec_bull_ok,
                            "hhhl": hhhl_ok,
                            "close15_gt_ema20": close_15m > ema20_now,
                            "ema20_slope": ema20_slope,
                        }
                        block_log_fh.write(json.dumps(block_event, ensure_ascii=False) + "\n")
                        block_log_fh.flush()
                    print(
                        f"[BACKTEST][BLOCK] {sym} { _ts_kst(exec_ts) } reasons={block_flags} "
                        f"uptrend_subflags={uptrend_subflags} consec_bull_15m={consec_bull_ok} hhhl={hhhl_ok} "
                        f"close15_gt_ema20={close_15m > ema20_now} ema20_slope={ema20_slope}"
                    )
                else:
                    entry_ready_bars += 1

            if block_flags or trade:
                # manage open trade with 3m bars
                if trade:
                    high = exec_high
                    low = exec_low
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
                        stats["losses"] += 1
                        trades_out.append({
                            "symbol": sym,
                            "result": "LOSS",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": exec_ts,
                        })
                        trade = None
                    elif low <= trade["tp_price"]:
                        exit_px = trade["tp_price"]
                        pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                        stats["exits"] += 1
                        stats["trades"] += 1
                        stats["mfe_sum"] += trade["mfe"]
                        stats["mae_sum"] += trade["mae"]
                        stats["hold_sum"] += trade["hold_bars"]
                        stats["net_sum"] += pnl_pct
                        stats["tp_sum"] += pnl_pct
                        stats["wins"] += 1
                        trades_out.append({
                            "symbol": sym,
                            "result": "WIN",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": exec_ts,
                        })
                        trade = None
                continue

            # entry logic (3m)
            breakout = exec_high > retest_upper if retest_upper is not None else False
            touch = exec_high >= retest_lower and exec_high <= retest_upper
            if args.ignore_retest_touch:
                touch = True
            if breakout:
                block_flags.append("BREAKOUT_3M")
            if block_flags:
                if env_on:
                    for reason in set(block_flags):
                        block_counts[reason] = block_counts.get(reason, 0) + 1
                continue

            if touch:
                armed = True
                armed_ttl = 8
            if armed:
                armed_ttl -= 1
                if armed_ttl <= 0:
                    armed = False

            break_low = False
            weak_rebound = False
            if idx_exec >= int(cfg.break_low_lookback):
                window = df_exec_sig.iloc[idx_exec - int(cfg.break_low_lookback) + 1: idx_exec + 1]
                break_low = exec_close < float(window["low"].min())
                weak_rebound = exec_high < float(ema20_exec.iloc[idx_exec])
            reject = exec_close < float(ema20_exec.iloc[idx_exec]) and exec_close < exec_open

            entry_ready = False
            if armed:
                if args.use_break_low or require_break_low:
                    entry_ready = break_low and weak_rebound
                else:
                    entry_ready = reject or break_low

            if args.entry_log and (env_on and not block_flags):
                print(
                    "[BACKTEST][ENTRY] "
                    f"{sym} {_ts_kst(exec_ts)} "
                    f"touch={touch} retest=({retest_lower:.6f},{retest_upper:.6f}) "
                    f"close={exec_close:.6f} open={exec_open:.6f} high={exec_high:.6f} "
                    f"ema20_3m={float(ema20_exec.iloc[idx_exec]):.6f} "
                    f"break_low={break_low} weak_rebound={weak_rebound} entry_ready={entry_ready}"
                )

            if not entry_ready:
                continue

            atr_val = float(atr_exec.iloc[idx_exec])
            if np.isnan(atr_val) or atr_val <= 0:
                continue
            sl_price = max(exec_high, retest_upper) + atr_val * float(cfg.sl_atr_mult)
            entry_px = exec_close
            risk = sl_price - entry_px
            if risk <= 0:
                continue
            tp_price = entry_px - risk * float(cfg.tp_r_mult)
            if tp_price >= entry_px:
                continue

            trade = {
                "entry_px": entry_px,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "mfe": 0.0,
                "mae": 0.0,
                "hold_bars": 0,
                "entry_ts": exec_ts,
            }
            stats["entries"] += 1

    print("[BACKTEST] TRADES(KST) symbol result pnl_pct entry_ts exit_ts")
    for t in trades_out:
        print(
            f"[BACKTEST] TRADE {t['symbol']} result={t['result']} pnl_pct={t['pnl_pct']:.2f}% "
            f"entry_ts={_ts_kst(t['entry_ts'])} exit_ts={_ts_kst(t['exit_ts'])}"
        )

    print(format_backtest_summary(None, stats))
    print(
        "[BACKTEST] FUNNEL "
        f"env_on_bars={env_on_bars} "
        f"entry_ready_bars={entry_ready_bars} "
        f"entries={stats['entries']}"
    )
    print(
        "[BACKTEST] BLOCK_COUNTS "
        + " ".join([f"{k}={v}" for k, v in block_counts.items()])
    )
    if block_log_fh:
        block_log_fh.close()


def _ts_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt + pd.Timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    run_backtest()
