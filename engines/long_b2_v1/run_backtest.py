#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import ccxt
import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import calc_warmup_window, load_common_universe, log_warmup_info


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
        "long_b2_v1",
        "ohlcv_cache",
        f"{safe}_{timeframe}_{start_ms}_{end_ms}.csv",
    )


def _entry_kst_info(ts_ms: int) -> Tuple[int, str]:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
    hour = int(dt.strftime("%H"))
    dow = dt.strftime("%a")
    return hour, dow


def _fmt_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M")


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
        if len(batch) < 2:
            break
        time.sleep(max(exchange.rateLimit, 200) / 1000.0)
    if out:
        out = out[:-1]
    _write_ohlcv_cache(cache_path, out)
    return out


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def _bb_mid(series: pd.Series, length: int = 20) -> pd.Series:
    return series.rolling(length).mean()


def _bb_lower(series: pd.Series, length: int = 20, mult: float = 2.0) -> pd.Series:
    ma = series.rolling(length).mean()
    std = series.rolling(length).std()
    return ma - mult * std


def _obv(df: pd.DataFrame) -> pd.Series:
    close = df["close"].astype(float)
    vol = df["volume"].astype(float)
    direction = np.sign(close.diff().fillna(0.0))
    return (vol * direction).fillna(0.0).cumsum()


def _fib_levels(high: float, low: float) -> List[float]:
    if high <= low:
        return []
    return [
        high - (high - low) * 0.382,
        high - (high - low) * 0.5,
        high - (high - low) * 0.618,
    ]


def _in_zone(price: float, level: float, eps: float) -> bool:
    if level <= 0:
        return False
    return abs(price - level) / level <= eps


def _avg_up_volume(df: pd.DataFrame, lookback: int) -> float:
    if len(df) < lookback:
        lookback = len(df)
    recent = df.iloc[-lookback:]
    up = recent[recent["close"] > recent["open"]]
    if up.empty:
        return 0.0
    return float(up["volume"].mean())


def _btc_guard(btc_1h: pd.DataFrame, btc_15m: pd.DataFrame, btc_1m: pd.DataFrame, ts_ms: int) -> bool:
    if btc_1h.empty or btc_15m.empty or btc_1m.empty:
        return False
    ts_1h = btc_1h["ts"].astype(int).to_numpy()
    ts_15m = btc_15m["ts"].astype(int).to_numpy()
    ts_1m = btc_1m["ts"].astype(int).to_numpy()
    idx_1h = int(np.searchsorted(ts_1h, ts_ms, side="right") - 1)
    idx_15m = int(np.searchsorted(ts_15m, ts_ms, side="right") - 1)
    idx_1m = int(np.searchsorted(ts_1m, ts_ms, side="right") - 1)
    if idx_1h <= 0 or idx_15m <= 0 or idx_1m < 5:
        return False
    ema20 = _ema(btc_1h["close"].astype(float), 20)
    if float(btc_1h["close"].iloc[idx_1h]) <= float(ema20.iloc[idx_1h]):
        return False
    rsi_15 = _rsi(btc_15m["close"].astype(float), 14)
    if float(rsi_15.iloc[idx_15m]) <= 48.0:
        return False
    closes = btc_1m["close"].astype(float).iloc[idx_1m - 4: idx_1m + 1].to_numpy()
    drops = (closes[1:] - closes[:-1]) / closes[:-1]
    if np.any(drops <= -0.003):
        return False
    return True


def _body_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    return abs(close - open_) / rng


def _is_bull_body(row: pd.Series, min_ratio: float) -> bool:
    return float(row["close"]) > float(row["open"]) and _body_ratio(row) >= min_ratio


def _rsi_fast_drop(rsi_series: pd.Series, idx: int, from_level: float, to_level: float, bars: int) -> bool:
    if idx <= 0 or bars <= 0:
        return False
    rsi_now = float(rsi_series.iloc[idx])
    if rsi_now >= to_level:
        return False
    start = max(0, idx - bars)
    window = rsi_series.iloc[start: idx + 1]
    return bool((window >= from_level).any())


def _read_oi_cache(path: str) -> List[Tuple[int, float]]:
    if not path or not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty or "ts" not in df.columns:
            return []
        return list(zip(df["ts"].astype(int).tolist(), df["oi"].astype(float).tolist()))
    except Exception:
        return []

def _oi_at(oi_rows: List[Tuple[int, float]], ts_ms: int) -> Optional[float]:
    if not oi_rows:
        return None
    # assume sorted by ts
    lo, hi = 0, len(oi_rows) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        ts, val = oi_rows[mid]
        if ts == ts_ms:
            return val
        if ts < ts_ms:
            lo = mid + 1
        else:
            hi = mid - 1
    if hi >= 0:
        return oi_rows[hi][1]
    return None


def run_backtest() -> None:
    parser = argparse.ArgumentParser("long_b2_v1 backtest")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--use-live-cache", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--tp-pct", type=float, default=0.09)
    parser.add_argument("--sl-pct", type=float, default=0.04)
    parser.add_argument("--cooldown-bars", type=int, default=18)
    parser.add_argument("--time-stop-min", type=int, default=120)
    parser.add_argument("--lookback-up", type=int, default=14)
    parser.add_argument("--vol-ratio-max", type=float, default=0.5)
    parser.add_argument("--obv-flat-min", type=float, default=0.0)
    parser.add_argument("--atr-spike-mult", type=float, default=2.0)
    parser.add_argument("--tp1-ratio", type=float, default=0.3)
    parser.add_argument("--trail-pct", type=float, default=0.01)
    parser.add_argument("--swing-lookback", type=int, default=50)
    parser.add_argument("--body-min", type=float, default=0.6)
    parser.add_argument("--rsi-drop-bars", type=int, default=4)
    parser.add_argument("--rsi-drop-from", type=float, default=70.0)
    parser.add_argument("--rsi-drop-to", type=float, default=40.0)
    parser.add_argument("--use-oi", action="store_true")
    parser.add_argument("--oi-cache-dir", type=str, default="")
    args = parser.parse_args()

    exchange = ccxt.binance({"enableRateLimit": True})

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    min_tr = 200
    min_main = max(args.swing_lookback, 120)
    min_exec = max(args.lookback_up + 20, 200)
    start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
        args.days,
        end_ms,
        {"1h": min_tr, "15m": min_main, "3m": min_exec},
    )

    universe = load_common_universe(args.universe, exchange, args.cache_only)
    base_dir = os.path.join(ROOT_DIR, "logs", "long_b2_v1", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(base_dir, f"backtest_{date_tag}.log")

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    use_common = bool(args.use_live_cache)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    oi_dir = args.oi_cache_dir or os.path.join(ROOT_DIR, "logs", "long_b2_v1", "oi_cache")

    # BTC guard data
    btc_1h_rows = _fetch_ohlcv_all(exchange, "BTC/USDT:USDT", "1h", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
    btc_15_rows = _fetch_ohlcv_all(exchange, "BTC/USDT:USDT", "15m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
    btc_1m_rows = _fetch_ohlcv_all(exchange, "BTC/USDT:USDT", "1m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
    btc_1h = pd.DataFrame(btc_1h_rows, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
    btc_15m = pd.DataFrame(btc_15_rows, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
    btc_1m = pd.DataFrame(btc_1m_rows, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)

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

    for sym in universe:
        oi_rows = []
        if args.use_oi:
            oi_path = os.path.join(oi_dir, f"{_sanitize_symbol(sym)}_3m.csv")
            oi_rows = _read_oi_cache(oi_path)
        rows_tr = _fetch_ohlcv_all(exchange, sym, "1h", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
        rows_main = _fetch_ohlcv_all(exchange, sym, "15m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
        rows_ex = _fetch_ohlcv_all(exchange, sym, "3m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=args.cache_only)
        if not rows_tr or not rows_main or not rows_ex:
            continue

        df_tr = pd.DataFrame(rows_tr, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_main = pd.DataFrame(rows_main, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_ex = pd.DataFrame(rows_ex, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)

        ema20_tr = _ema(df_tr["close"].astype(float), 20)
        ema60_tr = _ema(df_tr["close"].astype(float), 60)
        ema120_tr = _ema(df_tr["close"].astype(float), 120)

        ema20_main = _ema(df_main["close"].astype(float), 20)
        ema60_main = _ema(df_main["close"].astype(float), 60)
        bb_mid = _bb_mid(df_main["close"].astype(float), 20)
        bb_lower = _bb_lower(df_main["close"].astype(float), 20, 2.0)
        rsi_main = _rsi(df_main["close"].astype(float), 14)
        atr_main = _atr(df_main, 14)

        ema10_ex = _ema(df_ex["close"].astype(float), 10)
        rsi_ex = _rsi(df_ex["close"].astype(float), 14)
        atr_ex = _atr(df_ex, 14)
        obv_ex = _obv(df_ex)

        ts_tr = df_tr["ts"].astype(int).to_numpy()
        ts_main = df_main["ts"].astype(int).to_numpy()
        ts_ex = df_ex["ts"].astype(int).to_numpy()

        trade: Optional[dict] = None
        cooldown_left = 0
        last_breakdown_idx = None

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

        for i in range(1, len(df_ex)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx <= 0:
                continue
            ts = int(ts_ex[sig_idx])
            idx_tr = int(np.searchsorted(ts_tr, ts, side="right") - 1)
            idx_main = int(np.searchsorted(ts_main, ts, side="right") - 1)
            if idx_tr <= 0 or idx_main <= 0:
                continue

            if trade:
                high = float(df_ex.at[i, "high"])
                low = float(df_ex.at[i, "low"])
                close = float(df_ex.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))

                sl_price = trade["entry_px"] - max(trade["atr_main"] * 1.5, trade["entry_px"] - trade["prev_low"])
                if low <= sl_price:
                    exit_px = sl_price
                    if trade["tp1_done"]:
                        pnl_pct = trade["realized_pct"] + (1.0 - args.tp1_ratio) * (exit_px - trade["entry_px"]) / trade["entry_px"]
                    else:
                        pnl_pct = (exit_px - trade["entry_px"]) / trade["entry_px"]
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
                    if pnl_pct > 0:
                        stats["wins"] += 1
                        sym_stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                        sym_stats["losses"] += 1
                    trade = None
                    cooldown_left = max(cooldown_left, args.cooldown_bars)
                    continue

                if not trade["tp1_done"]:
                    if high >= trade["tp1_level"]:
                        trade["tp1_done"] = True
                        trade["realized_pct"] += args.tp1_ratio * (trade["tp1_level"] - trade["entry_px"]) / trade["entry_px"]
                        trade["peak"] = max(trade["peak"], high)
                else:
                    trade["peak"] = max(trade["peak"], high)
                    trail_stop = trade["peak"] * (1.0 - args.trail_pct)
                    if low <= trail_stop:
                        exit_px = trail_stop
                        pnl_pct = trade["realized_pct"] + (1.0 - args.tp1_ratio) * (exit_px - trade["entry_px"]) / trade["entry_px"]
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
                        if pnl_pct > 0:
                            stats["wins"] += 1
                            sym_stats["wins"] += 1
                        else:
                            stats["losses"] += 1
                            sym_stats["losses"] += 1
                        trade = None
                        cooldown_left = max(cooldown_left, args.cooldown_bars)
                        continue

                if args.time_stop_min > 0:
                    elapsed_min = (ts_ex[i] - trade["entry_ts"]) / 60000.0
                    if elapsed_min >= args.time_stop_min:
                        exit_px = close
                        if trade["tp1_done"]:
                            pnl_pct = trade["realized_pct"] + (1.0 - args.tp1_ratio) * (exit_px - trade["entry_px"]) / trade["entry_px"]
                        else:
                            pnl_pct = (exit_px - trade["entry_px"]) / trade["entry_px"]
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
                        if pnl_pct > 0:
                            stats["wins"] += 1
                            sym_stats["wins"] += 1
                        else:
                            stats["losses"] += 1
                            sym_stats["losses"] += 1
                        trade = None
                        cooldown_left = max(cooldown_left, args.cooldown_bars)
                        continue

                continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            if ts < eval_start_ms:
                continue

            if not _btc_guard(btc_1h, btc_15m, btc_1m, ts):
                continue

            if not (
                ema120_tr.iloc[idx_tr] < df_tr["close"].iloc[idx_tr]
                and ema20_tr.iloc[idx_tr] > ema60_tr.iloc[idx_tr]
                and ema20_main.iloc[idx_main] > ema60_main.iloc[idx_main]
            ):
                continue

            swing_start = max(0, idx_main - args.swing_lookback)
            swing_high = float(df_main["high"].iloc[swing_start: idx_main + 1].max())
            swing_low = float(df_main["low"].iloc[swing_start: idx_main + 1].min())
            fibs = _fib_levels(swing_high, swing_low)
            bbm = float(bb_mid.iloc[idx_main]) if not np.isnan(bb_mid.iloc[idx_main]) else float(ema20_main.iloc[idx_main])
            bbl = float(bb_lower.iloc[idx_main]) if not np.isnan(bb_lower.iloc[idx_main]) else float(bbm)
            main_close = float(df_main["close"].iloc[idx_main])

            if main_close < bbm:
                last_breakdown_idx = idx_main

            if last_breakdown_idx is not None and idx_main - last_breakdown_idx > 3:
                last_breakdown_idx = None

            rsi_main_now = float(rsi_main.iloc[idx_main]) if not np.isnan(rsi_main.iloc[idx_main]) else 0.0
            rsi_main_prev = float(rsi_main.iloc[idx_main - 1]) if idx_main > 0 else rsi_main_now
            if _rsi_fast_drop(rsi_main, idx_main, args.rsi_drop_from, args.rsi_drop_to, args.rsi_drop_bars):
                continue

            cur_ex = df_ex.iloc[sig_idx]
            prev_ex = df_ex.iloc[sig_idx - 1]

            avg_up_vol = _avg_up_volume(df_ex.iloc[:sig_idx + 1], args.lookback_up)
            vol_ratio = (float(cur_ex["volume"]) / avg_up_vol) if avg_up_vol > 0 else 0.0
            obv_now = float(obv_ex.iloc[sig_idx])
            obv_prev = float(obv_ex.iloc[sig_idx - 1]) if sig_idx > 0 else obv_now
            obv_ok = (obv_now - obv_prev) >= args.obv_flat_min

            atr_now = float(atr_ex.iloc[sig_idx]) if not np.isnan(atr_ex.iloc[sig_idx]) else 0.0
            if atr_now > 0:
                range_now = float(cur_ex["high"] - cur_ex["low"])
                if range_now >= atr_now * args.atr_spike_mult:
                    continue

            if float(cur_ex["close"]) < float(cur_ex["open"]):
                vol_ok = vol_ratio <= args.vol_ratio_max
            else:
                vol_ok = True

            # Trigger A: Gap Filler
            gap_filler = float(prev_ex["close"]) < float(ema10_ex.iloc[sig_idx - 1]) and float(cur_ex["close"]) > float(ema10_ex.iloc[sig_idx])

            # Trigger B: Spring Trap
            spring_trap = (
                float(df_main["low"].iloc[idx_main]) < bbm
                and main_close > bbm
                and rsi_main_now >= 48.0
                and rsi_main_now >= rsi_main_prev
            )
            if spring_trap and not _is_bull_body(df_main.iloc[idx_main], args.body_min):
                spring_trap = False

            # Trigger C: Golden Pocket
            gp_hit = any(_in_zone(main_close, lv, 0.002) for lv in fibs) or main_close <= bbl
            golden_pocket = gp_hit and obv_ok and vol_ok

            # Recovery trigger after breakdown
            recovery = last_breakdown_idx is not None and main_close > bbm and obv_ok and vol_ok
            if recovery and not _is_bull_body(df_main.iloc[idx_main], args.body_min):
                recovery = False

            if not (gap_filler or spring_trap or golden_pocket or recovery):
                continue

            if args.use_oi:
                oi_now = _oi_at(oi_rows, int(df_ex.iloc[sig_idx]["ts"]))
                oi_prev = _oi_at(oi_rows, int(df_ex.iloc[sig_idx - 1]["ts"]))
                if oi_now is None or oi_prev is None or oi_now < oi_prev:
                    continue

            entry = df_ex.iloc[i]
            entry_px = float(entry["close"])
            entry_ts = int(ts_ex[i])
            if entry_ts < eval_start_ms:
                continue

            atr_main_now = float(atr_main.iloc[idx_main]) if not np.isnan(atr_main.iloc[idx_main]) else 0.0
            prev_low = float(df_main["low"].iloc[max(0, idx_main - 1)])
            tp1_level = float(df_main["high"].iloc[swing_start: idx_main + 1].max())

            trade = {
                "entry_px": entry_px,
                "entry_ts": entry_ts,
                "hold_bars": 0,
                "mfe": 0.0,
                "mae": 0.0,
                "atr_main": atr_main_now,
                "prev_low": prev_low,
                "tp1_level": tp1_level,
                "tp1_done": False,
                "realized_pct": 0.0,
                "peak": entry_px,
            }
            stats["entries"] += 1
            sym_stats["entries"] += 1

        stats_by_symbol[sym] = sym_stats

    for sym, s in stats_by_symbol.items():
        trades = int(s["trades"])
        wins = int(s["wins"])
        losses = int(s["losses"])
        winrate = (wins / trades * 100.0) if trades > 0 else 0.0
        avg_mfe = s["mfe_sum"] / trades if trades > 0 else 0.0
        avg_mae = s["mae_sum"] / trades if trades > 0 else 0.0
        avg_hold = s["hold_sum"] / trades if trades > 0 else 0.0
        net_sum = s["net_sum"]
        tp_sum = s.get("tp_sum", 0.0)
        sl_sum = s.get("sl_sum", 0.0)
        net_sum_usdt = s.get("net_sum_usdt", 0.0)
        tp_sum_usdt = s.get("tp_sum_usdt", 0.0)
        sl_sum_usdt = s.get("sl_sum_usdt", 0.0)
        line = (
            f"[BACKTEST] {sym} entries={int(s['entries'])} exits={int(s['exits'])} trades={trades} "
            f"wins={wins} losses={losses} winrate={winrate:.2f}% "
            f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} "
            f"tp_sum={tp_sum:.3f} sl_sum={sl_sum:.3f} net_sum={net_sum:.3f} "
            f"tp_sum_usdt={tp_sum_usdt:.3f} sl_sum_usdt={sl_sum_usdt:.3f} net_sum_usdt={net_sum_usdt:.3f}"
        )
        print(line)
        _log(line)

    total_trades = int(stats["trades"])
    total_wins = int(stats["wins"])
    total_losses = int(stats["losses"])
    total_winrate = (total_wins / total_trades * 100.0) if total_trades > 0 else 0.0
    total_avg_mfe = stats["mfe_sum"] / total_trades if total_trades > 0 else 0.0
    total_avg_mae = stats["mae_sum"] / total_trades if total_trades > 0 else 0.0
    total_avg_hold = stats["hold_sum"] / total_trades if total_trades > 0 else 0.0
    total_net_sum = stats["net_sum"]
    total_tp_sum = stats.get("tp_sum", 0.0)
    total_sl_sum = stats.get("sl_sum", 0.0)
    total_net_sum_usdt = stats.get("net_sum_usdt", 0.0)
    total_tp_sum_usdt = stats.get("tp_sum_usdt", 0.0)
    total_sl_sum_usdt = stats.get("sl_sum_usdt", 0.0)
    total_line = (
        f"[BACKTEST] TOTAL entries={int(stats['entries'])} exits={int(stats['exits'])} trades={total_trades} "
        f"wins={total_wins} losses={total_losses} winrate={total_winrate:.2f}% "
        f"avg_mfe={total_avg_mfe:.4f} avg_mae={total_avg_mae:.4f} avg_hold={total_avg_hold:.1f} "
        f"tp_sum={total_tp_sum:.3f} sl_sum={total_sl_sum:.3f} net_sum={total_net_sum:.3f} "
        f"tp_sum_usdt={total_tp_sum_usdt:.3f} sl_sum_usdt={total_sl_sum_usdt:.3f} net_sum_usdt={total_net_sum_usdt:.3f}"
    )
    print(total_line)
    _log(total_line)
    log_warmup_info(_log, warmup_days, warmup_minutes, args.days)


if __name__ == "__main__":
    run_backtest()
