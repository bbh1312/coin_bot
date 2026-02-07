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

from engines.backtest_common import (
    calc_warmup_window,
    load_common_universe,
    log_warmup_info,
    format_backtest_summary,
    print_time_summaries,
    print_trades_by_symbol,
)
from engines.wash_short_suite.engine import (
    WashShortSuiteConfig,
    wash_btc_guard,
    wash_map_idx_by_ts,
    wash_short_entry_signal,
)


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
        "wash_short_suite",
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


def _read_snapshot(path: str) -> List[list]:
    return _read_common_warmup(path)


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
            return filtered
        if snapshot_only:
            return []
    if use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_common_warmup(warmup_path)
        if warmup_rows:
            return [r for r in warmup_rows if start_ms <= int(r[0]) <= end_ms]
        if cache_only:
            return []
    if common_only:
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


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _adx(df: pd.DataFrame, length: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = pd.concat(
        [(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, np.nan)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx.fillna(0.0), plus_di.fillna(0.0), minus_di.fillna(0.0)


def _close_pos(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    return float((row["close"] - row["low"]) / rng)


def _body_ratio(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    return float(abs(row["close"] - row["open"]) / rng)


def _upper_wick_ratio(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    upper = float(row["high"] - max(row["open"], row["close"]))
    return upper / rng


def _is_shooting_star(row: pd.Series) -> bool:
    return _upper_wick_ratio(row) >= 0.6 and _body_ratio(row) <= 0.5


def _is_bear_engulf(prev: pd.Series, cur: pd.Series) -> bool:
    return prev["close"] > prev["open"] and cur["close"] < cur["open"] and cur["open"] >= prev["close"] and cur["close"] <= prev["open"]


def _bb_mid(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


def _fib_levels(high: float, low: float) -> List[float]:
    diff = high - low
    return [high - diff * 0.382, high - diff * 0.5, high - diff * 0.618]


def _in_zone(price: float, level: float, eps: float) -> bool:
    if level <= 0:
        return False
    return abs(price - level) / level <= eps


def _map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def parse_args():
    p = argparse.ArgumentParser(description="Wash Short Suite Backtest Runner")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--universe", type=str, default="top30")
    p.add_argument("--use-confirmed", action="store_true")
    p.add_argument("--use-common-warmup", action="store_true")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--use-live-cache", action="store_true")
    p.add_argument("--cache-only", action="store_true")
    p.add_argument("--common-only", action="store_true")
    p.add_argument("--common-only", action="store_true")
    p.add_argument("--snapshot-dir", type=str, default="")
    p.add_argument("--snapshot-only", action="store_true")
    p.add_argument("--tf-trend", type=str, default="")
    p.add_argument("--tf-main", type=str, default="")
    p.add_argument("--tf-exec", type=str, default="")
    p.add_argument("--ema-fast", type=int, default=None)
    p.add_argument("--ema-mid", type=int, default=None)
    p.add_argument("--ema-slow", type=int, default=None)
    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--sl-pct", type=float, default=None)
    p.add_argument("--adx-min", type=float, default=None)
    p.add_argument("--pullback-eps", type=float, default=None)
    p.add_argument("--fib-eps", type=float, default=None)
    p.add_argument("--rsi-len", type=int, default=None)
    p.add_argument("--rsi-min", type=float, default=None)
    p.add_argument("--rsi-max", type=float, default=None)
    p.add_argument("--rsi-lower-high-delta", type=float, default=None)
    p.add_argument("--vol-reversal-min", type=float, default=None)
    p.add_argument("--time-stop-min", type=int, default=None)
    p.add_argument("--cooldown-bars", type=int, default=None)
    p.add_argument("--log-path", type=str, default="")
    p.add_argument("--no-btc-guard", action="store_true")
    p.add_argument("--btc-ema-len", type=int, default=20)
    p.add_argument("--btc-ema-guard-len", type=int, default=10)
    p.add_argument("--btc-ema-fast", type=int, default=7)
    p.add_argument("--btc-ema-slow", type=int, default=20)
    p.add_argument("--btc-rsi-len", type=int, default=14)
    p.add_argument("--btc-rsi-min", type=float, default=48.0)
    return p.parse_args()


def run_backtest():
    args = parse_args()
    exchange = ccxt.binance(
        {
            "apiKey": os.getenv("BACKTEST_BINANCE_API_KEY", ""),
            "secret": os.getenv("BACKTEST_BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    universe_arg = (args.universe or "").strip().lower()
    use_live_cache = bool(args.use_live_cache)
    use_common = bool(args.use_common_warmup) or use_live_cache or bool(args.common_only)
    cache_only = bool(args.cache_only) or bool(args.common_only)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")

    if not (cache_only and universe_arg in ("common", "common_universe")):
        exchange.load_markets()

    cfg = WashShortSuiteConfig()
    if args.tf_trend:
        cfg.tf_trend = args.tf_trend
    if args.tf_main:
        cfg.tf_main = args.tf_main
    if args.tf_exec:
        cfg.tf_exec = args.tf_exec
    if args.ema_fast is not None:
        cfg.ema_fast = args.ema_fast
    if args.ema_mid is not None:
        cfg.ema_mid = args.ema_mid
    if args.ema_slow is not None:
        cfg.ema_slow = args.ema_slow
    if args.tp_pct is not None:
        cfg.tp_pct = args.tp_pct
    if args.sl_pct is not None:
        cfg.sl_pct = args.sl_pct
    if args.adx_min is not None:
        cfg.adx_min = args.adx_min
    if args.pullback_eps is not None:
        cfg.pullback_eps = args.pullback_eps
    if args.fib_eps is not None:
        cfg.fib_eps = args.fib_eps
    if args.rsi_len is not None:
        cfg.rsi_len = args.rsi_len
    if args.rsi_min is not None:
        cfg.rsi_min = args.rsi_min
    if args.rsi_max is not None:
        cfg.rsi_max = args.rsi_max
    if args.rsi_lower_high_delta is not None:
        cfg.rsi_lower_high_delta = args.rsi_lower_high_delta
    if args.vol_reversal_min is not None:
        cfg.vol_reversal_min = args.vol_reversal_min
    if args.time_stop_min is not None:
        cfg.time_stop_minutes = args.time_stop_min
    if args.cooldown_bars is not None:
        cfg.cooldown_bars = args.cooldown_bars

    days = int(args.days)
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    min_tr = max(cfg.ema_slow + 20, 180)
    min_main = max(cfg.ema_slow + cfg.swing_lookback, 200)
    min_exec = max(cfg.vol_sma_len + 20, 200)
    start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
        days,
        end_ms,
        {
            cfg.tf_trend: min_tr,
            cfg.tf_main: min_main,
            cfg.tf_exec: min_exec,
        },
    )

    universe = load_common_universe(universe_arg, exchange, cache_only)

    base_dir = os.path.join(ROOT_DIR, "logs", "wash_short_suite", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = args.log_path or os.path.join(base_dir, f"backtest_{date_tag}.log")

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    symbol_data: Dict[str, Dict[str, List[list]]] = {}
    for sym in universe:
        rows_trend = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_trend,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=cache_only,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        rows_main = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_main,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=cache_only,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        rows_exec = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_exec,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=cache_only,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        if rows_trend and rows_main and rows_exec:
            symbol_data[sym] = {"trend": rows_trend, "main": rows_main, "exec": rows_exec}

    # BTC safety guard data (1h EMA20 + 15m RSI)
    btc_df_1h = pd.DataFrame()
    btc_df_15m = pd.DataFrame()
    if not args.no_btc_guard:
        btc_rows_1h = _fetch_ohlcv_all(
            exchange,
            "BTC/USDT:USDT",
            "1h",
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=cache_only,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        btc_rows_15m = _fetch_ohlcv_all(
            exchange,
            "BTC/USDT:USDT",
            "15m",
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=cache_only,
            snapshot_dir=args.snapshot_dir,
            snapshot_only=args.snapshot_only,
            common_only=args.common_only,
        )
        btc_df_1h = pd.DataFrame(btc_rows_1h, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True) if btc_rows_1h else pd.DataFrame()
        btc_df_15m = pd.DataFrame(btc_rows_15m, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True) if btc_rows_15m else pd.DataFrame()

    entry_usdt = 10.0
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
    stats_by_hour = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
    stats_by_dow = {d: {"entries": 0, "tp": 0, "sl": 0} for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}
    stats_by_symbol: Dict[str, Dict[str, float]] = {}
    trades_out: List[dict] = []

    for sym, data in symbol_data.items():
        df_tr = pd.DataFrame(data["trend"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_main = pd.DataFrame(data["main"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_ex = pd.DataFrame(data["exec"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if df_tr.empty or df_main.empty or df_ex.empty:
            continue

        df_tr_sig = df_tr.iloc[:-1] if args.use_confirmed else df_tr
        df_main_sig = df_main.iloc[:-1] if args.use_confirmed else df_main
        df_ex_sig = df_ex.iloc[:-1] if args.use_confirmed else df_ex
        ts_tr = df_tr_sig["ts"].astype(int).to_numpy()
        ts_main = df_main_sig["ts"].astype(int).to_numpy()
        ts_ex = df_ex_sig["ts"].astype(int).to_numpy()

        trade: Optional[dict] = None
        cooldown_left = 0
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

        for i in range(1, len(df_ex_sig)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx <= 0:
                continue
            ts = int(ts_ex[sig_idx])
            idx_tr = wash_map_idx_by_ts(ts_tr, ts)
            idx_main = wash_map_idx_by_ts(ts_main, ts)
            if idx_tr <= 0 or idx_main <= 0:
                continue

            # BTC safety guard (block shorts when BTC 15m close > EMA10 or EMA7>EMA20)
            if not args.no_btc_guard and btc_df_15m is not None and not btc_df_15m.empty:
                if wash_btc_guard(
                    btc_df_15m,
                    ts,
                    int(args.btc_ema_guard_len),
                    int(args.btc_ema_fast),
                    int(args.btc_ema_slow),
                ):
                    continue

            if trade:
                high = float(df_ex.at[i, "high"])
                low = float(df_ex.at[i, "low"])
                close = float(df_ex.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))

                sl_price = float(trade.get("sl_price", trade["entry_px"] * (1.0 + cfg.sl_pct)))
                tp_price = float(trade.get("tp_price", trade["entry_px"] * (1.0 - cfg.tp_pct)))

                exit_reason = None
                exit_px = None

                if high >= sl_price:
                    exit_reason = "SL"
                    exit_px = sl_price
                elif low <= tp_price:
                    exit_reason = "TP"
                    exit_px = tp_price
                else:
                    if cfg.time_stop_minutes > 0:
                        elapsed_min = (ts_ex[i] - trade["entry_ts"]) / 60000.0
                        if elapsed_min >= cfg.time_stop_minutes:
                            exit_reason = "TIME"
                            exit_px = close

                if exit_reason:
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    if exit_reason == "TP":
                        stats["tp_sum"] += pnl_pct
                        stats["tp_sum_usdt"] += pnl_pct * entry_usdt
                    elif exit_reason == "SL":
                        stats["sl_sum"] += pnl_pct
                        stats["sl_sum_usdt"] += pnl_pct * entry_usdt
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += pnl_pct
                    sym_stats["net_sum_usdt"] += pnl_pct * entry_usdt
                    if exit_reason == "TP":
                        sym_stats["tp_sum"] += pnl_pct
                        sym_stats["tp_sum_usdt"] += pnl_pct * entry_usdt
                    elif exit_reason == "SL":
                        sym_stats["sl_sum"] += pnl_pct
                        sym_stats["sl_sum_usdt"] += pnl_pct * entry_usdt
                    entry_hour = trade.get("entry_hour")
                    entry_dow = trade.get("entry_dow")
                    if isinstance(entry_hour, int) and entry_hour in stats_by_hour:
                        if exit_reason == "TP":
                            stats_by_hour[entry_hour]["tp"] += 1
                        elif exit_reason == "SL":
                            stats_by_hour[entry_hour]["sl"] += 1
                    if isinstance(entry_dow, str) and entry_dow in stats_by_dow:
                        if exit_reason == "TP":
                            stats_by_dow[entry_dow]["tp"] += 1
                        elif exit_reason == "SL":
                            stats_by_dow[entry_dow]["sl"] += 1
                    if pnl_pct > 0:
                        stats["wins"] += 1
                        sym_stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                        sym_stats["losses"] += 1
                    trades_out.append(
                        {
                            "symbol": sym,
                            "entry_ts": int(trade["entry_ts"]),
                            "exit_ts": int(ts_ex[i]),
                            "pnl_pct": pnl_pct * 100.0,
                            "result": "WIN" if pnl_pct > 0 else "LOSS",
                            "reason": exit_reason,
                        }
                    )
                    trade = None
                    cooldown_left = max(cooldown_left, cfg.cooldown_bars)
                continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            entry_info, reason = wash_short_entry_signal(
                df_tr_sig,
                df_main_sig,
                df_ex,
                df_ex_sig,
                sig_idx,
                idx_tr,
                idx_main,
                cfg,
                entry_offset=1 if args.use_confirmed else 0,
            )
            if not entry_info:
                continue

            entry_px = float(entry_info["entry_px"])
            entry_ts = int(entry_info["entry_ts"])
            if entry_ts < eval_start_ms:
                continue
            entry_hour, entry_dow = _entry_kst_info(entry_ts)
            trade = {
                "entry_px": entry_px,
                "entry_ts": entry_ts,
                "entry_hour": entry_hour,
                "entry_dow": entry_dow,
                "sl_price": float(entry_info["sl_price"]),
                "tp_price": float(entry_info["tp_price"]),
                "hold_bars": 0,
                "mfe": 0.0,
                "mae": 0.0,
            }
            stats["entries"] += 1
            sym_stats["entries"] += 1
            if entry_hour in stats_by_hour:
                stats_by_hour[entry_hour]["entries"] += 1
            if entry_dow in stats_by_dow:
                stats_by_dow[entry_dow]["entries"] += 1

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
        line = format_backtest_summary(sym, s)
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
    print_trades_by_symbol(trades_out, _log)
    print_time_summaries(trades_out, _log)
    total_line = format_backtest_summary(None, stats)
    print(total_line)
    _log(total_line)
    log_warmup_info(_log, warmup_days, warmup_minutes, days)


if __name__ == "__main__":
    run_backtest()
