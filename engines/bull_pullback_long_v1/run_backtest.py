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

from engines.universe import build_universe_from_tickers
from engines.bull_pullback_long_v1.engine import BullPullbackLongConfig


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
        "bull_pullback_long_v1",
        "ohlcv_cache",
        f"{safe}_{timeframe}_{start_ms}_{end_ms}.csv",
    )


def _fmt_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M")

def _parse_utc_dt(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _parse_kst_dt(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    kst = timezone(timedelta(hours=9))
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=kst).astimezone(timezone.utc)
        except Exception:
            continue
    return None


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


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    gain = up.ewm(alpha=1 / length, adjust=False).mean()
    loss = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _bbands(series: pd.Series, length: int, std_mult: float) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = series.rolling(length).mean()
    std = series.rolling(length).std()
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    return mid, upper, lower


def _fib_levels_up(swing_high: float, swing_low: float) -> dict:
    if swing_high <= swing_low:
        return {}
    diff = swing_high - swing_low
    return {
        "0.382": swing_high - diff * 0.382,
        "0.5": swing_high - diff * 0.5,
        "0.618": swing_high - diff * 0.618,
    }


def _map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    idx = int(np.searchsorted(ts_arr, ts, side="right") - 1)
    return idx


def _load_common_universe(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f.readlines() if line.strip()]


def _build_gainers_universe(exchange: ccxt.Exchange, top_n: int) -> List[str]:
    tickers = exchange.fetch_tickers()
    candidates = []
    for sym, t in (tickers or {}).items():
        if not isinstance(sym, str) or "/" not in sym:
            continue
        pct = t.get("percentage")
        qv = t.get("quoteVolume")
        if pct is None or qv is None:
            continue
        try:
            pct = float(pct)
            qv = float(qv)
        except Exception:
            continue
        if qv < 8_000_000.0:
            continue
        if pct <= 0:
            continue
        candidates.append((sym, pct))
    candidates.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in candidates[:top_n]]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", type=str, default="")
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--start", type=str, default="")
    p.add_argument("--end", type=str, default="")
    p.add_argument("--start-kst", type=str, default="")
    p.add_argument("--end-kst", type=str, default="")
    p.add_argument("--use-confirmed", action="store_true")
    p.add_argument("--use-live-cache", action="store_true")
    p.add_argument("--use-common-warmup", action="store_true")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--cache-only", action="store_true")
    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--sl-pct", type=float, default=None)
    p.add_argument("--cooldown-bars", type=int, default=None)
    p.add_argument("--entry-usdt", type=float, default=10.0)
    p.add_argument("--no-btc-filter", action="store_true")
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
    use_common = bool(args.use_common_warmup) or use_live_cache
    cache_only = bool(args.cache_only)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")

    if not (cache_only and universe_arg in ("common", "common_universe")):
        exchange.load_markets()

    cfg = BullPullbackLongConfig()
    if args.cooldown_bars is not None:
        cfg.cooldown_bars = args.cooldown_bars
    if args.no_btc_filter:
        cfg.btc_filter = False

    start_dt = None
    end_dt = None
    if args.start_kst or args.end_kst:
        start_dt = _parse_kst_dt(args.start_kst)
        end_dt = _parse_kst_dt(args.end_kst)
    elif args.start or args.end:
        start_dt = _parse_utc_dt(args.start)
        end_dt = _parse_utc_dt(args.end)

    if end_dt is None:
        end_dt = datetime.now(timezone.utc)
    if start_dt is None:
        start_dt = end_dt - timedelta(days=args.days)

    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    universe: List[str] = []
    if args.symbols:
        universe = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif universe_arg in ("common", "common_universe"):
        universe = _load_common_universe(os.path.join(ROOT_DIR, "logs", "common_universe", "latest.txt"))
    elif universe_arg in ("gainers", "gainer", "up"):
        universe = _build_gainers_universe(exchange, args.max_symbols if args.max_symbols and args.max_symbols > 0 else 50)
    else:
        tickers = exchange.fetch_tickers()
        universe = build_universe_from_tickers(tickers, min_quote_volume_usdt=8_000_000.0, top_n=50)
    if args.max_symbols and args.max_symbols > 0:
        universe = universe[: args.max_symbols]

    stats = {"entries": 0, "exits": 0, "trades": 0, "wins": 0, "losses": 0, "mfe_sum": 0.0, "mae_sum": 0.0, "hold_sum": 0.0, "net_sum": 0.0}
    stats_by_symbol: Dict[str, dict] = {}
    gate_stats = {"trend_fail": 0, "pullback_fail": 0, "exec_fail": 0, "btc_fail": 0}

    # prefetch BTC for filters
    btc_1h = btc_15m = btc_1m = btc_5m = None
    if cfg.btc_filter and cfg.btc_symbol:
        rows_1h = _fetch_ohlcv_all(exchange, cfg.btc_symbol, cfg.tf_trend, start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        rows_15m = _fetch_ohlcv_all(exchange, cfg.btc_symbol, "15m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        rows_1m = _fetch_ohlcv_all(exchange, cfg.btc_symbol, "1m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        rows_5m = _fetch_ohlcv_all(exchange, cfg.btc_symbol, "5m", start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        if rows_1h:
            btc_1h = pd.DataFrame(rows_1h, columns=["ts", "open", "high", "low", "close", "volume"])
        if rows_15m:
            btc_15m = pd.DataFrame(rows_15m, columns=["ts", "open", "high", "low", "close", "volume"])
        if rows_1m:
            btc_1m = pd.DataFrame(rows_1m, columns=["ts", "open", "high", "low", "close", "volume"])
        if rows_5m:
            btc_5m = pd.DataFrame(rows_5m, columns=["ts", "open", "high", "low", "close", "volume"])

    for sym in universe:
        rows_tr = _fetch_ohlcv_all(exchange, sym, cfg.tf_trend, start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        rows_main = _fetch_ohlcv_all(exchange, sym, cfg.tf_main, start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        rows_ex = _fetch_ohlcv_all(exchange, sym, cfg.tf_exec, start_ms, end_ms, use_common_warmup=use_common, common_warmup_dir=common_dir, cache_only=cache_only)
        if not rows_tr or not rows_main or not rows_ex:
            continue

        df_tr = pd.DataFrame(rows_tr, columns=["ts", "open", "high", "low", "close", "volume"])
        df_main = pd.DataFrame(rows_main, columns=["ts", "open", "high", "low", "close", "volume"])
        df_ex = pd.DataFrame(rows_ex, columns=["ts", "open", "high", "low", "close", "volume"])

        if len(df_tr) < cfg.ema_trend_slow + 5 or len(df_main) < cfg.fib_lookback + 5 or len(df_ex) < cfg.rsi_len + 5:
            continue

        ema20_tr = _ema(df_tr["close"].astype(float), cfg.ema_trend_fast)
        ema60_tr = _ema(df_tr["close"].astype(float), cfg.ema_trend_mid)
        ema120_tr = _ema(df_tr["close"].astype(float), cfg.ema_trend_slow)

        rsi_ex = _rsi(df_ex["close"].astype(float), cfg.rsi_len)
        ema10_ex = _ema(df_ex["close"].astype(float), cfg.ema_exec_fast)
        ema20_ex = _ema(df_ex["close"].astype(float), cfg.ema_exec_mid)

        bb_mid, _, bb_lower = _bbands(df_main["close"].astype(float), cfg.bb_len, cfg.bb_std)
        atr_main = _atr(df_main, cfg.atr_len)
        vol_sma_main = df_main["volume"].astype(float).rolling(cfg.vol_sma_len).mean()

        ts_tr = df_tr["ts"].astype(int).to_numpy()
        ts_main = df_main["ts"].astype(int).to_numpy()
        ts_ex = df_ex["ts"].astype(int).to_numpy()

        trade = None
        cooldown_left = 0
        sym_stats = {"entries": 0, "exits": 0, "trades": 0, "wins": 0, "losses": 0, "mfe_sum": 0.0, "mae_sum": 0.0, "hold_sum": 0.0, "net_sum": 0.0}

        for i in range(1, len(df_ex)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx <= 0:
                continue
            ts = int(ts_ex[sig_idx])
            if ts < start_ms:
                continue
            idx_tr = _map_idx_by_ts(ts_tr, ts)
            idx_main = _map_idx_by_ts(ts_main, ts)
            if idx_tr <= 0 or idx_main <= 0:
                continue

            if trade:
                high = float(df_ex.at[i, "high"])
                low = float(df_ex.at[i, "low"])
                close = float(df_ex.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))

                sl_price = trade["sl_price"]
                tp_price = trade["tp_price"]

                exit_reason = None
                exit_px = None
                # SL priority
                if low <= sl_price:
                    exit_reason = "SL"
                    exit_px = sl_price
                elif high >= tp_price:
                    exit_reason = "TP"
                    exit_px = tp_price

                if exit_reason:
                    pnl_pct = (exit_px - trade["entry_px"]) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += pnl_pct
                    if pnl_pct > 0:
                        stats["wins"] += 1
                        sym_stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                        sym_stats["losses"] += 1
                    trade = None
                    cooldown_left = cfg.cooldown_bars
                continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            # BTC filter
            if cfg.btc_filter:
                if btc_1h is None or btc_15m is None or btc_1m is None or btc_5m is None:
                    gate_stats["btc_fail"] += 1
                    continue
                btc_1h_sig = btc_1h.iloc[:-1] if len(btc_1h) > 1 else btc_1h
                btc_15m_sig = btc_15m.iloc[:-1] if len(btc_15m) > 1 else btc_15m
                btc_1m_sig = btc_1m.iloc[:-1] if len(btc_1m) > 1 else btc_1m
                btc_5m_sig = btc_5m.iloc[:-1] if len(btc_5m) > 1 else btc_5m
                if len(btc_1h_sig) < cfg.ema_trend_slow + 5 or len(btc_15m_sig) < cfg.btc_rsi_len + 5 or len(btc_1m_sig) < 2 or len(btc_5m_sig) < cfg.btc_atr_lookback + 5:
                    gate_stats["btc_fail"] += 1
                    continue
                btc_ema20 = _ema(btc_1h_sig["close"].astype(float), cfg.ema_trend_fast)
                btc_ema60 = _ema(btc_1h_sig["close"].astype(float), cfg.ema_trend_mid)
                btc_idx = len(btc_1h_sig) - 1
                if float(btc_1h_sig.iloc[btc_idx]["close"]) <= float(btc_ema60.iloc[btc_idx]):
                    gate_stats["btc_fail"] += 1
                    continue
                if float(btc_ema20.iloc[btc_idx]) < float(btc_ema60.iloc[btc_idx]):
                    gate_stats["btc_fail"] += 1
                    continue
                btc_rsi = _rsi(btc_15m_sig["close"].astype(float), cfg.btc_rsi_len)
                if float(btc_rsi.iloc[-1]) < float(cfg.btc_rsi_min):
                    gate_stats["btc_fail"] += 1
                    continue
                prev_close = float(btc_1m_sig.iloc[-2]["close"]) if len(btc_1m_sig) >= 2 else None
                cur_close = float(btc_1m_sig.iloc[-1]["close"]) if len(btc_1m_sig) >= 1 else None
                if prev_close and cur_close:
                    chg_1m = (cur_close / prev_close - 1.0) * 100.0
                    if chg_1m <= float(cfg.btc_drop_1m_pct):
                        gate_stats["btc_fail"] += 1
                        continue
                atr_5m = _atr(btc_5m_sig, cfg.btc_atr_len)
                atr_now = float(atr_5m.iloc[-1]) if not np.isnan(atr_5m.iloc[-1]) else 0.0
                atr_avg = float(atr_5m.iloc[-cfg.btc_atr_lookback:].mean()) if len(atr_5m) >= cfg.btc_atr_lookback else float(atr_5m.mean())
                if atr_avg > 0 and (atr_now / atr_avg) > float(cfg.btc_atr_mult_max):
                    gate_stats["btc_fail"] += 1
                    continue

            slope_len = max(1, int(cfg.ema_trend_slope_len))
            if idx_tr - slope_len <= 0:
                gate_stats["trend_fail"] += 1
                continue
            trend_ok = (
                float(df_tr.iloc[idx_tr]["close"]) > float(ema120_tr.iloc[idx_tr])
                and float(ema20_tr.iloc[idx_tr]) > float(ema60_tr.iloc[idx_tr])
                and (ema20_tr.iloc[idx_tr] - ema20_tr.iloc[idx_tr - slope_len]) > 0
            )
            if not trend_ok:
                gate_stats["trend_fail"] += 1
                continue

            # main frame pullback
            close_main = float(df_main.iloc[idx_main]["close"])
            open_main = float(df_main.iloc[idx_main]["open"])
            vol_main = float(df_main.iloc[idx_main]["volume"])
            vol_avg = float(vol_sma_main.iloc[idx_main]) if not np.isnan(vol_sma_main.iloc[idx_main]) else 0.0
            if open_main > close_main and vol_avg > 0:
                if (vol_main / vol_avg) > cfg.vol_pullback_max:
                    gate_stats["pullback_fail"] += 1
                    continue

            swing_start = max(0, idx_main - cfg.fib_lookback + 1)
            swing_high = float(df_main["high"].iloc[swing_start: idx_main + 1].max())
            swing_low = float(df_main["low"].iloc[swing_start: idx_main + 1].min())
            fibs = _fib_levels_up(swing_high, swing_low)
            fib_382 = fibs.get("0.382")
            fib_50 = fibs.get("0.5")
            fib_618 = fibs.get("0.618")

            bbm = float(bb_mid.iloc[idx_main]) if not np.isnan(bb_mid.iloc[idx_main]) else close_main
            bbl = float(bb_lower.iloc[idx_main]) if not np.isnan(bb_lower.iloc[idx_main]) else close_main
            prev_close_main = float(df_main.iloc[idx_main - 1]["close"]) if idx_main > 0 else close_main
            fakeout = prev_close_main < bbm and close_main > bbm

            fib_norm_ok = False
            if isinstance(fib_382, (int, float)) and isinstance(fib_50, (int, float)):
                low_zone = min(fib_382, fib_50) * (1.0 - cfg.fib_eps)
                high_zone = max(fib_382, fib_50) * (1.0 + cfg.fib_eps)
                fib_norm_ok = low_zone <= close_main <= high_zone

            deep_zone_ok = False
            if isinstance(fib_618, (int, float)):
                low_zone = float(fib_618) * (1.0 - cfg.fib_eps)
                high_zone = float(fib_618) * (1.0 + cfg.fib_eps)
                deep_zone_ok = low_zone <= close_main <= high_zone
            if close_main <= bbl:
                deep_zone_ok = True

            pb_low = float(df_main["low"].iloc[max(0, idx_main - 1): idx_main + 1].min())
            pb_high = float(df_main["high"].iloc[max(0, idx_main - 1): idx_main + 1].max())
            pullback_gain = (pb_high - pb_low) / pb_low if pb_low > 0 else 0.0
            if pullback_gain < cfg.pullback_gain_min:
                gate_stats["pullback_fail"] += 1
                continue

            # exec frame
            ex = df_ex.iloc[sig_idx]
            ex_open = float(ex["open"])
            ex_close = float(ex["close"])
            ex_high = float(ex["high"])
            ex_low = float(ex["low"])
            ex_range = max(ex_high - ex_low, 1e-9)
            lower_wick = min(ex_open, ex_close) - ex_low
            lower_wick_ratio = lower_wick / ex_range

            ema_touch = False
            if not np.isnan(ema10_ex.iloc[sig_idx]) and ex_low <= float(ema10_ex.iloc[sig_idx]) * (1.0 + cfg.exec_ema_touch_eps):
                ema_touch = True
            if not np.isnan(ema20_ex.iloc[sig_idx]) and ex_low <= float(ema20_ex.iloc[sig_idx]) * (1.0 + cfg.exec_ema_touch_eps):
                ema_touch = True

            rsi_now = float(rsi_ex.iloc[sig_idx])
            rsi_prev = float(rsi_ex.iloc[sig_idx - 1]) if sig_idx > 0 else rsi_now

            aggressive_ok = (
                ema_touch
                and lower_wick_ratio >= cfg.exec_wick_min
                and cfg.rsi_aggr_min <= rsi_now <= cfg.rsi_aggr_max
                and rsi_now >= rsi_prev
            )
            normal_ok = fakeout and fib_norm_ok
            deep_ok = deep_zone_ok and cfg.rsi_deep_min <= rsi_now <= cfg.rsi_deep_max and ex_close > ex_open

            entry_type = None
            support_price = None
            if aggressive_ok:
                entry_type = "aggressive"
                support_price = float(ema20_ex.iloc[sig_idx]) if not np.isnan(ema20_ex.iloc[sig_idx]) else ex_low
            elif normal_ok:
                entry_type = "normal"
                support_price = float(bbm)
            elif deep_ok:
                entry_type = "deep"
                support_price = float(fib_618) if isinstance(fib_618, (int, float)) else float(bbl)

            if not entry_type:
                gate_stats["exec_fail"] += 1
                continue

            # entry on next bar
            entry = df_ex.iloc[i]
            entry_px = float(entry["close"])
            entry_ts = int(ts_ex[i])
            if entry_ts < start_ms:
                continue

            atr_px = float(atr_main.iloc[idx_main]) if not np.isnan(atr_main.iloc[idx_main]) else 0.0
            if args.sl_pct is not None:
                sl_price = entry_px * (1.0 - args.sl_pct)
            else:
                support = support_price if isinstance(support_price, (int, float)) else entry_px
                sl_price = support - (atr_px * cfg.sl_atr_mult if atr_px > 0 else entry_px * 0.01)
            if args.tp_pct is not None:
                tp_price = entry_px * (1.0 + args.tp_pct)
            else:
                tp_pct = (float(swing_high) - entry_px) / entry_px if swing_high > entry_px else cfg.tp_fallback_pct
                tp_price = entry_px * (1.0 + tp_pct)

            trade = {
                "entry_px": entry_px,
                "entry_ts": entry_ts,
                "hold_bars": 0,
                "mfe": 0.0,
                "mae": 0.0,
                "sl_price": sl_price,
                "tp_price": tp_price,
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
        line = (
            f"[BACKTEST] {sym} entries={int(s['entries'])} exits={int(s['exits'])} trades={trades} "
            f"wins={wins} losses={losses} winrate={winrate:.2f}% "
            f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} net_sum={net_sum:.3f}"
        )
        print(line)

    total_trades = int(stats["trades"])
    total_wins = int(stats["wins"])
    total_losses = int(stats["losses"])
    total_winrate = (total_wins / total_trades * 100.0) if total_trades > 0 else 0.0
    total_avg_mfe = stats["mfe_sum"] / total_trades if total_trades > 0 else 0.0
    total_avg_mae = stats["mae_sum"] / total_trades if total_trades > 0 else 0.0
    total_avg_hold = stats["hold_sum"] / total_trades if total_trades > 0 else 0.0
    total_net_sum = stats["net_sum"]
    print(
        f"[BACKTEST] TOTAL entries={stats['entries']} exits={stats['exits']} trades={total_trades} "
        f"wins={total_wins} losses={total_losses} winrate={total_winrate:.2f}% "
        f"avg_mfe={total_avg_mfe:.4f} avg_mae={total_avg_mae:.4f} avg_hold={total_avg_hold:.1f} net_sum={total_net_sum:.3f}"
    )
    print(f"[BACKTEST] GATES trend_fail={gate_stats['trend_fail']} pullback_fail={gate_stats['pullback_fail']} exec_fail={gate_stats['exec_fail']} btc_fail={gate_stats['btc_fail']}")


if __name__ == "__main__":
    run_backtest()
