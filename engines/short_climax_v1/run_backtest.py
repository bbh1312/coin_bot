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

from engines.backtest_common import calc_warmup_window, load_common_universe
from engines.short_climax_v1.engine import ShortClimaxConfig


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
        "short_climax_v1",
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
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def _fib_618(high: float, low: float) -> float:
    diff = high - low
    return high - diff * 0.618


def parse_args():
    p = argparse.ArgumentParser(description="Short Climax Backtest Runner")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--use-confirmed", action="store_true")
    p.add_argument("--use-common-warmup", action="store_true")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--use-live-cache", action="store_true")
    p.add_argument("--cache-only", action="store_true")
    p.add_argument("--tf-exec", type=str, default="1m")
    p.add_argument("--tf-mtf", type=str, default="5m")
    p.add_argument("--tf-htf", type=str, default="1h")
    p.add_argument("--ema-ref-tf", type=str, default="15m")
    p.add_argument("--ema-ref-len", type=int, default=120)
    p.add_argument("--ema-ref-mult", type=float, default=1.08)
    p.add_argument("--vol-mult", type=float, default=2.0)
    p.add_argument("--runup-24h-min", type=float, default=0.50)
    p.add_argument("--runup-7d-min", type=float, default=3.0)
    p.add_argument("--runup-1h-min", type=float, default=0.15)
    p.add_argument("--sfp-lookback", type=int, default=5)
    p.add_argument("--vol-div-threshold", type=float, default=0.7)
    p.add_argument("--vol-spike-mult", type=float, default=2.0)
    p.add_argument("--vol-spike-ma", type=int, default=20)
    p.add_argument("--funding-limit", type=float, default=0.001)
    p.add_argument("--assume-funding-rate", type=float, default=0.0)
    p.add_argument("--assume-oi-desc", action="store_true")
    p.add_argument("--sl-multiplier", type=float, default=1.007)
    p.add_argument("--score-threshold", type=int, default=50)
    p.add_argument("--tp-mode", type=str, default="trailing")
    p.add_argument("--tp-pct", type=float, default=0.03)
    p.add_argument("--tp1-pct", type=float, default=0.02)
    p.add_argument("--max-hold-bars", type=int, default=240)
    p.add_argument("--cooldown-bars", type=int, default=30)
    p.add_argument("--log-path", type=str, default="")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = ShortClimaxConfig(
        tf_exec=args.tf_exec,
        tf_mtf=args.tf_mtf,
        tf_htf=args.tf_htf,
        ema_ref_tf=args.ema_ref_tf,
        ema_ref_len=args.ema_ref_len,
        ema_ref_mult=args.ema_ref_mult,
        vol_mult=args.vol_mult,
        runup_24h_min=args.runup_24h_min,
        runup_7d_min=args.runup_7d_min,
        runup_1h_min=args.runup_1h_min,
        sfp_lookback=args.sfp_lookback,
        vol_div_threshold=args.vol_div_threshold,
        vol_spike_mult=args.vol_spike_mult,
        vol_spike_ma=args.vol_spike_ma,
        funding_limit=args.funding_limit,
        sl_multiplier=args.sl_multiplier,
        score_threshold=args.score_threshold,
        tp1_pct=args.tp1_pct,
        tp_mode=args.tp_mode,
        tp_pct=args.tp_pct,
    )

    exchange = ccxt.binance({"options": {"defaultType": "future"}})
    use_live_cache = bool(args.use_live_cache)
    use_common = bool(args.use_common_warmup) or use_live_cache
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    days = int(args.days)
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    min_exec = max(cfg.sfp_lookback + 5, 200)
    min_mtf = max(60, 120)
    min_htf = max(cfg.ema_ref_len + 10, 220)

    start_ms, eval_start_ms, _, _ = calc_warmup_window(
        days,
        end_ms,
        {
            cfg.tf_exec: min_exec,
            cfg.tf_mtf: min_mtf,
            cfg.tf_htf: min_htf,
        },
    )

    universe = load_common_universe(args.universe, exchange, args.cache_only)

    base_dir = os.path.join(ROOT_DIR, "logs", "short_climax_v1", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = args.log_path or os.path.join(base_dir, f"backtest_{date_tag}.log")

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    symbol_data: Dict[str, Dict[str, List[list]]] = {}
    for sym in universe:
        rows_exec = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_exec,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        rows_mtf = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_mtf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        rows_htf = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_htf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        rows_ref = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.ema_ref_tf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        if rows_exec and rows_mtf and rows_htf and rows_ref:
            symbol_data[sym] = {"exec": rows_exec, "mtf": rows_mtf, "htf": rows_htf, "ref": rows_ref}

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
    }

    for sym, data in symbol_data.items():
        df_ex = pd.DataFrame(data["exec"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_mtf = pd.DataFrame(data["mtf"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_htf = pd.DataFrame(data["htf"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_ref = pd.DataFrame(data["ref"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if df_ex.empty or df_mtf.empty or df_htf.empty or df_ref.empty:
            continue

        ts_ex = df_ex["ts"].astype(int).to_numpy()
        ts_mtf = df_mtf["ts"].astype(int).to_numpy()
        ts_htf = df_htf["ts"].astype(int).to_numpy()
        ts_ref = df_ref["ts"].astype(int).to_numpy()

        ema_ref = _ema(df_ref["close"].astype(float), cfg.ema_ref_len)

        vol_avg_len = 1440  # 24h of 1m
        vol_avg = df_ex["volume"].astype(float).rolling(vol_avg_len).mean()

        trade = None
        cooldown_left = 0

        for i in range(1, len(df_ex)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx <= 0:
                continue
            ts = int(ts_ex[sig_idx])
            idx_mtf = _map_idx_by_ts(ts_mtf, ts)
            idx_htf = _map_idx_by_ts(ts_htf, ts)
            idx_ref = _map_idx_by_ts(ts_ref, ts)
            if idx_mtf <= 0 or idx_htf <= 0 or idx_ref <= 0:
                continue

            if trade:
                high = float(df_ex.at[i, "high"])
                low = float(df_ex.at[i, "low"])
                close = float(df_ex.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))
                exit_reason = None
                exit_px = None
                sl_price = float(trade.get("sl_price") or (trade["entry_px"] * cfg.sl_multiplier))
                tp_price = trade.get("tp_price")
                remaining = float(trade.get("remaining", 1.0))
                realized = float(trade.get("realized", 0.0))
                be_done = bool(trade.get("be_done"))
                ema20 = trade.get("ema20")
                ema20_val = None
                if ema20 is not None and i < len(ema20):
                    ema20_val = float(ema20.iloc[i]) if not np.isnan(ema20.iloc[i]) else None

                if cfg.tp_mode == "trailing" and not be_done:
                    tp1_price = trade["entry_px"] * (1.0 - cfg.tp1_pct)
                    if low <= tp1_price:
                        realized += (trade["entry_px"] - tp1_price) / trade["entry_px"] * 0.5
                        remaining = max(0.0, remaining - 0.5)
                        be_done = True
                        sl_price = trade["entry_px"]

                if high >= sl_price:
                    exit_reason = "SL"
                    exit_px = sl_price
                elif cfg.tp_mode == "trailing" and be_done and ema20_val is not None and close >= ema20_val:
                    exit_reason = "TP2"
                    exit_px = close
                elif cfg.tp_mode != "trailing" and tp_price is not None and low <= tp_price:
                    exit_reason = "TP"
                    exit_px = tp_price
                elif trade["hold_bars"] >= args.max_hold_bars:
                    exit_reason = "TIME"
                    exit_px = close
                if exit_reason:
                    pnl_pct = realized + (trade["entry_px"] - exit_px) / trade["entry_px"] * remaining
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    if pnl_pct > 0:
                        stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                    trade = None
                    cooldown_left = max(cooldown_left, args.cooldown_bars)
                if trade is not None:
                    trade["remaining"] = remaining
                    trade["realized"] = realized
                    trade["be_done"] = be_done
                    trade["sl_price"] = sl_price
                continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            # Scanning filters
            close_now = float(df_ex.at[sig_idx, "close"])
            if sig_idx < vol_avg_len:
                continue
            vol_now = float(df_ex.at[sig_idx, "volume"])
            vol_mean = float(vol_avg.iloc[sig_idx]) if not np.isnan(vol_avg.iloc[sig_idx]) else 0.0
            if vol_mean <= 0 or vol_now < vol_mean * cfg.vol_mult:
                continue

            # runup conditions
            runup_24h = (close_now / float(df_ex.at[sig_idx - 1440, "close"]) - 1.0) if sig_idx >= 1440 else 0.0
            runup_7d = (close_now / float(df_ex.at[sig_idx - 10080, "close"]) - 1.0) if sig_idx >= 10080 else 0.0
            runup_1h = (close_now / float(df_ex.at[sig_idx - 60, "close"]) - 1.0) if sig_idx >= 60 else 0.0
            if not (
                runup_24h >= cfg.runup_24h_min
                or runup_7d >= cfg.runup_7d_min
                or runup_1h >= cfg.runup_1h_min
            ):
                continue

            ref_ema = float(ema_ref.iloc[idx_ref])
            if close_now <= ref_ema * cfg.ema_ref_mult:
                continue

            # Entry logic (advanced climax module)
            score = 0
            cur = df_ex.iloc[sig_idx]
            if sig_idx >= cfg.sfp_lookback:
                recent_high = float(df_ex["high"].iloc[sig_idx - cfg.sfp_lookback : sig_idx].max())
                sfp_trigger = (float(cur["high"]) > recent_high) and (float(cur["close"]) < recent_high)
                if sfp_trigger:
                    score += 30
            prev_vol_ma = df_ex["volume"].astype(float).rolling(cfg.vol_spike_ma).mean()
            vol_spike = False
            if sig_idx >= cfg.vol_spike_ma:
                vma = float(prev_vol_ma.iloc[sig_idx]) if not np.isnan(prev_vol_ma.iloc[sig_idx]) else 0.0
                vol_spike = vma > 0 and float(cur["volume"]) > vma * cfg.vol_spike_mult
            upper_wick = float(cur["high"] - max(cur["open"], cur["close"]))
            body = float(abs(cur["close"] - cur["open"]))
            is_upper_wick = upper_wick > body
            if is_upper_wick and vol_spike:
                score += 40

            # Second mountain (5m)
            first_peak = None
            if idx_mtf >= 10:
                window = df_mtf.iloc[max(0, idx_mtf - 5): idx_mtf - 1]
                if not window.empty:
                    first_peak = float(window["high"].max())
                    first_peak_idx = window["high"].idxmax()
                    first_peak_vol = float(df_mtf.at[first_peak_idx, "volume"]) if first_peak_idx is not None else 0.0
                    current_peak = float(df_mtf.at[idx_mtf, "high"])
                    current_vol = float(df_mtf.at[idx_mtf, "volume"])
                    is_second_mountain = (current_peak < first_peak * 1.01) and (current_vol < first_peak_vol * cfg.vol_div_threshold)
                    if is_second_mountain:
                        score += 40

            # OI + funding (assumed in backtest)
            funding_rate = float(args.assume_funding_rate)
            oi_descending = bool(args.assume_oi_desc)
            if oi_descending and funding_rate >= cfg.funding_limit:
                score += 20

            # Bearish engulfing (30점)
            prev = df_ex.iloc[sig_idx - 1]
            engulf = (prev["close"] > prev["open"]) and (cur["close"] < cur["open"]) and (cur["close"] < prev["low"])
            if engulf:
                score += 30

            if score < cfg.score_threshold:
                continue

            entry = df_ex.iloc[i]
            entry_px = float(entry["close"])
            entry_ts = int(entry["ts"])
            if entry_ts < eval_start_ms:
                continue
            sl_base = float(cur["high"])
            if isinstance(first_peak, (int, float)) and first_peak > sl_base:
                sl_base = float(first_peak)
            sl_price = min(sl_base * cfg.sl_multiplier, entry_px * 1.015)

            tp_price = None
            if cfg.tp_mode == "pct":
                tp_price = entry_px * (1.0 - cfg.tp_pct)
            else:
                ema_len = 20
                if cfg.tp_mode == "ema30":
                    ema_len = 30
                elif cfg.tp_mode == "ema60":
                    ema_len = 60
                ema_exec = _ema(df_ex["close"].astype(float), ema_len)
                tp_val = float(ema_exec.iloc[i]) if not np.isnan(ema_exec.iloc[i]) else None
                if tp_val and tp_val < entry_px:
                    tp_price = tp_val
                elif cfg.tp_mode == "fib618":
                    swing_hi = float(df_mtf["high"].iloc[max(0, idx_mtf - 10): idx_mtf + 1].max())
                    swing_lo = float(df_mtf["low"].iloc[max(0, idx_mtf - 10): idx_mtf + 1].min())
                    fib_val = _fib_618(swing_hi, swing_lo)
                    if fib_val < entry_px:
                        tp_price = fib_val
                if tp_price is None:
                    tp_price = entry_px * (1.0 - cfg.tp_pct)

            ema20_exec = _ema(df_ex["close"].astype(float), 20)
            trade = {
                "entry_px": entry_px,
                "entry_ts": entry_ts,
                "hold_bars": 0,
                "mfe": 0.0,
                "mae": 0.0,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "remaining": 1.0,
                "realized": 0.0,
                "be_done": False,
                "ema20": ema20_exec,
            }
            stats["entries"] += 1

    trades = stats["trades"]
    wins = stats["wins"]
    losses = stats["losses"]
    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats["mfe_sum"] / trades if trades > 0 else 0.0
    avg_mae = stats["mae_sum"] / trades if trades > 0 else 0.0
    avg_hold = stats["hold_sum"] / trades if trades > 0 else 0.0
    net_sum = stats["net_sum"]

    line = (
        f"[BACKTEST] TOTAL entries={stats['entries']} exits={stats['exits']} trades={trades} "
        f"wins={wins} losses={losses} winrate={winrate:.2f}% "
        f"net_sum={net_sum:.3f} avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}"
    )
    print(line)
    _log(line)


if __name__ == "__main__":
    main()
