from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

import ccxt
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engines.backtest_common import (
    calc_warmup_window,
    load_common_universe,
)


def _read_cached_csv(path: str) -> List[List[float]]:
    rows: List[List[float]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if not row:
                    continue
                try:
                    rows.append([float(x) for x in row[:6]])
                except Exception:
                    continue
    except Exception:
        return []
    return rows


def _fetch_ohlcv_all(
    exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    cache_only: bool = False,
    use_common_warmup: bool = False,
    common_warmup_dir: str | None = None,
    common_only: bool = False,
) -> List[List[float]]:
    if use_common_warmup and common_warmup_dir:
        fname = symbol.replace("/", "_").replace(":", "_")
        cached_path = os.path.join(common_warmup_dir, f"{fname}_{timeframe}.csv")
        if os.path.exists(cached_path):
            rows = _read_cached_csv(cached_path)
            return [r for r in rows if start_ms <= r[0] <= end_ms]
        if common_only:
            return []
    if cache_only or exchange is None:
        return []
    since = start_ms
    out: List[List[float]] = []
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
        if not batch:
            break
        out.extend(batch)
        last = batch[-1][0]
        if last == since:
            break
        since = last + 1
    return [r for r in out if start_ms <= r[0] <= end_ms]


def _parse_floats(csv_list: str) -> List[float]:
    return [float(x.strip()) for x in csv_list.split(",") if x.strip()]


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _tf_to_minutes(tf: str) -> int:
    tf = (tf or "").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    return 0


def run_sweep() -> None:
    parser = argparse.ArgumentParser("shockwave_short_v20 sweep")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--tf", type=str, default="3m")
    parser.add_argument("--bb-len", type=int, default=20)
    parser.add_argument("--rsi-len", type=int, default=14)
    parser.add_argument("--vol-ma-len", type=int, default=20)
    parser.add_argument("--min-24h-change", type=float, default=0.10)
    parser.add_argument("--confirm-drop-from-high", type=float, default=0.0)
    parser.add_argument("--bb-std-list", type=str, default="2.5,3.0,3.5")
    parser.add_argument("--rsi-hot-list", type=str, default="70,75,80")
    parser.add_argument("--wick-ratio-list", type=str, default="0.30,0.35,0.40")
    parser.add_argument("--vol-min-mult-list", type=str, default="0.8,1.0")
    parser.add_argument("--sl-buffer-list", type=str, default="0.012,0.015,0.02")
    parser.add_argument("--tp2-mult-list", type=str, default="0.96,0.97,0.98")
    parser.add_argument("--min-body-ratio-list", type=str, default="0.25,0.30")
    parser.add_argument("--min-winrate", type=float, default=45.0)
    args = parser.parse_args()

    bb_std_vals = _parse_floats(args.bb_std_list)
    rsi_hot_vals = _parse_floats(args.rsi_hot_list)
    wick_ratio_vals = _parse_floats(args.wick_ratio_list)
    vol_min_mult_vals = _parse_floats(args.vol_min_mult_list)
    sl_buffer_vals = _parse_floats(args.sl_buffer_list)
    tp2_mult_vals = _parse_floats(args.tp2_mult_list)
    min_body_ratio_vals = _parse_floats(args.min_body_ratio_list)

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {args.tf: max(60, args.bb_len + 5, args.vol_ma_len + 5, args.rsi_len + 5)}
    start_ms, eval_start_ms, _, _ = calc_warmup_window(args.days, end_ms, min_bars)

    use_common = bool(args.common_warmup_dir)
    common_dir = args.common_warmup_dir or os.path.join("logs", "common_warmup", "ohlcv")

    universe = load_common_universe(
        args.universe, exchange, args.cache_only, top_n=args.top_n
    )
    if not universe:
        print("[SWEEP] no_universe")
        return

    data: Dict[str, Dict[str, pd.Series]] = {}
    for sym in universe:
        rows = _fetch_ohlcv_all(
            exchange,
            sym,
            args.tf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df_sig = df.iloc[:-1] if args.use_confirmed else df
        if len(df_sig) < max(50, args.bb_len + 5, args.vol_ma_len + 5, args.rsi_len + 5):
            continue
        close = df_sig["close"].astype(float)
        high = df_sig["high"].astype(float)
        low = df_sig["low"].astype(float)
        open_ = df_sig["open"].astype(float)
        basis = close.rolling(args.bb_len).mean()
        std = close.rolling(args.bb_len).std(ddof=0)
        rsi = _rsi(close, args.rsi_len)
        vol_ma = df_sig["volume"].astype(float).rolling(args.vol_ma_len).mean()
        data[sym] = {
            "df": df_sig,
            "close": close,
            "high": high,
            "low": low,
            "open": open_,
            "basis": basis,
            "std": std,
            "rsi": rsi,
            "vol_ma": vol_ma,
        }

    if not data:
        print("[SWEEP] no_data")
        return

    tf_minutes = _tf_to_minutes(args.tf)
    bars_24h = int((24 * 60) / tf_minutes) if tf_minutes > 0 else 0

    results = []

    for bb_std in bb_std_vals:
        for rsi_hot in rsi_hot_vals:
            for wick_ratio in wick_ratio_vals:
                for vol_min_mult in vol_min_mult_vals:
                    for sl_buffer in sl_buffer_vals:
                        for tp2_mult in tp2_mult_vals:
                            for min_body_ratio in min_body_ratio_vals:
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
                                for sym, d in data.items():
                                    df_sig = d["df"]
                                    upper = d["basis"] + float(bb_std) * d["std"]
                                    rsi = d["rsi"]
                                    vol_ma = d["vol_ma"]
                                    trade = None
                                    for i in range(1, len(df_sig)):
                                        ts_ms = int(df_sig.at[i, "ts"])
                                        if ts_ms < eval_start_ms:
                                            continue
                                        if trade:
                                            high_i = float(df_sig.at[i, "high"])
                                            low_i = float(df_sig.at[i, "low"])
                                            trade["hold_bars"] += 1
                                            trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low_i) / trade["entry_px"]))
                                            trade["mae"] = max(trade["mae"], max(0.0, (high_i - trade["entry_px"]) / trade["entry_px"]))
                                            if trade["tp1_hit"] is False and low_i <= trade["tp1_price"]:
                                                pnl_pct = (trade["entry_px"] - trade["tp1_price"]) / trade["entry_px"]
                                                stats["tp_sum"] += pnl_pct * trade["tp1_frac"]
                                                stats["net_sum"] += pnl_pct * trade["tp1_frac"]
                                                trade["tp1_hit"] = True
                                                trade["remaining_frac"] = 1.0 - trade["tp1_frac"]
                                                trade["sl_price"] = trade["entry_px"]
                                                continue
                                            if high_i >= trade["sl_price"]:
                                                exit_px = trade["sl_price"]
                                                pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                                                pnl_pct *= trade["remaining_frac"]
                                                stats["exits"] += 1
                                                stats["trades"] += 1
                                                stats["mfe_sum"] += trade["mfe"]
                                                stats["mae_sum"] += trade["mae"]
                                                stats["hold_sum"] += trade["hold_bars"]
                                                stats["net_sum"] += pnl_pct
                                                stats["sl_sum"] += pnl_pct
                                                stats["losses"] += 1
                                                trade = None
                                            elif low_i <= trade["tp_price"]:
                                                exit_px = trade["tp_price"]
                                                pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                                                pnl_pct *= trade["remaining_frac"]
                                                stats["exits"] += 1
                                                stats["trades"] += 1
                                                stats["mfe_sum"] += trade["mfe"]
                                                stats["mae_sum"] += trade["mae"]
                                                stats["hold_sum"] += trade["hold_bars"]
                                                stats["net_sum"] += pnl_pct
                                                stats["tp_sum"] += pnl_pct
                                                stats["wins"] += 1
                                                trade = None
                                            continue

                                        if i < 3:
                                            continue
                                        if bars_24h > 0 and i - 1 - bars_24h >= 0:
                                            close_now = float(df_sig.at[i - 1, "close"])
                                            close_then = float(df_sig.at[i - 1 - bars_24h, "close"])
                                            change_24h = (close_now / close_then - 1.0) if close_then > 0 else 0.0
                                        else:
                                            change_24h = 0.0
                                        if change_24h < args.min_24h_change:
                                            continue

                                        p2 = df_sig.iloc[i - 2]
                                        p1 = df_sig.iloc[i - 1]

                                        is_hot_zone = (float(p2["high"]) > float(upper.iloc[i - 2])) or (float(rsi.iloc[i - 2]) > float(rsi_hot))
                                        if not is_hot_zone:
                                            continue
                                        candle_range = float(p2["high"]) - float(p2["low"])
                                        if candle_range <= 0:
                                            continue
                                        upper_wick = float(p2["high"]) - max(float(p2["open"]), float(p2["close"]))
                                        wick_ok = upper_wick > candle_range * float(wick_ratio)
                                        if not wick_ok:
                                            continue

                                        if args.confirm_drop_from_high > 0:
                                            is_confirmed = float(p1["high"]) < float(p2["high"]) and float(p1["close"]) < float(p2["high"]) * (1.0 - float(args.confirm_drop_from_high))
                                        else:
                                            p2_body_mid = float(p2["open"]) + (float(p2["close"]) - float(p2["open"])) * 0.5
                                            is_confirmed = (
                                                float(p1["high"]) < float(p2["high"])
                                                and float(p1["close"]) < float(p1["open"])
                                                and float(p1["close"]) < p2_body_mid
                                            )
                                        if not is_confirmed:
                                            continue
                                        body = abs(float(p1["close"]) - float(p1["open"]))
                                        prange = float(p1["high"]) - float(p1["low"])
                                        if prange <= 0 or (body / prange) < min_body_ratio:
                                            continue

                                        entry_px = float(df_sig.at[i, "open"])
                                        sl_price = float(p2["high"]) * (1.0 + float(sl_buffer))
                                        tp_price = entry_px * float(tp2_mult)
                                        trade = {
                                            "entry_px": entry_px,
                                            "sl_price": sl_price,
                                            "tp_price": tp_price,
                                            "tp1_price": entry_px * float(tp2_mult),
                                            "tp1_frac": 1.0,
                                            "tp1_hit": False,
                                            "remaining_frac": 1.0,
                                            "mfe": 0.0,
                                            "mae": 0.0,
                                            "hold_bars": 0,
                                        }
                                        stats["entries"] += 1

                                trades = int(stats.get("trades", 0))
                                wins = int(stats.get("wins", 0))
                                winrate = (wins / trades * 100.0) if trades > 0 else 0.0
                                if trades < 3:
                                    continue
                                if winrate >= args.min_winrate:
                                    results.append(
                                        {
                                            "winrate": winrate,
                                            "trades": trades,
                                            "net_sum": stats.get("net_sum", 0.0),
                                            "params": {
                                                "bb_std": bb_std,
                                                "rsi_hot": rsi_hot,
                                                "wick_ratio": wick_ratio,
                                                "vol_min_mult": vol_min_mult,
                                                "sl_buffer": sl_buffer,
                                                "tp2_mult": tp2_mult,
                                                "min_body_ratio": min_body_ratio,
                                            },
                                        }
                                    )

    results.sort(key=lambda x: (x["net_sum"], x["winrate"], x["trades"]), reverse=True)
    print("[SWEEP] TOP10")
    for row in results[:10]:
        params = " ".join([f"{k}={v}" for k, v in row["params"].items()])
        print(
            f"[SWEEP] winrate={row['winrate']:.2f}% trades={row['trades']} net_sum={row['net_sum']:.3f} {params}"
        )


if __name__ == "__main__":
    run_sweep()
