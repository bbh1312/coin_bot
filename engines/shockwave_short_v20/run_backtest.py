from __future__ import annotations

import argparse
import csv
import os
import sys
import time
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
    format_backtest_summary,
    load_common_universe,
    log_warmup_info,
)
from engines.shockwave_short_v20.engine import ShockwaveShortV20Config


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


def run_backtest() -> None:
    parser = argparse.ArgumentParser("shockwave_short_v20 backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--bb-len", type=int, default=20)
    parser.add_argument("--bb-std", type=float, default=3.0)
    parser.add_argument("--rsi-len", type=int, default=14)
    parser.add_argument("--rsi-hot", type=float, default=80.0)
    parser.add_argument("--vol-ma-len", type=int, default=20)
    parser.add_argument("--vol-min-mult", type=float, default=0.8)
    parser.add_argument("--wick-ratio", type=float, default=0.35)
    parser.add_argument("--confirm-drop-from-high", type=float, default=0.0)
    parser.add_argument("--min-body-ratio", type=float, default=0.3)
    parser.add_argument("--sl-buffer", type=float, default=0.015)
    parser.add_argument("--tp1-mult", type=float, default=0.97)
    parser.add_argument("--tp2-mult", type=float, default=0.97)
    parser.add_argument("--tp1-frac", type=float, default=1.0)
    parser.add_argument("--min-24h-change", type=float, default=0.10)
    parser.add_argument("--log-gates", action="store_true")
    args = parser.parse_args()

    cfg = ShockwaveShortV20Config(
        bb_len=args.bb_len,
        bb_std=args.bb_std,
        rsi_len=args.rsi_len,
        rsi_hot=args.rsi_hot,
        vol_ma_len=args.vol_ma_len,
        vol_min_mult=args.vol_min_mult,
        wick_ratio=args.wick_ratio,
        sl_buffer=args.sl_buffer,
        tp_mult=args.tp2_mult,
    )
    min_24h_change = float(args.min_24h_change)
    tp1_mult = float(args.tp1_mult)
    tp2_mult = float(args.tp2_mult)
    tp1_frac = float(args.tp1_frac)
    min_body_ratio = float(args.min_body_ratio)

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {"3m": max(50, cfg.bb_len + 5, cfg.vol_ma_len + 5, cfg.rsi_len + 5)}
    start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
        args.days, end_ms, min_bars
    )

    use_common = bool(args.common_warmup_dir)
    common_dir = args.common_warmup_dir or os.path.join("logs", "common_warmup", "ohlcv")

    universe = load_common_universe(
        args.universe, exchange, args.cache_only, top_n=args.top_n
    )
    if not universe:
        print("[BACKTEST] no_universe")
        return

    log_warmup_info(lambda _: None, warmup_days, warmup_minutes, args.days)

    data: Dict[str, pd.DataFrame] = {}
    for sym in universe:
        rows = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        if rows:
            df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
            data[sym] = df

    if not data:
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
    gate_counts = {
        "chg24h": 0,
        "hot_zone": 0,
        "selling_pressure": 0,
        "confirm": 0,
    }
    tf_minutes = _tf_to_minutes(cfg.tf)
    bars_24h = int((24 * 60) / tf_minutes) if tf_minutes > 0 else 0

    for sym, df in data.items():
        df_sig = df.iloc[:-1] if args.use_confirmed else df
        if len(df_sig) < max(4, cfg.bb_len + 2, cfg.vol_ma_len + 2, cfg.rsi_len + 2):
            continue
        trade = None
        # indicators
        close = df_sig["close"].astype(float)
        high = df_sig["high"].astype(float)
        low = df_sig["low"].astype(float)
        open_ = df_sig["open"].astype(float)
        basis = close.rolling(cfg.bb_len).mean()
        std = close.rolling(cfg.bb_len).std(ddof=0)
        upper = basis + float(cfg.bb_std) * std
        rsi = _rsi(close, cfg.rsi_len)
        vol_ma = df_sig["volume"].astype(float).rolling(cfg.vol_ma_len).mean()
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
                    trades_out.append(
                        {
                            "symbol": sym,
                            "result": "LOSS",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts_ms,
                        }
                    )
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
                    trades_out.append(
                        {
                            "symbol": sym,
                            "result": "WIN",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts_ms,
                        }
                    )
                    trade = None
                continue

            if i < 3:
                continue
            p2 = df_sig.iloc[i - 2]
            p1 = df_sig.iloc[i - 1]
            if bars_24h > 0 and i - 1 - bars_24h >= 0:
                close_now = float(df_sig.at[i - 1, "close"])
                close_then = float(df_sig.at[i - 1 - bars_24h, "close"])
                change_24h = (close_now / close_then - 1.0) if close_then > 0 else 0.0
            else:
                change_24h = 0.0
            if change_24h < min_24h_change:
                if args.log_gates:
                    gate_counts["chg24h"] += 1
                continue
            # hot zone
            is_hot_zone = (float(p2["high"]) > float(upper.iloc[i - 2])) or (float(rsi.iloc[i - 2]) > float(cfg.rsi_hot))
            if not is_hot_zone:
                if args.log_gates:
                    gate_counts["hot_zone"] += 1
                continue
            # selling pressure
            candle_range = float(p2["high"]) - float(p2["low"])
            if candle_range <= 0:
                if args.log_gates:
                    gate_counts["selling_pressure"] += 1
                continue
            upper_wick = float(p2["high"]) - max(float(p2["open"]), float(p2["close"]))
            wick_ok = upper_wick > candle_range * float(cfg.wick_ratio)
            if not wick_ok:
                if args.log_gates:
                    gate_counts["selling_pressure"] += 1
                continue
            # confirmation
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
                if args.log_gates:
                    gate_counts["confirm"] += 1
                continue
            body = abs(float(p1["close"]) - float(p1["open"]))
            prange = float(p1["high"]) - float(p1["low"])
            if prange <= 0 or (body / prange) < min_body_ratio:
                if args.log_gates:
                    gate_counts["confirm"] += 1
                continue

            entry_px = float(df_sig.at[i, "open"])
            sl_price = float(p2["high"]) * (1.0 + float(cfg.sl_buffer))
            tp_price = entry_px * tp2_mult
            trade = {
                "entry_px": entry_px,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "tp1_price": entry_px * tp1_mult,
                "tp1_frac": tp1_frac,
                "tp1_hit": False,
                "remaining_frac": 1.0,
                "mfe": 0.0,
                "mae": 0.0,
                "hold_bars": 0,
                "entry_ts": int(p1["ts"]),
            }
            stats["entries"] += 1

    print("[BACKTEST] TRADES(KST) symbol result pnl_pct entry_ts exit_ts")
    for t in trades_out:
        print(
            f"[BACKTEST] TRADE {t['symbol']} result={t['result']} pnl_pct={t['pnl_pct']:.2f}% "
            f"entry_ts={_ts_kst(t['entry_ts'])} exit_ts={_ts_kst(t['exit_ts'])}"
        )
    print(format_backtest_summary(None, stats))
    if args.log_gates:
        print(
            "[BACKTEST] GATE_COUNTS "
            f"chg24h={gate_counts['chg24h']} hot_zone={gate_counts['hot_zone']} "
            f"selling_pressure={gate_counts['selling_pressure']} confirm={gate_counts['confirm']}"
        )


def _ts_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt + pd.Timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")


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


if __name__ == "__main__":
    run_backtest()
