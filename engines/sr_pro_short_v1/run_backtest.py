from __future__ import annotations

import argparse
import json
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

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
from engines.sr_pro_short_v1.engine import SrProShortV1Config


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


def _tf_to_minutes(tf: str) -> int:
    tf = (tf or "").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    return 0


def _pivot_high(series: pd.Series, left: int, right: int, idx: int) -> Optional[float]:
    if idx - left < 0 or idx + right >= len(series):
        return None
    window = series.iloc[idx - left : idx + right + 1]
    val = series.iloc[idx]
    if float(val) == float(window.max()):
        return float(val)
    return None


def _pivot_low(series: pd.Series, left: int, right: int, idx: int) -> Optional[float]:
    if idx - left < 0 or idx + right >= len(series):
        return None
    window = series.iloc[idx - left : idx + right + 1]
    val = series.iloc[idx]
    if float(val) == float(window.min()):
        return float(val)
    return None


@dataclass
class Zone:
    mid: float
    top: float
    bot: float
    side: int
    live: bool
    born: int
    start: int
    vol: float


def run_backtest() -> None:
    parser = argparse.ArgumentParser("sr_pro_short_v1 backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--relaxed-lookback", type=int, default=10)
    parser.add_argument("--auto-relax", action="store_true")
    parser.add_argument("--atr-mult", type=float, default=1.0)
    parser.add_argument("--delta-len", type=int, default=2)
    parser.add_argument("--cluster-atr", type=float, default=1.5)
    parser.add_argument("--max-zones-per-side", type=int, default=8)
    parser.add_argument("--touch-mode", type=str, default="bot", choices=["bot", "mid"])
    parser.add_argument("--touch-use-close", action="store_true")
    parser.add_argument("--dvf-norm-max", type=float, default=0.0)
    parser.add_argument("--require-reject-close", action="store_true")
    parser.add_argument("--reject-mode", type=str, default="bot", choices=["bot", "mid"])
    parser.add_argument("--reject-source", type=str, default="1h", choices=["1h", "15m"])
    parser.add_argument("--ema200-filter", action="store_true")
    parser.add_argument("--ema-filter-len", type=int, default=200)
    parser.add_argument("--retest-bars", type=int, default=6)
    parser.add_argument("--retest-atr-mult", type=float, default=0.25)
    parser.add_argument("--retest-near-atr-mult", type=float, default=0.15)
    parser.add_argument("--retest-wick-max", type=float, default=0.4)
    parser.add_argument("--retest-dyn", action="store_true")
    parser.add_argument("--retest-dyn-th", type=float, default=0.6)
    parser.add_argument("--retest-dyn-bars", type=int, default=10)
    parser.add_argument("--shallow-atr-mult", type=float, default=0.35)
    parser.add_argument("--shallow-wick-max", type=float, default=0.35)
    parser.add_argument("--shallow-dvf-max", type=float, default=0.0)
    parser.add_argument("--sl-buffer", type=float, default=0.01)
    parser.add_argument("--tp-mult", type=float, default=0.985)
    parser.add_argument("--tp-mult-weak", type=float, default=0.985)
    parser.add_argument("--sl-min-weak", type=float, default=1.0015)
    parser.add_argument("--base-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-usdt", type=float, default=10.0)
    parser.add_argument("--freeze-zones", action="store_true")
    parser.add_argument("--total-window-days", type=int, default=0)
    parser.add_argument("--rolling-zones", action="store_true")
    parser.add_argument("--zones-snapshot-in", type=str, default="")
    parser.add_argument("--zones-snapshot-out", type=str, default="")
    parser.add_argument("--log-gates", action="store_true")
    parser.add_argument("--debug-zone", action="store_true")
    args = parser.parse_args()

    cfg = SrProShortV1Config(
        lookback=args.lookback,
        relaxed_lookback=args.relaxed_lookback,
        auto_relax=args.auto_relax,
        atr_mult=args.atr_mult,
        delta_len=args.delta_len,
        cluster_atr=args.cluster_atr,
        max_zones_per_side=args.max_zones_per_side,
        sl_buffer=args.sl_buffer,
        tp_mult=args.tp_mult,
    )

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {
        cfg.tf_ltf: 120,
        cfg.tf_mtf: 120,
        cfg.tf_htf: 120,
    }
    if args.total_window_days:
        if args.total_window_days < args.days:
            print("[BACKTEST] total_window_days must be >= days")
            return
        total_window_days = args.total_window_days
        warmup_days = max(0, total_window_days - args.days)
        warmup_minutes = warmup_days * 1440
        start_ms = end_ms - int(total_window_days * 24 * 60 * 60 * 1000)
        eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    else:
        start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
            args.days, end_ms, min_bars
        )

    if args.rolling_zones and not args.total_window_days:
        print("[BACKTEST] rolling_zones requires total_window_days")
        return

    use_common = bool(args.common_warmup_dir)
    common_dir = args.common_warmup_dir or os.path.join("logs", "common_warmup", "ohlcv")
    universe = load_common_universe(
        args.universe, exchange, args.cache_only, top_n=args.top_n
    )
    if not universe:
        print("[BACKTEST] no_universe")
        return

    log_warmup_info(lambda _: None, warmup_days, warmup_minutes, args.days)

    data: Dict[str, Dict[str, pd.DataFrame]] = {}
    for sym in universe:
        rows_3m = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_ltf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        rows_15m = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_mtf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        rows_1h = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_htf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        if rows_3m and rows_15m and rows_1h:
            data[sym] = {
                "3m": pd.DataFrame(rows_3m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "15m": pd.DataFrame(rows_15m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "1h": pd.DataFrame(rows_1h, columns=["ts", "open", "high", "low", "close", "volume"]),
            }

    if not data:
        print("[BACKTEST] no_data")
        return

    zones_snapshot_in: Dict[str, List[dict]] = {}
    if args.zones_snapshot_in:
        try:
            with open(args.zones_snapshot_in, "r", encoding="utf-8") as f:
                zones_snapshot_in = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[BACKTEST] zones_snapshot_in_error={exc}")
            zones_snapshot_in = {}

    zones_snapshot_out: Dict[str, List[dict]] = {}

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
    trades_out: List[dict] = []
    gate_counts = {
        "zone_touch": 0,
        "lh_15m": 0,
        "break_3m": 0,
        "break_3m_strong": 0,
        "break_3m_weak": 0,
        "retest_seen": 0,
        "retest_pass_close": 0,
        "retest_pass_low": 0,
        "retest_fail_far": 0,
        "retest_fail_shallow": 0,
        "entry_by_pass_close": 0,
        "entry_by_pass_low": 0,
        "reject_pass_1h": 0,
        "reject_pass_15m": 0,
        "ema200_pass": 0,
        "entries_strong": 0,
        "entries_weak": 0,
        "wins_strong": 0,
        "wins_weak": 0,
        "losses_strong": 0,
        "losses_weak": 0,
        "net_strong": 0.0,
        "net_weak": 0.0,
        "mfe_strong_sum": 0.0,
        "mfe_weak_sum": 0.0,
        "mae_strong_sum": 0.0,
        "mae_weak_sum": 0.0,
        "hold_strong_sum": 0.0,
        "hold_weak_sum": 0.0,
    }

    entries_by_day: Dict[str, int] = {}

    for sym, frames in data.items():
        df_3m = frames["3m"]
        df_15m = frames["15m"]
        df_1h = frames["1h"]
        if args.use_confirmed:
            df_3m = df_3m.iloc[:-1]
            df_15m = df_15m.iloc[:-1]

        if len(df_3m) < 10 or len(df_15m) < 5 or len(df_1h) < (cfg.lookback * 2 + 5):
            continue

        # precompute 1h indicators for zones
        close_1h = df_1h["close"].astype(float)
        open_1h = df_1h["open"].astype(float)
        high_1h = df_1h["high"].astype(float)
        low_1h = df_1h["low"].astype(float)
        volume_1h = df_1h["volume"].astype(float)
        atr_1h = _atr(df_1h, 14)
        ema200_1h = _ema(close_1h, 200)
        dv = np.where(close_1h > open_1h, volume_1h, np.where(close_1h < open_1h, -volume_1h, 0.0))
        dv = pd.Series(dv, index=df_1h.index)
        dvf = _ema(dv, cfg.delta_len)
        vol_ema = _ema(volume_1h, cfg.delta_len)
        atr_3m = _atr(df_3m, 14)
        atr_15m = _atr(df_15m, 14)

        zones: List[Zone] = []
        if args.zones_snapshot_in and sym in zones_snapshot_in:
            zones = [
                Zone(
                    mid=float(z["mid"]),
                    top=float(z["top"]),
                    bot=float(z["bot"]),
                    side=int(z["side"]),
                    live=bool(z["live"]),
                    born=int(z["born"]),
                    start=int(z["start"]),
                    vol=float(z["vol"]),
                )
                for z in zones_snapshot_in.get(sym, [])
            ]

        def _pivot_confirmed(
            series: pd.Series,
            i: int,
            lb: int,
            mode: str,
            start_idx: int,
            end_idx: int,
        ) -> Optional[float]:
            pivot_idx = i - lb
            if pivot_idx - lb < start_idx or pivot_idx + lb > end_idx:
                return None
            window = series.iloc[pivot_idx - lb : pivot_idx + lb + 1]
            val = series.iloc[pivot_idx]
            if mode == "high":
                return float(val) if float(val) == float(window.max()) else None
            return float(val) if float(val) == float(window.min()) else None

        window_bars_1h = int(args.total_window_days * 24) if args.total_window_days else 0

        def build_zones(end_idx: int) -> List[Zone]:
            zones_local: List[Zone] = []
            if end_idx < 0:
                return zones_local
            start_idx = 0
            if window_bars_1h:
                start_idx = max(0, end_idx - window_bars_1h + 1)

            def merge_or_create(
                zones_ref: List[Zone],
                side: int,
                level: float,
                vol_val: float,
                half_w: float,
                cluster_dist: float,
                born_idx: int,
                start_bar: int,
            ) -> None:
                merged = False
                for z in zones_ref:
                    if z.live and z.side == side and abs(z.mid - level) <= cluster_dist:
                        new_mid = (z.mid + level) * 0.5
                        z.mid = new_mid
                        z.top = new_mid + half_w
                        z.bot = new_mid - half_w
                        z.vol = (z.vol + vol_val) * 0.5
                        merged = True
                        break
                if not merged:
                    zones_ref.append(
                        Zone(
                            mid=level,
                            top=level + half_w,
                            bot=level - half_w,
                            side=side,
                            live=True,
                            born=born_idx,
                            start=start_bar,
                            vol=vol_val,
                        )
                    )

            for i1 in range(start_idx, end_idx + 1):
                lb = cfg.lookback
                ph = _pivot_confirmed(high_1h, i1, lb, "high", start_idx, end_idx)
                pl = _pivot_confirmed(low_1h, i1, lb, "low", start_idx, end_idx)
                lb_used = lb
                if cfg.auto_relax and ph is None and pl is None:
                    lb2 = cfg.relaxed_lookback
                    ph = _pivot_confirmed(high_1h, i1, lb2, "high", start_idx, end_idx)
                    pl = _pivot_confirmed(low_1h, i1, lb2, "low", start_idx, end_idx)
                    lb_used = lb2
                if ph is None and pl is None:
                    if i1 % 20 == 0:
                        for side in (1, -1):
                            side_z = [z for z in zones_local if z.side == side]
                            if len(side_z) > cfg.max_zones_per_side:
                                oldest = min(side_z, key=lambda z: z.born)
                                zones_local.remove(oldest)
                    continue
                pivot_bar = i1 - lb_used
                if pivot_bar < start_idx:
                    continue
                half_w = float(atr_1h.iloc[i1]) * cfg.atr_mult * 0.5
                cluster_dist = float(atr_1h.iloc[i1]) * cfg.cluster_atr
                vol_val = float(dvf.iloc[pivot_bar]) if pivot_bar < len(dvf) else float(dvf.iloc[i1])
                if ph is not None:
                    merge_or_create(zones_local, 1, float(ph), vol_val, half_w, cluster_dist, i1, pivot_bar)
                if pl is not None:
                    merge_or_create(zones_local, -1, float(pl), vol_val, half_w, cluster_dist, i1, pivot_bar)
                if i1 % 20 == 0:
                    for side in (1, -1):
                        side_z = [z for z in zones_local if z.side == side]
                        if len(side_z) > cfg.max_zones_per_side:
                            oldest = min(side_z, key=lambda z: z.born)
                            zones_local.remove(oldest)
            return zones_local

        # build zones from 1h history first (TradingView pivot confirmed style)
        if not zones and not args.rolling_zones:
            zone_end_idx = len(df_1h)
            if args.freeze_zones:
                zone_end_idx = int(np.searchsorted(df_1h["ts"].values, eval_start_ms, side="right"))
            zones = build_zones(zone_end_idx - 1)

        if args.zones_snapshot_out:
            zones_snapshot_out[sym] = [
                {
                    "mid": z.mid,
                    "top": z.top,
                    "bot": z.bot,
                    "side": z.side,
                    "live": z.live,
                    "born": z.born,
                    "start": z.start,
                    "vol": z.vol,
                }
                for z in zones
            ]

        # map 1h index by ts for fast lookup
        ts_1h = df_1h["ts"].values
        ts_15m = df_15m["ts"].values
        ts_3m = df_3m["ts"].values

        trade = None
        retest_active = False
        retest_level = 0.0
        retest_until = -1
        last_zone_end_idx = None
        if args.log_gates:
            print(
                f"[BACKTEST_START_STATE] {sym} in_position=False cooldown=0 "
                f"zones_total={len(zones)} zones_res={len([z for z in zones if z.side==1])} "
                f"zones_sup={len([z for z in zones if z.side==-1])}"
            )

        for i3 in range(3, len(df_3m) - 1):
            ts = int(ts_3m[i3])
            if ts < eval_start_ms:
                continue

            # resolve current 1h bar index
            idx_1h = int(np.searchsorted(ts_1h, ts, side="right") - 1)
            if idx_1h < 0:
                continue


            if args.rolling_zones and not zones_snapshot_in:
                if last_zone_end_idx != idx_1h:
                    zones = build_zones(idx_1h)
                    last_zone_end_idx = idx_1h

            # invalidate zones
            close_1h_now = float(close_1h.iloc[idx_1h])
            for z in zones:
                if z.live and z.side == 1 and close_1h_now > z.top:
                    z.live = False
                if z.live and z.side == -1 and close_1h_now < z.bot:
                    z.live = False

            # handle existing trade
            if trade:
                high_i = float(df_3m.at[i3, "high"])
                low_i = float(df_3m.at[i3, "low"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low_i) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high_i - trade["entry_px"]) / trade["entry_px"]))
                if high_i >= trade["sl_price"]:
                    exit_px = trade["sl_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    stats["sl_sum"] += pnl_pct
                    stats["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                    stats["sl_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                    stats["losses"] += 1
                    if trade.get("track") == "strong":
                        gate_counts["losses_strong"] += 1
                        gate_counts["net_strong"] += pnl_pct
                        gate_counts["mfe_strong_sum"] += trade["mfe"]
                        gate_counts["mae_strong_sum"] += trade["mae"]
                        gate_counts["hold_strong_sum"] += trade["hold_bars"]
                    elif trade.get("track") == "weak":
                        gate_counts["losses_weak"] += 1
                        gate_counts["net_weak"] += pnl_pct
                        gate_counts["mfe_weak_sum"] += trade["mfe"]
                        gate_counts["mae_weak_sum"] += trade["mae"]
                        gate_counts["hold_weak_sum"] += trade["hold_bars"]
                    trades_out.append(
                        {
                            "symbol": sym,
                            "result": "LOSS",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                        }
                    )
                    trade = None
                elif low_i <= trade["tp_price"]:
                    exit_px = trade["tp_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    stats["tp_sum"] += pnl_pct
                    stats["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                    stats["tp_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                    stats["wins"] += 1
                    if trade.get("track") == "strong":
                        gate_counts["wins_strong"] += 1
                        gate_counts["net_strong"] += pnl_pct
                        gate_counts["mfe_strong_sum"] += trade["mfe"]
                        gate_counts["mae_strong_sum"] += trade["mae"]
                        gate_counts["hold_strong_sum"] += trade["hold_bars"]
                    elif trade.get("track") == "weak":
                        gate_counts["wins_weak"] += 1
                        gate_counts["net_weak"] += pnl_pct
                        gate_counts["mfe_weak_sum"] += trade["mfe"]
                        gate_counts["mae_weak_sum"] += trade["mae"]
                        gate_counts["hold_weak_sum"] += trade["hold_bars"]
                    trades_out.append(
                        {
                            "symbol": sym,
                            "result": "WIN",
                            "pnl_pct": pnl_pct * 100.0,
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                        }
                    )
                    trade = None
                continue

            # 1h current bar touching resistance zone with negative delta
            h1_high = float(high_1h.iloc[idx_1h])
            h1_low = float(low_1h.iloc[idx_1h])
            h1_close = float(close_1h.iloc[idx_1h])
            h1_touch_level = None
            if args.touch_mode == "mid":
                h1_touch_level = "mid"
            else:
                h1_touch_level = "bot"
            dvf_norm = (
                float(dvf.iloc[idx_1h]) / float(vol_ema.iloc[idx_1h])
                if float(vol_ema.iloc[idx_1h]) > 0
                else 0.0
            )
            h1_touch_px = h1_close if args.touch_use_close else h1_high
            if args.ema200_filter:
                ema_len = max(1, int(args.ema_filter_len))
                ema_line = _ema(close_1h, ema_len)
                ema_now = float(ema_line.iloc[idx_1h])
                if h1_close >= ema_now:
                    if args.log_gates:
                        gate_counts["zone_touch"] += 1
                    continue
                if args.log_gates:
                    gate_counts["ema200_pass"] += 1
            resist_candidates = [
                z
                for z in zones
                if z.live
                and z.side == 1
                and dvf_norm <= float(args.dvf_norm_max)
                and h1_touch_px >= (z.mid if h1_touch_level == "mid" else z.bot)
                and h1_low <= z.top
            ]
            if resist_candidates and args.require_reject_close:
                reject_level = "mid" if args.reject_mode == "mid" else "bot"
                if args.reject_source == "1h":
                    resist_candidates = [
                        z
                        for z in resist_candidates
                        if h1_close < (z.mid if reject_level == "mid" else z.bot)
                    ]
                    if resist_candidates and args.log_gates:
                        gate_counts["reject_pass_1h"] += 1
                else:
                    idx_15m_rej = int(np.searchsorted(ts_15m, ts, side="right") - 1)
                    if idx_15m_rej >= 0:
                        close_15m = float(df_15m.at[idx_15m_rej, "close"])
                        resist_candidates = [
                            z
                            for z in resist_candidates
                            if close_15m < (z.mid if reject_level == "mid" else z.bot)
                        ]
                        if resist_candidates and args.log_gates:
                            gate_counts["reject_pass_15m"] += 1
            if args.debug_zone and (i3 % 200 == 0):
                live_res = [z for z in zones if z.live and z.side == 1]
                live_sup = [z for z in zones if z.live and z.side == -1]
                nearest_res = min(live_res, key=lambda z: abs(z.mid - h1_touch_px)) if live_res else None
                if nearest_res:
                    print(
                        f"[DEBUG][ZONE] {sym} ts={_ts_kst(ts)} "
                        f"h1_touch_px={h1_touch_px:.6f} res_mid={nearest_res.mid:.6f} "
                        f"res_top={nearest_res.top:.6f} res_bot={nearest_res.bot:.6f} "
                        f"dvf_norm={dvf_norm:.4f} zones_res={len(live_res)} zones_sup={len(live_sup)}"
                    )
                else:
                    print(
                        f"[DEBUG][ZONE] {sym} ts={_ts_kst(ts)} "
                        f"h1_touch_px={h1_touch_px:.6f} res_mid=NONE "
                        f"dvf_norm={dvf_norm:.4f} zones_res={len(live_res)} zones_sup={len(live_sup)}"
                    )
            if not resist_candidates:
                if args.log_gates:
                    gate_counts["zone_touch"] += 1
                continue

            # 15m lower high
            idx_15m = int(np.searchsorted(ts_15m, ts, side="right") - 1)
            if idx_15m < 2:
                continue
            if (
                float(df_15m.at[idx_15m, "high"]) >= float(df_15m.at[idx_15m - 1, "high"])
                and float(df_15m.at[idx_15m - 1, "high"]) >= float(df_15m.at[idx_15m - 2, "high"])
            ):
                if args.log_gates:
                    gate_counts["lh_15m"] += 1
                continue

            # 3m structure break: close < min(low[-3:])
            close_now = float(df_3m.at[i3, "close"])
            low_prev = [
                float(df_3m.at[i3 - 1, "low"]),
                float(df_3m.at[i3 - 2, "low"]),
                float(df_3m.at[i3 - 3, "low"]),
            ]
            low_min = min(low_prev)
            strong_break = close_now < low_min
            weak_break = (float(df_3m.at[i3, "low"]) < low_min) and (close_now >= low_min) and (close_now < float(df_3m.at[i3, "open"]))
            if not strong_break and not weak_break:
                if args.log_gates:
                    gate_counts["break_3m"] += 1
                continue
            if args.log_gates:
                if strong_break:
                    gate_counts["break_3m_strong"] += 1
                elif weak_break:
                    gate_counts["break_3m_weak"] += 1

            # arm retest after break
            retest_level = low_min
            retest_active = True
            retest_bars = max(1, int(args.retest_bars))
            if args.retest_dyn:
                atr3 = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                atr15 = float(atr_15m.iloc[idx_15m]) if not np.isnan(atr_15m.iloc[idx_15m]) else 0.0
                if atr15 > 0 and (atr3 / atr15) < float(args.retest_dyn_th):
                    retest_bars = max(retest_bars, int(args.retest_dyn_bars))
            retest_until = i3 + retest_bars
            if args.log_gates:
                gate_counts["retest_seen"] += 1

            if retest_active and i3 <= retest_until:
                high_now = float(df_3m.at[i3, "high"])
                atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                if high_now >= retest_level - (atr_now * float(args.retest_atr_mult)):
                    if close_now < retest_level:
                        if args.log_gates:
                            gate_counts["retest_pass_close"] += 1
                        entry_px = float(df_3m.at[i3 + 1, "open"])
                        nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                        if high_now > nearest.top:
                            continue
                        sl_raw = nearest.top * (1.0 + cfg.sl_buffer)
                        sl_price = max(sl_raw, entry_px * 1.002)
                        tp_price = entry_px * (cfg.tp_mult if strong_break else float(args.tp_mult_weak))
                        trade = {
                            "entry_px": entry_px,
                            "sl_price": sl_price,
                            "tp_price": tp_price,
                            "mfe": 0.0,
                            "mae": 0.0,
                            "hold_bars": 0,
                            "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                            "track": "strong" if strong_break else "weak",
                        }
                        stats["entries"] += 1
                        if args.log_gates:
                            gate_counts["entry_by_pass_close"] += 1
                            if strong_break:
                                gate_counts["entries_strong"] += 1
                            else:
                                gate_counts["entries_weak"] += 1
                        day_key = _ts_kst(trade["entry_ts"]).split(" ")[0]
                        entries_by_day[day_key] = entries_by_day.get(day_key, 0) + 1
                        retest_active = False
                    else:
                        low_now = float(df_3m.at[i3, "low"])
                        if low_now < retest_level and close_now < float(df_3m.at[i3, "open"]):
                            rng = float(df_3m.at[i3, "high"]) - float(df_3m.at[i3, "low"])
                            upper_wick = float(df_3m.at[i3, "high"]) - max(float(df_3m.at[i3, "open"]), float(df_3m.at[i3, "close"]))
                            wick_ratio = (upper_wick / rng) if rng > 0 else 0.0
                            if wick_ratio > float(args.retest_wick_max):
                                if args.log_gates:
                                    gate_counts["retest_fail_shallow"] += 1
                                retest_active = True
                                if i3 >= retest_until:
                                    retest_active = False
                                continue
                            if args.log_gates:
                                gate_counts["retest_pass_low"] += 1
                            entry_px = float(df_3m.at[i3 + 1, "open"])
                            nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                            sl_raw = nearest.top * (1.0 + cfg.sl_buffer)
                            sl_min = entry_px * (1.002 if strong_break else float(args.sl_min_weak))
                            sl_price = max(sl_raw, sl_min)
                            tp_price = entry_px * (cfg.tp_mult if strong_break else float(args.tp_mult_weak))
                            trade = {
                                "entry_px": entry_px,
                                "sl_price": sl_price,
                                "tp_price": tp_price,
                                "mfe": 0.0,
                                "mae": 0.0,
                                "hold_bars": 0,
                                "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                                "track": "strong" if strong_break else "weak",
                            }
                            stats["entries"] += 1
                            if args.log_gates:
                                gate_counts["entry_by_pass_low"] += 1
                                if strong_break:
                                    gate_counts["entries_strong"] += 1
                                else:
                                    gate_counts["entries_weak"] += 1
                            day_key = _ts_kst(trade["entry_ts"]).split(" ")[0]
                            entries_by_day[day_key] = entries_by_day.get(day_key, 0) + 1
                            retest_active = False
                        else:
                            near_limit = retest_level + (atr_now * float(args.retest_near_atr_mult))
                            if high_now < near_limit and close_now < float(df_3m.at[i3, "open"]):
                                if args.log_gates:
                                    gate_counts["retest_fail_shallow"] += 1
                            shallow_limit = retest_level + (atr_now * float(args.shallow_atr_mult))
                            if (
                                weak_break
                                and high_now < shallow_limit
                                and close_now < float(df_3m.at[i3, "open"])
                                and dvf_norm <= float(args.shallow_dvf_max)
                            ):
                                rng = float(df_3m.at[i3, "high"]) - float(df_3m.at[i3, "low"])
                                upper_wick = float(df_3m.at[i3, "high"]) - max(float(df_3m.at[i3, "open"]), float(df_3m.at[i3, "close"]))
                                wick_ratio = (upper_wick / rng) if rng > 0 else 0.0
                                if wick_ratio <= float(args.shallow_wick_max):
                                    entry_px = float(df_3m.at[i3 + 1, "open"])
                                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                                    sl_raw = nearest.top * (1.0 + cfg.sl_buffer)
                                    sl_min = entry_px * float(args.sl_min_weak)
                                    sl_price = max(sl_raw, sl_min)
                                    tp_price = entry_px * float(args.tp_mult_weak)
                                    trade = {
                                        "entry_px": entry_px,
                                        "sl_price": sl_price,
                                        "tp_price": tp_price,
                                        "mfe": 0.0,
                                        "mae": 0.0,
                                        "hold_bars": 0,
                                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                                        "track": "weak",
                                    }
                                    stats["entries"] += 1
                                    if args.log_gates:
                                        gate_counts["entry_by_pass_low"] += 1
                                        gate_counts["entries_weak"] += 1
                                    day_key = _ts_kst(trade["entry_ts"]).split(" ")[0]
                                    entries_by_day[day_key] = entries_by_day.get(day_key, 0) + 1
                                    retest_active = False
                if i3 >= retest_until:
                    if args.log_gates:
                        gate_counts["retest_fail_far"] += 1
                    retest_active = False

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
            f"zone_touch={gate_counts['zone_touch']} "
            f"lh_15m={gate_counts['lh_15m']} "
            f"break_3m={gate_counts['break_3m']} "
            f"break_strong={gate_counts['break_3m_strong']} "
            f"break_weak={gate_counts['break_3m_weak']} "
            f"retest_seen={gate_counts['retest_seen']} "
            f"retest_pass_close={gate_counts['retest_pass_close']} "
            f"retest_pass_low={gate_counts['retest_pass_low']} "
            f"retest_fail_far={gate_counts['retest_fail_far']} "
            f"retest_fail_shallow={gate_counts['retest_fail_shallow']} "
            f"entry_by_pass_close={gate_counts['entry_by_pass_close']} "
            f"entry_by_pass_low={gate_counts['entry_by_pass_low']} "
            f"reject_pass_1h={gate_counts['reject_pass_1h']} "
            f"reject_pass_15m={gate_counts['reject_pass_15m']} "
            f"ema200_pass={gate_counts['ema200_pass']} "
            f"entries_strong={gate_counts['entries_strong']} "
            f"entries_weak={gate_counts['entries_weak']} "
            f"wins_strong={gate_counts['wins_strong']} "
            f"wins_weak={gate_counts['wins_weak']} "
            f"losses_strong={gate_counts['losses_strong']} "
            f"losses_weak={gate_counts['losses_weak']} "
            f"net_strong={gate_counts['net_strong']:.3f} "
            f"net_weak={gate_counts['net_weak']:.3f} "
            f"mfe_strong={gate_counts['mfe_strong_sum']:.3f} "
            f"mfe_weak={gate_counts['mfe_weak_sum']:.3f} "
            f"mae_strong={gate_counts['mae_strong_sum']:.3f} "
            f"mae_weak={gate_counts['mae_weak_sum']:.3f} "
            f"hold_strong={gate_counts['hold_strong_sum']:.1f} "
            f"hold_weak={gate_counts['hold_weak_sum']:.1f}"
        )
        if entries_by_day:
            print("[BACKTEST] ENTRIES_BY_DAY")
            for day in sorted(entries_by_day.keys()):
                print(f"[BACKTEST] {day} entries={entries_by_day[day]}")

    if args.zones_snapshot_out:
        try:
            out_dir = os.path.dirname(args.zones_snapshot_out)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with open(args.zones_snapshot_out, "w", encoding="utf-8") as f:
                json.dump(zones_snapshot_out, f, ensure_ascii=False)
            print(
                f"[BACKTEST] zones_snapshot_out saved={args.zones_snapshot_out} "
                f"symbols={len(zones_snapshot_out)}"
            )
        except OSError as exc:
            print(f"[BACKTEST] zones_snapshot_out_error={exc}")


def _ts_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt + pd.Timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    run_backtest()
