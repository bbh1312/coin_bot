from __future__ import annotations

import argparse
import csv
import os
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

import ccxt
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engines.backtest_common import calc_warmup_window, load_common_universe
from engines.sr_pro_common import build_sr_zones
from types import SimpleNamespace


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
        if not os.path.exists(cached_path):
            # allow common_ohlcv_cache files with limit suffix; prefer freshest large file
            try:
                prefix = f"{fname}_{timeframe}_"
                candidates = []
                tf_ms = _tf_to_minutes(timeframe) * 60 * 1000
                for name in os.listdir(common_warmup_dir):
                    if not name.startswith(prefix) or not name.endswith(".csv"):
                        continue
                    full = os.path.join(common_warmup_dir, name)
                    try:
                        mtime = os.path.getmtime(full)
                        lim = int(name[len(prefix):-4])
                        last_ts = 0
                        with open(full, "rb") as fh:
                            try:
                                fh.seek(-2, os.SEEK_END)
                                while fh.read(1) != b"\n":
                                    fh.seek(-2, os.SEEK_CUR)
                            except Exception:
                                fh.seek(0)
                            last_line = fh.readline().decode("utf-8").strip()
                        if last_line:
                            try:
                                last_ts = int(float(last_line.split(",")[0]))
                            except Exception:
                                last_ts = 0
                    except Exception:
                        continue
                    candidates.append((lim, last_ts, mtime, full))
                if candidates:
                    fresh = [c for c in candidates if c[1] and c[1] >= (end_ms - max(tf_ms * 2, 1))]
                    if fresh:
                        fresh.sort(key=lambda x: (x[0], x[1]), reverse=True)
                        cached_path = fresh[0][3]
                    else:
                        candidates.sort(key=lambda x: x[2], reverse=True)
                        cached_path = candidates[0][3]
            except Exception:
                pass
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


def _pivot_high(series: pd.Series, left: int, right: int, idx: int):
    if idx - left < 0 or idx + right >= len(series):
        return None
    window = series.iloc[idx - left : idx + right + 1]
    val = series.iloc[idx]
    if float(val) == float(window.max()):
        return float(val)
    return None


def _pivot_low(series: pd.Series, left: int, right: int, idx: int):
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


def _parse_floats(csv_list: str) -> List[float]:
    return [float(x.strip()) for x in csv_list.split(",") if x.strip()]


def _z_get(z, key: str, default=None):
    if isinstance(z, dict):
        return z.get(key, default)
    return getattr(z, key, default)


def _z_set(z, key: str, val) -> None:
    if isinstance(z, dict):
        z[key] = val
    else:
        setattr(z, key, val)


def run_sweep() -> None:
    parser = argparse.ArgumentParser("sr_pro_short_v1 sweep")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--rolling-zones", action="store_true")
    parser.add_argument("--total-window-days", type=int, default=0)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--relaxed-lookback", type=int, default=10)
    parser.add_argument("--auto-relax", action="store_true")
    parser.add_argument("--atr-mult", type=float, default=1.0)
    parser.add_argument("--delta-len", type=int, default=2)
    parser.add_argument("--cluster-atr", type=float, default=1.5)
    parser.add_argument("--max-zones-per-side", type=int, default=8)
    parser.add_argument("--touch-mode-list", type=str, default="bot,mid")
    parser.add_argument("--touch-use-close", action="store_true")
    parser.add_argument("--dvf-norm-max-list", type=str, default="0.0,0.2,0.4")
    parser.add_argument("--require-reject-close", action="store_true")
    parser.add_argument("--reject-mode", type=str, default="bot", choices=["bot", "mid"])
    parser.add_argument("--ema200-filter", action="store_true")
    parser.add_argument("--retest-bars", type=int, default=3)
    parser.add_argument("--retest-atr-mult", type=float, default=0.1)
    parser.add_argument("--sl-buffer-list", type=str, default="0.006,0.008,0.01")
    parser.add_argument("--tp-mult-list", type=str, default="0.976,0.97,0.965")
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--min-winrate", type=float, default=0.0)
    args = parser.parse_args()

    touch_modes = [x.strip() for x in args.touch_mode_list.split(",") if x.strip()]
    dvf_norm_vals = _parse_floats(args.dvf_norm_max_list)
    sl_vals = _parse_floats(args.sl_buffer_list)
    tp_vals = _parse_floats(args.tp_mult_list)

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {"3m": 120, "15m": 120, "1h": 120}
    if args.total_window_days:
        total_window_days = args.total_window_days
        start_ms = end_ms - int((total_window_days + args.days) * 24 * 60 * 60 * 1000)
        eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    else:
        start_ms, eval_start_ms, _, _ = calc_warmup_window(args.days, end_ms, min_bars)
    if args.rolling_zones and not args.total_window_days:
        print("[SWEEP] rolling_zones requires total_window_days")
        return

    use_common = bool(args.common_warmup_dir) or args.common_only or args.cache_only
    common_dir = args.common_warmup_dir
    if not common_dir:
        live_cache_dir = os.path.join("logs", "common_ohlcv_cache")
        if os.path.isdir(live_cache_dir) and os.listdir(live_cache_dir):
            common_dir = live_cache_dir
        else:
            common_dir = os.path.join("logs", "common_warmup", "ohlcv")
    universe = load_common_universe(args.universe, exchange, args.cache_only, top_n=args.top_n)
    if not universe:
        print("[SWEEP] no_universe")
        return

    data: Dict[str, Dict[str, pd.DataFrame]] = {}
    for sym in universe:
        rows_3m = _fetch_ohlcv_all(
            exchange, sym, "3m", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        rows_15m = _fetch_ohlcv_all(
            exchange, sym, "15m", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        rows_1h = _fetch_ohlcv_all(
            exchange, sym, "1h", start_ms, end_ms,
            cache_only=args.cache_only, use_common_warmup=use_common,
            common_warmup_dir=common_dir, common_only=args.common_only,
        )
        if rows_3m and rows_15m and rows_1h:
            data[sym] = {
                "3m": pd.DataFrame(rows_3m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "15m": pd.DataFrame(rows_15m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "1h": pd.DataFrame(rows_1h, columns=["ts", "open", "high", "low", "close", "volume"]),
            }

    if not data:
        print("[SWEEP] no_data")
        return

    results = []
    zones_cache: Dict[str, Dict[int, List[Zone]]] = {}

    for touch_mode in touch_modes:
        for dvf_norm_max in dvf_norm_vals:
            for sl_buffer in sl_vals:
                for tp_mult in tp_vals:
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
                    for sym, frames in data.items():
                        if args.rolling_zones:
                            zones_cache.setdefault(sym, {})
                        df_3m = frames["3m"].iloc[:-1] if args.use_confirmed else frames["3m"]
                        df_15m = frames["15m"].iloc[:-1] if args.use_confirmed else frames["15m"]
                        df_1h = frames["1h"]
                        if len(df_3m) < 10 or len(df_15m) < 5 or len(df_1h) < (args.lookback * 2 + 5):
                            continue

                        close_1h = df_1h["close"].astype(float)
                        open_1h = df_1h["open"].astype(float)
                        high_1h = df_1h["high"].astype(float)
                        low_1h = df_1h["low"].astype(float)
                        volume_1h = df_1h["volume"].astype(float)
                        atr_1h = _atr(df_1h, 14)
                        ema200_1h = _ema(close_1h, 200)
                        dv = np.where(close_1h > open_1h, volume_1h, np.where(close_1h < open_1h, -volume_1h, 0.0))
                        dv = pd.Series(dv, index=df_1h.index)
                        dvf = _ema(dv, args.delta_len)
                        vol_ema = _ema(volume_1h, args.delta_len)
                        atr_3m = _atr(df_3m, 14)

                        zones: List[Zone] = []
                        zones_ts = -1
                        window_bars_1h = int(args.total_window_days * 24) if args.rolling_zones else 0
                        zone_cfg = SimpleNamespace(
                            lookback=args.lookback,
                            relaxed_lookback=args.relaxed_lookback,
                            auto_relax=args.auto_relax,
                            atr_mult=args.atr_mult,
                            delta_len=args.delta_len,
                            cluster_atr=args.cluster_atr,
                            max_zones_per_side=args.max_zones_per_side,
                        )
                        ts_1h = df_1h["ts"].values
                        ts_15m = df_15m["ts"].values
                        ts_3m = df_3m["ts"].values
                        trade = None
                        retest_active = False
                        retest_level = 0.0
                        retest_until = -1

                        for i3 in range(3, len(df_3m) - 1):
                            ts = int(ts_3m[i3])
                            if ts < eval_start_ms:
                                continue

                            idx_1h = int(np.searchsorted(ts_1h, ts, side="right") - 1)
                            if idx_1h < 0:
                                continue

                            if args.rolling_zones:
                                if zones_ts != int(ts_1h[idx_1h]):
                                    cached = zones_cache[sym].get(idx_1h)
                                    if cached is None:
                                        cached = build_sr_zones(df_1h.iloc[:idx_1h + 1], zone_cfg, window_bars=window_bars_1h)
                                        zones_cache[sym][idx_1h] = cached
                                    zones = deepcopy(cached)
                                    zones_ts = int(ts_1h[idx_1h])
                            else:
                                for i1 in range(max(0, idx_1h - 1), idx_1h + 1):
                                    lb = args.lookback
                                    ph = _pivot_high(high_1h, lb, lb, i1 - lb) if i1 - lb >= 0 else None
                                    pl = _pivot_low(low_1h, lb, lb, i1 - lb) if i1 - lb >= 0 else None
                                    lb_used = lb
                                    if args.auto_relax and ph is None and pl is None:
                                        lb2 = args.relaxed_lookback
                                        ph = _pivot_high(high_1h, lb2, lb2, i1 - lb2) if i1 - lb2 >= 0 else None
                                        pl = _pivot_low(low_1h, lb2, lb2, i1 - lb2) if i1 - lb2 >= 0 else None
                                        lb_used = lb2
                                    pivot_bar = i1 - lb_used
                                    if pivot_bar < 0:
                                        continue

                                    half_w = float(atr_1h.iloc[i1]) * args.atr_mult * 0.5
                                    cluster_dist = float(atr_1h.iloc[i1]) * args.cluster_atr

                                    def merge_or_create(side: int, level: float, vol_val: float) -> None:
                                        merged = False
                                        for z in zones:
                                            if z.live and z.side == side and abs(z.mid - level) <= cluster_dist:
                                                new_mid = (z.mid + level) * 0.5
                                                z.mid = new_mid
                                                z.top = new_mid + half_w
                                                z.bot = new_mid - half_w
                                                z.vol = (z.vol + vol_val) * 0.5
                                                merged = True
                                                break
                                        if not merged:
                                            zones.append(
                                                Zone(
                                                    mid=level,
                                                    top=level + half_w,
                                                    bot=level - half_w,
                                                    side=side,
                                                    live=True,
                                                    born=i1,
                                                    start=pivot_bar,
                                                    vol=vol_val,
                                                )
                                            )

                                    if ph is not None:
                                        merge_or_create(1, ph, float(dvf.iloc[pivot_bar]))
                                    if pl is not None:
                                        merge_or_create(-1, pl, float(dvf.iloc[pivot_bar]))

                                for side in (1, -1):
                                    side_z = [z for z in zones if z.side == side]
                                    if len(side_z) > args.max_zones_per_side:
                                        oldest = min(side_z, key=lambda z: z.born)
                                        zones.remove(oldest)

                            close_1h_now = float(close_1h.iloc[idx_1h])
                            for z in zones:
                                if _z_get(z, "live", True) and _z_get(z, "side") == 1 and close_1h_now > float(_z_get(z, "top", 0)):
                                    _z_set(z, "live", False)
                                if _z_get(z, "live", True) and _z_get(z, "side") == -1 and close_1h_now < float(_z_get(z, "bot", 0)):
                                    _z_set(z, "live", False)

                            if trade:
                                high_i = float(df_3m.at[i3, "high"])
                                low_i = float(df_3m.at[i3, "low"])
                                trade["hold_bars"] += 1
                                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low_i) / trade["entry_px"]))
                                trade["mae"] = max(trade["mae"], max(0.0, (high_i - trade["entry_px"]) / trade["entry_px"]))
                                if high_i >= trade["sl_price"]:
                                    pnl_pct = (trade["entry_px"] - trade["sl_price"]) / trade["entry_px"]
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
                                    pnl_pct = (trade["entry_px"] - trade["tp_price"]) / trade["entry_px"]
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

                            h1_high = float(high_1h.iloc[idx_1h])
                            h1_low = float(low_1h.iloc[idx_1h])
                            h1_close = float(close_1h.iloc[idx_1h])
                            h1_touch_px = h1_close if args.touch_use_close else h1_high
                            if args.ema200_filter:
                                ema200_now = float(ema200_1h.iloc[idx_1h])
                                if h1_close >= ema200_now:
                                    continue
                            dvf_norm = (
                                float(dvf.iloc[idx_1h]) / float(vol_ema.iloc[idx_1h])
                                if float(vol_ema.iloc[idx_1h]) > 0
                                else 0.0
                            )
                            resist_candidates = [
                                z
                                for z in zones
                                if _z_get(z, "live", True)
                                and _z_get(z, "side") == 1
                                and dvf_norm <= dvf_norm_max
                                and h1_touch_px >= (_z_get(z, "mid") if touch_mode == "mid" else _z_get(z, "bot"))
                                and h1_low <= float(_z_get(z, "top", 0))
                            ]
                            if resist_candidates and args.require_reject_close:
                                reject_level = "mid" if args.reject_mode == "mid" else "bot"
                                resist_candidates = [
                                    z
                                    for z in resist_candidates
                                    if h1_close < (_z_get(z, "mid") if reject_level == "mid" else _z_get(z, "bot"))
                                ]
                            if not resist_candidates:
                                continue

                            idx_15m = int(np.searchsorted(ts_15m, ts, side="right") - 1)
                            if idx_15m < 2:
                                continue
                            if float(df_15m.at[idx_15m, "high"]) >= float(df_15m.at[idx_15m - 1, "high"]):
                                continue

                            close_now = float(df_3m.at[i3, "close"])
                            low_prev = [
                                float(df_3m.at[i3 - 1, "low"]),
                                float(df_3m.at[i3 - 2, "low"]),
                                float(df_3m.at[i3 - 3, "low"]),
                            ]
                            if close_now >= min(low_prev):
                                continue

                            retest_level = min(low_prev)
                            retest_active = True
                            retest_until = i3 + max(1, int(args.retest_bars))

                            if retest_active and i3 <= retest_until:
                                high_now = float(df_3m.at[i3, "high"])
                                atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                                if high_now >= retest_level - (atr_now * float(args.retest_atr_mult)) and close_now < retest_level:
                                    entry_px = float(df_3m.at[i3 + 1, "open"])
                                    nearest = min(resist_candidates, key=lambda z: abs(_z_get(z, "mid") - entry_px))
                                    sl_raw = float(_z_get(nearest, "top")) * (1.0 + sl_buffer)
                                    sl_price = max(sl_raw, entry_px * 1.002)
                                    tp_price = entry_px * tp_mult
                                    trade = {
                                        "entry_px": entry_px,
                                        "sl_price": sl_price,
                                        "tp_price": tp_price,
                                        "mfe": 0.0,
                                        "mae": 0.0,
                                        "hold_bars": 0,
                                    }
                                    stats["entries"] += 1
                                    retest_active = False
                                if i3 >= retest_until:
                                    retest_active = False

                    trades = int(stats.get("trades", 0))
                    wins = int(stats.get("wins", 0))
                    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
                    if trades < args.min_trades:
                        continue
                    if winrate < args.min_winrate:
                        continue
                    results.append(
                        {
                            "winrate": winrate,
                            "trades": trades,
                            "net_sum": stats.get("net_sum", 0.0),
                            "params": {
                                "touch_mode": touch_mode,
                                "dvf_norm_max": dvf_norm_max,
                                "sl_buffer": sl_buffer,
                                "tp_mult": tp_mult,
                            },
                        }
                    )

    results.sort(key=lambda x: (x["winrate"], x["trades"], x["net_sum"]), reverse=True)
    print("[SWEEP] TOP10")
    for row in results[:10]:
        params = " ".join([f"{k}={v}" for k, v in row["params"].items()])
        print(
            f"[SWEEP] winrate={row['winrate']:.2f}% trades={row['trades']} net_sum={row['net_sum']:.3f} {params}"
        )


if __name__ == "__main__":
    run_sweep()
