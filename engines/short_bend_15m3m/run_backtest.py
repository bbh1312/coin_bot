from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import ccxt
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engines.backtest_common import load_common_universe, log_warmup_info
from engines.short_bend_15m3m.engine import ShortBend15m3mConfig


def _read_cached_csv(path: str) -> List[List[float]]:
    rows: List[List[float]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            _ = next(reader, None)
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
    common_warmup_dir: str | None = None,
    common_only: bool = False,
) -> List[List[float]]:
    if common_warmup_dir:
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
        last = int(batch[-1][0])
        if last == since:
            break
        since = last + 1
    return [r for r in out if start_ms <= r[0] <= end_ms]


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=max(int(length), 1), adjust=False).mean()


def _new_stats() -> Dict[str, float]:
    return {
        "entries": 0,
        "exits": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
        "tp_sum": 0.0,
        "sl_sum": 0.0,
        "net_sum": 0.0,
        "net_sum_usdt": 0.0,
    }


def _minute_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M")


def _dow_label(dt: datetime) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]


def _fmt_summary_line(
    symbol: Optional[str],
    stats: Dict[str, float],
    base_usdt: float,
    last_day_exits: int,
    entry_syms: int,
) -> str:
    trades = int(stats.get("trades", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    entries = int(stats.get("entries", 0))
    exits = int(stats.get("exits", 0))
    tp = int(stats.get("tp", wins))
    sl = int(stats.get("sl", losses))
    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats.get("mfe_sum", 0.0) / trades if trades > 0 else 0.0
    avg_mae = stats.get("mae_sum", 0.0) / trades if trades > 0 else 0.0
    avg_hold = stats.get("hold_sum", 0.0) / trades if trades > 0 else 0.0
    tag = "TOTAL" if symbol is None else symbol
    return (
        f"[BACKTEST] {tag} entries={entries} exits={exits} trades={trades} "
        f"wins={wins} losses={losses} winrate={winrate:.2f}% "
        f"tp={tp} sl={sl} avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} "
        f"last_day_exits={last_day_exits} "
        f"base_usdt={base_usdt:.2f} tp_sum={stats.get('tp_sum', 0.0):.3f} "
        f"sl_sum={stats.get('sl_sum', 0.0):.3f} net_sum={stats.get('net_sum', 0.0):.3f} "
        f"net_sum_usdt={stats.get('net_sum_usdt', 0.0):.3f} entry_syms={entry_syms}"
    )


def run_backtest() -> None:
    cfg = ShortBend15m3mConfig()
    parser = argparse.ArgumentParser("short_bend_15m3m backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--exclude-symbols", type=str, default="")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--use-confirmed", action="store_true", default=cfg.use_confirmed)

    parser.add_argument("--lookback-15m", type=int, default=cfg.lookback_15m)
    parser.add_argument("--rise-min-pct", type=float, default=cfg.rise_min_pct)
    parser.add_argument("--hh-ratio-min", type=float, default=cfg.hh_ratio_min)
    parser.add_argument("--htf-top-zone-ratio", type=float, default=cfg.htf_top_zone_ratio)
    parser.add_argument("--ema-fast-len", type=int, default=cfg.ema_fast_len)
    parser.add_argument("--ema-mid-len", type=int, default=cfg.ema_mid_len)
    parser.add_argument("--ema-slow-len", type=int, default=cfg.ema_slow_len)
    parser.add_argument("--htf-require-vol-confirm", action="store_true", default=cfg.htf_require_vol_confirm)
    parser.add_argument("--no-htf-require-vol-confirm", action="store_false", dest="htf_require_vol_confirm")
    parser.add_argument("--htf-vol-mult-min", type=float, default=cfg.htf_vol_mult_min)
    parser.add_argument("--htf-vol-spike-lookback", type=int, default=cfg.htf_vol_spike_lookback)
    parser.add_argument("--htf-vol-spike-mult", type=float, default=cfg.htf_vol_spike_mult)
    parser.add_argument("--armed-bars-3m", type=int, default=cfg.armed_bars_3m)
    parser.add_argument("--ltf-ema-len", type=int, default=cfg.ltf_ema_len)
    parser.add_argument("--swing-lookback-3m", type=int, default=cfg.swing_lookback_3m)
    parser.add_argument("--require-vol-confirm", action="store_true", default=cfg.require_vol_confirm)
    parser.add_argument("--vol-mult-min", type=float, default=cfg.vol_mult_min)
    parser.add_argument("--ltf-wait-counter-momo", action="store_true", default=cfg.ltf_wait_counter_momo)
    parser.add_argument("--no-ltf-wait-counter-momo", action="store_false", dest="ltf_wait_counter_momo")
    parser.add_argument("--ltf-counter-momo-bars", type=int, default=cfg.ltf_counter_momo_bars)

    parser.add_argument("--sl-min-pct", type=float, default=cfg.sl_min_pct)
    parser.add_argument("--sl-max-pct", type=float, default=cfg.sl_max_pct)
    parser.add_argument("--tp-min-pct", type=float, default=cfg.tp_min_pct)
    parser.add_argument("--tp-max-pct", type=float, default=cfg.tp_max_pct)
    parser.add_argument("--rr-min", type=float, default=cfg.rr_min)
    parser.add_argument("--bend-sl-buffer-pct", type=float, default=cfg.bend_sl_buffer_pct)
    parser.add_argument("--sl-first", action="store_true", default=cfg.sl_first)

    parser.add_argument("--base-usdt", type=float, default=10.0)
    parser.add_argument("--log-gates", action="store_true")
    args = parser.parse_args()

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    warmup_days = 7
    start_ms = end_ms - int((int(args.days) + warmup_days) * 24 * 60 * 60 * 1000)
    eval_start_ms = end_ms - int(int(args.days) * 24 * 60 * 60 * 1000)
    log_warmup_info(lambda _x: None, warmup_days, warmup_days * 1440, int(args.days))

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    common_dir = args.common_warmup_dir or os.getenv(
        "COMMON_WARMUP_CACHE_DIR", os.path.join("logs", "common_warmup", "ohlcv")
    )
    use_common = bool(args.common_only or args.cache_only or args.common_warmup_dir)
    universe = load_common_universe(args.universe, exchange, args.cache_only, top_n=args.top_n)
    if args.exclude_symbols:
        raw = args.exclude_symbols.replace(" ", "").replace(";", ",").replace("|", ",")
        banned = {s for s in raw.split(",") if s}
        universe = [s for s in universe if s not in banned]
    if not universe:
        print("[BACKTEST] no_universe")
        return

    gates = {
        "no_data": 0,
        "uptrend_fail": 0,
        "rise_fail": 0,
        "ema_stack_fail": 0,
        "hh_fail": 0,
        "top_zone_fail": 0,
        "htf_vol_fail": 0,
        "htf_vol_spike_fail": 0,
        "bend_fail": 0,
        "armed": 0,
        "confirm_fail": 0,
        "counter_momo_fail": 0,
        "counter_momo_hit": 0,
        "entry_hit": 0,
        "tp_hit": 0,
        "sl_hit": 0,
        "open_left": 0,
    }

    stats = _new_stats()
    per_symbol: Dict[str, Dict[str, float]] = {}
    exit_logs: List[dict] = []
    open_logs: List[dict] = []
    entry_symbols: set[str] = set()
    hour_stats: Dict[int, Dict[str, int]] = {}
    dow_stats: Dict[str, Dict[str, int]] = {}
    date_stats: Dict[str, Dict[str, float]] = {}

    for sym in universe:
        rows15 = _fetch_ohlcv_all(
            exchange,
            sym,
            "15m",
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            common_warmup_dir=common_dir if use_common else None,
            common_only=args.common_only,
        )
        rows3 = _fetch_ohlcv_all(
            exchange,
            sym,
            "3m",
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            common_warmup_dir=common_dir if use_common else None,
            common_only=args.common_only,
        )
        if not rows15 or not rows3:
            gates["no_data"] += 1
            continue

        d15 = pd.DataFrame(rows15, columns=["ts", "open", "high", "low", "close", "volume"])
        d3 = pd.DataFrame(rows3, columns=["ts", "open", "high", "low", "close", "volume"])
        d15 = d15.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        d3 = d3.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)

        if args.use_confirmed and len(d15) > 0:
            d15 = d15.iloc[:-1].reset_index(drop=True)
        if args.use_confirmed and len(d3) > 0:
            d3 = d3.iloc[:-1].reset_index(drop=True)
        if len(d15) < max(int(args.lookback_15m) + 2, int(args.ema_slow_len) + 2) or len(d3) < 200:
            gates["no_data"] += 1
            continue

        d15["ema_fast"] = _ema(d15["close"].astype(float), int(args.ema_fast_len))
        d15["ema_mid"] = _ema(d15["close"].astype(float), int(args.ema_mid_len))
        d15["ema_slow"] = _ema(d15["close"].astype(float), int(args.ema_slow_len))
        d15["vol_sma20"] = d15["volume"].astype(float).rolling(20, min_periods=1).mean()

        d3["ema_ltf"] = _ema(d3["close"].astype(float), int(args.ltf_ema_len))
        d3["vol_sma20"] = d3["volume"].astype(float).rolling(20, min_periods=1).mean()
        d3["swing_low_prev"] = d3["low"].astype(float).rolling(int(args.swing_lookback_3m), min_periods=2).min().shift(1)

        sym_stats = per_symbol.setdefault(sym, _new_stats())
        next_eligible_ts = eval_start_ms
        open_left = False

        for i in range(max(int(args.lookback_15m), int(args.ema_slow_len)) + 1, len(d15) - 1):
            ts15 = int(d15.at[i, "ts"])
            if ts15 < eval_start_ms or ts15 < next_eligible_ts:
                continue

            w0 = i - int(args.lookback_15m)
            w1 = i  # 이전 구간(꺾임 직전)까지
            highs = d15["high"].iloc[w0:w1].astype(float)
            lows = d15["low"].iloc[w0:w1].astype(float)
            if len(highs) < int(args.lookback_15m) - 1:
                continue

            min_low = max(float(lows.min()), 1e-12)
            rise_pct = (float(highs.max()) - min_low) / min_low * 100.0
            if rise_pct < float(args.rise_min_pct):
                gates["rise_fail"] += 1
                continue

            ema_fast = float(d15.at[i - 1, "ema_fast"])
            ema_mid = float(d15.at[i - 1, "ema_mid"])
            ema_slow = float(d15.at[i - 1, "ema_slow"])
            if not (ema_fast > ema_mid > ema_slow):
                gates["ema_stack_fail"] += 1
                continue

            hh_ratio = float((highs.diff() > 0).sum()) / max(len(highs) - 1, 1)
            if hh_ratio < float(args.hh_ratio_min):
                gates["hh_fail"] += 1
                continue
            # 15m 고점권 체크: 직전 종가가 롤링 구간 상단 영역에 있어야 함.
            rolling_high = float(highs.max())
            rolling_range = max(rolling_high - min_low, 1e-12)
            top_ratio = max(min(float(args.htf_top_zone_ratio), 0.9), 0.05)
            top_zone_floor = rolling_high - (rolling_range * top_ratio)
            if float(d15.at[i - 1, "close"]) < top_zone_floor:
                gates["top_zone_fail"] += 1
                continue

            prev_high = float(d15.at[i - 1, "high"])
            prev_close = float(d15.at[i - 1, "close"])
            now_high = float(d15.at[i, "high"])
            now_close = float(d15.at[i, "close"])
            bend = now_high < prev_high and (now_close < prev_close)
            if not bend:
                gates["bend_fail"] += 1
                continue
            if bool(args.htf_require_vol_confirm):
                now_vol = float(d15.at[i, "volume"])
                vol_sma = max(float(d15.at[i, "vol_sma20"]), 1e-12)
                if now_vol < (vol_sma * float(args.htf_vol_mult_min)):
                    gates["htf_vol_fail"] += 1
                    continue
                lb = max(int(args.htf_vol_spike_lookback), 2)
                v0 = max(0, i - lb)
                prev_vol_max = float(d15["volume"].iloc[v0:i].max()) if i > v0 else 0.0
                if prev_vol_max > 0 and now_vol < (prev_vol_max * float(args.htf_vol_spike_mult)):
                    gates["htf_vol_spike_fail"] += 1
                    continue

            gates["armed"] += 1
            tf15_ms = 15 * 60 * 1000
            tf3_ms = 3 * 60 * 1000
            arm_start_ts = ts15 + (tf15_ms if bool(args.use_confirmed) else 0)
            arm_end_ts = arm_start_ts + int(args.armed_bars_3m) * tf3_ms
            c3 = d3[(d3["ts"] > arm_start_ts) & (d3["ts"] <= arm_end_ts)]
            if c3.empty:
                gates["confirm_fail"] += 1
                continue

            entry_j = -1
            for j in c3.index:
                c = float(d3.at[j, "close"])
                ema_ltf = float(d3.at[j, "ema_ltf"])
                sw = d3.at[j, "swing_low_prev"]
                if not np.isfinite(sw):
                    continue
                sw_low = float(sw)
                vol_ok = True
                if bool(args.require_vol_confirm):
                    vol_ok = float(d3.at[j, "volume"]) >= float(d3.at[j, "vol_sma20"]) * float(args.vol_mult_min)
                if (c < ema_ltf) and (c < sw_low) and vol_ok:
                    entry_j = int(j)
                    break

            if entry_j < 0:
                gates["confirm_fail"] += 1
                continue

            entry_ref_j = entry_j
            if bool(args.ltf_wait_counter_momo):
                max_wait = max(int(args.ltf_counter_momo_bars), 1)
                end_wait_j = min(entry_j + max_wait, len(d3) - 2)
                counter_j = -1
                for j2 in range(entry_j, end_wait_j + 1):
                    c2 = float(d3.at[j2, "close"])
                    o2 = float(d3.at[j2, "open"])
                    ema2 = float(d3.at[j2, "ema_ltf"])
                    prev_h = float(d3.at[j2 - 1, "high"]) if j2 > 0 else float(d3.at[j2, "high"])
                    h2 = float(d3.at[j2, "high"])
                    # 역모멘텀(상방 반등) 캔들: 양봉 + EMA 위 안착 + 직전 고점 상향
                    if (c2 > o2) and (c2 > ema2) and (h2 > prev_h):
                        counter_j = j2
                        break
                if counter_j < 0:
                    gates["counter_momo_fail"] += 1
                    continue
                gates["counter_momo_hit"] += 1
                entry_ref_j = counter_j

            if (entry_ref_j + 1) >= len(d3):
                gates["confirm_fail"] += 1
                continue
            entry_i = entry_ref_j + 1
            entry_ts = int(d3.at[entry_i, "ts"])
            entry_px = float(d3.at[entry_i, "open"])
            bend_high = float(now_high)
            sl_by_bend = bend_high * (1.0 + float(args.bend_sl_buffer_pct))
            sl_by_floor = entry_px * (1.0 + float(args.sl_min_pct))
            sl = max(sl_by_bend, sl_by_floor)
            risk = max(sl - entry_px, entry_px * 0.001)
            tp_rr = entry_px - risk * float(args.rr_min)
            tp_floor = entry_px * (1.0 - float(args.tp_min_pct))
            tp = min(tp_rr, tp_floor)
            # clamp to avoid excessively wide targets/stops
            if float(args.sl_max_pct) > 0.0:
                sl_cap = entry_px * (1.0 + abs(float(args.sl_max_pct)))
                sl = min(sl, sl_cap)
            if float(args.tp_max_pct) > 0.0:
                tp_cap = entry_px * (1.0 - abs(float(args.tp_max_pct)))
                tp = max(tp, tp_cap)

            stats["entries"] += 1
            sym_stats["entries"] += 1
            gates["entry_hit"] += 1
            entry_symbols.add(sym)
            dt_kst = datetime.fromtimestamp(entry_ts / 1000.0, tz=timezone.utc) + timedelta(hours=9)
            hour_bucket = dt_kst.hour
            dow_bucket = _dow_label(dt_kst)
            day_bucket = dt_kst.strftime("%Y-%m-%d")
            hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
            dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
            date_stats.setdefault(day_bucket, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
            hour_stats[hour_bucket]["entries"] += 1
            dow_stats[dow_bucket]["entries"] += 1
            date_stats[day_bucket]["entries"] += 1

            exited = False
            for k in range(entry_i, len(d3)):
                ts3 = int(d3.at[k, "ts"])
                h = float(d3.at[k, "high"])
                l = float(d3.at[k, "low"])
                hit_sl = h >= sl
                hit_tp = l <= tp
                if not (hit_sl or hit_tp):
                    continue
                if hit_sl and hit_tp:
                    if bool(args.sl_first):
                        hit_tp = False
                    else:
                        hit_sl = False
                reason = "SL" if hit_sl else "TP"
                exit_px = sl if hit_sl else tp
                pnl = (entry_px - exit_px) / max(entry_px, 1e-12)

                hold_bars = max(k - entry_i, 0)
                hold_minutes = hold_bars * 3.0
                span = d3.iloc[entry_i : k + 1]
                mfe = (entry_px - float(span["low"].min())) / max(entry_px, 1e-12)
                mae = (float(span["high"].max()) - entry_px) / max(entry_px, 1e-12)

                for bucket in (stats, sym_stats):
                    bucket["exits"] += 1
                    bucket["trades"] += 1
                    bucket["mfe_sum"] += max(mfe, 0.0)
                    bucket["mae_sum"] += max(mae, 0.0)
                    bucket["hold_sum"] += hold_minutes
                    bucket["net_sum"] += pnl
                    bucket["net_sum_usdt"] += pnl * float(args.base_usdt)
                    if reason == "TP":
                        bucket["wins"] += 1
                        bucket["tp"] += 1
                        bucket["tp_sum"] += abs(pnl)
                    else:
                        bucket["losses"] += 1
                        bucket["sl"] += 1
                        bucket["sl_sum"] += abs(pnl)

                if reason == "TP":
                    gates["tp_hit"] += 1
                else:
                    gates["sl_hit"] += 1

                exit_logs.append(
                    {
                        "sym": sym,
                        "mode": "short_bend_15m3m",
                        "side": "SHORT",
                        "entry_ts": entry_ts,
                        "exit_ts": ts3,
                        "entry_px": entry_px,
                        "exit_px": exit_px,
                        "reason": reason,
                        "tp_pct": abs(tp - entry_px) / max(entry_px, 1e-12) * 100.0,
                        "sl_pct": abs(sl - entry_px) / max(entry_px, 1e-12) * 100.0,
                        "pnl_pct": pnl,
                    }
                )
                next_eligible_ts = ts3
                exited = True
                break

            if not exited:
                open_left = True
                gates["open_left"] += 1
                last_px = float(d3.iloc[-1]["close"]) if len(d3) > 0 else entry_px
                last_ts = int(d3.iloc[-1]["ts"]) if len(d3) > 0 else entry_ts
                unrealized = (entry_px - last_px) / max(entry_px, 1e-12) * 100.0
                open_logs.append(
                    {
                        "sym": sym,
                        "mode": "short_bend_15m3m",
                        "side": "SHORT",
                        "entry_ts": entry_ts,
                        "entry_px": entry_px,
                        "last_px": last_px,
                        "last_ts": last_ts,
                        "unrealized_pct": unrealized,
                    }
                )
                break

        if open_left:
            continue

    for ex in exit_logs:
        dt_kst = datetime.fromtimestamp(ex["entry_ts"] / 1000.0, tz=timezone.utc) + timedelta(hours=9)
        hour_bucket = dt_kst.hour
        dow_bucket = _dow_label(dt_kst)
        day_bucket = dt_kst.strftime("%Y-%m-%d")
        hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
        dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
        date_stats.setdefault(day_bucket, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
        date_stats[day_bucket]["net_sum"] += float(ex.get("pnl_pct", 0.0))
        date_stats[day_bucket]["net_sum_usdt"] += float(ex.get("pnl_pct", 0.0)) * float(args.base_usdt)
        if ex["reason"] == "TP":
            hour_stats[hour_bucket]["tp"] += 1
            dow_stats[dow_bucket]["tp"] += 1
            date_stats[day_bucket]["tp"] += 1
        elif ex["reason"] == "SL":
            hour_stats[hour_bucket]["sl"] += 1
            dow_stats[dow_bucket]["sl"] += 1
            date_stats[day_bucket]["sl"] += 1

    last_day_threshold = end_ms - (24 * 60 * 60 * 1000)

    for sym, sym_stats in sorted(per_symbol.items(), key=lambda x: float(x[1].get("net_sum_usdt", 0.0)), reverse=True):
        if not (sym_stats.get("entries", 0) or sym_stats.get("trades", 0)):
            continue
        sym_last_day_exits = sum(1 for ex in exit_logs if ex["sym"] == sym and ex["exit_ts"] >= last_day_threshold)
        print(
            _fmt_summary_line(
                sym,
                sym_stats,
                float(args.base_usdt),
                sym_last_day_exits,
                1 if sym_stats.get("entries", 0) > 0 else 0,
            )
        )
        sym_items: List[dict] = []
        sym_items.extend([ex for ex in exit_logs if ex["sym"] == sym])
        sym_items.extend([op for op in open_logs if op["sym"] == sym])
        sym_items.sort(key=lambda x: x["entry_ts"], reverse=True)
        for item in sym_items:
            if "exit_ts" in item:
                result = "WIN" if item.get("reason") == "TP" else "LOSS" if item.get("reason") == "SL" else "OTHER"
                print(
                    "[BACKTEST][EXIT] "
                    f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                    f"entry_dt={_minute_str(item['entry_ts'])} exit_dt={_minute_str(item['exit_ts'])} "
                    f"entry_px={item['entry_px']:.6f} exit_px={item['exit_px']:.6f} "
                    f"reason={item['reason']} result={result} "
                    f"tp_pct={float(item.get('tp_pct', 0.0)):.2f} sl_pct={float(item.get('sl_pct', 0.0)):.2f}"
                )
            else:
                print(
                    "[BACKTEST][OPEN] "
                    f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                    f"entry_dt={_minute_str(item['entry_ts'])} exit_dt= "
                    f"entry_px={item['entry_px']:.6f} last_px={item['last_px']:.6f} "
                    f"last_dt={_minute_str(item['last_ts'])} unrealized_pct={item['unrealized_pct']:.2f}%"
                )

    total_last_day_exits = sum(1 for ex in exit_logs if ex["exit_ts"] >= last_day_threshold)
    print(
        _fmt_summary_line(
            None,
            stats,
            float(args.base_usdt),
            total_last_day_exits,
            len(entry_symbols),
        )
    )
    if args.log_gates:
        print(f"[BACKTEST] GATES {gates}")

    print("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for hour in range(24):
        bucket = hour_stats.get(hour, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl_cnt = bucket["sl"]
        sl_rate = (sl_cnt / entries * 100.0) if entries > 0 else 0.0
        print(f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={bucket['tp']} sl={sl_cnt} sl_rate={sl_rate:.2f}%")

    print("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        bucket = dow_stats.get(dow, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl_cnt = bucket["sl"]
        sl_rate = (sl_cnt / entries * 100.0) if entries > 0 else 0.0
        print(f"[BACKTEST] DOW {dow} entries={entries} tp={bucket['tp']} sl={sl_cnt} sl_rate={sl_rate:.2f}%")

    if date_stats:
        print("[BACKTEST] BY_DATE(KST) date entries tp sl sl_rate winrate net_sum net_sum_usdt")
        total_entries = 0
        total_tp = 0
        total_sl = 0
        total_net_sum = 0.0
        total_net_sum_usdt = 0.0
        for day_key in sorted(date_stats.keys()):
            bucket = date_stats[day_key]
            entries = int(bucket.get("entries", 0))
            tp_cnt = int(bucket.get("tp", 0))
            sl_cnt = int(bucket.get("sl", 0))
            trades = tp_cnt + sl_cnt
            sl_rate = (sl_cnt / entries * 100.0) if entries > 0 else 0.0
            winrate = (tp_cnt / trades * 100.0) if trades > 0 else 0.0
            net_sum = float(bucket.get("net_sum", 0.0))
            net_sum_usdt = float(bucket.get("net_sum_usdt", 0.0))
            total_entries += entries
            total_tp += tp_cnt
            total_sl += sl_cnt
            total_net_sum += net_sum
            total_net_sum_usdt += net_sum_usdt
            print(
                f"[BACKTEST] DATE {day_key} entries={entries} tp={tp_cnt} sl={sl_cnt} "
                f"sl_rate={sl_rate:.2f}% winrate={winrate:.2f}% net_sum={net_sum:.3f} net_sum_usdt={net_sum_usdt:.3f}"
            )
        total_trades = total_tp + total_sl
        total_sl_rate = (total_sl / total_entries * 100.0) if total_entries > 0 else 0.0
        total_winrate = (total_tp / total_trades * 100.0) if total_trades > 0 else 0.0
        print(
            f"[BACKTEST] DATE TOTAL entries={total_entries} tp={total_tp} sl={total_sl} "
            f"sl_rate={total_sl_rate:.2f}% winrate={total_winrate:.2f}% "
            f"net_sum={total_net_sum:.3f} net_sum_usdt={total_net_sum_usdt:.3f}"
        )


if __name__ == "__main__":
    run_backtest()
