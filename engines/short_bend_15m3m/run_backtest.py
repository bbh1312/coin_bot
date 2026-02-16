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


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean()


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
    parser.add_argument("--htf-resistance-require", action="store_true", default=cfg.htf_resistance_require)
    parser.add_argument("--no-htf-resistance-require", action="store_false", dest="htf_resistance_require")
    parser.add_argument("--htf-resistance-touch-tol-pct", type=float, default=cfg.htf_resistance_touch_tol_pct)
    parser.add_argument("--htf-resistance-reject-min-pct", type=float, default=cfg.htf_resistance_reject_min_pct)
    parser.add_argument("--htf-dev-min-pct", type=float, default=cfg.htf_dev_min_pct)
    parser.add_argument("--use-1h-regime", action="store_true", default=cfg.use_1h_regime)
    parser.add_argument("--no-use-1h-regime", action="store_false", dest="use_1h_regime")
    parser.add_argument("--lookback-1h", type=int, default=cfg.lookback_1h)
    parser.add_argument("--h1-use-rise-filter", action="store_true", default=cfg.h1_use_rise_filter)
    parser.add_argument("--no-h1-use-rise-filter", action="store_false", dest="h1_use_rise_filter")
    parser.add_argument("--h1-rise-min-pct", type=float, default=cfg.h1_rise_min_pct)
    parser.add_argument("--h1-top-zone-ratio", type=float, default=cfg.h1_top_zone_ratio)
    parser.add_argument("--h1-resistance-touch-tol-pct", type=float, default=cfg.h1_resistance_touch_tol_pct)
    parser.add_argument("--h1-resistance-reject-min-pct", type=float, default=cfg.h1_resistance_reject_min_pct)
    parser.add_argument("--h1-use-dev-filter", action="store_true", default=cfg.h1_use_dev_filter)
    parser.add_argument("--no-h1-use-dev-filter", action="store_false", dest="h1_use_dev_filter")
    parser.add_argument("--h1-dev-min-pct", type=float, default=cfg.h1_dev_min_pct)
    parser.add_argument("--h1-bend-require", action="store_true", default=cfg.h1_bend_require)
    parser.add_argument("--no-h1-bend-require", action="store_false", dest="h1_bend_require")
    parser.add_argument("--h1-ema-len", type=int, default=cfg.h1_ema_len)
    parser.add_argument("--ema-fast-len", type=int, default=cfg.ema_fast_len)
    parser.add_argument("--ema-mid-len", type=int, default=cfg.ema_mid_len)
    parser.add_argument("--ema-slow-len", type=int, default=cfg.ema_slow_len)
    parser.add_argument("--htf-require-vol-confirm", action="store_true", default=cfg.htf_require_vol_confirm)
    parser.add_argument("--no-htf-require-vol-confirm", action="store_false", dest="htf_require_vol_confirm")
    parser.add_argument("--htf-vol-mult-min", type=float, default=cfg.htf_vol_mult_min)
    parser.add_argument("--htf-vol-spike-lookback", type=int, default=cfg.htf_vol_spike_lookback)
    parser.add_argument("--htf-vol-spike-mult", type=float, default=cfg.htf_vol_spike_mult)
    parser.add_argument("--bend-atr-len", type=int, default=cfg.bend_atr_len)
    parser.add_argument("--bend-min-drop-atr", type=float, default=cfg.bend_min_drop_atr)
    parser.add_argument("--bend-min-body-ratio", type=float, default=cfg.bend_min_body_ratio)
    parser.add_argument("--bend-require-prev-low-break", action="store_true", default=cfg.bend_require_prev_low_break)
    parser.add_argument("--no-bend-require-prev-low-break", action="store_false", dest="bend_require_prev_low_break")
    parser.add_argument("--bend-two-step-enable", action="store_true", default=cfg.bend_two_step_enable)
    parser.add_argument("--no-bend-two-step-enable", action="store_false", dest="bend_two_step_enable")
    parser.add_argument("--bend-use-upper-wick-filter", action="store_true", default=cfg.bend_use_upper_wick_filter)
    parser.add_argument("--no-bend-use-upper-wick-filter", action="store_false", dest="bend_use_upper_wick_filter")
    parser.add_argument("--bend-min-upper-wick-ratio", type=float, default=cfg.bend_min_upper_wick_ratio)
    parser.add_argument(
        "--bend-use-close-location-filter",
        action="store_true",
        default=cfg.bend_use_close_location_filter,
    )
    parser.add_argument(
        "--no-bend-use-close-location-filter",
        action="store_false",
        dest="bend_use_close_location_filter",
    )
    parser.add_argument("--bend-max-close-pos-ratio", type=float, default=cfg.bend_max_close_pos_ratio)
    parser.add_argument("--armed-bars-3m", type=int, default=cfg.armed_bars_3m)
    parser.add_argument("--ltf-ema-len", type=int, default=cfg.ltf_ema_len)
    parser.add_argument("--swing-lookback-3m", type=int, default=cfg.swing_lookback_3m)
    parser.add_argument("--require-vol-confirm", action="store_true", default=cfg.require_vol_confirm)
    parser.add_argument("--vol-mult-min", type=float, default=cfg.vol_mult_min)
    parser.add_argument("--ltf-two-step-confirm", action="store_true", default=cfg.ltf_two_step_confirm)
    parser.add_argument("--no-ltf-two-step-confirm", action="store_false", dest="ltf_two_step_confirm")
    parser.add_argument("--ltf-retest-enable", action="store_true", default=cfg.ltf_retest_enable)
    parser.add_argument("--no-ltf-retest-enable", action="store_false", dest="ltf_retest_enable")
    parser.add_argument("--ltf-retest-bars", type=int, default=cfg.ltf_retest_bars)
    parser.add_argument("--ltf-retest-min-bars", type=int, default=cfg.ltf_retest_min_bars)
    parser.add_argument("--ltf-retest-tol-atr-mult", type=float, default=cfg.ltf_retest_tol_atr_mult)
    parser.add_argument("--retest-score-min", type=int, default=cfg.retest_score_min)
    parser.add_argument("--retest-use-weighted-score", action="store_true", default=cfg.retest_use_weighted_score)
    parser.add_argument("--no-retest-use-weighted-score", action="store_false", dest="retest_use_weighted_score")
    parser.add_argument("--retest-score-min-float", type=float, default=cfg.retest_score_min_float)
    parser.add_argument("--retest-score-use-resistance", action="store_true", default=cfg.retest_score_use_resistance)
    parser.add_argument("--no-retest-score-use-resistance", action="store_false", dest="retest_score_use_resistance")
    parser.add_argument("--retest-score-use-volume", action="store_true", default=cfg.retest_score_use_volume)
    parser.add_argument("--no-retest-score-use-volume", action="store_false", dest="retest_score_use_volume")
    parser.add_argument(
        "--retest-score-use-reject-strength",
        action="store_true",
        default=cfg.retest_score_use_reject_strength,
    )
    parser.add_argument(
        "--no-retest-score-use-reject-strength",
        action="store_false",
        dest="retest_score_use_reject_strength",
    )
    parser.add_argument("--retest-score-vol-mult-min", type=float, default=cfg.retest_score_vol_mult_min)
    parser.add_argument("--retest-score-reject-atr-mult", type=float, default=cfg.retest_score_reject_atr_mult)
    parser.add_argument("--retest-score-weight-resistance", type=float, default=cfg.retest_score_weight_resistance)
    parser.add_argument("--retest-score-weight-volume", type=float, default=cfg.retest_score_weight_volume)
    parser.add_argument("--retest-score-weight-reject", type=float, default=cfg.retest_score_weight_reject)
    parser.add_argument("--ltf-wait-counter-momo", action="store_true", default=cfg.ltf_wait_counter_momo)
    parser.add_argument("--no-ltf-wait-counter-momo", action="store_false", dest="ltf_wait_counter_momo")
    parser.add_argument("--ltf-counter-momo-bars", type=int, default=cfg.ltf_counter_momo_bars)
    parser.add_argument("--ltf-resistance-require", action="store_true", default=cfg.ltf_resistance_require)
    parser.add_argument("--no-ltf-resistance-require", action="store_false", dest="ltf_resistance_require")
    parser.add_argument("--ltf-resistance-as-gate", action="store_true", default=cfg.ltf_resistance_as_gate)
    parser.add_argument("--no-ltf-resistance-as-gate", action="store_false", dest="ltf_resistance_as_gate")
    parser.add_argument("--ltf-resistance-lookback", type=int, default=cfg.ltf_resistance_lookback)
    parser.add_argument("--ltf-resistance-touch-bars", type=int, default=cfg.ltf_resistance_touch_bars)
    parser.add_argument(
        "--ltf-resistance-touch-tol-atr-mult", type=float, default=cfg.ltf_resistance_touch_tol_atr_mult
    )
    parser.add_argument("--ltf-resistance-reject-min-pct", type=float, default=cfg.ltf_resistance_reject_min_pct)

    parser.add_argument("--sl-min-pct", type=float, default=cfg.sl_min_pct)
    parser.add_argument("--sl-max-pct", type=float, default=cfg.sl_max_pct)
    parser.add_argument("--sl-floor-atr-mult", type=float, default=cfg.sl_floor_atr_mult)
    parser.add_argument("--sl-use-entry-floor", action="store_true", default=cfg.sl_use_entry_floor)
    parser.add_argument("--no-sl-use-entry-floor", action="store_false", dest="sl_use_entry_floor")
    parser.add_argument("--sl-use-retest-high", action="store_true", default=cfg.sl_use_retest_high)
    parser.add_argument("--no-sl-use-retest-high", action="store_false", dest="sl_use_retest_high")
    parser.add_argument("--sl-retest-buffer-atr-mult", type=float, default=cfg.sl_retest_buffer_atr_mult)
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
        "h1_data_fail": 0,
        "h1_rise_fail": 0,
        "h1_top_zone_fail": 0,
        "h1_resist_fail": 0,
        "h1_dev_fail": 0,
        "h1_bend_fail": 0,
        "ema_stack_fail": 0,
        "hh_fail": 0,
        "top_zone_fail": 0,
        "htf_resist_fail": 0,
        "htf_dev_fail": 0,
        "htf_vol_fail": 0,
        "htf_vol_spike_fail": 0,
        "bend_fail": 0,
        "bend_strength_fail": 0,
        "bend_two_step_fail": 0,
        "bend_wick_fail": 0,
        "bend_close_pos_fail": 0,
        "armed": 0,
        "confirm_fail": 0,
        "retest_fail": 0,
        "retest_score_fail": 0,
        "two_step_fail": 0,
        "ltf_resist_fail": 0,
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
        rows1h = []
        if bool(args.use_1h_regime):
            rows1h = _fetch_ohlcv_all(
                exchange,
                sym,
                "1h",
                start_ms,
                end_ms,
                cache_only=args.cache_only,
                common_warmup_dir=common_dir if use_common else None,
                common_only=args.common_only,
            )
        if (not rows15) or (not rows3) or (bool(args.use_1h_regime) and not rows1h):
            gates["no_data"] += 1
            continue

        d15 = pd.DataFrame(rows15, columns=["ts", "open", "high", "low", "close", "volume"])
        d3 = pd.DataFrame(rows3, columns=["ts", "open", "high", "low", "close", "volume"])
        d1h = pd.DataFrame(rows1h, columns=["ts", "open", "high", "low", "close", "volume"]) if rows1h else pd.DataFrame()
        d15 = d15.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        d3 = d3.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        if not d1h.empty:
            d1h = d1h.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)

        if args.use_confirmed and len(d15) > 0:
            d15 = d15.iloc[:-1].reset_index(drop=True)
        if args.use_confirmed and len(d3) > 0:
            d3 = d3.iloc[:-1].reset_index(drop=True)
        if args.use_confirmed and not d1h.empty:
            d1h = d1h.iloc[:-1].reset_index(drop=True)
        if len(d15) < max(int(args.lookback_15m) + 2, int(args.ema_slow_len) + 2) or len(d3) < 200:
            gates["no_data"] += 1
            continue
        if bool(args.use_1h_regime) and len(d1h) < max(int(args.lookback_1h) + 2, int(args.h1_ema_len) + 2):
            gates["no_data"] += 1
            continue

        d15["ema_fast"] = _ema(d15["close"].astype(float), int(args.ema_fast_len))
        d15["ema_mid"] = _ema(d15["close"].astype(float), int(args.ema_mid_len))
        d15["ema_slow"] = _ema(d15["close"].astype(float), int(args.ema_slow_len))
        d15["vol_sma20"] = d15["volume"].astype(float).rolling(20, min_periods=1).mean()
        d15["atr"] = _atr(d15, int(args.bend_atr_len)).fillna(0.0)

        d3["ema_ltf"] = _ema(d3["close"].astype(float), int(args.ltf_ema_len))
        d3["vol_sma20"] = d3["volume"].astype(float).rolling(20, min_periods=1).mean()
        d3["swing_low_prev"] = d3["low"].astype(float).rolling(int(args.swing_lookback_3m), min_periods=2).min().shift(1)
        d3["atr"] = _atr(d3, 14).fillna(0.0)
        d3["res_high_prev"] = (
            d3["high"].astype(float).rolling(max(int(args.ltf_resistance_lookback), 2), min_periods=2).max().shift(1)
        )
        d3["recent_high_prev"] = (
            d3["high"].astype(float).rolling(max(int(args.ltf_resistance_touch_bars), 2), min_periods=1).max().shift(1)
        )
        ts_1h = d1h["ts"].values if not d1h.empty else np.array([], dtype=np.int64)
        if not d1h.empty:
            d1h["ema_h1"] = _ema(d1h["close"].astype(float), int(args.h1_ema_len))

        sym_stats = per_symbol.setdefault(sym, _new_stats())
        next_eligible_ts = eval_start_ms
        open_left = False

        for i in range(max(int(args.lookback_15m), int(args.ema_slow_len)) + 1, len(d15) - 1):
            ts15 = int(d15.at[i, "ts"])
            if ts15 < eval_start_ms or ts15 < next_eligible_ts:
                continue
            if bool(args.use_1h_regime):
                decision_ts = ts15 - (60 * 60 * 1000 if bool(args.use_confirmed) else 0)
                idx1h = int(np.searchsorted(ts_1h, decision_ts, side="right") - 1)
                if idx1h < max(int(args.lookback_1h), 2):
                    gates["h1_data_fail"] += 1
                    continue
                h0 = idx1h - int(args.lookback_1h) + 1
                highs1 = d1h["high"].iloc[h0 : idx1h + 1].astype(float)
                lows1 = d1h["low"].iloc[h0 : idx1h + 1].astype(float)
                if len(highs1) < int(args.lookback_1h):
                    gates["h1_data_fail"] += 1
                    continue
                h1_low = max(float(lows1.min()), 1e-12)
                h1_high = float(highs1.max())
                h1_rise_pct = (h1_high - h1_low) / h1_low * 100.0
                if bool(args.h1_use_rise_filter):
                    if h1_rise_pct < float(args.h1_rise_min_pct):
                        gates["h1_rise_fail"] += 1
                        continue
                h1_range = max(h1_high - h1_low, 1e-12)
                h1_top_ratio = max(min(float(args.h1_top_zone_ratio), 0.9), 0.05)
                h1_top_floor = h1_high - (h1_range * h1_top_ratio)
                close1 = float(d1h.at[idx1h, "close"])
                if close1 < h1_top_floor:
                    gates["h1_top_zone_fail"] += 1
                    continue
                high1 = float(d1h.at[idx1h, "high"])
                tol1 = max(float(args.h1_resistance_touch_tol_pct), 0.0)
                rej1 = max(float(args.h1_resistance_reject_min_pct), 0.0)
                near_res1 = ((h1_high - high1) / max(h1_high, 1e-12)) <= tol1
                reject_res1 = close1 <= (h1_high * (1.0 - rej1))
                if not (near_res1 or reject_res1):
                    gates["h1_resist_fail"] += 1
                    continue
                ema1 = max(float(d1h.at[idx1h, "ema_h1"]), 1e-12)
                h1_dev_pct = (close1 - ema1) / ema1 * 100.0
                if bool(args.h1_use_dev_filter):
                    if h1_dev_pct < float(args.h1_dev_min_pct):
                        gates["h1_dev_fail"] += 1
                        continue
                if bool(args.h1_bend_require):
                    if idx1h < 1:
                        gates["h1_bend_fail"] += 1
                        continue
                    prev_h1_high = float(d1h.at[idx1h - 1, "high"])
                    prev_h1_close = float(d1h.at[idx1h - 1, "close"])
                    bend1 = (high1 < prev_h1_high) and (close1 < prev_h1_close)
                    if not bend1:
                        gates["h1_bend_fail"] += 1
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
            if bool(args.htf_resistance_require):
                prev_high = float(d15.at[i - 1, "high"])
                prev_close = float(d15.at[i - 1, "close"])
                resist_tol = max(float(args.htf_resistance_touch_tol_pct), 0.0)
                reject_min = max(float(args.htf_resistance_reject_min_pct), 0.0)
                near_res = ((rolling_high - prev_high) / max(rolling_high, 1e-12)) <= resist_tol
                reject_from_res = prev_close <= (rolling_high * (1.0 - reject_min))
                if not (near_res or reject_from_res):
                    gates["htf_resist_fail"] += 1
                    continue
            prev_close_for_dev = float(d15.at[i - 1, "close"])
            dev_base = max(float(d15.at[i - 1, "ema_fast"]), 1e-12)
            dev_pct = (prev_close_for_dev - dev_base) / dev_base * 100.0
            if dev_pct < float(args.htf_dev_min_pct):
                gates["htf_dev_fail"] += 1
                continue

            prev_high = float(d15.at[i - 1, "high"])
            prev_low = float(d15.at[i - 1, "low"])
            prev_close = float(d15.at[i - 1, "close"])
            now_open = float(d15.at[i, "open"])
            now_high = float(d15.at[i, "high"])
            now_low = float(d15.at[i, "low"])
            now_close = float(d15.at[i, "close"])
            atr15 = max(float(d15.at[i, "atr"]), 1e-12)
            range_now = max(now_high - now_low, 1e-12)
            body_ratio = abs(now_close - now_open) / range_now
            upper_wick_ratio = (now_high - max(now_open, now_close)) / range_now
            close_pos_ratio = (now_close - now_low) / range_now
            drop_atr = (prev_close - now_close) / atr15
            bend = now_high < prev_high and (now_close < prev_close)
            if bend and bool(args.bend_require_prev_low_break):
                bend = now_close < prev_low
            if bend and (drop_atr < float(args.bend_min_drop_atr) or body_ratio < float(args.bend_min_body_ratio)):
                gates["bend_strength_fail"] += 1
                bend = False
            if bend and bool(args.bend_two_step_enable):
                if i < 2:
                    gates["bend_two_step_fail"] += 1
                    bend = False
                else:
                    prev2_high = float(d15.at[i - 2, "high"])
                    prev2_close = float(d15.at[i - 2, "close"])
                    prev_bend = (prev_high < prev2_high) and (prev_close < prev2_close)
                    if bool(args.bend_require_prev_low_break):
                        prev2_low = float(d15.at[i - 2, "low"])
                        prev_bend = prev_bend and (prev_close < prev2_low)
                    if not prev_bend:
                        gates["bend_two_step_fail"] += 1
                        bend = False
            if bend and bool(args.bend_use_upper_wick_filter):
                if upper_wick_ratio < float(args.bend_min_upper_wick_ratio):
                    gates["bend_wick_fail"] += 1
                    bend = False
            if bend and bool(args.bend_use_close_location_filter):
                if close_pos_ratio > float(args.bend_max_close_pos_ratio):
                    gates["bend_close_pos_fail"] += 1
                    bend = False
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

            entry_ref_j = -1
            retest_high_ref = np.nan
            resist_blocked = False
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
                break_cond = (c < ema_ltf) and (c < sw_low) and vol_ok
                if not break_cond:
                    continue

                if not bool(args.ltf_retest_enable):
                    entry_ref_j = int(j)
                    break

                retest_ok = False
                atr3 = max(float(d3.at[j, "atr"]), 1e-12)
                tol = atr3 * max(float(args.ltf_retest_tol_atr_mult), 0.0)
                min_wait = max(int(args.ltf_retest_min_bars), 1)
                max_wait = max(int(args.ltf_retest_bars), min_wait)
                start_j = int(j) + min_wait
                end_j = min(int(j) + max_wait, len(d3) - 2)
                for j2 in range(start_j, end_j + 1):
                    h2 = float(d3.at[j2, "high"])
                    c2 = float(d3.at[j2, "close"])
                    ema2 = float(d3.at[j2, "ema_ltf"])
                    touched = h2 >= (sw_low - tol)
                    if not touched:
                        continue

                    reject_same = (c2 < sw_low) and (c2 < ema2)
                    reject_next = False
                    rej_j = j2
                    if not reject_same and (j2 + 1) < len(d3):
                        j3 = j2 + 1
                        c3n = float(d3.at[j3, "close"])
                        h3n = float(d3.at[j3, "high"])
                        ema3n = float(d3.at[j3, "ema_ltf"])
                        reject_next = (c3n < sw_low) and (c3n < ema3n) and (h3n <= (sw_low + tol))
                        if reject_next:
                            rej_j = j3
                    if not (reject_same or reject_next):
                        continue

                    ltf_res_ok = True
                    if bool(args.ltf_resistance_require):
                        res3 = d3.at[rej_j, "res_high_prev"]
                        recent_h = d3.at[rej_j, "recent_high_prev"]
                        if not (np.isfinite(res3) and np.isfinite(recent_h)):
                            ltf_res_ok = False
                        else:
                            res3f = float(res3)
                            recent_hf = float(recent_h)
                            atr_rej = max(float(d3.at[rej_j, "atr"]), 1e-12)
                            tol_res = atr_rej * max(float(args.ltf_resistance_touch_tol_atr_mult), 0.0)
                            touched_res = recent_hf >= (res3f - tol_res)
                            reject_res = float(d3.at[rej_j, "close"]) <= (
                                res3f * (1.0 - max(float(args.ltf_resistance_reject_min_pct), 0.0))
                            )
                            ltf_res_ok = touched_res and reject_res
                    if bool(args.ltf_resistance_as_gate) and (not ltf_res_ok):
                        resist_blocked = True
                        continue

                    score = 0
                    score_f = 0.0
                    if bool(args.retest_score_use_resistance):
                        if ltf_res_ok:
                            score += 1
                            score_f += float(args.retest_score_weight_resistance)
                    if bool(args.retest_score_use_volume):
                        vol_now = float(d3.at[rej_j, "volume"])
                        vol_sma_now = max(float(d3.at[rej_j, "vol_sma20"]), 1e-12)
                        if vol_now >= (vol_sma_now * float(args.retest_score_vol_mult_min)):
                            score += 1
                            score_f += float(args.retest_score_weight_volume)
                    if bool(args.retest_score_use_reject_strength):
                        close_now = float(d3.at[rej_j, "close"])
                        atr_now = max(float(d3.at[rej_j, "atr"]), 1e-12)
                        if (sw_low - close_now) >= (atr_now * float(args.retest_score_reject_atr_mult)):
                            score += 1
                            score_f += float(args.retest_score_weight_reject)
                    if bool(args.retest_use_weighted_score):
                        if score_f < max(float(args.retest_score_min_float), 0.0):
                            gates["retest_score_fail"] += 1
                            continue
                    else:
                        if score < max(int(args.retest_score_min), 0):
                            gates["retest_score_fail"] += 1
                            continue

                    entry_ref_j = int(rej_j)
                    retest_high_ref = max(
                        float(d3.at[j2, "high"]),
                        float(d3.at[rej_j, "high"]),
                    )
                    retest_ok = True
                    break
                if retest_ok:
                    break

            if entry_ref_j < 0:
                if bool(args.ltf_resistance_require) and bool(args.ltf_resistance_as_gate) and resist_blocked:
                    gates["ltf_resist_fail"] += 1
                if bool(args.ltf_retest_enable):
                    gates["retest_fail"] += 1
                gates["confirm_fail"] += 1
                continue

            if bool(args.ltf_two_step_confirm):
                jn = entry_ref_j + 1
                if jn >= len(d3):
                    gates["two_step_fail"] += 1
                    gates["confirm_fail"] += 1
                    continue
                c_sig = float(d3.at[entry_ref_j, "close"])
                h_sig = float(d3.at[entry_ref_j, "high"])
                c_n = float(d3.at[jn, "close"])
                h_n = float(d3.at[jn, "high"])
                step_ok = (c_n < c_sig) or (h_n < h_sig)
                if not step_ok:
                    gates["two_step_fail"] += 1
                    gates["confirm_fail"] += 1
                    continue
                entry_ref_j = int(jn)
            if bool(args.ltf_wait_counter_momo):
                max_wait = max(int(args.ltf_counter_momo_bars), 1)
                end_wait_j = min(entry_ref_j + max_wait, len(d3) - 2)
                counter_j = -1
                for j2 in range(entry_ref_j, end_wait_j + 1):
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
            atr3_entry = max(float(d3.at[entry_i, "atr"]), 1e-12)
            sl_by_atr = entry_px + (atr3_entry * max(float(args.sl_floor_atr_mult), 0.0))
            sl_candidates = [sl_by_bend, sl_by_atr]
            if bool(args.sl_use_entry_floor):
                sl_candidates.append(entry_px * (1.0 + float(args.sl_min_pct)))
            if bool(args.sl_use_retest_high) and np.isfinite(retest_high_ref):
                sl_candidates.append(float(retest_high_ref) + (atr3_entry * max(float(args.sl_retest_buffer_atr_mult), 0.0)))
            sl = max(sl_candidates)
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
