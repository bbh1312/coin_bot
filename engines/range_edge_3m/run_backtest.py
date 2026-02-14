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
from engines.range_edge_3m.engine import RangeEdge3MConfig


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


def _mfi(df: pd.DataFrame, length: int = 14) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"].astype(float)
    sign = tp.diff().fillna(0.0)
    pos = mf.where(sign > 0.0, 0.0)
    neg = mf.where(sign < 0.0, 0.0)
    pos_sum = pos.rolling(length, min_periods=1).sum()
    neg_sum = neg.rolling(length, min_periods=1).sum().replace(0.0, np.nan)
    mfr = pos_sum / neg_sum
    out = 100.0 - (100.0 / (1.0 + mfr))
    return out.fillna(50.0)


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


def _adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean().replace(0.0, np.nan)
    plus_di = 100.0 * pd.Series(plus_dm).ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean() / atr
    minus_di = 100.0 * pd.Series(minus_dm).ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean() / atr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)) * 100.0
    return dx.ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean().fillna(50.0)


def _kst_day(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d")


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
        f"tp={tp} sl={sl} avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} "
        f"avg_hold={avg_hold:.1f} last_day_exits={last_day_exits} "
        f"base_usdt={base_usdt:.2f} tp_sum={stats.get('tp_sum', 0.0):.3f} "
        f"sl_sum={stats.get('sl_sum', 0.0):.3f} net_sum={stats.get('net_sum', 0.0):.3f} "
        f"net_sum_usdt={stats.get('net_sum_usdt', 0.0):.3f} entry_syms={entry_syms}"
    )


def _dominance_side(obv_delta: float, cvd_delta: float) -> str:
    score = (1 if obv_delta > 0 else -1) + (1 if cvd_delta > 0 else -1)
    return "LONG" if score >= 0 else "SHORT"


def _opposite_side(side: str) -> str:
    return "SHORT" if str(side).upper() == "LONG" else "LONG"


def _flip_entry_side_and_risk(side: str, entry_px: float, tp: float, sl: float) -> tuple[str, float, float, float]:
    """
    Final step only:
    - flip direction (LONG<->SHORT)
    - swap TP/SL distances around entry without recalculating signals
    """
    old_tp_dist = abs(float(tp) - float(entry_px))
    old_sl_dist = abs(float(entry_px) - float(sl))
    new_side = _opposite_side(side)
    new_tp_dist = old_sl_dist
    new_sl_dist = old_tp_dist
    if new_side == "LONG":
        new_tp = float(entry_px) + new_tp_dist
        new_sl = float(entry_px) - new_sl_dist
    else:
        new_tp = float(entry_px) - new_tp_dist
        new_sl = float(entry_px) + new_sl_dist
    return new_side, new_tp, new_sl, new_sl_dist


def run_backtest() -> None:
    cfg = RangeEdge3MConfig()
    parser = argparse.ArgumentParser("range_edge_3m backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--lookback", type=int, default=cfg.lookback)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--exclude-symbols", type=str, default="")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--allow-shorts", action="store_true", default=cfg.allow_shorts)
    parser.add_argument("--disallow-shorts", action="store_false", dest="allow_shorts")
    parser.add_argument("--adx-threshold", type=float, default=cfg.adx_threshold)
    parser.add_argument("--bb-rel-mult", type=float, default=cfg.bb_rel_mult)
    parser.add_argument("--atr-filter-mult", type=float, default=cfg.atr_filter_mult)
    parser.add_argument("--edge-min-score", type=int, default=cfg.edge_min_score)
    parser.add_argument("--zone-tolerance", type=float, default=cfg.zone_tolerance)
    parser.add_argument("--sfp-tolerance", type=float, default=cfg.sfp_tolerance)
    parser.add_argument("--max-daily-sl", type=int, default=cfg.max_daily_sl)
    parser.add_argument("--sl-atr-mult", type=float, default=cfg.sl_atr_mult)
    parser.add_argument("--rr-min", type=float, default=cfg.rr_min)
    parser.add_argument("--sl-min-pct", type=float, default=cfg.sl_min_pct)
    parser.add_argument("--tp-min-pct", type=float, default=cfg.tp_min_pct)
    parser.add_argument("--base-usdt", type=float, default=10.0)
    parser.add_argument("--flip-last-position", action="store_true")
    parser.add_argument("--log-gates", action="store_true")
    args = parser.parse_args()

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    lookback_days = max(2, int(args.lookback * 3 / 1440) + 2)
    start_ms = end_ms - int((args.days + lookback_days) * 24 * 60 * 60 * 1000)
    eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    log_warmup_info(lambda _x: None, lookback_days, lookback_days * 1440, int(args.days))

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
        "regime_fail": 0,
        "edge_fail": 0,
        "zone_fail": 0,
        "liq_fail": 0,
        "breakout_block": 0,
        "daily_sl_block": 0,
        "sfp_hit": 0,
        "vbp_hit": 0,
        "delta_hit": 0,
        "bull_rev_hit": 0,
        "trigger5m_hit": 0,
        "short_blocked": 0,
    }
    stats = _new_stats()
    per_symbol_stats: Dict[str, Dict[str, float]] = {}
    exit_logs: List[dict] = []
    open_logs: List[dict] = []
    hour_stats: Dict[int, Dict[str, int]] = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
    dow_stats: Dict[str, Dict[str, int]] = {
        d: {"entries": 0, "tp": 0, "sl": 0} for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    }
    date_stats: Dict[str, Dict[str, float]] = {}
    entry_symbols = set()
    daily_sl: Dict[str, int] = {}
    last_signal_side: Optional[str] = None
    risk_stats = {"sl_pct_sum": 0.0, "tp_pct_sum": 0.0, "count": 0}

    for sym in universe:
        rows = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            common_warmup_dir=common_dir if use_common else None,
            common_only=args.common_only,
        )
        if not rows:
            gates["no_data"] += 1
            continue
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        if len(df) < max(int(args.lookback) + 10, 120):
            gates["no_data"] += 1
            continue
        sym_stats = per_symbol_stats.setdefault(sym, _new_stats())

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        vol = df["volume"].astype(float)

        df["ema5"] = _ema(close, 5)
        df["ema20"] = _ema(close, 20)
        bb_mid = close.rolling(cfg.bb_len, min_periods=1).mean()
        bb_std = close.rolling(cfg.bb_len, min_periods=1).std(ddof=0).fillna(0.0)
        bb_up = bb_mid + cfg.bb_std * bb_std
        bb_dn = bb_mid - cfg.bb_std * bb_std
        bb_width = (bb_up - bb_dn) / bb_mid.replace(0.0, np.nan)
        df["bb_width"] = bb_width.fillna(0.0)
        df["bb_width_avg"] = df["bb_width"].rolling(100, min_periods=20).mean().fillna(df["bb_width"])
        df["atr"] = _atr(df, cfg.atr_len).fillna(0.0)
        df["atr_avg"] = df["atr"].rolling(100, min_periods=20).mean().fillna(df["atr"])
        df["adx"] = _adx(df, cfg.adx_len).fillna(50.0)
        df["mfi"] = _mfi(df, 14).fillna(50.0)

        obv_delta = np.sign(close.diff().fillna(0.0)) * vol
        df["obv"] = obv_delta.cumsum().fillna(0.0)
        df["obv_ema"] = _ema(df["obv"], 21)
        candle_delta = ((close - df["open"].astype(float)) / (high - low).replace(0.0, np.nan)).fillna(0.0) * vol
        df["cvd"] = candle_delta.cumsum().fillna(0.0)
        df["cvd_ema"] = _ema(df["cvd"], 21)
        df["vol_sma20"] = vol.rolling(20, min_periods=1).mean()

        position: Optional[Dict[str, float]] = None
        pending: Optional[Dict[str, float]] = None
        end_i = len(df) - 2 - (1 if args.use_confirmed else 0)
        start_i = max(int(args.lookback), 120)

        for i in range(start_i, end_i + 1):
            ts = int(df.at[i, "ts"])
            if ts < eval_start_ms:
                continue

            day_key = _kst_day(ts)
            if pending is not None and int(pending["entry_i"]) == i and position is None:
                position = pending
                stats["entries"] += 1
                sym_stats["entries"] += 1
                entry_symbols.add(sym)
                entry_ts = int(position.get("entry_ts", ts))
                dt_entry = datetime.fromtimestamp(entry_ts / 1000.0, tz=timezone.utc) + timedelta(hours=9)
                hour_bucket = int(dt_entry.hour)
                dow_bucket = dt_entry.strftime("%a")
                hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
                hour_stats[hour_bucket]["entries"] += 1
                dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
                dow_stats[dow_bucket]["entries"] += 1
                day_bucket = dt_entry.strftime("%Y-%m-%d")
                date_stats.setdefault(day_bucket, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
                date_stats[day_bucket]["entries"] += 1
                try:
                    risk_stats["sl_pct_sum"] += float(pending.get("sl_pct", 0.0))
                    risk_stats["tp_pct_sum"] += float(pending.get("tp_pct", 0.0))
                    risk_stats["count"] += 1
                except Exception:
                    pass
                pending = None

            if position is not None:
                h = float(df.at[i, "high"])
                l = float(df.at[i, "low"])
                c = float(df.at[i, "close"])
                side = str(position["side"])
                entry = float(position["entry"])
                sl = float(position["sl"])
                tp = float(position["tp"])
                risk = max(float(position["risk"]), 1e-9)

                if side == "LONG" and c >= entry + risk:
                    sl = max(sl, entry)
                    position["sl"] = sl
                if side == "SHORT" and c <= entry - risk:
                    sl = min(sl, entry)
                    position["sl"] = sl

                prev_c = float(df.at[i - 1, "close"]) if i > 0 else c
                hit_tp = (c >= tp and prev_c >= tp) if side == "LONG" else (c <= tp and prev_c <= tp)
                hit_sl = (l <= sl) if side == "LONG" else (h >= sl)
                if not (hit_tp or hit_sl):
                    continue

                if hit_sl and hit_tp:
                    hit_sl = False  # TP 우선
                reason = "TP" if hit_tp else "SL"
                exit_px = tp if hit_tp else sl
                pnl = (exit_px - entry) / max(entry, 1e-12) if side == "LONG" else (entry - exit_px) / max(entry, 1e-12)

                entry_i = int(position.get("entry_i", i))
                hold_bars = max(i - entry_i, 0)
                hold_minutes = hold_bars * 3.0
                span = df.iloc[entry_i : i + 1]
                if side == "LONG":
                    mfe = (float(span["high"].max()) - entry) / max(entry, 1e-12)
                    mae = (entry - float(span["low"].min())) / max(entry, 1e-12)
                else:
                    mfe = (entry - float(span["low"].min())) / max(entry, 1e-12)
                    mae = (float(span["high"].max()) - entry) / max(entry, 1e-12)

                stats["net_sum"] += pnl
                stats["net_sum_usdt"] += pnl * float(args.base_usdt)
                stats["exits"] += 1
                stats["trades"] += 1
                stats["mfe_sum"] += max(float(mfe), 0.0)
                stats["mae_sum"] += max(float(mae), 0.0)
                stats["hold_sum"] += float(hold_minutes)
                sym_stats["net_sum"] += pnl
                sym_stats["net_sum_usdt"] += pnl * float(args.base_usdt)
                sym_stats["exits"] += 1
                sym_stats["trades"] += 1
                sym_stats["mfe_sum"] += max(float(mfe), 0.0)
                sym_stats["mae_sum"] += max(float(mae), 0.0)
                sym_stats["hold_sum"] += float(hold_minutes)
                if reason == "TP":
                    stats["tp"] += 1
                    stats["wins"] += 1
                    stats["tp_sum"] += float(abs(pnl))
                    sym_stats["tp"] += 1
                    sym_stats["wins"] += 1
                    sym_stats["tp_sum"] += float(abs(pnl))
                else:
                    stats["sl"] += 1
                    stats["losses"] += 1
                    stats["sl_sum"] += float(abs(pnl))
                    sym_stats["sl"] += 1
                    sym_stats["losses"] += 1
                    sym_stats["sl_sum"] += float(abs(pnl))
                    daily_sl[day_key] = daily_sl.get(day_key, 0) + 1
                entry_ts_ms = int(position.get("entry_ts", ts))
                dt_entry = datetime.fromtimestamp(entry_ts_ms / 1000.0, tz=timezone.utc) + timedelta(hours=9)
                hour_bucket = int(dt_entry.hour)
                dow_bucket = dt_entry.strftime("%a")
                day_bucket = dt_entry.strftime("%Y-%m-%d")
                if reason == "TP":
                    hour_stats[hour_bucket]["tp"] += 1
                    dow_stats[dow_bucket]["tp"] += 1
                    date_stats[day_bucket]["tp"] += 1
                else:
                    hour_stats[hour_bucket]["sl"] += 1
                    dow_stats[dow_bucket]["sl"] += 1
                    date_stats[day_bucket]["sl"] += 1
                date_stats[day_bucket]["net_sum"] += float(pnl)
                date_stats[day_bucket]["net_sum_usdt"] += float(pnl * float(args.base_usdt))
                exit_logs.append(
                    {
                        "sym": sym,
                        "mode": "range_edge_3m",
                        "side": side,
                        "entry_ts": entry_ts_ms,
                        "exit_ts": ts,
                        "entry_px": float(entry),
                        "exit_px": float(exit_px),
                        "reason": reason,
                        "tp_pct": float(position.get("tp_pct", 0.0)),
                        "sl_pct": float(position.get("sl_pct", 0.0)),
                    }
                )
                position = None
                continue

            if position is not None or pending is not None:
                continue

            if daily_sl.get(day_key, 0) >= int(args.max_daily_sl):
                gates["daily_sl_block"] += 1
                continue

            c = float(df.at[i, "close"])
            h = float(df.at[i, "high"])
            l = float(df.at[i, "low"])
            o = float(df.at[i, "open"])
            adx = float(df.at[i, "adx"])
            atr = float(df.at[i, "atr"])
            atr_avg = max(float(df.at[i, "atr_avg"]), 1e-12)
            bb_w = float(df.at[i, "bb_width"])
            bb_w_avg = max(float(df.at[i, "bb_width_avg"]), 1e-12)

            upper = float(df["high"].iloc[i - int(args.lookback) : i + 1].max())
            lower = float(df["low"].iloc[i - int(args.lookback) : i + 1].min())
            box_mid = (upper + lower) * 0.5
            box_range = max(upper - lower, 1e-12)
            range_pct = box_range / max(box_mid, 1e-12)

            breakout_up = c > upper * (1.0 + 0.0015)
            breakout_dn = c < lower * (1.0 - 0.0015)
            if breakout_up or breakout_dn:
                gates["breakout_block"] += 1
                continue

            regime_ok = ((adx < float(args.adx_threshold)) or (range_pct <= 0.020)) and (bb_w <= bb_w_avg * float(args.bb_rel_mult))
            regime_ok = regime_ok and (atr <= atr_avg * float(args.atr_filter_mult))
            if not regime_ok:
                gates["regime_fail"] += 1
                continue

            obv_delta_now = float(df.at[i, "obv"] - df.at[i - 3, "obv"])
            cvd_delta_now = float(df.at[i, "cvd"] - df.at[i - 3, "cvd"])
            edge_score = 0
            edge_score += int(float(df.at[i, "obv"]) > float(df.at[i, "obv_ema"]) or obv_delta_now > 0.0)
            edge_score += int(float(df.at[i, "cvd"]) > float(df.at[i, "cvd_ema"]) or cvd_delta_now > 0.0)
            edge_score += int(float(df.at[i, "mfi"]) > float(df.at[i - 1, "mfi"]))
            if edge_score < int(args.edge_min_score):
                gates["edge_fail"] += 1
                continue

            dominant_side = _dominance_side(obv_delta_now, cvd_delta_now)
            side = _opposite_side(dominant_side)
            if side == "SHORT" and not bool(args.allow_shorts):
                gates["short_blocked"] += 1
                continue

            near_lower = c <= lower * (1.0 + float(args.zone_tolerance))
            near_upper = c >= upper * (1.0 - float(args.zone_tolerance))
            zone_ok = near_lower if side == "LONG" else near_upper
            if not zone_ok:
                gates["zone_fail"] += 1
                continue

            vol_now = float(df.at[i, "volume"])
            vol_avg = max(float(df.at[i, "vol_sma20"]), 1e-12)
            vol_ok = vol_now >= vol_avg * 0.80
            if not vol_ok:
                gates["liq_fail"] += 1
                continue

            sfp = False
            if side == "LONG":
                sfp = (l < lower * (1.0 - float(args.sfp_tolerance))) and (c > lower) and (vol_now >= vol_avg * 1.2)
            else:
                sfp = (h > upper * (1.0 + float(args.sfp_tolerance))) and (c < upper) and (vol_now >= vol_avg * 1.2)
            if sfp:
                gates["sfp_hit"] += 1

            poc = float((df["close"].iloc[i - 20 : i + 1] * df["volume"].iloc[i - 20 : i + 1]).sum() / max(df["volume"].iloc[i - 20 : i + 1].sum(), 1e-12))
            vbp = (c > poc and l <= poc) if side == "LONG" else (c < poc and h >= poc)
            if vbp:
                gates["vbp_hit"] += 1

            prev_low = float(df["low"].iloc[i - 8 : i].min())
            prev_high = float(df["high"].iloc[i - 8 : i].max())
            prev_cvd_low = float(df["cvd"].iloc[i - 8 : i].min())
            prev_cvd_high = float(df["cvd"].iloc[i - 8 : i].max())
            delta_div = False
            if side == "LONG":
                delta_div = (l <= prev_low * 1.001) and (float(df.at[i, "cvd"]) > prev_cvd_low)
            else:
                delta_div = (h >= prev_high * 0.999) and (float(df.at[i, "cvd"]) < prev_cvd_high)
            if delta_div:
                gates["delta_hit"] += 1

            bull_rev = (c > o and c > float(df.at[i, "ema5"])) if side == "LONG" else (c < o and c < float(df.at[i, "ema5"]))
            if bull_rev:
                gates["bull_rev_hit"] += 1

            trigger_core = sfp or vbp or delta_div
            # Anti-edge mode: 우위 반대 진입을 강화하기 위해 컨펌을 불리한 방향으로 둔다.
            confirm = (c > lower and c < float(df.at[i, "ema5"])) if side == "LONG" else (c < upper and c > float(df.at[i, "ema5"]))
            if trigger_core and confirm:
                gates["trigger5m_hit"] += 1
            else:
                continue

            entry_i = i + 1
            if entry_i > end_i + 1:
                continue
            entry_px = float(df.at[entry_i, "open"])
            atr_now = max(float(df.at[i, "atr"]), 1e-12)
            if side == "LONG":
                sl_by_atr = entry_px - atr_now * float(args.sl_atr_mult)
                sl_by_floor = entry_px * (1.0 - abs(float(args.sl_min_pct)))
                sl = min(lower * (1.0 - 0.0005), sl_by_atr, sl_by_floor)
                risk = max(entry_px - sl, entry_px * 0.001)
                tp_by_rr = entry_px + risk * float(args.rr_min)
                tp_by_floor = entry_px * (1.0 + abs(float(args.tp_min_pct)))
                tp = max(upper * 0.999, tp_by_rr, tp_by_floor)
            else:
                sl_by_atr = entry_px + atr_now * float(args.sl_atr_mult)
                sl_by_floor = entry_px * (1.0 + abs(float(args.sl_min_pct)))
                sl = max(upper * (1.0 + 0.0005), sl_by_atr, sl_by_floor)
                risk = max(sl - entry_px, entry_px * 0.001)
                tp_by_rr = entry_px - risk * float(args.rr_min)
                tp_by_floor = entry_px * (1.0 - abs(float(args.tp_min_pct)))
                tp = min(lower * 1.001, tp_by_rr, tp_by_floor)

            # Final mutation only: do not recalculate signals/filters.
            side, tp, sl, risk = _flip_entry_side_and_risk(side, entry_px, tp, sl)
            # Log/store actual order distances after final flip/swap.
            sl_pct = (abs(entry_px - sl) / max(entry_px, 1e-12)) * 100.0
            tp_pct = (abs(tp - entry_px) / max(entry_px, 1e-12)) * 100.0
            pending = {
                "entry_i": float(entry_i),
                "entry_ts": int(df.at[entry_i, "ts"]),
                "entry": entry_px,
                "sl": sl,
                "tp": tp,
                "risk": risk,
                "side": side,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
            }
            last_signal_side = side

        if position is not None:
            last_px = float(df.iloc[-1]["close"])
            last_ts = int(df.iloc[-1]["ts"])
            entry_px = float(position.get("entry", last_px))
            side = str(position.get("side", "LONG"))
            unrealized = ((last_px - entry_px) / max(entry_px, 1e-12) * 100.0) if side == "LONG" else (
                (entry_px - last_px) / max(entry_px, 1e-12) * 100.0
            )
            open_logs.append(
                {
                    "sym": sym,
                    "mode": "range_edge_3m",
                    "side": side,
                    "entry_ts": int(position.get("entry_ts", last_ts)),
                    "entry_px": float(entry_px),
                    "last_px": float(last_px),
                    "last_ts": int(last_ts),
                    "unrealized_pct": float(unrealized),
                }
            )

    last_day_threshold = end_ms - (24 * 60 * 60 * 1000)
    for sym, sym_stats in sorted(per_symbol_stats.items(), key=lambda x: float(x[1].get("net_sum_usdt", 0.0)), reverse=True):
        if not (sym_stats.get("entries", 0) or sym_stats.get("trades", 0)):
            continue
        last_day_exits = sum(1 for ex in exit_logs if ex["sym"] == sym and ex["exit_ts"] >= last_day_threshold)
        print(_fmt_summary_line(sym, sym_stats, float(args.base_usdt), last_day_exits, 1 if sym_stats.get("entries", 0) > 0 else 0))
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
                    f"reason={item['reason']} result={result} tp_pct={float(item.get('tp_pct', 0.0)):.2f} sl_pct={float(item.get('sl_pct', 0.0)):.2f}"
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
    print(_fmt_summary_line(None, stats, float(args.base_usdt), total_last_day_exits, len(entry_symbols)))
    risk_n = int(risk_stats["count"])
    avg_sl_pct = (float(risk_stats["sl_pct_sum"]) / risk_n) if risk_n > 0 else 0.0
    avg_tp_pct = (float(risk_stats["tp_pct_sum"]) / risk_n) if risk_n > 0 else 0.0
    print(f"[BACKTEST] RISK avg_sl_pct={avg_sl_pct:.3f}% avg_tp_pct={avg_tp_pct:.3f}% samples={risk_n}")
    if args.log_gates:
        print(f"[BACKTEST] GATES {gates}")

    print("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for hour in range(24):
        bucket = hour_stats.get(hour, {"entries": 0, "tp": 0, "sl": 0})
        entries = int(bucket["entries"])
        sl = int(bucket["sl"])
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={int(bucket['tp'])} "
            f"sl={sl} sl_rate={sl_rate:.2f}%"
        )

    print("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        bucket = dow_stats.get(dow, {"entries": 0, "tp": 0, "sl": 0})
        entries = int(bucket["entries"])
        sl = int(bucket["sl"])
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] DOW {dow} entries={entries} tp={int(bucket['tp'])} "
            f"sl={sl} sl_rate={sl_rate:.2f}%"
        )

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
            tp = int(bucket.get("tp", 0))
            sl = int(bucket.get("sl", 0))
            sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
            trades = tp + sl
            winrate = (tp / trades * 100.0) if trades > 0 else 0.0
            net_sum = float(bucket.get("net_sum", 0.0))
            net_sum_usdt = float(bucket.get("net_sum_usdt", 0.0))
            total_entries += entries
            total_tp += tp
            total_sl += sl
            total_net_sum += net_sum
            total_net_sum_usdt += net_sum_usdt
            print(
                f"[BACKTEST] DATE {day_key} entries={entries} tp={tp} "
                f"sl={sl} sl_rate={sl_rate:.2f}% winrate={winrate:.2f}% "
                f"net_sum={net_sum:.3f} net_sum_usdt={net_sum_usdt:.3f}"
            )
        total_trades = total_tp + total_sl
        total_sl_rate = (total_sl / total_entries * 100.0) if total_entries > 0 else 0.0
        total_winrate = (total_tp / total_trades * 100.0) if total_trades > 0 else 0.0
        print(
            f"[BACKTEST] DATE TOTAL entries={total_entries} tp={total_tp} sl={total_sl} "
            f"sl_rate={total_sl_rate:.2f}% winrate={total_winrate:.2f}% "
            f"net_sum={total_net_sum:.3f} net_sum_usdt={total_net_sum_usdt:.3f}"
        )

    if args.flip_last_position:
        if last_signal_side == "LONG":
            print("[BACKTEST] LAST_FLIP side=SHORT")
        elif last_signal_side == "SHORT":
            print("[BACKTEST] LAST_FLIP side=LONG")
        else:
            print("[BACKTEST] LAST_FLIP side=NONE")


if __name__ == "__main__":
    run_backtest()
