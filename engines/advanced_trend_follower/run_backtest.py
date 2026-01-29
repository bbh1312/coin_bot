#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import ccxt
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.universe import build_universe_from_tickers
from engines.atlas.atlas_engine import AtlasSwaggyConfig


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _utc_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def _dt_kst(ts_ms: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=9))
        ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "N/A"


def _fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> List[list]:
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
                break
            if last_ts is None or ts > last_ts:
                out.append(row)
                last_ts = ts
        new_last = int(batch[-1][0])
        if last_ts is None or new_last == last_ts:
            since = new_last + tf_ms
        else:
            since = last_ts + tf_ms
        time.sleep(exchange.rateLimit / 1000.0)
    if out:
        out = out[:-1]
    return out


def _to_df(rows: List[list]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _mfi(df: pd.DataFrame, length: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"]
    tp_diff = tp.diff()
    pos_mf = mf.where(tp_diff > 0, 0.0)
    neg_mf = mf.where(tp_diff < 0, 0.0).abs()
    pos_sum = pos_mf.rolling(length).sum()
    neg_sum = neg_mf.rolling(length).sum()
    ratio = pos_sum / neg_sum.replace(0, float("nan"))
    mfi = 100 - (100 / (1 + ratio))
    return mfi.fillna(0.0)


def _atlas_dir_flags(df_ref: pd.DataFrame, cfg: AtlasSwaggyConfig) -> pd.DataFrame:
    if df_ref.empty:
        return pd.DataFrame()
    df_ref = df_ref.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    ema20 = _ema(df_ref["close"], 20)
    ema60 = _ema(df_ref["close"], 60)
    atr14 = _atr(df_ref, 14)
    atr_sma = atr14.rolling(20).mean()
    trend_raw = ema20 - ema60
    denom = atr14.where(atr14 > 0)
    denom = denom.fillna(df_ref["close"].where(df_ref["close"] > 0, 1.0))
    trend_norm = trend_raw / denom
    allow_long = []
    allow_short = []
    for i in range(len(df_ref)):
        tl = False
        ts = False
        l20 = ema20.iloc[i]
        l60 = ema60.iloc[i]
        tn = trend_norm.iloc[i]
        atr_now = atr14.iloc[i] if pd.notna(atr14.iloc[i]) else None
        atr_mean = atr_sma.iloc[i] if pd.notna(atr_sma.iloc[i]) else None
        atr_ratio = (atr_now / atr_mean) if (atr_now and atr_mean and atr_mean > 0) else None
        if pd.isna(l20) or pd.isna(l60) or pd.isna(tn):
            allow_long.append(0)
            allow_short.append(0)
            continue
        if l20 > l60 and tn >= cfg.trend_norm_bull:
            tl = True
            ts = False
        elif l20 < l60 and tn <= cfg.trend_norm_bear:
            tl = False
            ts = True
        else:
            if abs(float(tn)) <= cfg.trend_norm_flat and (atr_ratio is not None and atr_ratio < cfg.chaos_atr_low):
                tl = False
                ts = False
            else:
                tl = True
                ts = True
        allow_long.append(1 if tl else 0)
        allow_short.append(1 if ts else 0)
    return pd.DataFrame({
        "ts": df_ref["ts"],
        "atlas_allow_long": allow_long,
        "atlas_allow_short": allow_short,
    })


def _adx(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float("nan"))) * 100
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx.fillna(0.0)


def _supertrend(df: pd.DataFrame, atr_len: int, mult: float) -> Tuple[pd.Series, pd.Series]:
    atr = _atr(df, atr_len)
    hl2 = (df["high"] + df["low"]) / 2.0
    upper = hl2 + (mult * atr)
    lower = hl2 - (mult * atr)
    final_upper = upper.copy()
    final_lower = lower.copy()
    trend = pd.Series(index=df.index, dtype="int")
    for i in range(len(df)):
        if i == 0:
            trend.iloc[i] = 1
            continue
        if upper.iloc[i] < final_upper.iloc[i - 1] or df["close"].iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]
        if lower.iloc[i] > final_lower.iloc[i - 1] or df["close"].iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]
        if trend.iloc[i - 1] == 1:
            trend.iloc[i] = -1 if df["close"].iloc[i] < final_lower.iloc[i] else 1
        else:
            trend.iloc[i] = 1 if df["close"].iloc[i] > final_upper.iloc[i] else -1
    st_line = pd.Series(index=df.index, dtype="float")
    for i in range(len(df)):
        st_line.iloc[i] = final_lower.iloc[i] if trend.iloc[i] == 1 else final_upper.iloc[i]
    return st_line, trend


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


def _find_pivots(series: pd.Series, lookback: int, mode: str) -> list:
    if series.empty or lookback <= 0:
        return []
    pivots = []
    n = len(series)
    end = n - lookback
    for i in range(lookback, end):
        window = series.iloc[i - lookback : i + lookback + 1]
        if window.empty:
            continue
        val = series.iloc[i]
        if mode == "low":
            if val == window.min():
                pivots.append(i)
        else:
            if val == window.max():
                pivots.append(i)
    return pivots


def _divergence(df: pd.DataFrame, rsi_series: pd.Series, side: str, lookback: int) -> bool:
    if df.empty or rsi_series.empty:
        return False
    side_key = (side or "").upper()
    if side_key == "LONG":
        pivots = _find_pivots(df["low"], lookback, "low")
    else:
        pivots = _find_pivots(df["high"], lookback, "high")
    if len(pivots) < 2:
        return False
    i1, i2 = pivots[-2], pivots[-1]
    if side_key == "LONG":
        price_ok = df["low"].iloc[i2] < df["low"].iloc[i1]
        rsi_ok = rsi_series.iloc[i2] > rsi_series.iloc[i1]
    else:
        price_ok = df["high"].iloc[i2] > df["high"].iloc[i1]
        rsi_ok = rsi_series.iloc[i2] < rsi_series.iloc[i1]
    return bool(price_ok and rsi_ok)


@dataclass
class Position:
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    stop_px: float
    size: float
    risk_per_unit: float
    tp1_px: float
    remaining: float
    took_tp1: bool = False
    realized_pnl: float = 0.0
    high_max: float = 0.0
    low_min: float = 0.0
    exit_ts: Optional[int] = None
    exit_px: Optional[float] = None
    exit_reason: Optional[str] = None


def _parse_universe_arg(text: str) -> Optional[int]:
    raw = (text or "").strip().lower()
    if raw.startswith("top"):
        try:
            return int(raw.replace("top", ""))
        except Exception:
            return None
    return None


def _select_symbols(
    exchange: ccxt.Exchange,
    symbols_arg: str,
    symbols_file: str,
    universe_arg: str,
    min_qv: float,
) -> List[str]:
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            return [s.strip() for s in f.read().split(",") if s.strip()]
    if symbols_arg:
        return [s.strip() for s in symbols_arg.split(",") if s.strip()]
    top_n = _parse_universe_arg(universe_arg) or 50
    tickers = exchange.fetch_tickers()
    return build_universe_from_tickers(
        tickers,
        min_quote_volume_usdt=min_qv,
        top_n=top_n,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--symbols-file", default="")
    parser.add_argument("--universe", default="top50")
    parser.add_argument("--min-qv", type=float, default=30_000_000.0)
    parser.add_argument("--initial-usdt", type=float, default=1000.0)
    parser.add_argument("--risk-pct", type=float, default=2.0)
    parser.add_argument("--risk-base", default="equity", choices=["equity", "fixed"])
    parser.add_argument("--fixed-equity", type=float, default=1000.0)
    parser.add_argument("--slip-pct", type=float, default=0.0005)
    parser.add_argument("--min-stop-atr", type=float, default=0.5)
    parser.add_argument("--max-notional-mult", type=float, default=10.0)
    parser.add_argument("--atr-scale-lookback", type=int, default=100)
    parser.add_argument("--atr-scale-min", type=float, default=0.5)
    parser.add_argument("--atr-scale-max", type=float, default=1.5)
    parser.add_argument("--adx-min", type=float, default=20.0)
    parser.add_argument("--mfi-long-max", type=float, default=80.0)
    parser.add_argument("--mfi-short-min", type=float, default=20.0)
    parser.add_argument("--st-atr", type=int, default=10)
    parser.add_argument("--st-mult", type=float, default=3.0)
    parser.add_argument("--tf-main", default="15m")
    parser.add_argument("--tf-trend", default="4h")
    parser.add_argument("--atlas-dir", default="on")
    args = parser.parse_args()

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    end_dt = datetime.utcnow()
    start_dt = end_dt - pd.Timedelta(days=args.days)
    start_ms = _utc_ms(start_dt)
    end_ms = _utc_ms(end_dt)

    symbols = _select_symbols(exchange, args.symbols, args.symbols_file, args.universe, args.min_qv)
    if not symbols:
        print("[backtest] no symbols")
        return

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(ROOT_DIR, "logs", "advanced_trend_follower", "backtest")
    _ensure_dir(log_dir)
    trades_path = os.path.join(log_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(log_dir, f"summary_{run_id}.json")
    universe_path = os.path.join(log_dir, f"universe_{run_id}.json")
    log_path = os.path.join(log_dir, f"backtest_{run_id}.log")

    with open(universe_path, "w", encoding="utf-8") as f:
        json.dump({"symbols": symbols, "run_id": run_id}, f, ensure_ascii=True, indent=2)

    def _bt_log(line: str) -> None:
        print(line)
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(line + "\n")

    with open(trades_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol",
            "side",
            "entry_ts",
            "entry_px",
            "exit_ts",
            "exit_px",
            "exit_reason",
            "pnl_usdt",
            "pnl_pct",
            "tp1_taken",
            "duration_bars",
            "mfe",
            "mae",
        ])

    equity = float(args.initial_usdt)
    trades = []
    stats_by_symbol: Dict[str, dict] = {}
    open_positions: Dict[str, dict] = {}

    atlas_dir_on = str(args.atlas_dir).strip().lower() in {"on", "true", "1", "yes", "y"}
    _bt_log(
        "[run] mode=advanced_trend_follower days=%d start_ms=%d end_ms=%d slip=%.4f atlas_dir=%s"
        % (args.days, start_ms, end_ms, float(args.slip_pct), "on" if atlas_dir_on else "off")
    )

    atlas_df = None
    if atlas_dir_on:
        atlas_cfg = AtlasSwaggyConfig()
        rows_ref = _fetch_ohlcv_all(exchange, atlas_cfg.ref_symbol, args.tf_main, start_ms, end_ms)
        df_ref = _to_df(rows_ref)
        atlas_df = _atlas_dir_flags(df_ref, atlas_cfg)

    for symbol in symbols:
        rows_main = _fetch_ohlcv_all(exchange, symbol, args.tf_main, start_ms, end_ms)
        rows_trend = _fetch_ohlcv_all(exchange, symbol, args.tf_trend, start_ms, end_ms)
        df = _to_df(rows_main)
        df_trend = _to_df(rows_trend)
        if df.empty or df_trend.empty:
            continue
        df = df.drop_duplicates("ts").reset_index(drop=True)
        df_trend = df_trend.drop_duplicates("ts").reset_index(drop=True)

        df_trend["ema200"] = _ema(df_trend["close"], 200)
        df_trend = df_trend[["ts", "ema200"]].dropna()
        df_trend = df_trend.sort_values("ts")
        df = df.sort_values("ts")
        df = pd.merge_asof(df, df_trend, on="ts", direction="backward")
        df.rename(columns={"ema200": "ema200_trend"}, inplace=True)
        if atlas_dir_on and isinstance(atlas_df, pd.DataFrame) and not atlas_df.empty:
            df = pd.merge_asof(df, atlas_df, on="ts", direction="backward")

        df["mfi"] = _mfi(df, 14)
        df["adx"] = _adx(df, 14)
        df["rsi"] = _rsi(df["close"], 14)
        df["ema7"] = _ema(df["close"], 7)
        df["ema20"] = _ema(df["close"], 20)
        st_line, st_trend = _supertrend(df, args.st_atr, args.st_mult)
        df["st_line"] = st_line
        df["st_trend"] = st_trend
        df["atr"] = _atr(df, args.st_atr)
        df["atr14"] = _atr(df, 14)
        df["atr_median"] = df["atr"].rolling(int(args.atr_scale_lookback)).median()
        bb_len = 20
        bb_std = 2.0
        bb_mid = df["close"].rolling(bb_len).mean()
        bb_dev = df["close"].rolling(bb_len).std(ddof=0)
        df["bb_upper"] = bb_mid + (bb_std * bb_dev)
        df["bb_lower"] = bb_mid - (bb_std * bb_dev)

        pos: Optional[Position] = None
        for i in range(220, len(df)):
            row = df.iloc[i]
            ts = int(row["ts"])
            close_px = float(row["close"])
            st_line_val = float(row["st_line"]) if pd.notna(row["st_line"]) else None
            st_score = None
            st_score_prev = None
            st_dir = int(row["st_trend"]) if pd.notna(row["st_trend"]) else 0
            ema_val = row.get("ema200_trend")
            if isinstance(ema_val, pd.Series):
                try:
                    ema_val = ema_val.iloc[-1]
                except Exception:
                    ema_val = None
            ema_trend = float(ema_val) if pd.notna(ema_val) else None
            mfi = float(row["mfi"]) if pd.notna(row["mfi"]) else None
            adx = float(row["adx"]) if pd.notna(row["adx"]) else None
            rsi = float(row["rsi"]) if pd.notna(row["rsi"]) else None
            ema7 = float(row["ema7"]) if pd.notna(row["ema7"]) else None
            ema20 = float(row["ema20"]) if pd.notna(row["ema20"]) else None
            atlas_allow_long = int(row["atlas_allow_long"]) if pd.notna(row.get("atlas_allow_long")) else 0
            atlas_allow_short = int(row["atlas_allow_short"]) if pd.notna(row.get("atlas_allow_short")) else 0
            vol_now = float(row["volume"]) if pd.notna(row["volume"]) else None
            vol_prev = float(df["volume"].iloc[i - 1]) if i >= 1 and pd.notna(df["volume"].iloc[i - 1]) else None

            if pos:
                high_now = float(row["high"])
                low_now = float(row["low"])
                pos.high_max = max(pos.high_max, high_now)
                pos.low_min = min(pos.low_min, low_now)
                if pos.side == "LONG":
                    if st_line_val is not None:
                        pos.stop_px = st_line_val
                    if not pos.took_tp1 and close_px >= pos.tp1_px:
                        pos.took_tp1 = True
                        pos.stop_px = pos.entry_px
                    if float(row["low"]) <= pos.stop_px:
                        pos.exit_ts = ts
                        pos.exit_px = pos.stop_px * (1.0 - float(args.slip_pct))
                        pos.exit_reason = "SL"
                    elif st_dir < 0:
                        pos.exit_ts = ts
                        pos.exit_px = close_px * (1.0 - float(args.slip_pct))
                        pos.exit_reason = "ST_FLIP"
                else:
                    if st_line_val is not None:
                        pos.stop_px = st_line_val
                    if not pos.took_tp1 and close_px <= pos.tp1_px:
                        pos.took_tp1 = True
                        pos.stop_px = pos.entry_px
                    if float(row["high"]) >= pos.stop_px:
                        pos.exit_ts = ts
                        pos.exit_px = pos.stop_px * (1.0 + float(args.slip_pct))
                        pos.exit_reason = "SL"
                    elif st_dir > 0:
                        pos.exit_ts = ts
                        pos.exit_px = close_px * (1.0 + float(args.slip_pct))
                        pos.exit_reason = "ST_FLIP"

                if pos.exit_ts is not None:
                    remaining_pnl = (
                        (pos.exit_px - pos.entry_px) * pos.remaining
                        if pos.side == "LONG"
                        else (pos.entry_px - pos.exit_px) * pos.remaining
                    )
                    total_pnl = pos.realized_pnl + remaining_pnl
                    pnl_pct = total_pnl / max(1e-9, float(args.initial_usdt))
                    duration_bars = max(1, i - pos.entry_idx + 1)
                    if pos.side == "LONG":
                        mfe = (pos.high_max - pos.entry_px) / pos.entry_px
                        mae = (pos.low_min - pos.entry_px) / pos.entry_px
                    else:
                        mfe = (pos.entry_px - pos.low_min) / pos.entry_px
                        mae = (pos.entry_px - pos.high_max) / pos.entry_px
                    trades.append({
                        "symbol": symbol,
                        "side": pos.side,
                        "entry_ts": pos.entry_ts,
                        "entry_px": pos.entry_px,
                        "exit_ts": pos.exit_ts,
                        "exit_px": pos.exit_px,
                        "exit_reason": pos.exit_reason,
                        "pnl_usdt": total_pnl,
                        "pnl_pct": pnl_pct,
                        "tp1_taken": "Y" if pos.took_tp1 else "N",
                        "duration_bars": duration_bars,
                        "mfe": mfe,
                        "mae": abs(mae),
                    })
                    equity += total_pnl
                    sym_stats = stats_by_symbol.setdefault(symbol, {
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
                    })
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += float(mfe)
                    sym_stats["mae_sum"] += float(abs(mae))
                    sym_stats["hold_sum"] += float(duration_bars)
                    if pnl_pct > 0:
                        sym_stats["wins"] += 1
                        sym_stats["tp"] += 1
                    else:
                        sym_stats["losses"] += 1
                        sym_stats["sl"] += 1
                    with open(trades_path, "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            symbol,
                            pos.side,
                            pos.entry_ts,
                            pos.entry_px,
                            pos.exit_ts,
                            pos.exit_px,
                            pos.exit_reason,
                            total_pnl,
                            pnl_pct,
                            "Y" if pos.took_tp1 else "N",
                            duration_bars,
                            mfe,
                            abs(mae),
                        ])
                    pos = None
                continue

            if ema_trend is None or mfi is None or adx is None or st_line_val is None:
                continue
            if adx <= float(args.adx_min):
                continue
            if st_line_val is not None and pd.notna(row.get("atr")) and float(row.get("atr")) > 0:
                st_score = (close_px - st_line_val) / float(row.get("atr"))
            if i > 0:
                prev = df.iloc[i - 1]
                if pd.notna(prev.get("st_line")) and pd.notna(prev.get("atr")) and float(prev.get("atr")) > 0:
                    st_score_prev = (float(prev["close"]) - float(prev["st_line"])) / float(prev.get("atr"))

            bb_upper = float(row["bb_upper"]) if pd.notna(row.get("bb_upper")) else None
            bb_lower = float(row["bb_lower"]) if pd.notna(row.get("bb_lower")) else None
            if close_px > float(ema_trend) and mfi < float(args.mfi_long_max) and st_dir > 0:
                if atlas_dir_on and not atlas_allow_long:
                    continue
                if isinstance(st_score, (int, float)) and isinstance(st_score_prev, (int, float)):
                    if st_score <= st_score_prev:
                        continue
                if isinstance(st_score, (int, float)) and abs(st_score) >= 4.0:
                    continue
                if isinstance(bb_upper, (int, float)) and close_px > bb_upper:
                    continue
                if isinstance(rsi, (int, float)) and rsi >= 70:
                    continue
                if isinstance(ema7, (int, float)) and isinstance(ema20, (int, float)) and ema20 > 0:
                    if ema7 > (ema20 * 1.03):
                        continue
                if all(isinstance(v, (int, float)) for v in (vol_now, vol_prev)):
                    upper_wick = float(row["high"]) - max(float(row["open"]), close_px)
                    if vol_prev and vol_now >= (vol_prev * 2.0) and upper_wick > 0:
                        continue
                atr14 = float(row["atr14"]) if pd.notna(row["atr14"]) else None
                if isinstance(atr14, (int, float)) and atr14 > 0:
                    if abs(close_px - st_line_val) > (atr14 * 2.5):
                        continue
                stop_px = st_line_val
                risk_per_unit = close_px - stop_px
                if risk_per_unit <= 0:
                    continue
                atr_val = float(row["atr"]) if pd.notna(row["atr"]) else None
                atr_med = float(row["atr_median"]) if pd.notna(row["atr_median"]) else None
                if isinstance(atr_val, (int, float)) and atr_val > 0:
                    if risk_per_unit < float(args.min_stop_atr) * float(atr_val):
                        continue
                scale = 1.0
                if isinstance(atr_val, (int, float)) and atr_val > 0 and isinstance(atr_med, (int, float)) and atr_med > 0:
                    raw = atr_med / atr_val
                    scale = min(float(args.atr_scale_max), max(float(args.atr_scale_min), raw))
                base_equity = equity if args.risk_base == "equity" else float(args.fixed_equity)
                risk_usdt = base_equity * (float(args.risk_pct) / 100.0) * scale
                size = risk_usdt / risk_per_unit
                entry_fill = close_px * (1.0 + float(args.slip_pct))
                tp1_px = entry_fill + risk_per_unit * 1.5
                max_notional = base_equity * float(args.max_notional_mult)
                notional = size * entry_fill
                if max_notional > 0 and notional > max_notional:
                    size = max_notional / entry_fill
                    notional = max_notional
                pos = Position(
                    side="LONG",
                    entry_idx=i,
                    entry_ts=ts,
                    entry_px=entry_fill,
                    stop_px=stop_px,
                    size=size,
                    risk_per_unit=risk_per_unit,
                    tp1_px=tp1_px,
                    remaining=size,
                    high_max=float(row["high"]),
                    low_min=float(row["low"]),
                )
                sym_stats = stats_by_symbol.setdefault(symbol, {
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
                })
                sym_stats["entries"] += 1
                # per-symbol summaries are printed at the end only
                continue

            if close_px < float(ema_trend) and mfi > float(args.mfi_short_min) and st_dir < 0:
                if atlas_dir_on and not atlas_allow_short:
                    continue
                if isinstance(st_score, (int, float)) and isinstance(st_score_prev, (int, float)):
                    if st_score >= st_score_prev:
                        continue
                if isinstance(st_score, (int, float)) and abs(st_score) >= 4.0:
                    continue
                if isinstance(bb_lower, (int, float)) and close_px < bb_lower:
                    continue
                if isinstance(rsi, (int, float)) and rsi <= 30:
                    continue
                if isinstance(ema7, (int, float)) and isinstance(ema20, (int, float)) and ema20 > 0:
                    if ema7 < (ema20 * 0.97):
                        continue
                atr14 = float(row["atr14"]) if pd.notna(row["atr14"]) else None
                if isinstance(atr14, (int, float)) and atr14 > 0:
                    if abs(close_px - st_line_val) > (atr14 * 2.5):
                        continue
                stop_px = st_line_val
                risk_per_unit = stop_px - close_px
                if risk_per_unit <= 0:
                    continue
                atr_val = float(row["atr"]) if pd.notna(row["atr"]) else None
                atr_med = float(row["atr_median"]) if pd.notna(row["atr_median"]) else None
                if isinstance(atr_val, (int, float)) and atr_val > 0:
                    if risk_per_unit < float(args.min_stop_atr) * float(atr_val):
                        continue
                scale = 1.0
                if isinstance(atr_val, (int, float)) and atr_val > 0 and isinstance(atr_med, (int, float)) and atr_med > 0:
                    raw = atr_med / atr_val
                    scale = min(float(args.atr_scale_max), max(float(args.atr_scale_min), raw))
                base_equity = equity if args.risk_base == "equity" else float(args.fixed_equity)
                risk_usdt = base_equity * (float(args.risk_pct) / 100.0) * scale
                size = risk_usdt / risk_per_unit
                entry_fill = close_px * (1.0 - float(args.slip_pct))
                tp1_px = entry_fill - risk_per_unit * 1.5
                max_notional = base_equity * float(args.max_notional_mult)
                notional = size * entry_fill
                if max_notional > 0 and notional > max_notional:
                    size = max_notional / entry_fill
                    notional = max_notional
                pos = Position(
                    side="SHORT",
                    entry_idx=i,
                    entry_ts=ts,
                    entry_px=entry_fill,
                    stop_px=stop_px,
                    size=size,
                    risk_per_unit=risk_per_unit,
                    tp1_px=tp1_px,
                    remaining=size,
                    high_max=float(row["high"]),
                    low_min=float(row["low"]),
                )
                sym_stats = stats_by_symbol.setdefault(symbol, {
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
                })
                sym_stats["entries"] += 1
                # per-symbol summaries are printed at the end only
        if pos:
            open_positions[symbol] = {
                "side": pos.side,
                "entry_ts": pos.entry_ts,
                "entry_px": pos.entry_px,
                "last_px": float(df["close"].iloc[-1]),
                "last_ts": int(df["ts"].iloc[-1]),
                "remaining": pos.remaining,
            }

    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    losses = sum(1 for t in trades if t["pnl_pct"] <= 0)
    winrate = wins / max(1, len(trades))
    summary = {
        "run_id": run_id,
        "symbols": len(symbols),
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "winrate": winrate,
        "final_equity": equity,
        "initial_equity": float(args.initial_usdt),
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)

    for sym, stats in stats_by_symbol.items():
        if stats["trades"] <= 0:
            continue
        winrate_sym = (stats["wins"] / max(1, stats["trades"])) * 100.0
        avg_mfe = stats["mfe_sum"] / max(1, stats["trades"])
        avg_mae = stats["mae_sum"] / max(1, stats["trades"])
        avg_hold = stats["hold_sum"] / max(1, stats["trades"])
        _bt_log(
            "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
            "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f dca_adds=0 dca_usdt=0.00"
            % (
                sym,
                stats["entries"],
                stats["exits"],
                stats["trades"],
                stats["wins"],
                stats["losses"],
                winrate_sym,
                stats["tp"],
                stats["sl"],
                avg_mfe,
                avg_mae,
                avg_hold,
            )
        )

    # 심볼별 오픈 포지션 로그는 생략 (요약/토탈만 출력)

    total_entries = sum(s["entries"] for s in stats_by_symbol.values())
    total_exits = sum(s["exits"] for s in stats_by_symbol.values())
    total_trades = sum(s["trades"] for s in stats_by_symbol.values())
    total_wins = sum(s["wins"] for s in stats_by_symbol.values())
    total_losses = sum(s["losses"] for s in stats_by_symbol.values())
    total_tp = sum(s["tp"] for s in stats_by_symbol.values())
    total_sl = sum(s["sl"] for s in stats_by_symbol.values())
    total_winrate = (total_wins / max(1, total_trades)) * 100.0
    total_mfe = sum(s["mfe_sum"] for s in stats_by_symbol.values()) / max(1, total_trades)
    total_mae = sum(s["mae_sum"] for s in stats_by_symbol.values()) / max(1, total_trades)
    total_hold = sum(s["hold_sum"] for s in stats_by_symbol.values()) / max(1, total_trades)
    tp_sum = sum(float(t.get("pnl_usdt") or 0.0) for t in trades if float(t.get("pnl_usdt") or 0.0) > 0)
    sl_sum = sum(abs(float(t.get("pnl_usdt") or 0.0)) for t in trades if float(t.get("pnl_usdt") or 0.0) < 0)
    net_sum = tp_sum - sl_sum
    _bt_log(
        "[BACKTEST] TOTAL entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
        "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f dca_adds=0 dca_usdt=0.00 "
        "tp_sum=%.3f sl_sum=%.3f net_sum=%.3f"
        % (
            total_entries,
            total_exits,
            total_trades,
            total_wins,
            total_losses,
            total_winrate,
            total_tp,
            total_sl,
            total_mfe,
            total_mae,
            total_hold,
            tp_sum,
            sl_sum,
            net_sum,
        )
    )

    by_hour = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
    by_dow = {d: {"entries": 0, "tp": 0, "sl": 0} for d in range(7)}
    for t in trades:
        ts = int(t["entry_ts"])
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=9))
        )
        hour = dt.hour
        dow = dt.weekday()
        by_hour[hour]["entries"] += 1
        by_dow[dow]["entries"] += 1
        if t["pnl_pct"] > 0:
            by_hour[hour]["tp"] += 1
            by_dow[dow]["tp"] += 1
        else:
            by_hour[hour]["sl"] += 1
            by_dow[dow]["sl"] += 1

    _bt_log("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for h in range(24):
        e = by_hour[h]["entries"]
        tp = by_hour[h]["tp"]
        sl = by_hour[h]["sl"]
        sl_rate = (sl / max(1, e)) * 100.0
        _bt_log(f"[BACKTEST] HOUR {h:02d} entries={e} tp={tp} sl={sl} sl_rate={sl_rate:.2f}%")

    dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    _bt_log("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for d in range(7):
        e = by_dow[d]["entries"]
        tp = by_dow[d]["tp"]
        sl = by_dow[d]["sl"]
        sl_rate = (sl / max(1, e)) * 100.0
        _bt_log(f"[BACKTEST] DOW {dow_labels[d]} entries={e} tp={tp} sl={sl} sl_rate={sl_rate:.2f}%")

    if total_trades == 0:
        _bt_log("[BACKTEST] no trades/entries")
        return

    worst = sorted(trades, key=lambda t: float(t.get("pnl_usdt") or 0.0))[:5]
    _bt_log("[BACKTEST] TOP5_LOSS sym side entry_dt exit_dt pnl_usdt pnl_pct reason stop_dist notional")
    for t in worst:
        try:
            entry_px = float(t.get("entry_px") or 0.0)
            exit_px = float(t.get("exit_px") or 0.0)
            stop_dist = abs(entry_px - exit_px) / entry_px if entry_px else 0.0
        except Exception:
            stop_dist = 0.0
        try:
            notional = float(t.get("pnl_usdt") or 0.0) / float(t.get("pnl_pct") or 1e-9)
        except Exception:
            notional = 0.0
        _bt_log(
            "[BACKTEST] LOSS %s %s %s %s pnl_usdt=%.3f pnl_pct=%.4f reason=%s stop_dist=%.4f notional=%.2f"
            % (
                t.get("symbol"),
                t.get("side"),
                _dt_kst(int(t.get("entry_ts"))),
                _dt_kst(int(t.get("exit_ts"))),
                float(t.get("pnl_usdt") or 0.0),
                float(t.get("pnl_pct") or 0.0),
                t.get("exit_reason") or "N/A",
                stop_dist,
                notional,
            )
        )


if __name__ == "__main__":
    main()
