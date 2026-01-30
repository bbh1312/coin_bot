#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
import glob
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import ccxt
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.universe import build_universe_from_tickers


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _ohlcv_cache_path(root_dir: str, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe = _sanitize_symbol(symbol)
    return os.path.join(
        root_dir,
        "logs",
        "srp_st_regime_pullback_v1",
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
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        return cached
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
    _write_ohlcv_cache(cache_path, out)
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


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


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


def _vol_sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


def _find_swing_level(df: pd.DataFrame, start_idx: int, end_idx: int, side: str) -> Optional[float]:
    if start_idx < 0 or end_idx < 0 or end_idx < start_idx:
        return None
    window = df.iloc[start_idx : end_idx + 1]
    if window.empty:
        return None
    if side == "LONG":
        return float(window["low"].min())
    return float(window["high"].max())


def _parse_universe_arg(text: str) -> Optional[int]:
    raw = (text or "").strip().lower()
    if raw.startswith("top"):
        try:
            return int(raw.replace("top", ""))
        except Exception:
            return None
    return None


def _timeframe_minutes(tf: str) -> Optional[int]:
    raw = (tf or "").strip().lower()
    if not raw:
        return None
    try:
        if raw.endswith("m"):
            return int(raw[:-1])
        if raw.endswith("h"):
            return int(raw[:-1]) * 60
        if raw.endswith("d"):
            return int(raw[:-1]) * 1440
    except Exception:
        return None
    return None


def _read_common_universe_file(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    symbols: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                if raw.startswith("COMMON_UNIVERSE"):
                    continue
                if raw.startswith("#"):
                    continue
                symbols.append(raw)
    except Exception:
        return []
    return symbols


def _latest_common_universe_file(root_dir: str) -> str:
    pattern = os.path.join(root_dir, "logs", "common_universe", "common_universe_*.log")
    candidates = glob.glob(pattern)
    if not candidates:
        return ""
    try:
        return max(candidates, key=os.path.getmtime)
    except Exception:
        return ""


def _read_symbol_cache(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [s for s in data if isinstance(s, str) and s]
    except Exception:
        return []
    return []


def _write_symbol_cache(path: str, symbols: List[str]) -> None:
    if not path or not symbols:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(symbols, f)
    except Exception:
        return


def _select_symbols(
    exchange: ccxt.Exchange,
    symbols_arg: str,
    symbols_file: str,
    universe_arg: str,
    min_qv: float,
    common_universe_file: str,
    use_common_universe_latest: bool,
    common_universe_cache: str,
) -> List[str]:
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            return [s.strip() for s in f.read().split(",") if s.strip()]
    if symbols_arg:
        return [s.strip() for s in symbols_arg.split(",") if s.strip()]
    cached = _read_symbol_cache(common_universe_cache)
    if cached:
        return cached
    common_path = common_universe_file
    if not common_path and use_common_universe_latest:
        common_path = _latest_common_universe_file(ROOT_DIR)
    if common_path:
        common_syms = _read_common_universe_file(common_path)
        if common_syms:
            _write_symbol_cache(common_universe_cache, common_syms)
            return common_syms
    top_n = _parse_universe_arg(universe_arg) or 50
    tickers = exchange.fetch_tickers()
    symbols = build_universe_from_tickers(
        tickers,
        min_quote_volume_usdt=min_qv,
        top_n=top_n,
    )
    _write_symbol_cache(common_universe_cache, symbols)
    return symbols


@dataclass
class Position:
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    stop_px: float
    tp_px: float
    size: float
    risk_per_unit: float
    max_favorable: float = 0.0
    max_adverse: float = 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--symbols-file", default="")
    parser.add_argument("--universe", default="top50")
    parser.add_argument("--min-qv", type=float, default=30_000_000.0)
    parser.add_argument("--common-universe-file", default="", help="use common universe log file")
    parser.add_argument("--common-universe-latest", action="store_true", help="use latest common universe log")
    parser.add_argument(
        "--common-universe-cache",
        default=os.path.join("logs", "srp_st_regime_pullback_v1", "common_universe_cache.json"),
        help="cache resolved universe symbols",
    )
    parser.add_argument("--initial-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-pct", type=float, default=1.0)
    parser.add_argument("--entry-base", default="equity", choices=["equity", "fixed"])
    parser.add_argument("--fixed-equity", type=float, default=1000.0)
    parser.add_argument("--slip-pct", type=float, default=0.0)
    parser.add_argument("--pb-lookback", type=int, default=24)
    parser.add_argument("--vol-min-mult", type=float, default=0.8)
    parser.add_argument("--max-st-dist-atr", type=float, default=2.0)
    parser.add_argument("--rsi-long-max", type=float, default=70.0)
    parser.add_argument("--rsi-short-min", type=float, default=30.0)
    parser.add_argument("--r-mult", type=float, default=1.5)
    parser.add_argument("--max-hold-bars", type=int, default=240)
    parser.add_argument("--regime-cooldown-bars", type=int, default=2)
    parser.add_argument("--cooldown-minutes", type=int, default=30)
    parser.add_argument("--st-atr", type=int, default=10)
    parser.add_argument("--st-mult", type=float, default=3.0)
    parser.add_argument("--tf-ltf", default="5m")
    parser.add_argument("--tf-htf", default="1h")
    parser.add_argument("--use-confirmed", action="store_true", help="use previous bar for signal (confirmed)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(ROOT_DIR, "logs", "srp_st_regime_pullback_v1")
    _ensure_dir(log_dir)
    trades_path = os.path.join(log_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(log_dir, f"summary_{run_id}.json")

    def _bt_log(line: str) -> None:
        print(line)
        with open(os.path.join(log_dir, f"run_{run_id}.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")

    with open(trades_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "symbol",
                "side",
                "entry_ts",
                "exit_ts",
                "entry_px",
                "exit_px",
                "pnl_usdt",
                "pnl_pct",
                "exit_reason",
                "hold_bars",
                "mfe",
                "mae",
            ]
        )

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

    symbols = _select_symbols(
        exchange,
        args.symbols,
        args.symbols_file,
        args.universe,
        args.min_qv,
        args.common_universe_file,
        args.common_universe_latest,
        args.common_universe_cache,
    )

    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = _utc_ms(start_dt)
    end_ms = _utc_ms(end_dt)

    trades: List[Dict] = []
    stats_by_symbol: Dict[str, Dict] = {}
    equity = float(args.initial_usdt)

    for symbol in symbols:
        try:
            rows_ltf = _fetch_ohlcv_all(exchange, symbol, args.tf_ltf, start_ms, end_ms)
            rows_htf = _fetch_ohlcv_all(exchange, symbol, args.tf_htf, start_ms, end_ms)
        except Exception:
            continue

        df_5m = _to_df(rows_ltf)
        df_1h = _to_df(rows_htf)
        if df_5m.empty or df_1h.empty:
            continue

        df_5m["ema20"] = _ema(df_5m["close"], 20)
        df_5m["ema50"] = _ema(df_5m["close"], 50)
        df_5m["rsi14"] = _rsi(df_5m["close"], 14)
        df_5m["atr14"] = _atr(df_5m, 14)
        df_5m["vol_sma20"] = _vol_sma(df_5m["volume"], 20)
        st_line_ltf, st_dir_ltf = _supertrend(df_5m, args.st_atr, args.st_mult)
        df_5m["st_line"] = st_line_ltf
        df_5m["st_dir"] = st_dir_ltf

        st_line_htf, st_dir_htf = _supertrend(df_1h, args.st_atr, args.st_mult)
        df_1h["st_line"] = st_line_htf
        df_1h["st_dir"] = st_dir_htf

        htf_flip_idx = -999
        flip_cooldown = []
        last_dir = None
        for i, val in enumerate(df_1h["st_dir"].tolist()):
            if last_dir is None:
                last_dir = val
            elif val != last_dir:
                htf_flip_idx = i
                last_dir = val
            flip_cooldown.append(1 if (i - htf_flip_idx) < int(args.regime_cooldown_bars) else 0)
        df_1h["flip_cd"] = flip_cooldown

        open_position: Optional[Position] = None
        cooldown_left = 0
        tf_minutes = _timeframe_minutes(args.tf_ltf) or 5
        cooldown_bars = max(1, int(round(float(args.cooldown_minutes) / float(tf_minutes))))

        sig_offset = 1 if args.use_confirmed else 0
        start_i = 1 + sig_offset
        for i in range(start_i, len(df_5m) - 1):
            row = df_5m.iloc[i]
            sig_idx = i - sig_offset
            sig_row = df_5m.iloc[sig_idx]
            ts = int(sig_row["ts"]) if args.use_confirmed else int(row["ts"])

            idx_1h = int(df_1h["ts"].searchsorted(ts, side="right") - 1)
            if idx_1h < 0:
                continue
            htf_row = df_1h.iloc[idx_1h]
            htf_dir = int(htf_row["st_dir"])
            flip_cd = int(htf_row["flip_cd"])
            if htf_dir == 0:
                continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            if open_position:
                high_px = float(row["high"])
                low_px = float(row["low"])
                exit_reason = None
                exit_px = None
                if open_position.side == "LONG":
                    open_position.max_favorable = max(open_position.max_favorable, (high_px - open_position.entry_px) / open_position.entry_px)
                    open_position.max_adverse = max(open_position.max_adverse, (open_position.entry_px - low_px) / open_position.entry_px)
                    if low_px <= open_position.stop_px:
                        exit_reason = "SL"
                        exit_px = open_position.stop_px
                    elif high_px >= open_position.tp_px:
                        exit_reason = "TP"
                        exit_px = open_position.tp_px
                else:
                    open_position.max_favorable = max(open_position.max_favorable, (open_position.entry_px - low_px) / open_position.entry_px)
                    open_position.max_adverse = max(open_position.max_adverse, (high_px - open_position.entry_px) / open_position.entry_px)
                    if high_px >= open_position.stop_px:
                        exit_reason = "SL"
                        exit_px = open_position.stop_px
                    elif low_px <= open_position.tp_px:
                        exit_reason = "TP"
                        exit_px = open_position.tp_px

                hold_bars = i - open_position.entry_idx
                if exit_reason is None and hold_bars >= int(args.max_hold_bars):
                    exit_reason = "TIME"
                    exit_px = float(row["close"])

                if exit_reason:
                    pnl_per_unit = (exit_px - open_position.entry_px) if open_position.side == "LONG" else (open_position.entry_px - exit_px)
                    pnl_usdt = pnl_per_unit * open_position.size
                    pnl_pct = pnl_per_unit / open_position.entry_px

                    trades.append(
                        {
                            "symbol": symbol,
                            "side": open_position.side,
                            "entry_ts": open_position.entry_ts,
                            "exit_ts": ts,
                            "entry_px": open_position.entry_px,
                            "exit_px": exit_px,
                            "pnl_usdt": pnl_usdt,
                            "pnl_pct": pnl_pct,
                            "exit_reason": exit_reason,
                            "hold_bars": hold_bars,
                            "mfe": open_position.max_favorable,
                            "mae": open_position.max_adverse,
                        }
                    )
                    with open(trades_path, "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(
                            [
                                symbol,
                                open_position.side,
                                _dt_kst(open_position.entry_ts),
                                _dt_kst(ts),
                                open_position.entry_px,
                                exit_px,
                                pnl_usdt,
                                pnl_pct,
                                exit_reason,
                                hold_bars,
                                open_position.max_favorable,
                                open_position.max_adverse,
                            ]
                        )

                    sym_stats = stats_by_symbol.setdefault(
                        symbol,
                        {
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
                        },
                    )
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += float(open_position.max_favorable)
                    sym_stats["mae_sum"] += float(open_position.max_adverse)
                    sym_stats["hold_sum"] += float(hold_bars)
                    if pnl_usdt > 0:
                        sym_stats["wins"] += 1
                    else:
                        sym_stats["losses"] += 1
                    if exit_reason == "TP":
                        sym_stats["tp"] += 1
                    elif exit_reason == "SL":
                        sym_stats["sl"] += 1

                    equity += pnl_usdt
                    if args.entry_base == "equity":
                        equity = max(0.0, equity)

                    if args.verbose:
                        _bt_log(
                            "SRP_EXIT sym=%s side=%s reason=%s pnl=%.4f"
                            % (symbol, open_position.side, exit_reason, pnl_usdt)
                        )
                    open_position = None
                    cooldown_left = int(cooldown_bars)
                continue

            if flip_cd:
                if args.verbose:
                    _bt_log("SRP_REGIME sym=%s dir=%s flip_cd=1" % (symbol, "LONG" if htf_dir == 1 else "SHORT"))
                continue

            ema20 = float(sig_row["ema20"]) if pd.notna(sig_row["ema20"]) else None
            ema50 = float(sig_row["ema50"]) if pd.notna(sig_row["ema50"]) else None
            rsi14 = float(sig_row["rsi14"]) if pd.notna(sig_row["rsi14"]) else None
            atr14 = float(sig_row["atr14"]) if pd.notna(sig_row["atr14"]) else None
            st_line = float(sig_row["st_line"]) if pd.notna(sig_row["st_line"]) else None
            vol_sma = float(sig_row["vol_sma20"]) if pd.notna(sig_row["vol_sma20"]) else None
            close_px = float(sig_row["close"])
            volume = float(sig_row["volume"])

            if not isinstance(ema20, (int, float)) or not isinstance(ema50, (int, float)):
                continue
            if not isinstance(rsi14, (int, float)) or not isinstance(atr14, (int, float)):
                continue
            if not isinstance(st_line, (int, float)) or not isinstance(vol_sma, (int, float)):
                continue

            start_idx = max(0, sig_idx - int(args.pb_lookback))
            if htf_dir == 1:
                pb_zone = (df_5m["close"].iloc[start_idx : sig_idx + 1] <= df_5m["ema20"].iloc[start_idx : sig_idx + 1]) & (
                    df_5m["close"].iloc[start_idx : sig_idx + 1] >= df_5m["ema50"].iloc[start_idx : sig_idx + 1]
                )
            else:
                pb_zone = (df_5m["close"].iloc[start_idx : sig_idx + 1] >= df_5m["ema20"].iloc[start_idx : sig_idx + 1]) & (
                    df_5m["close"].iloc[start_idx : sig_idx + 1] <= df_5m["ema50"].iloc[start_idx : sig_idx + 1]
                )
            pb_seen = bool(pb_zone.any())

            if args.verbose:
                _bt_log(
                    "SRP_PB sym=%s pb_zone=%d pb_seen=%d ema20=%.6g ema50=%.6g"
                    % (symbol, int(pb_zone.iloc[-1]) if len(pb_zone) > 0 else 0, int(pb_seen), ema20, ema50)
                )

            if not pb_seen:
                continue

            prev = df_5m.iloc[sig_idx - 1]
            prev_close = float(prev["close"])
            prev_ema20 = float(prev["ema20"]) if pd.notna(prev["ema20"]) else None
            if not isinstance(prev_ema20, (int, float)):
                continue

            long_cross = prev_close <= prev_ema20 and close_px > ema20
            short_cross = prev_close >= prev_ema20 and close_px < ema20

            vol_ok = volume >= float(args.vol_min_mult) * vol_sma
            if htf_dir == 1:
                st_dist = (close_px - st_line) / atr14 if atr14 else 999.0
                st_dist_ok = st_dist <= float(args.max_st_dist_atr)
                if (
                    long_cross
                    and close_px >= ema50
                    and rsi14 < float(args.rsi_long_max)
                    and vol_ok
                    and st_dist_ok
                ):
                    if args.verbose:
                        _bt_log(
                            "SRP_ENTRY_READY sym=%s side=LONG trig=EMA20_CROSS rsi=%.2f vol_ok=%d st_dist_atr=%.2f"
                            % (symbol, rsi14, int(vol_ok), st_dist)
                        )
                else:
                    continue
                entry_side = "LONG"
            else:
                st_dist = (st_line - close_px) / atr14 if atr14 else 999.0
                st_dist_ok = st_dist <= float(args.max_st_dist_atr)
                if (
                    short_cross
                    and close_px <= ema50
                    and rsi14 > float(args.rsi_short_min)
                    and vol_ok
                    and st_dist_ok
                ):
                    if args.verbose:
                        _bt_log(
                            "SRP_ENTRY_READY sym=%s side=SHORT trig=EMA20_CROSS rsi=%.2f vol_ok=%d st_dist_atr=%.2f"
                            % (symbol, rsi14, int(vol_ok), st_dist)
                        )
                else:
                    continue
                entry_side = "SHORT"

            entry_idx = i if args.use_confirmed else i + 1
            if entry_idx >= len(df_5m):
                continue

            entry_row = df_5m.iloc[entry_idx]
            entry_px = float(entry_row["open"])
            if args.slip_pct:
                if entry_side == "LONG":
                    entry_px *= 1.0 + float(args.slip_pct)
                else:
                    entry_px *= 1.0 - float(args.slip_pct)

            swing_level = _find_swing_level(df_5m, start_idx, i, entry_side)
            if entry_side == "LONG":
                sl_price = swing_level if swing_level is not None else (st_line - (0.2 * atr14))
                tp_price = entry_px + float(args.r_mult) * (entry_px - sl_price)
            else:
                sl_price = swing_level if swing_level is not None else (st_line + (0.2 * atr14))
                tp_price = entry_px - float(args.r_mult) * (sl_price - entry_px)

            risk_per_unit = abs(entry_px - sl_price)
            if risk_per_unit <= 0:
                continue

            base_equity = equity if args.entry_base == "equity" else float(args.fixed_equity)
            entry_usdt = base_equity * (float(args.entry_pct) / 100.0)
            size = entry_usdt / entry_px

            open_position = Position(
                side=entry_side,
                entry_idx=entry_idx,
                entry_ts=int(entry_row["ts"]),
                entry_px=entry_px,
                stop_px=sl_price,
                tp_px=tp_price,
                size=size,
                risk_per_unit=risk_per_unit,
            )

            sym_stats = stats_by_symbol.setdefault(
                symbol,
                {
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
                },
            )
            sym_stats["entries"] += 1

            if args.verbose:
                _bt_log(
                    "SRP_ENTRY sym=%s side=%s entry=%.6g sl=%.6g tp=%.6g"
                    % (symbol, entry_side, entry_px, sl_price, tp_price)
                )

        if open_position:
            open_position = None

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

    if total_trades == 0:
        _bt_log("[BACKTEST] no trades/entries")


if __name__ == "__main__":
    main()
