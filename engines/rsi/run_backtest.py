#!/usr/bin/env python3
import argparse
import bisect
import csv
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

from engines.base import EngineContext
from engines.rsi.engine import RsiEngine
from engines.rsi.config import RsiConfig
import cycle_cache


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
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df


def _safe_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _timeframe_minutes(tf: str) -> int:
    raw = (tf or "").strip().lower()
    if raw.endswith("m"):
        return int(raw[:-1])
    if raw.endswith("h"):
        return int(raw[:-1]) * 60
    if raw.endswith("d"):
        return int(raw[:-1]) * 1440
    return 0


def _read_common_cache(symbol: str, tf: str, min_rows: int) -> Optional[List[list]]:
    cache_dir = os.getenv("COMMON_OHLCV_CACHE_DIR", os.path.join("logs", "common_ohlcv_cache"))
    safe = _safe_symbol(symbol)
    if not os.path.isdir(cache_dir):
        return None
    prefix = f"{safe}_{tf}_"
    best_limit = -1
    best_path = None
    for name in os.listdir(cache_dir):
        if not name.startswith(prefix) or not name.endswith(".csv"):
            continue
        try:
            limit = int(name[len(prefix) : -4])
        except Exception:
            continue
        if limit > best_limit:
            best_limit = limit
            best_path = os.path.join(cache_dir, name)
    if not best_path:
        return None
    try:
        df = pd.read_csv(best_path)
        if df.empty or len(df) < min_rows:
            return None
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return None


def _symbols_from_common_cache(exchange: ccxt.Exchange, cache_dir: str) -> List[str]:
    if not cache_dir or not os.path.isdir(cache_dir):
        return []
    safe_map: Dict[str, str] = {}
    for sym in getattr(exchange, "symbols", []) or []:
        safe_map[_safe_symbol(sym)] = sym
    symbols: List[str] = []
    for name in os.listdir(cache_dir):
        if not name.endswith(".csv"):
            continue
        base = name[:-4]
        parts = base.split("_")
        if len(parts) < 3:
            continue
        safe = "_".join(parts[:-2])
        sym = safe_map.get(safe)
        if sym and sym not in symbols:
            symbols.append(sym)
    return symbols


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _write_csv_header(path: str, columns: List[str]) -> None:
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)


def _append_csv(path: str, row: List) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


@dataclass
class PendingEntry:
    entry_idx: int
    signal_idx: int
    entry_ts: int
    entry_px: float


@dataclass
class TradeState:
    entry_idx: int
    entry_ts: int
    entry_px: float
    sl_px: float
    tp_px: float
    high_max: float
    low_min: float
    exit_idx: Optional[int] = None
    exit_ts: Optional[int] = None
    exit_px: Optional[float] = None
    exit_reason: Optional[str] = None


def _calc_pnl_short(entry_px: float, exit_px: float, fee_rate: float, slip_pct: float) -> float:
    entry_fill = entry_px * (1 - slip_pct)
    exit_fill = exit_px * (1 + slip_pct)
    fees = entry_fill * fee_rate + exit_fill * fee_rate
    return (entry_fill - exit_fill - fees) / entry_fill if entry_fill > 0 else 0.0


def _rsi_wilder(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = avg_loss.replace(0, float("nan"))
    rs = (avg_gain / avg_loss).astype(float)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


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
    return tr.rolling(length).mean()


def _volume_surge_5m(df_5m: pd.DataFrame, lookback: int, mult: float) -> Tuple[bool, float, float]:
    if len(df_5m) < lookback + 2:
        return False, 0.0, 0.0
    cur = float(df_5m["volume"].iloc[-1])
    avg = float(df_5m["volume"].iloc[-(lookback + 1):-1].mean())
    return (avg > 0 and cur >= avg * mult), cur, avg


def _structural_rejection_5m(df_5m: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
    if len(df_5m) < 3:
        return False, {}
    highs = df_5m["high"].iloc[-3:].tolist()
    lows = df_5m["low"].iloc[-3:].tolist()
    opens = df_5m["open"].iloc[-3:].tolist()
    closes = df_5m["close"].iloc[-3:].tolist()

    lower_highs = highs[-3] > highs[-2] and highs[-2] > highs[-1]

    def upper_wick_ratio(idx: int) -> float:
        high = highs[idx]
        low = lows[idx]
        o = opens[idx]
        c = closes[idx]
        rng = high - low
        if rng <= 0:
            return 0.0
        upper = high - max(o, c)
        return max(0.0, upper / rng)

    upper_wick_ratio_1 = upper_wick_ratio(-1)
    upper_wick_ratio_2 = upper_wick_ratio(-2)
    wick_reject = upper_wick_ratio_1 >= 0.30 and upper_wick_ratio_2 >= 0.30

    metrics = {
        "lower_highs": lower_highs,
        "upper_wick_ratio_1": upper_wick_ratio_1,
        "upper_wick_ratio_2": upper_wick_ratio_2,
        "wick_reject": wick_reject,
    }
    return (lower_highs or wick_reject), metrics


def _impulse_block(df_5m: pd.DataFrame) -> bool:
    if len(df_5m) < 60:
        return False
    ema7 = float(_ema(df_5m["close"], 7).iloc[-1])
    ema20 = float(_ema(df_5m["close"], 20).iloc[-1])
    ema60 = float(_ema(df_5m["close"], 60).iloc[-1])
    atr20 = float(_atr(df_5m, 20).iloc[-1])
    close_5m = float(df_5m["close"].iloc[-1])
    return close_5m > ema7 and (ema7 - ema20) > atr20 * 0.6 and (ema20 - ema60) > atr20 * 0.6


def _slice_until(df: pd.DataFrame, idx: int) -> pd.DataFrame:
    if idx < 0:
        return df.iloc[:0]
    return df.iloc[: idx + 1]


def parse_args():
    parser = argparse.ArgumentParser(description="RSI Short Backtest Runner")
    parser.add_argument("--symbols", type=str, default="")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start", type=str, default="", help="UTC start, format: YYYY-MM-DD HH:MM")
    parser.add_argument("--end", type=str, default="", help="UTC end, format: YYYY-MM-DD HH:MM")
    parser.add_argument("--start-kst", type=str, default="", help="KST start, format: YYYY-MM-DD HH:MM")
    parser.add_argument("--end-kst", type=str, default="", help="KST end, format: YYYY-MM-DD HH:MM")
    parser.add_argument("--tf-base", type=str, default="3m")
    parser.add_argument("--out-signals", type=str, default="rsi_signals.csv")
    parser.add_argument("--out-trades", type=str, default="rsi_trades.csv")
    parser.add_argument("--log-path", type=str, default="backtest.log")
    parser.add_argument("--entry-mode", type=str, default="NEXT_OPEN", choices=["NEXT_OPEN", "SIGNAL_CLOSE"])
    parser.add_argument("--use-confirmed", action="store_true", default=True, help="use previous candle for signal")
    parser.add_argument("--no-use-confirmed", action="store_false", dest="use_confirmed")
    parser.add_argument("--reentry-minutes", type=int, default=60)
    parser.add_argument("--sl-pct", type=float, default=0.02)
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--timeout-bars", type=int, default=40)
    parser.add_argument("--rsi-thresholds", type=str, default="", help="override thresholds JSON, e.g. '{\"3m\":80,\"5m\":80,\"15m\":78,\"1h\":77}'")
    parser.add_argument("--rsi3m-downturn-threshold", type=float, default=None, help="3m RSI downturn threshold")
    parser.add_argument("--vol-surge-lookback", type=int, default=None, help="5m volume surge lookback bars")
    parser.add_argument("--vol-surge-mult", type=float, default=None, help="5m volume surge multiplier")
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--slippage-pct", type=float, default=0.0)
    parser.add_argument("--position-usdt", type=float, default=100.0)
    parser.add_argument("--use-common-cache", action="store_true", default=True)
    parser.add_argument("--no-use-common-cache", action="store_false", dest="use_common_cache")
    parser.add_argument("--symbols-from-common-cache", action="store_true", default=False)
    parser.add_argument("--common-cache-dir", type=str, default="")
    parser.add_argument("--replay-entries-log", type=str, default="", help="replay live rsi_entries log for signal audit")
    parser.add_argument("--replay-out", type=str, default="rsi_replay.csv")
    parser.add_argument("--warmup-hours", type=int, default=48, help="extra history (hours) for indicator warmup")
    return parser.parse_args()


def _parse_utc_dt(text: str) -> Optional[datetime]:
    raw = (text or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _parse_kst_dt(text: str) -> Optional[datetime]:
    raw = (text or "").strip()
    if not raw:
        return None
    kst = timezone(timedelta(hours=9))
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=kst).astimezone(timezone.utc)
        except Exception:
            continue
    return None


def _parse_kst_ts(text: str) -> Optional[int]:
    dt = _parse_kst_dt(text)
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def _load_replay_entries(path: str) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    if not path or not os.path.exists(path):
        return entries
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            # Example:
            # 2026-02-01 06:43:05 engine=rsi side=SHORT symbol=C98/USDT:USDT price=0.0263 ...
            parts = raw.split()
            if len(parts) < 4:
                continue
            ts_text = " ".join(parts[:2])
            meta = {"ts_text": ts_text, "raw": raw}
            for p in parts[2:]:
                if "=" in p:
                    k, v = p.split("=", 1)
                    meta[k.strip()] = v.strip()
            entries.append(meta)
    return entries


def main() -> None:
    args = parse_args()
    replay_entries = _load_replay_entries(args.replay_entries_log)
    start_dt = None
    end_dt = None
    if args.start_kst or args.end_kst:
        start_dt = _parse_kst_dt(args.start_kst)
        end_dt = _parse_kst_dt(args.end_kst)
    elif args.start or args.end:
        start_dt = _parse_utc_dt(args.start)
        end_dt = _parse_utc_dt(args.end)
    elif replay_entries:
        ts_vals = []
        for ent in replay_entries:
            ts_ms = _parse_kst_ts(ent.get("ts_text", ""))
            if ts_ms is not None:
                ts_vals.append(ts_ms)
        if ts_vals:
            min_ts = min(ts_vals)
            max_ts = max(ts_vals)
            start_dt = datetime.fromtimestamp((min_ts / 1000.0), tz=timezone.utc) - timedelta(hours=6)
            end_dt = datetime.fromtimestamp((max_ts / 1000.0), tz=timezone.utc) + timedelta(hours=6)

    if end_dt is None:
        end_dt = datetime.now(timezone.utc)
    if start_dt is None:
        start_dt = end_dt - timedelta(days=args.days)

    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)
    warmup_hours = max(0, int(args.warmup_hours or 0))
    fetch_start_dt = start_dt - timedelta(hours=warmup_hours) if warmup_hours > 0 else start_dt
    fetch_start_ms = int(fetch_start_dt.timestamp() * 1000)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    base_dir = os.path.join("logs", "rsi", "backtest")
    _ensure_dir(base_dir)
    out_signals = os.path.join(base_dir, os.path.basename(args.out_signals)) if args.out_signals else ""
    out_trades = os.path.join(base_dir, os.path.basename(args.out_trades)) if args.out_trades else ""
    log_path = os.path.join(base_dir, os.path.basename(args.log_path)) if args.log_path else ""
    replay_out = ""
    if replay_entries:
        replay_out = os.path.join(base_dir, os.path.basename(args.replay_out)) if args.replay_out else ""
    if out_signals:
        _ensure_dir(os.path.dirname(out_signals))
    if out_trades:
        _ensure_dir(os.path.dirname(out_trades))
    if log_path:
        _ensure_dir(os.path.dirname(log_path))
    log_fp = open(log_path, "a", encoding="utf-8") if log_path else open(os.devnull, "w", encoding="utf-8")

    signal_cols = [
        "ts",
        "symbol",
        "event",
        "entry_px",
        "reason",
        "rsi1h",
        "rsi15m",
        "rsi5m",
        "rsi3m",
        "vol_cur",
        "vol_avg",
        "struct_ok",
    ]
    trade_cols = [
        "symbol",
        "entry_ts",
        "entry_px",
        "exit_ts",
        "exit_px",
        "exit_reason",
        "pnl_pct",
        "pnl_usdt",
        "holding_bars",
        "mfe",
        "mae",
        "sl_px",
        "tp_px",
    ]
    if out_signals:
        _write_csv_header(out_signals, signal_cols)
    if out_trades:
        _write_csv_header(out_trades, trade_cols)
    if replay_out:
        _write_csv_header(
            replay_out,
            [
                "ts",
                "symbol",
                "ready_entry",
                "reason",
                "rsi1h",
                "rsi15m",
                "rsi5m",
                "rsi5m_prev",
                "rsi3m",
                "rsi3m_prev",
                "rsi3m_prev2",
                "vol_ok",
                "vol_cur",
                "vol_avg",
                "struct_ok",
                "lower_highs",
                "wick_reject",
                "impulse_block",
                "spike_ready",
                "struct_ready",
                "raw",
            ],
        )

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    exchange.load_markets()

    cfg = RsiConfig()
    if args.rsi_thresholds:
        try:
            parsed = json.loads(args.rsi_thresholds)
        except Exception:
            try:
                parsed = ast.literal_eval(args.rsi_thresholds)
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            cfg.thresholds = parsed
    if args.rsi3m_downturn_threshold is not None:
        cfg.rsi3m_downturn_threshold = float(args.rsi3m_downturn_threshold)
    if args.vol_surge_lookback is not None:
        cfg.vol_surge_lookback = int(args.vol_surge_lookback)
    if args.vol_surge_mult is not None:
        cfg.vol_surge_mult = float(args.vol_surge_mult)
    engine = RsiEngine(cfg)
    if not symbols and args.symbols_from_common_cache:
        cache_dir = args.common_cache_dir.strip() or os.getenv("COMMON_OHLCV_CACHE_DIR", os.path.join("logs", "common_ohlcv_cache"))
        symbols = _symbols_from_common_cache(exchange, cache_dir)
    if not symbols:
        tickers = exchange.fetch_tickers()
        state = {"_tickers": tickers, "_symbols": list(tickers.keys())}
        ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=lambda *_: None, config=cfg)
        engine.on_start(ctx)
        symbols = engine.build_universe(ctx)
        if not symbols:
            log_fp.write("[BACKTEST] No symbols from build_universe().\n")
            log_fp.flush()
            return
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    if replay_entries:
        replay_syms = [e.get("symbol") for e in replay_entries if e.get("symbol")]
        for sym in replay_syms:
            if sym not in symbols:
                symbols.append(sym)

    tfs = {
        "1h": "1h",
        "15m": "15m",
        "5m": "5m",
        "3m": args.tf_base,
    }

    total = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }
    tf_minutes = float(exchange.parse_timeframe(args.tf_base)) / 60.0
    replay_by_symbol: Dict[str, List[Dict[str, str]]] = {}
    if replay_entries:
        for ent in replay_entries:
            sym = ent.get("symbol", "")
            if sym:
                replay_by_symbol.setdefault(sym, []).append(ent)
        for sym in replay_by_symbol:
            replay_by_symbol[sym].sort(key=lambda x: x.get("ts_text", ""))
    for symbol in symbols:
        log_fp.write(f"[rsi] fetch sym={symbol} start={fetch_start_ms} end={end_ms}\n")
        log_fp.flush()
        def _fetch_with_cache(tf: str) -> List[list]:
            minutes = _timeframe_minutes(tf)
            min_rows = int((end_ms - fetch_start_ms) / (minutes * 60 * 1000)) + 2 if minutes else 0
            if args.use_common_cache and min_rows > 0:
                cached = _read_common_cache(symbol, tf, min_rows)
                if cached:
                    return cached
            return _fetch_ohlcv_all(exchange, symbol, tf, fetch_start_ms, end_ms)

        raw_1h = _fetch_with_cache(tfs["1h"])
        raw_15m = _fetch_with_cache(tfs["15m"])
        raw_5m = _fetch_with_cache(tfs["5m"])
        raw_3m = _fetch_with_cache(tfs["3m"])
        df_1h = _to_df(raw_1h)
        df_15m = _to_df(raw_15m)
        df_5m = _to_df(raw_5m)
        df_3m = _to_df(raw_3m)
        if df_3m.empty or len(df_3m) < 50:
            log_fp.write(f"[rsi] empty_or_short sym={symbol} rows={len(df_3m)}\n")
            log_fp.flush()
            continue

        ts_1h = [int(x) for x in df_1h["ts"].tolist()]
        ts_15m = [int(x) for x in df_15m["ts"].tolist()]
        ts_5m = [int(x) for x in df_5m["ts"].tolist()]
        ts_3m = [int(x) for x in df_3m["ts"].tolist()]
        i1h = i15m = i5m = -1
        pending: Optional[PendingEntry] = None
        trade: Optional[TradeState] = None
        reentry_until = 0.0
        trade_stats: Dict[str, float] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "tp": 0,
            "sl": 0,
            "mfe_sum": 0.0,
            "mae_sum": 0.0,
            "hold_sum": 0.0,
        }

        start_i = 1 if args.use_confirmed else 0
        for i in range(start_i, len(df_3m)):
            ts = int(df_3m["ts"].iloc[i])
            o = float(df_3m["open"].iloc[i])
            h = float(df_3m["high"].iloc[i])
            l = float(df_3m["low"].iloc[i])
            c = float(df_3m["close"].iloc[i])
            if ts < start_ms:
                continue

            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx < 0:
                continue
            sig_ts = int(df_3m["ts"].iloc[sig_idx])

            if args.use_confirmed:
                i1h = bisect.bisect_right(ts_1h, sig_ts) - 1
                i15m = bisect.bisect_right(ts_15m, sig_ts) - 1
                i5m = bisect.bisect_right(ts_5m, sig_ts) - 1
            else:
                while (i1h + 1) < len(df_1h) and int(df_1h["ts"].iloc[i1h + 1]) <= ts:
                    i1h += 1
                while (i15m + 1) < len(df_15m) and int(df_15m["ts"].iloc[i15m + 1]) <= ts:
                    i15m += 1
                while (i5m + 1) < len(df_5m) and int(df_5m["ts"].iloc[i5m + 1]) <= ts:
                    i5m += 1

            slice_1h = _slice_until(df_1h, i1h)
            slice_15m = _slice_until(df_15m, i15m)
            slice_5m = _slice_until(df_5m, i5m)
            slice_3m = _slice_until(df_3m, sig_idx)

            if trade is not None:
                trade.high_max = max(trade.high_max, h)
                trade.low_min = min(trade.low_min, l)
                sl_hit = h >= trade.sl_px
                tp_hit = l <= trade.tp_px
                exit_reason = None
                exit_px = None
                if sl_hit and tp_hit:
                    exit_reason = "SL"
                    exit_px = trade.sl_px
                elif sl_hit:
                    exit_reason = "SL"
                    exit_px = trade.sl_px
                elif tp_hit:
                    exit_reason = "TP"
                    exit_px = trade.tp_px
                elif args.timeout_bars > 0 and (i - trade.entry_idx) >= args.timeout_bars:
                    exit_reason = "TIMEOUT"
                    exit_px = c
                if exit_reason:
                    trade.exit_idx = i
                    trade.exit_ts = ts
                    trade.exit_px = exit_px
                    trade.exit_reason = exit_reason
                    pnl_pct = _calc_pnl_short(trade.entry_px, exit_px, args.fee_rate, args.slippage_pct)
                    pnl_usdt = pnl_pct * args.position_usdt
                    holding_bars = i - trade.entry_idx + 1
                    mfe = (trade.entry_px - trade.low_min) / trade.entry_px if trade.entry_px > 0 else 0.0
                    mae = (trade.high_max - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
                    hold_min = holding_bars * tf_minutes
                    if out_trades:
                        _append_csv(
                            out_trades,
                            [
                                symbol,
                                trade.entry_ts,
                                f"{trade.entry_px:.6g}",
                                trade.exit_ts,
                                f"{trade.exit_px:.6g}",
                                trade.exit_reason,
                                f"{pnl_pct:.6f}",
                                f"{pnl_usdt:.6f}",
                                holding_bars,
                                f"{mfe:.6f}",
                                f"{mae:.6f}",
                                f"{trade.sl_px:.6g}",
                                f"{trade.tp_px:.6g}",
                            ],
                        )
                    trade_stats["trades"] += 1
                    trade_stats["mfe_sum"] += mfe
                    trade_stats["mae_sum"] += mae
                    trade_stats["hold_sum"] += hold_min
                    if exit_reason == "TP":
                        trade_stats["wins"] += 1
                        trade_stats["tp"] += 1
                    else:
                        trade_stats["losses"] += 1
                        if exit_reason == "SL":
                            trade_stats["sl"] += 1
                    trade = None
                    if args.reentry_minutes and args.reentry_minutes > 0:
                        reentry_until = (ts / 1000.0) + (args.reentry_minutes * 60)

            if pending is not None and i == pending.entry_idx:
                entry_px = o if args.entry_mode == "NEXT_OPEN" else c
                sl_px = entry_px * (1 + args.sl_pct)
                tp_px = entry_px * (1 - args.tp_pct)
                trade = TradeState(
                    entry_idx=i,
                    entry_ts=ts,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp_px=tp_px,
                    high_max=h,
                    low_min=l,
                )
                pending = None

            if len(slice_1h) < max(20, cfg.rsi_len + 2) or len(slice_15m) < 20 or len(slice_5m) < 20:
                continue

            r1h = float(_rsi_wilder(slice_1h["close"], cfg.rsi_len).iloc[-1]) if len(slice_1h) >= cfg.rsi_len else None
            r15 = float(_rsi_wilder(slice_15m["close"], cfg.rsi_len).iloc[-1]) if len(slice_15m) >= cfg.rsi_len else None
            r5_series = _rsi_wilder(slice_5m["close"], cfg.rsi_len)
            r5 = float(r5_series.iloc[-1]) if len(r5_series) >= 1 else None
            r5_prev = float(r5_series.iloc[-2]) if len(r5_series) >= 2 else None
            r3_series = _rsi_wilder(slice_3m["close"], cfg.rsi_len)
            r3m_val = float(r3_series.iloc[-3]) if len(r3_series) >= 3 else None
            r3_prev = float(r3_series.iloc[-2]) if len(r3_series) >= 2 else None
            r3 = float(r3_series.iloc[-1]) if len(r3_series) >= 1 else None

            passed_1h = (r1h is not None) and (r1h >= cfg.thresholds["1h"])
            passed_15m = (r15 is not None) and (r15 >= cfg.thresholds["15m"])
            passed_5m = (r5 is not None) and (r5 >= cfg.thresholds["5m"])
            ok_tf = passed_15m and passed_5m

            rsi5m_downturn = r5_prev is not None and r5 is not None and r5_prev > r5
            thr_3m_downturn = float(cfg.rsi3m_downturn_threshold)
            rsi3m_downturn = (
                (r3_prev is not None and r3 is not None and r3_prev >= thr_3m_downturn and r3_prev > r3)
                or (r3m_val is not None and r3_prev is not None and r3m_val >= thr_3m_downturn and r3m_val > r3_prev)
            )

            vol_ok, vol_cur, vol_avg = _volume_surge_5m(slice_5m, cfg.vol_surge_lookback, cfg.vol_surge_mult)
            struct_ok, _ = _structural_rejection_5m(slice_5m)
            impulse_block = _impulse_block(slice_5m)

            trigger_ok = (rsi3m_downturn or struct_ok)
            spike_ready = (
                (r3 is not None)
                and (r3 >= cfg.rsi3m_spike_threshold)
                and rsi3m_downturn
                and vol_ok
                and passed_1h
                and passed_15m
            )
            struct_ready = ok_tf and vol_ok and struct_ok and rsi5m_downturn and (not impulse_block)
            ready_entry = spike_ready or struct_ready

            if ready_entry and trade is None and pending is None:
                if args.reentry_minutes and args.reentry_minutes > 0:
                    if (ts / 1000.0) < reentry_until:
                        continue
                reason = "spike_ready" if spike_ready else "struct_ready"
                if out_signals:
                    _append_csv(
                        out_signals,
                        [
                            ts,
                            symbol,
                            "ENTRY_READY",
                            f"{c:.6g}",
                            reason,
                            f"{r1h:.2f}" if r1h is not None else "",
                            f"{r15:.2f}" if r15 is not None else "",
                            f"{r5:.2f}" if r5 is not None else "",
                            f"{r3:.2f}" if r3 is not None else "",
                            f"{vol_cur:.2f}",
                            f"{vol_avg:.2f}",
                            "Y" if struct_ok else "N",
                        ],
                    )
                if args.use_confirmed:
                    entry_px = o if args.entry_mode == "NEXT_OPEN" else float(df_3m["close"].iloc[sig_idx])
                    sl_px = entry_px * (1 + args.sl_pct)
                    tp_px = entry_px * (1 - args.tp_pct)
                    trade = TradeState(
                        entry_idx=i,
                        entry_ts=ts,
                        entry_px=entry_px,
                        sl_px=sl_px,
                        tp_px=tp_px,
                        high_max=h,
                        low_min=l,
                    )
                else:
                    if args.entry_mode == "SIGNAL_CLOSE":
                        entry_px = c
                        sl_px = entry_px * (1 + args.sl_pct)
                        tp_px = entry_px * (1 - args.tp_pct)
                        trade = TradeState(
                            entry_idx=i,
                            entry_ts=ts,
                            entry_px=entry_px,
                            sl_px=sl_px,
                            tp_px=tp_px,
                            high_max=h,
                            low_min=l,
                        )
                    else:
                        if (i + 1) < len(df_3m):
                            pending = PendingEntry(entry_idx=i + 1, signal_idx=i, entry_ts=ts, entry_px=c)

        if replay_out and symbol in replay_by_symbol:
            for ent in replay_by_symbol[symbol]:
                ts_ms = _parse_kst_ts(ent.get("ts_text", ""))
                if ts_ms is None:
                    continue
                sig_ts = ts_ms
                sig_idx = bisect.bisect_right(ts_3m, sig_ts) - 1
                if args.use_confirmed:
                    sig_idx = max(-1, sig_idx)
                if sig_idx < 0:
                    continue
                sig_ts = int(df_3m["ts"].iloc[sig_idx])
                i1h = bisect.bisect_right(ts_1h, sig_ts) - 1
                i15m = bisect.bisect_right(ts_15m, sig_ts) - 1
                i5m = bisect.bisect_right(ts_5m, sig_ts) - 1

                slice_1h = _slice_until(df_1h, i1h)
                slice_15m = _slice_until(df_15m, i15m)
                slice_5m = _slice_until(df_5m, i5m)
                slice_3m = _slice_until(df_3m, sig_idx)
                if len(slice_1h) < max(20, cfg.rsi_len + 2) or len(slice_15m) < 20 or len(slice_5m) < 20:
                    continue

                r1h = float(_rsi_wilder(slice_1h["close"], cfg.rsi_len).iloc[-1]) if len(slice_1h) >= cfg.rsi_len else None
                r15 = float(_rsi_wilder(slice_15m["close"], cfg.rsi_len).iloc[-1]) if len(slice_15m) >= cfg.rsi_len else None
                r5_series = _rsi_wilder(slice_5m["close"], cfg.rsi_len)
                r5 = float(r5_series.iloc[-1]) if len(r5_series) >= 1 else None
                r5_prev = float(r5_series.iloc[-2]) if len(r5_series) >= 2 else None
                r3_series = _rsi_wilder(slice_3m["close"], cfg.rsi_len)
                r3m_val = float(r3_series.iloc[-3]) if len(r3_series) >= 3 else None
                r3_prev = float(r3_series.iloc[-2]) if len(r3_series) >= 2 else None
                r3 = float(r3_series.iloc[-1]) if len(r3_series) >= 1 else None

                passed_1h = (r1h is not None) and (r1h >= cfg.thresholds["1h"])
                passed_15m = (r15 is not None) and (r15 >= cfg.thresholds["15m"])
                passed_5m = (r5 is not None) and (r5 >= cfg.thresholds["5m"])
                ok_tf = passed_15m and passed_5m

                rsi5m_downturn = r5_prev is not None and r5 is not None and r5_prev > r5
                thr_3m_downturn = float(cfg.rsi3m_downturn_threshold)
                rsi3m_downturn = (
                    (r3_prev is not None and r3 is not None and r3_prev >= thr_3m_downturn and r3_prev > r3)
                    or (r3m_val is not None and r3_prev is not None and r3m_val >= thr_3m_downturn and r3m_val > r3_prev)
                )

                vol_ok, vol_cur, vol_avg = _volume_surge_5m(slice_5m, cfg.vol_surge_lookback, cfg.vol_surge_mult)
                struct_ok, struct_metrics = _structural_rejection_5m(slice_5m)
                impulse_block = _impulse_block(slice_5m)

                spike_ready = (
                    (r3 is not None)
                    and (r3 >= cfg.rsi3m_spike_threshold)
                    and rsi3m_downturn
                    and vol_ok
                    and passed_1h
                    and passed_15m
                )
                struct_ready = ok_tf and vol_ok and struct_ok and rsi5m_downturn and (not impulse_block)
                ready_entry = spike_ready or struct_ready
                reason = "spike_ready" if spike_ready else ("struct_ready" if struct_ready else "")

                _append_csv(
                    replay_out,
                    [
                        sig_ts,
                        symbol,
                        "Y" if ready_entry else "N",
                        reason,
                        f"{r1h:.2f}" if r1h is not None else "",
                        f"{r15:.2f}" if r15 is not None else "",
                        f"{r5:.2f}" if r5 is not None else "",
                        f"{r5_prev:.2f}" if r5_prev is not None else "",
                        f"{r3:.2f}" if r3 is not None else "",
                        f"{r3_prev:.2f}" if r3_prev is not None else "",
                        f"{r3m_val:.2f}" if r3m_val is not None else "",
                        "Y" if vol_ok else "N",
                        f"{vol_cur:.2f}",
                        f"{vol_avg:.2f}",
                        "Y" if struct_ok else "N",
                        "Y" if struct_metrics.get("lower_highs") else "N",
                        "Y" if struct_metrics.get("wick_reject") else "N",
                        "Y" if impulse_block else "N",
                        "Y" if spike_ready else "N",
                        "Y" if struct_ready else "N",
                        ent.get("raw", ""),
                    ],
                )

        if trade is not None:
            last_idx = len(df_3m) - 1
            last_ts = int(df_3m["ts"].iloc[last_idx])
            last_close = float(df_3m["close"].iloc[last_idx])
            pnl_pct = _calc_pnl_short(trade.entry_px, last_close, args.fee_rate, args.slippage_pct)
            pnl_usdt = pnl_pct * args.position_usdt
            holding_bars = last_idx - trade.entry_idx + 1
            mfe = (trade.entry_px - trade.low_min) / trade.entry_px if trade.entry_px > 0 else 0.0
            mae = (trade.high_max - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
            hold_min = holding_bars * tf_minutes
            if out_trades:
                _append_csv(
                    out_trades,
                    [
                        symbol,
                        trade.entry_ts,
                        f"{trade.entry_px:.6g}",
                        last_ts,
                        f"{last_close:.6g}",
                        "EOT",
                        f"{pnl_pct:.6f}",
                        f"{pnl_usdt:.6f}",
                        holding_bars,
                        f"{mfe:.6f}",
                        f"{mae:.6f}",
                        f"{trade.sl_px:.6g}",
                        f"{trade.tp_px:.6g}",
                    ],
                )
            trade_stats["trades"] += 1
            trade_stats["mfe_sum"] += mfe
            trade_stats["mae_sum"] += mae
            trade_stats["hold_sum"] += hold_min
            trade_stats["losses"] += 1

        trades = int(trade_stats["trades"])
        wins = int(trade_stats["wins"])
        losses = int(trade_stats["losses"])
        tp = int(trade_stats["tp"])
        sl = int(trade_stats["sl"])
        win_rate = (wins / trades * 100.0) if trades > 0 else 0.0
        avg_mfe = (trade_stats["mfe_sum"] / trades) if trades > 0 else 0.0
        avg_mae = (trade_stats["mae_sum"] / trades) if trades > 0 else 0.0
        avg_hold = (trade_stats["hold_sum"] / trades) if trades > 0 else 0.0
        if trades > 0:
            summary_line = (
                f"[BACKTEST] {symbol} trades={trades} wins={wins} losses={losses} "
                f"winrate={win_rate:.2f}% tp={tp} sl={sl} "
                f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}"
            )
            log_fp.write(summary_line + "\n")
            log_fp.flush()
            print(summary_line)
        total["trades"] += trades
        total["wins"] += wins
        total["losses"] += losses
        total["tp"] += tp
        total["sl"] += sl
        total["mfe_sum"] += trade_stats["mfe_sum"]
        total["mae_sum"] += trade_stats["mae_sum"]
        total["hold_sum"] += trade_stats["hold_sum"]

    total_trades = total["trades"]
    total_winrate = (total["wins"] / total_trades * 100.0) if total_trades > 0 else 0.0
    total_avg_mfe = (total["mfe_sum"] / total_trades) if total_trades > 0 else 0.0
    total_avg_mae = (total["mae_sum"] / total_trades) if total_trades > 0 else 0.0
    total_avg_hold = (total["hold_sum"] / total_trades) if total_trades > 0 else 0.0
    total_line = (
        f"[BACKTEST] TOTAL trades={total_trades} wins={total['wins']} losses={total['losses']} "
        f"winrate={total_winrate:.2f}% tp={total['tp']} sl={total['sl']} "
        f"avg_mfe={total_avg_mfe:.4f} avg_mae={total_avg_mae:.4f} avg_hold={total_avg_hold:.1f}"
    )
    log_fp.write(total_line + "\n")
    log_fp.flush()
    print(total_line)

    log_fp.close()


if __name__ == "__main__":
    main()
