#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

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


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


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
    universe_mode: str,
    adv_min_qv: float,
    adv_top_n: int,
) -> List[str]:
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            return [s.strip() for s in f.read().split(",") if s.strip()]
    if symbols_arg:
        return [s.strip() for s in symbols_arg.split(",") if s.strip()]

    raw = (universe_arg or "").strip().lower()
    if raw.startswith("top"):
        try:
            top_n = int(raw.replace("top", ""))
        except Exception:
            top_n = 50
    else:
        top_n = 50

    tickers = exchange.fetch_tickers()
    anchors = ("BTC/USDT:USDT", "ETH/USDT:USDT")
    shared_universe = build_universe_from_tickers(
        tickers,
        min_quote_volume_usdt=min_qv,
        top_n=top_n,
        anchors=anchors,
    )
    if (universe_mode or "").lower() != "adv_trend":
        return shared_universe

    adv_candidates = []
    for sym, t in (tickers or {}).items():
        if not isinstance(t, dict):
            continue
        pct = t.get("percentage")
        qv = t.get("quoteVolume")
        if pct is None or qv is None:
            continue
        try:
            pct = float(pct)
            qv = float(qv)
        except Exception:
            continue
        if qv < float(adv_min_qv):
            continue
        adv_candidates.append((sym, abs(pct)))
    adv_candidates.sort(key=lambda x: x[1])
    low_vol = [sym for sym, _ in adv_candidates[: int(adv_top_n)]]
    return list(dict.fromkeys(list(shared_universe) + low_vol))


@dataclass
class Position:
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    tp_px: float
    sl_px: float
    size: float
    max_favorable: float = 0.0
    max_adverse: float = 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--symbols-file", default="")
    parser.add_argument("--universe", default="top50")
    parser.add_argument("--min-qv", type=float, default=30_000_000.0)
    parser.add_argument("--universe-mode", default="adv_trend", choices=["adv_trend", "simple"])
    parser.add_argument("--adv-min-qv", type=float, default=float(os.getenv("ADV_TREND_MIN_QV", "5000000")))
    parser.add_argument("--adv-top-n", type=int, default=int(os.getenv("ADV_TREND_UNIVERSE_TOP_N", "30")))
    parser.add_argument("--initial-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-pct", type=float, default=1.0)
    parser.add_argument("--entry-base", default="equity", choices=["equity", "fixed"])
    parser.add_argument("--fixed-equity", type=float, default=1000.0)
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--sl-pct", type=float, default=0.02)
    parser.add_argument("--slip-pct", type=float, default=0.0)
    parser.add_argument("--rsi-len", type=int, default=14)
    parser.add_argument("--rsi-long-min", type=float, default=70.0)
    parser.add_argument("--rsi-short-max", type=float, default=30.0)
    parser.add_argument("--breakout-lookback", type=int, default=10)
    parser.add_argument("--vol-lookback", type=int, default=5)
    parser.add_argument("--vol-mult", type=float, default=2.5)
    parser.add_argument("--wick-min-ratio", type=float, default=0.1)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(ROOT_DIR, "logs", "liquidity_trap_v1", "backtest")
    _ensure_dir(log_dir)
    trades_path = os.path.join(log_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(log_dir, f"summary_{run_id}.json")
    log_path = os.path.join(log_dir, f"backtest_{run_id}.log")

    def _bt_log(line: str) -> None:
        print(line)
        with open(log_path, "a", encoding="utf-8") as f:
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

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})

    symbols = _select_symbols(
        exchange,
        args.symbols,
        args.symbols_file,
        args.universe,
        args.min_qv,
        args.universe_mode,
        args.adv_min_qv,
        args.adv_top_n,
    )
    if not symbols:
        _bt_log("[backtest] no symbols")
        return

    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = _utc_ms(start_dt)
    end_ms = _utc_ms(end_dt)

    _bt_log(
        "[run] mode=liquidity_trap_v1 days=%d start_ms=%d end_ms=%d universe=%s tp=%.3f sl=%.3f"
        % (
            args.days,
            start_ms,
            end_ms,
            args.universe,
            float(args.tp_pct),
            float(args.sl_pct),
        )
    )

    trades: List[Dict] = []
    stats_by_symbol: Dict[str, Dict] = {}
    equity = float(args.initial_usdt)

    for symbol in symbols:
        try:
            rows_1m = _fetch_ohlcv_all(exchange, symbol, "1m", start_ms, end_ms)
        except Exception:
            continue

        df = _to_df(rows_1m)
        if df.empty or len(df) < max(args.rsi_len, args.breakout_lookback, args.vol_lookback) + 5:
            continue

        df = df.drop_duplicates("ts").reset_index(drop=True)
        df["rsi"] = _rsi(df["close"], int(args.rsi_len))
        df["vol_sma"] = df["volume"].rolling(int(args.vol_lookback)).mean().shift(1)
        df["hi20"] = df["high"].rolling(int(args.breakout_lookback)).max().shift(1)
        df["lo20"] = df["low"].rolling(int(args.breakout_lookback)).min().shift(1)

        open_pos: Optional[Position] = None
        kst_tz = timezone(timedelta(hours=9))

        for i in range(1, len(df) - 1):
            row = df.iloc[i]
            ts = int(row["ts"])
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            if open_pos:
                exit_reason = None
                exit_px = None
                if open_pos.side == "LONG":
                    tp_hit = h >= open_pos.tp_px
                    sl_hit = l <= open_pos.sl_px
                    if tp_hit and sl_hit:
                        exit_reason = "SL"
                        exit_px = open_pos.sl_px
                    elif sl_hit:
                        exit_reason = "SL"
                        exit_px = open_pos.sl_px
                    elif tp_hit:
                        exit_reason = "TP"
                        exit_px = open_pos.tp_px
                else:
                    tp_hit = l <= open_pos.tp_px
                    sl_hit = h >= open_pos.sl_px
                    if tp_hit and sl_hit:
                        exit_reason = "SL"
                        exit_px = open_pos.sl_px
                    elif sl_hit:
                        exit_reason = "SL"
                        exit_px = open_pos.sl_px
                    elif tp_hit:
                        exit_reason = "TP"
                        exit_px = open_pos.tp_px

                hold_bars = i - open_pos.entry_idx

                if exit_reason:
                    pnl_per_unit = (
                        exit_px - open_pos.entry_px
                        if open_pos.side == "LONG"
                        else open_pos.entry_px - exit_px
                    )
                    pnl_usdt = pnl_per_unit * open_pos.size
                    pnl_pct = pnl_per_unit / open_pos.entry_px

                    trades.append(
                        {
                            "symbol": symbol,
                            "side": open_pos.side,
                            "entry_ts": open_pos.entry_ts,
                            "exit_ts": ts,
                            "entry_px": open_pos.entry_px,
                            "exit_px": exit_px,
                            "pnl_usdt": pnl_usdt,
                            "pnl_pct": pnl_pct,
                            "exit_reason": exit_reason,
                            "hold_bars": hold_bars,
                            "mfe": open_pos.max_favorable,
                            "mae": open_pos.max_adverse,
                        }
                    )
                    with open(trades_path, "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(
                            [
                                symbol,
                                open_pos.side,
                                _dt_kst(open_pos.entry_ts),
                                _dt_kst(ts),
                                open_pos.entry_px,
                                exit_px,
                                pnl_usdt,
                                pnl_pct,
                                exit_reason,
                                hold_bars,
                                open_pos.max_favorable,
                                open_pos.max_adverse,
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
                            "pnl_sum": 0.0,
                        },
                    )
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += float(open_pos.max_favorable)
                    sym_stats["mae_sum"] += float(open_pos.max_adverse)
                    sym_stats["hold_sum"] += float(hold_bars)
                    sym_stats["pnl_sum"] += float(pnl_usdt)
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
                            "LIQ_EXIT sym=%s side=%s reason=%s pnl=%.4f"
                            % (symbol, open_pos.side, exit_reason, pnl_usdt)
                        )

                    open_pos = None
                    continue

            dt_kst = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(kst_tz)
            hour = dt_kst.hour
            if hour < 10 or hour >= 14:
                continue

            sig_row = df.iloc[i - 1]
            hi20 = float(sig_row["hi20"]) if pd.notna(sig_row["hi20"]) else None
            lo20 = float(sig_row["lo20"]) if pd.notna(sig_row["lo20"]) else None
            vol_sma = float(sig_row["vol_sma"]) if pd.notna(sig_row["vol_sma"]) else None
            rsi_val = float(sig_row["rsi"]) if pd.notna(sig_row["rsi"]) else None
            sig_h = float(sig_row["high"])
            sig_l = float(sig_row["low"])
            sig_close = float(sig_row["close"])
            sig_vol = float(sig_row["volume"])

            entry_side: Optional[str] = None
            vol_ok = isinstance(vol_sma, (int, float)) and vol_sma > 0 and sig_vol >= vol_sma * float(args.vol_mult)
            wick_ratio = None
            rng = sig_h - sig_l
            if rng > 0:
                wick_ratio = (sig_h - sig_close) / rng
            if isinstance(hi20, (int, float)) and vol_ok and isinstance(rsi_val, (int, float)):
                if sig_h > hi20 and rsi_val >= float(args.rsi_long_min):
                    if isinstance(wick_ratio, (int, float)) and wick_ratio > float(args.wick_min_ratio):
                        entry_side = "LONG"
                elif sig_l < lo20 and rsi_val <= float(args.rsi_short_max):
                    if isinstance(wick_ratio, (int, float)) and wick_ratio > float(args.wick_min_ratio):
                        entry_side = "SHORT"

            if not entry_side:
                continue

            entry_row = df.iloc[i + 1]
            entry_ts = int(entry_row["ts"])
            if entry_side == "LONG":
                entry_px = sig_close * 1.01
            else:
                entry_px = sig_close * 0.99

            if entry_side == "LONG":
                tp_px = entry_px * (1.0 + float(args.tp_pct))
                sl_px = entry_px * (1.0 - float(args.sl_pct))
            else:
                tp_px = entry_px * (1.0 - float(args.tp_pct))
                sl_px = entry_px * (1.0 + float(args.sl_pct))

            base_equity = equity if args.entry_base == "equity" else float(args.fixed_equity)
            entry_usdt = base_equity * (float(args.entry_pct) / 100.0)
            size = entry_usdt / entry_px

            open_pos = Position(
                side=entry_side,
                entry_idx=i + 1,
                entry_ts=entry_ts,
                entry_px=entry_px,
                tp_px=tp_px,
                sl_px=sl_px,
                size=size,
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
                    "pnl_sum": 0.0,
                },
            )
            sym_stats["entries"] += 1

            if args.verbose:
                _bt_log("LIQ_ENTRY sym=%s side=%s entry=%.6g" % (symbol, entry_side, entry_px))

        open_pos = None

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
        avg_hold = stats["hold_sum"] / max(1, stats["trades"])
        avg_mfe = stats["mfe_sum"] / max(1, stats["trades"])
        avg_mae = stats["mae_sum"] / max(1, stats["trades"])
        _bt_log(
            "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
            "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f net_sum=%.3f"
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
                stats["pnl_sum"],
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
    total_pnl = sum(s["pnl_sum"] for s in stats_by_symbol.values())
    _bt_log(
        "[BACKTEST] TOTAL entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
        "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f net_sum=%.3f"
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
            total_pnl,
        )
    )

    if total_trades == 0:
        _bt_log("[BACKTEST] no trades/entries")


if __name__ == "__main__":
    main()
