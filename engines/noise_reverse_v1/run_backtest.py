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

# Backtest-baseline defaults (overridable via CLI)
NOISE_REVERSE_LOOKBACK = 100
NOISE_REVERSE_MA_LEN = 25
NOISE_REVERSE_VOL_SMA_LEN = 20
NOISE_REVERSE_VOL_SPIKE_MULT = 5.0
NOISE_REVERSE_DISPARITY_PCT = 0.03

def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _utc_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _parse_utc_dt(text: str) -> Optional[datetime]:
    raw = (text or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


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
            symbols = [s.strip() for s in f.read().split(",") if s.strip()]
        return symbols
    if symbols_arg:
        symbols = [s.strip() for s in symbols_arg.split(",") if s.strip()]
        return symbols

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
    adv_candidates.sort(key=lambda x: x[1])  # low volatility first
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start", default="", help="UTC start, format: YYYY-MM-DD or YYYY-MM-DD HH:MM")
    parser.add_argument("--end", default="", help="UTC end, format: YYYY-MM-DD or YYYY-MM-DD HH:MM")
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
    parser.add_argument("--lookback", type=int, default=NOISE_REVERSE_LOOKBACK)
    parser.add_argument("--ma-len", type=int, default=NOISE_REVERSE_MA_LEN)
    parser.add_argument("--vol-sma-len", type=int, default=NOISE_REVERSE_VOL_SMA_LEN)
    parser.add_argument("--vol-spike-mult", type=float, default=NOISE_REVERSE_VOL_SPIKE_MULT)
    parser.add_argument("--disparity-pct", type=float, default=NOISE_REVERSE_DISPARITY_PCT)
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--sl-pct", type=float, default=0.02)
    parser.add_argument("--cooldown-bars", type=int, default=15)
    parser.add_argument("--use-confirmed", action="store_true", help="use previous bar for signal (confirmed)")
    parser.add_argument("--invert-side", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    # Backtest params can override defaults via CLI
    lookback = int(args.lookback)
    ma_len = int(args.ma_len)
    vol_len = int(args.vol_sma_len)
    vol_spike_mult = float(args.vol_spike_mult)
    disparity_pct = float(args.disparity_pct)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(ROOT_DIR, "logs", "noise_reverse_v1", "backtest")
    _ensure_dir(log_dir)
    trades_path = os.path.join(log_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(log_dir, f"summary_{run_id}.json")
    log_path = os.path.join(log_dir, f"run_{run_id}.log")

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

    if args.start or args.end:
        start_dt = _parse_utc_dt(args.start)
        end_dt = _parse_utc_dt(args.end)
        if end_dt is None:
            end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        if start_dt is None:
            start_dt = end_dt - timedelta(days=args.days)
    else:
        end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=args.days)
    start_ms = _utc_ms(start_dt)
    end_ms = _utc_ms(end_dt)

    _bt_log(
        "[run] mode=noise_reverse_v1 days=%d start_ms=%d end_ms=%d universe=%s cooldown=%d tp=%.3f sl=%.3f"
        % (
            args.days,
            start_ms,
            end_ms,
            args.universe,
            int(args.cooldown_bars),
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
        if df.empty or len(df) < 120:
            continue
        df["ma20"] = df["close"].rolling(ma_len).mean()
        df["vol_sma20"] = df["volume"].rolling(vol_len).mean()
        df["hi100"] = df["high"].rolling(lookback).max().shift(1)
        df["lo100"] = df["low"].rolling(lookback).min().shift(1)

        open_pos: Optional[Position] = None
        cooldown_left = 0
        loss_streak = 0
        max_loss_streak = 0

        min_len = max(lookback + 2, ma_len + 2, vol_len + 2, 120)
        end_idx = len(df) - 1 if args.use_confirmed else len(df)
        for i in range(min_len, end_idx):
            row = df.iloc[i]
            ts = int(row["ts"])
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            if open_pos:
                exit_reason = None
                exit_px = None
                # Conservative: if TP and SL both touched, SL wins
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
                            "mfe": abs((h - open_pos.entry_px) / open_pos.entry_px)
                            if open_pos.side == "LONG"
                            else abs((open_pos.entry_px - l) / open_pos.entry_px),
                            "mae": abs((open_pos.entry_px - l) / open_pos.entry_px)
                            if open_pos.side == "LONG"
                            else abs((h - open_pos.entry_px) / open_pos.entry_px),
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
                                trades[-1]["mfe"],
                                trades[-1]["mae"],
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
                            "max_loss_streak": 0,
                            "pnl_sum": 0.0,
                        },
                    )
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += float(trades[-1]["mfe"])
                    sym_stats["mae_sum"] += float(trades[-1]["mae"])
                    sym_stats["hold_sum"] += float(hold_bars)
                    sym_stats["pnl_sum"] += float(pnl_usdt)
                    if pnl_usdt > 0:
                        sym_stats["wins"] += 1
                        loss_streak = 0
                    else:
                        sym_stats["losses"] += 1
                        loss_streak += 1
                        max_loss_streak = max(max_loss_streak, loss_streak)
                    sym_stats["max_loss_streak"] = max(sym_stats["max_loss_streak"], max_loss_streak)
                    if exit_reason == "TP":
                        sym_stats["tp"] += 1
                    elif exit_reason == "SL":
                        sym_stats["sl"] += 1

                    equity += pnl_usdt
                    if args.entry_base == "equity":
                        equity = max(0.0, equity)

                    if args.verbose:
                        _bt_log(
                            "NOISE_EXIT sym=%s side=%s reason=%s pnl=%.4f"
                            % (symbol, open_pos.side, exit_reason, pnl_usdt)
                        )

                    open_pos = None
                    cooldown_left = int(args.cooldown_bars)
                    continue

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            # use confirmed candle (previous bar) for signal when enabled
            sig_row = df.iloc[i - 1] if args.use_confirmed else row
            ma20 = float(sig_row["ma20"]) if pd.notna(sig_row["ma20"]) else None
            vol_sma20 = float(sig_row["vol_sma20"]) if pd.notna(sig_row["vol_sma20"]) else None
            hi100 = float(sig_row["hi100"]) if pd.notna(sig_row["hi100"]) else None
            lo100 = float(sig_row["lo100"]) if pd.notna(sig_row["lo100"]) else None
            vol_now = float(sig_row["volume"])
            h_sig = float(sig_row["high"])
            l_sig = float(sig_row["low"])
            c_sig = float(sig_row["close"])

            entry_side: Optional[str] = None
            vol_spike = (
                isinstance(vol_sma20, (int, float))
                and vol_sma20 > 0
                and vol_now >= (vol_sma20 * vol_spike_mult)
            )
            if vol_spike and isinstance(hi100, (int, float)) and isinstance(lo100, (int, float)) and isinstance(ma20, (int, float)):
                if h_sig > hi100 and c_sig > ma20 * (1.0 + float(disparity_pct)):
                    entry_side = "LONG"
                elif l_sig < lo100 and c_sig < ma20 * (1.0 - float(disparity_pct)):
                    entry_side = "SHORT"

            if not entry_side:
                continue
            tp_pct = float(args.tp_pct)
            sl_pct = float(args.sl_pct)
            if args.invert_side:
                entry_side = "SHORT" if entry_side == "LONG" else "LONG"

            base_equity = equity if args.entry_base == "equity" else float(args.fixed_equity)
            entry_usdt = base_equity * (float(args.entry_pct) / 100.0)
            if entry_usdt <= 0:
                continue

            # enter at next bar open to align with live confirmed-candle signal timing
            entry_px = o
            if entry_side == "LONG":
                tp_px = entry_px * (1.0 + tp_pct)
                sl_px = entry_px * (1.0 - sl_pct)
            else:
                tp_px = entry_px * (1.0 - tp_pct)
                sl_px = entry_px * (1.0 + sl_pct)

            size = entry_usdt / entry_px
            open_pos = Position(
                side=entry_side,
                entry_idx=i,
                entry_ts=ts,
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
                    "max_loss_streak": 0,
                    "pnl_sum": 0.0,
                },
            )
            sym_stats["entries"] += 1

            if args.verbose:
                _bt_log("NOISE_ENTRY sym=%s side=%s entry=%.6g" % (symbol, entry_side, entry_px))

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
        # exit details under symbol summary
        sym_trades = [t for t in trades if t.get("symbol") == sym]
        for tr in sym_trades:
            entry_ts = tr.get("entry_ts")
            entry_kst = _dt_kst(int(entry_ts)) if isinstance(entry_ts, (int, float)) else "N/A"
            side = tr.get("side") or "N/A"
            pnl = tr.get("pnl_usdt")
            win_flag = "W" if isinstance(pnl, (int, float)) and pnl > 0 else "L"
            _bt_log(
                "  [EXIT] entry=%s side=%s win=%s avg_mae=%.4f avg_hold=%.1f net_sum=%.3f"
                % (entry_kst, side, win_flag, avg_mae, avg_hold, stats["pnl_sum"])
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
