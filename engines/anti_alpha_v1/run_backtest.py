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


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


def _vol_sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


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
    universe_cache: str,
) -> List[str]:
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            return [s.strip() for s in f.read().split(",") if s.strip()]
    if symbols_arg:
        return [s.strip() for s in symbols_arg.split(",") if s.strip()]
    if universe_cache and os.path.exists(universe_cache):
        with open(universe_cache, "r", encoding="utf-8") as f:
            cached = json.load(f)
        if isinstance(cached, list):
            return [s for s in cached if isinstance(s, str) and s]
    top_n = _parse_universe_arg(universe_arg) or 50
    tickers = exchange.fetch_tickers()
    anchors = ("BTC/USDT:USDT", "ETH/USDT:USDT")
    shared_universe = build_universe_from_tickers(
        tickers,
        min_quote_volume_usdt=min_qv,
        top_n=top_n,
        anchors=anchors,
    )
    if (universe_mode or "").lower() != "adv_trend":
        if universe_cache:
            os.makedirs(os.path.dirname(universe_cache), exist_ok=True)
            with open(universe_cache, "w", encoding="utf-8") as f:
                json.dump(shared_universe, f)
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
    symbols = list(dict.fromkeys(list(shared_universe) + low_vol))
    if universe_cache:
        os.makedirs(os.path.dirname(universe_cache), exist_ok=True)
        with open(universe_cache, "w", encoding="utf-8") as f:
            json.dump(symbols, f)
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
    max_favorable: float = 0.0
    max_adverse: float = 0.0


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
    parser.add_argument("--universe-cache", default="", help="cache universe symbols to reduce API calls")
    parser.add_argument("--initial-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-pct", type=float, default=1.0)
    parser.add_argument("--entry-base", default="equity", choices=["equity", "fixed"])
    parser.add_argument("--fixed-equity", type=float, default=1000.0)
    parser.add_argument("--slip-pct", type=float, default=0.0)
    parser.add_argument("--ema-len", type=int, default=200)
    parser.add_argument("--rsi-len", type=int, default=14)
    parser.add_argument("--vol-sma-len", type=int, default=20)
    parser.add_argument("--streak-n", type=int, default=2)
    parser.add_argument("--vol-spike-mult", type=float, default=1.5)
    parser.add_argument("--body-pct-min", type=float, default=0.008)
    parser.add_argument("--short-rsi-min", type=float, default=80.0)
    parser.add_argument("--long-rsi-max", type=float, default=30.0)
    parser.add_argument("--ema-dist-min", type=float, default=0.003)
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--sl-pct", type=float, default=0.02)
    parser.add_argument("--cooldown-bars", type=int, default=0)
    parser.add_argument("--invert-side", action="store_true")
    parser.add_argument("--use-confirmed", action="store_true", help="use previous bar for signal (confirmed)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    # keep confirmed-candle behavior aligned with live
    args.use_confirmed = True

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(ROOT_DIR, "logs", "anti_alpha_v1", "backtest")
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
        args.universe_mode,
        args.adv_min_qv,
        args.adv_top_n,
        args.universe_cache,
    )

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

    trades: List[Dict] = []
    stats_by_symbol: Dict[str, Dict] = {}
    equity = float(args.initial_usdt)

    for symbol in symbols:
        try:
            rows_1m = _fetch_ohlcv_all(exchange, symbol, "1m", start_ms, end_ms)
        except Exception:
            continue

        df = _to_df(rows_1m)
        if df.empty or len(df) < max(args.ema_len, args.rsi_len, args.vol_sma_len) + args.streak_n + 5:
            continue

        df["ema20"] = _ema(df["close"], int(args.ema_len))
        df["rsi14"] = _rsi(df["close"], int(args.rsi_len))
        df["vol_sma20"] = _vol_sma(df["volume"], int(args.vol_sma_len))

        open_long: Optional[Position] = None
        open_short: Optional[Position] = None
        cooldown_left = 0

        min_idx = max(int(args.streak_n), 1)
        if args.use_confirmed:
            min_idx = max(min_idx, 2)
        for i in range(min_idx, len(df) - 1):
            row = df.iloc[i]
            ts = int(row["ts"])

            if cooldown_left > 0:
                cooldown_left -= 1
                continue

            if open_long or open_short:
                high_px = float(row["high"])
                low_px = float(row["low"])
                close_px = float(row["close"])
                for pos_side, pos in (("LONG", open_long), ("SHORT", open_short)):
                    if not pos:
                        continue
                    exit_reason = None
                    exit_px = None
                    if pos.side == "LONG":
                        pos.max_favorable = max(pos.max_favorable, (high_px - pos.entry_px) / pos.entry_px)
                        pos.max_adverse = max(pos.max_adverse, (pos.entry_px - low_px) / pos.entry_px)
                        sl_hit = low_px <= pos.stop_px
                        tp_hit = high_px >= pos.tp_px
                        if sl_hit:
                            exit_reason = "SL"
                            exit_px = pos.stop_px
                        elif tp_hit:
                            exit_reason = "TP"
                            exit_px = pos.tp_px
                    else:
                        pos.max_favorable = max(pos.max_favorable, (pos.entry_px - low_px) / pos.entry_px)
                        pos.max_adverse = max(pos.max_adverse, (high_px - pos.entry_px) / pos.entry_px)
                        sl_hit = high_px >= pos.stop_px
                        tp_hit = low_px <= pos.tp_px
                        if sl_hit:
                            exit_reason = "SL"
                            exit_px = pos.stop_px
                        elif tp_hit:
                            exit_reason = "TP"
                            exit_px = pos.tp_px

                    hold_bars = i - pos.entry_idx

                    if exit_reason:
                        pnl_per_unit = (exit_px - pos.entry_px) if pos.side == "LONG" else (pos.entry_px - exit_px)
                        pnl_usdt = pnl_per_unit * pos.size
                        pnl_pct = pnl_per_unit / pos.entry_px

                        trades.append(
                            {
                                "symbol": symbol,
                                "side": pos.side,
                                "entry_ts": pos.entry_ts,
                                "exit_ts": ts,
                                "entry_px": pos.entry_px,
                                "exit_px": exit_px,
                                "pnl_usdt": pnl_usdt,
                                "pnl_pct": pnl_pct,
                                "exit_reason": exit_reason,
                                "hold_bars": hold_bars,
                                "mfe": pos.max_favorable,
                                "mae": pos.max_adverse,
                            }
                        )
                        with open(trades_path, "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(
                                [
                                    symbol,
                                    pos.side,
                                    _dt_kst(pos.entry_ts),
                                    _dt_kst(ts),
                                    pos.entry_px,
                                    exit_px,
                                    pnl_usdt,
                                    pnl_pct,
                                    exit_reason,
                                    hold_bars,
                                    pos.max_favorable,
                                    pos.max_adverse,
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
                        sym_stats["mfe_sum"] += float(pos.max_favorable)
                        sym_stats["mae_sum"] += float(pos.max_adverse)
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
                                "ANTI_EXIT sym=%s side=%s reason=%s pnl=%.4f"
                                % (symbol, pos.side, exit_reason, pnl_usdt)
                            )
                        if pos_side == "LONG":
                            open_long = None
                        else:
                            open_short = None
                        cooldown_left = int(args.cooldown_bars)
                if open_long or open_short:
                    continue

            sig_idx = i - 1 if args.use_confirmed else i
            sig_row = df.iloc[sig_idx]
            ema20 = float(sig_row["ema20"]) if pd.notna(sig_row["ema20"]) else None
            rsi14 = float(sig_row["rsi14"]) if pd.notna(sig_row["rsi14"]) else None
            vol_sma = float(sig_row["vol_sma20"]) if pd.notna(sig_row["vol_sma20"]) else None
            close_px = float(sig_row["close"])
            volume = float(sig_row["volume"])

            if not isinstance(ema20, (int, float)) or not isinstance(rsi14, (int, float)):
                continue
            if not isinstance(vol_sma, (int, float)) or vol_sma <= 0:
                continue

            if close_px == ema20:
                continue

            up_streak = True
            down_streak = True
            for j in range(int(args.streak_n)):
                c = df.iloc[sig_idx - j]
                if float(c["close"]) <= float(c["open"]):
                    up_streak = False
                if float(c["close"]) >= float(c["open"]):
                    down_streak = False

            vol_spike = volume >= vol_sma * float(args.vol_spike_mult)

            entry_side: Optional[str] = None
            prev_rsi_idx = sig_idx - 1
            prev_rsi = float(df["rsi14"].iloc[prev_rsi_idx]) if prev_rsi_idx >= 0 and pd.notna(df["rsi14"].iloc[prev_rsi_idx]) else None
            if not isinstance(prev_rsi, (int, float)):
                continue

            body_pct = abs(close_px - float(row["open"])) / close_px if close_px else 0.0
            ema_dist = abs(close_px - ema20) / ema20 if ema20 else 0.0
            if ema_dist < float(args.ema_dist_min):
                continue

            if close_px > ema20:
                if (
                    rsi14 >= float(args.short_rsi_min)
                    and up_streak
                    and vol_spike
                    and rsi14 >= prev_rsi
                    and close_px > float(df["high"].iloc[prev_rsi_idx])
                    and body_pct >= float(args.body_pct_min)
                ):
                    entry_side = "SHORT"
            elif close_px < ema20:
                if (
                    rsi14 <= float(args.long_rsi_max)
                    and down_streak
                    and vol_spike
                    and rsi14 <= prev_rsi
                    and close_px < float(df["low"].iloc[prev_rsi_idx])
                    and body_pct >= float(args.body_pct_min)
                ):
                    entry_side = "LONG"

            if not entry_side:
                continue

            if args.verbose:
                _bt_log("ANTI_SIG sym=%s side=%s rsi=%.2f vol_spike=%d" % (symbol, entry_side, rsi14, int(vol_spike)))

            base_side = entry_side
            entry_row = df.iloc[i + 1]
            if base_side == "LONG":
                entry_px = float(entry_row["high"])
            else:
                entry_px = float(entry_row["low"])
            if args.invert_side:
                entry_side = "SHORT" if base_side == "LONG" else "LONG"
            else:
                entry_side = base_side

            if entry_side == "LONG" and open_long:
                continue
            if entry_side == "SHORT" and open_short:
                continue
            if args.slip_pct:
                if entry_side == "LONG":
                    entry_px *= 1.0 + float(args.slip_pct)
                else:
                    entry_px *= 1.0 - float(args.slip_pct)

            if entry_side == "LONG":
                tp_px = entry_px * (1.0 + float(args.tp_pct))
                sl_px = entry_px * (1.0 - float(args.sl_pct))
            else:
                tp_px = entry_px * (1.0 - float(args.tp_pct))
                sl_px = entry_px * (1.0 + float(args.sl_pct))

            base_equity = equity if args.entry_base == "equity" else float(args.fixed_equity)
            entry_usdt = base_equity * (float(args.entry_pct) / 100.0)
            size = entry_usdt / entry_px

            open_position = Position(
                side=entry_side,
                entry_idx=i,
                entry_ts=int(row["ts"]),
                entry_px=entry_px,
                stop_px=sl_px,
                tp_px=tp_px,
                size=size,
            )
            if entry_side == "LONG":
                open_long = open_position
            else:
                open_short = open_position

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
                _bt_log("ANTI_ENTRY sym=%s side=%s entry=%.6g sl=%.6g tp=%.6g" % (symbol, entry_side, entry_px, sl_px, tp_px))

        if open_long:
            open_long = None
        if open_short:
            open_short = None

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
                stats["mfe_sum"] - stats["mae_sum"],
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
            net_sum,
        )
    )

    if total_trades == 0:
        _bt_log("[BACKTEST] no trades/entries")


if __name__ == "__main__":
    main()
