#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import ccxt
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.swaggy.swaggy_engine import SwaggyConfig, SwaggyDecision, SwaggyEngine


def _parse_ts(text: str) -> int:
    if text.isdigit():
        return int(text)
    t = text.strip().replace("T", " ")
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> List[list]:
    out: List[list] = []
    since = start_ms
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if ts > end_ms:
                break
            out.append(row)
        last_ts = int(batch[-1][0])
        if last_ts <= since:
            since = since + tf_ms
        else:
            since = last_ts + tf_ms
        time.sleep(exchange.rateLimit / 1000.0)
    return out


def _to_df(rows: List[list]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def _slice_df(df: pd.DataFrame, ts_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["ts"] <= ts_ms]


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _log_factory(log_path: str, verbose: bool):
    _ensure_dir(os.path.dirname(log_path))
    fp = open(log_path, "a", encoding="utf-8")

    def _log(msg: str) -> None:
        fp.write(msg + "\n")
        fp.flush()
        if verbose:
            print(msg)

    return _log, fp


def _write_trade_header(path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "symbol",
                "side",
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
                "tp1_px",
                "tp2_px",
            ]
        )


def _append_trade(path: str, row: List[Any]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def _write_signal_header(path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts",
                "symbol",
                "side",
                "entry_px",
                "sl_px",
                "tp1_px",
                "tp2_px",
                "reason",
            ]
        )


def _append_signal(path: str, row: List[Any]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def _calc_pnl(
    side: str,
    entry_px: float,
    exit_px: float,
    fee_rate: float,
    slip_pct: float,
) -> float:
    side = (side or "").upper()
    if side == "SHORT":
        entry_fill = entry_px * (1 - slip_pct)
        exit_fill = exit_px * (1 + slip_pct)
        fees = entry_fill * fee_rate + exit_fill * fee_rate
        return (entry_fill - exit_fill - fees) / entry_fill if entry_fill > 0 else 0.0
    entry_fill = entry_px * (1 + slip_pct)
    exit_fill = exit_px * (1 - slip_pct)
    fees = entry_fill * fee_rate + exit_fill * fee_rate
    return (exit_fill - entry_fill - fees) / entry_fill if entry_fill > 0 else 0.0


@dataclass
class PendingEntry:
    symbol: str
    side: str
    signal_idx: int
    entry_idx: int
    entry_px: float
    sl_px: float
    tp1_px: Optional[float]
    tp2_px: Optional[float]
    sl_pct: float = 0.0
    tp_pct: float = 0.0


@dataclass
class TradeState:
    symbol: str
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    sl_px: float
    tp1_px: Optional[float]
    tp2_px: Optional[float]
    high_max: float
    low_min: float
    tp1_hit: bool = False
    exit_idx: Optional[int] = None
    exit_ts: Optional[int] = None
    exit_px: Optional[float] = None
    exit_reason: Optional[str] = None


def parse_args():
    parser = argparse.ArgumentParser(description="Swaggy backtest with trade simulation")
    parser.add_argument("--symbols", type=str, default="BTC/USDT:USDT")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start", type=str, default="")
    parser.add_argument("--end", type=str, default="")
    parser.add_argument("--entry-mode", type=str, default="NEXT_OPEN", choices=["NEXT_OPEN", "SIGNAL_CLOSE"])
    parser.add_argument("--sl-pct", type=float, default=0.0)
    parser.add_argument("--tp-pct", type=float, default=0.0)
    parser.add_argument("--out-signals", type=str, default="logs/swaggy/backtest_swaggy_signals.csv")
    parser.add_argument("--out-trades", type=str, default="logs/swaggy/backtest_swaggy_trades.csv")
    parser.add_argument("--log-path", type=str, default="logs/swaggy/backtest_swaggy.log")
    parser.add_argument("--time-stop-bars", type=int, default=-1)
    parser.add_argument("--trade-cooldown-bars", type=int, default=0)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--slippage-pct", type=float, default=0.0)
    parser.add_argument("--position-size-usdt", type=float, default=100.0)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _decision_entry_ready(decision: SwaggyDecision) -> bool:
    return isinstance(decision, SwaggyDecision) and decision.entry_ready == 1


def _open_trade(
    symbol: str,
    side: str,
    idx: int,
    ts: int,
    entry_px: float,
    sl_px: float,
    tp1_px: Optional[float],
    tp2_px: Optional[float],
    h: float,
    l: float,
) -> TradeState:
    return TradeState(
        symbol=symbol,
        side=side,
        entry_idx=idx,
        entry_ts=ts,
        entry_px=entry_px,
        sl_px=sl_px,
        tp1_px=tp1_px,
        tp2_px=tp2_px,
        high_max=h,
        low_min=l,
    )


def _apply_exit_logic(
    trade: TradeState,
    idx: int,
    ts: int,
    h: float,
    l: float,
    c: float,
    time_stop_bars: int,
) -> Optional[TradeState]:
    trade.high_max = max(trade.high_max, h)
    trade.low_min = min(trade.low_min, l)
    side = (trade.side or "").upper()
    sl_hit = False
    tp2_hit = False
    tp1_hit = False
    if side == "SHORT":
        sl_hit = h >= trade.sl_px
        tp2_hit = trade.tp2_px is not None and l <= trade.tp2_px
        tp1_hit = trade.tp1_px is not None and l <= trade.tp1_px
    else:
        sl_hit = l <= trade.sl_px
        tp2_hit = trade.tp2_px is not None and h >= trade.tp2_px
        tp1_hit = trade.tp1_px is not None and h >= trade.tp1_px
    exit_reason = None
    exit_px = None
    if sl_hit:
        exit_reason = "SL"
        exit_px = trade.sl_px
    elif tp2_hit:
        exit_reason = "TP2"
        exit_px = trade.tp2_px
    elif tp1_hit and not trade.tp1_hit:
        trade.tp1_hit = True
        trade.sl_px = trade.entry_px
    elif time_stop_bars > 0 and (idx - trade.entry_idx) >= time_stop_bars:
        exit_reason = "TIME_STOP"
        exit_px = c
    if exit_reason:
        trade.exit_idx = idx
        trade.exit_ts = ts
        trade.exit_px = float(exit_px)
        trade.exit_reason = exit_reason
        return trade
    return None


def main() -> None:
    args = parse_args()
    if args.start and args.end:
        start_ms = _parse_ts(args.start)
        end_ms = _parse_ts(args.end)
    else:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = int((datetime.now(timezone.utc) - pd.Timedelta(days=args.days)).timestamp() * 1000)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    log_fn, log_fp = _log_factory(args.log_path, args.verbose)
    if args.out_signals:
        _write_signal_header(args.out_signals)
    if args.out_trades:
        _write_trade_header(args.out_trades)

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    exchange.load_markets()

    cfg = SwaggyConfig()
    time_stop_bars = args.time_stop_bars if args.time_stop_bars > 0 else cfg.time_stop_bars_5m
    ltf_minutes = float(exchange.parse_timeframe(cfg.tf_ltf)) / 60.0
    total_stats = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }

    for symbol in symbols:
        log_fn(f"[swaggy] fetch sym={symbol} start={start_ms} end={end_ms}")
        rows_5m = _fetch_ohlcv_all(exchange, symbol, cfg.tf_ltf, start_ms, end_ms)
        rows_15m = _fetch_ohlcv_all(exchange, symbol, cfg.tf_mtf, start_ms, end_ms)
        rows_1h = _fetch_ohlcv_all(exchange, symbol, cfg.tf_htf, start_ms, end_ms)
        rows_4h = _fetch_ohlcv_all(exchange, symbol, cfg.tf_htf2, start_ms, end_ms)
        df_5m = _to_df(rows_5m)
        df_15m = _to_df(rows_15m)
        df_1h = _to_df(rows_1h)
        df_4h = _to_df(rows_4h)
        if df_5m.empty or len(df_5m) < 50:
            log_fn(f"[swaggy] empty_or_short sym={symbol} rows={len(df_5m)}")
            continue

        engine = SwaggyEngine(cfg)
        engine.begin_cycle()
        state: Dict[str, Any] = engine._state.setdefault(symbol, {})
        state["_swaggy_quiet"] = True
        pending: Optional[PendingEntry] = None
        trade: Optional[TradeState] = None
        cooldown_until = -10**9
        trade_stats = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "timeouts": 0,
            "tp": 0,
            "sl": 0,
            "mfe_sum": 0.0,
            "mae_sum": 0.0,
            "hold_sum": 0.0,
            "skipped_in_pos": 0,
            "skipped_cooldown": 0,
            "skipped_no_next": 0,
        }

        for idx in range(30, len(df_5m)):
            ts = int(df_5m["ts"].iloc[idx])
            o = float(df_5m["open"].iloc[idx])
            h = float(df_5m["high"].iloc[idx])
            l = float(df_5m["low"].iloc[idx])
            c = float(df_5m["close"].iloc[idx])

            if trade is not None:
                exited = _apply_exit_logic(trade, idx, ts, h, l, c, time_stop_bars)
                if exited is not None:
                    pnl_pct = _calc_pnl(trade.side, trade.entry_px, trade.exit_px, args.fee_rate, args.slippage_pct)
                    pnl_usdt = pnl_pct * args.position_size_usdt
                    holding_bars = idx - trade.entry_idx + 1
                    mfe = (
                        (trade.entry_px - trade.low_min) / trade.entry_px
                        if trade.side.upper() == "SHORT"
                        else (trade.high_max - trade.entry_px) / trade.entry_px
                    )
                    mae = (
                        (trade.high_max - trade.entry_px) / trade.entry_px
                        if trade.side.upper() == "SHORT"
                        else (trade.entry_px - trade.low_min) / trade.entry_px
                    )
                    log_fn(
                        "SWAGGY_TRADE_EXIT "
                        f"sym={symbol} side={trade.side} reason={trade.exit_reason} "
                        f"exit_px={trade.exit_px} exit_ts={trade.exit_ts} "
                        f"pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} mae={mae:.4f} hold={holding_bars}"
                    )
                    trade_stats["trades"] += 1
                    if trade.exit_reason == "TIME_STOP":
                        trade_stats["timeouts"] += 1
                    if trade.exit_reason == "TP2":
                        trade_stats["tp"] += 1
                    elif trade.exit_reason == "SL":
                        trade_stats["sl"] += 1
                    if pnl_pct >= 0:
                        trade_stats["wins"] += 1
                    else:
                        trade_stats["losses"] += 1
                    trade_stats["mfe_sum"] += mfe
                    trade_stats["mae_sum"] += mae
                    trade_stats["hold_sum"] += holding_bars * ltf_minutes
                    if args.out_trades:
                        _append_trade(
                            args.out_trades,
                            [
                                symbol,
                                trade.side,
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
                                f"{trade.tp1_px:.6g}" if trade.tp1_px is not None else "",
                                f"{trade.tp2_px:.6g}" if trade.tp2_px is not None else "",
                            ],
                        )
                    trade = None
                    state["in_pos"] = False
                    if exited.exit_reason == "SL":
                        state["swaggy_last_sl_ts"] = float(exited.exit_ts or ts) / 1000.0
                    cooldown_until = idx + max(0, int(args.trade_cooldown_bars))

            if pending is not None and idx == pending.entry_idx:
                if trade is None:
                    entry_px = o if args.entry_mode == "NEXT_OPEN" else pending.entry_px
                    side = pending.side.upper()
                    sl_px = pending.sl_px
                    tp1_px = pending.tp1_px
                    tp2_px = pending.tp2_px
                    trade = _open_trade(
                        symbol,
                        pending.side,
                        idx,
                        ts,
                        entry_px,
                        sl_px,
                        tp1_px,
                        tp2_px,
                        h,
                        l,
                    )
                    state["in_pos"] = True
                    state["side"] = trade.side
                    state["entry_px"] = trade.entry_px
                    state["sl_px"] = trade.sl_px
                    state["tp1_px"] = trade.tp1_px
                    state["tp2_px"] = trade.tp2_px
                    state["entry_bar"] = trade.entry_idx
                    state["tp1_hit"] = False
                    log_fn(
                        "SWAGGY_TRADE_ENTRY "
                        f"sym={symbol} side={trade.side} entry_px={trade.entry_px} entry_ts={trade.entry_ts} "
                        f"sl={trade.sl_px} tp1={trade.tp1_px} tp2={trade.tp2_px}"
                    )
                pending = None

            if trade is not None:
                continue
            if pending is not None and idx < pending.entry_idx:
                continue
            if idx <= cooldown_until:
                trade_stats["skipped_cooldown"] += 1
                continue

            now_ts = ts / 1000.0
            d5 = df_5m.iloc[: idx + 1]
            d15 = _slice_df(df_15m, ts)
            d1h = _slice_df(df_1h, ts)
            d4h = _slice_df(df_4h, ts)
            decision = engine.evaluate_symbol(
                sym=symbol,
                candles_4h=d4h,
                candles_1h=d1h,
                candles_15m=d15,
                candles_5m=d5,
                position_state=state,
                engine_config=cfg,
                now_ts=now_ts,
            )
            reason = (decision.reason_codes or [""])[0]
            if reason == "COOLDOWN":
                bars_left = ""
                if isinstance(decision.evidence, dict):
                    bars_left = decision.evidence.get("bars_left", "")
                if args.verbose:
                    log_fn(f"[swaggy] ENTRY_SKIP reason=COOLDOWN sym={symbol} bars_left={bars_left}")
            elif reason == "ZONE_REENTRY":
                if args.verbose:
                    log_fn(f"[swaggy] ENTRY_SKIP reason=ZONE_REENTRY sym={symbol}")
            elif reason == "SL_COOLDOWN":
                bars_left = ""
                if isinstance(decision.evidence, dict):
                    bars_left = decision.evidence.get("bars_left", "")
                if args.verbose:
                    log_fn(f"[swaggy] SL_COOLDOWN_ACTIVE sym={symbol} bars_left={bars_left}")
            if _decision_entry_ready(decision):
                if trade is not None:
                    trade_stats["skipped_in_pos"] += 1
                    continue
                if pending is not None:
                    trade_stats["skipped_in_pos"] += 1
                    continue
                if args.entry_mode == "NEXT_OPEN" and idx + 1 >= len(df_5m):
                    trade_stats["skipped_no_next"] += 1
                    continue
                entry_px = float(decision.entry_px or c)
                sl_pct = float(args.sl_pct or 0.0)
                tp_pct = float(args.tp_pct or 0.0)
                if sl_pct <= 0 or tp_pct <= 0:
                    continue
                side = str(decision.side or "").upper()
                sl_px = entry_px * (1 + sl_pct) if side == "SHORT" else entry_px * (1 - sl_pct)
                tp1_px = entry_px * (1 - tp_pct) if side == "SHORT" else entry_px * (1 + tp_pct)
                pending = PendingEntry(
                    symbol=symbol,
                    side=side,
                    signal_idx=idx,
                    entry_idx=(idx + 1) if args.entry_mode == "NEXT_OPEN" else idx,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp1_px=tp1_px,
                    tp2_px=None,
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                )
                if args.out_signals:
                    _append_signal(
                        args.out_signals,
                        [
                            ts,
                            symbol,
                            pending.side,
                            f"{pending.entry_px:.6g}",
                            f"{pending.sl_px:.6g}",
                            f"{pending.tp1_px:.6g}" if pending.tp1_px is not None else "",
                            "",
                            "ENTRY_READY",
                        ],
                    )

        if trade is not None:
            last_idx = len(df_5m) - 1
            last_ts = int(df_5m["ts"].iloc[last_idx])
            last_close = float(df_5m["close"].iloc[last_idx])
            trade.exit_idx = last_idx
            trade.exit_ts = last_ts
            trade.exit_px = last_close
            trade.exit_reason = "EOT"
            pnl_pct = _calc_pnl(trade.side, trade.entry_px, last_close, args.fee_rate, args.slippage_pct)
            pnl_usdt = pnl_pct * args.position_size_usdt
            holding_bars = last_idx - trade.entry_idx + 1
            mfe = (
                (trade.entry_px - trade.low_min) / trade.entry_px
                if trade.side.upper() == "SHORT"
                else (trade.high_max - trade.entry_px) / trade.entry_px
            )
            mae = (
                (trade.high_max - trade.entry_px) / trade.entry_px
                if trade.side.upper() == "SHORT"
                else (trade.entry_px - trade.low_min) / trade.entry_px
            )
            log_fn(
                "SWAGGY_TRADE_EXIT "
                f"sym={symbol} side={trade.side} reason=EOT exit_px={last_close} exit_ts={last_ts} "
                f"pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} mae={mae:.4f} hold={holding_bars}"
            )
            trade_stats["trades"] += 1
            if pnl_pct >= 0:
                trade_stats["wins"] += 1
            else:
                trade_stats["losses"] += 1
            trade_stats["mfe_sum"] += mfe
            trade_stats["mae_sum"] += mae
            trade_stats["hold_sum"] += holding_bars * ltf_minutes
            if args.out_trades:
                _append_trade(
                    args.out_trades,
                    [
                        symbol,
                        trade.side,
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
                        f"{trade.tp1_px:.6g}" if trade.tp1_px is not None else "",
                        f"{trade.tp2_px:.6g}" if trade.tp2_px is not None else "",
                    ],
                )
            trade = None
            state["in_pos"] = False

        trades = int(trade_stats["trades"] or 0)
        wins = int(trade_stats["wins"] or 0)
        losses = int(trade_stats["losses"] or 0)
        tp = int(trade_stats["tp"] or 0)
        sl = int(trade_stats["sl"] or 0)
        win_rate = (wins / trades * 100.0) if trades else 0.0
        avg_mfe = (trade_stats["mfe_sum"] / trades) if trades else 0.0
        avg_mae = (trade_stats["mae_sum"] / trades) if trades else 0.0
        avg_hold = (trade_stats["hold_sum"] / trades) if trades else 0.0
        print(
            "[BACKTEST] %s trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
            "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f"
            % (
                symbol,
                trades,
                wins,
                losses,
                win_rate,
                tp,
                sl,
                avg_mfe,
                avg_mae,
                avg_hold,
            )
        )
        total_stats["trades"] += trades
        total_stats["wins"] += wins
        total_stats["losses"] += losses
        total_stats["tp"] += tp
        total_stats["sl"] += sl
        total_stats["mfe_sum"] += trade_stats["mfe_sum"]
        total_stats["mae_sum"] += trade_stats["mae_sum"]
        total_stats["hold_sum"] += trade_stats["hold_sum"]

    log_fp.close()
    total_trades = int(total_stats["trades"] or 0)
    total_wins = int(total_stats["wins"] or 0)
    total_losses = int(total_stats["losses"] or 0)
    total_tp = int(total_stats["tp"] or 0)
    total_sl = int(total_stats["sl"] or 0)
    total_win_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
    total_avg_mfe = (total_stats["mfe_sum"] / total_trades) if total_trades else 0.0
    total_avg_mae = (total_stats["mae_sum"] / total_trades) if total_trades else 0.0
    total_avg_hold = (total_stats["hold_sum"] / total_trades) if total_trades else 0.0
    print(
        "[BACKTEST] TOTAL trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
        "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f"
        % (
            total_trades,
            total_wins,
            total_losses,
            total_win_rate,
            total_tp,
            total_sl,
            total_avg_mfe,
            total_avg_mae,
            total_avg_hold,
        )
    )


if __name__ == "__main__":
    main()
