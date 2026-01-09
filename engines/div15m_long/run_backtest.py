#!/usr/bin/env python3
import argparse
import csv
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt
import pandas as pd

from engines.div15m_long.config import Div15mConfig
from engines.div15m_long.engine import Div15mLongEngine, Div15mEvent


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
    limit: int = 1500,
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
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _log_factory(log_path: str):
    _ensure_dir(os.path.dirname(log_path))
    fp = open(log_path, "a", encoding="utf-8")

    def _log(msg: str) -> None:
        fp.write(msg + "\n")
        fp.flush()
        print(msg)

    return _log, fp


def _write_csv_header(path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    exists = os.path.exists(path)
    if exists:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts",
                "symbol",
                "event",
                "entry_px",
                "p1_idx",
                "p2_idx",
                "low1",
                "low2",
                "rsi1",
                "rsi2",
                "score",
                "reasons",
            ]
        )


def _append_csv(path: str, event: Div15mEvent) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                event.ts,
                event.symbol,
                event.event,
                f"{event.entry_px:.6g}",
                event.p1_idx,
                event.p2_idx,
                f"{event.low1:.6g}",
                f"{event.low2:.6g}",
                f"{event.rsi1:.6g}",
                f"{event.rsi2:.6g}",
                f"{event.score:.4f}",
                event.reasons,
            ]
        )


@dataclass
class PendingEntry:
    symbol: str
    signal_idx: int
    entry_idx: int
    p1_idx: int
    p2_idx: int
    low1: float
    low2: float
    rsi1: float
    rsi2: float
    score: float


@dataclass
class TradeState:
    symbol: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    sl_px: float
    tp_px: float
    high_max: float
    low_min: float
    p1_idx: Optional[int] = None
    p2_idx: Optional[int] = None
    low1: Optional[float] = None
    low2: Optional[float] = None
    rsi1: Optional[float] = None
    rsi2: Optional[float] = None
    score: Optional[float] = None
    exit_idx: Optional[int] = None
    exit_ts: Optional[int] = None
    exit_px: Optional[float] = None
    exit_reason: Optional[str] = None


def _calc_pnl(entry_px: float, exit_px: float, fee_rate: float, slip_pct: float) -> float:
    entry_fill = entry_px * (1 + slip_pct)
    exit_fill = exit_px * (1 - slip_pct)
    fees = entry_fill * fee_rate + exit_fill * fee_rate
    return (exit_fill - entry_fill - fees) / entry_fill if entry_fill > 0 else 0.0


def _write_trade_header(path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    exists = os.path.exists(path)
    if exists:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
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
        )


def _append_trade(path: str, row: List) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def parse_args():
    parser = argparse.ArgumentParser(description="DIV15m Long Divergence Backtest")
    parser.add_argument("--symbols", type=str, default="BTC/USDT:USDT")
    parser.add_argument("--start", type=str, required=True)
    parser.add_argument("--end", type=str, required=True)
    parser.add_argument("--tf", type=str, default="15m")
    parser.add_argument("--out-csv", type=str, default="logs/div15m_long/div15m_long_signals.csv")
    parser.add_argument("--out-trades", type=str, default="logs/div15m_long/div15m_long_trades.csv")
    parser.add_argument("--log-path", type=str, default="logs/div15m_long/backtest.log")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_ms = _parse_ts(args.start)
    end_ms = _parse_ts(args.end)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    log_fn, log_fp = _log_factory(args.log_path)
    if args.out_csv:
        _write_csv_header(args.out_csv)
    if args.out_trades:
        _write_trade_header(args.out_trades)

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    exchange.load_markets()

    cfg = Div15mConfig()
    engine = Div15mLongEngine(cfg)

    def _snapshot():
        return dict(engine.counters)

    for symbol in symbols:
        before = _snapshot()
        log_fn(f"[div15m] fetch sym={symbol} tf={args.tf} start={start_ms} end={end_ms}")
        rows = _fetch_ohlcv_all(exchange, symbol, args.tf, start_ms, end_ms)
        df = _to_df(rows)
        if df.empty or len(df) < 50:
            log_fn(f"[div15m] empty_or_short sym={symbol} rows={len(df)}")
            continue

        close = df["close"]
        rsi = engine._rsi_wilder(close, cfg.RSI_LEN)
        ema_fast = engine._ema(close, cfg.EMA_FAST)
        ema_slow = engine._ema(close, cfg.EMA_SLOW)
        ema_regime = engine._ema(close, cfg.EMA_REGIME_LEN)

        pending: Optional[PendingEntry] = None
        trade: Optional[TradeState] = None
        cooldown_until = -10**9
        seen_signals = set()
        trade_stats: Dict[str, int] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "timeouts": 0,
            "skipped_in_pos": 0,
            "skipped_cooldown": 0,
            "skipped_no_next": 0,
        }

        def _handle_entry_ready(event: Div15mEvent, ts: int, idx: int, c: float, h: float, l: float) -> None:
            nonlocal pending, trade, cooldown_until
            if event is None or event.event != "ENTRY_READY":
                return
            if trade is not None:
                trade_stats["skipped_in_pos"] += 1
                log_fn(f"DIV15_TRADE_SKIP sym={symbol} reason=in_position")
                if args.out_csv:
                    _append_csv(
                        args.out_csv,
                        Div15mEvent(
                            ts=ts,
                            symbol=symbol,
                            event="TRADE_SKIP",
                            entry_px=c,
                            p1_idx=event.p1_idx,
                            p2_idx=event.p2_idx,
                            low1=event.low1,
                            low2=event.low2,
                            rsi1=event.rsi1,
                            rsi2=event.rsi2,
                            score=event.score,
                            reasons="in_position",
                        ),
                    )
                return
            if idx <= cooldown_until:
                trade_stats["skipped_cooldown"] += 1
                log_fn(f"DIV15_TRADE_SKIP sym={symbol} reason=cooldown")
                if args.out_csv:
                    _append_csv(
                        args.out_csv,
                        Div15mEvent(
                            ts=ts,
                            symbol=symbol,
                            event="TRADE_SKIP",
                            entry_px=c,
                            p1_idx=event.p1_idx,
                            p2_idx=event.p2_idx,
                            low1=event.low1,
                            low2=event.low2,
                            rsi1=event.rsi1,
                            rsi2=event.rsi2,
                            score=event.score,
                            reasons="cooldown",
                        ),
                    )
                return
            if idx + 1 >= len(df) and cfg.ENTRY_MODE == "NEXT_OPEN":
                trade_stats["skipped_no_next"] += 1
                log_fn(f"DIV15_TRADE_SKIP sym={symbol} reason=no_next_bar")
                if args.out_csv:
                    _append_csv(
                        args.out_csv,
                        Div15mEvent(
                            ts=ts,
                            symbol=symbol,
                            event="TRADE_SKIP",
                            entry_px=c,
                            p1_idx=event.p1_idx,
                            p2_idx=event.p2_idx,
                            low1=event.low1,
                            low2=event.low2,
                            rsi1=event.rsi1,
                            rsi2=event.rsi2,
                            score=event.score,
                            reasons="no_next_bar",
                        ),
                    )
                return
            if cfg.ENTRY_MODE == "SIGNAL_CLOSE":
                entry_px = c
                if cfg.EXIT_MODE == "R_MULT":
                    risk_pct = cfg.RISK_PCT
                    sl_px = entry_px * (1 - risk_pct * cfg.SL_R_MULT)
                    tp_px = entry_px * (1 + risk_pct * cfg.TP_R_MULT)
                else:
                    sl_px = entry_px * (1 - cfg.SL_PCT)
                    tp_px = entry_px * (1 + cfg.TP_PCT)
                trade = TradeState(
                    symbol=symbol,
                    entry_idx=idx,
                    entry_ts=ts,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp_px=tp_px,
                    high_max=h,
                    low_min=l,
                    p1_idx=event.p1_idx,
                    p2_idx=event.p2_idx,
                    low1=event.low1,
                    low2=event.low2,
                    rsi1=event.rsi1,
                    rsi2=event.rsi2,
                    score=event.score,
                )
                log_fn(
                    "DIV15_TRADE_ENTRY "
                    f"sym={symbol} entry_px={entry_px} entry_ts={ts} "
                    f"sl={sl_px} tp={tp_px} mode={cfg.EXIT_MODE} "
                    f"risk_pct={cfg.RISK_PCT} sl_r={cfg.SL_R_MULT} tp_r={cfg.TP_R_MULT}"
                )
                log_fn(
                    "DIV15_TRADE_OPEN "
                    f"sym={symbol} entry_ts={ts} entry_px={entry_px} "
                    f"sl_px={sl_px} tp_px={tp_px} mode={cfg.EXIT_MODE}"
                )
                if args.out_csv:
                    _append_csv(
                        args.out_csv,
                        Div15mEvent(
                            ts=ts,
                            symbol=symbol,
                            event="TRADE_OPEN",
                            entry_px=entry_px,
                            p1_idx=event.p1_idx,
                            p2_idx=event.p2_idx,
                            low1=event.low1,
                            low2=event.low2,
                            rsi1=event.rsi1,
                            rsi2=event.rsi2,
                            score=event.score,
                            reasons=f"mode={cfg.EXIT_MODE}",
                        ),
                    )
                return

            entry_idx = idx + 1
            pending = PendingEntry(
                symbol=symbol,
                signal_idx=idx,
                entry_idx=entry_idx,
                p1_idx=event.p1_idx,
                p2_idx=event.p2_idx,
                low1=event.low1,
                low2=event.low2,
                rsi1=event.rsi1,
                rsi2=event.rsi2,
                score=event.score,
            )
            log_fn(
                "DIV15_TRADE_PENDING "
                f"sym={symbol} entry_idx={entry_idx} signal_ts={ts}"
            )
            if args.out_csv:
                _append_csv(
                    args.out_csv,
                    Div15mEvent(
                        ts=ts,
                        symbol=symbol,
                        event="TRADE_PENDING",
                        entry_px=c,
                        p1_idx=event.p1_idx,
                        p2_idx=event.p2_idx,
                        low1=event.low1,
                        low2=event.low2,
                        rsi1=event.rsi1,
                        rsi2=event.rsi2,
                        score=event.score,
                        reasons=f"entry_idx={entry_idx}",
                    ),
                )

        for idx in range(len(df)):
            ts = int(df["ts"].iloc[idx])
            o = float(df["open"].iloc[idx])
            h = float(df["high"].iloc[idx])
            l = float(df["low"].iloc[idx])
            c = float(df["close"].iloc[idx])

            if trade is not None:
                trade.high_max = max(trade.high_max, h)
                trade.low_min = min(trade.low_min, l)
                sl_hit = l <= trade.sl_px
                tp_hit = h >= trade.tp_px
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
                elif cfg.TIMEOUT_BARS > 0 and (idx - trade.entry_idx) >= cfg.TIMEOUT_BARS:
                    exit_reason = "TIMEOUT"
                    exit_px = c

                if exit_reason:
                    trade.exit_idx = idx
                    trade.exit_ts = ts
                    trade.exit_px = exit_px
                    trade.exit_reason = exit_reason
                    pnl_pct = _calc_pnl(trade.entry_px, exit_px, cfg.FEE_RATE, cfg.SLIPPAGE_PCT)
                    pnl_usdt = pnl_pct * cfg.POSITION_SIZE_USDT
                    holding_bars = idx - trade.entry_idx + 1
                    mfe = (trade.high_max - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
                    mae = (trade.low_min - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
                    log_fn(
                        "DIV15_TRADE_EXIT "
                        f"sym={symbol} reason={exit_reason} exit_px={exit_px} exit_ts={ts} "
                        f"pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} mae={mae:.4f} hold={holding_bars}"
                    )
                    trade_stats["trades"] += 1
                    if exit_reason == "TIMEOUT":
                        trade_stats["timeouts"] += 1
                    if pnl_pct >= 0:
                        trade_stats["wins"] += 1
                    else:
                        trade_stats["losses"] += 1
                    if args.out_trades:
                        _append_trade(
                            args.out_trades,
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
                    trade = None
                    cooldown_until = idx + cfg.TRADE_COOLDOWN_BARS

            if pending is not None and idx == pending.entry_idx:
                if trade is None:
                    entry_px = o if cfg.ENTRY_MODE == "NEXT_OPEN" else c
                    if cfg.EXIT_MODE == "R_MULT":
                        risk_pct = cfg.RISK_PCT
                        sl_px = entry_px * (1 - risk_pct * cfg.SL_R_MULT)
                        tp_px = entry_px * (1 + risk_pct * cfg.TP_R_MULT)
                    else:
                        sl_px = entry_px * (1 - cfg.SL_PCT)
                        tp_px = entry_px * (1 + cfg.TP_PCT)
                    trade = TradeState(
                        symbol=symbol,
                        entry_idx=idx,
                        entry_ts=ts,
                        entry_px=entry_px,
                        sl_px=sl_px,
                        tp_px=tp_px,
                        high_max=h,
                        low_min=l,
                        p1_idx=pending.p1_idx,
                        p2_idx=pending.p2_idx,
                        low1=pending.low1,
                        low2=pending.low2,
                        rsi1=pending.rsi1,
                        rsi2=pending.rsi2,
                        score=pending.score,
                    )
                    log_fn(
                        "DIV15_TRADE_ENTRY "
                        f"sym={symbol} entry_px={entry_px} entry_ts={ts} "
                        f"sl={sl_px} tp={tp_px} mode={cfg.EXIT_MODE} "
                        f"risk_pct={cfg.RISK_PCT} sl_r={cfg.SL_R_MULT} tp_r={cfg.TP_R_MULT}"
                    )
                    log_fn(
                        "DIV15_TRADE_OPEN "
                        f"sym={symbol} entry_ts={ts} entry_px={entry_px} "
                        f"sl_px={sl_px} tp_px={tp_px} mode={cfg.EXIT_MODE}"
                    )
                    if args.out_csv:
                        _append_csv(
                            args.out_csv,
                            Div15mEvent(
                                ts=ts,
                                symbol=symbol,
                                event="TRADE_OPEN",
                                entry_px=entry_px,
                                p1_idx=pending.p1_idx,
                                p2_idx=pending.p2_idx,
                                low1=pending.low1,
                                low2=pending.low2,
                                rsi1=pending.rsi1,
                                rsi2=pending.rsi2,
                                score=pending.score,
                                reasons=f"mode={cfg.EXIT_MODE}",
                            ),
                        )
                pending = None

            event = engine.process_candidate(symbol, df, idx, rsi, ema_fast, ema_slow, ema_regime, log_fn)
            if event and args.out_csv:
                key = (event.ts, event.p2_idx)
                if key not in seen_signals:
                    _append_csv(args.out_csv, event)
                    seen_signals.add(key)
            if event:
                _handle_entry_ready(event, ts, idx, c, h, l)

            event = engine.on_bar(symbol, df, idx, rsi, ema_fast, ema_slow, log_fn)
            if event and args.out_csv:
                key = (event.ts, event.p2_idx)
                if key not in seen_signals:
                    _append_csv(args.out_csv, event)
                    seen_signals.add(key)
            if event:
                _handle_entry_ready(event, ts, idx, c, h, l)

        if trade is not None:
            last_idx = len(df) - 1
            last_ts = int(df["ts"].iloc[last_idx])
            last_close = float(df["close"].iloc[last_idx])
            trade.exit_idx = last_idx
            trade.exit_ts = last_ts
            trade.exit_px = last_close
            trade.exit_reason = "EOT"
            pnl_pct = _calc_pnl(trade.entry_px, last_close, cfg.FEE_RATE, cfg.SLIPPAGE_PCT)
            pnl_usdt = pnl_pct * cfg.POSITION_SIZE_USDT
            holding_bars = last_idx - trade.entry_idx + 1
            mfe = (trade.high_max - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
            mae = (trade.low_min - trade.entry_px) / trade.entry_px if trade.entry_px > 0 else 0.0
            log_fn(
                "DIV15_TRADE_EXIT "
                f"sym={symbol} reason=EOT exit_px={last_close} exit_ts={last_ts} "
                f"pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} mae={mae:.4f} hold={holding_bars}"
            )
            trade_stats["trades"] += 1
            if pnl_pct >= 0:
                trade_stats["wins"] += 1
            else:
                trade_stats["losses"] += 1
            if args.out_trades:
                _append_trade(
                    args.out_trades,
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
            trade = None

        after = _snapshot()
        c = {k: after.get(k, 0) - before.get(k, 0) for k in after}
        log_fn(
            "[div15m-funnel] "
            f"sym={symbol} bars={c['bars']} pivots={c['pivots_found']} "
            f"candidate={c['candidate']} confirm_ok={c['confirm_ok']} "
            f"entry_ready={c['trigger_ok']} cooldown={c['skipped_cooldown']} dup={c['skipped_dup']}"
        )
        log_fn(
            "[div15m-confirm-fail] "
            f"no_os={c['confirm_fail_no_os']} "
            f"no_reclaim={c['confirm_fail_no_reclaim']} "
            f"spike_block={c['confirm_fail_spike']}"
        )
        log_fn(
            "[div15m-trades] "
            f"sym={symbol} trades={trade_stats['trades']} wins={trade_stats['wins']} "
            f"losses={trade_stats['losses']} timeouts={trade_stats['timeouts']} "
            f"skip_in_pos={trade_stats['skipped_in_pos']} "
            f"skip_cooldown={trade_stats['skipped_cooldown']} skip_no_next={trade_stats['skipped_no_next']} "
            f"regime_block={c.get('regime_block', 0)}"
        )

    total = engine.counters
    log_fn(
        "[div15m-funnel] "
        f"symbols={len(symbols)} bars={total['bars']} pivots={total['pivots_found']} "
        f"candidate={total['candidate']} confirm_ok={total['confirm_ok']} "
        f"entry_ready={total['trigger_ok']} cooldown={total['skipped_cooldown']} dup={total['skipped_dup']}"
    )
    log_fn(
        "[div15m-confirm-fail] "
        f"no_os={total['confirm_fail_no_os']} "
        f"no_reclaim={total['confirm_fail_no_reclaim']} "
        f"spike_block={total['confirm_fail_spike']}"
    )

    log_fp.close()


if __name__ == "__main__":
    main()
