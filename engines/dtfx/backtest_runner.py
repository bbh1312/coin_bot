"""
DTFX backtest runner: fetches historical OHLCV and runs engines on past candles.
"""
import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import ccxt
from ccxt.base.errors import DDoSProtection, NetworkError, RateLimitExceeded

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.base import EngineContext
from engines.dtfx.engine import DTFXConfig, DTFXEngine
from engines.dtfx.core.types import Candle
from engines.dtfx.dtfx_long import DTFXLongEngine
from engines.dtfx.dtfx_short import DTFXShortEngine
from engines.dtfx.config import get_default_params


def parse_args():
    parser = argparse.ArgumentParser(description="DTFX Backtest Runner")
    parser.add_argument("--symbols", type=str, default="")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--sl-pct", type=float, default=0.0)
    parser.add_argument("--tp-pct", type=float, default=0.0)
    parser.add_argument("--tf_ltf", type=str, default="5m")
    parser.add_argument("--tf_mtf", type=str, default="15m")
    parser.add_argument("--tf_htf", type=str, default="1h")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--sleep-ms", type=int, default=200)
    parser.add_argument("--retry", type=int, default=3)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _since_ms(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _log_factory(path: str, verbose: bool):
    _ensure_dir(os.path.dirname(path))
    fp = open(path, "a", encoding="utf-8")

    def _log(msg: str) -> None:
        fp.write(msg + "\n")
        fp.flush()
        if verbose:
            print(msg)

    return _log, fp


def _write_trades_header(path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "symbol,side,entry_ts,entry_px,exit_ts,exit_px,exit_reason,pnl_pct,holding_bars,mfe,mae,hold_min\n"
        )


def _append_trade(path: str, row: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(row + "\n")


def _append_log_line(path: str, line: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _summarize_dtfx_events(path: str) -> Optional[dict]:
    if not path or not os.path.exists(path):
        return None
    summary = {
        "sweep_rejected_strong_breakout": 0,
        "mss_rejected_soft_not_reached": 0,
        "mss_rejected_confirm_not_reached": 0,
        "mss_rejected_other": 0,
        "zone_touch_seen": 0,
        "entry_blocked_total": 0,
        "entry_blocked_wick_ratio": 0,
        "entry_blocked_body_atr": 0,
        "entry_blocked_close_ratio": 0,
        "entry_blocked_color": 0,
        "entry_blocked_requires_full_close": 0,
        "entry_blocked_doji": 0,
        "entry_blocked_mss_dir": 0,
    }
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            event = row.get("event")
            context = row.get("context") if isinstance(row, dict) else None
            if not isinstance(context, dict):
                context = {}
            if event == "SWEEP_REJECTED":
                if context.get("reason") == "strong_breakout":
                    summary["sweep_rejected_strong_breakout"] += 1
                continue
            if event == "MSS_REJECTED":
                reason = context.get("reason")
                if reason == "soft_not_reached":
                    summary["mss_rejected_soft_not_reached"] += 1
                elif reason == "confirm_not_reached":
                    summary["mss_rejected_confirm_not_reached"] += 1
                else:
                    summary["mss_rejected_other"] += 1
                continue
            if event == "ZONE_TOUCH_SEEN":
                summary["zone_touch_seen"] += 1
                continue
            if event == "ENTRY_BLOCKED":
                summary["entry_blocked_total"] += 1
                entry_conditions = context.get("entry_conditions")
                failed = entry_conditions.get("failed") if isinstance(entry_conditions, dict) else None
                if isinstance(failed, list):
                    for reason in failed:
                        if reason == "wick_ratio_ok":
                            summary["entry_blocked_wick_ratio"] += 1
                        elif reason == "body_atr_min":
                            summary["entry_blocked_body_atr"] += 1
                        elif reason == "confirm_close_ratio":
                            summary["entry_blocked_close_ratio"] += 1
                        elif reason == "candle_opposite_color":
                            summary["entry_blocked_color"] += 1
                        elif reason == "requires_full_close":
                            summary["entry_blocked_requires_full_close"] += 1
                        elif reason == "doji_block":
                            summary["entry_blocked_doji"] += 1
                        elif reason == "mss_dir_aligned":
                            summary["entry_blocked_mss_dir"] += 1
                continue
    return summary


def _to_candles(ohlcv: List[list]) -> List[Candle]:
    return [
        Candle(ts=int(c[0]), o=float(c[1]), h=float(c[2]), l=float(c[3]), c=float(c[4]), v=float(c[5]))
        for c in ohlcv
    ]


def _tf_minutes(tf: str) -> float:
    if not tf:
        return 0.0
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except Exception:
        return 0.0
    if unit == "m":
        return float(value)
    if unit == "h":
        return float(value * 60)
    if unit == "d":
        return float(value * 1440)
    return 0.0


@dataclass
class TradeState:
    symbol: str
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    sl_px: float
    tp_px: float
    mfe: float = 0.0
    mae: float = 0.0


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    tp: int = 0
    sl: int = 0
    sum_mfe: float = 0.0
    sum_mae: float = 0.0
    sum_hold_min: float = 0.0


def _update_excursions(trade: TradeState, candle: Candle) -> None:
    if trade.side == "SHORT":
        favorable = (trade.entry_px - candle.l) / trade.entry_px
        adverse = (candle.h - trade.entry_px) / trade.entry_px
    else:
        favorable = (candle.h - trade.entry_px) / trade.entry_px
        adverse = (trade.entry_px - candle.l) / trade.entry_px
    trade.mfe = max(trade.mfe, favorable)
    trade.mae = max(trade.mae, adverse)


def _check_exit(trade: TradeState, candle: Candle) -> Optional[tuple]:
    if trade.side == "SHORT":
        if candle.h >= trade.sl_px:
            return ("SL", trade.sl_px)
        if candle.l <= trade.tp_px:
            return ("TP", trade.tp_px)
        return None
    if candle.l <= trade.sl_px:
        return ("SL", trade.sl_px)
    if candle.h >= trade.tp_px:
        return ("TP", trade.tp_px)
    return None


def _pnl_pct(side: str, entry_px: float, exit_px: float) -> float:
    if entry_px <= 0:
        return 0.0
    if side == "SHORT":
        return (entry_px - exit_px) / entry_px
    return (exit_px - entry_px) / entry_px


def fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since_ms: int,
    end_ms: int,
    sleep_ms: int,
    max_retries: int,
) -> List[list]:
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    since = since_ms
    out = []
    last_ts = None
    while since < end_ms:
        attempt = 0
        ohlcv = None
        while attempt <= max_retries:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1500)
                break
            except (DDoSProtection, RateLimitExceeded, NetworkError):
                attempt += 1
                time.sleep(min(10, 2 + attempt * 2))
        if not ohlcv:
            break
        for row in ohlcv:
            ts = int(row[0])
            if ts > end_ms:
                continue
            if last_ts is None or ts > last_ts:
                out.append(row)
                last_ts = ts
        new_last = int(ohlcv[-1][0])
        if last_ts is None or new_last == last_ts:
            since = new_last + tf_ms
        else:
            since = last_ts + tf_ms
        if len(ohlcv) < 2:
            break
        time.sleep(max(exchange.rateLimit, sleep_ms) / 1000.0)
    if out:
        out = out[:-1]
    return out


def run_backtest(
    symbols: List[str],
    tfs: dict,
    days: int,
    exchange: ccxt.Exchange,
    params: dict,
    dtfx_cfg: DTFXConfig,
    sl_pct: float,
    tp_pct: float,
    verbose: bool,
    log_path: str,
    trades_csv: str,
    sleep_ms: int,
    max_retries: int,
) -> dict:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    if verbose:
        print(f"[BACKTEST] Fetching {len(symbols)} symbols LTF={tfs['ltf']} from last {days} days")

    symbol_data = {}
    for symbol in symbols:
        ltf_raw = fetch_ohlcv_all(exchange, symbol, tfs["ltf"], since_ms, end_ms, sleep_ms, max_retries)
        mtf_raw = fetch_ohlcv_all(exchange, symbol, tfs["mtf"], since_ms, end_ms, sleep_ms, max_retries)
        htf_raw = fetch_ohlcv_all(exchange, symbol, tfs["htf"], since_ms, end_ms, sleep_ms, max_retries)
        if not ltf_raw or not mtf_raw or not htf_raw:
            if verbose:
                print(f"[BACKTEST] Missing data for {symbol}. Skipping.")
            continue
        symbol_data[symbol] = {
            "ltf": _to_candles(ltf_raw),
            "mtf": _to_candles(mtf_raw),
            "htf": _to_candles(htf_raw),
        }

    if not symbol_data:
        return {}

    long_engine = DTFXLongEngine(dict(params), dict(tfs), exchange)
    short_engine = DTFXShortEngine(dict(params), dict(tfs), exchange)
    for symbol in symbol_data:
        base = symbol.split("/")[0].split(":")[0].upper()
        role = "anchor" if base in params.get("anchor_symbols", set()) else "alt"
        long_engine.symbol_roles[symbol] = role
        short_engine.symbol_roles[symbol] = role

    ltf_minutes = _tf_minutes(tfs["ltf"])
    ltf_limit = int(getattr(dtfx_cfg, "ltf_limit", 200) or 200)
    mtf_limit = int(getattr(dtfx_cfg, "mtf_limit", 200) or 200)
    htf_limit = int(getattr(dtfx_cfg, "htf_limit", 200) or 200)

    state: dict = {}
    log, _log_fp = _log_factory(log_path, verbose)
    stats_map = {symbol: Stats() for symbol in symbol_data}
    active_trades: dict = {symbol: None for symbol in symbol_data}
    mtf_idx = {symbol: -1 for symbol in symbol_data}
    htf_idx = {symbol: -1 for symbol in symbol_data}

    events = []
    for symbol, data in symbol_data.items():
        ltf = data["ltf"]
        for i in range(1, len(ltf)):
            events.append((ltf[i - 1].ts, symbol, i))
    events.sort(key=lambda x: x[0])

    for effective_ts, symbol, i in events:
        data = symbol_data[symbol]
        ltf = data["ltf"]
        mtf = data["mtf"]
        htf = data["htf"]
        now_ts = effective_ts / 1000.0
        state["_current_cycle_ts"] = effective_ts

        while (mtf_idx[symbol] + 1) < len(mtf) and mtf[mtf_idx[symbol] + 1].ts <= effective_ts:
            mtf_idx[symbol] += 1
        while (htf_idx[symbol] + 1) < len(htf) and htf[htf_idx[symbol] + 1].ts <= effective_ts:
            htf_idx[symbol] += 1

        ltf_window = ltf[max(0, i - ltf_limit + 1) : i + 1]
        ltf_slice = ltf_window[:-1] if len(ltf_window) > 1 else []
        mtf_window = mtf[max(0, mtf_idx[symbol] - mtf_limit + 1) : mtf_idx[symbol] + 1] if mtf_idx[symbol] >= 0 else []
        mtf_slice = mtf_window[:-1] if len(mtf_window) > 1 else []
        htf_window = htf[max(0, htf_idx[symbol] - htf_limit + 1) : htf_idx[symbol] + 1] if htf_idx[symbol] >= 0 else []
        htf_slice = htf_window[:-1] if len(htf_window) > 1 else []

        trade = active_trades.get(symbol)
        if trade and i > trade.entry_idx:
            _update_excursions(trade, ltf[i])
            exit_hit = _check_exit(trade, ltf[i])
            if exit_hit:
                reason, exit_px = exit_hit
                stats = stats_map[symbol]
                stats.trades += 1
                if reason == "TP":
                    stats.wins += 1
                    stats.tp += 1
                else:
                    stats.losses += 1
                    stats.sl += 1
                stats.sum_mfe += trade.mfe
                stats.sum_mae += trade.mae
                hold_bars = i - trade.entry_idx
                stats.sum_hold_min += hold_bars * ltf_minutes
                pnl = _pnl_pct(trade.side, trade.entry_px, exit_px)
                log(
                    f"[BACKTEST][EXIT] sym={symbol} side={trade.side} "
                    f"entry_ts={trade.entry_ts} entry_px={trade.entry_px:.6f} "
                    f"exit_ts={ltf[i].ts} exit_px={exit_px:.6f} reason={reason} pnl_pct={pnl:.4f} "
                    f"hold_bars={hold_bars} mfe={trade.mfe:.4f} mae={trade.mae:.4f}"
                )
                _append_trade(
                    trades_csv,
                    f"{symbol},{trade.side},{trade.entry_ts},{trade.entry_px:.6f},"
                    f"{ltf[i].ts},{exit_px:.6f},{reason},{pnl:.6f},{hold_bars},"
                    f"{trade.mfe:.6f},{trade.mae:.6f},{hold_bars * ltf_minutes:.1f}",
                )
                active_trades[symbol] = None
                st = state.get(symbol, {})
                if isinstance(st, dict):
                    st["in_pos"] = False
                    state[symbol] = st
            continue

        if trade or not ltf_slice or not mtf_slice or not htf_slice:
            continue

        long_sig = long_engine.on_candle(symbol, ltf_slice, mtf_slice, htf_slice)
        short_sig = short_engine.on_candle(symbol, ltf_slice, mtf_slice, htf_slice)
        if sl_pct <= 0 or tp_pct <= 0:
            continue

        for sig, side in ((long_sig, "LONG"), (short_sig, "SHORT")):
            if trade or active_trades.get(symbol):
                break
            if not sig or not sig.event.startswith("ENTRY_READY"):
                continue
            entry_px = ltf[i - 1].c
            trade = TradeState(
                symbol=symbol,
                side=side,
                entry_idx=i - 1,
                entry_ts=effective_ts,
                entry_px=entry_px,
                sl_px=entry_px * (1 - sl_pct) if side == "LONG" else entry_px * (1 + sl_pct),
                tp_px=entry_px * (1 + tp_pct) if side == "LONG" else entry_px * (1 - tp_pct),
            )
            active_trades[symbol] = trade
            log(
                f"[BACKTEST][ENTRY] sym={symbol} side={side} entry_ts={effective_ts} entry_px={entry_px:.6f} "
                f"sl_px={trade.sl_px:.6f} tp_px={trade.tp_px:.6f}"
            )
            st = state.get(symbol, {})
            if isinstance(st, dict):
                st["in_pos"] = True
                st["last_entry"] = now_ts
                state[symbol] = st
            break

    return stats_map


def _merge_stats(total: Stats, part: Stats) -> None:
    total.trades += part.trades
    total.wins += part.wins
    total.losses += part.losses
    total.tp += part.tp
    total.sl += part.sl
    total.sum_mfe += part.sum_mfe
    total.sum_mae += part.sum_mae
    total.sum_hold_min += part.sum_hold_min


def _format_summary(symbol: str, stats: Stats) -> str:
    if stats.trades <= 0:
        winrate = 0.0
        avg_mfe = 0.0
        avg_mae = 0.0
        avg_hold = 0.0
    else:
        winrate = (stats.wins / stats.trades) * 100
        avg_mfe = stats.sum_mfe / stats.trades
        avg_mae = stats.sum_mae / stats.trades
        avg_hold = stats.sum_hold_min / stats.trades
    return (
        f"[BACKTEST] {symbol} trades={stats.trades} wins={stats.wins} losses={stats.losses} "
        f"winrate={winrate:.2f}% tp={stats.tp} sl={stats.sl} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}"
    )


def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.environ["DTFX_LOG_PREFIX"] = f"backtest_{run_id}"
    os.environ["DTFX_LOG_MODE"] = "backtest"
    os.environ["DTFX_LOG_QUIET"] = "1"
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    tfs = {"ltf": args.tf_ltf, "mtf": args.tf_mtf, "htf": args.tf_htf}

    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.load_markets()

    params = get_default_params()
    params["major_symbols"] = set(params.get("major_symbols") or [])
    params["anchor_symbols"] = set(params.get("anchor_symbols") or [])

    dtfx_cfg = DTFXConfig(tf_ltf=args.tf_ltf, tf_mtf=args.tf_mtf, tf_htf=args.tf_htf)
    if not symbols:
        tickers = exchange.fetch_tickers()
        dtfx_engine = DTFXEngine(config=dtfx_cfg)
        state = {"_tickers": tickers, "_symbols": list(tickers.keys())}
        ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=lambda *_: None, config=dtfx_cfg)
        dtfx_engine.on_start(ctx)
        symbols = dtfx_engine.build_universe(ctx)
        if args.verbose:
            print(f"[BACKTEST] dtfx universe size={len(symbols)}")
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]

    run_day = datetime.now(timezone.utc).strftime("%Y%m%d")
    log_dir = os.path.join("logs", "dtfx", "backtest")
    _ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"backtest_{run_day}.log")
    trades_csv = os.path.join(log_dir, f"backtest_trades_{run_day}.csv")
    _write_trades_header(trades_csv)
    dtfx_log_path = os.path.join("logs", "dtfx", f"dtfx_backtest_{run_id}_{run_day}.jsonl")

    stats_map = run_backtest(
        symbols,
        tfs,
        args.days,
        exchange,
        params,
        dtfx_cfg,
        args.sl_pct,
        args.tp_pct,
        args.verbose,
        log_path,
        trades_csv,
        args.sleep_ms,
        args.retry,
    )

    total = Stats()
    for symbol in symbols:
        stats = stats_map.get(symbol, Stats())
        print(_format_summary(symbol, stats))
        _merge_stats(total, stats)
    print(_format_summary("TOTAL", total))
    event_summary = _summarize_dtfx_events(dtfx_log_path)
    if event_summary:
        summary_line = (
            "[BACKTEST][FUNNEL] "
            f"sweep_rejected_strong_breakout={event_summary['sweep_rejected_strong_breakout']} "
            f"mss_rejected_soft_not_reached={event_summary['mss_rejected_soft_not_reached']} "
            f"mss_rejected_confirm_not_reached={event_summary['mss_rejected_confirm_not_reached']} "
            f"mss_rejected_other={event_summary['mss_rejected_other']} "
            f"zone_touch_seen={event_summary['zone_touch_seen']} "
            f"entry_blocked_total={event_summary['entry_blocked_total']} "
            f"entry_blocked_wick_ratio={event_summary['entry_blocked_wick_ratio']} "
            f"entry_blocked_body_atr={event_summary['entry_blocked_body_atr']} "
            f"entry_blocked_close_ratio={event_summary['entry_blocked_close_ratio']} "
            f"entry_blocked_color={event_summary['entry_blocked_color']} "
            f"entry_blocked_requires_full_close={event_summary['entry_blocked_requires_full_close']} "
            f"entry_blocked_doji={event_summary['entry_blocked_doji']} "
            f"entry_blocked_mss_dir={event_summary['entry_blocked_mss_dir']}"
        )
        print(summary_line)
        _append_log_line(log_path, summary_line)


if __name__ == "__main__":
    main()
