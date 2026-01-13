"""
PumpFade backtest runner: fetches historical OHLCV and emits ENTRY_READY signals.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import ccxt

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import cycle_cache
from engines.base import EngineContext
from engines.pumpfade.engine import PumpFadeEngine
from engines.pumpfade.config import PumpFadeConfig


def parse_args():
    parser = argparse.ArgumentParser(description="PumpFade Backtest Runner")
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
    )
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--tf_trigger", type=str, default="15m")
    parser.add_argument("--tf_confirm", type=str, default="5m")
    parser.add_argument("--tf_vol", type=str, default="1h")
    parser.add_argument("--use_5m_confirm", action="store_true")
    parser.add_argument("--sl-pct", type=float, default=0.03)
    parser.add_argument("--tp-pct", type=float, default=0.03)
    parser.add_argument("--out", type=str, default="pumpfade_signals.csv")
    parser.add_argument("--out-trades", type=str, default="pumpfade_trades.csv")
    parser.add_argument("--out-cancels", type=str, default="pumpfade_cancels.csv")
    parser.add_argument("--log-path", type=str, default="backtest.log")
    parser.add_argument("--aggressive", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _since_ms(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


def fetch_ohlcv_all(exchange: ccxt.Exchange, symbol: str, timeframe: str, since_ms: int, end_ms: int) -> List[list]:
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    since = since_ms
    out = []
    last_ts = None
    while since < end_ms:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1500)
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
        time.sleep(exchange.rateLimit / 1000.0)
    if out:
        out = out[:-1]
    return out


def _slice_to_idx(rows: List[list], idx: int) -> List[list]:
    if idx < 0:
        return []
    end = min(len(rows), idx + 1)
    return rows[:end]


def _append_line(path: str, line: str) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _log_summary(line: str, log_path: str) -> None:
    _append_line(log_path, line)
    print(line)

def _csv_header(path: str, header: str) -> None:
    if not path:
        return
    if os.path.exists(path):
        return
    _append_line(path, header)


def run_symbol(
    symbol: str,
    tfs: dict,
    days: int,
    exchange: ccxt.Exchange,
    engine: PumpFadeEngine,
    cfg: PumpFadeConfig,
    out_path: str,
    out_trades: str,
    out_cancels: str,
    log_path: str,
    sl_pct: float,
    tp_pct: float,
    tf_minutes: float,
    verbose: bool,
) -> None:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    tf15_raw = fetch_ohlcv_all(exchange, symbol, tfs["trigger"], since_ms, end_ms)
    tf5_raw = fetch_ohlcv_all(exchange, symbol, tfs["confirm"], since_ms, end_ms)
    tf1h_raw = fetch_ohlcv_all(exchange, symbol, tfs["vol"], since_ms, end_ms)
    if not tf15_raw:
        _log_summary(f"[BACKTEST] {symbol} trades=0 wins=0 losses=0 winrate=0.00% tp=0 sl=0 avg_mfe=0.0000 avg_mae=0.0000 avg_hold=0.0", log_path)
        return

    state = {"_universe": [symbol]}
    ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=print, config=cfg)

    idx_15 = -1
    idx_5 = -1
    idx_1h = -1
    pending = None
    trade = None
    stats = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "pending_cancel": 0,
        "pending_cancel_timeout": 0,
        "pending_cancel_prior_hh": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }
    block_counts = {}
    eval_count = 0
    confirm_stats = {}
    for i in range(len(tf15_raw) - 1):
        idx_15 = i
        ts = int(tf15_raw[idx_15][0])
        while (idx_5 + 1) < len(tf5_raw) and int(tf5_raw[idx_5 + 1][0]) <= ts:
            idx_5 += 1
        while (idx_1h + 1) < len(tf1h_raw) and int(tf1h_raw[idx_1h + 1][0]) <= ts:
            idx_1h += 1

        cycle_cache.clear_cycle_cache(keep_raw=True)
        cycle_cache.set_raw(symbol, tfs["trigger"], _slice_to_idx(tf15_raw, idx_15))
        if tf5_raw:
            cycle_cache.set_raw(symbol, tfs["confirm"], _slice_to_idx(tf5_raw, idx_5))
        if tf1h_raw:
            cycle_cache.set_raw(symbol, tfs["vol"], _slice_to_idx(tf1h_raw, idx_1h))

        ctx.now_ts = ts / 1000.0
        o = float(tf15_raw[idx_15][1])
        h = float(tf15_raw[idx_15][2])
        l = float(tf15_raw[idx_15][3])
        c = float(tf15_raw[idx_15][4])

        if trade is not None:
            entry_px = trade["entry_px"]
            if entry_px > 0:
                mfe = (entry_px - l) / entry_px
                mae = (h - entry_px) / entry_px
                if mfe > trade["mfe"]:
                    trade["mfe"] = mfe
                if mae > trade["mae"]:
                    trade["mae"] = mae
            sl_hit = h >= trade["sl_px"]
            tp_hit = l <= trade["tp_px"]
            exit_reason = None
            exit_px = None
            if sl_hit and tp_hit:
                exit_reason = "SL"
                exit_px = trade["sl_px"]
            elif sl_hit:
                exit_reason = "SL"
                exit_px = trade["sl_px"]
            elif tp_hit:
                exit_reason = "TP"
                exit_px = trade["tp_px"]
            if exit_reason:
                pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                stats["trades"] += 1
                if pnl_pct >= 0:
                    stats["wins"] += 1
                else:
                    stats["losses"] += 1
                if exit_reason == "TP":
                    stats["tp"] += 1
                else:
                    stats["sl"] += 1
                holding_bars = max(1, idx_15 - trade["entry_idx"] + 1)
                hold_min = holding_bars * tf_minutes
                stats["mfe_sum"] += trade["mfe"]
                stats["mae_sum"] += trade["mae"]
                stats["hold_sum"] += hold_min
                confirm_type = trade.get("confirm_type") or "UNKNOWN"
                ct_stats = confirm_stats.setdefault(
                    confirm_type,
                    {
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
                ct_stats["trades"] += 1
                if pnl_pct >= 0:
                    ct_stats["wins"] += 1
                else:
                    ct_stats["losses"] += 1
                if exit_reason == "TP":
                    ct_stats["tp"] += 1
                else:
                    ct_stats["sl"] += 1
                ct_stats["mfe_sum"] += trade["mfe"]
                ct_stats["mae_sum"] += trade["mae"]
                ct_stats["hold_sum"] += hold_min
                log_exit = (
                    "PUMPFADE_TRADE_EXIT "
                    f"sym={symbol} reason={exit_reason} entry_px={trade['entry_px']:.6g} "
                    f"exit_px={exit_px:.6g} pnl_pct={pnl_pct:.4f} "
                    f"mfe={trade['mfe']:.4f} mae={trade['mae']:.4f} hold={holding_bars}"
                )
                if verbose:
                    print(log_exit)
                _append_line(
                    out_trades,
                    ",".join(
                        [
                            trade.get("entry_dt") or "",
                            datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            symbol,
                            f"{trade['entry_px']:.6g}",
                            f"{exit_px:.6g}",
                            exit_reason,
                            f"{pnl_pct:.6f}",
                            f"{trade['mfe']:.6f}",
                            f"{trade['mae']:.6f}",
                    f"{hold_min:.1f}",
                            str(trade.get("confirm_type") or ""),
                            "1" if trade.get("aggressive_mode") else "0",
                            f"{trade.get('prior_hh') or ''}",
                            f"{trade.get('failure_high') or ''}",
                            f"{trade.get('failure_low') or ''}",
                            f"{trade.get('entry_gap_high') or ''}",
                            f"{trade.get('entry_gap_ema7') or ''}",
                        ]
                    ),
                )
                trade = None

        if pending is not None and trade is None:
            if pending.get("prior_hh") and h >= pending["prior_hh"] * (1 + cfg.failure_eps):
                stats["pending_cancel"] += 1
                stats["pending_cancel_prior_hh"] += 1
                _append_line(
                    out_cancels,
                    ",".join(
                        [
                            pending.get("signal_dt") or "",
                            datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            symbol,
                            f"{pending.get('entry_px'):.6g}" if pending.get("entry_px") else "",
                            "PRIOR_HH",
                            str(pending.get("confirm_type") or ""),
                            "1" if pending.get("aggressive_mode") else "0",
                            f"{pending.get('prior_hh') or ''}",
                            f"{pending.get('failure_high') or ''}",
                            f"{pending.get('failure_low') or ''}",
                            f"{pending.get('entry_gap_high') or ''}",
                            f"{pending.get('entry_gap_ema7') or ''}",
                        ]
                    ),
                )
                pending = None
            elif h >= pending["entry_px"]:
                entry_px = pending["entry_px"]
                trade = {
                    "entry_px": entry_px,
                    "entry_ts": ts,
                    "entry_dt": pending.get("signal_dt"),
                    "sl_px": entry_px * (1 + sl_pct),
                    "tp_px": entry_px * (1 - tp_pct),
                    "entry_idx": idx_15,
                    "mfe": 0.0,
                    "mae": 0.0,
                    "confirm_type": pending.get("confirm_type"),
                    "aggressive_mode": pending.get("aggressive_mode"),
                    "prior_hh": pending.get("prior_hh"),
                    "failure_high": pending.get("failure_high"),
                    "failure_low": pending.get("failure_low"),
                    "entry_gap_high": pending.get("entry_gap_high"),
                    "entry_gap_ema7": pending.get("entry_gap_ema7"),
                }
                if verbose:
                    print(
                        "PUMPFADE_TRADE_ENTRY "
                        f"sym={symbol} entry_px={entry_px:.6g} sl={trade['sl_px']:.6g} "
                        f"tp={trade['tp_px']:.6g} confirm={trade.get('confirm_type') or 'N/A'}"
                    )
                pending = None
            elif idx_15 > pending["expire_idx"]:
                stats["pending_cancel"] += 1
                stats["pending_cancel_timeout"] += 1
                _append_line(
                    out_cancels,
                    ",".join(
                        [
                            pending.get("signal_dt") or "",
                            datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            symbol,
                            f"{pending.get('entry_px'):.6g}" if pending.get("entry_px") else "",
                            "TIMEOUT",
                            str(pending.get("confirm_type") or ""),
                            "1" if pending.get("aggressive_mode") else "0",
                            f"{pending.get('prior_hh') or ''}",
                            f"{pending.get('failure_high') or ''}",
                            f"{pending.get('failure_low') or ''}",
                            f"{pending.get('entry_gap_high') or ''}",
                            f"{pending.get('entry_gap_ema7') or ''}",
                        ]
                    ),
                )
                pending = None

        if trade is not None or pending is not None:
            continue

        sig = engine.evaluate_symbol(ctx, symbol, cfg)
        if not sig:
            continue
        eval_count += 1
        if not sig.entry_ready:
            reason = (sig.meta or {}).get("block_reason")
            if reason:
                block_counts[reason] = block_counts.get(reason, 0) + 1
            continue
        meta = sig.meta or {}
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        confirm_subtype = meta.get("confirm_subtype") or meta.get("confirm_type") or ""
        aggr_flag = "Y" if meta.get("aggressive_mode") else "N"
        if verbose:
            print(
                f"[ENTRY] {symbol} {dt}Z price={sig.entry_price:.6g} confirm={confirm_subtype} "
                f"aggr={aggr_flag} reasons={','.join(meta.get('reasons') or [])}"
            )
        entry_gap_high = None
        entry_gap_ema7 = None
        failure_high = meta.get("failure_high")
        ema7_now = meta.get("ema7_now")
        if isinstance(failure_high, (int, float)):
            entry_gap_high = float(failure_high) - float(sig.entry_price)
        if isinstance(ema7_now, (int, float)):
            entry_gap_ema7 = float(sig.entry_price) - float(ema7_now)
        line = ",".join(
            [
                dt,
                symbol,
                f"{sig.entry_price:.6g}",
                f"{meta.get('hh_n') or ''}",
                f"{meta.get('failure_high') or ''}",
                f"{meta.get('failure_low') or ''}",
                f"{meta.get('confirm_close') or ''}",
                f"{confirm_subtype}",
                "1" if meta.get("aggressive_mode") else "0",
                f"{entry_gap_high or ''}",
                f"{entry_gap_ema7 or ''}",
                f"{meta.get('vol_failure') or ''}",
                f"{meta.get('vol_peak') or ''}",
                f"{meta.get('rsi') or ''}",
                "Y" if meta.get("rsi_turn") else "N",
                "Y" if meta.get("macd_hist_increasing") else "N",
                ";".join(meta.get("reasons") or []),
            ]
        )
        _append_line(out_path, line)
        pending = {
            "entry_px": sig.entry_price,
            "signal_idx": idx_15,
            "expire_idx": idx_15 + max(1, int(cfg.entry_timeout_bars)),
            "prior_hh": meta.get("prior_hh"),
            "confirm_type": confirm_subtype or "UNKNOWN",
            "aggressive_mode": bool(meta.get("aggressive_mode")),
            "signal_dt": dt,
            "failure_high": meta.get("failure_high"),
            "failure_low": meta.get("failure_low"),
            "entry_gap_high": entry_gap_high,
            "entry_gap_ema7": entry_gap_ema7,
        }

    winrate = (stats["wins"] / stats["trades"] * 100.0) if stats["trades"] > 0 else 0.0
    avg_mfe = (stats["mfe_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    avg_mae = (stats["mae_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    avg_hold = (stats["hold_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    _log_summary(
        f"[BACKTEST] {symbol} trades={stats['trades']} wins={stats['wins']} losses={stats['losses']} "
        f"winrate={winrate:.2f}% tp={stats['tp']} sl={stats['sl']} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}",
        log_path,
    )
    if verbose and eval_count:
        _log_summary(f"[BACKTEST] {symbol} block_summary total_eval={eval_count}", log_path)
        for reason, count in sorted(block_counts.items(), key=lambda x: x[1], reverse=True):
            ratio = count / eval_count * 100.0
            _log_summary(f"[BACKTEST] {symbol} block={reason} count={count} ratio={ratio:.2f}%", log_path)
    if verbose and confirm_stats:
        _log_summary(f"[BACKTEST] {symbol} confirm_type_stats", log_path)
        for ctype, row in sorted(confirm_stats.items(), key=lambda x: x[0]):
            trades = row["trades"]
            win_rate = (row["wins"] / trades * 100.0) if trades else 0.0
            avg_mfe_ct = (row["mfe_sum"] / trades) if trades else 0.0
            avg_mae_ct = (row["mae_sum"] / trades) if trades else 0.0
            avg_hold_ct = (row["hold_sum"] / trades) if trades else 0.0
            sl_rate = (row["sl"] / trades * 100.0) if trades else 0.0
            _log_summary(
                "[BACKTEST] {sym} confirm={ctype} trades={trades} wins={wins} "
                "winrate={win_rate:.2f}% sl_rate={sl_rate:.2f}% "
                "avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}".format(
                    sym=symbol,
                    ctype=ctype,
                    trades=trades,
                    wins=row["wins"],
                    win_rate=win_rate,
                    sl_rate=sl_rate,
                    avg_mfe=avg_mfe_ct,
                    avg_mae=avg_mae_ct,
                    avg_hold=avg_hold_ct,
                ),
                log_path,
            )
    return stats


def main():
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    tfs = {"trigger": args.tf_trigger, "confirm": args.tf_confirm, "vol": args.tf_vol}

    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.load_markets()

    cfg = PumpFadeConfig()
    cfg.tf_trigger = args.tf_trigger
    cfg.tf_confirm = args.tf_confirm
    cfg.confirm_use_5m = bool(args.use_5m_confirm)
    cfg.aggressive_mode = bool(args.aggressive)
    engine = PumpFadeEngine(cfg)

    base_dir = os.path.join("logs", "pumpfade", "backtest")
    os.makedirs(base_dir, exist_ok=True)
    out_path = os.path.join(base_dir, os.path.basename(args.out)) if args.out else ""
    out_trades = os.path.join(base_dir, os.path.basename(args.out_trades)) if args.out_trades else ""
    out_cancels = os.path.join(base_dir, os.path.basename(args.out_cancels)) if args.out_cancels else ""
    log_path = os.path.join(base_dir, os.path.basename(args.log_path)) if args.log_path else ""
    if out_path and not out_trades:
        base, ext = os.path.splitext(out_path)
        if not ext:
            ext = ".csv"
        out_trades = f"{base}_trades{ext}"
    if out_path and not out_cancels:
        base, ext = os.path.splitext(out_path)
        if not ext:
            ext = ".csv"
        out_cancels = f"{base}_cancels{ext}"
    if out_path:
        _append_line(
            out_path,
            "ts,symbol,entry,hh,failure_high,failure_low,confirm_close,confirm_subtype,aggressive_mode,"
            "entry_gap_high,entry_gap_ema7,vol_failure,vol_peak,rsi,rsi_turn,macd_increasing,reasons",
        )
    _csv_header(
        out_trades,
        "entry_ts,exit_ts,symbol,entry_px,exit_px,exit_reason,pnl_pct,mfe,mae,hold_min,confirm_subtype,"
        "aggressive_mode,prior_hh,failure_high,failure_low,entry_gap_high,entry_gap_ema7",
    )
    _csv_header(
        out_cancels,
        "signal_ts,cancel_ts,symbol,entry_px,cancel_reason,confirm_subtype,aggressive_mode,"
        "prior_hh,failure_high,failure_low,entry_gap_high,entry_gap_ema7",
    )

    if not symbols:
        tickers = exchange.fetch_tickers()
        state = {"_tickers": tickers, "_symbols": list(tickers.keys())}
        ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=lambda *_: None, config=cfg)
        engine.on_start(ctx)
        symbols = engine.build_universe(ctx)
        if not symbols:
            _log_summary("[BACKTEST] No symbols from build_universe().", log_path)
            return
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]

    tf_minutes = float(exchange.parse_timeframe(tfs["trigger"])) / 60.0
    total = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "pending_cancel": 0,
        "pending_cancel_timeout": 0,
        "pending_cancel_prior_hh": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }
    for symbol in symbols:
        stats = run_symbol(
            symbol,
            tfs,
            args.days,
            exchange,
            engine,
            cfg,
            out_path,
            out_trades,
            out_cancels,
            log_path,
            args.sl_pct,
            args.tp_pct,
            tf_minutes,
            args.verbose,
        )
        if stats:
            for k in total:
                val = stats.get(k, 0)
                if isinstance(total[k], float):
                    total[k] += float(val or 0.0)
                else:
                    total[k] += int(val or 0)
    winrate = (total["wins"] / total["trades"] * 100.0) if total["trades"] > 0 else 0.0
    avg_mfe = (total["mfe_sum"] / total["trades"]) if total["trades"] > 0 else 0.0
    avg_mae = (total["mae_sum"] / total["trades"]) if total["trades"] > 0 else 0.0
    avg_hold = (total["hold_sum"] / total["trades"]) if total["trades"] > 0 else 0.0
    _log_summary(
        f"[BACKTEST] TOTAL trades={total['trades']} wins={total['wins']} losses={total['losses']} "
        f"winrate={winrate:.2f}% tp={total['tp']} sl={total['sl']} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}",
        log_path,
    )


if __name__ == "__main__":
    main()
