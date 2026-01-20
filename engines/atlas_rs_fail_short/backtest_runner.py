"""
Atlas RS Fail Short backtest runner.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import ccxt
from ccxt.base.errors import DDoSProtection, NetworkError, RateLimitExceeded

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import cycle_cache
from atlas_test.atlas_engine import atlas_swaggy_cfg
from engines.base import EngineContext
from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig
from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine



def parse_args():
    parser = argparse.ArgumentParser(description="Atlas RS Fail Short Backtest Runner")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--ltf", type=str, default="15m")
    parser.add_argument("--reentry-minutes", type=int, default=120)
    parser.add_argument("--sl-pct", type=float, default=0.03)
    parser.add_argument("--tp-pct", type=float, default=0.03)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--out-trades", type=str, default="")
    parser.add_argument("--out-decisions", type=str, default="")
    parser.add_argument("--log-path", type=str, default="")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--sleep-ms", type=int, default=200)
    parser.add_argument("--retry", type=int, default=3)
    return parser.parse_args()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _since_ms(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


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


def _slice_to_idx(rows: List[list], idx: int) -> List[list]:
    if idx < 0:
        return []
    end = min(len(rows), idx + 1)
    return rows[:end]


def _append_line(path: str, line: str) -> None:
    if not path:
        return
    dir_path = os.path.dirname(path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _csv_header(path: str, header: str) -> None:
    if not path:
        return
    if os.path.exists(path):
        return
    _append_line(path, header)

def _fmt_tags(val: Optional[list]) -> str:
    if not val:
        return ""
    return "|".join(str(v) for v in val)


def _log_file(line: str, path: str) -> None:
    if path:
        _append_line(path, line)


def _log_summary(line: str, path: str) -> None:
    print(line)
    _log_file(line, path)


def run_backtest(
    symbols: List[str],
    days: int,
    exchange: ccxt.Exchange,
    engine: AtlasRsFailShortEngine,
    cfg: AtlasRsFailShortConfig,
    out_path: str,
    out_trades: str,
    out_decisions: str,
    sl_pct: float,
    tp_pct: float,
    reentry_minutes: int,
    log_path: str,
    sleep_ms: int,
    max_retries: int,
) -> Dict[str, Dict[str, float]]:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    ref_symbol = getattr(atlas_swaggy_cfg, "ref_symbol", "BTC/USDT:USDT")
    ref_raw = fetch_ohlcv_all(exchange, ref_symbol, cfg.ltf_tf, since_ms, end_ms, sleep_ms, max_retries)
    if not ref_raw:
        _log_file(f"[BACKTEST] Missing ref data for {ref_symbol}. Skipping.", log_path)
        return {}
    ltf_minutes = float(exchange.parse_timeframe(cfg.ltf_tf)) / 60.0

    state: Dict[str, dict] = {"_universe": list(symbols)}
    ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=None, config=cfg)
    symbol_data: Dict[str, List[list]] = {}
    block_counts: Dict[str, Dict[str, int]] = {}
    eval_counts: Dict[str, int] = {}
    stats_map: Dict[str, Dict[str, float]] = {}
    trade_logs: Dict[str, List[str]] = {}
    trades: Dict[str, Optional[dict]] = {}
    reentry_until: Dict[str, float] = {}
    idx_ref = -1

    for symbol in symbols:
        _log_file(
            f"[BACKTEST] Fetching {symbol} LTF={cfg.ltf_tf} from last {days} days",
            log_path,
        )
        ltf_raw = fetch_ohlcv_all(exchange, symbol, cfg.ltf_tf, since_ms, end_ms, sleep_ms, max_retries)
        if not ltf_raw:
            _log_file(f"[BACKTEST] Missing data for {symbol}. Skipping.", log_path)
            continue
        symbol_data[symbol] = ltf_raw
        stats_map[symbol] = {
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
        }
        trade_logs[symbol] = []
        block_counts[symbol] = {}
        eval_counts[symbol] = 0
        trades[symbol] = None
        reentry_until[symbol] = 0.0

    if not symbol_data:
        return stats_map

    events = []
    for symbol, rows in symbol_data.items():
        for idx in range(len(rows) - 1):
            ts = int(rows[idx][0])
            events.append((ts, symbol, idx))
    events.sort(key=lambda x: x[0])

    for ts, symbol, idx in events:
        rows = symbol_data[symbol]
        while (idx_ref + 1) < len(ref_raw) and int(ref_raw[idx_ref + 1][0]) <= ts:
            idx_ref += 1
        if idx_ref < 0:
            continue

        cycle_cache.clear_cycle_cache(keep_raw=True)
        cycle_cache.set_raw(symbol, cfg.ltf_tf, _slice_to_idx(rows, idx))
        cycle_cache.set_raw(ref_symbol, cfg.ltf_tf, _slice_to_idx(ref_raw, idx_ref))
        state.pop("_atlas_swaggy_gate", None)
        ctx.now_ts = ts / 1000.0

        h = float(rows[idx][2])
        l = float(rows[idx][3])
        c = float(rows[idx][4])

        trade = trades.get(symbol)
        if trade is not None:
            entry_px = trade["entry_px"]
            if entry_px > 0:
                mfe = (entry_px - l) / entry_px
                mae = (h - entry_px) / entry_px
                trade["mfe"] = max(trade["mfe"], mfe)
                trade["mae"] = max(trade["mae"], mae)
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
                trade_logs[symbol].append(
                    "[BACKTEST][EXIT] symbol=%s entry_px=%.6g exit_px=%.6g reason=%s"
                    % (symbol, trade["entry_px"], exit_px, exit_reason)
                )
                stats = stats_map[symbol]
                stats["exits"] += 1
                stats["trades"] += 1
                if pnl_pct >= 0:
                    stats["wins"] += 1
                else:
                    stats["losses"] += 1
                if exit_reason == "TP":
                    stats["tp"] += 1
                else:
                    stats["sl"] += 1
                holding_bars = max(1, idx - trade["entry_idx"] + 1)
                stats["mfe_sum"] += trade["mfe"]
                stats["mae_sum"] += trade["mae"]
                stats["hold_sum"] += holding_bars * ltf_minutes
                hold_minutes = holding_bars * ltf_minutes
                if log_path:
                    _log_file(
                        "[BACKTEST][EXIT] symbol=%s entry_dt=%s exit_dt=%s entry_px=%.6g exit_px=%.6g "
                        "pnl_pct=%.4f hold_min=%.1f reason=%s"
                        % (
                            symbol,
                            trade.get("entry_dt"),
                            datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            trade.get("entry_px") or 0.0,
                            exit_px,
                            pnl_pct,
                            hold_minutes,
                            exit_reason,
                        ),
                        log_path,
                    )
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
                            str(holding_bars),
                            trade.get("confirm_type") or "",
                            f"{trade.get('size_mult') or ''}",
                            f"{trade.get('rs_z') or ''}",
                            str(trade.get("pullback_id") or ""),
                            _fmt_tags(trade.get("risk_tags") if isinstance(trade.get("risk_tags"), list) else None),
                            str(trade.get("entry_ts") or ""),
                            "",
                            str(trade.get("rsi") or ""),
                            str(trade.get("atr") or ""),
                            str(trade.get("ema20") or ""),
                            str(trade.get("high_minus_ema20") or ""),
                            str(trade.get("trigger_bits") or ""),
                        ]
                    ),
                )
                trades[symbol] = None
                if reentry_minutes and reentry_minutes > 0:
                    reentry_until[symbol] = (ts / 1000.0) + (reentry_minutes * 60)
                st = state.get(symbol, {})
                if isinstance(st, dict):
                    st["in_pos"] = False
                    state[symbol] = st
            continue

        sig = engine.evaluate_symbol(ctx, symbol)
        if not sig:
            continue
        eval_counts[symbol] += 1
        meta = sig.meta or {}
        reason = meta.get("block_reason")
        if reason:
            bc = block_counts[symbol]
            bc[reason] = bc.get(reason, 0) + 1
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        atlas = meta.get("atlas") if isinstance(meta.get("atlas"), dict) else {}
        tech = meta.get("tech") if isinstance(meta.get("tech"), dict) else {}
        decision_row = [
            dt,
            symbol,
            str(atlas.get("dir") or ""),
            str(atlas.get("regime") or ""),
            "1" if sig.entry_ready else "0",
            f"{sig.entry_price:.6g}" if sig.entry_price is not None else "",
            str(reason or ""),
            str(meta.get("state_transition") or ""),
            str(meta.get("pullback_id") or ""),
            _fmt_tags(meta.get("risk_tags") if isinstance(meta.get("risk_tags"), list) else None),
            str(tech.get("rsi") or ""),
            str(tech.get("atr") or ""),
            str(tech.get("ema20") or ""),
            str(tech.get("high_minus_ema20") or ""),
            str(tech.get("trigger_bits") or ""),
        ]
        _append_line(out_decisions, ",".join(decision_row))
        if not sig.entry_ready or sig.entry_price is None:
            continue
        if reentry_minutes and reentry_minutes > 0:
            if (ts / 1000.0) < float(reentry_until.get(symbol) or 0.0):
                bc = block_counts[symbol]
                bc["reentry_cooldown"] = bc.get("reentry_cooldown", 0) + 1
                if log_path:
                    _log_file(f"[BACKTEST] SKIP reentry_cooldown symbol={symbol} ts={dt}", log_path)
                continue

        _append_line(
            out_path,
            ",".join(
                [
                    dt,
                    symbol,
                    str(atlas.get("dir") or ""),
                    str(atlas.get("regime") or ""),
                    f"{sig.entry_price:.6g}",
                    f"{meta.get('size_mult') or ''}",
                    str(atlas.get("score") or ""),
                    str(atlas.get("rs_z") or ""),
                    str(atlas.get("rs_z_slow") or ""),
                    str(tech.get("confirm_type") or ""),
                    str(tech.get("wick_ratio") or ""),
                    str(meta.get("pullback_id") or ""),
                    _fmt_tags(meta.get("risk_tags") if isinstance(meta.get("risk_tags"), list) else None),
                    str(meta.get("state_transition") or ""),
                    str(reason or ""),
                    str(tech.get("rsi") or ""),
                    str(tech.get("atr") or ""),
                    str(tech.get("ema20") or ""),
                    str(tech.get("high_minus_ema20") or ""),
                    str(tech.get("trigger_bits") or ""),
                ]
            ),
        )
        if log_path:
            _log_file(
                "[BACKTEST][ENTRY] symbol=%s entry_dt=%s entry_px=%.6g sl_px=%.6g tp_px=%.6g confirm=%s rsi=%s atr=%s trigger=%s"
                % (
                    symbol,
                    dt,
                    float(sig.entry_price or 0.0),
                    float(sig.entry_price or 0.0) * (1 + sl_pct),
                    float(sig.entry_price or 0.0) * (1 - tp_pct),
                    str(tech.get("confirm_type") or ""),
                    str(tech.get("rsi") or ""),
                    str(tech.get("atr") or ""),
                    str(tech.get("trigger_bits") or ""),
                ),
                log_path,
            )
        trades[symbol] = {
            "entry_px": sig.entry_price,
            "entry_ts": ts,
            "entry_dt": dt,
            "sl_px": sig.entry_price * (1 + sl_pct),
            "tp_px": sig.entry_price * (1 - tp_pct),
            "entry_idx": idx,
            "mfe": 0.0,
            "mae": 0.0,
            "confirm_type": tech.get("confirm_type") or "",
            "size_mult": meta.get("size_mult"),
            "rs_z": atlas.get("rs_z"),
            "pullback_id": meta.get("pullback_id"),
            "risk_tags": meta.get("risk_tags") if isinstance(meta.get("risk_tags"), list) else None,
            "rsi": tech.get("rsi"),
            "atr": tech.get("atr"),
            "ema20": tech.get("ema20"),
            "high_minus_ema20": tech.get("high_minus_ema20"),
            "trigger_bits": tech.get("trigger_bits"),
        }
        stats_map[symbol]["entries"] += 1
        trade_logs[symbol].append(
            "[BACKTEST][ENTRY] symbol=%s entry_px=%.6g" % (symbol, sig.entry_price)
        )
        st = state.get(symbol, {})
        if isinstance(st, dict):
            st["in_pos"] = True
            state[symbol] = st

    for symbol, stats in stats_map.items():
        trades_count = stats.get("trades", 0) or 0
        winrate = (stats["wins"] / trades_count * 100.0) if trades_count > 0 else 0.0
        avg_mfe = (stats["mfe_sum"] / trades_count) if trades_count > 0 else 0.0
        avg_mae = (stats["mae_sum"] / trades_count) if trades_count > 0 else 0.0
        avg_hold = (stats["hold_sum"] / trades_count) if trades_count > 0 else 0.0
        _log_summary(
            f"[BACKTEST] {symbol} trades={trades_count} wins={stats['wins']} losses={stats['losses']} "
            f"winrate={winrate:.2f}% tp={stats['tp']} sl={stats['sl']} "
            f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}",
            log_path,
        )
        for line in trade_logs.get(symbol, []):
            _log_summary(line, log_path)
        open_trade = trades.get(symbol)
        if isinstance(open_trade, dict):
            _log_summary(
                "[BACKTEST][OPEN] symbol=%s entry_px=%.6g entry_dt=%s"
                % (
                    symbol,
                    open_trade.get("entry_px") or 0.0,
                    open_trade.get("entry_dt") or "",
                ),
                log_path,
            )
        eval_count = eval_counts.get(symbol, 0)
        if eval_count:
            for reason, count in sorted(block_counts[symbol].items(), key=lambda x: x[1], reverse=True):
                ratio = count / eval_count * 100.0
                _log_file(f"[BACKTEST] {symbol} block={reason} count={count} ratio={ratio:.2f}%", log_path)
    return stats_map


def main():
    args = parse_args()
    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    exchange.load_markets()

    base_dir = os.path.join("logs", "atlas_rs_fail_short", "backtest")
    os.makedirs(base_dir, exist_ok=True)
    date_tag = time.strftime("%Y%m%d")
    if args.log_path:
        log_path = os.path.join(base_dir, os.path.basename(args.log_path))
    else:
        log_path = os.path.join(base_dir, f"backtest_{date_tag}.log")

    cfg = AtlasRsFailShortConfig()
    if args.ltf and args.ltf != cfg.ltf_tf:
        _log_summary(
            f"[BACKTEST] match-live enforced: ignoring --ltf={args.ltf}, using cfg.ltf_tf={cfg.ltf_tf}",
            log_path,
        )
    engine = AtlasRsFailShortEngine(cfg)
    tickers = exchange.fetch_tickers()
    state = {
        "_tickers": tickers,
        "_symbols": list(tickers.keys()),
    }
    ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=lambda *_: None, config=cfg)
    engine.on_start(ctx)
    symbols = engine.build_universe(ctx)
    if not symbols:
        _log_summary("[BACKTEST] No symbols from build_universe().", log_path)
        return
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    _log_summary(
        f"[BACKTEST] universe_count={len(symbols)} symbols={','.join(symbols)}",
        log_path,
    )

    def _normalize_out(path: str) -> str:
        if not path:
            return path
        return os.path.join(base_dir, os.path.basename(path))

    out_path = _normalize_out(args.out)
    out_trades = _normalize_out(args.out_trades)
    out_decisions = _normalize_out(args.out_decisions)
    if out_path and not out_trades:
        base, ext = os.path.splitext(out_path)
        if not ext:
            ext = ".csv"
        out_trades = f"{base}_trades{ext}"
    if out_path and not out_decisions:
        base, ext = os.path.splitext(out_path)
        if not ext:
            ext = ".csv"
        out_decisions = f"{base}_decisions{ext}"
    if out_path:
        _csv_header(
            out_path,
            "ts,symbol,atlas_dir,atlas_regime,entry_px,size_mult,score,rs_z,rs_z_slow,confirm_type,wick_ratio,pullback_id,risk_tags,state_transition,block_reason,rsi,atr,ema20,high_minus_ema20,trigger_bits",
        )
    _log_summary(
        f"[BACKTEST] match-live config ltf_tf={cfg.ltf_tf} ref_symbol={atlas_swaggy_cfg.ref_symbol}",
        log_path,
    )
    _csv_header(
        out_trades,
        "entry_ts,exit_ts,symbol,entry_px,exit_px,exit_reason,pnl_pct,mfe,mae,hold_bars,confirm_type,size_mult,rs_z,pullback_id,risk_tags,entry_ts_ms,skip_reason,rsi,atr,ema20,high_minus_ema20,trigger_bits",
    )
    _csv_header(
        out_decisions,
        "ts,symbol,atlas_dir,atlas_regime,entry_ready,entry_px,block_reason,state_transition,pullback_id,risk_tags,rsi,atr,ema20,high_minus_ema20,trigger_bits",
    )

    total = {
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
    }
    stats_map = run_backtest(
        symbols,
        args.days,
        exchange,
        engine,
        cfg,
        out_path,
        out_trades,
        out_decisions,
        args.sl_pct,
        args.tp_pct,
        args.reentry_minutes,
        log_path,
        args.sleep_ms,
        args.retry,
    )
    for stats in stats_map.values():
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
        f"[BACKTEST] total entries={total['entries']} exits={total['exits']} trades={total['trades']} "
        f"wins={total['wins']} losses={total['losses']} "
        f"winrate={winrate:.2f}% tp={total['tp']} sl={total['sl']} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}",
        log_path,
    )


if __name__ == "__main__":
    main()
