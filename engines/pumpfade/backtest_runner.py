"""
PumpFade backtest runner: fetches historical OHLCV and emits ENTRY_READY signals.
"""
import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import ccxt

import cycle_cache
from engines.base import EngineContext
from engines.pumpfade.engine import PumpFadeEngine
from engines.pumpfade.config import PumpFadeConfig


def parse_args():
    parser = argparse.ArgumentParser(description="PumpFade Backtest Runner")
    parser.add_argument(
        "--symbols",
        type=str,
        default="BTC/USDT:USDT,ETH/USDT:USDT",
    )
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--tf_trigger", type=str, default="15m")
    parser.add_argument("--tf_confirm", type=str, default="5m")
    parser.add_argument("--tf_vol", type=str, default="1h")
    parser.add_argument("--use_5m_confirm", action="store_true")
    parser.add_argument("--sl-pct", type=float, default=0.03)
    parser.add_argument("--tp-pct", type=float, default=0.03)
    parser.add_argument("--out", type=str, default="")
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


def run_symbol(
    symbol: str,
    tfs: dict,
    days: int,
    exchange: ccxt.Exchange,
    engine: PumpFadeEngine,
    cfg: PumpFadeConfig,
    out_path: str,
    sl_pct: float,
    tp_pct: float,
) -> None:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    print(f"[BACKTEST] Fetching {symbol} {tfs} from last {days} days")
    tf15_raw = fetch_ohlcv_all(exchange, symbol, tfs["trigger"], since_ms, end_ms)
    tf5_raw = fetch_ohlcv_all(exchange, symbol, tfs["confirm"], since_ms, end_ms)
    tf1h_raw = fetch_ohlcv_all(exchange, symbol, tfs["vol"], since_ms, end_ms)
    if not tf15_raw:
        print(f"[BACKTEST] No 15m data for {symbol}. Skipping.")
        return

    state = {"_universe": [symbol]}
    ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=print, config=cfg)

    idx_15 = -1
    idx_5 = -1
    idx_1h = -1
    hits = 0
    pending = None
    trade = None
    stats = {"trades": 0, "wins": 0, "losses": 0, "tp": 0, "sl": 0, "pending_cancel": 0}
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
                log_exit = (
                    "PUMPFADE_TRADE_EXIT "
                    f"sym={symbol} reason={exit_reason} entry_px={trade['entry_px']:.6g} "
                    f"exit_px={exit_px:.6g} pnl_pct={pnl_pct:.4f}"
                )
                print(log_exit)
                trade = None

        if pending is not None and trade is None:
            if h >= pending["entry_px"]:
                entry_px = pending["entry_px"]
                trade = {
                    "entry_px": entry_px,
                    "entry_ts": ts,
                    "sl_px": entry_px * (1 + sl_pct),
                    "tp_px": entry_px * (1 - tp_pct),
                }
                print(
                    "PUMPFADE_TRADE_ENTRY "
                    f"sym={symbol} entry_px={entry_px:.6g} sl={trade['sl_px']:.6g} tp={trade['tp_px']:.6g}"
                )
                pending = None
            elif idx_15 > pending["expire_idx"]:
                stats["pending_cancel"] += 1
                pending = None

        if trade is not None or pending is not None:
            continue

        sig = engine.on_tick(ctx, symbol)
        if not sig:
            continue
        meta = sig.meta or {}
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"[ENTRY] {symbol} {dt}Z price={sig.entry_price:.6g} reasons={','.join(meta.get('reasons') or [])}")
        line = ",".join(
            [
                dt,
                symbol,
                f"{sig.entry_price:.6g}",
                f"{meta.get('hh_n') or ''}",
                f"{meta.get('failure_high') or ''}",
                f"{meta.get('failure_low') or ''}",
                f"{meta.get('confirm_close') or ''}",
                f"{meta.get('confirm_type') or ''}",
                f"{meta.get('vol_failure') or ''}",
                f"{meta.get('vol_peak') or ''}",
                f"{meta.get('rsi') or ''}",
                "Y" if meta.get("rsi_turn") else "N",
                "Y" if meta.get("macd_hist_increasing") else "N",
                ";".join(meta.get("reasons") or []),
            ]
        )
        _append_line(out_path, line)
        hits += 1
        if hits % 50 == 0:
            print(f"[BACKTEST] {symbol} entries={hits}")
        pending = {
            "entry_px": sig.entry_price,
            "signal_idx": idx_15,
            "expire_idx": idx_15 + max(1, int(cfg.entry_timeout_bars)),
        }

    winrate = (stats["wins"] / stats["trades"] * 100.0) if stats["trades"] > 0 else 0.0
    print(
        f"[BACKTEST] {symbol} trades={stats['trades']} wins={stats['wins']} losses={stats['losses']} "
        f"winrate={winrate:.2f}% tp={stats['tp']} sl={stats['sl']} pending_cancel={stats['pending_cancel']}"
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
    engine = PumpFadeEngine(cfg)

    out_path = args.out
    if out_path:
        _append_line(
            out_path,
            "ts,symbol,entry,hh,failure_high,failure_low,confirm_close,confirm_type,vol_failure,vol_peak,rsi,rsi_turn,macd_increasing,reasons",
        )

    total = {"trades": 0, "wins": 0, "losses": 0, "tp": 0, "sl": 0, "pending_cancel": 0}
    for symbol in symbols:
        stats = run_symbol(symbol, tfs, args.days, exchange, engine, cfg, out_path, args.sl_pct, args.tp_pct)
        if stats:
            for k in total:
                total[k] += int(stats.get(k, 0))
    winrate = (total["wins"] / total["trades"] * 100.0) if total["trades"] > 0 else 0.0
    print(
        f"[BACKTEST] total trades={total['trades']} wins={total['wins']} losses={total['losses']} "
        f"winrate={winrate:.2f}% tp={total['tp']} sl={total['sl']} pending_cancel={total['pending_cancel']}"
    )


if __name__ == "__main__":
    main()
