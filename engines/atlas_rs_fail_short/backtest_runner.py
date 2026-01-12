"""
Atlas RS Fail Short backtest runner.
"""
import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import ccxt

import cycle_cache
from atlas_test.atlas_engine import atlas_swaggy_cfg
from engines.base import EngineContext
from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig
from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine


def parse_args():
    parser = argparse.ArgumentParser(description="Atlas RS Fail Short Backtest Runner")
    parser.add_argument("--symbols", type=str, default="BTC/USDT:USDT,ETH/USDT:USDT")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--ltf", type=str, default="15m")
    parser.add_argument("--htf", type=str, default="1h")
    parser.add_argument("--sl-pct", type=float, default=0.03)
    parser.add_argument("--tp-pct", type=float, default=0.03)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--out-trades", type=str, default="")
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


def _csv_header(path: str, header: str) -> None:
    if not path:
        return
    if os.path.exists(path):
        return
    _append_line(path, header)


def run_symbol(
    symbol: str,
    days: int,
    exchange: ccxt.Exchange,
    engine: AtlasRsFailShortEngine,
    cfg: AtlasRsFailShortConfig,
    out_path: str,
    out_trades: str,
    sl_pct: float,
    tp_pct: float,
) -> Dict[str, float]:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    ref_symbol = getattr(atlas_swaggy_cfg, "ref_symbol", "BTC/USDT:USDT")
    print(f"[BACKTEST] Fetching {symbol} LTF={cfg.ltf_tf} HTF={cfg.htf_tf} from last {days} days")
    ltf_raw = fetch_ohlcv_all(exchange, symbol, cfg.ltf_tf, since_ms, end_ms)
    htf_raw = fetch_ohlcv_all(exchange, symbol, cfg.htf_tf, since_ms, end_ms)
    ref_raw = fetch_ohlcv_all(exchange, ref_symbol, cfg.ltf_tf, since_ms, end_ms)
    if not ltf_raw or not htf_raw or not ref_raw:
        print(f"[BACKTEST] Missing data for {symbol}. Skipping.")
        return {}

    state: Dict[str, dict] = {"_universe": [symbol]}
    ctx = EngineContext(exchange=exchange, state=state, now_ts=time.time(), logger=None, config=cfg)
    idx_htf = -1
    idx_ref = -1
    trade = None
    stats = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "tp": 0,
        "sl": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }
    block_counts: Dict[str, int] = {}
    eval_count = 0
    for idx in range(len(ltf_raw) - 1):
        ts = int(ltf_raw[idx][0])
        while (idx_htf + 1) < len(htf_raw) and int(htf_raw[idx_htf + 1][0]) <= ts:
            idx_htf += 1
        while (idx_ref + 1) < len(ref_raw) and int(ref_raw[idx_ref + 1][0]) <= ts:
            idx_ref += 1

        cycle_cache.clear_cycle_cache(keep_raw=True)
        cycle_cache.set_raw(symbol, cfg.ltf_tf, _slice_to_idx(ltf_raw, idx))
        cycle_cache.set_raw(symbol, cfg.htf_tf, _slice_to_idx(htf_raw, idx_htf))
        cycle_cache.set_raw(ref_symbol, cfg.ltf_tf, _slice_to_idx(ref_raw, idx_ref))

        state.pop("_atlas_swaggy_gate", None)
        ctx.now_ts = ts / 1000.0

        o = float(ltf_raw[idx][1])
        h = float(ltf_raw[idx][2])
        l = float(ltf_raw[idx][3])
        c = float(ltf_raw[idx][4])

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
                stats["hold_sum"] += holding_bars
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
                        ]
                    ),
                )
                trade = None
                st = state.get(symbol, {})
                if isinstance(st, dict):
                    st["in_pos"] = False
                    state[symbol] = st

        if trade is not None:
            continue

        sig = engine.on_tick(ctx, symbol)
        if not sig:
            continue
        eval_count += 1
        meta = sig.meta or {}
        if sig.entry_price is None:
            reason = meta.get("block_reason")
            if reason:
                block_counts[reason] = block_counts.get(reason, 0) + 1
            continue
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        atlas = meta.get("atlas") if isinstance(meta.get("atlas"), dict) else {}
        tech = meta.get("tech") if isinstance(meta.get("tech"), dict) else {}
        _append_line(
            out_path,
            ",".join(
                [
                    dt,
                    symbol,
                    f"{sig.entry_price:.6g}",
                    f"{meta.get('size_mult') or ''}",
                    str(atlas.get("score") or ""),
                    str(atlas.get("rs_z") or ""),
                    str(atlas.get("rs_z_slow") or ""),
                    str(tech.get("confirm_type") or ""),
                    str(tech.get("wick_ratio") or ""),
                ]
            ),
        )
        trade = {
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
        }
        st = state.get(symbol, {})
        if isinstance(st, dict):
            st["in_pos"] = True
            state[symbol] = st

    winrate = (stats["wins"] / stats["trades"] * 100.0) if stats["trades"] > 0 else 0.0
    avg_mfe = (stats["mfe_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    avg_mae = (stats["mae_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    avg_hold = (stats["hold_sum"] / stats["trades"]) if stats["trades"] > 0 else 0.0
    print(
        f"[BACKTEST] {symbol} trades={stats['trades']} wins={stats['wins']} losses={stats['losses']} "
        f"winrate={winrate:.2f}% tp={stats['tp']} sl={stats['sl']} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}"
    )
    if eval_count:
        for reason, count in sorted(block_counts.items(), key=lambda x: x[1], reverse=True):
            ratio = count / eval_count * 100.0
            print(f"[BACKTEST] {symbol} block={reason} count={count} ratio={ratio:.2f}%")
    return stats


def main():
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    exchange.load_markets()

    cfg = AtlasRsFailShortConfig()
    cfg.ltf_tf = args.ltf
    cfg.htf_tf = args.htf
    engine = AtlasRsFailShortEngine(cfg)

    out_path = args.out
    out_trades = args.out_trades
    if out_path and not out_trades:
        base, ext = os.path.splitext(out_path)
        if not ext:
            ext = ".csv"
        out_trades = f"{base}_trades{ext}"
    if out_path:
        _csv_header(
            out_path,
            "ts,symbol,entry_px,size_mult,score,rs_z,rs_z_slow,confirm_type,wick_ratio",
        )
    _csv_header(
        out_trades,
        "entry_ts,exit_ts,symbol,entry_px,exit_px,exit_reason,pnl_pct,mfe,mae,hold_bars,confirm_type,size_mult,rs_z",
    )

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
    for symbol in symbols:
        stats = run_symbol(
            symbol,
            args.days,
            exchange,
            engine,
            cfg,
            out_path,
            out_trades,
            args.sl_pct,
            args.tp_pct,
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
    print(
        f"[BACKTEST] total trades={total['trades']} wins={total['wins']} losses={total['losses']} "
        f"winrate={winrate:.2f}% tp={total['tp']} sl={total['sl']} "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f}"
    )


if __name__ == "__main__":
    main()
