#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import List

import ccxt

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import load_common_universe


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _write_common_warmup(out_dir: str, symbol: str, tf: str, rows: List[list]) -> None:
    if not rows:
        return
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{_sanitize_symbol(symbol)}_{tf}.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("ts,open,high,low,close,volume\n")
        for row in rows:
            if not row or len(row) < 6:
                continue
            f.write(
                f"{int(row[0])},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}\n"
            )


def _fetch_ohlcv_range(exchange: ccxt.Exchange, symbol: str, tf: str, start_ms: int, end_ms: int, limit: int) -> List[list]:
    out: List[list] = []
    since = start_ms
    tf_ms = int(exchange.parse_timeframe(tf) * 1000)
    last_ts = None
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, tf, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if ts > end_ms:
                continue
            if last_ts is None or ts > last_ts:
                out.append(row)
                last_ts = ts
        new_last = int(batch[-1][0])
        if last_ts is None or new_last == last_ts:
            since = new_last + tf_ms
        else:
            since = last_ts + tf_ms
        if last_ts is not None and last_ts >= end_ms:
            break
    return out


def main() -> None:
    p = argparse.ArgumentParser("refresh common_warmup any timeframe")
    p.add_argument("--tf", type=str, default="1h")
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--use-rest-universe", action="store_true")
    p.add_argument("--sleep-ms", type=int, default=150)
    p.add_argument("--limit", type=int, default=1000)
    args = p.parse_args()

    out_dir = args.common_warmup_dir or os.path.join(ROOT_DIR, "logs", "common_warmup", "ohlcv")
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=int(args.days))).timestamp() * 1000)

    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )

    universe = load_common_universe(
        (args.universe or "").strip().lower(),
        exchange if args.use_rest_universe else None,
        cache_only=not args.use_rest_universe,
    )
    if not universe:
        print("[refresh] no universe symbols (check latest.txt or use --use-rest-universe)")
        return

    total = len(universe)
    ok = 0
    for idx, sym in enumerate(universe, start=1):
        try:
            rows = _fetch_ohlcv_range(exchange, sym, args.tf, start_ms, end_ms, args.limit)
        except Exception as e:
            print(f"[refresh] {idx}/{total} {sym} fetch_fail err={e}")
            continue
        if rows:
            _write_common_warmup(out_dir, sym, args.tf, rows)
            ok += 1
            print(f"[refresh] {idx}/{total} {sym} ok bars={len(rows)} tf={args.tf}")
        else:
            print(f"[refresh] {idx}/{total} {sym} empty tf={args.tf}")
        if args.sleep_ms and args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)
    print(f"[refresh] done ok={ok} total={total} tf={args.tf} days={args.days} out={out_dir}")


if __name__ == "__main__":
    main()
