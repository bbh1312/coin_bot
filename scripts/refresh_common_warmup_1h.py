#!/usr/bin/env python3
import argparse
import os
import sys
import time
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


def _bars_for_1h(days: int, min_bars: int) -> int:
    bars = max(int(days) * 24, 1)
    if min_bars and bars < min_bars:
        bars = int(min_bars)
    return bars


def main() -> None:
    p = argparse.ArgumentParser("refresh common_warmup 1h")
    p.add_argument("--days", type=int, default=8)
    p.add_argument("--min-bars", type=int, default=180)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--use-rest-universe", action="store_true")
    p.add_argument("--sleep-ms", type=int, default=150)
    args = p.parse_args()

    out_dir = args.common_warmup_dir or os.path.join(
        ROOT_DIR, "logs", "common_warmup", "ohlcv"
    )
    tf = "1h"
    bars = _bars_for_1h(args.days, args.min_bars)

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
            rows = exchange.fetch_ohlcv(sym, tf, limit=bars)
        except Exception as e:
            print(f"[refresh] {idx}/{total} {sym} fetch_fail err={e}")
            continue
        if rows:
            _write_common_warmup(out_dir, sym, tf, rows)
            ok += 1
            print(f"[refresh] {idx}/{total} {sym} ok bars={len(rows)}")
        else:
            print(f"[refresh] {idx}/{total} {sym} empty")
        if args.sleep_ms and args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)
    print(f"[refresh] done ok={ok} total={total} tf={tf} bars={bars} out={out_dir}")


if __name__ == "__main__":
    main()
