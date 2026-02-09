#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import ccxt
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import calc_warmup_window, load_common_universe
from engines.top_fail_short_v1.engine import TopFailShortV1Config


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _ohlcv_cache_path(root_dir: str, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe = _sanitize_symbol(symbol)
    return os.path.join(
        root_dir,
        "logs",
        "top_fail_short_v1",
        "ohlcv_cache",
        f"{safe}_{timeframe}_{start_ms}_{end_ms}.csv",
    )


def _write_ohlcv_cache(path: str, rows: List[list]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df.to_csv(path, index=False)


def _read_ohlcv_cache(path: str) -> List[list]:
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty:
            return []
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return []


def _parse_ts_arg(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone(timedelta(hours=9)))
            return int(dt.astimezone(timezone.utc).timestamp() * 1000)
        except Exception:
            continue
    return None


def _fetch_ohlcv_rest(exchange: ccxt.Exchange, symbol: str, timeframe: str, start_ms: int, end_ms: int, limit: int = 1500) -> List[list]:
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
    p = argparse.ArgumentParser("prefetch top_fail_short_v1 ohlcv cache")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--start", type=str, default="")
    p.add_argument("--end", type=str, default="")
    p.add_argument("--sleep-ms", type=int, default=250)
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--retry-sleep-ms", type=int, default=2000)
    args = p.parse_args()

    cfg = TopFailShortV1Config()
    ltf_tf = cfg.tf_ltf
    mtf_tf = cfg.tf_mtf
    htf_tf = cfg.tf_htf

    end_ms = _parse_ts_arg(args.end) or int(datetime.now(timezone.utc).timestamp() * 1000)
    start_arg_ms = _parse_ts_arg(args.start)
    min_ltf = max(cfg.swing_lookback + 5, cfg.vol_sma_len + 5, cfg.atr_len + 5, cfg.ema_len + 5, 80)
    min_mtf = max(cfg.ema_len + 5, 60)
    min_htf = max(cfg.stall_high_lookback + 5, 180)
    if start_arg_ms is None:
        start_ms, eval_start_ms, _, _ = calc_warmup_window(
            args.days,
            end_ms,
            {ltf_tf: min_ltf, mtf_tf: min_mtf, htf_tf: min_htf},
        )
    else:
        eval_start_ms = start_arg_ms
        start_ms = start_arg_ms

    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    universe = load_common_universe((args.universe or "").strip().lower(), exchange, cache_only=False)
    if args.max_symbols and args.max_symbols > 0:
        universe = universe[: args.max_symbols]

    total = len(universe)
    if total == 0:
        print("[prefetch] no universe symbols")
        return

    tfs = [ltf_tf, mtf_tf, htf_tf]
    for idx, sym in enumerate(universe, start=1):
        for tf in tfs:
            cache_path = _ohlcv_cache_path(ROOT_DIR, sym, tf, start_ms, end_ms)
            cached = _read_ohlcv_cache(cache_path)
            if cached:
                continue
            ok = False
            try:
                rows = _fetch_ohlcv_rest(exchange, sym, tf, start_ms, end_ms, limit=1500)
                if rows:
                    _write_ohlcv_cache(cache_path, rows)
                    ok = True
            except Exception as e:
                msg = str(e)
                print(f"[prefetch] {idx}/{total} {sym} tf={tf} err={msg}")
                time.sleep(max(args.retry_sleep_ms, 1000) / 1000.0)
            if ok:
                print(f"[prefetch] {idx}/{total} {sym} tf={tf} ok")
            else:
                print(f"[prefetch] {idx}/{total} {sym} tf={tf} empty")
            if args.sleep_ms and args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

    print(f"[prefetch] done symbols={total} start_ms={start_ms} end_ms={end_ms} eval_start_ms={eval_start_ms}")


if __name__ == "__main__":
    main()
