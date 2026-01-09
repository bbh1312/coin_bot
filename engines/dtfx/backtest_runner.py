"""
DTFX backtest runner: fetches historical OHLCV and runs engines on past candles.
"""
import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List

import ccxt

from engines.dtfx.core.types import Candle
from engines.dtfx.dtfx_long import DTFXLongEngine
from engines.dtfx.dtfx_short import DTFXShortEngine
from engines.dtfx.config import get_default_params


def parse_args():
    parser = argparse.ArgumentParser(description="DTFX Backtest Runner")
    parser.add_argument(
        "--symbols",
        type=str,
        default="BTC/USDT:USDT,ETH/USDT:USDT,RIVER/USDT:USDT,ASTER/USDT:USDT,PTB/USDT:USDT",
    )
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--tf_ltf", type=str, default="5m")
    parser.add_argument("--tf_mtf", type=str, default="15m")
    parser.add_argument("--tf_htf", type=str, default="1h")
    return parser.parse_args()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _since_ms(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


def _to_candles(ohlcv: List[list]) -> List[Candle]:
    return [
        Candle(ts=int(c[0]), o=float(c[1]), h=float(c[2]), l=float(c[3]), c=float(c[4]), v=float(c[5]))
        for c in ohlcv
    ]


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


def run_symbol(
    symbol: str,
    tfs: dict,
    days: int,
    exchange: ccxt.Exchange,
    params: dict,
    long_engine: DTFXLongEngine,
    short_engine: DTFXShortEngine,
) -> None:
    end_ms = _now_ms()
    since_ms = _since_ms(days)
    print(f"[BACKTEST] Fetching {symbol} {tfs} from last {days} days")
    ltf_raw = fetch_ohlcv_all(exchange, symbol, tfs["ltf"], since_ms, end_ms)
    mtf_raw = fetch_ohlcv_all(exchange, symbol, tfs["mtf"], since_ms, end_ms)
    htf_raw = fetch_ohlcv_all(exchange, symbol, tfs["htf"], since_ms, end_ms)
    if not ltf_raw:
        print(f"[BACKTEST] No LTF data for {symbol}. Skipping.")
        return
    ltf = _to_candles(ltf_raw)
    mtf = _to_candles(mtf_raw)
    htf = _to_candles(htf_raw)

    base = symbol.split("/")[0].split(":")[0].upper()
    role = "anchor" if base in params.get("anchor_symbols", set()) else "alt"
    long_engine.symbol_roles[symbol] = role
    short_engine.symbol_roles[symbol] = role

    mtf_idx = -1
    htf_idx = -1
    for i in range(len(ltf)):
        ts = ltf[i].ts
        while (mtf_idx + 1) < len(mtf) and mtf[mtf_idx + 1].ts <= ts:
            mtf_idx += 1
        while (htf_idx + 1) < len(htf) and htf[htf_idx + 1].ts <= ts:
            htf_idx += 1
        ltf_slice = ltf[: i + 1]
        mtf_slice = mtf[: mtf_idx + 1] if mtf_idx >= 0 else []
        htf_slice = htf[: htf_idx + 1] if htf_idx >= 0 else []
        long_engine.on_candle(symbol, ltf_slice, mtf_slice, htf_slice)
        short_engine.on_candle(symbol, ltf_slice, mtf_slice, htf_slice)
        if i % 200 == 0 and i > 0:
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"[BACKTEST] {symbol} progress {i}/{len(ltf)} at {dt}Z")


def main():
    os.environ["DTFX_LOG_PREFIX"] = "backtest"
    os.environ["DTFX_LOG_MODE"] = "backtest"
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
    long_engine = DTFXLongEngine(params, tfs, exchange)
    short_engine = DTFXShortEngine(params, tfs, exchange)

    for symbol in symbols:
        run_symbol(symbol, tfs, args.days, exchange, params, long_engine, short_engine)


if __name__ == "__main__":
    main()
