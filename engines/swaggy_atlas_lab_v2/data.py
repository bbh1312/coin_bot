from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import ccxt
import pandas as pd


def parse_ts(text: str) -> int:
    if text.isdigit():
        return int(text)
    t = text.strip().replace("T", " ")
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
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


def to_df(rows: List[list]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def slice_df(df: pd.DataFrame, ts_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["ts"] <= ts_ms]


def build_universe_from_tickers(
    tickers: Dict[str, dict],
    top_n: int,
    anchor_symbols: List[str],
) -> List[str]:
    vols = []
    for sym, t in tickers.items():
        try:
            qv = float(t.get("quoteVolume") or 0.0)
        except Exception:
            qv = 0.0
        vols.append((sym, qv))
    vols.sort(key=lambda x: x[1], reverse=True)
    top = [s for s, _ in vols[:top_n]]
    for a in anchor_symbols:
        if a and a not in top:
            top.append(a)
    return top


def save_universe(path: str, symbols: List[str], meta: Dict[str, str]) -> None:
    ensure_dir(os.path.dirname(path))
    payload = {"symbols": symbols, "meta": meta}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
