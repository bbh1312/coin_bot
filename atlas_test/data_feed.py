import time
from typing import List, Dict, Any

import ccxt

from env_loader import load_env


def init_exchange() -> ccxt.Exchange:
    load_env()
    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    return exchange


def fetch_ohlcv(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    limit: int,
    drop_last: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch OHLCV and return list of dicts.
    If drop_last=True, drop the last (potentially incomplete) candle.
    """
    candles = None
    backoff = 1.0
    for _ in range(3):
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            break
        except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
            print(f"[atlas-test] fetch_ohlcv retry {symbol} {timeframe}: {e}")
            wait_with_backoff(backoff)
            backoff = min(backoff * 2, 8.0)
        except Exception as e:
            print(f"[atlas-test] fetch_ohlcv failed {symbol} {timeframe}: {e}")
            break
    if not candles:
        return []
    if not candles:
        return []
    if drop_last and len(candles) > 1:
        candles = candles[:-1]
    out = []
    for c in candles:
        try:
            out.append(
                {
                    "ts": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
            )
        except Exception:
            continue
    return out


def wait_with_backoff(secs: float) -> None:
    time.sleep(secs)
