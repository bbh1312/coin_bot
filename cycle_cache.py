from typing import Dict, Tuple, Any, Optional, Callable
import os
import time

import pandas as pd

RAW_OHLCV: Dict[Tuple[str, str], dict] = {}
DF_CACHE: Dict[Tuple[str, str, int], pd.DataFrame] = {}
IND_CACHE: Dict[Tuple[str, str, Tuple[Any, ...]], Any] = {}
FETCHER: Optional[Callable[[str, str, int], Optional[list]]] = None
DISK_CACHE_DIR = os.getenv("COMMON_OHLCV_CACHE_DIR", os.path.join("logs", "common_ohlcv_cache"))
DISK_CACHE_ENABLED = os.getenv("COMMON_OHLCV_CACHE_ENABLED", "1") not in ("0", "false", "off", "no")


def _safe_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _disk_path(symbol: str, tf: str, limit: int) -> str:
    fname = f"{_safe_symbol(symbol)}_{tf}_{limit}.csv"
    return os.path.join(DISK_CACHE_DIR, fname)


def _read_disk_cache(symbol: str, tf: str, limit: int) -> Optional[list]:
    if not DISK_CACHE_ENABLED:
        return None
    path = _disk_path(symbol, tf, limit)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return None


def _write_disk_cache(symbol: str, tf: str, limit: int, data: list) -> None:
    if not DISK_CACHE_ENABLED:
        return
    if not data:
        return
    path = _disk_path(symbol, tf, limit)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
        df.to_csv(path, index=False)
    except Exception:
        return


def clear_cycle_cache(keep_raw: bool = False) -> None:
    if not keep_raw:
        RAW_OHLCV.clear()
    DF_CACHE.clear()
    IND_CACHE.clear()


def drop_raw_by_tf(tfs) -> None:
    if not tfs:
        return
    drop = set(tfs)
    for key in list(RAW_OHLCV.keys()):
        if key[1] in drop:
            RAW_OHLCV.pop(key, None)


def set_raw(symbol: str, tf: str, data: list) -> None:
    RAW_OHLCV[(symbol, tf)] = {"ts": time.time(), "data": data}
    _write_disk_cache(symbol, tf, len(data), data)
    # Raw data updated; drop derived caches for this symbol/tf so next get_df/get_ind recompute.
    for key in list(DF_CACHE.keys()):
        if key[0] == symbol and key[1] == tf:
            DF_CACHE.pop(key, None)
    for key in list(IND_CACHE.keys()):
        if key[0] == symbol and key[1] == tf:
            IND_CACHE.pop(key, None)


def get_raw(symbol: str, tf: str) -> Optional[list]:
    item = RAW_OHLCV.get((symbol, tf))
    if not item:
        return None
    return item.get("data")


def is_fresh(symbol: str, tf: str, ttl_sec: int) -> bool:
    item = RAW_OHLCV.get((symbol, tf))
    if not item:
        return False
    ts = item.get("ts")
    if not isinstance(ts, (int, float)):
        return False
    return (time.time() - ts) <= ttl_sec


def set_fetcher(fetcher: Optional[Callable[[str, str, int], Optional[list]]]) -> None:
    global FETCHER
    FETCHER = fetcher


def get_df(symbol: str, tf: str, limit: int, force: bool = False) -> pd.DataFrame:
    key = (symbol, tf, limit)
    if not force:
        cached = DF_CACHE.get(key)
        if cached is not None:
            return cached
    raw = get_raw(symbol, tf)
    if force or not raw:
        disk = _read_disk_cache(symbol, tf, limit)
        if disk:
            raw = disk
            set_raw(symbol, tf, raw)
        if FETCHER:
            raw = FETCHER(symbol, tf, limit)
            if raw:
                set_raw(symbol, tf, raw)
    if not raw:
        return pd.DataFrame()
    sliced = raw[-limit:] if len(raw) >= limit else raw
    df = pd.DataFrame(sliced, columns=["ts", "open", "high", "low", "close", "volume"])
    DF_CACHE[key] = df
    return df


def get_ind(symbol: str, tf: str, ind_key: Tuple[Any, ...], compute_fn: Callable[[], Any]) -> Any:
    key = (symbol, tf, ind_key)
    cached = IND_CACHE.get(key)
    if cached is not None:
        return cached
    val = compute_fn()
    IND_CACHE[key] = val
    return val
