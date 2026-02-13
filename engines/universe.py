from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def build_universe_from_tickers(
    tickers: Dict[str, dict],
    symbols: Optional[Iterable[str]] = None,
    min_quote_volume_usdt: float = 30_000_000.0,
    top_n: Optional[int] = 50,
    anchors: Sequence[str] = ("BTC/USDT:USDT", "ETH/USDT:USDT"),
    excluded_bases: Optional[Sequence[str]] = None,
) -> List[str]:
    if not isinstance(tickers, dict) or not tickers:
        return list(dict.fromkeys(anchors))
    if symbols is None:
        symbols = tickers.keys()
    excluded = {str(x).strip().upper() for x in (excluded_bases or []) if str(x).strip()}
    candidates: List[Tuple[str, float]] = []
    for sym in symbols:
        t = tickers.get(sym)
        if not t:
            continue
        try:
            base = str(sym).strip().upper().split("/", 1)[0]
        except Exception:
            base = ""
        if base and base in excluded:
            continue
        pct = t.get("percentage")
        qv = t.get("quoteVolume")
        if pct is None or qv is None:
            continue
        try:
            pct = float(pct)
            qv = float(qv)
        except Exception:
            continue
        if qv < min_quote_volume_usdt:
            continue
        candidates.append((sym, abs(pct)))
    candidates.sort(key=lambda x: x[1], reverse=True)

    result: List[str] = []
    for sym in anchors:
        if sym not in result:
            result.append(sym)

    for sym, _ in candidates:
        if sym in result:
            continue
        result.append(sym)
        if top_n and len(result) >= top_n:
            break
    return result
