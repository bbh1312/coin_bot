from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def build_universe_from_tickers(
    tickers: Dict[str, dict],
    symbols: Optional[Iterable[str]] = None,
    min_quote_volume_usdt: float = 30_000_000.0,
    top_n: Optional[int] = 50,
    pos_top_n: Optional[int] = None,
    abs_top_n: Optional[int] = None,
    anchors: Sequence[str] = ("BTC/USDT:USDT", "ETH/USDT:USDT"),
    excluded_bases: Optional[Sequence[str]] = None,
) -> List[str]:
    if not isinstance(tickers, dict) or not tickers:
        return list(dict.fromkeys(anchors))
    if symbols is None:
        symbols = tickers.keys()
    excluded = {str(x).strip().upper() for x in (excluded_bases or []) if str(x).strip()}
    candidates_pct: List[Tuple[str, float]] = []
    candidates_abs: List[Tuple[str, float]] = []
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
        if pct > 0:
            candidates_pct.append((sym, pct))
        candidates_abs.append((sym, abs(pct)))
    candidates_pct.sort(key=lambda x: x[1], reverse=True)
    candidates_abs.sort(key=lambda x: x[1], reverse=True)

    use_mix = bool(
        isinstance(pos_top_n, int)
        and isinstance(abs_top_n, int)
        and pos_top_n > 0
        and abs_top_n > 0
    )
    result: List[str] = []
    if use_mix:
        for sym, _ in candidates_pct:
            if sym in result:
                continue
            result.append(sym)
            if len(result) >= int(pos_top_n):
                break
        added_non_anchor = len(result)
        target_non_anchor = int(pos_top_n) + int(abs_top_n)
        for sym, _ in candidates_abs:
            if sym in result:
                continue
            result.append(sym)
            added_non_anchor += 1
            if added_non_anchor >= target_non_anchor:
                break
    else:
        for sym in anchors:
            if sym not in result:
                result.append(sym)
        for sym, _ in candidates_pct:
            if sym in result:
                continue
            result.append(sym)
            if top_n and len(result) >= top_n:
                break

    if top_n and len(result) > top_n:
        result = result[: int(top_n)]
    return result
