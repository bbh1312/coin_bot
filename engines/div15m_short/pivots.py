from typing import List, Optional, Tuple


def _is_pivot_high(highs: List[float], idx: int, left: int, right: int) -> bool:
    if idx - left < 0 or idx + right >= len(highs):
        return False
    cur = highs[idx]
    if cur <= max(highs[idx - left : idx]):
        return False
    if cur < max(highs[idx + 1 : idx + right + 1]):
        return False
    return True


def find_recent_pivot_highs(
    highs: List[float],
    idx: int,
    left_right: int,
    lookback: int,
    min_gap: int,
) -> Tuple[Optional[int], Optional[int], int]:
    """
    Returns (p1_idx, p2_idx, pivot_count) for the most recent two pivot highs
    confirmed by available data up to idx.
    """
    if idx <= left_right:
        return None, None, 0
    start = max(0, idx - lookback)
    last_confirmable = idx - left_right
    pivots: List[int] = []
    for i in range(start + left_right, last_confirmable + 1):
        if _is_pivot_high(highs, i, left_right, left_right):
            pivots.append(i)
    if len(pivots) < 2:
        return None, None, len(pivots)
    p2 = pivots[-1]
    p1 = None
    for i in range(len(pivots) - 2, -1, -1):
        if p2 - pivots[i] >= min_gap:
            p1 = pivots[i]
            break
    if p1 is None:
        return None, None, len(pivots)
    return p1, p2, len(pivots)
