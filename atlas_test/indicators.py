from typing import List, Tuple


def sma(values: List[float], n: int) -> List[float]:
    if n <= 0:
        return []
    out = [None] * (n - 1)
    for i in range(n - 1, len(values)):
        window = values[i - n + 1:i + 1]
        out.append(sum(window) / n)
    return out


def atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    n: int,
) -> List[float]:
    if len(highs) < n + 1 or len(lows) < n + 1 or len(closes) < n + 1:
        return []
    trs = []
    for i in range(len(highs)):
        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        trs.append(tr)
    out = [None] * (n - 1)
    for i in range(n - 1, len(trs)):
        out.append(sum(trs[i - n + 1:i + 1]) / n)
    return out


def supertrend(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 10,
    mult: float = 3.0,
) -> Tuple[List[str], List[float]]:
    """
    Return (trend_dir_list, supertrend_line_list).
    trend_dir_list values are "UP" or "DOWN".
    """
    n = len(highs)
    if n < period + 2:
        return [], []
    atr_vals = atr(highs, lows, closes, period)
    if not atr_vals:
        return [], []
    basic_upper = [None] * n
    basic_lower = [None] * n
    final_upper = [None] * n
    final_lower = [None] * n
    trend = [None] * n
    st_line = [None] * n
    for i in range(n):
        if atr_vals[i] is None:
            continue
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper[i] = hl2 + mult * atr_vals[i]
        basic_lower[i] = hl2 - mult * atr_vals[i]
        if i == 0 or final_upper[i - 1] is None or final_lower[i - 1] is None:
            final_upper[i] = basic_upper[i]
            final_lower[i] = basic_lower[i]
            trend[i] = "UP"
            st_line[i] = final_lower[i]
            continue
        if basic_upper[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]:
            final_upper[i] = basic_upper[i]
        else:
            final_upper[i] = final_upper[i - 1]
        if basic_lower[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]:
            final_lower[i] = basic_lower[i]
        else:
            final_lower[i] = final_lower[i - 1]
        if closes[i] > final_upper[i - 1]:
            trend[i] = "UP"
        elif closes[i] < final_lower[i - 1]:
            trend[i] = "DOWN"
        else:
            trend[i] = trend[i - 1] or "UP"
        st_line[i] = final_lower[i] if trend[i] == "UP" else final_upper[i]
    return trend, st_line
