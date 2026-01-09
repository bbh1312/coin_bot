from typing import Tuple


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def score_divergence(
    low1: float,
    low2: float,
    rsi1: float,
    rsi2: float,
    target_ll: float,
    target_rsi: float,
) -> Tuple[float, float, float]:
    if low1 <= 0 or target_ll <= 0 or target_rsi <= 0:
        return 0.0, 0.0, 0.0
    price_ll_strength = _clamp(((low1 - low2) / low1) / target_ll)
    rsi_hl_strength = _clamp((rsi2 - rsi1) / target_rsi)
    score = 0.5 * price_ll_strength + 0.5 * rsi_hl_strength
    return score, price_ll_strength, rsi_hl_strength
