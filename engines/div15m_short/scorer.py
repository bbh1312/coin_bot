from typing import Tuple


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def score_divergence(
    high1: float,
    high2: float,
    rsi1: float,
    rsi2: float,
    target_hh: float,
    target_rsi: float,
) -> Tuple[float, float, float]:
    if high1 <= 0 or target_hh <= 0 or target_rsi <= 0:
        return 0.0, 0.0, 0.0
    price_hh_strength = _clamp(((high2 - high1) / high1) / target_hh)
    rsi_lh_strength = _clamp((rsi1 - rsi2) / target_rsi)
    score = 0.5 * price_hh_strength + 0.5 * rsi_lh_strength
    return score, price_hh_strength, rsi_lh_strength
