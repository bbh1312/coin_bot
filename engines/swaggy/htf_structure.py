from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class RegimeState:
    regime: str
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]
    regime_score: float = 0.0
    range_reason: str = ""


def _swing_points(df: pd.DataFrame, lookback: int, k: int) -> tuple[list[float], list[float]]:
    if df.empty or len(df) < (k * 2 + 1):
        return [], []
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    start = max(k, len(df) - lookback - k - 1)
    swing_highs = []
    swing_lows = []
    for i in range(start, len(df) - k):
        left_high = max(highs[i - k:i])
        right_high = max(highs[i + 1:i + k + 1])
        left_low = min(lows[i - k:i])
        right_low = min(lows[i + 1:i + k + 1])
        if highs[i] > left_high and highs[i] > right_high:
            swing_highs.append(highs[i])
        if lows[i] < left_low and lows[i] < right_low:
            swing_lows.append(lows[i])
    return swing_highs, swing_lows


def detect_regime(df: pd.DataFrame, lookback: int = 30, k: int = 2) -> RegimeState:
    swing_highs, swing_lows = _swing_points(df, lookback, k)
    last_high = swing_highs[-1] if swing_highs else None
    last_low = swing_lows[-1] if swing_lows else None
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        hh = swing_highs[-1] > swing_highs[-2]
        hl = swing_lows[-1] > swing_lows[-2]
        lh = swing_highs[-1] < swing_highs[-2]
        ll = swing_lows[-1] < swing_lows[-2]
        if hh and hl:
            return RegimeState("bull", last_high, last_low, regime_score=1.0, range_reason="")
        if lh and ll:
            return RegimeState("bear", last_high, last_low, regime_score=1.0, range_reason="")
        return RegimeState("range", last_high, last_low, regime_score=0.0, range_reason="no_trend")
    return RegimeState("range", last_high, last_low, regime_score=0.0, range_reason="insufficient_swings")
