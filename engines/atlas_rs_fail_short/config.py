from __future__ import annotations

from dataclasses import dataclass, field
from typing import Set


@dataclass
class AtlasRsFailShortConfig:
    ltf_tf: str = "15m"
    ltf_limit: int = 200

    # Atlas gate
    min_score: float = 50.0
    allow_regimes: Set[str] = field(default_factory=lambda: {"neutral", "bear", "range", "bull_extreme"})
    bull_extreme_score_max: float = 80.0
    dir_score_max: float = 60.0

    # LTF indicators
    ema_len: int = 20
    atr_len: int = 14
    rsi_len: int = 14
    bb_len: int = 20
    bb_mult: float = 2.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    vol_sma_len: int = 20

    # Location / setup
    fade_atr_mult: float = 0.35
    fade_atr_min_mult: float = 0.7
    fade_atr_max_mult: float = 1.3
    bb_upper_eps: float = 0.1
    bb_touch_required: bool = False

    # RSI filter
    rsi_min: float = 45.0
    rsi_max: float = 60.0
    rsi_block: float = 70.0

    # Trigger
    wick_ratio_min: float = 0.55
    trigger_min: int = 2
    trigger_exact: bool = True

    # Noise filter
    atr_min_ratio: float = 0.002
    min_quote_volume: float = 0.0

    # cooldown
    cooldown_minutes: int = 45
