from __future__ import annotations

from dataclasses import dataclass, field
from typing import Set


@dataclass
class AtlasRsFailShortConfig:
    ltf_tf: str = "15m"
    htf_tf: str = "1h"
    ltf_limit: int = 160
    htf_limit: int = 120

    # Atlas gate
    min_score: float = 50.0
    rsz_enter: float = -1.5
    rsz_slow_max: float = -0.5
    allow_regimes: Set[str] = field(default_factory=lambda: {"bull", "neutral"})
    beta_abs_max: float = 4.0
    vol_min: float = 0.3

    # HTF gate
    htf_ema_fast: int = 20
    htf_ema_slow: int = 60
    htf_allow_fast_le_slow: bool = True

    # LTF trigger
    ltf_ema_fast: int = 20
    ltf_ema_slow: int = 60
    wick_ratio_min: float = 0.55
    body_ratio_min: float = 0.5
    pullback_swing_lookback: int = 20
    pullback_anchor_eps: float = 0.001
    pullback_break_eps: float = 0.001
    pullback_timeout_bars: int = 6

    # cooldown
    cooldown_minutes: int = 45
