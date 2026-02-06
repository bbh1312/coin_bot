from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class ShortClimaxConfig:
    tf_exec: str = "1m"
    tf_mtf: str = "5m"
    tf_htf: str = "1h"
    ema_ref_tf: str = "15m"
    ema_ref_len: int = 120
    ema_ref_mult: float = 1.08
    vol_avg_days: int = 1
    vol_mult: float = 2.0
    runup_24h_min: float = 0.50
    runup_7d_min: float = 3.0
    runup_1h_min: float = 0.15
    sfp_lookback: int = 5
    vol_div_threshold: float = 0.7
    vol_spike_mult: float = 2.0
    vol_spike_ma: int = 20
    funding_limit: float = 0.001
    sl_multiplier: float = 1.007
    score_threshold: int = 50
    tp_mode: str = "trailing"  # trailing, pct, ema20, ema30, ema60, fib618
    tp_pct: float = 0.03
    tp1_pct: float = 0.02


class ShortClimaxEngine(BaseEngine):
    name = "short_climax_v1"

    def __init__(self, config: ShortClimaxConfig | None = None) -> None:
        self.config = config or ShortClimaxConfig()
