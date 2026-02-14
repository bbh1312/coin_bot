from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class RangeEdge3MConfig:
    tf: str = "3m"
    lookback: int = 80
    adx_len: int = 14
    adx_threshold: float = 42.0
    bb_len: int = 20
    bb_std: float = 2.0
    bb_rel_mult: float = 1.8
    atr_len: int = 14
    atr_filter_mult: float = 1.6
    zone_tolerance: float = 0.008
    sfp_tolerance: float = 0.0015
    max_daily_sl: int = 100
    edge_min_score: int = 1
    allow_shorts: bool = True
    sl_atr_mult: float = 0.8
    rr_min: float = 30.0
    sl_min_pct: float = 0.015
    tp_min_pct: float = 0.020


class RangeEdge3MEngine(BaseEngine):
    name = "range_edge_3m"

    def __init__(self, config: RangeEdge3MConfig | None = None) -> None:
        self.config = config or RangeEdge3MConfig()
