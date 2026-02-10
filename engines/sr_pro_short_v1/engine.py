from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class SrProShortV1Config:
    tf_ltf: str = "3m"
    tf_mtf: str = "15m"
    tf_htf: str = "1h"
    lookback: int = 20
    relaxed_lookback: int = 10
    auto_relax: bool = True
    atr_mult: float = 1.0
    delta_len: int = 2
    cluster_atr: float = 1.5
    max_zones_per_side: int = 8
    touch_mode: str = "bot"
    touch_use_close: bool = False
    dvf_norm_max: float = 0.0
    require_reject_close: bool = False
    reject_mode: str = "bot"
    reject_source: str = "1h"
    ema200_filter: bool = True
    total_window_days: int = 10
    rolling_zones: bool = True
    retest_bars: int = 6
    retest_atr_mult: float = 0.25
    retest_near_atr_mult: float = 0.15
    retest_wick_max: float = 0.4
    retest_dyn: bool = False
    retest_dyn_th: float = 0.6
    retest_dyn_bars: int = 10
    shallow_atr_mult: float = 0.35
    shallow_wick_max: float = 0.35
    shallow_dvf_max: float = 0.0
    sl_buffer: float = 0.01
    tp_mult: float = 0.985
    tp_mult_weak: float = 0.985
    sl_min_weak: float = 1.0015


class SrProShortV1Engine(BaseEngine):
    name = "sr_pro_short_v1"

    def __init__(self, config: SrProShortV1Config | None = None) -> None:
        self.config = config or SrProShortV1Config()
