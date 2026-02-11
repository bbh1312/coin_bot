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
    dvf_norm_max: float = -0.10
    dvf_norm_immediate: float = -0.20
    dvf_norm_diff_th: float = -0.03
    require_reject_close: bool = False
    reject_mode: str = "bot"
    reject_source: str = "1h"
    ema200_filter: bool = True
    ema_filter_len: int = 200
    total_window_days: int = 14
    rolling_zones: bool = True
    retest_bars: int = 4
    retest_atr_mult: float = 0.2
    retest_above_atr_mult: float = 0.1
    retest_timeout_bars: int = 2
    retest_near_atr_mult: float = 0.15
    retest_wick_max: float = 0.35
    retest_dyn: bool = False
    retest_dyn_th: float = 0.6
    retest_dyn_bars: int = 10
    shallow_atr_mult: float = 0.25
    shallow_wick_max: float = 0.35
    shallow_dvf_max: float = 0.0
    big_bear_body_mult: float = 1.2
    atr_filter_len: int = 20
    atr_filter_mult: float = 0.7
    ema60_15m_len: int = 60
    ema120_15m_len: int = 120
    ema_slope_min: float = 0.001
    sl_buffer: float = 0.01
    sl_atr_mult: float = 0.5
    tp_atr_mult: float = 0.0
    tp_atr_mult_weak: float = 0.0
    tp_mult: float = 0.99
    tp_mult_weak: float = 0.99


class SrProShortV1Engine(BaseEngine):
    name = "sr_pro_short_v1"

    def __init__(self, config: SrProShortV1Config | None = None) -> None:
        self.config = config or SrProShortV1Config()
