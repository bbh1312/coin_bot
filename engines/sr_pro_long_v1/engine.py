from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class SrProLongV1Config:
    tf_ltf: str = "3m"
    tf_mtf: str = "15m"
    tf_htf: str = "1h"
    lookback: int = 20
    relaxed_lookback: int = 10
    auto_relax: bool = False
    atr_mult: float = 1.0
    delta_len: int = 2
    cluster_atr: float = 1.5
    max_zones_per_side: int = 8
    touch_mode: str = "top"
    touch_use_close: bool = False
    zone_accept_mode: str = "off"
    dvf_norm_min: float = 0.08
    require_reject_close: bool = False
    reject_mode: str = "top"
    reject_source: str = "1h"
    ema200_filter: bool = True
    ema_filter_len: int = 200
    total_window_days: int = 14
    rolling_zones: bool = True
    retest_bars: int = 6
    retest_atr_mult: float = 0.20
    retest_near_atr_mult: float = 0.15
    retest_wick_max: float = 0.4
    retest_reclaim_min_atr: float = 0.05
    retest_dyn: bool = False
    retest_dyn_th: float = 0.6
    retest_dyn_bars: int = 10
    pullback_lookback: int = 36
    pullback_min_pct: float = 0.0
    pullback_min_atr: float = 0.0
    require_sweep_reclaim: bool = False
    sweep_lookback: int = 12
    sweep_tol_atr: float = 0.05
    max_break_ext_atr: float = 3.0
    shallow_atr_mult: float = 0.35
    shallow_wick_max: float = 0.35
    shallow_dvf_min: float = 0.0
    entry_ema_len: int = 7
    entry_atr_offset: float = 0.15
    entry_atr_offset_weak: float = 0.15
    entry_candle_guard: bool = False
    retest_breakdown_block_atr: float = 0.0
    sl_buffer: float = 0.00
    sl_atr_mult: float = 0.04
    sl_cap_pct: float = 0.018
    sl_cap_atr_mult: float = 0.0
    tp_atr_mult: float = 0.0
    tp_atr_mult_weak: float = 0.0
    tp_mult: float = 1.020
    tp_mult_weak: float = 1.022


class SrProLongV1Engine(BaseEngine):
    name = "sr_pro_long_v1"

    def __init__(self, config: SrProLongV1Config | None = None) -> None:
        self.config = config or SrProLongV1Config()
