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
    auto_relax: bool = True
    atr_mult: float = 1.0
    delta_len: int = 2
    cluster_atr: float = 1.5
    max_zones_per_side: int = 8
    touch_mode: str = "top"
    touch_use_close: bool = False
    dvf_norm_min: float = 0.1
    require_reject_close: bool = False
    reject_mode: str = "top"
    reject_source: str = "1h"
    ema200_filter: bool = True
    ema_filter_len: int = 200
    touch_recent_bars: int = 2
    touch_react_require: bool = True
    zone_height_atr_min: float = 0.3
    zone_height_atr_max: float = 2.0
    total_window_days: int = 14
    rolling_zones: bool = True
    retest_bars: int = 6
    retest_atr_mult: float = 0.4
    retest_near_atr_mult: float = 0.15
    retest_wick_max: float = 0.35
    retest_sweep_atr_mult: float = 0.05
    retest_close_atr_tol: float = 0.05
    # aggressive break (V3)
    aggr_break_enabled: bool = True
    aggr_body_ratio_min: float = 0.6
    aggr_close_atr_min: float = 0.1
    aggr_entry_cap_atr: float = 0.3
    aggr_fast_fail_atr: float = 0.15
    aggr_tp_atr_mult: float = 1.5
    aggr_sl_atr_mult: float = 1.0
    retest_dyn: bool = False
    retest_dyn_th: float = 0.6
    retest_dyn_bars: int = 10
    shallow_atr_mult: float = 0.35
    shallow_wick_max: float = 0.35
    shallow_dvf_min: float = 0.1
    shallow_reclaim_atr_mult: float = 0.05
    entry_ema_len: int = 7
    entry_atr_offset: float = 0.01
    entry_target_allow_atr: float = 0.12
    entry_max_atr_over_retest: float = 0.60
    sl_buffer: float = 0.01
    sl_atr_mult: float = 1.0
    tp_atr_mult: float = 1.0
    tp_atr_mult_weak: float = 1.0
    tp_mult: float = 1.02
    tp_mult_weak: float = 1.02
    # 15m 강화
    hl_min_count: int = 2
    hl_require_ema20: bool = True
    overext_k_atr_15m: float = 2.0
    # 3m break 강화
    break_pivot_left: int = 2
    break_pivot_right: int = 2
    break_fallback_lookback: int = 5
    break_body_ratio_min: float = 0.50
    break_close_atr_min: float = 0.05


class SrProLongV1Engine(BaseEngine):
    name = "sr_pro_long_v1"

    def __init__(self, config: SrProLongV1Config | None = None) -> None:
        self.config = config or SrProLongV1Config()
