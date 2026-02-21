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
    auto_relax: bool = False
    atr_mult: float = 1.0
    delta_len: int = 2
    cluster_atr: float = 1.5
    max_zones_per_side: int = 8
    touch_mode: str = "bot"
    touch_use_close: bool = False
    dvf_norm_max: float = -0.10
    dvf_norm_immediate: float = -0.25
    dvf_norm_diff_th: float = -0.03
    dvf_slope_enabled: bool = False
    require_reject_close: bool = False
    reject_mode: str = "bot"
    reject_source: str = "1h"
    reject_wick_min: float = 0.45
    reject_vol_mult: float = 1.2
    reject_vol_enabled: bool = False
    reject_dvf_extra_th: float = -0.15
    reject_lh_lookback: int = 8
    mtf_bos_lookback: int = 5
    mtf_swing_lookback: int = 8
    ema200_filter: bool = True
    ema_filter_len: int = 200
    btc_filter_enabled: bool = False
    btc_filter_tf: str = "1h"
    btc_filter_ema_len: int = 200
    btc_filter_slope_len: int = 4
    btc_bull_dvf_only: bool = True
    total_window_days: int = 14
    rolling_zones: bool = False
    retest_bars: int = 6
    retest_atr_mult: float = 0.60
    retest_above_atr_mult: float = 0.25
    retest_timeout_bars: int = 6
    retest_near_atr_mult: float = 0.15
    retest_wick_max: float = 0.35
    retest_dyn: bool = False
    retest_dyn_th: float = 0.6
    retest_dyn_bars: int = 10
    shallow_atr_mult: float = 0.25
    shallow_wick_max: float = 0.35
    shallow_dvf_max: float = -0.05
    big_bear_body_mult: float = 1.4
    big_bear_strong_only: bool = True
    disable_immediate_big_bear: bool = False
    disable_immediate_dvf_accel: bool = True
    disable_immediate_dvf_slope: bool = True
    big_bear_near_bonus_atr: float = 0.05
    dvf_accel_retest_bars: int = 4
    use_weak_break: bool = False
    retest_entry_enabled: bool = False
    atr_filter_len: int = 20
    atr_filter_mult: float = 0.7
    ema60_15m_len: int = 60
    ema120_15m_len: int = 120
    ema_slope_min: float = 0.0
    sl_buffer: float = 0.009
    sl_atr_mult: float = 0.4
    sl_cap_pct: float = 0.01
    sl_cap_atr_mult: float = 0.0
    tp_atr_mult: float = 0.0
    tp_atr_mult_weak: float = 0.0
    tp_mult: float = 0.99
    tp_mult_weak: float = 0.99
    dvf_confirm_bars: int = 1


class SrProShortV1Engine(BaseEngine):
    name = "sr_pro_short_v1"

    def __init__(self, config: SrProShortV1Config | None = None) -> None:
        self.config = config or SrProShortV1Config()
