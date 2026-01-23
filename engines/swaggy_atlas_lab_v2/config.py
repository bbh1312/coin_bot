from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SwaggyConfig:
    tf_htf: str = "1h"
    tf_htf2: str = "4h"
    tf_mtf: str = "15m"
    tf_ltf: str = "5m"
    tf_d1: str = "1d"
    vp_lookback_1h: int = 240
    ltf_limit: int = 400
    level_cluster_pct: float = 0.0022
    touch_eps: float = 0.0015
    touch_pct_min: float = 0.0002
    touch_pct_min_bull: float = 0.0010
    touch_pct_min_bear: float = 0.0012
    touch_pct_min_range: float = 0.0016
    touch_pct_min_chaos: float = 0.0016
    touch_atr_mult: float = 0.15
    touch_atr_len: int = 14
    touch_eps_atr_mult: float = 0.05
    touch_eps_pct: float = 0.0005
    max_dist: float = 0.005
    sweep_eps: float = 0.0010
    hold_eps: float = 0.0010
    wick_ratio: float = 0.45
    vol_mult: float = 1.2
    invalid_eps: float = 0.002
    expansion_atr_mult: float = 1.5
    cooldown_min: int = 20
    allow_countertrend: bool = False
    range_short_allowed: bool = False
    tick_size: float = 0.001
    regime_lookback: int = 30
    regime_fractal_k: int = 2
    bin_size_pct: float = 0.1
    bin_size_abs: float = 0.0
    value_area_pct: float = 0.70
    min_zone_width_pct: float = 0.15
    hvn_threshold_ratio: float = 0.55
    lvn_threshold_ratio: float = 0.18
    lvn_ratio: float = 0.35
    min_strength_sweep: float = 0.55
    min_strength_reclaim: float = 0.40
    min_strength_rejection: float = 0.50
    min_strength_retest: float = 0.45
    weak_cut: float = 0.32
    entry_min: float = 0.40
    entry_min_bull: float = 0.40
    entry_min_range: float = 0.45
    entry_min_bear: float = 0.40
    short_bull_min_strength: float = 0.55
    short_bull_wick_ratio: float = 0.70
    short_bull_vol_mult: float = 1.3
    max_dist_reclaim: float = 0.0025
    max_dist_retest: float = 0.0020
    short_max_dist_reclaim: float = 0.0025
    short_max_dist_retest: float = 0.0020
    short_max_dist_rejection: float = 0.0025
    short_max_dist_sweep: float = 0.0015
    overext_ema_len: int = 20
    overext_atr_mult: float = 1.5
    d1_ema_len: int = 7
    d1_atr_len: int = 14
    d1_overext_atr_mult: float = 1.2
    swing_lookback_1h: int = 60
    swing_lookback_mtf: int = 32
    level_recent_secs: int = 14400
    level_max_dist_pct: float = 0.06
    level_cap: int = 25
    skip_beta_mid: bool = True
    skip_overext_mid: bool = True
    skip_confirm_body: bool = False
    use_trigger_min: bool = False


@dataclass
class AtlasConfig:
    ref_symbol: str = "BTC/USDT:USDT"
    rs_n: int = 12
    rs_n_slow: int = 36
    corr_m: int = 32
    corr_m_slow: int = 96
    rs_pass: float = 0.0060
    rs_strong: float = 0.0120
    rs_z_pass: float = 0.75
    rs_z_strong: float = 1.25
    indep_corr: float = 0.55
    indep_beta: float = 0.80
    indep_strong_corr: float = 0.35
    indep_strong_beta: float = 0.60
    indep_risk_corr: float = 0.75
    indep_risk_beta: float = 1.10
    vol_pass: float = 1.3
    vol_override: float = 1.5
    exception_min_score: int = 3
    trend_norm_bull: float = 0.35
    trend_norm_bear: float = -0.35
    trend_norm_flat: float = 0.20
    chaos_atr_low: float = 0.85
    chaos_atr_high: float = 1.20
    chaos_trigger_strength_min: float = 0.60
    chaos_vol_ratio_min: float = 1.2
    override_max_dist_pct: float = 0.009


@dataclass
class BacktestConfig:
    tp_pct: float = 0.02
    sl_pct: float = 0.02
    fee_rate: float = 0.0
    slippage_pct: float = 0.0
    base_usdt: float = 100.0
    timeout_bars: int = 0
    mode: str = "all"
    universe: str = "top50"
    anchor_symbols: str = "BTC,ETH"
