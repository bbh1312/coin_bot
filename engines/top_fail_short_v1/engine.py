from dataclasses import dataclass


@dataclass
class TopFailShortV1Config:
    tf_ltf: str = "3m"
    tf_mtf: str = "15m"
    tf_htf: str = "1h"
    universe_24h_change: float = 20.0
    universe_7d_mult: float = 3.0
    min_quote_vol_24h: float = 0.0
    stall_high_lookback: int = 6
    stall_wick_min: float = 0.5
    stall_min_count: int = 2
    mtf_require_weak_close: bool = False
    ema_len: int = 20
    swing_lookback: int = 30
    atr_len: int = 14
    wash_atr_mult: float = 1.3
    vol_spike_mult: float = 1.8
    vol_sma_len: int = 20
    retest_ema_tol: float = 0.1
    limit_entry: bool = True
    limit_offset_atr: float = 0.15
    retest_max_depth_atr: float = 1.15
    retest_wait_next_high: bool = False
    fail_wick_max: float = 0.30
    fail_require_ema: bool = False
    stop_atr_mult: float = 0.35
    min_hold_bars: int = 3
    tp_min_pct: float = 0.015
    tp_r_mult: float = 0.9
    max_wait_bars: int = 60
    cooldown_bars: int = 20
