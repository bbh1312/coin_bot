from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class WashShortSuiteConfig:
    tf_trend: str = "1h"
    tf_main: str = "15m"
    tf_exec: str = "1m"
    ema_fast: int = 20
    ema_mid: int = 60
    ema_slow: int = 120
    adx_len: int = 14
    adx_min: float = 16.0
    swing_lookback: int = 50
    fib_eps: float = 0.002
    pullback_eps: float = 0.0025
    accel_close_pos_min: float = 0.55
    accel_body_ratio_min: float = 0.35
    accel_vol_ratio_min: float = 1.0
    rsi_len: int = 14
    rsi_min: float = 25.0
    rsi_max: float = 55.0
    rsi_lower_high_delta: float = 0.3
    vol_sma_len: int = 20
    vol_pullback_max: float = 0.8
    vol_reversal_min: float = 0.4
    btc_ema_guard_len: int = 10
    btc_ema_fast: int = 7
    btc_ema_slow: int = 20
    entry_block_high_mult: float = 1.01
    entry_block_close_pos: float = 0.7
    entry_block_vol_ratio: float = 1.5
    sl_fixed_pct: float = 0.005
    sl_atr_mult: float = 1.5
    tp_atr_mult: float = 2.0
    tp_pct: float = 0.09
    sl_pct: float = 0.04
    time_stop_minutes: int = 300
    cooldown_bars: int = 18


class WashShortSuiteEngine(BaseEngine):
    name = "wash_short_suite"

    def __init__(self, config: WashShortSuiteConfig | None = None) -> None:
        self.config = config or WashShortSuiteConfig()
