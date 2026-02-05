from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class BullPullbackLongConfig:
    tf_trend: str = "1h"
    tf_main: str = "15m"
    tf_exec: str = "1m"

    ema_trend_fast: int = 20
    ema_trend_mid: int = 60
    ema_trend_slow: int = 120
    ema_trend_slope_len: int = 3

    ema_exec_fast: int = 10
    ema_exec_mid: int = 20

    rsi_len: int = 14
    rsi_aggr_min: float = 50.0
    rsi_aggr_max: float = 60.0
    rsi_deep_min: float = 30.0
    rsi_deep_max: float = 40.0

    bb_len: int = 20
    bb_std: float = 2.0

    fib_lookback: int = 96
    fib_norm_low: float = 0.382
    fib_norm_high: float = 0.5
    fib_deep: float = 0.618
    fib_eps: float = 0.003

    vol_sma_len: int = 20
    vol_pullback_max: float = 0.8

    exec_wick_min: float = 0.35
    exec_ema_touch_eps: float = 0.001

    pullback_gain_min: float = 0.006

    atr_len: int = 14
    sl_atr_mult: float = 1.0
    tp_fallback_pct: float = 0.03

    cooldown_bars: int = 20

    btc_filter: bool = True
    btc_symbol: str = "BTC/USDT:USDT"
    btc_rsi_len: int = 14
    btc_rsi_min: float = 45.0
    btc_drop_1m_pct: float = -0.5
    btc_atr_len: int = 14
    btc_atr_lookback: int = 288
    btc_atr_mult_max: float = 1.5


class BullPullbackLongEngine(BaseEngine):
    name = "bull_pullback_long_v1"

    def __init__(self, config: BullPullbackLongConfig | None = None) -> None:
        self.config = config or BullPullbackLongConfig()
