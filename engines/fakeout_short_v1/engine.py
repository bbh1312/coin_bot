from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class FakeoutShortV1Config:
    tf_scan: str = "15m"
    tf_main: str = "15m"
    tf_exec: str = "3m"

    # scan filters (15m)
    change_24h_min_pct: float = 10.0
    trend_rsi_len: int = 14
    trend_rsi_min: float = 60.0

    # 15m indicators
    rsi_len: int = 14
    ema20_len: int = 20
    ema120_len: int = 120
    obv_ema_len: int = 20
    vol_sma_len: int = 20

    # HOD / session
    hod_fail_min: int = 6
    near_hod_band: float = 0.0015
    touch_band: float = 0.0015
    touch_break: float = 0.0003
    touch_max: int = 4

    # BLOCK params
    ext_ema7_max: float = 0.007
    ema120_floor: float = 1.0
    vol_expand_min: float = 1.1

    # retest / entry
    retest_band: float = 0.0025
    retest_break: float = 0.0008
    break_low_lookback: int = 5

    # 3m indicators
    ema7_len: int = 7
    ema20_len_3m: int = 20
    atr_len: int = 14

    # risk / TP
    sl_atr_mult: float = 0.3
    tp_r_mult: float = 1.5


class FakeoutShortV1Engine(BaseEngine):
    name = "fakeout_short_v1"

    def __init__(self, config: FakeoutShortV1Config | None = None) -> None:
        self.config = config or FakeoutShortV1Config()
