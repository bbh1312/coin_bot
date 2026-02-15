from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class ScoutOnlyExhaustionShortConfig:
    tf_ltf: str = "3m"

    pump_rise_3bars_min: float = 0.06
    pump_rise_1bar_min: float = 0.02
    pump_mfi_min: float = 99.0
    pump_vol_mult_min: float = 3.0
    pump_bb_excess_mult: float = 0.60
    pump_optional_min_score: int = 1
    pump_use_score_mode: bool = True
    vol_sma_len: int = 20
    bb_len: int = 20
    bb_std: float = 2.0
    watch_bars: int = 120

    ema_fast_len: int = 7
    ema_slow_len: int = 20
    rsi_len: int = 14

    scout_size: float = 0.12
    scout_tp_atr_mult: float = 0.45
    scout_tp_dynamic: bool = False
    scout_tp_dynamic_step: float = 0.15
    scout_tp_dynamic_max: float = 1.0
    scout_loss_cap_pct: float = 0.011


class ScoutOnlyExhaustionShortEngine(BaseEngine):
    name = "scout_only_exhaustion_short"

    def __init__(self, config: ScoutOnlyExhaustionShortConfig | None = None) -> None:
        self.config = config or ScoutOnlyExhaustionShortConfig()
