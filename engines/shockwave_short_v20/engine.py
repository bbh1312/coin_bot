from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class ShockwaveShortV20Config:
    tf: str = "3m"
    bb_len: int = 20
    bb_std: float = 2.5
    rsi_len: int = 14
    rsi_hot: float = 70.0
    vol_ma_len: int = 20
    vol_min_mult: float = 0.8
    wick_ratio: float = 0.35
    sl_buffer: float = 0.002
    tp_mult: float = 0.92


class ShockwaveShortV20Engine(BaseEngine):
    name = "shockwave_short_v20"

    def __init__(self, config: ShockwaveShortV20Config | None = None) -> None:
        self.config = config or ShockwaveShortV20Config()
