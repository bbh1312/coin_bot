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
    auto_relax: bool = True
    atr_mult: float = 1.0
    delta_len: int = 2
    cluster_atr: float = 1.5
    max_zones_per_side: int = 8
    sl_buffer: float = 0.006
    tp_mult: float = 0.976


class SrProShortV1Engine(BaseEngine):
    name = "sr_pro_short_v1"

    def __init__(self, config: SrProShortV1Config | None = None) -> None:
        self.config = config or SrProShortV1Config()
