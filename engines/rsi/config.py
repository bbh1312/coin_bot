from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RsiConfig:
    thresholds: Dict[str, float] = field(
        default_factory=lambda: {"3m": 80.0, "5m": 80.0, "15m": 75.0, "1h": 75.0}
    )
    tf_check_order: List[str] = field(default_factory=lambda: ["1h", "15m", "5m", "3m"])
    min_quote_volume_usdt: float = 30_000_000.0
    universe_top_n: Optional[int] = 40
    anchors: List[str] = field(default_factory=lambda: ["BTC/USDT:USDT", "ETH/USDT:USDT"])
    vol_surge_lookback: int = 20
    vol_surge_mult: float = 1.1
    ema20_exit_buffer: float = 0.001
    rsi_len: int = 14
    rsi1h_ttl_sec: int = 300
    rsi_1h_limit: int = 30
    rsi_default_limit: int = 60
