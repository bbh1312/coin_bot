from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Signal:
    symbol: str
    side: str  # LONG | SHORT
    pattern: str  # REJECT | RECLAIM | TRAVERSE
    entry_price: float
    sl: float
    targets: List[float]
    meta: Dict[str, Any]


@dataclass
class EngineContext:
    exchange: Any
    state: Dict[str, Any]
    now_ts: float
    logger: Any
    config: Any


class BaseEngine:
    name: str = "base"

    def on_start(self, ctx: EngineContext) -> None:
        return None

    def build_universe(self, ctx: EngineContext) -> List[str]:
        return []

    def on_tick(self, ctx: EngineContext, symbol: str) -> Optional[Signal]:
        return None

    def on_position_update(self, ctx: EngineContext, symbol: str, pos_state: Dict[str, Any]) -> None:
        return None
