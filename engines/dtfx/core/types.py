from dataclasses import dataclass
from typing import Dict, Any
from enum import Enum # Added

class DTFXState(str, Enum): # Added
    IDLE = "IDLE"
    SWEEP_SEEN = "SWEEP_SEEN"
    MSS_SOFT = "MSS_SOFT"
    MSS_CONFIRMED = "MSS_CONFIRMED"
    ZONE_SET = "ZONE_SET"
    ENTRY_READY = "ENTRY_READY"
    COOLDOWN = "COOLDOWN"

@dataclass
class Candle:
    ts: int  # Timestamp
    o: float # Open
    h: float # High
    l: float # Low
    c: float # Close
    v: float # Volume

@dataclass
class Swing:
    ts: int
    price: float
    type: str # 'high' or 'low'

@dataclass
class Signal:
    ts: int
    engine: str
    symbol: str
    tf_ltf: str
    tf_mtf: str
    tf_htf: str
    event: str
    state: str
    context: Dict[str, Any]
