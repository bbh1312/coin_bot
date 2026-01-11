from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CandidateState:
    p1_idx: int
    p2_idx: int
    high1: float
    high2: float
    rsi1: float
    rsi2: float
    hh_pct: float
    rsi_down: float
    score: float
    confirm_window_end: int
    rsi_ob_ok: bool
    spike_block: bool
    ema_reject_found: bool = False
    confirm_ok: bool = False
    confirm_fail: Optional[List[str]] = None
    confirm_logged: bool = False
    trigger_logged: bool = False
    regime_block_logged: bool = False
    macd_block_logged: bool = False


@dataclass
class SymbolState:
    last_signal_idx: int = -10**9
    last_p2_idx: int = -10**9
    cooldown_until: int = -10**9
    active: Optional[CandidateState] = None
