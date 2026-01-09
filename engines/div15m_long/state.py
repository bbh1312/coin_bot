from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CandidateState:
    p1_idx: int
    p2_idx: int
    low1: float
    low2: float
    rsi1: float
    rsi2: float
    ll_pct: float
    rsi_up: float
    score: float
    confirm_window_end: int
    rsi_os_ok: bool
    spike_block: bool
    ema_reclaim_found: bool = False
    confirm_ok: bool = False
    confirm_fail: Optional[List[str]] = None
    confirm_logged: bool = False
    trigger_logged: bool = False
    regime_block_logged: bool = False


@dataclass
class SymbolState:
    last_signal_idx: int = -10**9
    last_p2_idx: int = -10**9
    cooldown_until: int = -10**9
    active: Optional[CandidateState] = None
