from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ManageResult:
    action: str
    info: Dict[str, float]


def manage_position(
    pos_state: Dict[str, float],
    last_price: float,
    bar_index: int,
    time_stop_bars: int,
) -> Optional[ManageResult]:
    if not pos_state or not pos_state.get("in_pos"):
        return None
    side = pos_state.get("side")
    if isinstance(side, str):
        side = side.upper()
    entry_px = float(pos_state.get("entry_px") or 0.0)
    tp1 = pos_state.get("tp1_px")
    tp2 = pos_state.get("tp2_px")
    entry_bar = int(pos_state.get("entry_bar") or bar_index)
    tp1_hit = bool(pos_state.get("tp1_hit"))
    if entry_px <= 0:
        return None
    if tp1 and not tp1_hit:
        if (side == "LONG" and last_price >= tp1) or (side == "SHORT" and last_price <= tp1):
            pos_state["tp1_hit"] = True
            pos_state["sl_px"] = entry_px
            return ManageResult("TP1_HIT", {"tp1": float(tp1)})
    if tp2:
        if (side == "LONG" and last_price >= tp2) or (side == "SHORT" and last_price <= tp2):
            pos_state["in_pos"] = False
            return ManageResult("TP2_EXIT", {"tp2": float(tp2)})
    if (bar_index - entry_bar) >= time_stop_bars:
        pos_state["in_pos"] = False
        return ManageResult("TIME_STOP", {"bars": float(bar_index - entry_bar)})
    return None
