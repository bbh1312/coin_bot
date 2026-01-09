from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from engines.swaggy.levels import Level, next_levels


@dataclass
class RiskPlan:
    entry_px: float
    sl_px: float
    tp1_px: Optional[float]
    tp2_px: Optional[float]


def build_risk_plan(
    side: str,
    entry_px: float,
    level: Level,
    all_levels: List[Level],
    invalid_eps: float,
) -> Optional[RiskPlan]:
    if entry_px <= 0:
        return None
    side = side.upper()
    if side == "LONG":
        base = level.low if level.low is not None else level.price
        sl_px = base * (1 - invalid_eps)
    else:
        base = level.high if level.high is not None else level.price
        sl_px = base * (1 + invalid_eps)
    if sl_px <= 0:
        return None
    next_lv = next_levels(all_levels, entry_px, side, n=2)
    tp1 = next_lv[0].price if len(next_lv) > 0 else None
    tp2 = next_lv[1].price if len(next_lv) > 1 else None
    return RiskPlan(entry_px=entry_px, sl_px=sl_px, tp1_px=tp1, tp2_px=tp2)
