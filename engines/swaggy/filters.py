from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from engines.swaggy.levels import Level
from engines.swaggy.vp_profile import VPLevels


@dataclass
class FilterResult:
    ok: bool
    reason: str


def dist_to_level_ok(price: float, level: Level, max_dist: float) -> FilterResult:
    if level.price <= 0:
        return FilterResult(False, "dist")
    dist = abs(price - level.price) / level.price
    if dist > max_dist:
        return FilterResult(False, "dist")
    return FilterResult(True, "")


def in_lvn_gap(last_price: float, vp: Optional[VPLevels]) -> FilterResult:
    if vp is None:
        return FilterResult(True, "")
    for low, high in vp.lvn_gaps:
        if low <= last_price <= high:
            return FilterResult(False, "lvn_gap")
    return FilterResult(True, "")


def expansion_bar(df: pd.DataFrame, atr_len: int, expansion_mult: float) -> FilterResult:
    if df.empty or len(df) < atr_len + 2:
        return FilterResult(True, "")
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < atr_len:
        return FilterResult(True, "")
    atr = float(np.mean(trs[-atr_len:]))
    last_range = highs[-1] - lows[-1]
    if atr > 0 and last_range > atr * expansion_mult:
        return FilterResult(False, "expansion")
    return FilterResult(True, "")


def cooldown_ok(now_ts: float, last_ts: Optional[float], cooldown_min: int) -> FilterResult:
    if not last_ts:
        return FilterResult(True, "")
    if (now_ts - last_ts) < cooldown_min * 60:
        return FilterResult(False, "cooldown")
    return FilterResult(True, "")


def regime_ok(
    regime: str,
    side: str,
    allow_countertrend: bool,
    range_short_allowed: bool,
) -> FilterResult:
    if allow_countertrend:
        return FilterResult(True, "")
    if regime == "bull" and side == "SHORT":
        return FilterResult(False, "regime")
    if regime == "bear" and side == "LONG":
        return FilterResult(False, "regime")
    if regime == "range" and side == "SHORT" and not range_short_allowed:
        return FilterResult(False, "regime")
    return FilterResult(True, "")
