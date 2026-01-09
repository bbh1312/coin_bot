from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from engines.swaggy.vp_profile import VPLevels


@dataclass
class Level:
    price: float
    kind: str
    low: Optional[float] = None
    high: Optional[float] = None
    ts: Optional[float] = None


def _cluster_levels(levels: List[Level], cluster_pct: float) -> List[Level]:
    if not levels:
        return []
    levels = sorted(levels, key=lambda x: x.price)
    clustered: List[Level] = []
    bucket = [levels[0]]
    for lv in levels[1:]:
        anchor = bucket[-1]
        dist = abs(lv.price - anchor.price) / anchor.price if anchor.price else 0.0
        if dist <= cluster_pct:
            bucket.append(lv)
        else:
            clustered.append(_merge_bucket(bucket))
            bucket = [lv]
    if bucket:
        clustered.append(_merge_bucket(bucket))
    return clustered


def _merge_bucket(bucket: List[Level]) -> Level:
    prices = [b.price for b in bucket]
    kinds = {b.kind for b in bucket}
    low = min(b.low for b in bucket if b.low is not None) if any(b.low is not None for b in bucket) else None
    high = max(b.high for b in bucket if b.high is not None) if any(b.high is not None for b in bucket) else None
    kind = "+".join(sorted(kinds))
    return Level(price=float(np.mean(prices)), kind=kind, low=low, high=high)


def _swing_levels(df: pd.DataFrame, lookback: int = 60) -> List[Level]:
    if df.empty or len(df) < 5:
        return []
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    ts_list = None
    if "ts" in df.columns:
        ts_list = df["ts"].tolist()
    levels: List[Level] = []
    start = max(1, len(df) - lookback - 2)
    for i in range(start, len(df) - 1):
        ts_val = None
        if ts_list is not None:
            try:
                ts_val = float(ts_list[i]) / 1000.0
            except Exception:
                ts_val = None
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            levels.append(Level(price=highs[i], kind="SWING_HIGH", ts=ts_val))
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            levels.append(Level(price=lows[i], kind="SWING_LOW", ts=ts_val))
    return levels


def _round_levels(price: float, tick_size: float, count: int = 6) -> List[Level]:
    if price <= 0 or tick_size <= 0:
        return []
    step = tick_size * 250
    base = round(price / step) * step
    levels = []
    for i in range(-count, count + 1):
        levels.append(Level(price=base + step * i, kind="ROUND"))
    return levels


def build_levels(
    df_1h: pd.DataFrame,
    vp: Optional[VPLevels],
    last_price: float,
    level_cluster_pct: float,
    tick_size: float,
    df_mtf: Optional[pd.DataFrame] = None,
    swing_lookback_1h: int = 60,
    swing_lookback_mtf: int = 32,
    max_dist_pct: float = 0.06,
    level_cap: int = 25,
    return_meta: bool = False,
) -> List[Level] | tuple[List[Level], dict]:
    levels: List[Level] = []
    swing_1h = _swing_levels(df_1h, lookback=swing_lookback_1h)
    swing_mtf = _swing_levels(df_mtf, lookback=swing_lookback_mtf) if df_mtf is not None and not df_mtf.empty else []
    swing_1h_raw = list(swing_1h)
    swing_mtf_raw = list(swing_mtf)
    if not swing_1h and not swing_mtf:
        fallback_src = df_mtf if df_mtf is not None and not df_mtf.empty else df_1h
        swing_fallback = _fallback_swing_levels(fallback_src)
        swing_1h = swing_fallback
    levels.extend(swing_1h)
    levels.extend(swing_mtf)
    round_lv = _round_levels(last_price, tick_size)
    levels.extend(round_lv)
    vp_levels: List[Level] = []
    if vp:
        vp_levels.append(Level(price=vp.poc, kind="VP_POC", low=vp.profile.poc.low, high=vp.profile.poc.high))
        vp_levels.append(Level(price=vp.vah, kind="VP_VAH"))
        vp_levels.append(Level(price=vp.val, kind="VP_VAL"))
        for b in vp.profile.hvn:
            vp_levels.append(Level(price=(b.low + b.high) / 2.0, kind="VP_HVN", low=b.low, high=b.high))
        for b in vp.profile.lvn:
            vp_levels.append(Level(price=(b.low + b.high) / 2.0, kind="VP_LVN", low=b.low, high=b.high))
    levels.extend(vp_levels)
    levels = [lv for lv in levels if lv.price > 0]
    raw_levels = list(levels)
    before_cluster = len(levels)
    levels = _cluster_levels(levels, level_cluster_pct)
    after_cluster = len(levels)
    if last_price > 0 and max_dist_pct and max_dist_pct > 0:
        levels = [
            lv for lv in levels
            if abs(lv.price - last_price) / last_price <= max_dist_pct
        ]
    after_dist = len(levels)
    if level_cap and level_cap > 0:
        levels = sorted(levels, key=lambda lv: abs(lv.price - last_price))[:level_cap]
    after_cap = len(levels)
    if not return_meta:
        return levels
    meta = {
        "swing_1h": len(swing_1h),
        "swing_mtf": len(swing_mtf),
        "swing_1h_raw": len(swing_1h_raw),
        "swing_mtf_raw": len(swing_mtf_raw),
        "round": len(round_lv),
        "vp": len(vp_levels),
        "before_cluster": before_cluster,
        "after_cluster": after_cluster,
        "after_dist": after_dist,
        "after_cap": after_cap,
        "max_dist_pct": max_dist_pct,
        "level_cap": level_cap,
        "raw_levels": raw_levels,
    }
    return levels, meta


def _fallback_swing_levels(df: pd.DataFrame) -> List[Level]:
    if df is None or df.empty:
        return []
    highs = df["high"].astype(float)
    lows = df["low"].astype(float)
    levels: List[Level] = []
    for window in (60, 20):
        if len(df) < 5:
            continue
        w = min(len(df), window)
        high = float(highs.iloc[-w:].max())
        low = float(lows.iloc[-w:].min())
        levels.append(Level(price=high, kind="FALLBACK_SWING"))
        levels.append(Level(price=low, kind="FALLBACK_SWING"))
    return levels


def nearest_level(
    levels: Iterable[Level],
    price: float,
    now_ts: Optional[float] = None,
    recent_secs: Optional[float] = None,
) -> Optional[Level]:
    best = None
    best_dist = None
    for lv in levels:
        dist = abs(price - lv.price)
        penalty = 0.0
        if now_ts is not None and recent_secs:
            if lv.ts is not None:
                age = max(0.0, now_ts - lv.ts)
                if age > recent_secs:
                    penalty = dist * 0.15
        score = dist + penalty
        if best_dist is None or score < best_dist:
            best = lv
            best_dist = score
    return best


def next_levels(levels: Iterable[Level], price: float, side: str, n: int = 2) -> List[Level]:
    if side == "LONG":
        candidates = sorted([lv for lv in levels if lv.price > price], key=lambda x: x.price)
    else:
        candidates = sorted([lv for lv in levels if lv.price < price], key=lambda x: x.price, reverse=True)
    return candidates[:n]
