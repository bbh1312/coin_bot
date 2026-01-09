from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


@dataclass
class ZoneBand:
    name: str
    low: float
    high: float
    volume: float


@dataclass
class VolumeProfile:
    poc: ZoneBand
    vah: float
    val: float
    hvn: List[ZoneBand]
    lvn: List[ZoneBand]
    bins: Dict[int, float]
    bin_size: float


def build_volume_profile(
    df: pd.DataFrame,
    bin_size_pct: float,
    bin_size_abs: float,
    value_area_pct: float,
    min_zone_width_pct: float,
    hvn_threshold_ratio: float,
    lvn_threshold_ratio: float,
) -> Optional[VolumeProfile]:
    if df is None or df.empty:
        return None
    closes = df["close"].astype(float)
    vols = df["volume"].astype(float)
    if closes.empty or vols.empty:
        return None
    min_price = float(closes.min())
    max_price = float(closes.max())
    if min_price <= 0 or max_price <= 0 or max_price <= min_price:
        return None

    if bin_size_abs and bin_size_abs > 0:
        bin_size = float(bin_size_abs)
    else:
        bin_size = float(min_price * bin_size_pct)
    if bin_size <= 0:
        return None

    bins: Dict[int, float] = {}
    for price, vol in zip(closes.tolist(), vols.tolist()):
        try:
            idx = int((float(price) - min_price) / bin_size)
        except Exception:
            continue
        bins[idx] = bins.get(idx, 0.0) + float(vol)

    if not bins:
        return None

    total_vol = sum(bins.values())
    if total_vol <= 0:
        return None

    max_idx = max(bins.items(), key=lambda x: x[1])[0]
    poc_low = min_price + max_idx * bin_size
    poc_high = poc_low + bin_size
    poc = ZoneBand("POC", poc_low, poc_high, bins[max_idx])

    sorted_bins = sorted(bins.items(), key=lambda x: x[1], reverse=True)
    target_vol = total_vol * value_area_pct
    acc = 0.0
    va_idxs: List[int] = []
    for idx, vol in sorted_bins:
        acc += vol
        va_idxs.append(idx)
        if acc >= target_vol:
            break
    if not va_idxs:
        va_idxs = [max_idx]
    val = min_price + min(va_idxs) * bin_size
    vah = min_price + (max(va_idxs) + 1) * bin_size

    max_vol = max(bins.values())
    hvn = []
    lvn = []
    for idx, vol in bins.items():
        low = min_price + idx * bin_size
        high = low + bin_size
        if vol >= max_vol * hvn_threshold_ratio:
            hvn.append(ZoneBand("HVN", low, high, vol))
        if vol <= max_vol * lvn_threshold_ratio:
            lvn.append(ZoneBand("LVN", low, high, vol))

    return VolumeProfile(
        poc=poc,
        vah=vah,
        val=val,
        hvn=hvn,
        lvn=lvn,
        bins=bins,
        bin_size=bin_size,
    )
