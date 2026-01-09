from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from engines.volume_profile import VolumeProfile, build_volume_profile


@dataclass
class VPLevels:
    profile: VolumeProfile
    poc: float
    vah: float
    val: float
    lvn_gaps: List[Tuple[float, float]]


def build_vp_levels(
    df_1h: pd.DataFrame,
    bin_size_pct: float,
    bin_size_abs: float,
    value_area_pct: float,
    min_zone_width_pct: float,
    hvn_threshold_ratio: float,
    lvn_threshold_ratio: float,
    lvn_ratio: float,
) -> Optional[VPLevels]:
    if bin_size_abs <= 0 and bin_size_pct <= 0:
        return None
    profile = build_volume_profile(
        df_1h,
        bin_size_pct,
        bin_size_abs,
        value_area_pct,
        min_zone_width_pct,
        hvn_threshold_ratio,
        lvn_threshold_ratio,
    )
    if profile is None:
        return None
    poc = (profile.poc.low + profile.poc.high) / 2.0
    lvn_gaps = _lvn_gaps_from_bins(profile.bins, profile.bin_size, lvn_ratio)
    return VPLevels(profile=profile, poc=poc, vah=profile.vah, val=profile.val, lvn_gaps=lvn_gaps)


def _lvn_gaps_from_bins(bins: dict, bin_size: float, lvn_ratio: float) -> List[Tuple[float, float]]:
    if not bins or bin_size <= 0:
        return []
    vols = list(bins.values())
    if not vols:
        return []
    median = sorted(vols)[len(vols) // 2]
    cutoff = median * lvn_ratio
    idxs = sorted([idx for idx, v in bins.items() if v < cutoff])
    if not idxs:
        return []
    gaps: List[Tuple[float, float]] = []
    cur = [idxs[0]]
    for idx in idxs[1:]:
        if idx == cur[-1] + 1:
            cur.append(idx)
        else:
            gaps.append((cur[0] * bin_size, (cur[-1] + 1) * bin_size))
            cur = [idx]
    if cur:
        gaps.append((cur[0] * bin_size, (cur[-1] + 1) * bin_size))
    return gaps
