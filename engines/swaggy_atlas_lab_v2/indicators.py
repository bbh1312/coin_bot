from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def atr(df: pd.DataFrame, length: int) -> float:
    if df.empty or len(df) < length + 2:
        return 0.0
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < length:
        return 0.0
    return float(sum(trs[-length:]) / length)


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


@dataclass
class RegimeState:
    regime: str
    last_swing_high: Optional[float]
    last_swing_low: Optional[float]
    regime_score: float = 0.0
    range_reason: str = ""


def _swing_points(df: pd.DataFrame, lookback: int, k: int) -> tuple[list[float], list[float]]:
    if df.empty or len(df) < (k * 2 + 1):
        return [], []
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    start = max(k, len(df) - lookback - k - 1)
    swing_highs = []
    swing_lows = []
    for i in range(start, len(df) - k):
        left_high = max(highs[i - k:i])
        right_high = max(highs[i + 1:i + k + 1])
        left_low = min(lows[i - k:i])
        right_low = min(lows[i + 1:i + k + 1])
        if highs[i] > left_high and highs[i] > right_high:
            swing_highs.append(highs[i])
        if lows[i] < left_low and lows[i] < right_low:
            swing_lows.append(lows[i])
    return swing_highs, swing_lows


def detect_regime(df: pd.DataFrame, lookback: int = 30, k: int = 2) -> RegimeState:
    swing_highs, swing_lows = _swing_points(df, lookback, k)
    last_high = swing_highs[-1] if swing_highs else None
    last_low = swing_lows[-1] if swing_lows else None
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        hh = swing_highs[-1] > swing_highs[-2]
        hl = swing_lows[-1] > swing_lows[-2]
        lh = swing_highs[-1] < swing_highs[-2]
        ll = swing_lows[-1] < swing_lows[-2]
        if hh and hl:
            return RegimeState("bull", last_high, last_low, regime_score=1.0, range_reason="")
        if lh and ll:
            return RegimeState("bear", last_high, last_low, regime_score=1.0, range_reason="")
        return RegimeState("range", last_high, last_low, regime_score=0.0, range_reason="no_trend")
    return RegimeState("range", last_high, last_low, regime_score=0.0, range_reason="insufficient_swings")
