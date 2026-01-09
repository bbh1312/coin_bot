from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from engines.swaggy.levels import Level


@dataclass
class Trigger:
    side: str
    kind: str
    level: Level
    strength: float
    evidence: Dict[str, float]


def _vol_sma(df: pd.DataFrame, length: int = 20) -> float:
    if df.empty:
        return 0.0
    vols = df["volume"].astype(float)
    if len(vols) < length:
        return float(vols.mean())
    return float(vols.rolling(length).mean().iloc[-1])


def detect_reclaim(df: pd.DataFrame, level: Level, touch_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 4:
        return None
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    c1_close = float(c1["close"])
    c2_close = float(c2["close"])
    c3_close = float(c3["close"])
    if (
        c1_close < level.price * (1 - touch_eps)
        and c2_close >= level.price * (1 - touch_eps)
        and c3_close >= level.price * (1 - touch_eps)
    ):
        return Trigger(
            "long",
            "RECLAIM",
            level,
            0.0,
            {"c1_close": c1_close, "c2_close": c2_close, "c3_close": c3_close},
        )
    if (
        c1_close > level.price * (1 + touch_eps)
        and c2_close <= level.price * (1 + touch_eps)
        and c3_close <= level.price * (1 + touch_eps)
    ):
        return Trigger(
            "short",
            "RECLAIM",
            level,
            0.0,
            {"c1_close": c1_close, "c2_close": c2_close, "c3_close": c3_close},
        )
    return None


def detect_rejection_wick(df: pd.DataFrame, level: Level, wick_ratio: float, vol_mult: float) -> Optional[Trigger]:
    if df.empty:
        return None
    last = df.iloc[-1]
    high = float(last["high"])
    low = float(last["low"])
    close = float(last["close"])
    open_ = float(last["open"])
    vol = float(last["volume"])
    vr = _vol_sma(df)
    if vr <= 0 or vol < vr * vol_mult:
        return None
    candle_range = max(1e-9, high - low)
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    if high >= level.price and upper_wick / candle_range >= wick_ratio and close < level.price:
        return Trigger(
            "short",
            "REJECTION",
            level,
            0.0,
            {"wick_ratio": upper_wick / candle_range, "vol": vol, "vol_sma": vr},
        )
    if low <= level.price and lower_wick / candle_range >= wick_ratio and close > level.price:
        return Trigger(
            "long",
            "REJECTION",
            level,
            0.0,
            {"wick_ratio": lower_wick / candle_range, "vol": vol, "vol_sma": vr},
        )
    return None


def detect_sweep_and_return(df: pd.DataFrame, level: Level, sweep_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 2:
        return None
    prev = df.iloc[-2]
    last = df.iloc[-1]
    prev_high = float(prev["high"])
    prev_low = float(prev["low"])
    last_high = float(last["high"])
    last_low = float(last["low"])
    last_close = float(last["close"])
    if (prev_high >= level.price * (1 + sweep_eps) or last_high >= level.price * (1 + sweep_eps)) and last_close < level.price:
        return Trigger("short", "SWEEP", level, 0.0, {"last_close": last_close})
    if (prev_low <= level.price * (1 - sweep_eps) or last_low <= level.price * (1 - sweep_eps)) and last_close > level.price:
        return Trigger("long", "SWEEP", level, 0.0, {"last_close": last_close})
    return None


def detect_breakout_retest(df: pd.DataFrame, level: Level, hold_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 4:
        return None
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    c1_close = float(c1["close"])
    c2_low = float(c2["low"])
    c2_high = float(c2["high"])
    c3_close = float(c3["close"])
    hold_tol = hold_eps * 0.5
    if c1_close > level.price and c2_low <= level.price and c3_close > level.price * (1 + hold_tol):
        return Trigger("long", "RETEST", level, 0.0, {"c1": c1_close, "c3": c3_close})
    if c1_close < level.price and c2_high >= level.price and c3_close < level.price * (1 - hold_tol):
        return Trigger("short", "RETEST", level, 0.0, {"c1": c1_close, "c3": c3_close})
    return None
