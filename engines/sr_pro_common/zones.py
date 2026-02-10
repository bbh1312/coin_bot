from __future__ import annotations

from typing import Optional, Protocol, List

import numpy as np
import pandas as pd


class SrZoneConfig(Protocol):
    lookback: int
    relaxed_lookback: int
    auto_relax: bool
    atr_mult: float
    cluster_atr: float
    max_zones_per_side: int
    delta_len: int


def build_sr_zones(
    df_1h_hist: pd.DataFrame,
    cfg: SrZoneConfig,
    window_bars: Optional[int] = None,
) -> List[dict]:
    """Build SR zones from 1h pivots. Returns list of dicts with mid/top/bot/side/vol/born/start."""
    if df_1h_hist is None or df_1h_hist.empty:
        return []
    if window_bars and window_bars > 0 and len(df_1h_hist) > window_bars:
        df_1h_hist = df_1h_hist.iloc[-window_bars:].copy()

    close = df_1h_hist["close"].astype(float)
    open_ = df_1h_hist["open"].astype(float)
    high = df_1h_hist["high"].astype(float)
    low = df_1h_hist["low"].astype(float)
    vol = df_1h_hist["volume"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
    dv = np.where(close > open_, vol, np.where(close < open_, -vol, 0.0))
    dv = pd.Series(dv, index=df_1h_hist.index)
    dvf = dv.ewm(span=cfg.delta_len, adjust=False).mean()

    def _pivot_confirmed(series: pd.Series, i: int, lb: int, mode: str) -> Optional[float]:
        pivot_idx = i - lb
        if pivot_idx < lb or pivot_idx + lb >= len(series):
            return None
        window = series.iloc[pivot_idx - lb : pivot_idx + lb + 1]
        val = series.iloc[pivot_idx]
        if mode == "high":
            return float(val) if float(val) == float(window.max()) else None
        return float(val) if float(val) == float(window.min()) else None

    zones: List[dict] = []
    for i in range(len(df_1h_hist)):
        lb = cfg.lookback
        ph = _pivot_confirmed(high, i, lb, "high")
        pl = _pivot_confirmed(low, i, lb, "low")
        lb_used = lb
        if cfg.auto_relax and ph is None and pl is None:
            lb2 = cfg.relaxed_lookback
            ph = _pivot_confirmed(high, i, lb2, "high")
            pl = _pivot_confirmed(low, i, lb2, "low")
            lb_used = lb2
        if ph is None and pl is None:
            if i % 20 == 0:
                for side in (1, -1):
                    side_z = [z for z in zones if z["side"] == side]
                    if len(side_z) > cfg.max_zones_per_side:
                        oldest = min(side_z, key=lambda z: z["born"])
                        zones.remove(oldest)
            continue

        pivot_bar = i - lb_used
        if pivot_bar < 0:
            continue
        half_w = float(atr.iloc[i]) * cfg.atr_mult * 0.5
        cluster_dist = float(atr.iloc[i]) * cfg.cluster_atr
        vol_val = float(dvf.iloc[pivot_bar]) if pivot_bar < len(dvf) else float(dvf.iloc[i])

        def merge_or_create(side: int, level: float) -> None:
            for z in zones:
                if z["side"] == side and abs(z["mid"] - level) <= cluster_dist:
                    new_mid = (z["mid"] + level) * 0.5
                    z["mid"] = new_mid
                    z["top"] = new_mid + half_w
                    z["bot"] = new_mid - half_w
                    z["vol"] = (z["vol"] + vol_val) * 0.5
                    return
            zones.append(
                {
                    "mid": level,
                    "top": level + half_w,
                    "bot": level - half_w,
                    "side": side,
                    "vol": vol_val,
                    "born": i,
                    "start": pivot_bar,
                }
            )

        if ph is not None:
            merge_or_create(1, float(ph))
        if pl is not None:
            merge_or_create(-1, float(pl))

        if i % 20 == 0:
            for side in (1, -1):
                side_z = [z for z in zones if z["side"] == side]
                if len(side_z) > cfg.max_zones_per_side:
                    oldest = min(side_z, key=lambda z: z["born"])
                    zones.remove(oldest)

    return zones
