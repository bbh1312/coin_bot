from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from engines.base import BaseEngine


@dataclass
class WashShortSuiteConfig:
    tf_trend: str = "1h"
    tf_main: str = "15m"
    tf_exec: str = "1m"
    ema_fast: int = 20
    ema_mid: int = 60
    ema_slow: int = 120
    adx_len: int = 14
    adx_min: float = 16.0
    swing_lookback: int = 50
    fib_eps: float = 0.002
    pullback_eps: float = 0.0025
    accel_close_pos_min: float = 0.55
    accel_body_ratio_min: float = 0.35
    accel_vol_ratio_min: float = 1.0
    rsi_len: int = 14
    rsi_min: float = 25.0
    rsi_max: float = 55.0
    rsi_lower_high_delta: float = 0.3
    vol_sma_len: int = 20
    vol_pullback_max: float = 0.8
    vol_reversal_min: float = 0.4
    btc_ema_guard_len: int = 14
    btc_ema_fast: int = 7
    btc_ema_slow: int = 20
    entry_block_high_mult: float = 1.01
    entry_block_close_pos: float = 0.7
    entry_block_vol_ratio: float = 1.5
    sl_fixed_pct: float = 0.005
    sl_atr_mult: float = 1.5
    tp_atr_mult: float = 2.0
    tp_pct: float = 0.09
    sl_pct: float = 0.04
    time_stop_minutes: int = 300
    cooldown_bars: int = 18


class WashShortSuiteEngine(BaseEngine):
    name = "wash_short_suite"

    def __init__(self, config: WashShortSuiteConfig | None = None) -> None:
        self.config = config or WashShortSuiteConfig()


def wash_ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def wash_atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def wash_rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0.0)


def wash_adx(df: pd.DataFrame, length: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = pd.concat(
        [(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, np.nan)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx.fillna(0.0), plus_di.fillna(0.0), minus_di.fillna(0.0)


def wash_bb_mid(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


def wash_upper_wick_ratio(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    upper = float(row["high"] - max(row["open"], row["close"]))
    return upper / rng


def wash_body_ratio(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    return float(abs(row["close"] - row["open"]) / rng)


def wash_is_shooting_star(row: pd.Series) -> bool:
    return wash_upper_wick_ratio(row) >= 0.6 and wash_body_ratio(row) <= 0.5


def wash_is_bear_engulf(prev: pd.Series, cur: pd.Series) -> bool:
    return (
        float(prev["close"]) > float(prev["open"])
        and float(cur["close"]) < float(cur["open"])
        and float(cur["open"]) >= float(prev["close"])
        and float(cur["close"]) <= float(prev["open"])
    )


def wash_fib_levels(high: float, low: float) -> list[float]:
    diff = high - low
    return [high - diff * 0.382, high - diff * 0.5, high - diff * 0.618]


def wash_in_zone(price: float, level: float, eps: float) -> bool:
    if level <= 0:
        return False
    return abs(price - level) / level <= eps


def wash_map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def wash_btc_guard(
    btc_df_15m: pd.DataFrame,
    ts_ms: int,
    ema_guard_len: int,
    ema_fast: int,
    ema_slow: int,
) -> bool:
    if btc_df_15m is None or btc_df_15m.empty:
        return False
    ts_arr = btc_df_15m["ts"].astype(int).to_numpy()
    idx = wash_map_idx_by_ts(ts_arr, ts_ms)
    if idx <= 0:
        return False
    start_idx = max(0, idx - 288 + 1)
    window = btc_df_15m["close"].astype(float).iloc[start_idx : idx + 1]
    if window.empty:
        return False
    ema_guard = window.ewm(span=int(ema_guard_len), adjust=False).mean().iloc[-1]
    ema7 = window.ewm(span=int(ema_fast), adjust=False).mean().iloc[-1]
    ema20 = window.ewm(span=int(ema_slow), adjust=False).mean().iloc[-1]
    last_px = float(window.iloc[-1])
    if last_px > float(ema_guard):
        return True
    if ema7 > ema20:
        return True
    return False


def wash_short_entry_signal(
    df_tr_sig: pd.DataFrame,
    df_main_sig: pd.DataFrame,
    df_ex: pd.DataFrame,
    df_ex_sig: pd.DataFrame,
    i_ex: int,
    idx_tr: int,
    idx_main: int,
    cfg: WashShortSuiteConfig,
    entry_offset: int = 1,
) -> Tuple[Optional[dict], str]:
    if i_ex <= 0 or idx_tr <= 0 or idx_main <= 0:
        return None, "skip_eval"

    ema20_tr = wash_ema(df_tr_sig["close"].astype(float), cfg.ema_fast)
    ema60_tr = wash_ema(df_tr_sig["close"].astype(float), cfg.ema_mid)
    ema120_tr = wash_ema(df_tr_sig["close"].astype(float), cfg.ema_slow)
    adx_tr, pdi_tr, mdi_tr = wash_adx(df_tr_sig, cfg.adx_len)

    ema20_main = wash_ema(df_main_sig["close"].astype(float), cfg.ema_fast)
    ema60_main = wash_ema(df_main_sig["close"].astype(float), cfg.ema_mid)
    ema120_main = wash_ema(df_main_sig["close"].astype(float), cfg.ema_slow)
    adx_main, pdi_main, mdi_main = wash_adx(df_main_sig, cfg.adx_len)
    bb_mid = wash_bb_mid(df_main_sig["close"].astype(float), cfg.ema_fast)
    atr_main = wash_atr(df_main_sig, cfg.adx_len)

    trend_ok = (
        ema20_tr.iloc[idx_tr] < ema60_tr.iloc[idx_tr] < ema120_tr.iloc[idx_tr]
        and adx_tr.iloc[idx_tr] > cfg.adx_min
        and mdi_tr.iloc[idx_tr] > pdi_tr.iloc[idx_tr]
        and float(df_tr_sig.iloc[idx_tr]["close"]) < float(ema60_tr.iloc[idx_tr])
        and ema20_main.iloc[idx_main] < ema60_main.iloc[idx_main] < ema120_main.iloc[idx_main]
        and adx_main.iloc[idx_main] > cfg.adx_min
        and mdi_main.iloc[idx_main] > pdi_main.iloc[idx_main]
        and float(df_main_sig.iloc[idx_main]["close"]) < float(ema60_main.iloc[idx_main])
    )
    if not trend_ok:
        return None, "trend_fail"

    swing_start = max(0, idx_main - cfg.swing_lookback)
    swing_high = float(df_main_sig["high"].iloc[swing_start: idx_main + 1].max())
    swing_low = float(df_main_sig["low"].iloc[swing_start: idx_main + 1].min())
    fibs = wash_fib_levels(swing_high, swing_low)
    ema_mid = float(ema20_main.iloc[idx_main])
    bbm = float(bb_mid.iloc[idx_main]) if not np.isnan(bb_mid.iloc[idx_main]) else ema_mid
    levels = fibs + [ema_mid, bbm, swing_low]
    price = float(df_main_sig.iloc[idx_main]["close"])
    if not any(wash_in_zone(price, lv, cfg.pullback_eps) for lv in levels):
        return None, "pullback_fail"

    rsi_ex = wash_rsi(df_ex_sig["close"].astype(float), cfg.rsi_len)
    vol_sma_ex = df_ex["volume"].astype(float).rolling(cfg.vol_sma_len).mean()
    rsi_now = float(rsi_ex.iloc[i_ex])
    rsi_prev = float(rsi_ex.iloc[i_ex - 1]) if i_ex > 0 else rsi_now
    rsi_turn = cfg.rsi_min <= rsi_now <= cfg.rsi_max and (rsi_now + cfg.rsi_lower_high_delta) < rsi_prev
    cur = df_ex_sig.iloc[i_ex]
    prev = df_ex_sig.iloc[i_ex - 1]
    candle_ok = wash_is_shooting_star(cur) or wash_is_bear_engulf(prev, cur)
    vol_now = float(cur["volume"])
    vol_avg = float(vol_sma_ex.iloc[i_ex]) if not np.isnan(vol_sma_ex.iloc[i_ex]) else 0.0
    vol_ratio = (vol_now / vol_avg) if vol_avg > 0 else 0.0
    pullback_ok = vol_ratio >= cfg.vol_reversal_min
    if not (rsi_turn and candle_ok and pullback_ok):
        return None, "trigger_fail"

    entry_idx = i_ex + int(entry_offset)
    if entry_idx < 0 or entry_idx >= len(df_ex):
        return None, "no_data_exec"
    entry = df_ex.iloc[entry_idx]
    entry_high = float(entry["high"])
    entry_low = float(entry["low"])
    entry_close = float(entry["close"])
    entry_rng = max(entry_high - entry_low, 1e-9)
    entry_close_pos = (entry_close - entry_low) / entry_rng
    entry_vol = float(entry["volume"])
    if entry_idx >= len(vol_sma_ex):
        return None, "no_data_exec"
    entry_vol_avg = float(vol_sma_ex.iloc[entry_idx]) if not np.isnan(vol_sma_ex.iloc[entry_idx]) else 0.0
    entry_vol_ratio = (entry_vol / entry_vol_avg) if entry_vol_avg > 0 else 0.0
    if entry_high > float(cur["high"]) * cfg.entry_block_high_mult:
        return None, "entry_block_fail"
    if entry_close_pos >= cfg.entry_block_close_pos and entry_vol_ratio >= cfg.entry_block_vol_ratio:
        return None, "entry_block_fail"

    entry_px = float(entry_close)
    atr_px = float(atr_main.iloc[idx_main]) if not np.isnan(atr_main.iloc[idx_main]) else 0.0
    sl_price = entry_px * (1.0 + cfg.sl_pct)
    if atr_px > 0:
        sl_price = max(sl_price, entry_px + atr_px * cfg.sl_atr_mult)
    tp_price = entry_px - atr_px * cfg.tp_atr_mult if atr_px > 0 else entry_px * (1.0 - cfg.tp_pct)
    tp_pct = ((entry_px - tp_price) / entry_px) * 100.0 if entry_px > 0 else None
    sl_pct = ((sl_price - entry_px) / entry_px) * 100.0 if entry_px > 0 else None
    return {
        "entry_px": entry_px,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "entry_ts": int(entry["ts"]),
    }, "ok"
