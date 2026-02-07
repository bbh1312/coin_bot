from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class TopFailShortV1Config:
    tf_ltf: str = "3m"
    tf_mtf: str = "15m"
    tf_htf: str = "1h"
    universe_24h_change: float = 20.0
    universe_7d_mult: float = 3.0
    min_quote_vol_24h: float = 0.0
    stall_high_lookback: int = 6
    stall_wick_min: float = 0.5
    stall_min_count: int = 2
    mtf_require_weak_close: bool = False
    ema_len: int = 20
    swing_lookback: int = 30
    atr_len: int = 14
    wash_atr_mult: float = 1.3
    vol_spike_mult: float = 1.8
    vol_sma_len: int = 20
    retest_ema_tol: float = 0.1
    limit_entry: bool = True
    limit_offset_atr: float = 0.15
    retest_max_depth_atr: float = 1.15
    retest_wait_next_high: bool = False
    fail_wick_max: float = 0.30
    fail_require_ema: bool = False
    stop_atr_mult: float = 0.35
    min_hold_bars: int = 3
    tp_min_pct: float = 0.015
    tp_r_mult: float = 0.9
    max_wait_bars: int = 60
    cooldown_bars: int = 20


def top_fail_ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def top_fail_upper_wick_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    upper = high - max(open_, close)
    return upper / rng


def top_fail_map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def top_fail_short_entry_signal(
    df_ltf_sig: pd.DataFrame,
    df_mtf_sig: pd.DataFrame,
    df_htf_sig: pd.DataFrame,
    sig_idx: int,
    cfg: TopFailShortV1Config,
    state: Optional[dict] = None,
    update_state: bool = True,
) -> Tuple[Optional[dict], str, dict]:
    meta = {
        "universe_ok": False,
        "stall_ok": False,
        "armed_ok": False,
        "entry_window": False,
        "retest_touch": False,
    }
    if sig_idx <= 0:
        return None, "no_data_ltf", meta
    ts_ms = int(df_ltf_sig.iloc[sig_idx]["ts"])
    ts_mtf = df_mtf_sig["ts"].astype(int).to_numpy()
    ts_htf = df_htf_sig["ts"].astype(int).to_numpy()
    idx_mtf = top_fail_map_idx_by_ts(ts_mtf, ts_ms)
    idx_htf = top_fail_map_idx_by_ts(ts_htf, ts_ms)
    if idx_mtf <= 0 or idx_htf <= 0:
        return None, "no_data_htf", meta

    close_htf = float(df_htf_sig["close"].iloc[idx_htf])
    close_prev_htf = float(df_htf_sig["close"].iloc[idx_htf - 1])
    high_prev_htf = float(df_htf_sig["high"].iloc[idx_htf - 1])
    high_htf = float(df_htf_sig["high"].iloc[idx_htf])

    u1 = False
    u2 = False
    if idx_htf >= 24:
        c_24h = float(df_htf_sig["close"].iloc[idx_htf - 24])
        if c_24h > 0:
            u1 = ((close_htf - c_24h) / c_24h * 100.0) >= float(cfg.universe_24h_change)
    if idx_htf >= 168:
        low_7d = float(df_htf_sig["low"].iloc[idx_htf - 168: idx_htf + 1].min())
        if low_7d > 0:
            u2 = close_htf >= low_7d * float(cfg.universe_7d_mult)
    if not (u1 or u2):
        return None, "universe_fail", meta
    meta["universe_ok"] = True

    if float(cfg.min_quote_vol_24h) > 0 and idx_htf >= 24:
        qv = (
            df_htf_sig["close"].iloc[idx_htf - 23: idx_htf + 1].astype(float)
            * df_htf_sig["volume"].iloc[idx_htf - 23: idx_htf + 1].astype(float)
        ).sum()
        if qv < float(cfg.min_quote_vol_24h):
            return None, "universe_fail", meta

    expanding = close_htf > high_prev_htf
    if expanding:
        return None, "stall_fail", meta
    stall_count = 0
    if close_htf <= close_prev_htf:
        stall_count += 1
    if top_fail_upper_wick_ratio(df_htf_sig.iloc[idx_htf]) >= float(cfg.stall_wick_min):
        stall_count += 1
    if idx_htf >= cfg.stall_high_lookback:
        prev_hi = float(df_htf_sig["high"].iloc[idx_htf - cfg.stall_high_lookback: idx_htf].max())
        if high_htf <= prev_hi:
            stall_count += 1
    if stall_count < int(cfg.stall_min_count):
        return None, "stall_fail", meta
    meta["stall_ok"] = True

    cur_close_mtf = float(df_mtf_sig["close"].iloc[idx_mtf])
    cur_ema_mtf = float(top_fail_ema(df_mtf_sig["close"], cfg.ema_len).iloc[idx_mtf])
    if cfg.mtf_require_weak_close:
        prev_close_mtf = float(df_mtf_sig["close"].iloc[idx_mtf - 1])
        washdown_armed = bool(cur_close_mtf < cur_ema_mtf and cur_close_mtf < prev_close_mtf)
    else:
        washdown_armed = bool(cur_close_mtf < cur_ema_mtf)
    if not washdown_armed:
        return None, "armed_fail", meta
    meta["armed_ok"] = True

    atr_ltf = (df_ltf_sig["high"] - df_ltf_sig["low"]).astype(float).rolling(cfg.atr_len).mean()
    vol_sma = df_ltf_sig["volume"].astype(float).rolling(cfg.vol_sma_len).mean()
    swing_low_prev = df_ltf_sig["low"].astype(float).rolling(cfg.swing_lookback).min().shift(1)
    atr_now = atr_ltf.iloc[sig_idx]
    vol_ma = vol_sma.iloc[sig_idx]
    swing_low = swing_low_prev.iloc[sig_idx]
    if np.isnan(atr_now) or np.isnan(vol_ma) or np.isnan(swing_low):
        return None, "entry_window_fail", meta
    conds = 0
    if float(df_ltf_sig["high"].iloc[sig_idx] - df_ltf_sig["low"].iloc[sig_idx]) >= float(cfg.wash_atr_mult) * float(atr_now):
        conds += 1
    if float(df_ltf_sig["volume"].iloc[sig_idx]) >= float(cfg.vol_spike_mult) * float(vol_ma):
        conds += 1
    entry_window = conds >= 1 and float(df_ltf_sig["low"].iloc[sig_idx]) < float(swing_low)
    meta["entry_window"] = bool(entry_window)

    state = state or {}
    entry_window_start = state.get("entry_window_start")
    break_level = state.get("break_level")
    retest_high = state.get("retest_high")
    retest_touch_high = state.get("retest_touch_high")
    retest_wait = bool(state.get("retest_wait"))

    if entry_window:
        if entry_window_start is None:
            entry_window_start = sig_idx
        break_level = float(swing_low)
    if entry_window_start is None or break_level is None:
        if update_state:
            state["entry_window_start"] = None
            state["break_level"] = None
            state["retest_high"] = None
            state["retest_wait"] = False
            state["retest_touch_high"] = None
        return None, "entry_window_fail", meta
    if (sig_idx - int(entry_window_start)) > int(cfg.max_wait_bars):
        if update_state:
            state["entry_window_start"] = None
            state["break_level"] = None
            state["retest_high"] = None
            state["retest_wait"] = False
            state["retest_touch_high"] = None
        return None, "entry_window_fail", meta

    ema_now = top_fail_ema(df_ltf_sig["close"], cfg.ema_len).iloc[sig_idx]
    retest_touch = float(df_ltf_sig["high"].iloc[sig_idx]) >= float(break_level) - float(cfg.retest_ema_tol) * float(atr_now)
    if retest_touch:
        meta["retest_touch"] = True
        touch_high = float(df_ltf_sig["high"].iloc[sig_idx])
        if retest_high is None or touch_high > float(retest_high):
            retest_high = touch_high
        if cfg.retest_wait_next_high:
            if retest_wait and retest_touch_high is not None and touch_high > float(retest_touch_high):
                retest_touch_high = touch_high
            elif not retest_wait:
                retest_touch_high = touch_high
            retest_wait = True

    if retest_touch and retest_high is not None:
        retest_depth = (float(retest_high) - float(break_level)) / float(atr_now) if atr_now > 0 else 0.0
        if retest_depth > float(cfg.retest_max_depth_atr):
            if update_state:
                state["retest_high"] = retest_high
                state["retest_wait"] = retest_wait
                state["retest_touch_high"] = retest_touch_high
                state["entry_window_start"] = entry_window_start
                state["break_level"] = break_level
            return None, "depth_fail", meta

    if cfg.retest_wait_next_high and retest_wait:
        if retest_touch_high is not None:
            cur_high = float(df_ltf_sig["high"].iloc[sig_idx])
            if cur_high > float(retest_touch_high):
                if update_state:
                    state["retest_high"] = retest_high
                    state["retest_wait"] = retest_wait
                    state["retest_touch_high"] = retest_touch_high
                    state["entry_window_start"] = entry_window_start
                    state["break_level"] = break_level
                return None, "retest_fail", meta
        retest_wait = False

    open_now = float(df_ltf_sig["open"].iloc[sig_idx])
    close_now = float(df_ltf_sig["close"].iloc[sig_idx])
    low_now = float(df_ltf_sig["low"].iloc[sig_idx])
    if close_now >= open_now:
        return None, "fail_candle_fail", meta
    if top_fail_upper_wick_ratio(df_ltf_sig.iloc[sig_idx]) > float(cfg.fail_wick_max):
        return None, "fail_candle_fail", meta
    if close_now >= float(break_level):
        return None, "fail_candle_fail", meta
    if cfg.fail_require_ema:
        if close_now >= float(ema_now):
            return None, "fail_candle_fail", meta
    else:
        if sig_idx <= 0:
            return None, "fail_candle_fail", meta
        prev_low = float(df_ltf_sig["low"].iloc[sig_idx - 1])
        if low_now >= prev_low:
            return None, "fail_candle_fail", meta

    if retest_high is None:
        return None, "retest_fail", meta

    entry_type = "limit" if cfg.limit_entry else "market"
    if cfg.limit_entry:
        entry_limit = float(break_level) - float(cfg.limit_offset_atr) * float(atr_now)
        if low_now > entry_limit:
            return None, "fail_candle_fail", meta
        entry_px = float(entry_limit)
    else:
        entry_px = float(close_now)
    sl_candidate = float(retest_high)
    ema_stop = float(ema_now) + 0.3 * float(atr_now)
    sl_price = max(sl_candidate, ema_stop)
    r_val = sl_price - entry_px
    if r_val <= 0:
        return None, "fail_candle_fail", meta
    tp_dist = max(float(cfg.tp_r_mult) * r_val, float(cfg.tp_min_pct) * entry_px)
    tp_price = entry_px - tp_dist
    tp_pct = ((entry_px - tp_price) / entry_px) * 100.0 if entry_px > 0 else None
    sl_pct = ((sl_price - entry_px) / entry_px) * 100.0 if entry_px > 0 else None
    retest_depth_atr = (
        (float(retest_high) - float(break_level)) / float(atr_now)
        if (break_level is not None and retest_high is not None and atr_now > 0)
        else None
    )

    if update_state:
        state["entry_window_start"] = entry_window_start
        state["break_level"] = break_level
        state["retest_high"] = retest_high
        state["retest_wait"] = retest_wait
        state["retest_touch_high"] = retest_touch_high

    return {
        "entry_px": entry_px,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "entry_type": entry_type,
        "entry_ts": ts_ms,
        "retest_depth_atr": retest_depth_atr,
    }, "ok", meta
