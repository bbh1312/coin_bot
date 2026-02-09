from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from engines.base import BaseEngine


@dataclass
class HodFailShortV1Config:
    tf_env: str = "15m"
    tf_entry: str = "3m"

    # ENV
    hod_fail_min: int = 6
    near_hod_band: float = 0.001
    mfi_len: int = 14
    mfi_hot: float = 52.0
    mfi_hot_rise: bool = True

    # PATH_B
    pathb_hod_drop: float = 0.010
    pathb_ema_fail_k: int = 2
    lower_high_delta: float = 0.001
    lower_high_window: int = 8

    # Uptrend block
    block_uptrend: bool = True
    uptrend_min_flags: int = 2
    uptrend_consec_bull: int = 3
    ema20_slope_mult: float = 0.0002
    uptrend_use_consec: bool = True
    uptrend_use_hhhl: bool = True
    uptrend_use_ema20: bool = True

    # Entry
    retest_band: float = 0.0035
    retest_break: float = 0.001
    retrace_atr: float = 0.20

    # Indicators
    ema7_len: int = 7
    ema20_len: int = 20
    atr_len: int = 14

    # Risk
    sl_atr_mult: float = 0.4
    tp_r1: float = 1.2
    tp_r2: float = 1.5


class HodFailShortV1Engine(BaseEngine):
    name = "hod_fail_short_v1"

    def __init__(self, config: HodFailShortV1Config | None = None) -> None:
        self.config = config or HodFailShortV1Config()


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def mfi(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    typical = (high + low + close) / 3.0
    raw_flow = typical * volume
    prev_typical = typical.shift(1)
    pos_flow = raw_flow.where(typical > prev_typical, 0.0)
    neg_flow = raw_flow.where(typical < prev_typical, 0.0)
    pos_sum = pos_flow.rolling(length).sum()
    neg_sum = neg_flow.rolling(length).sum()
    money_ratio = pos_sum / neg_sum.replace(0.0, np.nan)
    out = 100 - (100 / (1 + money_ratio))
    return out.fillna(50.0)


def session_day_start_kst(ts_ms: int, hour: int = 9) -> int:
    # KST = UTC+9
    kst_ms = ts_ms + 9 * 3600 * 1000
    dt = pd.to_datetime(kst_ms, unit="ms", utc=True).tz_convert("Asia/Seoul")
    day = dt.date()
    start = pd.Timestamp(year=day.year, month=day.month, day=day.day, hour=hour, tz="Asia/Seoul")
    if dt < start:
        start = start - pd.Timedelta(days=1)
    return int(start.tz_convert("UTC").timestamp() * 1000)


def _compute_pivot_highs(highs: pd.Series) -> list[Tuple[int, float]]:
    pivots: list[Tuple[int, float]] = []
    for i in range(2, len(highs) - 2):
        h0 = float(highs.iloc[i])
        if h0 > float(highs.iloc[i - 1]) and h0 > float(highs.iloc[i - 2]) and h0 >= float(highs.iloc[i + 1]) and h0 >= float(highs.iloc[i + 2]):
            pivots.append((i, h0))
    return pivots


def hod_fail_short_entry_signal(
    df_3m: pd.DataFrame,
    df_15m: pd.DataFrame,
    cfg: HodFailShortV1Config,
    use_confirmed: bool = True,
) -> Tuple[Optional[dict], Optional[str]]:
    if df_3m.empty or df_15m.empty:
        return None, "no_data"

    df_3m_sig = df_3m.iloc[:-1] if use_confirmed else df_3m
    df_15m_sig = df_15m.iloc[:-1] if use_confirmed else df_15m

    if len(df_3m_sig) < max(cfg.ema20_len + 5, cfg.atr_len + 5, 30):
        return None, "no_data"
    if len(df_15m_sig) < max(cfg.ema20_len + 5, 60):
        return None, "no_data"

    last_15m_ts = int(df_15m_sig["ts"].iloc[-1])
    sess_start = session_day_start_kst(last_15m_ts)
    session_df = df_15m_sig[df_15m_sig["ts"] >= sess_start]
    if session_df.empty:
        return None, "no_session"

    highs = session_df["high"].astype(float)
    hod = float(highs.max())
    last_hod_idx = int(highs[highs == hod].index[-1])
    last_idx = session_df.index[-1]
    hod_fail_count = int(last_idx - last_hod_idx)
    if hod_fail_count < cfg.hod_fail_min:
        return None, "hod_fail_min"

    # mfi filter
    mfi_series = mfi(df_15m_sig, cfg.mfi_len)
    mfi_now = float(mfi_series.iloc[-1])
    mfi_prev = float(mfi_series.iloc[-2]) if len(mfi_series) >= 2 else mfi_now
    if mfi_now >= cfg.mfi_hot and (not cfg.mfi_hot_rise or mfi_now > mfi_prev):
        return None, "mfi_hot"

    # uptrend block
    if cfg.block_uptrend:
        close_15m = session_df["close"].astype(float)
        open_15m = session_df["open"].astype(float)
        consec = 0
        for j in range(len(close_15m) - 1, -1, -1):
            if float(close_15m.iloc[j]) > float(open_15m.iloc[j]):
                consec += 1
                if consec >= cfg.uptrend_consec_bull:
                    break
            else:
                break
        consecutive_bull = consec >= cfg.uptrend_consec_bull
        hhhl = False
        if len(highs) >= 2:
            h = float(highs.iloc[-1])
            l = float(session_df["low"].iloc[-1])
            prev_h = float(highs.iloc[-2])
            prev_l = float(session_df["low"].iloc[-2])
            if h > prev_h and l > prev_l:
                hhhl = True
        ema20 = ema(df_15m_sig["close"].astype(float), cfg.ema20_len)
        ema20_now = float(ema20.iloc[-1])
        ema20_prev = float(ema20.iloc[-2]) if len(ema20) >= 2 else ema20_now
        ema20_slope = ema20_now - ema20_prev
        ema20_slope_thr = cfg.ema20_slope_mult * float(df_15m_sig["close"].iloc[-1])
        close_gt_ema20 = float(df_15m_sig["close"].iloc[-1]) > ema20_now
        ema20_rising = ema20_slope > ema20_slope_thr

        subflags = 0
        if cfg.uptrend_use_consec and consecutive_bull:
            subflags += 1
        if cfg.uptrend_use_hhhl and hhhl:
            subflags += 1
        if cfg.uptrend_use_ema20 and close_gt_ema20 and ema20_rising:
            subflags += 1
        if subflags >= cfg.uptrend_min_flags:
            return None, "uptrend"

    # PATH_B
    last_close_15m = float(df_15m_sig["close"].iloc[-1])
    ctx_ok = last_close_15m <= hod * (1 - cfg.pathb_hod_drop)
    if not ctx_ok:
        return None, "pathb_ctx"

    closes_3m = df_3m_sig["close"].astype(float)
    highs_3m = df_3m_sig["high"].astype(float)
    lows_3m = df_3m_sig["low"].astype(float)
    ema7 = ema(closes_3m, cfg.ema7_len)
    ema20 = ema(closes_3m, cfg.ema20_len)
    atr_3m = atr(df_3m_sig, cfg.atr_len)

    cur_close = float(closes_3m.iloc[-1])
    cur_high = float(highs_3m.iloc[-1])
    cur_ema7 = float(ema7.iloc[-1])
    cur_ema20 = float(ema20.iloc[-1])
    cur_atr = float(atr_3m.iloc[-1]) if not np.isnan(atr_3m.iloc[-1]) else 0.0
    if cur_atr <= 0:
        return None, "atr"

    # EMA recover then fail within K bars
    k = cfg.pathb_ema_fail_k
    recover_seen = False
    lookback = max(1, k)
    for j in range(len(highs_3m) - lookback, len(highs_3m)):
        if j < 0:
            continue
        if float(highs_3m.iloc[j]) > float(ema7.iloc[j]) or float(highs_3m.iloc[j]) > float(ema20.iloc[j]):
            recover_seen = True
            break
    ema_fail = recover_seen and cur_close < cur_ema7

    # lower high via pivots (optional)
    lower_high = False
    pivots = _compute_pivot_highs(highs_3m.iloc[-20:])
    if len(pivots) >= 2:
        (p_idx, p_val), (l_idx, l_val) = pivots[-2], pivots[-1]
        # convert indices to global positions
        base = len(highs_3m) - 20
        p_idx += base
        l_idx += base
        if l_val < p_val * (1 - cfg.lower_high_delta):
            if (len(highs_3m) - 1 - l_idx) <= cfg.lower_high_window and (len(highs_3m) - 1 - p_idx) <= cfg.lower_high_window:
                lower_high = True

    if not (ema_fail or lower_high):
        return None, "pathb_no_trigger"

    entry_px = float(cur_close)
    sl_base = max(cur_high, cur_ema20)
    sl_px = sl_base + cfg.sl_atr_mult * cur_atr
    r_val = sl_px - entry_px
    if r_val <= 0:
        return None, "bad_r"
    tp_px = entry_px - cfg.tp_r1 * r_val
    return {
        "entry_px": entry_px,
        "sl_price": sl_px,
        "tp_price": tp_px,
        "entry_type": "market",
    }, "signal"
