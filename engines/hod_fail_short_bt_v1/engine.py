from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class HodFailShortBtV1Config:
    # timeframes
    tf_env: str = "15m"
    tf_entry: str = "3m"

    # session (KST)
    session_start_hour_kst: int = 9

    # ENV params
    hod_fail_min: int = 6
    near_hod_band: float = 0.0015

    rsi_len: int = 14
    rsi_sma_len: int = 5
    ema20_len: int = 20
    ema120_len: int = 120

    obv_ema_len: int = 20
    vol_sma_len: int = 20
    mfi_len: int = 14

    # BLOCK params
    ext_ema7_max: float = 0.007
    ema120_floor: float = 1.000
    vol_expand_min: float = 1.1
    touch_band: float = 0.0015
    touch_break: float = 0.0003
    touch_max: int = 4

    # ENTRY params
    retest_band: float = 0.0010
    retest_break: float = 0.0003
    entry_break_low_len: int = 5
    use_break_low: bool = False

    # RISK
    sl_atr_mult: float = 0.3
    tp_r1: float = 1.0
    tp_r2: float = 2.0

    # ENTRY window / wash
    atr_len_entry: int = 14
    ema7_len: int = 7
    ema20_len_entry: int = 20
    wash_atr_mult: float = 1.25
    vol_spike_mult: float = 1.3


BLOCK_UPTREND_15M = "UPTREND_15M"
BLOCK_EXTENSION_3M = "EXTENSION_3M"
BLOCK_BELOW_EMA120_15M = "BELOW_EMA120_15M"
BLOCK_VOLUME_EXPAND_15M = "VOLUME_EXPAND_15M"
BLOCK_HOD_FATIGUE = "HOD_FATIGUE"
BLOCK_BREAKOUT_3M = "BREAKOUT_3M"
BLOCK_MFI_HOT = "MFI_HOT_15M"


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0.0))
    return (volume * direction).cumsum()


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


def upper_wick_ratio(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    open_ = float(row["open"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    upper = high - max(open_, close)
    return upper / rng


def close_pos(row: pd.Series) -> float:
    high = float(row["high"])
    low = float(row["low"])
    close = float(row["close"])
    rng = max(high - low, 1e-9)
    return (close - low) / rng


def map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def session_day_start_kst(ts_ms: int, hour: int = 9) -> int:
    # KST = UTC+9
    kst_ms = ts_ms + 9 * 3600 * 1000
    dt = pd.to_datetime(kst_ms, unit="ms", utc=True)
    dt = dt.tz_convert("Asia/Seoul")
    day = dt.date()
    start = pd.Timestamp(year=day.year, month=day.month, day=day.day, hour=hour, tz="Asia/Seoul")
    if dt < start:
        start = start - pd.Timedelta(days=1)
    return int(start.tz_convert("UTC").timestamp() * 1000)


def build_env_state(
    df_15m: pd.DataFrame,
    cfg: HodFailShortBtV1Config,
) -> dict:
    close = df_15m["close"].astype(float)
    high = df_15m["high"].astype(float)
    low = df_15m["low"].astype(float)
    open_ = df_15m["open"].astype(float)
    volume = df_15m["volume"].astype(float)

    rsi_15m = rsi(close, cfg.rsi_len)
    rsi_sma = rsi_15m.rolling(cfg.rsi_sma_len).mean()

    ema20 = ema(close, cfg.ema20_len)
    ema120 = ema(close, cfg.ema120_len)

    obv_15m = obv(close, volume)
    obv_ema = ema(obv_15m, cfg.obv_ema_len)
    obv_slope = obv_ema.diff().fillna(0.0)

    vol_sma = volume.rolling(cfg.vol_sma_len).mean()
    vol_ratio = volume / vol_sma.replace(0.0, np.nan)
    mfi_15m = mfi(df_15m, cfg.mfi_len)

    return {
        "rsi": rsi_15m,
        "rsi_sma": rsi_sma,
        "ema20": ema20,
        "ema120": ema120,
        "obv_slope": obv_slope,
        "vol_ratio": vol_ratio,
        "mfi": mfi_15m,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }


def build_entry_state(df_3m: pd.DataFrame, cfg: HodFailShortBtV1Config) -> dict:
    close = df_3m["close"].astype(float)
    high = df_3m["high"].astype(float)
    low = df_3m["low"].astype(float)
    open_ = df_3m["open"].astype(float)

    ema7 = ema(close, cfg.ema7_len)
    ema20 = ema(close, cfg.ema20_len_entry)
    atr_3m = atr(df_3m, cfg.atr_len_entry)

    return {
        "ema7": ema7,
        "ema20": ema20,
        "atr": atr_3m,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }
