from dataclasses import dataclass


@dataclass
class PumpFadeConfig:
    tf_trigger: str = "15m"
    tf_confirm: str = "5m"
    lookback_hh: int = 48  # 12h on 15m
    atr_len: int = 14
    ema_fast: int = 7
    ema_mid: int = 20
    ema_slow: int = 60
    ema20_slope_lookback: int = 3
    ema20_slope_atr_mult_max: float = 0.4

    # Pump candidate filters
    pct_6h_min: float = 6.0
    pct_24h_min: float = 12.0
    range_3h_min: float = 1.8
    vol_1h_mult: float = 1.5
    pump_min_hits: int = 2
    vol_peak_lookback: int = 24

    # Regime/chase filter
    dist_to_ema20_atr_mult: float = 1.2

    # Retest zone
    retest_atr_low: float = 0.3
    retest_atr_high: float = 0.6

    # Failure candle
    failure_wick_mult: float = 1.2
    failure_eps: float = 0.0005
    min_body_atr_mult: float = 0.2

    # Confirm
    confirm_use_ema7: bool = True
    confirm_use_low_break: bool = True
    confirm_use_5m: bool = False

    # Volume filter (weakened)
    vol_peak_mult_ok: float = 0.8
    vol_prev_mult_ok: float = 1.1
    vol_peak_block: float = 0.95

    # RSI / MACD filters
    rsi_len: int = 14
    rsi_turn_required: bool = True
    rsi_strong_threshold: float = 55.0
    rsi_strong_required: bool = False
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_block_increasing: bool = True

    # Entry
    entry_timeout_bars: int = 2  # 2 bars on 15m

    # Cooldown
    cooldown_bars: int = 8  # 2h on 15m
