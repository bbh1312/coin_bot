from dataclasses import dataclass


@dataclass
class TopFailShortConfig:
    tf: str = "3m"
    runup_bars: int = 40
    runup_min_pct: float = 4.0
    runup_top_pct: float = 10.0
    touch_lookback: int = 40
    touch_band_pct: float = 0.15
    touch_min: int = 3
    fake_break_lookback: int = 6
    range_avg_lookback: int = 10
    range_mult: float = 1.2
    upper_wick_min: float = 0.45
    close_pos_max: float = 0.35
    entry_mode: str = "break"
    entry_retrace_min: float = 0.382
    entry_retrace_max: float = 0.5
    sl_pad_pct: float = 0.1
    tp1_r: float = 0.5
    tp2_r: float = 1.0
    tp1_frac: float = 0.4
    tp2_frac: float = 0.3
    max_retries: int = 2
    cooldown_bars: int = 5
