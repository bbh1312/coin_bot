from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class ShortBend15m3mConfig:
    tf_htf: str = "15m"
    tf_ltf: str = "3m"
    tf_regime: str = "1h"
    lookback_15m: int = 70

    # 롤링 30봉 상승률(%)
    rise_min_pct: float = 8.0
    # 고점 상승 비율(최근 lookback-1 구간 중 high[i] > high[i-1] 비율)
    hh_ratio_min: float = 0.35
    # 15m 고점권 판별: 롤링 구간 상단 몇 % 영역에 위치해야 하는지
    htf_top_zone_ratio: float = 0.65
    htf_resistance_require: bool = False
    htf_resistance_touch_tol_pct: float = 0.004
    htf_resistance_reject_min_pct: float = 0.002
    htf_dev_min_pct: float = -2.0
    use_1h_regime: bool = False
    lookback_1h: int = 48
    h1_use_rise_filter: bool = True
    h1_rise_min_pct: float = 12.0
    h1_top_zone_ratio: float = 0.35
    h1_resistance_touch_tol_pct: float = 0.006
    h1_resistance_reject_min_pct: float = 0.002
    h1_use_dev_filter: bool = True
    h1_dev_min_pct: float = 0.5
    h1_bend_require: bool = False
    h1_ema_len: int = 60

    # 정배열
    ema_fast_len: int = 20
    ema_mid_len: int = 60
    ema_slow_len: int = 140

    # 15m 꺾임 조건
    bend_need_close_down: bool = True
    htf_require_vol_confirm: bool = True
    htf_vol_mult_min: float = 1.0
    htf_vol_spike_lookback: int = 10
    htf_vol_spike_mult: float = 1.00
    bend_atr_len: int = 14
    bend_min_drop_atr: float = 0.30
    bend_min_body_ratio: float = 0.50
    bend_require_prev_low_break: bool = True
    bend_two_step_enable: bool = False
    bend_use_upper_wick_filter: bool = False
    bend_min_upper_wick_ratio: float = 0.40
    bend_use_close_location_filter: bool = False
    bend_max_close_pos_ratio: float = 0.40

    # 3m 확인
    armed_bars_3m: int = 10
    ltf_ema_len: int = 20
    swing_lookback_3m: int = 5
    require_vol_confirm: bool = True
    vol_mult_min: float = 1.0
    ltf_two_step_confirm: bool = True
    ltf_retest_enable: bool = True
    ltf_retest_bars: int = 3
    ltf_retest_min_bars: int = 1
    ltf_retest_tol_atr_mult: float = 0.20
    retest_score_min: int = 1
    retest_use_weighted_score: bool = True
    retest_score_min_float: float = 1.0
    retest_score_use_resistance: bool = True
    retest_score_use_volume: bool = True
    retest_score_use_reject_strength: bool = True
    retest_score_vol_mult_min: float = 1.1
    retest_score_reject_atr_mult: float = 0.10
    retest_score_weight_resistance: float = 1.0
    retest_score_weight_volume: float = 0.7
    retest_score_weight_reject: float = 1.0
    # 3m 주도 진입: 신호 후 역모멘텀(반등) 캔들 출현까지 대기 후 숏 진입
    ltf_wait_counter_momo: bool = False
    ltf_counter_momo_bars: int = 2
    ltf_resistance_require: bool = False
    ltf_resistance_as_gate: bool = False
    ltf_resistance_lookback: int = 20
    ltf_resistance_touch_bars: int = 6
    ltf_resistance_touch_tol_atr_mult: float = 0.15
    ltf_resistance_reject_min_pct: float = 0.001

    # 리스크
    sl_min_pct: float = 0.006
    sl_max_pct: float = 0.040
    sl_floor_atr_mult: float = 0.80
    sl_use_entry_floor: bool = False
    sl_use_retest_high: bool = True
    sl_retest_buffer_atr_mult: float = 0.20
    tp_min_pct: float = 0.004
    tp_max_pct: float = 0.050
    rr_min: float = 1.5
    bend_sl_buffer_pct: float = 0.001

    # 체결/판정
    use_confirmed: bool = True
    sl_first: bool = True


class ShortBend15m3mEngine(BaseEngine):
    name = "short_bend_15m3m"

    def __init__(self, config: ShortBend15m3mConfig | None = None) -> None:
        self.config = config or ShortBend15m3mConfig()
