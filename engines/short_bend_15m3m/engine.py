from __future__ import annotations

from dataclasses import dataclass

from engines.base import BaseEngine


@dataclass
class ShortBend15m3mConfig:
    tf_htf: str = "15m"
    tf_ltf: str = "3m"
    lookback_15m: int = 120

    # 롤링 30봉 상승률(%)
    rise_min_pct: float = 18.0
    # 고점 상승 비율(최근 lookback-1 구간 중 high[i] > high[i-1] 비율)
    hh_ratio_min: float = 0.55
    # 15m 고점권 판별: 롤링 구간 상단 몇 % 영역에 위치해야 하는지
    htf_top_zone_ratio: float = 0.30

    # 정배열
    ema_fast_len: int = 20
    ema_mid_len: int = 60
    ema_slow_len: int = 140

    # 15m 꺾임 조건
    bend_need_close_down: bool = True
    htf_require_vol_confirm: bool = True
    htf_vol_mult_min: float = 1.2
    htf_vol_spike_lookback: int = 10
    htf_vol_spike_mult: float = 1.00
    bend_atr_len: int = 14
    bend_min_drop_atr: float = 0.30
    bend_min_body_ratio: float = 0.50
    bend_require_prev_low_break: bool = True

    # 3m 확인
    armed_bars_3m: int = 4
    ltf_ema_len: int = 20
    swing_lookback_3m: int = 10
    require_vol_confirm: bool = True
    vol_mult_min: float = 1.1
    ltf_two_step_confirm: bool = True
    ltf_retest_enable: bool = False
    ltf_retest_bars: int = 3
    ltf_retest_tol_atr_mult: float = 0.20
    # 3m 주도 진입: 신호 후 역모멘텀(반등) 캔들 출현까지 대기 후 숏 진입
    ltf_wait_counter_momo: bool = False
    ltf_counter_momo_bars: int = 2

    # 리스크
    sl_min_pct: float = 0.006
    sl_max_pct: float = 0.040
    sl_floor_atr_mult: float = 0.80
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
