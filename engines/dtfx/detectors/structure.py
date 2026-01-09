"""
시장 구조 변화(MSS/BOS)를 탐지합니다.
"""
from typing import Optional, List, Dict, Any
import numpy as np
from ..core.types import Candle, Swing
from ..core.utils import calculate_atr

def check_mss_up(
    candle: Candle,
    break_level_swing: Swing,
    candles: List[Candle],
    min_body_atr_mult: float,
    use_vol_filter: bool = False,
    vol_sma_period: int = 20,
    vol_mult_min: float = 1.1
) -> Optional[Dict[str, Any]]:
    """
    상방 시장 구조 변화(MSS/BOS)를 확인합니다.

    지정된 돌파 레벨(break_level_swing)을 캔들 종가가 상향 돌파했는지,
    그리고 필터 조건(캔들 몸통 크기, 거래량)을 만족하는지 확인합니다.

    Args:
        candle: 현재 확인하려는 캔들.
        break_level_swing: 돌파해야 할 단기 스윙 하이.
        candles: ATR 및 거래량 필터 계산에 필요한 최근 캔들 리스트.
        min_body_atr_mult: 최소 캔들 몸통 크기 (ATR 배수).
        use_vol_filter: 거래량 필터 사용 여부.
        vol_sma_period: 거래량 이동평균 계산 기간.
        vol_mult_min: 최소 거래량 조건 (이동평균 대비 배수).

    Returns:
        상방 구조 변화가 확인되면 관련 정보를 담은 딕셔너리를, 그렇지 않으면 None을 반환합니다.
    """
    if break_level_swing.type != 'high':
        return None

    # 조건 1: 캔들 종가가 스윙 하이 레벨 위로 돌파해야 함
    if candle.c <= break_level_swing.price:
        return None

    # 필터 1: 캔들 몸통 크기
    body_size = abs(candle.c - candle.o)
    atr = calculate_atr(candles, period=14)
    if atr > 0 and body_size < atr * min_body_atr_mult:
        return None  # 몸통 크기가 너무 작아 약한 돌파로 간주

    # 필터 2: 거래량 (선택 사항)
    if use_vol_filter:
        if len(candles) < vol_sma_period:
            return None  # 거래량 이평을 계산하기에 데이터가 부족
        recent_volumes = [c.v for c in candles[-vol_sma_period:]]
        volume_sma = np.mean(recent_volumes) if recent_volumes else 0
        if volume_sma > 0 and candle.v < volume_sma * vol_mult_min:
            return None  # 거래량이 충분하지 않음

    return {
        "dir": "up",
        "break_level": break_level_swing.price,
        "ref_swing_ts": break_level_swing.ts,
        "confirm_candle_ts": candle.ts,
    }

def check_mss_down(
    candle: Candle,
    break_level_swing: Swing,
    candles: List[Candle],
    min_body_atr_mult: float,
    use_vol_filter: bool = False,
    vol_sma_period: int = 20,
    vol_mult_min: float = 1.1
) -> Optional[Dict[str, Any]]:
    """
    하방 시장 구조 변화(MSS/BOS)를 확인합니다.

    지정된 돌파 레벨(break_level_swing)을 캔들 종가가 하향 돌파했는지,
    그리고 필터 조건을 만족하는지 확인합니다.
    """
    if break_level_swing.type != 'low':
        return None

    # 조건 1: 캔들 종가가 스윙 로우 레벨 아래로 돌파해야 함
    if candle.c >= break_level_swing.price:
        return None

    # 필터 1: 캔들 몸통 크기
    body_size = abs(candle.c - candle.o)
    atr = calculate_atr(candles, period=14)
    if atr > 0 and body_size < atr * min_body_atr_mult:
        return None

    # 필터 2: 거래량 (선택 사항)
    if use_vol_filter:
        if len(candles) < vol_sma_period:
            return None
        recent_volumes = [c.v for c in candles[-vol_sma_period:]]
        volume_sma = np.mean(recent_volumes) if recent_volumes else 0
        if volume_sma > 0 and candle.v < volume_sma * vol_mult_min:
            return None

    return {
        "dir": "down",
        "break_level": break_level_swing.price,
        "ref_swing_ts": break_level_swing.ts,
        "confirm_candle_ts": candle.ts,
    }
