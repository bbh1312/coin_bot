"""
스윙 하이/로우(프랙탈)를 탐지합니다.
"""
from typing import List
from ..core.types import Candle, Swing

def detect_swings(candles: List[Candle], n: int = 2) -> List[Swing]:
    """
    캔들 리스트에서 프랙탈 모델을 사용하여 스윙 하이/로우를 탐지합니다.

    - 스윙 하이: 한 캔들의 고가가 좌우 n개 캔들의 고가보다 높은 경우.
    - 스윙 로우: 한 캔들의 저가가 좌우 n개 캔들의 저가보다 낮은 경우.

    Args:
        candles (List[Candle]): 분석할 캔들 데이터 리스트.
        n (int): 스윙을 결정하기 위해 비교할 좌우 캔들의 수. 기본값은 2.

    Returns:
        List[Swing]: 탐지된 스윙 포인트(Swing 객체)의 리스트.
    """
    if not candles or len(candles) < 2 * n + 1:
        return []

    swings: List[Swing] = []
    
    # (2*n + 1) 크기의 윈도우를 사용할 수 있는 캔들만 순회
    # 첫 n개와 마지막 n개 캔들은 기준점이 될 수 없음
    for i in range(n, len(candles) - n):
        
        pivot_candle = candles[i]
        
        # 윈도우 내 다른 캔들과 비교
        window_candles = candles[i-n:i] + candles[i+1:i+n+1]
        
        # 스윙 하이 검사
        pivot_high = pivot_candle.h
        if all(c.h < pivot_high for c in window_candles):
            swings.append(Swing(ts=pivot_candle.ts, price=pivot_high, type='high'))

        # 스윙 로우 검사
        pivot_low = pivot_candle.l
        if all(c.l > pivot_low for c in window_candles):
            swings.append(Swing(ts=pivot_candle.ts, price=pivot_low, type='low'))
            
    return swings
