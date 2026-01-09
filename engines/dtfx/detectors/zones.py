"""
되돌림 진입을 위한 존(OB, FVG, OTE)을 탐지합니다.
"""
from typing import Optional, List, Dict, Any
from ..core.types import Candle
from ..core.utils import calculate_atr

def find_order_block(
    candles: List[Candle],
    mss_candle_idx: int,
    direction: str,  # 'up' 또는 'down'
    max_zone_height_atr_mult: float
) -> Optional[Dict[str, Any]]:
    """
    시장 구조 변화(MSS) 직전의 오더블록(OB)을 찾습니다.

    - 상방 구조 변화(direction='up')의 경우: MSS 직전 마지막 음봉.
    - 하방 구조 변화(direction='down')의 경우: MSS 직전 마지막 양봉.
    """
    if mss_candle_idx < 1:
        return None

    ob_candle = None
    # MSS 확정 캔들 바로 이전부터 역순으로 탐색
    search_range = range(mss_candle_idx - 1, -1, -1)

    if direction == 'up':  # 강세 OB (마지막 음봉) 찾기
        for i in search_range:
            if candles[i].c < candles[i].o:
                ob_candle = candles[i]
                break
    elif direction == 'down':  # 약세 OB (마지막 양봉) 찾기
        for i in search_range:
            if candles[i].c > candles[i].o:
                ob_candle = candles[i]
                break
    
    if not ob_candle:
        return None

    zone_low = min(ob_candle.o, ob_candle.c)
    zone_high = max(ob_candle.o, ob_candle.c)
    zone_height = zone_high - zone_low
    atr = calculate_atr(candles, period=14)

    # 필터: 존의 높이가 너무 크면 유효하지 않음
    if atr > 0 and zone_height > atr * max_zone_height_atr_mult:
        return None

    return {
        "type": "OB",
        "lo": zone_low,
        "hi": zone_high,
        "ref_candle_ts": ob_candle.ts,
    }

def find_fair_value_gaps(candles: List[Candle]) -> List[Dict[str, Any]]:
    """
    주어진 캔들 리스트 내에서 모든 공정가치갭(FVG)을 찾습니다.
    FVG는 3개의 캔들 패턴으로 식별됩니다.
    """
    if len(candles) < 3:
        return []
    
    fvgs = []
    for i in range(len(candles) - 2):
        c1, c2, c3 = candles[i], candles[i+1], candles[i+2]

        # 강세 FVG: 1번 캔들 고가 < 3번 캔들 저가
        if c1.h < c3.l:
            fvgs.append({
                "type": "FVG",
                "dir": "up",
                "lo": c1.h,
                "hi": c3.l,
                "ref_candle_ts": c2.ts,
            })
        
        # 약세 FVG: 1번 캔들 저가 > 3번 캔들 고가
        if c1.l > c3.h:
            fvgs.append({
                "type": "FVG",
                "dir": "down",
                "lo": c3.h,
                "hi": c1.l,
                "ref_candle_ts": c2.ts,
            })
    return fvgs

def calculate_ote(
    start_price: float,
    end_price: float
) -> Dict[str, Any]:
    """
    최적 거래 진입(OTE) 구간을 계산합니다. (피보나치 0.618-0.786 되돌림)
    """
    trade_range = abs(end_price - start_price)
    
    if end_price > start_price:  # 상승 파동
        ote_low = end_price - trade_range * 0.786
        ote_high = end_price - trade_range * 0.618
        direction = 'up'
    else:  # 하락 파동
        ote_low = end_price + trade_range * 0.618
        ote_high = end_price + trade_range * 0.786
        direction = 'down'

    return {
        "type": "OTE",
        "dir": direction,
        "lo": ote_low,
        "hi": ote_high,
    }
