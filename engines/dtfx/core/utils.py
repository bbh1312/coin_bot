"""
ATR, 프랙탈, 세션 등 기술적 분석에 필요한 유틸리티 함수들을 포함합니다.
"""
from typing import List, Tuple, Dict, Any, Optional # Added Dict, Any
import numpy as np
from ..core.types import Candle

def calculate_atr(candles: List[Candle], period: int = 14) -> float:
    """
    주어진 기간(period)에 대한 평균 실제 범위(ATR)를 계산합니다.
    ATR은 시장 변동성을 측정하는 지표입니다.

    참고: ATR 계산은 일반적으로 지수이동평균(EMA)의 변형인 와일더 평활법(Wilder's smoothing)을
    사용하지만, 여기서는 구현의 단순성을 위해 단순이동평균(SMA)을 사용합니다.
    이는 대부분의 목적에 충분한 근사치를 제공합니다.

    Args:
        candles (List[Candle]): ATR을 계산할 캔들 데이터 리스트.
        period (int): ATR 계산 기간. 기본값은 14.

    Returns:
        float: 계산된 ATR 값.
    """
    if len(candles) < 2:
        return 0.0

    true_ranges = []
    for i in range(1, len(candles)):
        high = candles[i].h
        low = candles[i].l
        prev_close = candles[i-1].c
        
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    
    if not true_ranges:
        return 0.0
    
    # ATR 계산을 위해 최근 'period' 개의 TR 값만 사용
    actual_period = min(len(true_ranges), period)
    if actual_period == 0:
        return 0.0
        
    return np.mean(true_ranges[-actual_period:])

def get_tick_size(market: Dict[str, Any]) -> Tuple[float, str]:
    """
    CCXT 마켓 정보에서 심볼의 실제 틱 사이즈를 추정합니다.
    우선순위: 거래소 정보 -> precision.price (정밀도 자릿수) -> fallback.

    Args:
        market (Dict[str, Any]): CCXT exchange.markets[symbol] 형태의 마켓 정보 딕셔너리.

    Returns:
        Tuple[float, str]: 틱 사이즈와 틱 사이즈 출처 문자열. 틱 사이즈를 찾지 못하면 (None, "fallback_none") 반환.
    """
    tick = None
    source = "fallback_none"

    # 1) exchange-provided tickSize (if present)
    info = market.get("info") or {}
    for k in ["tickSize", "priceTick", "price_tick", "minPrice"]:
        if k in info:
            try:
                tick = float(info[k])
                source = f"info.{k}"
                break
            except (ValueError, TypeError):
                continue

    # 2) derive from precision digits
    if not tick:
        p = (market.get("precision") or {}).get("price")
        if isinstance(p, int):
            tick = 10 ** (-p)
            source = "precision.digits"
        elif isinstance(p, float) and p > 0 and p < 1:
            # some exchanges may store tick-like value here
            tick = p
            source = "precision.price_float"
    
    # If tick is still None after all attempts, default to a very small value to prevent division by zero or errors
    # For now, if None, let's explicitly return None
    if tick is None:
        return (None, "fallback_none")
        
    return (tick, source)


def calculate_ema(candles: List[Candle], period: int) -> List[Optional[float]]:
    if period <= 0:
        return []
    if len(candles) < period:
        return [None for _ in candles]
    closes = [c.c for c in candles]
    ema: List[Optional[float]] = [None for _ in candles]
    sma = float(np.mean(closes[:period]))
    ema[period - 1] = sma
    alpha = 2.0 / (period + 1.0)
    prev = sma
    for i in range(period, len(closes)):
        prev = (closes[i] - prev) * alpha + prev
        ema[i] = prev
    return ema
