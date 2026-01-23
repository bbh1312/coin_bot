"""
유동성 스윕(Liquidity Sweep) 이벤트를 탐지합니다.
"""
from typing import Optional, Dict, Any, List
from ..core.types import Candle, Swing

def check_sell_side_sweep(
    candle: Candle, 
    last_swing_low: Swing, 
    atr: float, 
    max_depth_atr_mult: float,
    strongbreakout_deadband_atr_mult: float,
    tick_size: float, 
    tick_size_multiplier: int, 
    body_ratio_min: float,
    recent_candles: List[Candle] # Added
) -> Optional[Dict[str, Any]]:
    """
    매도측 유동성 스윕(Sell-Side Liquidity Sweep)을 확인합니다. (롱 포지션 진입 근거)
    
    캔들의 저가가 직전 스윙 로우를 하방으로 찌르고, 
    몸통은 스윙 로우 위에서 마감하는 경우를 유효한 스윕으로 간주합니다.

    Args:
        candle: 현재 확인하려는 캔들.
        last_swing_low: 가장 최근에 확정된 스윙 로우.
        atr: 현재 변동성(ATR) 값.
        max_depth_atr_mult: 허용되는 최대 스윕 깊이 (ATR 배수).
        strongbreakout_deadband_atr_mult: strong breakout 판정을 위한 deadband ATR 배수.
        tick_size: 심볼의 최소 틱 사이즈.
        tick_size_multiplier: deadband 계산에 사용되는 틱 사이즈 승수.
        body_ratio_min: strong breakout 판정을 위한 최소 몸통 비율.
        recent_candles: 최근 캔들 리스트 (strong3 판정에 사용).

    Returns:
        스윕이 탐지된 경우, 관련 정보를 담은 딕셔너리를 반환합니다. 그렇지 않으면 None을 반환합니다.
    """
    # 캔들 저가가 스윙 로우보다 높거나 같으면 스윕이 아님
    if candle.l >= last_swing_low.price:
        return None

    sweep_depth = last_swing_low.price - candle.l
    
    # 스윕 깊이가 ATR 기반 허용치를 초과하면 유효하지 않음
    if atr > 0 and sweep_depth > atr * max_depth_atr_mult:
        return None

    # Calculate deadband
    deadband_tick = tick_size * tick_size_multiplier
    deadband_atr = atr * strongbreakout_deadband_atr_mult
    if deadband_tick >= deadband_atr:
        deadband_value = deadband_tick
        deadband_source = "tick_size_clamp"
    else:
        deadband_value = deadband_atr
        deadband_source = "atr_based"

    strong1 = candle.c < (last_swing_low.price - deadband_value)
    body = abs(candle.c - candle.o)
    candle_range = abs(candle.h - candle.l)
    eps = 1e-9 # Small epsilon to prevent division by zero
    strong2 = (body / max(candle_range, eps)) >= body_ratio_min

    # strong3: 3 consecutive closes below level (for long)
    consec_closes_below_deadband = 0
    # recent_candles should at least contain the current candle (index -1) and previous (index -2)
    if len(recent_candles) >= 3:
        if recent_candles[-1].c < (last_swing_low.price - deadband_value):
            consec_closes_below_deadband += 1
        if recent_candles[-2].c < (last_swing_low.price - deadband_value):
            consec_closes_below_deadband += 1
        if recent_candles[-3].c < (last_swing_low.price - deadband_value):
            consec_closes_below_deadband += 1

    strong3 = consec_closes_below_deadband >= 3

    is_strong_breakout = strong1 and (strong2 or strong3) # Modified
    deadband_line = last_swing_low.price - deadband_value
    close_vs_deadband = candle.c - deadband_line
    body_ratio = body / max(candle_range, eps)
    
    return {
        "type": "sell-side",
        "ref_swing_ts": last_swing_low.ts,
        "ref_swing_price": last_swing_low.price,
        "sweep_depth": sweep_depth,
        "sweep_candle_low": candle.l,
        "is_strong_breakout": is_strong_breakout,
        "sweep_candle_close": candle.c,
        "deadband": {
            "value": deadband_value,
            "source": deadband_source,
            "tick_size": tick_size,
            "atr_based": deadband_atr,
        },
        "strong1": strong1, # For debugging
        "strong2": strong2, # For debugging
        "strong3": strong3, # For debugging
        "consec_closes_below_deadband": consec_closes_below_deadband, # For debugging
        "body": body,
        "range": candle_range,
        "body_ratio": body_ratio,
        "deadband_line": deadband_line,
        "close_vs_deadband": close_vs_deadband,
    }

def check_buy_side_sweep(
    candle: Candle, 
    last_swing_high: Swing, 
    atr: float, 
    max_depth_atr_mult: float,
    strongbreakout_deadband_atr_mult: float,
    tick_size: float,
    tick_size_multiplier: int,
    body_ratio_min: float,
    recent_candles: List[Candle] # Added
) -> Optional[Dict[str, Any]]:
    """
    매수측 유동성 스윕(Buy-Side Liquidity Sweep)을 확인합니다. (숏 포지션 진입 근거)

    캔들의 고가가 직전 스윙 하이를 상방으로 찌르고,
    몸통은 스윙 하이 아래에서 마감하는 경우를 유효한 스윕으로 간주합니다.
    
    Args:
        candle: 현재 확인하려는 캔들.
        last_swing_high: 가장 최근에 확정된 스윙 하이.
        atr: 현재 변동성(ATR) 값.
        max_depth_atr_mult: 허용되는 최대 스윕 깊이 (ATR 배수).
        strongbreakout_deadband_atr_mult: strong breakout 판정을 위한 deadband ATR 배수.
        tick_size: 심볼의 최소 틱 사이즈.
        tick_size_multiplier: deadband 계산에 사용되는 틱 사이즈 승수.
        body_ratio_min: strong breakout 판정을 위한 최소 몸통 비율.
        recent_candles: 최근 캔들 리스트 (strong3 판정에 사용).

    Returns:
        스윕이 탐지된 경우, 관련 정보를 담은 딕셔너리를 반환합니다. 그렇지 않으면 None을 반환합니다.
    """
    # 캔들 고가가 스윙 하이보다 낮거나 같으면 스윕이 아님
    if candle.h <= last_swing_high.price:
        return None

    sweep_depth = candle.h - last_swing_high.price
    
    # 스윕 깊이가 ATR 기반 허용치를 초과하면 유효하지 않음
    if atr > 0 and sweep_depth > atr * max_depth_atr_mult:
        return None

    # Calculate deadband
    deadband_tick = tick_size * tick_size_multiplier
    deadband_atr = atr * strongbreakout_deadband_atr_mult
    if deadband_tick >= deadband_atr:
        deadband_value = deadband_tick
        deadband_source = "tick_size_clamp"
    else:
        deadband_value = deadband_atr
        deadband_source = "atr_based"

    strong1 = candle.c > (last_swing_high.price + deadband_value)
    body = abs(candle.c - candle.o)
    candle_range = abs(candle.h - candle.l)
    eps = 1e-9 # Small epsilon to prevent division by zero
    strong2 = (body / max(candle_range, eps)) >= body_ratio_min

    # strong3: 3 consecutive closes above level (for short)
    consec_closes_above_deadband = 0
    if len(recent_candles) >= 3:
        if recent_candles[-1].c > (last_swing_high.price + deadband_value):
            consec_closes_above_deadband += 1
        if recent_candles[-2].c > (last_swing_high.price + deadband_value):
            consec_closes_above_deadband += 1
        if recent_candles[-3].c > (last_swing_high.price + deadband_value):
            consec_closes_above_deadband += 1

    strong3 = consec_closes_above_deadband >= 3

    is_strong_breakout = strong1 and (strong2 or strong3) # Modified
    deadband_line = last_swing_high.price + deadband_value
    close_vs_deadband = candle.c - deadband_line
    body_ratio = body / max(candle_range, eps)

    return {
        "type": "buy-side",
        "ref_swing_ts": last_swing_high.ts,
        "ref_swing_price": last_swing_high.price,
        "sweep_depth": sweep_depth,
        "sweep_candle_high": candle.h,
        "is_strong_breakout": is_strong_breakout,
        "sweep_candle_close": candle.c,
        "deadband": {
            "value": deadband_value,
            "source": deadband_source,
            "tick_size": tick_size,
            "atr_based": deadband_atr,
        },
        "strong1": strong1, # For debugging
        "strong2": strong2, # For debugging
        "strong3": strong3, # For debugging
        "consec_closes_above_deadband": consec_closes_above_deadband, # For debugging
        "body": body,
        "range": candle_range,
        "body_ratio": body_ratio,
        "deadband_line": deadband_line,
        "close_vs_deadband": close_vs_deadband,
    }
