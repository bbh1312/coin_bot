from typing import Dict, Any, List, Optional, Set, Tuple
import hashlib
import ccxt

from .core.types import Candle, Signal, DTFXState
from .core.logger import get_logger
from .detectors import swings, liquidity, structure, zones
from .core.utils import calculate_atr, get_tick_size
from .config import ATR_PERIOD, ZONE_PIERCE_ATR_MULT, STRONGBREAKOUT_DEADBAND_ATR_MULT, TICK_SIZE_MULTIPLIER, BODY_RATIO_MIN, MAJOR_PCT_WEIGHT, ZONE_TOO_WIDE_ATR_MULT, ZONE_TOO_FAR_ATR_MULT, WICK_RATIO_MIN

class DTFXLongEngine:
    """
    DTFX 롱 전략의 상태를 관리하고, 캔들 데이터를 기반으로 상태 전환을 처리합니다.
    (유동성 스윕 -> 구조 전환 -> 되돌림 -> 진입)
    """
    def __init__(self, params: Dict[str, Any], tfs: Dict[str, str], exchange: ccxt.Exchange): # Added exchange
        self.params = params
        self.tfs = tfs
        self.exchange = exchange # Added
        self.logger = get_logger()
        self.states: Dict[str, Dict[str, Any]] = {}
        self.symbol_roles: Dict[str, str] = {}
        self._unknown_state_emitted: Set[Tuple[str, DTFXState]] = set() # Added
        self.params.setdefault("timeout_sweep_to_mss", 12)
        self.params.setdefault("timeout_mss_to_touch", 20)
        self.params.setdefault("mss_body_atr_min", 0.2)
        self.params.setdefault("cooldown_bars", 3)
        self.params.setdefault("pct_major", 0.00012)
        self.params.setdefault("pct_alt", 0.0002)
        self.params.setdefault("pct_cap_mult", 1.5)
        self.params.setdefault("ok_mult", 1.5)
        self.params.setdefault("zone_pad_tick_mult", 2)
        self.params.setdefault("zone_pad_atr_mult", 0.05)
        if not self.params.get("major_symbols"):
            print("[MAJOR_WARN] major_symbols is empty - major logic disabled")
        
        # Set dynamic sweep_min_depth_atr_mult and sweep_min_depth_pct based on LTF
        if self.tfs['ltf'] == '1m':
            self.params['sweep_min_depth_atr_mult'] = self.params['ltf_1m_sweep_min_depth_atr_mult']
            self.params['sweep_min_depth_pct'] = self.params['ltf_1m_sweep_min_depth_pct'] # Added
        elif self.tfs['ltf'] == '5m':
            self.params['sweep_min_depth_atr_mult'] = self.params['ltf_5m_sweep_min_depth_atr_mult']
            self.params['sweep_min_depth_pct'] = self.params['ltf_5m_sweep_min_depth_pct'] # Added
        else:
            # Default or raise error for unsupported LTF
            self.params['sweep_min_depth_atr_mult'] = self.params['ltf_5m_sweep_min_depth_atr_mult'] # Default to 5m setting
            self.params['sweep_min_depth_pct'] = self.params['ltf_5m_sweep_min_depth_pct'] # Default to 5m setting

    def get_initial_state(self, symbol: str) -> Dict[str, Any]:
        """각 심볼에 대한 초기 상태를 정의합니다."""
        return {
            "symbol": symbol,
            "fsm_state": DTFXState.IDLE.value, # Use Enum
            "swings_ltf": [],
            "last_swing_low": None,
            "sweep_context": None,
            "sweep_seen_ts": None,
            "mss_break_level": None,
            "mss_context": None,
            "soft_counter": 0,
            "soft_seen_ts": None,
            "entry_zone": None,
            "timeout_counter": 0,
            "cooldown_counter": 0,
            "invalidate_consecutive_counter": 0, # Added
            "zone_touch_check_counter": 0,
        }

    def on_candle(self, symbol: str, ltf_candles: List[Candle], mtf_candles: List[Candle], htf_candles: List[Candle]) -> Optional[Signal]:
        """새로운 LTF 캔들마다 호출되어 상태 기계를 실행하고, ENTRY_READY 신호를 반환할 수 있습니다."""
        if symbol not in self.states:
            self.states[symbol] = self.get_initial_state(symbol)
        
        if not ltf_candles or len(ltf_candles) < self.params['swing_n'] * 2 + 1:
            return None

        state = self.states[symbol]
        current_candle = ltf_candles[-1]

        # 1. 항상 최신 스윙 정보를 업데이트
        state['swings_ltf'] = swings.detect_swings(ltf_candles, n=self.params['swing_n'])
        
        # 2. 현재 상태에 맞는 핸들러 호출 (중앙 집중식 디스패치)
        handlers = {
            DTFXState.IDLE: self._handle_idle,
            DTFXState.SWEEP_SEEN: self._handle_sweep_seen,
            DTFXState.MSS_SOFT: self._handle_mss_soft,
            DTFXState.MSS_CONFIRMED: self._handle_mss_confirmed,
            DTFXState.ZONE_SET: self._handle_retrace_zone_set, # Corrected to existing handler name
            DTFXState.COOLDOWN: self._handle_cooldown,
        }
        
        fsm_state_value = state['fsm_state']
        # Validate state value
        try:
            current_dtfx_state = DTFXState(fsm_state_value) # Convert to Enum
        except ValueError:
            # Handle invalid state values that are not in DTFXState Enum
            self._log_unknown_state_once(symbol, fsm_state_value, current_candle)
            self._change_state(state, DTFXState.IDLE) # Revert to IDLE
            return None
        
        handler = handlers.get(current_dtfx_state)

        if handler is None:
            self._log_unknown_state_once(symbol, fsm_state_value, current_candle)
            self._change_state(state, DTFXState.IDLE) # Revert to IDLE
            return None
        
        return handler(state, current_candle, ltf_candles, mtf_candles, htf_candles)

    def _log_and_create_signal(self, state: Dict[str, Any], candle: Candle, event: str, new_fsm_state: DTFXState, context: Dict[str, Any]) -> Signal: # Changed type hint
        """Helper to log and create a signal object."""
        self._apply_major_flags(state, context)
        signal = Signal(
            ts=candle.ts, engine="dtfx_long", symbol=state['symbol'],
            tf_ltf=self.tfs['ltf'], tf_mtf=self.tfs['mtf'], tf_htf=self.tfs['htf'],
            event=event, state=new_fsm_state.value, context=context # Use .value
        )
        self.logger.log(signal)
        return signal

    def _change_state(self, state: Dict[str, Any], new_state: DTFXState): # Changed new_state type hint
        """상태를 변경하고, 관련 카운터를 리셋합니다."""
        state['fsm_state'] = new_state.value # Use .value
        state['timeout_counter'] = 0
        if new_state == DTFXState.IDLE: # Use Enum
            for key in ['sweep_context', 'mss_context', 'entry_zone', 'last_swing_low', 'mss_break_level']:
                state[key] = None
            state['sweep_seen_ts'] = None
            state['soft_counter'] = 0
            state['soft_seen_ts'] = None
            state['invalidate_consecutive_counter'] = 0 # Added
            state['zone_touch_check_counter'] = 0

    def _zone_padding(self, state: Dict[str, Any], atr: float) -> float:
        tick_size = 0.0
        if state['symbol'] in self.exchange.markets:
            market_info = self.exchange.markets[state['symbol']]
            tick_size, _ = get_tick_size(market_info)
        tick_size = tick_size or 0.0
        pad_tick = tick_size * self.params['zone_pad_tick_mult']
        pad_atr = atr * self.params['zone_pad_atr_mult']
        return max(pad_tick, pad_atr)

    def _get_zone_skipped_reason_long(self, ltf: List[Candle], mss_context: Dict[str, Any], zone: Optional[Dict[str, Any]], zone_type: Optional[str]) -> Optional[str]:
        if not zone:
            if zone_type == "ob":
                return "no_ob_found"
            elif zone_type == "fvg":
                return "no_fvg_found"
            return "zone_not_found" # General case if neither OB nor FVG found
        
        atr = calculate_atr(ltf, period=ATR_PERIOD)
        
        # Check zone_too_wide
        zone_height = zone.get("hi", 0.0) - zone.get("lo", 0.0)
        if zone_height > atr * self.params['zone_too_wide_atr_mult']:
            return "zone_too_wide"
        
        # Check zone_too_far
        current_price = ltf[-1].c
        zone_mid = (zone.get("lo", 0.0) + zone.get("hi", 0.0)) / 2
        
        # Distance from current price to zone mid relative to ATR
        dist_to_zone_mid = abs(current_price - zone_mid)
        if dist_to_zone_mid > atr * self.params['zone_too_far_atr_mult']: # Using ATR * factor as distance
            return "zone_too_far"

        return None # No skip reason found

    def _overlaps_long(self, candle: Candle, zone_lo: float, zone_hi: float) -> bool:
        return candle.l <= zone_hi and candle.h >= zone_lo

    def _log_unknown_state_once(self, symbol: str, unknown_state_value: str, candle: Candle):
        key = (symbol, unknown_state_value)
        if key not in self._unknown_state_emitted:
            # Note: _log_and_create_signal expects new_fsm_state as str (DTFXState.IDLE.value)
            self._log_and_create_signal(
                self.states[symbol], candle, "UNKNOWN_STATE", DTFXState.IDLE, {"unknown_state_value": unknown_state_value}
            )
            self._unknown_state_emitted.add(key)

    def _base_symbol(self, symbol: str) -> str:
        return symbol.split("/")[0].upper()

    def _is_major_symbol(self, symbol: str) -> bool:
        base = self._base_symbol(symbol)
        majors = self.params.get("major_symbols", set())
        return base in majors

    def _tf_seconds(self, tf: str) -> int:
        try:
            unit = tf[-1]
            value = int(tf[:-1])
        except Exception:
            return 0
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        return 0

    def _apply_major_flags(self, state: Dict[str, Any], context: Dict[str, Any]) -> None:
        filters = context.get("filters")
        if not isinstance(filters, dict):
            filters = {}
            context["filters"] = filters
        symbol_base = self._base_symbol(state["symbol"])
        anchor_symbols = self.params.get("anchor_symbols", set())
        major_symbols = self.params.get("major_symbols", set())
        is_anchor_computed = symbol_base in anchor_symbols
        is_major_computed = symbol_base in major_symbols
        is_anchor_logged = filters.get("is_anchor")
        if is_anchor_logged is None:
            is_anchor_logged = is_anchor_computed
        is_major_logged = filters.get("is_major")
        if is_major_logged is None:
            is_major_logged = is_major_computed
        major_list_version = self._major_list_version(major_symbols)
        anchor_list_version = self._major_list_version(anchor_symbols)
        role = self.symbol_roles.get(state["symbol"], "unknown")
        filters.setdefault("symbol_base", symbol_base)
        filters.setdefault("major_match", is_major_computed)
        filters["is_major_logged"] = bool(is_major_logged)
        filters["is_major_computed"] = bool(is_major_computed)
        filters["major_list_version"] = major_list_version
        filters["role"] = role
        filters["is_anchor"] = bool(is_anchor_logged)
        filters["is_anchor_computed"] = bool(is_anchor_computed)
        filters["anchor_match"] = bool(is_anchor_computed)
        filters["anchor_list_version"] = anchor_list_version

    def _major_list_version(self, major_symbols: set) -> str:
        payload = ",".join(sorted(major_symbols))
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


    def _handle_idle(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """IDLE 상태: 유동성 스윕을 감시합니다."""
        recent_lows = [s for s in state['swings_ltf'] if s.type == 'low']
        if not recent_lows: return None
        state['last_swing_low'] = recent_lows[-1]

        atr = calculate_atr(ltf, period=ATR_PERIOD)
        
        # Get tick_size for the current symbol
        if state['symbol'] not in self.exchange.markets:
            return None # Should not happen if symbols are correctly filtered and loaded
        
        market_info = self.exchange.markets[state['symbol']]
        tick_size, tick_size_source = get_tick_size(market_info)
        if tick_size is None: # If tick_size cannot be reliably determined, skip this symbol
            self._log_and_create_signal(state, candle, "SWEEP_REJECTED", DTFXState.IDLE, {"reason": "tick_size_unavailable", "symbol": state['symbol']})
            self._change_state(state, DTFXState.IDLE)
            return None

        # Pass recent_candles for strong3 calculation in liquidity.py
        # Need at least 2 candles for strong3
        recent_candles_for_strong3 = ltf[-3:] # Last 3 candles, current candle is ltf[-1], previous is ltf[-2]

        sweep_context = liquidity.check_sell_side_sweep(
            candle, state['last_swing_low'], atr, 
            self.params['sweep_depth_atr_max'],
            self.params['strongbreakout_deadband_atr_mult'], 
            tick_size, 
            self.params['tick_size_multiplier'], 
            self.params['body_ratio_min'],
            ltf # Passing all ltf candles for strong3 calculation
        )

        if sweep_context:
            # 1. strong_breakout 차단 (A2Z / GIGGLE 문제)
            if sweep_context['is_strong_breakout']:
                signal = self._log_and_create_signal(state, candle, "SWEEP_REJECTED", DTFXState.IDLE, {"reason": "strong_breakout", "sweep": sweep_context})
                self._change_state(state, DTFXState.IDLE)
                return signal
            
            symbol_base = self._base_symbol(state['symbol'])
            is_major = self._is_major_symbol(state['symbol'])
            min_depth_atr = atr * self.params['sweep_min_depth_atr_mult']
            if is_major:
                min_depth_pct = candle.c * self.params['pct_major']
                min_depth_pct_cap = min_depth_atr * self.params['pct_cap_mult']
                min_depth_threshold = max(min_depth_atr, min(min_depth_pct, min_depth_pct_cap))
            else:
                min_depth_atr = atr * self.params.get('alt_min_depth_atr_mult', self.params['sweep_min_depth_atr_mult'])
                min_depth_pct = candle.c * self.params['pct_alt'] # Use candle close for price
                min_depth_pct_cap = min_depth_atr * self.params['pct_cap_mult']
                min_depth_alt = max(min_depth_pct, min_depth_atr * self.params.get('alt_atr_blend', 0.8))
                min_depth_threshold = min(min_depth_alt, min_depth_pct_cap)

            if min_depth_threshold == min_depth_atr:
                min_depth_source = "atr"
            elif min_depth_pct_cap is not None and min_depth_threshold == min_depth_pct_cap:
                min_depth_source = "pct_cap"
            elif min_depth_threshold == min_depth_pct:
                min_depth_source = "pct"
            else:
                min_depth_source = "unknown"

            filter_context = {
                "min_depth": min_depth_threshold,
                "min_depth_atr": min_depth_atr,
                "min_depth_atr_raw": atr * self.params['sweep_min_depth_atr_mult'],
                "min_depth_pct": min_depth_pct,
                "min_depth_pct_abs": min_depth_pct,
                "min_depth_pct_ratio": self.params['pct_major'] if is_major else self.params['pct_alt'],
                "min_depth_final": min_depth_threshold,
                "min_depth_source": min_depth_source,
                "min_depth_rule": "MAJOR_ATR" if is_major else "ALT_HYBRID_CAPPED",
                "is_major": is_major,
                "symbol_base": symbol_base,
                "major_match": is_major,
                "atr": atr,
                "candle_close": candle.c,
                "sweep_depth": sweep_context['sweep_depth'],
                "tick_size": tick_size,
                "tick_size_source": tick_size_source, # Added
                "deadband": sweep_context['deadband'],
            }
            if is_major:
                filter_context["min_depth_pct_cap"] = min_depth_pct_cap

            # 2. sweep 깊이 필터 (RIVER / Q / PTB 문제)
            if sweep_context['sweep_depth'] < min_depth_threshold:
                signal = self._log_and_create_signal(state, candle, "SWEEP_REJECTED", DTFXState.IDLE, {
                    "reason": "min_depth_filter",
                    "sweep": sweep_context,
                    "filters": filter_context # Added filter details
                })
                self._change_state(state, DTFXState.IDLE)
                return signal
            
            state['sweep_context'] = sweep_context
            state['sweep_seen_ts'] = candle.ts
            self._change_state(state, DTFXState.SWEEP_SEEN)
            sweep_strength = "OK" if sweep_context['sweep_depth'] >= min_depth_threshold * self.params['ok_mult'] else "WEAK"
            signal = self._log_and_create_signal(state, candle, f"SWEEP_SEEN_{sweep_strength}", DTFXState.SWEEP_SEEN, {
                "sweep": sweep_context,
                "filters": filter_context # Added filter details
            })
            return signal
        return None

    def _handle_sweep_seen(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """SWEEP_SEEN 상태: 시장 구조 변화(MSS)를 감시합니다."""
        state['timeout_counter'] += 1
        if state['timeout_counter'] > self.params['timeout_sweep_to_mss']:
            elapsed_sec = None
            if state.get("sweep_seen_ts"):
                elapsed_sec = (candle.ts - state["sweep_seen_ts"]) / 1000.0
            self._log_and_create_signal(state, candle, "TIMEOUT", DTFXState.IDLE, {
                "from_state": DTFXState.SWEEP_SEEN.value,
                "elapsed_bars": state['timeout_counter'],
                "elapsed_sec": elapsed_sec,
                "reason": "timeout_sweep_to_mss",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "sweep_to_mss_max_bars": self.params['timeout_sweep_to_mss'],
                "sweep_to_mss_max_sec": self.params['timeout_sweep_to_mss'] * self._tf_seconds(self.tfs['ltf']),
            }) # Use Enum
            self._change_state(state, DTFXState.IDLE) # Use Enum
            return None

        sweep_ts = state['sweep_context']['ref_swing_ts']
        new_highs = [s for s in state['swings_ltf'] if s.type == 'high' and s.ts > sweep_ts]
        if not new_highs: return None
        state['mss_break_level'] = new_highs[-1] 

        break_level = state['mss_break_level'].price
        deadband_ctx = state['sweep_context'].get("deadband") if isinstance(state.get('sweep_context'), dict) else None
        if isinstance(deadband_ctx, dict):
            deadband_value = float(deadband_ctx.get("value") or 0.0)
        else:
            deadband_value = float(deadband_ctx or 0.0)
        tick_size = 0.0
        if state['symbol'] in self.exchange.markets:
            market_info = self.exchange.markets[state['symbol']]
            tick_size, _ = get_tick_size(market_info)
        tick_size = tick_size or 0.0
        deadband_mss_soft = max(
            tick_size * self.params.get("mss_soft_tick_mult", 1),
            deadband_value * self.params.get("mss_soft_deadband_mult", 0.25),
        )
        deadband_mss_confirm = max(
            tick_size * self.params.get("mss_confirm_tick_mult", 3),
            deadband_value * self.params.get("mss_confirm_deadband_mult", 0.60),
        )

        needs_close_beyond = True
        confirm_line = break_level + deadband_mss_confirm
        soft_line = break_level + deadband_mss_soft
        close_passed = candle.c >= confirm_line
        soft_passed = candle.h >= soft_line
        failed = []
        if close_passed:
            failed = []
        elif soft_passed:
            failed.append("confirm_close_not_beyond")
        else:
            failed.append("soft_wick_not_beyond")
        mss_check = {
            "dir": "up",
            "break_level": break_level,
            "close": candle.c,
            "needs_close_beyond": needs_close_beyond,
            "needs_close_beyond_confirm": True,
            "needs_close_beyond_soft": False,
            "passed": close_passed,
            "failed": failed,
            "candle": {"o": candle.o, "h": candle.h, "l": candle.l, "c": candle.c},
            "deadband_base": deadband_value,
            "deadband_mss_soft": deadband_mss_soft,
            "deadband_mss_confirm": deadband_mss_confirm,
            "soft": {"passed_soft": soft_passed, "soft_line": soft_line},
            "confirm": {"passed_confirm": close_passed, "confirm_line": confirm_line},
        }
        self._log_and_create_signal(state, candle, "MSS_CHECK", DTFXState.SWEEP_SEEN, {
            "from_state": DTFXState.SWEEP_SEEN.value,
            "elapsed_bars": state['timeout_counter'],
            "elapsed_sec": (candle.ts - state["sweep_seen_ts"]) / 1000.0 if state.get("sweep_seen_ts") else None,
            "reason": "mss_check",
            "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
            "sweep_to_mss_max_bars": self.params['timeout_sweep_to_mss'],
            "sweep_to_mss_max_sec": self.params['timeout_sweep_to_mss'] * self._tf_seconds(self.tfs['ltf']),
            "mss_check": mss_check,
        })

        if close_passed:
            mss_context = structure.check_mss_up(
                candle, state['mss_break_level'], ltf, 0.0
            )
            if mss_context:
                mss_context["deadband_mss_confirm"] = deadband_mss_confirm
                mss_context["confirm_close"] = candle.c
                state['mss_context'] = mss_context
                elapsed_bars = state['timeout_counter']
                elapsed_sec = (candle.ts - state["sweep_seen_ts"]) / 1000.0 if state.get("sweep_seen_ts") else None
                self._change_state(state, DTFXState.MSS_CONFIRMED) # Use Enum
                signal = self._log_and_create_signal(state, candle, "MSS_CONFIRMED", DTFXState.MSS_CONFIRMED, {
                    "from_state": DTFXState.SWEEP_SEEN.value,
                    "elapsed_bars": elapsed_bars,
                    "elapsed_sec": elapsed_sec,
                    "reason": "mss_confirmed",
                    "path": "direct",
                    "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                    "sweep_to_mss_max_bars": self.params['timeout_sweep_to_mss'],
                    "sweep_to_mss_max_sec": self.params['timeout_sweep_to_mss'] * self._tf_seconds(self.tfs['ltf']),
                    "mss": mss_context,
                    "sweep": state['sweep_context'],
                }) # Use Enum
                return signal

        soft_reasons = []
        if soft_passed:
            soft_reasons.append("touch")
        if candle.c >= break_level - deadband_mss_soft:
            soft_reasons.append("near")
        if soft_passed and (candle.c < break_level) and (candle.c >= break_level - deadband_mss_soft):
            soft_reasons.append("wick_reclaim")

        if soft_reasons and self.params.get("mss_soft_enabled", True):
            state['soft_counter'] = 0
            state['soft_seen_ts'] = candle.ts
            self._change_state(state, DTFXState.MSS_SOFT)
            self._log_and_create_signal(state, candle, "MSS_SOFT_SEEN", DTFXState.MSS_SOFT, {
                "from_state": DTFXState.SWEEP_SEEN.value,
                "elapsed_bars": state['timeout_counter'],
                "elapsed_sec": (candle.ts - state["sweep_seen_ts"]) / 1000.0 if state.get("sweep_seen_ts") else None,
                "reason": "mss_soft",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "sweep_to_mss_max_bars": self.params['timeout_sweep_to_mss'],
                "sweep_to_mss_max_sec": self.params['timeout_sweep_to_mss'] * self._tf_seconds(self.tfs['ltf']),
                "mss_soft": {
                    "dir": "up",
                    "break_level": break_level,
                    "close": candle.c,
                    "high": candle.h,
                    "deadband_mss_soft": deadband_mss_soft,
                    "reason": soft_reasons,
                },
            })
            return None

        self._log_and_create_signal(state, candle, "MSS_REJECTED", DTFXState.SWEEP_SEEN, {
            "from_state": DTFXState.SWEEP_SEEN.value,
            "elapsed_bars": state['timeout_counter'],
            "elapsed_sec": (candle.ts - state["sweep_seen_ts"]) / 1000.0 if state.get("sweep_seen_ts") else None,
            "reason": "soft_not_reached",
            "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
            "sweep_to_mss_max_bars": self.params['timeout_sweep_to_mss'],
            "sweep_to_mss_max_sec": self.params['timeout_sweep_to_mss'] * self._tf_seconds(self.tfs['ltf']),
            "mss_check": mss_check,
        })
        return None

    def _handle_mss_soft(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """MSS_SOFT 상태: 확정 돌파를 대기합니다."""
        if not state.get("sweep_context") or not state.get("mss_break_level"):
            self._log_and_create_signal(state, candle, "RESET", DTFXState.IDLE, {
                "from_state": DTFXState.MSS_SOFT.value,
                "reason": "missing_context",
            })
            self._change_state(state, DTFXState.IDLE)
            return None

        state['soft_counter'] += 1
        max_bars = self.params.get("mss_soft_to_confirm_max_bars", self.params.get("soft_to_confirm_max_bars", 6))
        max_sec_param = self.params.get("mss_soft_to_confirm_max_sec")
        if state['soft_counter'] > max_bars:
            elapsed_sec = None
            if state.get("soft_seen_ts"):
                elapsed_sec = (candle.ts - state["soft_seen_ts"]) / 1000.0
            self._log_and_create_signal(state, candle, "MSS_SOFT_TIMEOUT", DTFXState.IDLE, {
                "from_state": DTFXState.MSS_SOFT.value,
                "elapsed_bars": state['soft_counter'],
                "elapsed_sec": elapsed_sec,
                "reason": "soft_to_confirm_timeout",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "soft_to_confirm_max_bars": max_bars,
                "soft_to_confirm_max_sec": max_sec_param or (max_bars * self._tf_seconds(self.tfs['ltf'])),
            })
            self._change_state(state, DTFXState.IDLE)
            return None

        break_level = state['mss_break_level'].price
        deadband_ctx = state['sweep_context'].get("deadband") if isinstance(state.get('sweep_context'), dict) else None
        if isinstance(deadband_ctx, dict):
            deadband_value = float(deadband_ctx.get("value") or 0.0)
        else:
            deadband_value = float(deadband_ctx or 0.0)
        tick_size = 0.0
        if state['symbol'] in self.exchange.markets:
            market_info = self.exchange.markets[state['symbol']]
            tick_size, _ = get_tick_size(market_info)
        tick_size = tick_size or 0.0
        deadband_mss_confirm = max(
            tick_size * self.params.get("mss_confirm_tick_mult", 3),
            deadband_value * self.params.get("mss_confirm_deadband_mult", 0.60),
        )

        confirm_line = break_level + deadband_mss_confirm
        close_passed = candle.c >= confirm_line
        failed = []
        if not close_passed:
            failed.append("confirm_close_not_beyond")
        mss_check = {
            "dir": "up",
            "break_level": break_level,
            "close": candle.c,
            "needs_close_beyond": True,
            "needs_close_beyond_confirm": True,
            "needs_close_beyond_soft": False,
            "passed": close_passed,
            "failed": failed,
            "candle": {"o": candle.o, "h": candle.h, "l": candle.l, "c": candle.c},
            "deadband_base": deadband_value,
            "deadband_mss_confirm": deadband_mss_confirm,
            "confirm": {"passed_confirm": close_passed, "confirm_line": confirm_line},
        }
        self._log_and_create_signal(state, candle, "MSS_CHECK", DTFXState.MSS_SOFT, {
            "from_state": DTFXState.MSS_SOFT.value,
            "elapsed_bars": state['soft_counter'],
            "elapsed_sec": (candle.ts - state["soft_seen_ts"]) / 1000.0 if state.get("soft_seen_ts") else None,
            "reason": "mss_check",
            "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
            "soft_to_confirm_max_bars": self.params.get("soft_to_confirm_max_bars", 6),
            "soft_to_confirm_max_sec": self.params.get("soft_to_confirm_max_bars", 6) * self._tf_seconds(self.tfs['ltf']),
            "mss_check": mss_check,
        })

        if not close_passed:
            self._log_and_create_signal(state, candle, "MSS_REJECTED", DTFXState.MSS_SOFT, {
                "from_state": DTFXState.MSS_SOFT.value,
                "elapsed_bars": state['soft_counter'],
                "elapsed_sec": (candle.ts - state["soft_seen_ts"]) / 1000.0 if state.get("soft_seen_ts") else None,
                "reason": "confirm_not_reached",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "soft_to_confirm_max_bars": max_bars,
                "soft_to_confirm_max_sec": max_sec_param or (max_bars * self._tf_seconds(self.tfs['ltf'])),
                "mss_check": mss_check,
            })
            return None

        mss_context = structure.check_mss_up(
            candle, state['mss_break_level'], ltf, 0.0
        )
        if mss_context:
            mss_context["deadband_mss_confirm"] = deadband_mss_confirm
            mss_context["confirm_close"] = candle.c
            state['mss_context'] = mss_context
            elapsed_bars = state['soft_counter']
            elapsed_sec = (candle.ts - state["soft_seen_ts"]) / 1000.0 if state.get("soft_seen_ts") else None
            self._change_state(state, DTFXState.MSS_CONFIRMED)
            self._log_and_create_signal(state, candle, "MSS_SOFT_TO_CONFIRMED", DTFXState.MSS_CONFIRMED, {
                "from_state": DTFXState.MSS_SOFT.value,
                "elapsed_bars": elapsed_bars,
                "elapsed_sec": elapsed_sec,
                "reason": "mss_confirmed",
                "path": "from_soft",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "soft_to_confirm_max_bars": max_bars,
                "soft_to_confirm_max_sec": max_sec_param or (max_bars * self._tf_seconds(self.tfs['ltf'])),
                "mss": mss_context,
                "sweep": state['sweep_context'],
            })
            signal = self._log_and_create_signal(state, candle, "MSS_CONFIRMED", DTFXState.MSS_CONFIRMED, {
                "from_state": DTFXState.MSS_SOFT.value,
                "elapsed_bars": elapsed_bars,
                "elapsed_sec": elapsed_sec,
                "reason": "mss_confirmed",
                "path": "from_soft",
                "sweep_ref_ts": state['sweep_context']['ref_swing_ts'],
                "soft_to_confirm_max_bars": max_bars,
                "soft_to_confirm_max_sec": max_sec_param or (max_bars * self._tf_seconds(self.tfs['ltf'])),
                "mss": mss_context,
                "sweep": state['sweep_context'],
            })
            return signal
        return None

    def _handle_mss_confirmed(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """MSS_CONFIRMED 상태: 되돌림 진입 존을 설정합니다. (OB > FVG > OTE 순서)"""
        mss_candle_idx = next((i for i, c in enumerate(ltf) if c.ts == state['mss_context']['confirm_candle_ts']), -1)
        if mss_candle_idx == -1:
            self._log_and_create_signal(state, candle, "INVALIDATED", DTFXState.IDLE, {"reason": "mss_candle_not_found", "from_state": DTFXState.MSS_CONFIRMED.value}) # Use Enum
            self._change_state(state, DTFXState.IDLE) # Use Enum
            return None

        found_zone = None
        found_zone_type = None # Track type of zone found
        ob_zone = zones.find_order_block(ltf, mss_candle_idx, 'up', self.params.get('zone_ob_atr_max', 1.0))
        if ob_zone:
            found_zone = ob_zone
            found_zone_type = "ob"
        
        if not found_zone:
            fvg_search_candles = ltf[max(0, mss_candle_idx - 10):mss_candle_idx + 1]
            fvgs = zones.find_fair_value_gaps(fvg_search_candles)
            up_fvgs = [fvg for fvg in fvgs if fvg['dir'] == 'up']
            if up_fvgs:
                found_zone = up_fvgs[-1]
                found_zone_type = "fvg"

        if not found_zone:
            sweep_low_price = state['sweep_context']['sweep_candle_low']
            mss_high_price = state['mss_context']['break_level']
            ote_zone = zones.calculate_ote(sweep_low_price, mss_high_price)
            if ote_zone:
                 found_zone = ote_zone
                 found_zone_type = "ote"

        skipped_reason = self._get_zone_skipped_reason_long(ltf, state['mss_context'], found_zone, found_zone_type)

        if skipped_reason:
            self._log_and_create_signal(state, candle, "ZONE_SKIPPED", DTFXState.IDLE, {"reason": skipped_reason, "mss": state['mss_context'], "zone": found_zone})
            self._change_state(state, DTFXState.IDLE)
        elif found_zone:
            state['entry_zone'] = found_zone
            state['zone_touch_check_counter'] = 6
            self._change_state(state, DTFXState.ZONE_SET) # Use Enum
            zone_lo = found_zone.get("lo", 0.0)
            zone_hi = found_zone.get("hi", 0.0)
            atr = calculate_atr(ltf, period=ATR_PERIOD)
            pad = self._zone_padding(state, atr)
            self._log_and_create_signal(state, candle, "ZONE_SET", DTFXState.ZONE_SET, {
                "zone": {
                    "type": found_zone_type,
                    "lo": zone_lo,
                    "hi": zone_hi,
                    "pad_lo": zone_lo - pad,
                    "pad_hi": zone_hi + pad,
                },
                "mss": state['mss_context']
            }) # Use Enum
        else:
            # Fallback for when no zone is found and no specific skipped reason applies
            self._log_and_create_signal(state, candle, "ZONE_SKIPPED", DTFXState.IDLE, {"reason": "no_zone_found_general", "mss": state['mss_context']})
            self._change_state(state, DTFXState.IDLE)
        return None

    def _handle_retrace_zone_set(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """RETRACE_ZONE_SET 상태: 진입 존 터치를 감시합니다."""
        # Calculate ATR for this context
        atr = calculate_atr(ltf, period=ATR_PERIOD)

        state['timeout_counter'] += 1
        if state['timeout_counter'] > self.params['timeout_mss_to_touch']:
            zone = state['entry_zone'] or {}
            zone_mid = (zone.get("lo", 0.0) + zone.get("hi", 0.0)) / 2 if zone else 0.0
            dist_atr = abs(candle.c - zone_mid) / atr if atr > 0 else None
            self._log_and_create_signal(state, candle, "ZONE_EXPIRED", DTFXState.IDLE, {
                "reason": "timeout_mss_to_touch",
                "from_state": DTFXState.ZONE_SET.value,
                "zone_mid": zone_mid,
                "dist_atr": dist_atr,
            })
            self._log_and_create_signal(state, candle, "TIMEOUT", DTFXState.IDLE, {"reason": "timeout_mss_to_touch", "from_state": DTFXState.ZONE_SET.value}) # Use Enum
            self._change_state(state, DTFXState.IDLE) # Use Enum
            return None
        pad = self._zone_padding(state, atr)

        deadband = atr * self.params['strongbreakout_deadband_atr_mult']
        invalidate_price_level = state['sweep_context']['ref_swing_price'] - deadband # Use ref_swing_price for deadband logic as per v1.1

        # Check for invalidation (close below sweep_low - deadband for 2 consecutive candles)
        if candle.c < invalidate_price_level:
            state['invalidate_consecutive_counter'] += 1
            if state['invalidate_consecutive_counter'] >= 2:
                self._log_and_create_signal(state, candle, "ZONE_INVALIDATED", DTFXState.IDLE, {"reason": "consecutive_close_below_deadband", "from_state": DTFXState.ZONE_SET.value, "deadband_price": invalidate_price_level})
                self._log_and_create_signal(state, candle, "INVALIDATED", DTFXState.IDLE, {"reason": "consecutive_close_below_deadband", "from_state": DTFXState.ZONE_SET.value, "deadband_price": invalidate_price_level}) # Use Enum
                self._change_state(state, DTFXState.IDLE) # Use Enum
                return None
        else:
            state['invalidate_consecutive_counter'] = 0 # Reset if condition not met

        zone = state['entry_zone']
        zone_lo = zone['lo'] - pad
        zone_hi = zone['hi'] + pad
        overlap = self._overlaps_long(candle, zone_lo, zone_hi)

        if state['zone_touch_check_counter'] > 0:
            self._log_and_create_signal(state, candle, "ZONE_TOUCH_CHECK", DTFXState.ZONE_SET, {
                "candle": {
                    "high": candle.h,
                    "low": candle.l,
                    "close": candle.c,
                },
                "zone": {
                    "lo": zone['lo'],
                    "hi": zone['hi'],
                    "pad_lo": zone_lo,
                    "pad_hi": zone_hi,
                },
                "overlap": overlap,
                "counter_left": state['zone_touch_check_counter'] - 1, # Counter after this check
            })
            state['zone_touch_check_counter'] -= 1
        
        # Check for any interaction with the padded zone
        if overlap:
            self._log_and_create_signal(state, candle, "ZONE_TOUCH_SEEN", DTFXState.ZONE_SET, {
                "zone": zone,
                "zone_lo_pad": zone_lo,
                "zone_hi_pad": zone_hi,
                "candle_high": candle.h,
                "candle_low": candle.l,
            })
            touch_price = candle.l # Use candle low as the interaction price

            if zone_lo <= touch_price <= zone_hi:
                touch_type = "TOUCH"
            elif candle.h >= zone_lo and touch_price < zone_lo:
                touch_type = "PIERCE"
            else:
                return None

            # Check MSS direction alignment
            mss_dir_aligned = state['mss_context']['dir'] == 'up' # For long engine
            
            # Check touch candle properties
            candle_is_opposite_color = candle.c < candle.o # Red candle for long entry
            candle_range = candle.h - candle.l
            lower_wick = candle.l - min(candle.o, candle.c) # For long, lower wick is more important
            eps = 1e-9 # Small epsilon to prevent division by zero

            if candle_range <= eps:
                wick_ratio_value = 1.0
                doji_block = True
            else:
                wick_ratio_value = lower_wick / candle_range
                doji_block = False
            wick_ratio_value = min(max(wick_ratio_value, 0.0), 1.0)
            is_anchor = self._base_symbol(state["symbol"]) in self.params.get("anchor_symbols", set())
            wick_ratio_min = self.params.get(
                "entry_wick_ratio_min_major" if is_anchor else "entry_wick_ratio_min_alt",
                self.params['wick_ratio_min']
            )
            wick_ratio_ok = (wick_ratio_value >= wick_ratio_min) and (not doji_block)
            body = abs(candle.c - candle.o)
            atr_entry = calculate_atr(ltf, period=ATR_PERIOD)
            entry_body_atr_min = self.params.get("entry_body_atr_min")
            if entry_body_atr_min is None:
                entry_body_atr_min = self.params.get(
                    "entry_body_atr_min_major" if is_anchor else "entry_body_atr_min_alt",
                    0.25,
                )
            body_atr_ok = (atr_entry <= 0) or (body >= atr_entry * entry_body_atr_min)
            require_color_strict = bool(self.params.get("require_color_strict", True))
            confirm_close_ratio = float(self.params.get("entry_confirm_close_ratio", 0.55))
            if candle_range <= eps:
                close_ratio_value = 0.0
            else:
                close_ratio_value = (candle.c - candle.l) / candle_range
            close_ratio_value = min(max(close_ratio_value, 0.0), 1.0)
            confirm_close_ok = close_ratio_value >= confirm_close_ratio
            require_full_close = bool(self.params.get("entry_requires_full_close", False))
            close_within_zone = zone_lo <= candle.c <= zone_hi

            entry_conditions = {
                "mss_dir_aligned": mss_dir_aligned,
                "candle_opposite_color": candle_is_opposite_color,
                "candle_color_strict": require_color_strict,
                "wick_ratio_ok": wick_ratio_ok,
                "wick_ratio_value": wick_ratio_value,
                "wick_ratio_min": wick_ratio_min,
                "body_atr_ok": body_atr_ok,
                "entry_body_atr_min": entry_body_atr_min,
                "confirm_close_ratio": confirm_close_ratio,
                "confirm_close_ratio_value": close_ratio_value,
                "confirm_close_ok": confirm_close_ok,
                "entry_requires_full_close": require_full_close,
                "close_within_zone": close_within_zone,
                "doji_block": doji_block,
                "all_pass": False,
                "failed": [],
                "warnings": [],
            }
            if not mss_dir_aligned:
                entry_conditions["failed"].append("mss_dir_aligned")
            if not candle_is_opposite_color:
                if require_color_strict:
                    entry_conditions["failed"].append("candle_opposite_color")
                else:
                    entry_conditions["warnings"].append("candle_opposite_color")
            if doji_block:
                entry_conditions["failed"].append("doji_block")
            if not wick_ratio_ok and not doji_block:
                entry_conditions["failed"].append("wick_ratio_ok")
            if not body_atr_ok:
                entry_conditions["failed"].append("body_atr_min")
            if not confirm_close_ok:
                entry_conditions["failed"].append("confirm_close_ratio")
            if require_full_close and not close_within_zone:
                entry_conditions["failed"].append("requires_full_close")
            entry_conditions["all_pass"] = len(entry_conditions["failed"]) == 0

            self._log_and_create_signal(state, candle, "ENTRY_CHECK", DTFXState.ZONE_SET, {
                "sweep": state['sweep_context'],
                "mss": state['mss_context'],
                "zone": state['entry_zone'],
                "touch": {"price": touch_price, "ts": candle.ts, "type": touch_type},
                "entry_conditions": entry_conditions,
            })

            if not entry_conditions["all_pass"]:
                self._log_and_create_signal(state, candle, "ENTRY_BLOCKED", DTFXState.ZONE_SET, {
                    "sweep": state['sweep_context'],
                    "mss": state['mss_context'],
                    "zone": state['entry_zone'],
                    "touch": {"price": touch_price, "ts": candle.ts, "type": touch_type},
                    "entry_conditions": entry_conditions,
                })
                return None

            signal = self._log_and_create_signal(state, candle, f"ENTRY_READY_{touch_type}", DTFXState.COOLDOWN, {
                "sweep": state['sweep_context'],
                "mss": state['mss_context'],
                "zone": state['entry_zone'],
                "touch": {"price": touch_price, "ts": candle.ts, "type": touch_type},
                "entry_conditions": entry_conditions,
            })
            state['cooldown_counter'] = self.params['cooldown_bars']
            self._change_state(state, DTFXState.COOLDOWN) # Use Enum
            return signal
        return None

    def _handle_cooldown(self, state: Dict[str, Any], candle: Candle, ltf: List[Candle], mtf: List[Candle], htf: List[Candle]) -> Optional[Signal]:
        """COOLDOWN 상태: 일정 기간 추가 신호를 제한합니다."""
        state['cooldown_counter'] -= 1
        if state['cooldown_counter'] <= 0:
            self._change_state(state, DTFXState.IDLE) # Use Enum
        return None
