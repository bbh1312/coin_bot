"""
DTFX 엔진의 기본 파라미터를 정의합니다.
"""

MIN_QVOL_24H_USDT = 8_000_000
UNIVERSE_TOP_N = 40 # Re-added
SYMBOL_REFRESH_INTERVAL_SECONDS = 900 # 15 minutes
LOW_LIQUIDITY_QVOL_24H_USDT = 50_000_000
LTF_1M_SWEEP_MIN_DEPTH_ATR_MULT = 0.25
LTF_5M_SWEEP_MIN_DEPTH_ATR_MULT = 0.15
LTF_1M_SWEEP_MIN_DEPTH_PCT = 0.0003 # Added (0.03%)
LTF_5M_SWEEP_MIN_DEPTH_PCT = 0.0002 # Added (0.02%)
ALT_MIN_DEPTH_ATR_MULT = 0.11
ALT_ATR_BLEND = 0.8
MSS_SOFT_ENABLED = True
MSS_SOFT_DEADBAND_MULT = 0.25
MSS_CONFIRM_DEADBAND_MULT = 0.60
MSS_SOFT_TICK_MULT = 1
MSS_CONFIRM_TICK_MULT = 3
MSS_SOFT_TO_CONFIRM_MAX_BARS = 6
MSS_SOFT_TO_CONFIRM_MAX_SEC = 1800
ATR_PERIOD = 14
ZONE_PIERCE_ATR_MULT = 0.1
STRONGBREAKOUT_DEADBAND_ATR_MULT = 0.08 
TICK_SIZE_MULTIPLIER = 3 # Added
BODY_RATIO_MIN = 0.60 # Added
SUMMARY_INTERVAL_SECONDS = 900 # 15 minutes, Added
MAJOR_PCT_WEIGHT = 0.6 # Added
ZONE_TOO_WIDE_ATR_MULT = 1.5 # Added
ZONE_TOO_FAR_ATR_MULT = 10 # Changed from CANDLE_DIST
WICK_RATIO_MIN = 0.6 # Added
ENTRY_WICK_RATIO_MIN_MAJOR = 0.55
ENTRY_WICK_RATIO_MIN_ALT = 0.50
ENTRY_BODY_ATR_MIN_MAJOR = 0.30
ENTRY_BODY_ATR_MIN_ALT = 0.25
ENTRY_REQUIRE_COLOR_STRICT = False
ENTRY_BODY_ATR_MIN = 0.6
ENTRY_CONFIRM_CLOSE_RATIO = 0.55
ENTRY_REQUIRES_FULL_CLOSE = False

# CORE_MAJOR: BTC, ETH (anchors)
ANCHOR_SYMBOLS = [
    "BTC",
    "ETH",
]
# MAJOR list follows anchors for now (log/summary 기준)
MAJOR_SYMBOLS = ANCHOR_SYMBOLS

def get_default_params():
    """
    사용자 요구사항 11절에 명시된 기본 파라미터 값을 반환합니다.
    """
    return {
        # 스윙(프랙탈) 확정 n값
        "swing_n": 2,

        # Liquidity Sweep 관련
        "sweep_depth_atr_max": 0.6,
        "ltf_1m_sweep_min_depth_atr_mult": LTF_1M_SWEEP_MIN_DEPTH_ATR_MULT,
        "ltf_5m_sweep_min_depth_atr_mult": LTF_5M_SWEEP_MIN_DEPTH_ATR_MULT,
        "ltf_1m_sweep_min_depth_pct": LTF_1M_SWEEP_MIN_DEPTH_PCT, # Added to params
        "ltf_5m_sweep_min_depth_pct": LTF_5M_SWEEP_MIN_DEPTH_PCT, # Added to params
        "alt_min_depth_atr_mult": ALT_MIN_DEPTH_ATR_MULT,
        "alt_atr_blend": ALT_ATR_BLEND,
        "mss_soft_enabled": MSS_SOFT_ENABLED,
        "mss_soft_deadband_mult": MSS_SOFT_DEADBAND_MULT,
        "mss_confirm_deadband_mult": MSS_CONFIRM_DEADBAND_MULT,
        "mss_soft_tick_mult": MSS_SOFT_TICK_MULT,
        "mss_confirm_tick_mult": MSS_CONFIRM_TICK_MULT,
        "mss_soft_to_confirm_max_bars": MSS_SOFT_TO_CONFIRM_MAX_BARS,
        "mss_soft_to_confirm_max_sec": MSS_SOFT_TO_CONFIRM_MAX_SEC,
        "entry_wick_ratio_min_major": ENTRY_WICK_RATIO_MIN_MAJOR,
        "entry_wick_ratio_min_alt": ENTRY_WICK_RATIO_MIN_ALT,
        "entry_body_atr_min_major": ENTRY_BODY_ATR_MIN_MAJOR,
        "entry_body_atr_min_alt": ENTRY_BODY_ATR_MIN_ALT,
        "entry_body_atr_min": ENTRY_BODY_ATR_MIN,
        "entry_confirm_close_ratio": ENTRY_CONFIRM_CLOSE_RATIO,
        "entry_requires_full_close": ENTRY_REQUIRES_FULL_CLOSE,
        "require_color_strict": ENTRY_REQUIRE_COLOR_STRICT,
        "zone_pierce_atr_mult": ZONE_PIERCE_ATR_MULT,
        "strongbreakout_deadband_atr_mult": STRONGBREAKOUT_DEADBAND_ATR_MULT, 
        "tick_size_multiplier": TICK_SIZE_MULTIPLIER, # Added to params
        "body_ratio_min": BODY_RATIO_MIN, # Added to params
        "major_pct_weight": MAJOR_PCT_WEIGHT, # Added to params
        "zone_too_wide_atr_mult": ZONE_TOO_WIDE_ATR_MULT, # Added to params
        "zone_too_far_atr_mult": ZONE_TOO_FAR_ATR_MULT, # Changed from CANDLE_DIST
        "wick_ratio_min": WICK_RATIO_MIN, # Added to params
        "major_symbols": MAJOR_SYMBOLS,
        "anchor_symbols": ANCHOR_SYMBOLS,
        "soft_to_confirm_max_bars": 6,
        "entry_body_atr_min": 0.25,
        # ...
    }
