from dataclasses import dataclass


@dataclass
class Div15mConfig:
    # Pivot
    PIVOT_L: int = 3
    LOOKBACK_BARS: int = 160
    MIN_PIVOT_GAP: int = 6

    # RSI
    RSI_LEN: int = 14
    MIN_RSI_UP: float = 4.0
    RSI_OS: float = 32.0
    OS_LOOKBACK_W: int = 6
    WARMUP_BARS: int = 50

    # Price
    MIN_LL_PCT: float = 0.004

    # EMA confirm/trigger
    EMA_FAST: int = 7
    EMA_SLOW: int = 20
    CONFIRM_WINDOW: int = 6
    EMA_REGIME_LEN: int = 120

    # Trigger
    TRIGGER_MODE: str = "A"  # A: EMA20 close reclaim, B: break prev 3 highs

    # Signal cooldown (engine-level). Use 0 for trade-managed cooldown.
    SIGNAL_COOLDOWN_BARS: int = 0

    # Trade cooldown (applied after exit in backtest runner)
    TRADE_COOLDOWN_BARS: int = 12

    # Spike filter (optional)
    SPIKE_VOL_X: float = 2.5
    SPIKE_VOL_LOOKBACK: int = 20

    # Trade simulation
    ENTRY_MODE: str = "NEXT_OPEN"  # NEXT_OPEN | SIGNAL_CLOSE
    EXIT_MODE: str = "R_MULT"  # R_MULT | PCT
    RISK_PCT: float = 0.01
    SL_R_MULT: float = 2.0
    TP_R_MULT: float = 2.0
    SL_PCT: float = 0.02
    TP_PCT: float = 0.02
    TIMEOUT_BARS: int = 32
    FEE_RATE: float = 0.0
    SLIPPAGE_PCT: float = 0.0
    POSITION_SIZE_USDT: float = 100.0
