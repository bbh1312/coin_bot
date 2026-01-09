from dataclasses import dataclass


@dataclass
class Config:
    symbol: str = "BTC/USDT:USDT"
    htf_tf: str = "1h"
    ltf_tf: str = "15m"
    htf_limit: int = 200
    ltf_limit: int = 200

    supertrend_period: int = 10
    supertrend_mult: float = 3.0

    atr_period: int = 14
    atr_sma_period: int = 50
    atr_mult: float = 1.2

    vol_sma_period: int = 20
    vol_mult: float = 1.3

    strong_score_threshold: int = 70
    instant_score_min: int = 85
    summary_top_n: int = 5
    fabio_universe_top_n: int = 30
    min_quote_volume: float = 20000000.0
    trigger_symbol: str = "BTC/USDT:USDT"
    poll_sec: float = 15.0
    state_file: str = "atlas_state.json"
