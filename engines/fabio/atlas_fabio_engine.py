from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import cycle_cache
from atlas_test.indicators import atr, sma, supertrend


def ema(values: List[float], n: int) -> List[float]:
    if len(values) < n:
        return []
    k = 2 / (n + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] * (1 - k) + v * k)
    return out


@dataclass
class Config:
    htf_tf: str = "1h"
    ltf_tf: str = "15m"
    htf_limit: int = 200
    ltf_limit: int = 200
    d1_tf: str = "1d"
    d1_limit: int = 120
    supertrend_period: int = 10
    supertrend_mult: float = 3.0
    atr_period: int = 14
    atr_sma_period: int = 50
    atr_mult: float = 1.2
    d1_ema_len: int = 7
    d1_atr_len: int = 14
    d1_overext_atr_mult: float = 1.3
    vol_sma_period: int = 20
    vol_mult: float = 1.3
    strong_score_threshold: int = 70
    size_mult_base: float = 1.0
    size_mult_atr_boost: float = 0.1
    size_mult_vol_boost: float = 0.1
    size_mult_max: float = 1.2


def _load_gate_data(symbol: str, cfg: Config) -> Optional[Dict[str, Any]]:
    htf = cycle_cache.get_df(symbol, cfg.htf_tf, cfg.htf_limit)
    ltf = cycle_cache.get_df(symbol, cfg.ltf_tf, cfg.ltf_limit)
    d1 = cycle_cache.get_df(symbol, cfg.d1_tf, cfg.d1_limit)
    if htf.empty or ltf.empty:
        return None
    htf = htf.iloc[:-1]
    ltf = ltf.iloc[:-1]
    if not d1.empty:
        d1 = d1.iloc[:-1]
    if len(htf) < max(cfg.supertrend_period + 2, 20) or len(ltf) < max(cfg.atr_period + cfg.atr_sma_period, 70):
        return None

    htf_highs = htf["high"].tolist()
    htf_lows = htf["low"].tolist()
    htf_closes = htf["close"].tolist()

    ltf_highs = ltf["high"].tolist()
    ltf_lows = ltf["low"].tolist()
    ltf_closes = ltf["close"].tolist()
    ltf_vols = ltf["volume"].tolist()

    trend_dir, _ = supertrend(htf_highs, htf_lows, htf_closes, cfg.supertrend_period, cfg.supertrend_mult)
    if not trend_dir:
        return None
    st_dir = trend_dir[-1]
    flip_window = 6
    st_flip_bear = False
    if st_dir == "DOWN" and len(trend_dir) >= flip_window:
        st_flip_bear = "UP" in trend_dir[-flip_window:]

    atr_vals = atr(ltf_highs, ltf_lows, ltf_closes, cfg.atr_period)
    atr_sma_vals = sma([v if v is not None else 0.0 for v in atr_vals], cfg.atr_sma_period)
    atr_now = atr_vals[-1] if atr_vals else None
    atr_sma = atr_sma_vals[-1] if atr_sma_vals else None
    atr_score = 30 if (atr_now is not None and atr_sma and atr_now > (atr_sma * cfg.atr_mult)) else 0
    atr_mult = (atr_now / atr_sma) if (atr_now is not None and atr_sma) else None

    vol_sma_vals = sma(ltf_vols, cfg.vol_sma_period)
    vol_now = ltf_vols[-1] if ltf_vols else None
    vol_sma = vol_sma_vals[-1] if vol_sma_vals else None
    vol_score = 30 if (vol_now is not None and vol_sma and vol_now > (vol_sma * cfg.vol_mult)) else 0
    vol_mult = (vol_now / vol_sma) if (vol_now is not None and vol_sma) else None
    d1_dist_atr = None
    if not d1.empty and len(d1) >= max(cfg.d1_ema_len, cfg.d1_atr_len) + 2:
        d1_highs = d1["high"].tolist()
        d1_lows = d1["low"].tolist()
        d1_closes = d1["close"].tolist()
        ema_vals = ema(d1_closes, cfg.d1_ema_len)
        atr_vals = atr(d1_highs, d1_lows, d1_closes, cfg.d1_atr_len)
        ema_now = ema_vals[-1] if ema_vals else None
        atr_now = atr_vals[-1] if atr_vals else None
        last_close = d1_closes[-1] if d1_closes else None
        if isinstance(ema_now, (int, float)) and isinstance(atr_now, (int, float)) and atr_now > 0 and last_close:
            d1_dist_atr = abs(float(last_close) - float(ema_now)) / float(atr_now)

    return {
        "htf_closes": htf_closes,
        "st_dir": st_dir,
        "st_flip_bear": st_flip_bear,
        "atr_now": atr_now,
        "atr_sma": atr_sma,
        "atr_score": atr_score,
        "atr_mult": atr_mult,
        "vol_now": vol_now,
        "vol_sma": vol_sma,
        "vol_score": vol_score,
        "vol_mult": vol_mult,
        "d1_dist_atr": d1_dist_atr,
    }


def evaluate_gate_long(symbol: str, cfg: Config) -> Dict[str, Any]:
    data = _load_gate_data(symbol, cfg)
    if not data:
        return {"status": "warmup"}
    ltf3 = cycle_cache.get_df(symbol, "3m", 20)
    if ltf3.empty or len(ltf3) < 10:
        return {"status": "warmup"}
    ltf3 = ltf3.iloc[:-1]
    ema7_vals = ema(ltf3["close"].tolist(), 7)
    if not ema7_vals:
        return {"status": "warmup"}
    ema7_val = ema7_vals[-1]
    last_close = ltf3["close"].tolist()[-1]
    if isinstance(ema7_val, (int, float)) and isinstance(last_close, (int, float)):
        if float(last_close) > float(ema7_val):
            return {
                "status": "ok",
                "trade_allowed": False,
                "allow_long": False,
                "allow_short": False,
                "size_mult": cfg.size_mult_base,
                "st_dir": data["st_dir"],
                "block_reason": "EMA7_DIR",
            }
    d1_dist_atr = data.get("d1_dist_atr")
    if isinstance(d1_dist_atr, (int, float)) and d1_dist_atr > cfg.d1_overext_atr_mult:
        return {
            "status": "ok",
            "trade_allowed": False,
            "allow_long": False,
            "allow_short": False,
            "size_mult": cfg.size_mult_base,
            "st_dir": data["st_dir"],
            "d1_dist_atr": d1_dist_atr,
            "block_reason": "D1_EMA7_DIST",
        }
    st_dir = data["st_dir"]
    allow_long = st_dir == "UP"
    trade_allowed = bool(st_dir)
    size_mult = cfg.size_mult_base
    atr_mult = data.get("atr_mult")
    vol_mult = data.get("vol_mult")
    if isinstance(atr_mult, (int, float)) and atr_mult >= 1.2:
        size_mult += cfg.size_mult_atr_boost
    if isinstance(vol_mult, (int, float)) and vol_mult >= 1.2:
        size_mult += cfg.size_mult_vol_boost
    size_mult = min(cfg.size_mult_max, size_mult)
    return {
        "status": "ok",
        "trade_allowed": trade_allowed,
        "allow_long": allow_long,
        "allow_short": False,
        "size_mult": size_mult,
        "st_dir": st_dir,
        "atr_now": data["atr_now"],
        "atr_sma": data["atr_sma"],
        "vol_now": data["vol_now"],
        "vol_sma": data["vol_sma"],
        "atr_mult": data["atr_mult"],
        "vol_mult": data["vol_mult"],
    }


def evaluate_gate_short(symbol: str, cfg: Config) -> Dict[str, Any]:
    data = _load_gate_data(symbol, cfg)
    if not data:
        return {"status": "warmup"}
    ltf3 = cycle_cache.get_df(symbol, "3m", 20)
    if ltf3.empty or len(ltf3) < 10:
        return {"status": "warmup"}
    ltf3 = ltf3.iloc[:-1]
    ema7_vals = ema(ltf3["close"].tolist(), 7)
    if not ema7_vals:
        return {"status": "warmup"}
    ema7_val = ema7_vals[-1]
    last_close = ltf3["close"].tolist()[-1]
    if isinstance(ema7_val, (int, float)) and isinstance(last_close, (int, float)):
        if float(last_close) < float(ema7_val):
            return {
                "status": "ok",
                "trade_allowed": False,
                "allow_long": False,
                "allow_short": False,
                "size_mult": cfg.size_mult_base,
                "st_dir": data["st_dir"],
                "block_reason": "EMA7_DIR",
            }
    d1_dist_atr = data.get("d1_dist_atr")
    if isinstance(d1_dist_atr, (int, float)) and d1_dist_atr > cfg.d1_overext_atr_mult:
        return {
            "status": "ok",
            "trade_allowed": False,
            "allow_long": False,
            "allow_short": False,
            "size_mult": cfg.size_mult_base,
            "st_dir": data["st_dir"],
            "d1_dist_atr": d1_dist_atr,
            "block_reason": "D1_EMA7_DIST",
        }
    st_dir = data["st_dir"]
    allow_short = st_dir == "DOWN"
    trade_allowed = bool(st_dir)
    size_mult = cfg.size_mult_base
    atr_mult = data.get("atr_mult")
    vol_mult = data.get("vol_mult")
    if isinstance(atr_mult, (int, float)) and atr_mult >= 1.2:
        size_mult += cfg.size_mult_atr_boost
    if isinstance(vol_mult, (int, float)) and vol_mult >= 1.2:
        size_mult += cfg.size_mult_vol_boost
    size_mult = min(cfg.size_mult_max, size_mult)
    return {
        "status": "ok",
        "trade_allowed": trade_allowed,
        "allow_long": False,
        "allow_short": allow_short,
        "size_mult": size_mult,
        "st_dir": st_dir,
        "atr_now": data["atr_now"],
        "atr_sma": data["atr_sma"],
        "vol_now": data["vol_now"],
        "vol_sma": data["vol_sma"],
        "atr_mult": data["atr_mult"],
        "vol_mult": data["vol_mult"],
        "st_flip_bear": data["st_flip_bear"],
    }


def evaluate_gate(symbol: str, cfg: Config) -> Dict[str, Any]:
    gate_long = evaluate_gate_long(symbol, cfg)
    if gate_long.get("status") != "ok":
        return gate_long
    gate_short = evaluate_gate_short(symbol, cfg)
    allow_long = bool(gate_long.get("allow_long"))
    allow_short = bool(gate_short.get("allow_short"))
    trade_allowed = bool(gate_long.get("trade_allowed") or gate_short.get("trade_allowed"))
    size_mult = max(
        float(gate_long.get("size_mult") or 0.0),
        float(gate_short.get("size_mult") or 0.0),
    )
    return {
        "status": "ok",
        "trade_allowed": trade_allowed,
        "allow_long": allow_long,
        "allow_short": allow_short,
        "size_mult": size_mult or cfg.size_mult_base,
        "st_dir": gate_long.get("st_dir") or gate_short.get("st_dir"),
        "atr_mult": gate_long.get("atr_mult") or gate_short.get("atr_mult"),
        "vol_mult": gate_long.get("vol_mult") or gate_short.get("vol_mult"),
    }
