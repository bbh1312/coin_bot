from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from engines.swaggy_atlas_lab.config import AtlasConfig
from engines.swaggy_atlas_lab.indicators import atr, ema


@dataclass
class AtlasDecision:
    pass_hard: bool
    atlas_mult: float
    strength: float
    reasons: list[str]
    metrics: Dict[str, Any]


def _rs_metrics(df: pd.DataFrame, rs_n: int, corr_m: int) -> Optional[Dict[str, Any]]:
    if df.empty or len(df) < (corr_m + 2):
        return None
    df = df.iloc[:-1]
    if len(df) < (corr_m + 2):
        return None
    closes = df["close"].astype(float).tolist()
    vols = df["volume"].astype(float).tolist()
    rets = np.diff(np.log(np.clip(closes, 1e-12, None)))
    if len(rets) < corr_m:
        return None
    recent = rets[-corr_m:]
    rs_window = rets[-rs_n:]
    vol_sma = float(np.mean(vols[-20:])) if len(vols) >= 21 else None
    vol_ratio = (float(vols[-1]) / vol_sma) if (vol_sma and vol_sma > 0) else None
    return {"rets": recent, "rs_window": rs_window, "vol_ratio": vol_ratio}


def evaluate_global_gate(
    df_btc_15m: pd.DataFrame,
    cfg: AtlasConfig,
) -> Dict[str, Any]:
    gate = {
        "trade_allowed": 0,
        "allow_long": 0,
        "allow_short": 0,
        "long_requires_exception": 0,
        "short_requires_exception": 0,
        "regime": "range",
        "confidence": 0.0,
        "reason": "warmup",
    }
    if df_btc_15m.empty or len(df_btc_15m) < 60:
        return gate
    df15 = df_btc_15m.iloc[:-1]
    if len(df15) < 60:
        return gate
    ema20 = ema(df15["close"], 20)
    ema60 = ema(df15["close"], 60)
    if ema20.empty or ema60.empty:
        return gate
    l_close = float(df15["close"].iloc[-1])
    l20 = float(ema20.iloc[-1])
    l60 = float(ema60.iloc[-1])
    atr14 = atr(df15, 14)
    atr_sma = None
    if len(df15) >= 34:
        atr_sma = float(df15["high"].astype(float).rolling(14).mean().iloc[-20:].mean())
    atr_ratio = (atr14 / atr_sma) if (atr14 and atr_sma and atr_sma > 0) else None
    trend_raw = (l20 - l60)
    denom = atr14 if (atr14 and atr14 > 0) else (l_close if l_close > 0 else 1.0)
    trend_norm = trend_raw / denom
    if l20 > l60 and trend_norm >= cfg.trend_norm_bull:
        gate.update({
            "trade_allowed": 1,
            "allow_long": 1,
            "allow_short": 0,
            "long_requires_exception": 0,
            "regime": "bull",
            "confidence": 0.8,
            "reason": "HTF_BULL_OK",
        })
    elif l20 < l60 and trend_norm <= cfg.trend_norm_bear:
        gate.update({
            "trade_allowed": 1,
            "allow_short": 1,
            "allow_long": 0,
            "short_requires_exception": 0,
            "regime": "bear",
            "confidence": 0.8,
            "reason": "HTF_BEAR_OK",
        })
    else:
        if abs(trend_norm) <= cfg.trend_norm_flat and (atr_ratio is not None and atr_ratio < cfg.chaos_atr_low):
            gate.update({
                "trade_allowed": 1,
                "allow_short": 0,
                "allow_long": 0,
                "long_requires_exception": 1,
                "short_requires_exception": 1,
                "regime": "chaos_range",
                "confidence": 0.2,
                "reason": "CHAOS_RANGE",
            })
        else:
            gate.update({
                "trade_allowed": 1,
                "allow_short": 1,
                "allow_long": 1,
                "long_requires_exception": 1,
                "short_requires_exception": 1,
                "regime": "chaos_vol",
                "confidence": 0.4,
                "reason": "CHAOS_VOL",
            })
    gate["trend_norm"] = trend_norm
    gate["atr"] = atr14
    gate["atr_ratio"] = atr_ratio
    return gate


def evaluate_local(
    symbol: str,
    side: str,
    df_sym_15m: pd.DataFrame,
    df_btc_15m: pd.DataFrame,
    gate: Dict[str, Any],
    cfg: AtlasConfig,
) -> AtlasDecision:
    side = (side or "").upper()
    reasons: list[str] = []
    metrics: Dict[str, Any] = {"regime": gate.get("regime")}
    if not gate or not gate.get("trade_allowed"):
        return AtlasDecision(False, 0.0, 0.0, ["ATLAS_BLOCK"], metrics)
    if side == "LONG" and not gate.get("allow_long"):
        return AtlasDecision(False, 0.0, 0.0, ["ATLAS_SIDE_BLOCK"], metrics)
    if side == "SHORT" and not gate.get("allow_short"):
        return AtlasDecision(False, 0.0, 0.0, ["ATLAS_SIDE_BLOCK"], metrics)

    btc_fast = _rs_metrics(df_btc_15m, cfg.rs_n, cfg.corr_m)
    alt_fast = _rs_metrics(df_sym_15m, cfg.rs_n, cfg.corr_m)
    btc_slow = _rs_metrics(df_btc_15m, cfg.rs_n_slow, cfg.corr_m_slow)
    alt_slow = _rs_metrics(df_sym_15m, cfg.rs_n_slow, cfg.corr_m_slow)
    if not btc_fast or not alt_fast:
        return AtlasDecision(False, 0.0, 0.0, ["METRIC_MISSING"], metrics)

    rs_alt_fast = float(np.sum(alt_fast["rs_window"]))
    rs_btc_fast = float(np.sum(btc_fast["rs_window"]))
    rs_fast = rs_alt_fast - rs_btc_fast
    sigma_fast = float(np.std(btc_fast["rets"][-cfg.corr_m:])) if btc_fast["rets"] is not None else 0.0
    rs_z_fast = (rs_fast / (sigma_fast * np.sqrt(cfg.rs_n))) if sigma_fast > 0 else None
    corr_fast = None
    if len(alt_fast["rets"]) == len(btc_fast["rets"]):
        std_alt = float(np.std(alt_fast["rets"]))
        std_btc = float(np.std(btc_fast["rets"]))
        if std_alt > 1e-12 and std_btc > 1e-12:
            corr_fast = float(np.corrcoef(alt_fast["rets"], btc_fast["rets"])[0, 1])
    var_btc_fast = float(np.var(btc_fast["rets"])) if btc_fast["rets"] is not None else 0.0
    beta_fast = float(np.cov(alt_fast["rets"], btc_fast["rets"])[0, 1] / var_btc_fast) if var_btc_fast > 1e-12 else None
    rs_slow = None
    rs_z_slow = None
    corr_slow = None
    beta_slow = None
    if btc_slow and alt_slow:
        rs_alt_slow = float(np.sum(alt_slow["rs_window"]))
        rs_btc_slow = float(np.sum(btc_slow["rs_window"]))
        rs_slow = rs_alt_slow - rs_btc_slow
        sigma_slow = float(np.std(btc_slow["rets"][-cfg.corr_m_slow:])) if btc_slow["rets"] is not None else 0.0
        rs_z_slow = (rs_slow / (sigma_slow * np.sqrt(cfg.rs_n_slow))) if sigma_slow > 0 else None
        if len(alt_slow["rets"]) == len(btc_slow["rets"]):
            std_alt = float(np.std(alt_slow["rets"]))
            std_btc = float(np.std(btc_slow["rets"]))
            if std_alt > 1e-12 and std_btc > 1e-12:
                corr_slow = float(np.corrcoef(alt_slow["rets"], btc_slow["rets"])[0, 1])
        var_btc_slow = float(np.var(btc_slow["rets"])) if btc_slow["rets"] is not None else 0.0
        beta_slow = float(np.cov(alt_slow["rets"], btc_slow["rets"])[0, 1] / var_btc_slow) if var_btc_slow > 1e-12 else None

    vol_ratio = alt_fast.get("vol_ratio")
    metrics.update({
        "rs": rs_fast,
        "rs_z": rs_z_fast,
        "corr": corr_fast,
        "beta": beta_fast,
        "vol_ratio": vol_ratio,
        "rs_slow": rs_slow,
        "rs_z_slow": rs_z_slow,
        "corr_slow": corr_slow,
        "beta_slow": beta_slow,
    })

    rs_pass_fast = (rs_fast is not None and rs_fast >= cfg.rs_pass) or (rs_z_fast is not None and rs_z_fast >= cfg.rs_z_pass)
    rs_pass_slow = (rs_slow is not None and rs_slow >= cfg.rs_pass) or (rs_z_slow is not None and rs_z_slow >= cfg.rs_z_pass)
    rs_pass = rs_pass_fast or rs_pass_slow
    indep_pass_fast = (corr_fast is not None and corr_fast <= cfg.indep_corr) or (beta_fast is not None and beta_fast <= cfg.indep_beta)
    indep_pass_slow = (corr_slow is not None and corr_slow <= cfg.indep_corr) or (beta_slow is not None and beta_slow <= cfg.indep_beta)
    indep_pass = indep_pass_fast or indep_pass_slow
    vol_pass = (vol_ratio is not None and vol_ratio >= cfg.vol_pass)

    score = 0
    if rs_pass:
        score += 1
        reasons.append("RS")
    if indep_pass:
        score += 1
        reasons.append("INDEP")
    if vol_pass:
        score += 1
        reasons.append("VOL")

    pass_hard = score >= cfg.exception_min_score
    pass_soft = score >= 1
    if not pass_hard:
        reasons.append("QUALITY_FAIL")

    atlas_mult = 1.0
    if pass_hard:
        if score >= cfg.exception_min_score + 1:
            atlas_mult = 1.2
        elif score == cfg.exception_min_score:
            atlas_mult = 1.0
    strength = float(score) / max(1.0, float(cfg.exception_min_score))
    metrics["pass_soft"] = pass_soft
    metrics["pass_hard"] = pass_hard
    return AtlasDecision(pass_hard, atlas_mult, strength, reasons, metrics)
