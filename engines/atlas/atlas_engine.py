from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import time

import numpy as np


@dataclass
class AtlasSwaggyConfig:
    ref_symbol: str = "BTC/USDT:USDT"
    ttl_sec: int = 600
    rs_n: int = 12
    rs_n_slow: int = 36
    corr_m: int = 32
    corr_m_slow: int = 96
    rs_pass: float = 0.0060
    rs_strong: float = 0.0120
    rs_z_pass: float = 0.75
    rs_z_strong: float = 1.25
    indep_corr: float = 0.55
    indep_beta: float = 0.80
    indep_strong_corr: float = 0.35
    indep_strong_beta: float = 0.60
    indep_risk_corr: float = 0.75
    indep_risk_beta: float = 1.10
    vol_pass: float = 1.3
    vol_override: float = 1.5
    exception_min_score: int = 3
    trend_norm_bull: float = 0.35
    trend_norm_bear: float = -0.35
    trend_norm_flat: float = 0.20
    chaos_atr_low: float = 0.85
    chaos_atr_high: float = 1.20
    chaos_trigger_strength_min: float = 0.60
    chaos_vol_ratio_min: float = 1.2
    override_max_dist_pct: float = 0.009


class AtlasEngine:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _ema(series, span: int):
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def _atr(df, n: int = 14):
        if df.empty or len(df) < (n + 2):
            return None
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        prev_close = close.shift(1)
        tr = np.maximum(high - low, np.maximum((high - prev_close).abs(), (low - prev_close).abs()))
        atr = tr.rolling(n).mean()
        return atr

    def compute_swaggy_global(self, state: Dict[str, Any], cycle_cache, cfg: AtlasSwaggyConfig) -> Dict[str, Any]:
        now = time.time()
        cached = state.get("_atlas_swaggy_gate")
        if isinstance(cached, dict):
            ts = float(cached.get("ts", 0.0) or 0.0)
            ttl = int(cached.get("ttl_sec", cfg.ttl_sec))
            if (now - ts) < ttl:
                return cached
        gate = {
            "trade_allowed": 0,
            "allow_long": 0,
            "allow_short": 0,
            "long_requires_exception": 0,
            "short_requires_exception": 0,
            "regime": "range",
            "confidence": 0.0,
            "reason": "warmup",
            "ttl_sec": cfg.ttl_sec,
            "ts": now,
        }
        df15 = cycle_cache.get_df(cfg.ref_symbol, "15m", 200)
        if df15.empty or len(df15) < 60:
            state["_atlas_swaggy_gate"] = gate
            return gate
        df15 = df15.iloc[:-1]
        if len(df15) < 60:
            state["_atlas_swaggy_gate"] = gate
            return gate
        ema20_15 = self._ema(df15["close"], 20)
        ema60_15 = self._ema(df15["close"], 60)
        if ema20_15.empty or ema60_15.empty:
            state["_atlas_swaggy_gate"] = gate
            return gate
        l_close = float(df15["close"].iloc[-1])
        l20 = float(ema20_15.iloc[-1])
        l60 = float(ema60_15.iloc[-1])
        atr14 = self._atr(df15, 14)
        atr_now = float(atr14.iloc[-1]) if atr14 is not None and not atr14.empty else None
        atr_sma = float(atr14.iloc[-20:].mean()) if atr14 is not None and len(atr14) >= 20 else None
        atr_ratio = (atr_now / atr_sma) if (atr_now and atr_sma and atr_sma > 0) else None
        trend_raw = (l20 - l60)
        denom = atr_now if (atr_now and atr_now > 0) else (l_close if l_close > 0 else 1.0)
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
                "long_requires_exception": 1,
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
        gate["atr"] = atr_now
        gate["atr_ratio"] = atr_ratio
        state["_atlas_swaggy_gate"] = gate
        return gate

    def _rs_metrics(self, symbol: str, cycle_cache, cfg: AtlasSwaggyConfig, rs_n: int, corr_m: int) -> Optional[Dict[str, Any]]:
        df = cycle_cache.get_df(symbol, "15m", 200)
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
        vol_sma = None
        if len(vols) >= 21:
            vol_sma = float(np.mean(vols[-20:]))
        vol_ratio = (float(vols[-1]) / vol_sma) if (vol_sma and vol_sma > 0) else None
        return {"rets": recent, "rs_window": rs_window, "vol_ratio": vol_ratio}

    def compute_swaggy_local(
        self,
        symbol: str,
        decision,
        gate: Dict[str, Any],
        cycle_cache,
        cfg: AtlasSwaggyConfig,
        swaggy_cfg,
    ) -> Dict[str, Any]:
        out = {
            "trade_allowed": gate.get("trade_allowed", 0),
            "allow_long": gate.get("allow_long", 0),
            "allow_short": gate.get("allow_short", 0),
            "long_size_mult": 1.0,
            "short_size_mult": 1.0,
            "exception_long": 0,
            "exception_reason": "",
            "atlas_long_exception": 0,
            "atlas_long_block_reason": "",
            "atlas_long_block_subreason": "",
            "atlas_short_quality": False,
            "atlas_short_quality_reason": "",
            "atlas_short_block_subreason": "",
            "atlas_short_block_reason": "",
            "rs": None,
            "rs_z": None,
            "corr": None,
            "beta": None,
            "vol_ratio": None,
            "state": gate.get("regime"),
        }
        side = ""
        if decision is not None:
            side = str(getattr(decision, "side", "") or "").lower()
        if not gate or gate.get("long_requires_exception") != 1:
            return out
        btc_fast = self._rs_metrics(cfg.ref_symbol, cycle_cache, cfg, cfg.rs_n, cfg.corr_m)
        alt_fast = self._rs_metrics(symbol, cycle_cache, cfg, cfg.rs_n, cfg.corr_m)
        btc_slow = self._rs_metrics(cfg.ref_symbol, cycle_cache, cfg, cfg.rs_n_slow, cfg.corr_m_slow)
        alt_slow = self._rs_metrics(symbol, cycle_cache, cfg, cfg.rs_n_slow, cfg.corr_m_slow)
        if not btc_fast or not alt_fast:
            return out
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
        beta_clamped = None
        if isinstance(beta_fast, (int, float)):
            beta_clamped = max(-3.0, min(3.0, beta_fast))
        vol_ratio = alt_fast.get("vol_ratio")
        out.update({
            "rs": rs_fast,
            "rs_z": rs_z_fast,
            "corr": corr_fast,
            "beta": beta_clamped,
            "vol_ratio": vol_ratio,
            "rs_slow": rs_slow,
            "rs_z_slow": rs_z_slow,
            "corr_slow": corr_slow,
            "beta_slow": beta_slow,
        })
        rs_pass_fast = (rs_fast is not None and rs_fast >= cfg.rs_pass) or (rs_z_fast is not None and rs_z_fast >= cfg.rs_z_pass)
        rs_pass_slow = (rs_slow is not None and rs_slow >= cfg.rs_pass) or (rs_z_slow is not None and rs_z_slow >= cfg.rs_z_pass)
        rs_strong_fast = (rs_fast is not None and rs_fast >= cfg.rs_strong) or (rs_z_fast is not None and rs_z_fast >= cfg.rs_z_strong)
        rs_strong_slow = (rs_slow is not None and rs_slow >= cfg.rs_strong) or (rs_z_slow is not None and rs_z_slow >= cfg.rs_z_strong)
        rs_pass = rs_pass_fast or rs_pass_slow
        rs_strong = rs_strong_fast and rs_strong_slow
        beta_fast_val = beta_clamped if beta_clamped is not None else beta_fast
        indep_pass_fast = (corr_fast is not None and corr_fast <= cfg.indep_corr) or (beta_fast_val is not None and beta_fast_val <= cfg.indep_beta)
        indep_pass_slow = (corr_slow is not None and corr_slow <= cfg.indep_corr) or (beta_slow is not None and beta_slow <= cfg.indep_beta)
        indep_pass = indep_pass_fast or indep_pass_slow
        indep_strong_fast = (corr_fast is not None and corr_fast <= cfg.indep_strong_corr) or (beta_fast_val is not None and beta_fast_val <= cfg.indep_strong_beta)
        indep_strong_slow = (corr_slow is not None and corr_slow <= cfg.indep_strong_corr) or (beta_slow is not None and beta_slow <= cfg.indep_strong_beta)
        indep_strong = indep_strong_fast and indep_strong_slow
        indep_risk_fast = (corr_fast is not None and corr_fast >= cfg.indep_risk_corr) or (beta_fast_val is not None and beta_fast_val >= cfg.indep_risk_beta)
        indep_risk_slow = (corr_slow is not None and corr_slow >= cfg.indep_risk_corr) or (beta_slow is not None and beta_slow >= cfg.indep_risk_beta)
        indep_risk = indep_risk_fast and indep_risk_slow
        vol_pass = (vol_ratio is not None and vol_ratio >= cfg.vol_pass)
        strong_ok = False
        dist_pct = None
        strength = None
        trigger = None
        if decision is not None:
            dbg = getattr(decision, "debug", {}) or {}
            dist_pct = dbg.get("dist_pct")
            strength = dbg.get("strength")
            trigger = dbg.get("trigger")
            cap = swaggy_cfg.max_dist_reclaim if trigger == "RECLAIM" else swaggy_cfg.max_dist
            strong_ok = (
                trigger in ("RECLAIM", "REJECTION")
                and isinstance(strength, (int, float)) and strength >= 0.50
                and (dist_pct is not None and dist_pct <= cap)
            )
            if gate.get("regime") == "chaos_vol":
                strong_ok = strong_ok and isinstance(strength, (int, float)) and strength >= cfg.chaos_trigger_strength_min
        score = 0
        reason_parts = []
        if rs_pass:
            score += 1
            reason_parts.append("RS")
        if indep_pass:
            score += 1
            reason_parts.append("INDEP")
        if vol_pass:
            score += 1
            reason_parts.append("VOL")
        if strong_ok:
            score += 1
            reason_parts.append("SWAGGY")
        exception = score >= cfg.exception_min_score
        override = (
            rs_fast is not None and rs_fast >= 0.015
            and vol_ratio is not None and vol_ratio >= cfg.vol_override
            and isinstance(strength, (int, float)) and strength >= 0.55
            and (dist_pct is None or dist_pct <= cfg.override_max_dist_pct)
        )
        if exception or override:
            if side == "long":
                out["exception_long"] = 1
                out["exception_reason"] = "+".join(reason_parts) or "OVERRIDE"
                out["atlas_long_exception"] = 1
                out["atlas_long_block_reason"] = ""
            else:
                out["exception_long"] = 0
                out["exception_reason"] = ""
                out["atlas_long_exception"] = 0
            if gate.get("regime") == "bear":
                out["long_size_mult"] = 0.3
                if rs_strong and indep_strong:
                    out["long_size_mult"] = 0.5
                if indep_risk:
                    out["long_size_mult"] = 0.25
            else:
                out["long_size_mult"] = 0.5
                if indep_risk:
                    out["long_size_mult"] = 0.25
            out["allow_long"] = 1
        if side == "long" and not out.get("allow_long"):
            out["atlas_long_exception"] = 0
            out["atlas_long_block_reason"] = "ATLAS_LONG_NO_EXCEPTION"
        out.update(
            {
                "dist_pct": dist_pct,
                "strength": strength,
                "trigger": trigger,
            }
        )
        if side == "short":
            short_quality = True
            reason = ""
            if not isinstance(vol_ratio, (int, float)) or vol_ratio < 1.0:
                short_quality = False
                reason = "VOL_TOO_LOW"
            elif gate.get("regime") == "bear":
                if corr_fast is None or beta_clamped is None or rs_fast is None:
                    short_quality = False
                    reason = "MISSING_METRIC"
                elif ((corr_fast is not None and corr_fast >= 0.30 and beta_clamped >= 0.80)
                      or (corr_slow is not None and beta_slow is not None and corr_slow >= 0.30 and beta_slow >= 0.80)):
                    short_quality = True
                    reason = "CORR_BETA_OK"
                elif (corr_fast is not None and corr_fast < 0.30) or (corr_slow is not None and corr_slow < 0.30):
                    if (rs_fast is not None and rs_fast <= -0.01) or (rs_slow is not None and rs_slow <= -0.01):
                        short_quality = True
                        reason = "RS_NEG_OK"
                    else:
                        short_quality = False
                        reason = "RS_WEAK"
                else:
                    short_quality = False
                    reason = "CORR_WEAK"
            elif gate.get("regime") == "chaos_range":
                if not (isinstance(vol_ratio, (int, float)) and vol_ratio >= cfg.chaos_vol_ratio_min):
                    short_quality = False
                    reason = "CHAOS_RANGE_VOL_LOW"
            out["atlas_short_quality"] = short_quality
            out["atlas_short_quality_reason"] = reason
            if not short_quality:
                if reason == "VOL_TOO_LOW":
                    out["atlas_short_block_subreason"] = "VOL_LOW"
                elif reason == "MISSING_METRIC":
                    out["atlas_short_block_subreason"] = "METRIC_MISSING"
                elif reason == "RS_WEAK":
                    out["atlas_short_block_subreason"] = "RSZ_BAD"
                elif reason == "CORR_WEAK":
                    out["atlas_short_block_subreason"] = "CORR_BAD"
                else:
                    out["atlas_short_block_subreason"] = "BETA_BAD"
                out["atlas_short_block_reason"] = "ATLAS_SHORT_BLOCK_QUALITY"
            if not short_quality:
                out["allow_short"] = 0
        self.enforce_long_quality(out, gate, decision, out)
        return out

    def _calc_trigger_strength_ok(self, trigger: Optional[str], strength: Optional[float]) -> bool:
        if trigger is None or strength is None:
            return False
        mapping = {"RECLAIM": 0.55, "RETEST": 0.50, "REJECTION": 0.60}
        if trigger.upper() == "SWEEP":
            return False
        req = mapping.get(trigger.upper(), 1.0)
        return strength >= req

    def enforce_long_quality(self, out: Dict[str, Any], gate: Dict[str, Any], decision, atlas_local: Dict[str, Any]) -> None:
        regime = gate.get("regime")
        if regime not in ("bear", "chaos_range", "chaos_vol"):
            return
        rs_z = atlas_local.get("rs_z")
        beta = atlas_local.get("beta")
        vol_ratio = atlas_local.get("vol_ratio")
        dist_pct = atlas_local.get("dist_pct") or 0.0
        strength = atlas_local.get("strength")
        trigger = atlas_local.get("trigger")
        quality_ok = (
            isinstance(rs_z, (int, float)) and rs_z >= 1.2
            and isinstance(beta, (int, float)) and beta <= 0.55
            and isinstance(vol_ratio, (int, float)) and vol_ratio >= 1.0
            and self._calc_trigger_strength_ok(trigger, strength)
        )
        atlas_local["atlas_long_quality_ok"] = quality_ok
        if not quality_ok:
            out["allow_long"] = 0
            atlas_local["atlas_long_block_reason"] = "ATLAS_LONG_BLOCK_QUALITY"
            if not (isinstance(rs_z, (int, float)) and rs_z >= 1.2):
                atlas_local["atlas_long_block_subreason"] = "RSZ_LOW"
            elif not (isinstance(beta, (int, float)) and beta <= 0.55):
                atlas_local["atlas_long_block_subreason"] = "BETA_HIGH"
            elif not (isinstance(vol_ratio, (int, float)) and vol_ratio >= 1.0):
                atlas_local["atlas_long_block_subreason"] = "VOL_LOW"
            elif not self._calc_trigger_strength_ok(trigger, strength):
                atlas_local["atlas_long_block_subreason"] = "SWAGGY_WEAK"

    def evaluate_fabio_gate_long(self, symbol: str, cfg) -> Optional[Dict[str, Any]]:
        try:
            from engines.fabio import atlas_fabio_engine
        except Exception:
            return None
        return atlas_fabio_engine.evaluate_gate_long(symbol, cfg)

    def evaluate_fabio_gate_short(self, symbol: str, cfg) -> Optional[Dict[str, Any]]:
        try:
            from engines.fabio import atlas_fabio_engine
        except Exception:
            return None
        return atlas_fabio_engine.evaluate_gate_short(symbol, cfg)

    def evaluate_fabio_gate(self, symbol: str, cfg) -> Optional[Dict[str, Any]]:
        try:
            from engines.fabio import atlas_fabio_engine
        except Exception:
            return None
        return atlas_fabio_engine.evaluate_gate(symbol, cfg)
