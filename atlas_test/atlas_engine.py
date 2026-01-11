from typing import Dict, Any, Optional

import cycle_cache
import numpy as np

from engines.atlas.atlas_engine import AtlasEngine, AtlasSwaggyConfig
from .atlas_config import Config
from .data_feed import fetch_ohlcv


atlas_engine = AtlasEngine()
atlas_swaggy_cfg = AtlasSwaggyConfig()
atlas_state: Dict[str, Any] = {}


def set_cache(symbol: str, tf: str, data: list) -> None:
    if not data:
        return
    formatted = [[c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"]] for c in data]
    cycle_cache.set_raw(symbol, tf, formatted)


def _score_from_atlas(atlas_gate: Dict[str, Any], atlas_local: Dict[str, Any], cfg: AtlasSwaggyConfig) -> int:
    score = 0
    regime = atlas_gate.get("regime")
    if regime in ("bull", "bear"):
        score += 25
    elif regime == "chaos_vol":
        score += 15
    else:
        score += 5
    rs = atlas_local.get("rs")
    rs_z = atlas_local.get("rs_z")
    rs_slow = atlas_local.get("rs_slow")
    rs_z_slow = atlas_local.get("rs_z_slow")
    rs_pass = (
        (isinstance(rs, (int, float)) and rs >= cfg.rs_pass)
        or (isinstance(rs_z, (int, float)) and rs_z >= cfg.rs_z_pass)
        or (isinstance(rs_slow, (int, float)) and rs_slow >= cfg.rs_pass)
        or (isinstance(rs_z_slow, (int, float)) and rs_z_slow >= cfg.rs_z_pass)
    )
    if rs_pass:
        score += 25
    corr = atlas_local.get("corr")
    beta = atlas_local.get("beta")
    corr_slow = atlas_local.get("corr_slow")
    beta_slow = atlas_local.get("beta_slow")
    indep_pass = (
        (isinstance(corr, (int, float)) and corr <= cfg.indep_corr)
        or (isinstance(beta, (int, float)) and beta <= cfg.indep_beta)
        or (isinstance(corr_slow, (int, float)) and corr_slow <= cfg.indep_corr)
        or (isinstance(beta_slow, (int, float)) and beta_slow <= cfg.indep_beta)
    )
    if indep_pass:
        score += 25
    vol_ratio = atlas_local.get("vol_ratio")
    if isinstance(vol_ratio, (int, float)) and vol_ratio >= cfg.vol_pass:
        score += 25
    return score


def _state_from_atlas(atlas_local: Dict[str, Any]) -> str:
    allow_long = int(atlas_local.get("allow_long", 0) or 0)
    allow_short = int(atlas_local.get("allow_short", 0) or 0)
    if allow_long and not allow_short:
        return "STRONG_BULL"
    if allow_short and not allow_long:
        return "STRONG_BEAR"
    return "NO_TRADE"


def _rs_metrics(symbol: str, cfg: AtlasSwaggyConfig, rs_n: int, corr_m: int) -> Optional[Dict[str, Any]]:
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
    vol_sma = float(np.mean(vols[-20:])) if len(vols) >= 21 else None
    vol_ratio = (float(vols[-1]) / vol_sma) if (vol_sma and vol_sma > 0) else None
    return {"rets": recent, "rs_window": rs_window, "vol_ratio": vol_ratio}


def compute_atlas_local(symbol: str, gate: Dict[str, Any], cfg: AtlasSwaggyConfig) -> Dict[str, Any]:
    out = {
        "trade_allowed": gate.get("trade_allowed", 0),
        "allow_long": gate.get("allow_long", 0),
        "allow_short": gate.get("allow_short", 0),
        "rs": None,
        "rs_z": None,
        "corr": None,
        "beta": None,
        "vol_ratio": None,
        "rs_slow": None,
        "rs_z_slow": None,
        "corr_slow": None,
        "beta_slow": None,
        "state": gate.get("regime"),
        "symbol_direction": "UNKNOWN",
    }
    btc_fast = _rs_metrics(cfg.ref_symbol, cfg, cfg.rs_n, cfg.corr_m)
    alt_fast = _rs_metrics(symbol, cfg, cfg.rs_n, cfg.corr_m)
    btc_slow = _rs_metrics(cfg.ref_symbol, cfg, cfg.rs_n_slow, cfg.corr_m_slow)
    alt_slow = _rs_metrics(symbol, cfg, cfg.rs_n_slow, cfg.corr_m_slow)
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
    if isinstance(rs_fast, (int, float)):
        if rs_fast > 0:
            out["symbol_direction"] = "BULL"
        elif rs_fast < 0:
            out["symbol_direction"] = "BEAR"
        else:
            out["symbol_direction"] = "NEUTRAL"
    return out


def evaluate_atlas(exchange, cfg: Config, atlas_gate: Dict[str, Any]) -> Dict[str, Any]:
    ltf = fetch_ohlcv(exchange, cfg.symbol, cfg.ltf_tf, cfg.ltf_limit, drop_last=True)
    if len(ltf) < 120:
        return {"status": "warmup"}
    set_cache(cfg.symbol, cfg.ltf_tf, ltf)
    atlas_local = compute_atlas_local(cfg.symbol, atlas_gate, atlas_swaggy_cfg)
    state = _state_from_atlas(atlas_local)
    score = _score_from_atlas(atlas_gate, atlas_local, atlas_swaggy_cfg)
    ltf_ts = ltf[-1]["ts"] if ltf else None
    return {
        "status": "ok",
        "state": state,
        "score": score,
        "ltf_ts": ltf_ts,
        "atlas_gate": atlas_gate,
        "atlas_local": atlas_local,
    }


def compute_global_gate() -> Dict[str, Any]:
    if not atlas_engine:
        return {}
    atlas_engine.compute_swaggy_global(atlas_state, cycle_cache, atlas_swaggy_cfg)
    return atlas_state.get("_atlas_swaggy_gate") or {}


def get_ref_symbol() -> str:
    return atlas_swaggy_cfg.ref_symbol
