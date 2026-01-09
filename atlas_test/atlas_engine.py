from typing import Dict, Any

import cycle_cache

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


def evaluate_atlas(exchange, cfg: Config, atlas_gate: Dict[str, Any]) -> Dict[str, Any]:
    ltf = fetch_ohlcv(exchange, cfg.symbol, cfg.ltf_tf, cfg.ltf_limit, drop_last=True)
    if len(ltf) < 120:
        return {"status": "warmup"}
    set_cache(cfg.symbol, cfg.ltf_tf, ltf)
    atlas_local = atlas_engine.compute_swaggy_local(cfg.symbol, None, atlas_gate, cycle_cache, atlas_swaggy_cfg, None) if atlas_engine else {}
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
