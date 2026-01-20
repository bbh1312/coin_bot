from __future__ import annotations

from typing import Dict, Type

from engines.base import BaseEngine
from engines.rsi.engine import RsiEngine
try:
    from engines.swaggy.swaggy_engine import SwaggyEngine
except Exception:
    SwaggyEngine = None
from engines.atlas.atlas_engine import AtlasEngine
from engines.dtfx.engine import DTFXEngine
from engines.div15m_long.engine import Div15mLongEngine
from engines.div15m_short.engine import Div15mShortEngine
from engines.pumpfade.engine import PumpFadeEngine
from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine
from engines.swaggy_no_atlas.engine import SwaggyNoAtlasEngine


_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "rsi": RsiEngine,
    "atlas": AtlasEngine,
    "dtfx": DTFXEngine,
    "div15m_long": Div15mLongEngine,
    "div15m_short": Div15mShortEngine,
    "pumpfade": PumpFadeEngine,
    "atlas_rs_fail_short": AtlasRsFailShortEngine,
    "swaggy_no_atlas": SwaggyNoAtlasEngine,
}
if SwaggyEngine:
    _ENGINE_REGISTRY["swaggy"] = SwaggyEngine


def get_engine(name: str) -> BaseEngine:
    key = (name or "").strip().lower()
    if key in _ENGINE_REGISTRY:
        return _ENGINE_REGISTRY[key]()
    raise KeyError(f"unknown engine: {name}")


def list_engines() -> Dict[str, Type[BaseEngine]]:
    return dict(_ENGINE_REGISTRY)
