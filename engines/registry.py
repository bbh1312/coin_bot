from __future__ import annotations

from typing import Dict, Type

from engines.base import BaseEngine
from engines.rsi.engine import RsiEngine
from engines.swaggy.swaggy_engine import SwaggyEngine
from engines.atlas.atlas_engine import AtlasEngine
from engines.dtfx.engine import DTFXEngine
from engines.div15m_long.engine import Div15mLongEngine


_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "rsi": RsiEngine,
    "swaggy": SwaggyEngine,
    "atlas": AtlasEngine,
    "dtfx": DTFXEngine,
    "div15m_long": Div15mLongEngine,
}


def get_engine(name: str) -> BaseEngine:
    key = (name or "").strip().lower()
    if key in _ENGINE_REGISTRY:
        return _ENGINE_REGISTRY[key]()
    raise KeyError(f"unknown engine: {name}")


def list_engines() -> Dict[str, Type[BaseEngine]]:
    return dict(_ENGINE_REGISTRY)
