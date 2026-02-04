from __future__ import annotations

from typing import Dict, Type

from engines.base import BaseEngine
from engines.rsi.engine import RsiEngine
try:
    from engines.swaggy.swaggy_engine import SwaggyEngine
except Exception:
    SwaggyEngine = None
from engines.atlas.atlas_engine import AtlasEngine
from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine
from engines.st_flip_v1.engine import StFlipEngine


_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "rsi": RsiEngine,
    "atlas": AtlasEngine,
    "atlas_rs_fail_short": AtlasRsFailShortEngine,
    "st_flip_v1": StFlipEngine,
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
