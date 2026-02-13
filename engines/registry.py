from __future__ import annotations

from typing import Dict, Type

from engines.base import BaseEngine
from engines.scout_only_exhaustion_short.engine import ScoutOnlyExhaustionShortEngine
from engines.sr_pro_long_v1.engine import SrProLongV1Engine
from engines.sr_pro_short_v1.engine import SrProShortV1Engine
from engines.sr_pro_short_v2.engine import SrProShortV2Engine


_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "scout_only_exhaustion_short": ScoutOnlyExhaustionShortEngine,
    "sr_pro_long_v1": SrProLongV1Engine,
    "sr_pro_short_v1": SrProShortV1Engine,
    "sr_pro_short_v2": SrProShortV2Engine,
}


def get_engine(name: str) -> BaseEngine:
    key = (name or "").strip().lower()
    if key in _ENGINE_REGISTRY:
        return _ENGINE_REGISTRY[key]()
    raise KeyError(f"unknown engine: {name}")


def list_engines() -> Dict[str, Type[BaseEngine]]:
    return dict(_ENGINE_REGISTRY)
