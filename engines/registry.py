from __future__ import annotations

from typing import Dict, Type

from engines.base import BaseEngine
from engines.bb_reject_short_1h3m.engine import BbRejectShort1h3mEngine
from engines.regime_switch_scalp_v1.engine import RegimeSwitchScalpV1Engine
from engines.sr_pro_long_v1.engine import SrProLongV1Engine
from engines.sr_pro_short_v1.engine import SrProShortV1Engine
from engines.trend_resistance_short.engine import TrendResistanceShortEngine
from engines.trend_support_long.engine import TrendSupportLongEngine
from engines.sr_pro_long_v3.engine import SrProLongV3Engine


_ENGINE_REGISTRY: Dict[str, Type[BaseEngine]] = {
    "bb_reject_long_1h3m": BbRejectShort1h3mEngine,
    "bb_reject_short_1h3m": BbRejectShort1h3mEngine,
    "regime_switch_scalp_v1": RegimeSwitchScalpV1Engine,
    "sr_pro_long_v1": SrProLongV1Engine,
    "sr_pro_short_v1": SrProShortV1Engine,
    "trend_resistance_short": TrendResistanceShortEngine,
    "trend_support_long": TrendSupportLongEngine,
    "sr_pro_long_v3": SrProLongV3Engine,
}


def get_engine(name: str) -> BaseEngine:
    key = (name or "").strip().lower()
    if key in _ENGINE_REGISTRY:
        return _ENGINE_REGISTRY[key]()
    raise KeyError(f"unknown engine: {name}")


def list_engines() -> Dict[str, Type[BaseEngine]]:
    return dict(_ENGINE_REGISTRY)
