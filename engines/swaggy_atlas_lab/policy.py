from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from engines.swaggy_atlas_lab.atlas_eval import AtlasDecision


class AtlasMode(str, Enum):
    HARD = "hard"
    SOFT = "soft"
    SHADOW = "shadow"
    OFF = "off"


@dataclass
class PolicyDecision:
    allow: bool
    final_usdt: float
    atlas_pass: Optional[bool]
    atlas_mult: Optional[float]
    atlas_reasons: Optional[list[str]]
    shadow_pass: Optional[bool]
    shadow_mult: Optional[float]
    shadow_reasons: Optional[list[str]]


def apply_policy(
    mode: AtlasMode,
    base_usdt: float,
    atlas: Optional[AtlasDecision],
) -> PolicyDecision:
    if mode == AtlasMode.OFF or atlas is None:
        return PolicyDecision(
            allow=True,
            final_usdt=base_usdt,
            atlas_pass=None,
            atlas_mult=None,
            atlas_reasons=None,
            shadow_pass=None,
            shadow_mult=None,
            shadow_reasons=None,
        )
    if mode == AtlasMode.SHADOW:
        return PolicyDecision(
            allow=True,
            final_usdt=base_usdt,
            atlas_pass=None,
            atlas_mult=None,
            atlas_reasons=None,
            shadow_pass=atlas.pass_hard,
            shadow_mult=atlas.atlas_mult,
            shadow_reasons=atlas.reasons,
        )
    if mode == AtlasMode.SOFT:
        mult = atlas.atlas_mult if atlas.atlas_mult else 1.0
        return PolicyDecision(
            allow=True,
            final_usdt=base_usdt * mult,
            atlas_pass=atlas.pass_hard,
            atlas_mult=atlas.atlas_mult,
            atlas_reasons=atlas.reasons,
            shadow_pass=None,
            shadow_mult=None,
            shadow_reasons=None,
        )
    # HARD
    if atlas.pass_hard:
        mult = atlas.atlas_mult if atlas.atlas_mult else 1.0
        return PolicyDecision(
            allow=True,
            final_usdt=base_usdt * mult,
            atlas_pass=True,
            atlas_mult=atlas.atlas_mult,
            atlas_reasons=atlas.reasons,
            shadow_pass=None,
            shadow_mult=None,
            shadow_reasons=None,
        )
    return PolicyDecision(
        allow=False,
        final_usdt=0.0,
        atlas_pass=False,
        atlas_mult=atlas.atlas_mult,
        atlas_reasons=atlas.reasons,
        shadow_pass=None,
        shadow_mult=None,
        shadow_reasons=None,
    )
