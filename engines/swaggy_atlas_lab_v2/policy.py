from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from engines.swaggy_atlas_lab_v2.atlas_eval import AtlasDecision


class AtlasMode(str, Enum):
    HARD = "hard"
    SOFT = "soft"
    SHADOW = "shadow"
    OFF = "off"
    HYBRID = "hybrid"


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
    policy_action: Optional[str]


_POLICY_RULES = [
    ("ATLAS_SIDE_BLOCK", "BLOCK", None),
    ("ATLAS_BLOCK", "BLOCK", None),
    ("QUALITY_FAIL", "SCALE", 0.5),
    ("RS", "SCALE", 0.7),
    ("INDEP", "SCALE", 0.7),
    ("VOL", "SCALE", 0.8),
]
_POLICY_PRIORITY = {"BLOCK": 3, "CAP": 2, "SCALE": 1, "PASS": 0}


def _match_reason(reasons: Optional[list[str]], key: str) -> bool:
    if not reasons:
        return False
    for reason in reasons:
        if key in str(reason):
            return True
    return False


def _policy_action_from_reasons(reasons: Optional[list[str]]) -> tuple[str, Optional[float]]:
    best_action = "PASS"
    best_value: Optional[float] = None
    for key, action, value in _POLICY_RULES:
        if _match_reason(reasons, key):
            if _POLICY_PRIORITY.get(action, 0) > _POLICY_PRIORITY.get(best_action, 0):
                best_action = action
                best_value = value
    return best_action, best_value


def _format_action(action: str, value: Optional[float]) -> str:
    if action in ("SCALE", "CAP") and value is not None:
        return f"{action}_{value:.3g}"
    return action


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
            policy_action="OFF",
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
            policy_action="SHADOW",
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
            policy_action=f"SOFT_{mult:.3g}",
        )
    if mode == AtlasMode.HYBRID:
        action, value = _policy_action_from_reasons(atlas.reasons)
        if action == "BLOCK":
            return PolicyDecision(
                allow=False,
                final_usdt=0.0,
                atlas_pass=atlas.pass_hard,
                atlas_mult=atlas.atlas_mult,
                atlas_reasons=atlas.reasons,
                shadow_pass=None,
                shadow_mult=None,
                shadow_reasons=None,
                policy_action=_format_action(action, value),
            )
        if action == "CAP" and value is not None:
            final_usdt = min(base_usdt, base_usdt * value)
            return PolicyDecision(
                allow=True,
                final_usdt=final_usdt,
                atlas_pass=atlas.pass_hard,
                atlas_mult=atlas.atlas_mult,
                atlas_reasons=atlas.reasons,
                shadow_pass=None,
                shadow_mult=None,
                shadow_reasons=None,
                policy_action=_format_action(action, value),
            )
        if action == "SCALE" and value is not None:
            return PolicyDecision(
                allow=True,
                final_usdt=base_usdt * value,
                atlas_pass=atlas.pass_hard,
                atlas_mult=atlas.atlas_mult,
                atlas_reasons=atlas.reasons,
                shadow_pass=None,
                shadow_mult=None,
                shadow_reasons=None,
                policy_action=_format_action(action, value),
            )
        return PolicyDecision(
            allow=True,
            final_usdt=base_usdt,
            atlas_pass=atlas.pass_hard,
            atlas_mult=atlas.atlas_mult,
            atlas_reasons=atlas.reasons,
            shadow_pass=None,
            shadow_mult=None,
            shadow_reasons=None,
            policy_action=_format_action(action, value),
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
            policy_action=f"HARD_{mult:.3g}",
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
        policy_action="HARD_BLOCK",
    )
