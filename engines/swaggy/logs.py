from __future__ import annotations

from typing import Dict, List


def format_universe_log(mode: str, scan: int, unique: int, filled: int) -> str:
    return f"[universe] mode=swaggy scan={scan} unique={unique} filled={filled}"


def format_universe_sample(top3: List[str], bottom3: List[str]) -> str:
    return f"[universe-sample] top3={top3} bottom3={bottom3}"


def format_cut_top(
    trigger_counts: Dict[str, int],
    filter_counts: Dict[str, int],
    gate_fail_counts: Dict[str, int] | None = None,
    gate_stage_counts: Dict[str, int] | None = None,
) -> str:
    strong = (
        f"reclaim={trigger_counts.get('RECLAIM', 0)} "
        f"rejection={trigger_counts.get('REJECTION', 0)} "
        f"sweep={trigger_counts.get('SWEEP', 0)} "
        f"retest={trigger_counts.get('RETEST', 0)}"
    )
    filters = (
        f"dist={filter_counts.get('dist', 0)} "
        f"lvn_gap={filter_counts.get('lvn_gap', 0)} "
        f"expansion={filter_counts.get('expansion', 0)} "
        f"cooldown={filter_counts.get('cooldown', 0)} "
        f"regime={filter_counts.get('regime', 0)}"
    )
    gate_fail_top = ""
    if isinstance(gate_fail_counts, dict) and gate_fail_counts:
        items = sorted(gate_fail_counts.items(), key=lambda x: x[1], reverse=True)[:6]
        gate_fail_top = " | " + "gate_fail_top: " + " ".join([f"{k}={v}" for k, v in items])
    gate_stage_hist = ""
    if isinstance(gate_stage_counts, dict) and gate_stage_counts:
        items = sorted(gate_stage_counts.items(), key=lambda x: x[0])
        gate_stage_hist = " | " + "gate_stage: " + " ".join([f"{k}={v}" for k, v in items])
    return f"[swaggy-cut-top] strong: {strong} | filters: {filters}{gate_fail_top}{gate_stage_hist}"


def format_zone_stats(stats: Dict[str, int], ready_fail_counts: Dict[str, int] | None = None) -> str:
    ready_fail_top = ""
    if isinstance(ready_fail_counts, dict) and ready_fail_counts:
        items = sorted(ready_fail_counts.items(), key=lambda x: x[1], reverse=True)[:6]
        ready_fail_top = " | " + "ready_fail_top: " + " ".join([f"{k}={v}" for k, v in items])
    return (
        "[swaggy-zone-stats] "
        f"touch={stats.get('touch', 0)} "
        f"trigger={stats.get('trigger', 0)} "
        f"entry_ready={stats.get('entry_ready', 0)} "
        f"reject={stats.get('reject', 0)} "
        f"reclaim={stats.get('reclaim', 0)}"
        f"{ready_fail_top}"
    )


def format_debug(
    sym: str,
    px: float,
    regime: str,
    level: float,
    vp: Dict[str, float],
    trigger_kind: str,
    side: str,
    strength: float,
    dist_pct: float,
    in_lvn_gap: int,
    expansion: int,
    decision: str,
    reason: str,
) -> str:
    return (
        "[swaggy-zone-debug] "
        f"sym={sym} px={px:.6g} regime={regime} level={level:.6g} "
        f"vp: lvn={vp.get('lvn', '-')} hvn={vp.get('hvn', '-')} poc={vp.get('poc', '-')} val={vp.get('val', '-')}"
        f" trigger={trigger_kind} side={side} strength={strength:.2f} "
        f"dist_pct={dist_pct:.3f} in_lvn_gap={in_lvn_gap} expansion={expansion} "
        f"decision={decision} reason={reason}"
    )
