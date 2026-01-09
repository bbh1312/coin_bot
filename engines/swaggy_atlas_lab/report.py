from __future__ import annotations

import csv
import json
import os
from typing import Dict, List

from engines.swaggy_atlas_lab.metrics import compute_metrics


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def write_trades_csv(path: str, trades: List[Dict]) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "run_id",
                "mode",
                "sym",
                "side",
                "entry_ts",
                "entry_price",
                "exit_ts",
                "exit_price",
                "pnl_usdt",
                "pnl_pct",
                "fee",
                "duration_bars",
                "sw_strength",
                "sw_reasons",
                "atlas_pass",
                "atlas_mult",
                "atlas_reasons",
                "atlas_shadow_pass",
            ]
        )
        for t in trades:
            writer.writerow(
                [
                    t.get("run_id"),
                    t.get("mode"),
                    t.get("sym"),
                    t.get("side"),
                    t.get("entry_ts"),
                    t.get("entry_price"),
                    t.get("exit_ts"),
                    t.get("exit_price"),
                    t.get("pnl_usdt"),
                    t.get("pnl_pct"),
                    t.get("fee"),
                    t.get("duration_bars"),
                    t.get("sw_strength"),
                    ",".join(t.get("sw_reasons") or []),
                    t.get("atlas_pass"),
                    t.get("atlas_mult"),
                    ",".join(t.get("atlas_reasons") or []),
                    t.get("atlas_shadow_pass"),
                ]
            )


def build_summary(run_id: str, trades: List[Dict]) -> Dict:
    by_mode: Dict[str, List[Dict]] = {}
    for t in trades:
        by_mode.setdefault(t.get("mode") or "unknown", []).append(t)
    summary = {"run_id": run_id, "modes": {}}
    for mode, rows in by_mode.items():
        summary["modes"][mode] = compute_metrics(rows)
        if mode == "shadow":
            shadow_pass = [r for r in rows if r.get("atlas_shadow_pass") is True]
            shadow_fail = [r for r in rows if r.get("atlas_shadow_pass") is False]
            summary["modes"][mode]["shadow_pass"] = compute_metrics(shadow_pass)
            summary["modes"][mode]["shadow_fail"] = compute_metrics(shadow_fail)
            reason_map: Dict[str, List[Dict]] = {}
            for r in shadow_fail:
                reasons = r.get("atlas_shadow_reasons") or []
                for reason in reasons:
                    reason_map.setdefault(reason, []).append(r)
            summary["modes"][mode]["shadow_fail_by_reason"] = {
                reason: compute_metrics(rows) for reason, rows in reason_map.items()
            }
    return summary


def write_summary_json(path: str, summary: Dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
