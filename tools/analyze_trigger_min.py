#!/usr/bin/env python3
import argparse
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def _fmt(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    if math.isfinite(val):
        return f"{val:.4f}"
    return "N/A"


def _summarize(rows: List[Dict[str, object]]) -> Dict[str, object]:
    pnl_rs = [float(r["pnl_r"]) for r in rows if isinstance(r.get("pnl_r"), (int, float))]
    mfe_atr = [float(r["mfe_atr"]) for r in rows if isinstance(r.get("mfe_atr"), (int, float))]
    mae_atr = [float(r["mae_atr"]) for r in rows if isinstance(r.get("mae_atr"), (int, float))]
    hold = [float(r["hold_bars_ltf"]) for r in rows if isinstance(r.get("hold_bars_ltf"), (int, float))]
    wins = sum(1 for r in rows if isinstance(r.get("pnl_r"), (int, float)) and float(r["pnl_r"]) > 0)
    return {
        "trades": len(rows),
        "winrate": (wins / len(rows)) if rows else 0.0,
        "avg_pnl_r": _mean(pnl_rs),
        "median_pnl_r": _median(pnl_rs),
        "avg_mfe_atr": _mean(mfe_atr),
        "avg_mae_atr": _mean(mae_atr),
        "avg_hold_bars": _mean(hold),
    }


def _print_summary(title: str, stats: Dict[str, object]) -> None:
    print(f"\n{title}")
    print(
        "trades=%d winrate=%s avg_pnl_r=%s median_pnl_r=%s avg_mfe_atr=%s avg_mae_atr=%s avg_hold_bars=%s"
        % (
            stats.get("trades", 0),
            _fmt(stats.get("winrate")),
            _fmt(stats.get("avg_pnl_r")),
            _fmt(stats.get("median_pnl_r")),
            _fmt(stats.get("avg_mfe_atr")),
            _fmt(stats.get("avg_mae_atr")),
            _fmt(stats.get("avg_hold_bars")),
        )
    )


def _combo_breakdown(rows: List[Dict[str, object]]) -> List[Tuple[str, Dict[str, object]]]:
    bucket: Dict[str, List[Dict[str, object]]] = {}
    for r in rows:
        combo = r.get("trigger_combo") or "N/A"
        bucket.setdefault(str(combo), []).append(r)
    out = []
    for combo, items in sorted(bucket.items(), key=lambda x: (-len(x[1]), x[0])):
        out.append((combo, _summarize(items)))
    return out


def _regime_breakdown(rows: List[Dict[str, object]]) -> List[Tuple[str, Dict[str, object]]]:
    bucket: Dict[str, List[Dict[str, object]]] = {}
    for r in rows:
        regime = r.get("atlas_regime") or r.get("regime") or "N/A"
        bucket.setdefault(str(regime), []).append(r)
    out = []
    for regime, items in sorted(bucket.items(), key=lambda x: (-len(x[1]), x[0])):
        out.append((regime, _summarize(items)))
    return out


def _regime_key(row: Dict[str, object]) -> str:
    regime = row.get("atlas_regime")
    if regime in (None, ""):
        regime = row.get("regime")
    if regime in (None, ""):
        return "N/A"
    return str(regime)


def _combo_regime_cells(rows: List[Dict[str, object]]) -> Dict[Tuple[str, str], List[Dict[str, object]]]:
    cells: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
    for r in rows:
        combo = str(r.get("trigger_combo") or "N/A")
        regime = _regime_key(r)
        cells.setdefault((combo, regime), []).append(r)
    return cells


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze trigger min vs best on swaggy trades")
    parser.add_argument("--jsonl", default="logs/swaggy_trades.jsonl")
    parser.add_argument("--watch-out", default="", help="optional watchlist output json")
    args = parser.parse_args()

    records: List[Dict[str, object]] = []
    missing_threshold = 0
    missing_strengths = 0

    idx = 0
    with open(args.jsonl, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            if row.get("event") != "SWAGGY_TRADE":
                continue
            threshold = row.get("trigger_threshold_used")
            best = row.get("trigger_strength_best")
            min_val = row.get("trigger_strength_min")
            if not isinstance(threshold, (int, float)):
                missing_threshold += 1
                continue
            if not isinstance(best, (int, float)) or not isinstance(min_val, (int, float)):
                missing_strengths += 1
                continue
            row["trigger_threshold_used"] = float(threshold)
            row["trigger_strength_best"] = float(best)
            row["trigger_strength_min"] = float(min_val)
            row["_idx"] = idx
            idx += 1
            records.append(row)

    if not records:
        print("No SWAGGY_TRADE records with trigger_threshold_used found.")
        if missing_threshold:
            print(f"missing_threshold={missing_threshold}")
        return

    g1 = [r for r in records if r["trigger_strength_min"] >= r["trigger_threshold_used"]]
    g2 = [
        r
        for r in records
        if r["trigger_strength_best"] >= r["trigger_threshold_used"]
        and r["trigger_strength_min"] < r["trigger_threshold_used"]
    ]
    g3 = [r for r in records if r["trigger_strength_best"] < r["trigger_threshold_used"]]

    print("SWAGGY trigger min analysis")
    print(f"input={args.jsonl}")
    print(f"total_records={len(records)} missing_threshold={missing_threshold} missing_strengths={missing_strengths}")

    total_stats = _summarize(records)
    _print_summary("Total", total_stats)
    _print_summary("G1 min_pass", _summarize(g1))
    _print_summary("G2 best_pass_min_fail", _summarize(g2))
    _print_summary("G3 best_fail", _summarize(g3))

    print("\nTrigger combo breakdown (G1)")
    for combo, stats in _combo_breakdown(g1):
        print(f"{combo}\ttrades={stats['trades']}\twinrate={_fmt(stats['winrate'])}\tavg_pnl_r={_fmt(stats['avg_pnl_r'])}")

    print("\nTrigger combo breakdown (G2)")
    for combo, stats in _combo_breakdown(g2):
        print(f"{combo}\ttrades={stats['trades']}\twinrate={_fmt(stats['winrate'])}\tavg_pnl_r={_fmt(stats['avg_pnl_r'])}")

    print("\nRegime breakdown (G1)")
    for regime, stats in _regime_breakdown(g1):
        print(f"{regime}\ttrades={stats['trades']}\twinrate={_fmt(stats['winrate'])}\tavg_pnl_r={_fmt(stats['avg_pnl_r'])}")

    print("\nRegime breakdown (G2)")
    for regime, stats in _regime_breakdown(g2):
        print(f"{regime}\ttrades={stats['trades']}\twinrate={_fmt(stats['winrate'])}\tavg_pnl_r={_fmt(stats['avg_pnl_r'])}")

    g2_cells = _combo_regime_cells(g2)
    g2_cell_stats: List[Tuple[str, str, Dict[str, object], int]] = []
    bad_cells = set()
    total_winrate = float(total_stats.get("winrate") or 0.0)
    for (combo, regime), rows in g2_cells.items():
        stats = _summarize(rows)
        trades = int(stats.get("trades") or 0)
        bad = False
        if trades >= 8:
            winrate = float(stats.get("winrate") or 0.0)
            avg_pnl = stats.get("avg_pnl_r")
            if winrate <= (total_winrate - 0.10) or (isinstance(avg_pnl, (int, float)) and avg_pnl <= 0.0):
                bad = True
        if bad:
            bad_cells.add((combo, regime))
        g2_cell_stats.append((combo, regime, stats, 1 if bad else 0))

    print("\nG2 combo×regime breakdown")
    header = (
        "combo\tregime\ttrades\twinrate\tavg_pnl_r\tmedian_pnl_r\tavg_mfe_atr\tavg_mae_atr\tavg_hold_bars\tBAD_CANDIDATE"
    )
    print(header)
    for combo, regime, stats, bad in sorted(
        g2_cell_stats, key=lambda x: (-int(x[2].get("trades") or 0), x[0], x[1])
    ):
        print(
            f"{combo}\t{regime}\t{stats['trades']}\t{_fmt(stats['winrate'])}\t"
            f"{_fmt(stats['avg_pnl_r'])}\t{_fmt(stats['median_pnl_r'])}\t"
            f"{_fmt(stats['avg_mfe_atr'])}\t{_fmt(stats['avg_mae_atr'])}\t"
            f"{_fmt(stats['avg_hold_bars'])}\t{bad}"
        )

    worst_cells = []
    for combo, regime, stats, bad in g2_cell_stats:
        if not bad:
            continue
        worst_cells.append((combo, regime, stats))
    worst_cells = sorted(
        worst_cells,
        key=lambda x: (
            float(x[2].get("avg_pnl_r") or 0.0),
            float(x[2].get("winrate") or 0.0),
            -int(x[2].get("trades") or 0),
        ),
    )[:10]
    if worst_cells:
        print("\nWorst cells (BAD_CANDIDATE, top 10)")
        for combo, regime, stats in worst_cells:
            print(
                f"{combo}|{regime}\ttrades={stats['trades']}\twinrate={_fmt(stats['winrate'])}\tavg_pnl_r={_fmt(stats['avg_pnl_r'])}"
            )

    watch_strong = []
    watch_weak = []
    for combo, regime, stats, _bad in g2_cell_stats:
        trades = int(stats.get("trades") or 0)
        avg_pnl = stats.get("avg_pnl_r")
        if not isinstance(avg_pnl, (int, float)):
            continue
        if trades >= 3 and avg_pnl < 0:
            watch_strong.append((combo, regime, stats))
        elif trades >= 2 and avg_pnl < 0:
            watch_weak.append((combo, regime, stats))

    if watch_strong:
        print("\nWATCH_STRONG (G2 cells)")
        print("cell\ttrades\twinrate\tavg_pnl_r\tmedian_pnl_r\tavg_mfe_atr\tavg_mae_atr\tavg_hold_bars\tLOW_SAMPLE")
        for combo, regime, stats in sorted(watch_strong, key=lambda x: float(x[2].get("avg_pnl_r") or 0.0)):
            low_sample = 1 if int(stats.get("trades") or 0) < 5 else 0
            print(
                f"{combo}|{regime}\t{stats['trades']}\t{_fmt(stats['winrate'])}\t{_fmt(stats['avg_pnl_r'])}"
                f"\t{_fmt(stats['median_pnl_r'])}\t{_fmt(stats['avg_mfe_atr'])}\t{_fmt(stats['avg_mae_atr'])}"
                f"\t{_fmt(stats['avg_hold_bars'])}\t{low_sample}"
            )

    if watch_weak:
        print("\nWATCH_WEAK (G2 cells)")
        print("cell\ttrades\twinrate\tavg_pnl_r\tmedian_pnl_r\tavg_mfe_atr\tavg_mae_atr\tavg_hold_bars\tLOW_SAMPLE")
        for combo, regime, stats in sorted(watch_weak, key=lambda x: float(x[2].get("avg_pnl_r") or 0.0)):
            low_sample = 1 if int(stats.get("trades") or 0) < 5 else 0
            print(
                f"{combo}|{regime}\t{stats['trades']}\t{_fmt(stats['winrate'])}\t{_fmt(stats['avg_pnl_r'])}"
                f"\t{_fmt(stats['median_pnl_r'])}\t{_fmt(stats['avg_mfe_atr'])}\t{_fmt(stats['avg_mae_atr'])}"
                f"\t{_fmt(stats['avg_hold_bars'])}\t{low_sample}"
            )

    if args.watch_out:
        tz = timezone(timedelta(hours=9))
        now = datetime.now(tz=tz)
        today = now.strftime("%Y-%m-%d")
        generated_at = now.isoformat(timespec="seconds")
        criteria = {
            "group": "G2(best_pass_min_fail)",
            "watch_strong": "trades>=3 & avg_pnl_r<0",
            "watch_weak": "trades>=2 & avg_pnl_r<0",
            "low_sample": "trades<5",
        }

        current_cells: Dict[str, Dict[str, object]] = {}
        for combo, regime, stats in watch_strong:
            cell = f"{combo}|{regime}"
            low_sample = 1 if int(stats.get("trades") or 0) < 5 else 0
            current_cells[cell] = {
                "cell": cell,
                "watch_level": "STRONG",
                "last_stats": {
                    "trades": int(stats.get("trades") or 0),
                    "winrate": stats.get("winrate"),
                    "avg_pnl_r": stats.get("avg_pnl_r"),
                    "low_sample": low_sample,
                },
            }
        for combo, regime, stats in watch_weak:
            cell = f"{combo}|{regime}"
            if cell in current_cells:
                continue
            low_sample = 1 if int(stats.get("trades") or 0) < 5 else 0
            current_cells[cell] = {
                "cell": cell,
                "watch_level": "WEAK",
                "last_stats": {
                    "trades": int(stats.get("trades") or 0),
                    "winrate": stats.get("winrate"),
                    "avg_pnl_r": stats.get("avg_pnl_r"),
                    "low_sample": low_sample,
                },
            }

        existing = {}
        if os.path.exists(args.watch_out):
            try:
                with open(args.watch_out, "r", encoding="utf-8") as f:
                    existing = json.loads(f.read().strip() or "{}")
            except Exception:
                existing = {}

        existing_cells = {}
        if isinstance(existing, dict):
            for cell_entry in existing.get("cells", []) if isinstance(existing.get("cells"), list) else []:
                if isinstance(cell_entry, dict) and cell_entry.get("cell"):
                    existing_cells[cell_entry["cell"]] = cell_entry

        updated_cells = []
        promoted = []
        for cell, data in current_cells.items():
            entry = existing_cells.get(cell, {})
            if not entry:
                entry = {
                    "cell": cell,
                    "first_seen": today,
                }
            entry["last_seen"] = today
            entry["watch_level"] = data["watch_level"]
            entry["last_stats"] = data["last_stats"]
            entry["status"] = entry.get("status") or "watch"
            trades = int(entry["last_stats"].get("trades") or 0)
            avg_pnl = entry["last_stats"].get("avg_pnl_r")
            winrate = entry["last_stats"].get("winrate")
            promote = False
            if trades >= 8:
                if (isinstance(avg_pnl, (int, float)) and avg_pnl < 0) or (
                    isinstance(winrate, (int, float)) and winrate <= (total_winrate - 0.10)
                ):
                    promote = True
            if promote:
                entry["status"] = "candidate"
                promoted.append(cell)
            updated_cells.append(entry)

        for cell, entry in existing_cells.items():
            if cell in current_cells:
                continue
            entry["status"] = "inactive"
            updated_cells.append(entry)

        updated_cells = sorted(updated_cells, key=lambda x: (x.get("status") != "candidate", x.get("cell", "")))
        watch_payload = {
            "version": 1,
            "generated_at": generated_at,
            "criteria": criteria,
            "cells": updated_cells,
        }
        with open(args.watch_out, "w", encoding="utf-8") as f:
            f.write(json.dumps(watch_payload, ensure_ascii=True, separators=(",", ":")) + "\n")

        if promoted:
            print("\nPROMOTE TO BAD_CANDIDATE REVIEW")
            for cell in promoted:
                print(cell)

    g2_ids = {r["_idx"] for r in g2 if "_idx" in r}
    projected = [r for r in records if r.get("_idx") not in g2_ids]
    proj_stats = _summarize(projected)
    print("\nProjected (if use_trigger_min=True)")
    print(
        "trades=%d winrate=%s avg_pnl_r=%s median_pnl_r=%s avg_mfe_atr=%s avg_mae_atr=%s avg_hold_bars=%s"
        % (
            proj_stats.get("trades", 0),
            _fmt(proj_stats.get("winrate")),
            _fmt(proj_stats.get("avg_pnl_r")),
            _fmt(proj_stats.get("median_pnl_r")),
            _fmt(proj_stats.get("avg_mfe_atr")),
            _fmt(proj_stats.get("avg_mae_atr")),
            _fmt(proj_stats.get("avg_hold_bars")),
        )
    )

    if bad_cells:
        filtered = [
            r for r in records
            if not (
                r.get("_idx") in g2_ids
                and (str(r.get("trigger_combo") or "N/A"), _regime_key(r)) in bad_cells
            )
        ]
        filtered_stats = _summarize(filtered)
        print("\nProjected (skip BAD_CANDIDATE G2 cells)")
        print(
            "trades=%d winrate=%s avg_pnl_r=%s median_pnl_r=%s avg_mfe_atr=%s avg_mae_atr=%s avg_hold_bars=%s"
            % (
                filtered_stats.get("trades", 0),
                _fmt(filtered_stats.get("winrate")),
                _fmt(filtered_stats.get("avg_pnl_r")),
                _fmt(filtered_stats.get("median_pnl_r")),
                _fmt(filtered_stats.get("avg_mfe_atr")),
                _fmt(filtered_stats.get("avg_mae_atr")),
                _fmt(filtered_stats.get("avg_hold_bars")),
            )
        )


if __name__ == "__main__":
    main()
