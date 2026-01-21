#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from typing import List


def _parse_floats(text: str) -> List[float]:
    out: List[float] = []
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Swaggy x Atlas Lab backtest sweep")
    parser.add_argument("--symbols", default="", help="comma-separated symbols")
    parser.add_argument("--symbols-file", default="", help="path to fixed symbol list")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--mode", default="shadow", choices=("hard", "soft", "shadow", "off", "hybrid", "all"))
    parser.add_argument("--tp-pct", type=float, default=0.03)
    parser.add_argument("--sl-pct", type=float, default=0.0)
    parser.add_argument("--sl-sweep", default="")
    parser.add_argument("--universe", default="top50")
    parser.add_argument("--anchor", default="BTC,ETH")
    parser.add_argument("--fee", type=float, default=0.0)
    parser.add_argument("--slippage", type=float, default=0.0)
    parser.add_argument("--timeout-bars", type=int, default=0)
    parser.add_argument("--cooldown-min", type=int, default=0)
    parser.add_argument("--d1-overext-atr", type=float, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.sl_pct > 0:
        sl_values = [float(args.sl_pct)]
    else:
        sl_values = _parse_floats(args.sl_sweep)
        if not sl_values:
            raise SystemExit("sl-pct or sl-sweep must be provided")

    base_cmd = [
        sys.executable,
        "-m",
        "engines.swaggy_atlas_lab.run_backtest",
        "--days",
        str(args.days),
        "--mode",
        args.mode,
        "--tp-pct",
        str(args.tp_pct),
        "--fee",
        str(args.fee),
        "--slippage",
        str(args.slippage),
        "--timeout-bars",
        str(args.timeout_bars),
        "--universe",
        args.universe,
        "--anchor",
        args.anchor,
    ]
    if args.cooldown_min and args.cooldown_min > 0:
        base_cmd.extend(["--cooldown-min", str(args.cooldown_min)])
    if args.symbols:
        base_cmd.extend(["--symbols", args.symbols])
    if args.symbols_file:
        base_cmd.extend(["--symbols-file", args.symbols_file])
    if args.max_symbols:
        base_cmd.extend(["--max-symbols", str(args.max_symbols)])
    if args.verbose:
        base_cmd.append("--verbose")
    if isinstance(args.d1_overext_atr, (int, float)):
        base_cmd.extend(["--d1-overext-atr", str(args.d1_overext_atr)])

    for sl in sl_values:
        cmd = base_cmd + ["--sl-pct", str(sl)]
        print(f"[SWEEP] sl-pct={sl:.4f}")
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
