#!/usr/bin/env python3
import argparse
import os
import random
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCRIPT = os.path.join(ROOT_DIR, "engines", "top_fail_short_v1", "run_backtest.py")

TOTAL_RE = re.compile(
    r"\[BACKTEST\] TOTAL .*? trades=(?P<trades>\d+) .*? winrate=(?P<winrate>[\d.]+)% .*? net_sum=(?P<net_sum>[-\d.]+)"
)


@dataclass
class Result:
    params: Dict[str, str]
    winrate: float
    trades: int
    net_sum: float
    raw_total_line: str


PARAM_SPACE = {
    "rsi_overbought_level": [50, 55, 60, 65, 70],
    "rsi_overbought_bars": [2, 3, 4, 5],
    "vol_div_lookback": [2, 3, 4, 5, 6],
    "ema_len": [20, 25, 30, 35, 40],
    "vol_spike_mult": [1.2, 1.4, 1.6, 1.8, 2.0],
    "wash_atr_mult": [1.0, 1.1, 1.2, 1.3, 1.4],
}


def _build_cmd(base_args: List[str], combo: Dict[str, str]) -> List[str]:
    cmd = [sys.executable, SCRIPT] + base_args
    for k, v in combo.items():
        cmd.extend([f"--{k.replace('_', '-')}", str(v)])
    return cmd


def _parse_total(out: str) -> Optional[Tuple[float, int, float, str]]:
    for line in out.splitlines():
        m = TOTAL_RE.search(line)
        if m:
            winrate = float(m.group("winrate"))
            trades = int(m.group("trades"))
            net_sum = float(m.group("net_sum"))
            return winrate, trades, net_sum, line.strip()
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="TopFailShortV1 backtest sweep")
    ap.add_argument("--samples", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-trades", type=int, default=5)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--universe", type=str, default="common")
    ap.add_argument("--ltf-tf", type=str, default="3m")
    ap.add_argument("--mtf-tf", type=str, default="15m")
    ap.add_argument("--htf-tf", type=str, default="1h")
    ap.add_argument("--common-warmup-dir", type=str, default="")
    ap.add_argument("--cache-only", action="store_true")
    ap.add_argument("--common-only", action="store_true")
    args = ap.parse_args()

    common_dir = args.common_warmup_dir.strip()
    if not common_dir:
        common_dir = os.path.join(ROOT_DIR, "logs", "common_warmup", "ohlcv")

    base_args = [
        "--days",
        str(args.days),
        "--universe",
        args.universe,
        "--ltf-tf",
        args.ltf_tf,
        "--mtf-tf",
        args.mtf_tf,
        "--htf-tf",
        args.htf_tf,
        "--common-warmup-dir",
        common_dir,
    ]
    if args.cache_only:
        base_args.append("--cache-only")
    if args.common_only:
        base_args.append("--common-only")

    rnd = random.Random(args.seed)
    keys = list(PARAM_SPACE.keys())

    results: List[Result] = []
    for _ in range(args.samples):
        combo = {k: rnd.choice(PARAM_SPACE[k]) for k in keys}
        cmd = _build_cmd(base_args, combo)
        proc = subprocess.run(cmd, cwd=ROOT_DIR, capture_output=True, text=True)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        parsed = _parse_total(out)
        if not parsed:
            continue
        winrate, trades, net_sum, line = parsed
        if trades < args.min_trades:
            continue
        results.append(Result(params={k: str(v) for k, v in combo.items()}, winrate=winrate, trades=trades, net_sum=net_sum, raw_total_line=line))

    results.sort(key=lambda r: (r.winrate, r.trades, r.net_sum), reverse=True)

    print("[SWEEP] finished")
    print(f"[SWEEP] samples={args.samples} min_trades={args.min_trades} kept={len(results)}")
    for idx, r in enumerate(results[:10], start=1):
        params_str = " ".join([f"{k}={v}" for k, v in r.params.items()])
        print(f"[TOP {idx}] winrate={r.winrate:.2f}% trades={r.trades} net_sum={r.net_sum:.3f} {params_str}")
        print(f"         {r.raw_total_line}")


if __name__ == "__main__":
    main()
