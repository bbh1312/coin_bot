from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

from engines.universe import build_universe_from_tickers

TF_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


def calc_warmup_window(
    eval_days: int,
    now_ms: int,
    min_bars_by_tf: Dict[str, int],
) -> Tuple[int, int, int, int]:
    eval_start_ms = int((datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc) - timedelta(days=eval_days)).timestamp() * 1000)
    warmup_minutes = 0
    for tf, min_bars in min_bars_by_tf.items():
        minutes = TF_MINUTES.get(tf, 1) * int(min_bars)
        if minutes > warmup_minutes:
            warmup_minutes = minutes
    warmup_days = int((warmup_minutes + 1439) // 1440) if warmup_minutes > 0 else 0
    start_ms = eval_start_ms - int(warmup_minutes * 60 * 1000)
    return start_ms, eval_start_ms, warmup_days, warmup_minutes


def load_common_universe(
    universe_arg: str,
    exchange,
    cache_only: bool,
    min_quote_volume_usdt: float = 8_000_000.0,
    top_n: int = 50,
) -> List[str]:
    universe: List[str] = []
    if universe_arg in ("common", "common_universe"):
        latest_path = os.path.join("logs", "common_universe", "latest.txt")
        if os.path.exists(latest_path):
            with open(latest_path, "r", encoding="utf-8") as f:
                universe = [line.strip() for line in f.read().splitlines() if line.strip()]
        if not universe and not cache_only and exchange is not None:
            tickers = exchange.fetch_tickers()
            universe = build_universe_from_tickers(
                tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
            )
    elif universe_arg.startswith("top"):
        if exchange is None:
            return []
        tickers = exchange.fetch_tickers()
        universe = build_universe_from_tickers(
            tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
        )
        try:
            n = int(universe_arg.replace("top", ""))
            universe = universe[:n]
        except Exception:
            pass
    else:
        if exchange is None:
            return []
        tickers = exchange.fetch_tickers()
        universe = build_universe_from_tickers(
            tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
        )
    return universe


def log_warmup_info(log_fn, warmup_days: int, warmup_minutes: int, eval_days: int) -> None:
    line = f"[BACKTEST] WARMUP auto days={warmup_days} minutes={warmup_minutes} eval_days={eval_days}"
    try:
        print(line)
    except Exception:
        pass
    try:
        log_fn(line)
    except Exception:
        pass
