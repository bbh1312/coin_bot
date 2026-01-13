from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, Optional

import ccxt
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import cycle_cache
from env_loader import load_env
from engines.base import EngineContext
from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig
from engines.atlas_rs_fail_short.engine import AtlasRsFailShortEngine
from engines.universe import build_universe_from_tickers
from atlas_test.atlas_engine import atlas_swaggy_cfg


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
        "timeout": 30_000,
        "rateLimit": 200,
    })


def _set_cache(symbol: str, tf: str, raw: list) -> None:
    if not raw:
        return
    cycle_cache.set_raw(symbol, tf, raw)


def _log_line(line: str) -> None:
    date_tag = time.strftime("%Y-%m-%d")
    path = os.path.join(
        ROOT_DIR,
        "logs",
        "atlas_rs_fail_short",
        f"atlas_rs_fail_short-{date_tag}.log",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")

def _append_entry_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join(
        ROOT_DIR,
        "logs",
        "atlas_rs_fail_short",
        f"atlas_rs_fail_short_entries_{date_tag}.log",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _fetch_ohlcv(ex: ccxt.Exchange, symbol: str, tf: str, limit: int) -> Optional[list]:
    try:
        return ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    except Exception as e:
        print(f"[atlas-rs-fail-short] fetch failed sym={symbol} tf={tf}: {e}")
        return None


def _run_once(
    ex: ccxt.Exchange,
    engine: AtlasRsFailShortEngine,
    cfg: AtlasRsFailShortConfig,
    state: Dict[str, Any],
    symbols: list[str],
) -> None:
    now_ts = time.time()
    ctx = EngineContext(exchange=ex, state=state, now_ts=now_ts, logger=_log_line, config=cfg)
    ref_symbol = getattr(atlas_swaggy_cfg, "ref_symbol", None)
    if ref_symbol:
        raw_ref = _fetch_ohlcv(ex, ref_symbol, cfg.ltf_tf, cfg.ltf_limit)
        if raw_ref:
            _set_cache(ref_symbol, cfg.ltf_tf, raw_ref)
    for sym in symbols:
        raw_ltf = _fetch_ohlcv(ex, sym, cfg.ltf_tf, cfg.ltf_limit)
        if raw_ltf:
            _set_cache(sym, cfg.ltf_tf, raw_ltf)
        sig = engine.on_tick(ctx, sym)
        if sig:
            meta = sig.meta if isinstance(sig.meta, dict) else {}
            atlas = meta.get("atlas") if isinstance(meta.get("atlas"), dict) else {}
            tech = meta.get("tech") if isinstance(meta.get("tech"), dict) else {}
            _append_entry_log(
                "engine=atlas_rs_fail_short side=SHORT symbol=%s entry=%.6g size_mult=%.3f "
                "confirm=%s atlas_regime=%s atlas_dir=%s score=%s rs_z=%s rs_z_slow=%s wick_ratio=%s"
                % (
                    sym,
                    float(sig.entry_price or 0.0),
                    float(meta.get("size_mult") or 1.0),
                    tech.get("confirm_type") or "N/A",
                    atlas.get("regime") or "N/A",
                    atlas.get("dir") or "N/A",
                    atlas.get("score") if atlas.get("score") is not None else "N/A",
                    atlas.get("rs_z") if atlas.get("rs_z") is not None else "N/A",
                    atlas.get("rs_z_slow") if atlas.get("rs_z_slow") is not None else "N/A",
                    tech.get("wick_ratio") if tech.get("wick_ratio") is not None else "N/A",
                ),
            )
            print(
                "[atlas-rs-fail-short] ENTRY_READY "
                f"sym={sym} px={sig.entry_price:.6g} size_mult={sig.meta.get('size_mult')}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Atlas RS Fail Short runner")
    parser.add_argument("--symbols", type=str, default="")
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--min-qv", type=float, default=30_000_000.0)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    load_env()
    ex = _make_exchange()
    engine = AtlasRsFailShortEngine()
    cfg = engine.config
    state: Dict[str, Any] = {}

    while True:
        if args.symbols.strip():
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        else:
            tickers = ex.fetch_tickers()
            symbols = build_universe_from_tickers(
                tickers,
                min_quote_volume_usdt=args.min_qv,
                top_n=args.top_n,
            )
        if not symbols:
            print("[atlas-rs-fail-short] no symbols")
            time.sleep(args.interval)
            continue
        _run_once(ex, engine, cfg, state, symbols)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
