from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import ccxt

import cycle_cache
from env_loader import load_env
from engines.base import EngineContext
from engines.rsi.config import RsiConfig
from engines.rsi.engine import RsiEngine
from engines.universe import build_universe_from_tickers


def _now_kst_str() -> str:
    try:
        dt = datetime.now(timezone.utc) + timedelta(hours=9)
        return dt.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return "unknown"


def _fmt_float(val: Any, ndigits: int = 2) -> str:
    try:
        return f"{float(val):.{ndigits}f}"
    except Exception:
        return "N/A"


def _append_rsi_detail_log(line: str) -> None:
    try:
        full_path = os.path.join("logs", "rsi", "rsi_detail.log")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
    except Exception:
        pass


def _make_fetcher(ex: ccxt.Exchange):
    def _fetch(symbol: str, tf: str, limit: int) -> Optional[list]:
        try:
            data = ex.fetch_ohlcv(symbol, tf, limit=limit)
        except Exception:
            return []
        if not data:
            return []
        # drop last (incomplete)
        if len(data) > 1:
            data = data[:-1]
        return data
    return _fetch


def _build_shared_universe(cfg: RsiConfig, tickers: Dict[str, dict], symbols: list) -> list:
    return build_universe_from_tickers(
        tickers,
        symbols=symbols,
        min_quote_volume_usdt=cfg.min_quote_volume_usdt,
        top_n=cfg.universe_top_n,
        anchors=cfg.anchors,
    )


def _log_scan_detail(
    idx: int,
    total: int,
    symbol: str,
    scan_result,
    cfg: RsiConfig,
) -> None:
    rsis = scan_result.rsis
    rsis_prev = scan_result.rsis_prev
    r3m_val = scan_result.r3m_val
    vol_cur = scan_result.vol_cur
    vol_avg = scan_result.vol_avg
    vol_ratio = (float(vol_cur) / float(vol_avg)) if (vol_cur and vol_avg and vol_avg > 0) else None
    struct_metrics = scan_result.struct_metrics or {}
    detail_line = (
        "[{ts}] [rsi-detail] idx={idx}/{total} sym={sym} "
        "rsi1h={r1h} rsi15={r15} rsi5={r5} rsi5_prev={r5p} "
        "rsi3={r3} rsi3_prev={r3p} rsi3_prev2={r3p2} "
        "thr1h={t1h} thr15={t15} thr5={t5} thr3={t3} "
        "down5={d5} down3={d3} ok_tf={oktf} trigger={trig} "
        "vol_ok={vok} vol_cur={vcur} vol_avg={vavg} vol_ratio={vr} "
        "struct_ok={sok} lower_highs={lh} wick_reject={wr} wick1={w1} wick2={w2} "
        "impulse_block={imp} spike_ready={spk} struct_ready={str} ready={ready} "
        "pass={pc} miss={mc}"
    ).format(
        ts=_now_kst_str(),
        idx=idx,
        total=total,
        sym=symbol,
        r1h=_fmt_float(rsis.get("1h"), 2),
        r15=_fmt_float(rsis.get("15m"), 2),
        r5=_fmt_float(rsis.get("5m"), 2),
        r5p=_fmt_float(rsis_prev.get("5m"), 2),
        r3=_fmt_float(rsis.get("3m"), 2),
        r3p=_fmt_float(rsis_prev.get("3m"), 2),
        r3p2=_fmt_float(r3m_val, 2),
        t1h=_fmt_float(cfg.thresholds.get("1h"), 2),
        t15=_fmt_float(cfg.thresholds.get("15m"), 2),
        t5=_fmt_float(cfg.thresholds.get("5m"), 2),
        t3=_fmt_float(cfg.thresholds.get("3m"), 2),
        d5="Y" if scan_result.rsi5m_downturn else "N",
        d3="Y" if scan_result.rsi3m_downturn else "N",
        oktf="Y" if scan_result.ok_tf else "N",
        trig="Y" if scan_result.trigger_ok else "N",
        vok="Y" if scan_result.vol_ok else "N",
        vcur=_fmt_float(vol_cur, 2),
        vavg=_fmt_float(vol_avg, 2),
        vr=_fmt_float(vol_ratio, 2),
        sok="Y" if scan_result.struct_ok else "N",
        lh="Y" if struct_metrics.get("lower_highs") else "N",
        wr="Y" if struct_metrics.get("wick_reject") else "N",
        w1=_fmt_float(struct_metrics.get("upper_wick_ratio_1"), 3),
        w2=_fmt_float(struct_metrics.get("upper_wick_ratio_2"), 3),
        imp="Y" if scan_result.impulse_block else "N",
        spk="Y" if scan_result.spike_ready else "N",
        str="Y" if scan_result.struct_ready else "N",
        ready="Y" if scan_result.ready_entry else "N",
        pc=scan_result.pass_count,
        mc=scan_result.miss_count,
    )
    print(detail_line)
    _append_rsi_detail_log(detail_line)


def run_loop(poll_sec: float, once: bool) -> None:
    load_env()
    ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    ex.load_markets()
    cycle_cache.set_fetcher(_make_fetcher(ex))
    cfg = RsiConfig()
    rsi_engine = RsiEngine(cfg)

    symbols = [s for s in ex.symbols if s.endswith("USDT:USDT")]
    while True:
        try:
            tickers = ex.fetch_tickers(symbols)
        except Exception as e:
            print(f"[rsi-run] tickers error: {e}")
            time.sleep(poll_sec)
            if once:
                break
            continue
        state: Dict[str, Any] = {
            "_tickers": tickers,
            "_symbols": symbols,
        }
        shared = _build_shared_universe(cfg, tickers, symbols)
        state["_universe"] = shared
        ctx = EngineContext(exchange=ex, state=state, now_ts=time.time(), logger=print, config=cfg)
        universe = rsi_engine.build_universe(ctx)
        if not universe:
            print("[rsi-run] universe empty")
            if once:
                break
            time.sleep(poll_sec)
            continue
        pass_counts = {k: 0 for k in ["1h", "15m", "5m", "3m"]}
        for idx, symbol in enumerate(universe, start=1):
            scan_result = rsi_engine.scan_symbol(symbol, pass_counts, logger=lambda *args, **kwargs: None)
            if not scan_result:
                continue
            _log_scan_detail(idx, len(universe), symbol, scan_result, cfg)
        if once:
            break
        time.sleep(poll_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone RSI scan runner")
    parser.add_argument("--poll-sec", type=float, default=15.0, help="Polling interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run a single scan and exit")
    args = parser.parse_args()
    run_loop(poll_sec=float(args.poll_sec), once=bool(args.once))


if __name__ == "__main__":
    main()
