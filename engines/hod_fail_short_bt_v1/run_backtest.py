#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt
import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import calc_warmup_window, load_common_universe
from engines.hod_fail_short_bt_v1.engine import (
    HodFailShortBtV1Config,
    BLOCK_UPTREND_15M,
    BLOCK_EXTENSION_3M,
    BLOCK_BELOW_EMA120_15M,
    BLOCK_VOLUME_EXPAND_15M,
    BLOCK_HOD_FATIGUE,
    BLOCK_BREAKOUT_3M,
    BLOCK_MFI_HOT,
    build_env_state,
    build_entry_state,
    map_idx_by_ts,
    session_day_start_kst,
)


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").replace(":", "_")


def _ohlcv_cache_path(root_dir: str, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe = _sanitize_symbol(symbol)
    return os.path.join(
        root_dir,
        "logs",
        "hod_fail_short_bt_v1",
        "ohlcv_cache",
        f"{safe}_{timeframe}_{start_ms}_{end_ms}.csv",
    )


def _read_ohlcv_cache(path: str) -> List[list]:
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty:
            return []
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return []


def _write_ohlcv_cache(path: str, rows: List[list]) -> None:
    if not rows:
        return
    _ensure_dir(os.path.dirname(path))
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df.to_csv(path, index=False)


def _fetch_ohlcv_all(
    exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    cache_only: bool,
    use_common_warmup: bool,
    common_warmup_dir: str,
    common_only: bool,
) -> List[list]:
    if common_only and use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_ohlcv_cache(warmup_path)
        if warmup_rows:
            return [r for r in warmup_rows if int(r[0]) <= end_ms]
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        return cached
    if cache_only or exchange is None:
        return []
    rows = []
    since = start_ms
    limit = 1500
    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not batch:
            break
        for r in batch:
            if r[0] > end_ms:
                break
            rows.append(r)
        if batch[-1][0] >= end_ms:
            break
        since = batch[-1][0] + 1
        time.sleep(exchange.rateLimit / 1000.0)
    _write_ohlcv_cache(cache_path, rows)
    return rows


def _to_kst(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _log_jsonl(path: str, row: dict) -> None:
    if not path:
        return
    _ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _compute_mfe_mae_short(trade: dict, highs: np.ndarray, lows: np.ndarray, start_idx: int, end_idx: int) -> tuple[float, float]:
    entry = trade["entry_px"]
    if entry <= 0 or end_idx < start_idx:
        return 0.0, 0.0
    low_slice = lows[start_idx:end_idx + 1]
    high_slice = highs[start_idx:end_idx + 1]
    if len(low_slice) == 0:
        return 0.0, 0.0
    mfe = max(0.0, (entry - float(np.min(low_slice))) / entry)
    mae = max(0.0, (float(np.max(high_slice)) - entry) / entry)
    return mfe, mae


def run_backtest() -> None:
    parser = argparse.ArgumentParser("hod_fail_short_bt_v1 backtest")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--eval-days", type=int, default=0)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--use-live-cache", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")

    parser.add_argument("--ltf-tf", type=str, default="3m")
    parser.add_argument("--htf-tf", type=str, default="15m")

    parser.add_argument("--hod-fail-min", type=int, default=6)
    parser.add_argument("--near-hod-band", type=float, default=0.0015)
    parser.add_argument("--retest-band", type=float, default=0.006)
    parser.add_argument("--retest-break", type=float, default=0.002)

    parser.add_argument("--ext-ema7-max", type=float, default=0.007)
    parser.add_argument("--ignore-extension", action="store_true")
    parser.add_argument("--ema120-floor", type=float, default=1.000)
    parser.add_argument("--vol-expand-min", type=float, default=1.1)
    parser.add_argument("--touch-band", type=float, default=0.0015)
    parser.add_argument("--touch-break", type=float, default=0.0003)
    parser.add_argument("--touch-max", type=int, default=4)
    parser.add_argument("--mfi-len", type=int, default=14)
    parser.add_argument("--mfi-hot", type=float, default=60.0)
    parser.add_argument("--mfi-hot-rise", action="store_true")
    parser.add_argument("--uptrend-consec-bull", type=int, default=3)
    parser.add_argument("--ema20-slope-mult", type=float, default=0.0002)

    parser.add_argument("--wash-atr-mult", type=float, default=1.25)
    parser.add_argument("--vol-spike-mult", type=float, default=1.3)
    parser.add_argument("--fail-wick-max", type=float, default=0.25)
    parser.add_argument("--use-break-low", action="store_true")
    parser.add_argument("--retrace-atr", type=float, default=0.25)
    parser.add_argument("--wash-wick-min", type=float, default=0.45)
    parser.add_argument("--wash-close-pos-max", type=float, default=0.35)
    parser.add_argument("--pathb-hod-drop", type=float, default=0.006)
    parser.add_argument("--pathb-ema-fail-k", type=int, default=3)

    parser.add_argument("--sl-atr-mult", type=float, default=0.3)
    parser.add_argument("--tp-r1", type=float, default=1.0)
    parser.add_argument("--tp-r2", type=float, default=2.0)

    parser.add_argument("--start", type=str, default="")
    parser.add_argument("--end", type=str, default="")
    parser.add_argument("--end-ms", type=int, default=0)
    parser.add_argument("--fill-missing", action="store_true")

    parser.add_argument("--log-dir", type=str, default="")

    args = parser.parse_args()

    cfg = HodFailShortBtV1Config()
    cfg.hod_fail_min = int(args.hod_fail_min)
    cfg.near_hod_band = float(args.near_hod_band)
    cfg.retest_band = float(args.retest_band)
    cfg.retest_break = float(args.retest_break)
    cfg.ext_ema7_max = float(args.ext_ema7_max)
    cfg.ema120_floor = float(args.ema120_floor)
    cfg.vol_expand_min = float(args.vol_expand_min)
    cfg.touch_band = float(args.touch_band)
    cfg.touch_break = float(args.touch_break)
    cfg.touch_max = int(args.touch_max)
    cfg.mfi_len = int(args.mfi_len)
    mfi_hot = float(args.mfi_hot)
    mfi_hot_rise = bool(args.mfi_hot_rise)
    uptrend_consec_bull = int(args.uptrend_consec_bull)
    ema20_slope_mult = float(args.ema20_slope_mult)
    cfg.wash_atr_mult = float(args.wash_atr_mult)
    cfg.vol_spike_mult = float(args.vol_spike_mult)
    cfg.fail_wick_max = float(args.fail_wick_max)
    cfg.use_break_low = bool(args.use_break_low)
    retrace_atr = float(args.retrace_atr)
    wash_wick_min = float(args.wash_wick_min)
    wash_close_pos_max = float(args.wash_close_pos_max)
    pathb_hod_drop = float(args.pathb_hod_drop)
    pathb_ema_fail_k = int(args.pathb_ema_fail_k)
    cfg.sl_atr_mult = float(args.sl_atr_mult)
    cfg.tp_r1 = float(args.tp_r1)
    cfg.tp_r2 = float(args.tp_r2)

    exchange = ccxt.binance({"enableRateLimit": True})
    end_ms = int(args.end_ms) if args.end_ms and args.end_ms > 0 else int(datetime.now(timezone.utc).timestamp() * 1000)

    ltf_tf = args.ltf_tf
    htf_tf = args.htf_tf

    min_ltf = 300
    min_htf = 200
    start_ms, eval_start_ms, _, _ = calc_warmup_window(args.days, end_ms, {ltf_tf: min_ltf, htf_tf: min_htf})
    if args.eval_days and args.eval_days > 0:
        _, eval_start_ms, _, _ = calc_warmup_window(args.eval_days, end_ms, {ltf_tf: min_ltf, htf_tf: min_htf})

    universe = load_common_universe(args.universe, exchange, args.cache_only)

    use_common = bool(args.use_live_cache or args.common_only)
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    if args.common_only:
        args.cache_only = True

    base_dir = os.path.join(ROOT_DIR, "logs", "hod_fail_short_bt_v1", "backtest")
    _ensure_dir(base_dir)
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(base_dir, f"backtest_{date_tag}.log")
    env_log = os.path.join(args.log_dir, "env_snapshot.jsonl") if args.log_dir else ""
    entry_log = os.path.join(args.log_dir, "entry_check.jsonl") if args.log_dir else ""

    def _log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    stats = {
        "entries": 0,
        "exits": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "net_sum": 0.0,
        "tp_sum": 0.0,
        "sl_sum": 0.0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
    }

    block_counts = {
        BLOCK_UPTREND_15M: 0,
        BLOCK_EXTENSION_3M: 0,
        BLOCK_BELOW_EMA120_15M: 0,
        BLOCK_VOLUME_EXPAND_15M: 0,
        BLOCK_HOD_FATIGUE: 0,
        BLOCK_BREAKOUT_3M: 0,
        BLOCK_MFI_HOT: 0,
    }
    env_on_bars = 0
    retest_touch_bars = 0
    wash_ok_bars = 0
    armed_bars = 0
    entry_eval_bars = 0
    entry_touch_bars = 0
    entry_reject_bars = 0
    armed_and_reject_bars = 0
    armed_and_break_low_bars = 0
    entry_ready_pre_bars = 0
    pre_blocked_by_wash = 0
    pre_blocked_by_uptrend = 0
    pre_blocked_by_extension = 0
    pre_blocked_by_breakout = 0
    pre_blocked_by_other = 0
    pre_blocked_by_mfi_hot = 0
    trades_by_mfi_bucket = {}
    wins_by_mfi_bucket = {}
    mae_by_mfi_bucket = {}
    net_by_mfi_bucket = {}
    delay_entries = 0
    delay_missed = 0
    entry_path_break_low = 0
    entry_path_reject_wash = 0
    entries_quality_A = 0
    entries_quality_B = 0
    entries_by_path = {"A": 0, "B": 0}
    wins_by_path = {"A": 0, "B": 0}
    net_by_path = {"A": 0.0, "B": 0.0}
    mae_by_path = {"A": 0.0, "B": 0.0}
    path_trigger_counts = {"EMA_FAIL": 0, "LOWER_HIGH": 0}
    shadow_trades = 0
    shadow_wins = 0
    shadow_net = 0.0
    shadow_mae = 0.0
    retrace_entries = 0
    retrace_missed = 0
    entry_ready_bars = 0

    for sym in universe:
        rows_15m = _fetch_ohlcv_all(
            exchange,
            sym,
            htf_tf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        rows_3m = _fetch_ohlcv_all(
            exchange,
            sym,
            ltf_tf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        if not rows_15m or not rows_3m:
            continue
        df_15m = pd.DataFrame(rows_15m, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_3m = pd.DataFrame(rows_3m, columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if len(df_15m) < min_htf or len(df_3m) < min_ltf:
            continue

        env = build_env_state(df_15m, cfg)
        ent = build_entry_state(df_3m, cfg)

        ts_15m = df_15m["ts"].astype(int).to_numpy()
        ts_3m = df_3m["ts"].astype(int).to_numpy()

        hod = None
        hod_ts = None
        hod_fail_count = 0
        high_since_hod: List[float] = []
        touch_count = 0
        in_hod_zone = False
        last_session_start = None
        env_on = False
        last_block_flags: List[str] = []
        uptrend_subflags: List[str] = []
        consec_bull_count = 0
        hh_hl = False
        close_gt_ema20 = False
        ema20_slope = 0.0
        ema20_slope_thr = 0.0
        below_ema120 = False
        volume_expand = False
        fatigue = False
        armed = False
        armed_just_set = False
        armed_ttl = 0
        delay_active = False
        delay_ttl = 0
        last_reject_idx: Optional[int] = None
        retrace_active = False
        retrace_ttl = 0
        recover_seen_idx: Optional[int] = None
        prev_pivot_high: Optional[float] = None
        last_pivot_high: Optional[float] = None
        prev_pivot_idx: Optional[int] = None
        last_pivot_idx: Optional[int] = None
        last_close_15m: Optional[float] = None

        open_trade = None
        shadow_trade = None

        for i in range(len(df_3m)):
            if i == 0:
                continue
            ts_ms = int(ts_3m[i])
            if ts_ms < eval_start_ms:
                continue
            idx_15m = map_idx_by_ts(ts_15m, ts_ms)
            if idx_15m <= 0:
                continue

            # update ENV on 15m close boundary
            if ts_ms == ts_15m[idx_15m]:
                sess_start = session_day_start_kst(ts_ms, cfg.session_start_hour_kst)
                if last_session_start != sess_start:
                    last_session_start = sess_start
                    hod = None
                    hod_ts = None
                    hod_fail_count = 0
                    high_since_hod = []
                    touch_count = 0
                    in_hod_zone = False
                    armed = False
                    armed_just_set = False
                    armed_ttl = 0
                    delay_active = False
                    delay_ttl = 0
                    last_reject_idx = None
                    retrace_active = False
                    retrace_ttl = 0
                    recover_seen_idx = None
                    prev_pivot_high = None
                    last_pivot_high = None
                    prev_pivot_idx = None
                    last_pivot_idx = None
                    last_close_15m = None

                h = float(env["high"].iloc[idx_15m])
                l = float(env["low"].iloc[idx_15m])
                c = float(env["close"].iloc[idx_15m])
                o = float(env["open"].iloc[idx_15m])
                last_close_15m = c

                if hod is None or h > float(hod):
                    hod = h
                    hod_ts = ts_ms
                    hod_fail_count = 0
                    high_since_hod = []
                    armed = False
                    armed_just_set = False
                    armed_ttl = 0
                    delay_active = False
                    delay_ttl = 0
                    last_reject_idx = None
                    retrace_active = False
                    retrace_ttl = 0
                    recover_seen_idx = None
                    prev_pivot_high = None
                    last_pivot_high = None
                    prev_pivot_idx = None
                    last_pivot_idx = None
                    last_close_15m = None
                else:
                    hod_fail_count += 1
                    high_since_hod.append(h)

                if hod is not None:
                    in_zone = h >= float(hod) * (1 - cfg.touch_band) and l <= float(hod) * (1 + cfg.touch_break)
                    if in_zone and not in_hod_zone:
                        touch_count += 1
                        in_hod_zone = True
                    elif not in_zone:
                        in_hod_zone = False

                near_hod_attempt = False
                if hod is not None and hod_fail_count > 0 and high_since_hod:
                    near_hod_attempt = max(high_since_hod) >= float(hod) * (1 - cfg.near_hod_band)

                rsi_now = float(env["rsi"].iloc[idx_15m])
                rsi_weak = False
                if idx_15m >= 3:
                    r0 = float(env["rsi"].iloc[idx_15m])
                    r1 = float(env["rsi"].iloc[idx_15m - 1])
                    r2 = float(env["rsi"].iloc[idx_15m - 2])
                    r3 = float(env["rsi"].iloc[idx_15m - 3])
                    if r0 < r1 < r2 < r3:
                        rsi_weak = True
                if not rsi_weak:
                    rsi_weak = rsi_now < float(env["rsi_sma"].iloc[idx_15m])

                obv_weak = float(env["obv_slope"].iloc[idx_15m]) < 0

                env_on = (
                    hod is not None
                    and hod_fail_count >= cfg.hod_fail_min
                    and near_hod_attempt
                    and rsi_weak
                    and obv_weak
                )

                last_block_flags = []
                # BLOCK_UPTREND_15M
                consec_bull_count = 0
                if idx_15m >= 0:
                    for j in range(idx_15m, -1, -1):
                        cj = float(env["close"].iloc[j])
                        oj = float(env["open"].iloc[j])
                        if cj > oj:
                            consec_bull_count += 1
                            if consec_bull_count >= 3:
                                break
                        else:
                            break
                consecutive_bull = consec_bull_count >= uptrend_consec_bull
                hh_hl = False
                if idx_15m >= 1:
                    prev_h = float(env["high"].iloc[idx_15m - 1])
                    prev_l = float(env["low"].iloc[idx_15m - 1])
                    if h > prev_h and l > prev_l:
                        hh_hl = True
                ema20 = float(env["ema20"].iloc[idx_15m])
                ema20_prev = float(env["ema20"].iloc[idx_15m - 1]) if idx_15m >= 1 else ema20
                ema20_slope = ema20 - ema20_prev
                ema20_slope_thr = ema20_slope_mult * c
                close_gt_ema20 = c > ema20
                ema20_rising = ema20_slope > ema20_slope_thr
                uptrend_subflags = []
                if consecutive_bull:
                    uptrend_subflags.append("CONSEC_BULL")
                if hh_hl:
                    uptrend_subflags.append("HHHL")
                if close_gt_ema20 and ema20_rising:
                    uptrend_subflags.append("ABOVE_EMA20_SLOPEUP")
                if len(uptrend_subflags) >= 2:
                    last_block_flags.append(BLOCK_UPTREND_15M)

                # BELOW_EMA120_15M
                ema120 = float(env["ema120"].iloc[idx_15m])
                below_ema120 = c < ema120 * cfg.ema120_floor

                # VOLUME_EXPAND_15M
                vol_ratio = float(env["vol_ratio"].iloc[idx_15m]) if not np.isnan(env["vol_ratio"].iloc[idx_15m]) else 0.0
                volume_expand = vol_ratio >= cfg.vol_expand_min

                # HOD_FATIGUE
                fatigue = touch_count >= cfg.touch_max

                # MFI_HOT_15M
                mfi_now = float(env["mfi"].iloc[idx_15m])
                mfi_prev = float(env["mfi"].iloc[idx_15m - 1]) if idx_15m >= 1 else mfi_now
                mfi_hot_hit = mfi_now >= mfi_hot and (not mfi_hot_rise or mfi_now > mfi_prev)
                if mfi_hot_hit:
                    last_block_flags.append(BLOCK_MFI_HOT)

                if env_on:
                    env_on_bars += 1

                _log_jsonl(
                    env_log,
                    {
                        "ts_kst": _to_kst(ts_ms),
                        "symbol": sym,
                        "session_day_start_kst": _to_kst(sess_start),
                        "hod": hod,
                        "hod_ts": _to_kst(hod_ts) if hod_ts else None,
                        "hod_fail_count": hod_fail_count,
                        "near_hod_attempt": near_hod_attempt,
                        "rsi": rsi_now,
                        "rsi_weak": rsi_weak,
                        "obv_slope": float(env["obv_slope"].iloc[idx_15m]),
                        "obv_weak": obv_weak,
                        "vol_ratio": vol_ratio,
                        "touch_count": touch_count,
                        "volume_expand": volume_expand,
                        "fatigue": fatigue,
                        "mfi": mfi_now,
                        "mfi_prev": mfi_prev,
                        "mfi_hot": mfi_hot,
                        "env_on": env_on,
                        "block_reasons": list(last_block_flags),
                        "uptrend_subflags": list(uptrend_subflags),
                        "consec_bull_15m": consec_bull_count,
                        "hhhl": hh_hl,
                        "close15_gt_ema20": close_gt_ema20,
                        "ema20_slope": ema20_slope,
                        "ema20_slope_thr": ema20_slope_thr,
                        "below_ema120": below_ema120,
                    },
                )

            # manage open trade (signal on prev close, fill on current open)
            if open_trade is not None:
                prev_close = float(ent["close"].iloc[i - 1])
                open_now = float(ent["open"].iloc[i])
                entry_px = open_trade["entry_px"]
                sl_px = open_trade["sl_px"]
                tp1 = open_trade["tp1"]
                tp2 = open_trade["tp2"]
                remaining = float(open_trade.get("remaining", 1.0))
                realized_r = float(open_trade.get("realized_r", 0.0))

                exit_reason = None
                exit_px = None
                # conservative: evaluate prev close, fill at current open
                if prev_close >= sl_px:
                    exit_reason = "SL"
                    exit_px = open_now
                elif prev_close <= tp2:
                    exit_reason = "TP2"
                    exit_px = open_now
                elif remaining > 0.5 and prev_close <= tp1:
                    realized_r += 0.5 * cfg.tp_r1 * remaining
                    remaining = remaining * 0.5
                    open_trade["remaining"] = remaining
                    open_trade["realized_r"] = realized_r
                if exit_reason:
                    entry_idx = open_trade["entry_idx"]
                    hold_bars = i - entry_idx + 1
                    mfe, mae = _compute_mfe_mae_short(open_trade, ent["high"].to_numpy(), ent["low"].to_numpy(), entry_idx, i)
                    r_val = open_trade["r_val"]
                    if exit_reason == "SL":
                        pnl_r = realized_r - remaining * 1.0
                    else:
                        pnl_r = realized_r + remaining * cfg.tp_r2
                    stats["exits"] += 1
                    stats["trades"] += 1
                    if pnl_r > 0:
                        stats["wins"] += 1
                        stats["tp_sum"] += pnl_r
                    else:
                        stats["losses"] += 1
                        stats["sl_sum"] += pnl_r
                    mfi_bucket = open_trade.get("mfi_bucket")
                    if mfi_bucket:
                        trades_by_mfi_bucket[mfi_bucket] = trades_by_mfi_bucket.get(mfi_bucket, 0) + 1
                        if pnl_r > 0:
                            wins_by_mfi_bucket[mfi_bucket] = wins_by_mfi_bucket.get(mfi_bucket, 0) + 1
                        mae_by_mfi_bucket[mfi_bucket] = mae_by_mfi_bucket.get(mfi_bucket, 0.0) + mae
                        net_by_mfi_bucket[mfi_bucket] = net_by_mfi_bucket.get(mfi_bucket, 0.0) + pnl_r
                    stats["net_sum"] += pnl_r
                    stats["mfe_sum"] += mfe
                    stats["mae_sum"] += mae
                    stats["hold_sum"] += hold_bars
                    entry_path = open_trade.get("entry_path")
                    if entry_path in wins_by_path:
                        entries_by_path[entry_path] = entries_by_path.get(entry_path, 0) + 1
                        if pnl_r > 0:
                            wins_by_path[entry_path] = wins_by_path.get(entry_path, 0) + 1
                        net_by_path[entry_path] = net_by_path.get(entry_path, 0.0) + pnl_r
                        mae_by_path[entry_path] = mae_by_path.get(entry_path, 0.0) + mae
                    _log(
                        f"[TRADE] {sym} exit={exit_reason} pnl_r={pnl_r:.3f} entry={entry_px:.6f} exit={exit_px:.6f}"
                    )
                    open_trade = None
                continue

            # manage shadow trade for PATH_A
            if shadow_trade is not None:
                prev_close = float(ent["close"].iloc[i - 1])
                open_now = float(ent["open"].iloc[i])
                entry_px = shadow_trade["entry_px"]
                sl_px = shadow_trade["sl_px"]
                tp1 = shadow_trade["tp1"]
                tp2 = shadow_trade["tp2"]
                remaining = float(shadow_trade.get("remaining", 1.0))
                realized_r = float(shadow_trade.get("realized_r", 0.0))

                exit_reason = None
                exit_px = None
                if prev_close >= sl_px:
                    exit_reason = "SL"
                    exit_px = open_now
                elif prev_close <= tp2:
                    exit_reason = "TP2"
                    exit_px = open_now
                elif remaining > 0.5 and prev_close <= tp1:
                    realized_r += 0.5 * cfg.tp_r1 * remaining
                    remaining = remaining * 0.5
                    shadow_trade["remaining"] = remaining
                    shadow_trade["realized_r"] = realized_r
                if exit_reason:
                    entry_idx = shadow_trade["entry_idx"]
                    hold_bars = i - entry_idx + 1
                    mfe, mae = _compute_mfe_mae_short(shadow_trade, ent["high"].to_numpy(), ent["low"].to_numpy(), entry_idx, i)
                    r_val = shadow_trade["r_val"]
                    if exit_reason == "SL":
                        pnl_r = realized_r - remaining * 1.0
                    else:
                        pnl_r = realized_r + remaining * cfg.tp_r2
                    shadow_trades += 1
                    if pnl_r > 0:
                        shadow_wins += 1
                    shadow_net += pnl_r
                    shadow_mae += mae
                    _log(
                        f"[SHADOW] {sym} exit={exit_reason} pnl_r={pnl_r:.3f} entry={entry_px:.6f} exit={exit_px:.6f}"
                    )
                    shadow_trade = None
                continue

            # entry check
            if not env_on:
                continue
            entry_eval_bars += 1

            # preload prev-closed 3m values for signal eval
            p = i - 1
            ema7 = float(ent["ema7"].iloc[p])
            close_3m = float(ent["close"].iloc[p])
            open_3m = float(ent["open"].iloc[p])
            high_3m = float(ent["high"].iloc[p])
            low_3m = float(ent["low"].iloc[p])
            ema20_3m = float(ent["ema20"].iloc[p])
            atr_3m = float(ent["atr"].iloc[p])
            open_now = float(ent["open"].iloc[i])
            ts_sig = int(ts_3m[p])
            if hod is not None and not np.isnan(atr_3m) and atr_3m > 0:
                retest_lower = float(hod) * (1 - cfg.retest_band)
                retest_upper = float(hod) * (1 + cfg.retest_break)
                retest_touch = high_3m >= retest_lower
                breakout = high_3m >= retest_upper
                if retest_touch:
                    entry_touch_bars += 1
                if retest_touch and not breakout:
                    retest_touch_bars += 1
                reject_base = close_3m < ema20_3m and close_3m < open_3m
                reject_break = False
                if p >= cfg.entry_break_low_len:
                    recent_low = float(df_3m["low"].iloc[p - cfg.entry_break_low_len + 1: p + 1].min())
                    break_low = close_3m < recent_low
                    weak_rebound = high_3m < ema20_3m
                    reject_break = break_low and weak_rebound
                if reject_base or reject_break:
                    entry_reject_bars += 1

            # track recovery for PATH_B (EMA recover then fail)
            if high_3m > ema7 or high_3m > ema20_3m:
                recover_seen_idx = p

            # track pivot highs for PATH_B lower-high detection (confirmed bars)
            pivot_idx = p - 2
            if pivot_idx >= 2:
                h0 = float(df_3m["high"].iloc[pivot_idx])
                h1 = float(df_3m["high"].iloc[pivot_idx - 1])
                h2 = float(df_3m["high"].iloc[pivot_idx - 2])
                h3 = float(df_3m["high"].iloc[pivot_idx + 1])
                h4 = float(df_3m["high"].iloc[pivot_idx + 2])
                if h0 > h1 and h0 > h2 and h0 >= h3 and h0 >= h4:
                    prev_pivot_high = last_pivot_high
                    prev_pivot_idx = last_pivot_idx
                    last_pivot_high = h0
                    last_pivot_idx = pivot_idx

            if last_block_flags:
                for b in last_block_flags:
                    block_counts[b] += 1
                _log_jsonl(
                    entry_log,
                    {
                        "ts_kst": _to_kst(ts_sig),
                        "symbol": sym,
                        "env_on": env_on,
                        "blocked": True,
                        "block_reasons": list(last_block_flags),
                        "uptrend_subflags": list(uptrend_subflags),
                        "consec_bull_15m": consec_bull_count,
                        "hhhl": hh_hl,
                        "close15_gt_ema20": close_gt_ema20,
                        "ema20_slope": ema20_slope,
                        "ema20_slope_thr": ema20_slope_thr,
                    },
                )
                continue

            # delay-entry handling (extension -> retrace entry)
            if delay_active:
                delay_ttl -= 1
                recent_reject = last_reject_idx is not None and (i - last_reject_idx) <= 10
                return_ok = close_3m >= (ema7 - 0.25 * atr_3m)
                if recent_reject and return_ok:
                    entry_ready = True
                    delay_entries += 1
                else:
                    entry_ready = False
                if entry_ready:
                    delay_active = False
                    delay_ttl = 0
                elif delay_ttl <= 0:
                    delay_active = False
                    delay_ttl = 0
                    delay_missed += 1
                if entry_ready:
                    entry_ready_bars += 1
                    sl_base = max(high_3m, ema7)
                    sl_px = sl_base + cfg.sl_atr_mult * float(atr_3m)
                    r_val = sl_px - open_now
                    if r_val <= 0:
                        continue
                    tp1 = open_now - cfg.tp_r1 * r_val
                    tp2 = open_now - cfg.tp_r2 * r_val
                    mfi_bucket = "80+"
                    if mfi_now < 50:
                        mfi_bucket = "<50"
                    elif mfi_now < 60:
                        mfi_bucket = "50-60"
                    elif mfi_now < 70:
                        mfi_bucket = "60-70"
                    elif mfi_now < 75:
                        mfi_bucket = "70-75"
                    elif mfi_now < 80:
                        mfi_bucket = "75-80"
                    open_trade = {
                        "entry_px": open_now,
                        "entry_idx": i,
                        "sl_px": sl_px,
                        "tp1": tp1,
                        "tp2": tp2,
                        "r_val": r_val,
                        "remaining": 1.0,
                        "realized_r": 0.0,
                        "entry_path": "A",
                        "mfi_bucket": mfi_bucket,
                    }
                if delay_active or entry_ready:
                    continue

            # BLOCK_EXTENSION_3M
            dist_ema7 = (ema7 - close_3m) / ema7 if ema7 > 0 else 0.0
            ext_atr = (ema7 - close_3m) / atr_3m if atr_3m > 0 else 0.0
            close_lt_ema7 = close_3m < ema7
            extension_hit = close_lt_ema7 and dist_ema7 >= cfg.ext_ema7_max
            if not args.ignore_extension and extension_hit:
                _log_jsonl(
                    entry_log,
                    {
                        "ts_kst": _to_kst(ts_sig),
                        "symbol": sym,
                        "env_on": env_on,
                        "blocked": False,
                        "block_reasons": [],
                        "extension_hit": True,
                        "close": close_3m,
                        "ema7": ema7,
                        "atr": atr_3m,
                        "dist_ema7": dist_ema7,
                        "ext_atr": ext_atr,
                        "close_lt_ema7": close_lt_ema7,
                    },
                )

            if hod is None:
                continue
            if np.isnan(atr_3m) or atr_3m <= 0:
                continue
            retest_lower = float(hod) * (1 - cfg.retest_band)
            retest_upper = float(hod) * (1 + cfg.retest_break)

            range_3m = high_3m - low_3m
            upper_wick = high_3m - max(open_3m, close_3m)
            upper_wick_ratio = upper_wick / range_3m if range_3m > 0 else 0.0
            close_pos = (close_3m - low_3m) / range_3m if range_3m > 0 else 0.0
            wash_ok = upper_wick_ratio >= wash_wick_min and close_pos <= wash_close_pos_max
            if wash_ok:
                wash_ok_bars += 1
            breakout_hit = high_3m >= retest_upper
            if breakout_hit:
                block_counts[BLOCK_BREAKOUT_3M] += 1
                _log_jsonl(
                    entry_log,
                    {
                        "ts_kst": _to_kst(ts_sig),
                        "symbol": sym,
                        "env_on": env_on,
                        "blocked": True,
                        "block_reasons": [BLOCK_BREAKOUT_3M],
                        "hod": hod,
                        "retest_lower": retest_lower,
                        "retest_upper": retest_upper,
                        "touch_retest": False,
                        "close": close_3m,
                        "ema7": ema7,
                        "ema20": ema20_3m,
                        "atr": atr_3m,
                        "dist_ema7": dist_ema7,
                        "entry_ready": False,
                    },
                )
                continue
            retest_touch = high_3m >= retest_lower
            if retest_touch:
                armed = True
                armed_ttl = 10
                armed_just_set = True
            if armed:
                armed_bars += 1
                if not armed_just_set:
                    armed_ttl -= 1
                    if armed_ttl <= 0:
                        armed = False
                armed_just_set = False
            else:
                continue

            entry_ready = False
            reject_base = close_3m < ema20_3m and close_3m < open_3m
            reject_break = False
            break_low = False
            if p >= 5:
                recent_low = float(df_3m["low"].iloc[p - 5: p + 1].min())
                break_low = low_3m < recent_low
                weak_rebound = high_3m < ema20_3m
                reject_break = break_low and weak_rebound
            entry_ready_pre = armed and reject_base
            if armed and reject_base:
                armed_and_reject_bars += 1
            if armed and break_low:
                armed_and_break_low_bars += 1
            if entry_ready_pre:
                entry_ready_pre_bars += 1
                last_reject_idx = p
                if BLOCK_UPTREND_15M in last_block_flags:
                    pre_blocked_by_uptrend += 1
                if extension_hit:
                    pre_blocked_by_extension += 1
                if breakout_hit:
                    pre_blocked_by_breakout += 1
                if BLOCK_MFI_HOT in last_block_flags:
                    pre_blocked_by_mfi_hot += 1
                if not wash_ok:
                    pre_blocked_by_wash += 1
            if entry_ready_pre and extension_hit:
                delay_active = True
                delay_ttl = 10
                entry_ready = False
            else:
                retrace_ok = high_3m >= (ema20_3m - retrace_atr * atr_3m)
                if entry_ready_pre and not retrace_active:
                    retrace_active = True
                    retrace_ttl = 8
                if retrace_active:
                    retrace_ttl -= 1
                    if retrace_ok:
                        entry_ready = True
                        retrace_active = False
                        retrace_entries += 1
                    elif retrace_ttl <= 0:
                        retrace_active = False
                        retrace_missed += 1
                        entry_ready = False
                else:
                    entry_ready = False

            # PATH_B trigger (structure rebound failure)
            entry_ready_b = False
            path_trigger = None
            ctx_ok = last_close_15m is not None and last_close_15m <= float(hod) * (1 - pathb_hod_drop)
            ema_fail = recover_seen_idx is not None and (p - recover_seen_idx) <= pathb_ema_fail_k and close_3m < ema7
            lower_high = False
            if last_pivot_high is not None and prev_pivot_high is not None:
                if last_pivot_high < prev_pivot_high * (1 - 0.0015):
                    if last_pivot_idx is not None and prev_pivot_idx is not None:
                        if (p - last_pivot_idx) <= 8 and (p - prev_pivot_idx) <= 8:
                            lower_high = True
            if ctx_ok and (ema_fail or lower_high):
                entry_ready_b = True
                path_trigger = "EMA_FAIL" if ema_fail else "LOWER_HIGH"

            entry_path = None
            size = 1.0
            if entry_ready:
                entry_path = "A"
                size = 1.0
            elif entry_ready_b:
                entry_ready = True
                entry_path = "B"
                size = 0.65
                if path_trigger:
                    path_trigger_counts[path_trigger] += 1
            if entry_ready:
                armed = False
                armed_ttl = 0
                if break_low:
                    entry_path_break_low += 1
                elif reject_base:
                    entry_path_reject_wash += 1
                if wash_ok:
                    entries_quality_A += 1
                else:
                    entries_quality_B += 1
                # record mfi bucket for trade attribution (prev-closed 15m)
                mfi_bucket = "80+"
                if mfi_now < 50:
                    mfi_bucket = "<50"
                elif mfi_now < 60:
                    mfi_bucket = "50-60"
                elif mfi_now < 70:
                    mfi_bucket = "60-70"
                elif mfi_now < 75:
                    mfi_bucket = "70-75"
                elif mfi_now < 80:
                    mfi_bucket = "75-80"
                entry_mfi_bucket = mfi_bucket
            elif entry_ready_pre:
                if (
                    (BLOCK_UPTREND_15M not in last_block_flags)
                    and not extension_hit
                    and not breakout_hit
                    and wash_ok
                ):
                    pre_blocked_by_other += 1

            _log_jsonl(
                entry_log,
                {
                    "ts_kst": _to_kst(ts_sig),
                    "symbol": sym,
                    "env_on": env_on,
                    "blocked": False,
                    "block_reasons": [],
                    "entry_path": entry_path,
                    "path_trigger": path_trigger,
                    "uptrend_subflags": list(uptrend_subflags),
                    "consec_bull_15m": consec_bull_count,
                    "hhhl": hh_hl,
                    "close15_gt_ema20": close_gt_ema20,
                    "ema20_slope": ema20_slope,
                    "ema20_slope_thr": ema20_slope_thr,
                    "hod": hod,
                    "retest_lower": retest_lower,
                    "retest_upper": retest_upper,
                    "touch_retest": bool(high_3m >= retest_lower and high_3m <= retest_upper),
                    "close": close_3m,
                    "ema7": ema7,
                    "ema20": ema20_3m,
                    "atr": atr_3m,
                    "dist_ema7": dist_ema7,
                    "ext_atr": ext_atr,
                    "close_lt_ema7": close_lt_ema7,
                    "entry_ready": entry_ready,
                    "armed": armed,
                    "armed_ttl": armed_ttl,
                },
            )

            if not entry_ready:
                continue

            entry_ready_bars += 1

            sl_base = max(high_3m, retest_upper)
            if entry_path == "B":
                swing_high = last_pivot_high if last_pivot_high is not None else high_3m
                sl_base = max(swing_high, ema20_3m)
            sl_px = sl_base + cfg.sl_atr_mult * float(atr_3m)
            r_val = sl_px - open_now
            if r_val <= 0:
                continue
            tp1 = open_now - cfg.tp_r1 * r_val
            tp2 = open_now - cfg.tp_r2 * r_val

            # PATH_A shadow-only mode
            if entry_path == "A":
                shadow_trade = {
                    "entry_px": open_now,
                    "entry_idx": i,
                    "sl_px": sl_px,
                    "tp1": tp1,
                    "tp2": tp2,
                    "r_val": r_val,
                    "remaining": 1.0,
                    "realized_r": 0.0,
                }
                _log(
                    f"[SHADOW_ENTRY] {sym} path=A entry={open_now:.6f} sl={sl_px:.6f} tp1={tp1:.6f} tp2={tp2:.6f}"
                )
                continue

            open_trade = {
                "entry_px": open_now,
                "entry_idx": i,
                "sl_px": sl_px,
                "tp1": tp1,
                "tp2": tp2,
                "r_val": r_val,
                "remaining": size,
                "realized_r": 0.0,
                "entry_path": entry_path,
                "mfi_bucket": entry_mfi_bucket if "entry_mfi_bucket" in locals() else None,
            }
            stats["entries"] += 1
            _log(
                f"[ENTRY] {sym} path={entry_path} entry={open_now:.6f} sl={sl_px:.6f} tp1={tp1:.6f} tp2={tp2:.6f}"
            )

    trades = stats["trades"]
    winrate = (stats["wins"] / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats["mfe_sum"] / trades if trades > 0 else 0.0
    avg_mae = stats["mae_sum"] / trades if trades > 0 else 0.0
    avg_hold = stats["hold_sum"] / trades if trades > 0 else 0.0

    print(
        "[BACKTEST] TOTAL entries={entries} exits={exits} trades={trades} wins={wins} losses={losses} "
        "winrate={winrate:.2f}% avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} "
        "tp_sum={tp_sum:.3f} sl_sum={sl_sum:.3f} net_sum={net_sum:.3f}".format(
            entries=stats["entries"],
            exits=stats["exits"],
            trades=stats["trades"],
            wins=stats["wins"],
            losses=stats["losses"],
            winrate=winrate,
            avg_mfe=avg_mfe,
            avg_mae=avg_mae,
            avg_hold=avg_hold,
            tp_sum=stats["tp_sum"],
            sl_sum=stats["sl_sum"],
            net_sum=stats["net_sum"],
        )
    )
    print(
        "[BACKTEST] FUNNEL env_on_bars={env_on} entry_eval_bars={evals} entry_touch_bars={touch} entry_reject_bars={reject} armed_and_reject_bars={armed_reject} armed_and_break_low_bars={armed_break} entry_ready_pre_bars={pre} pre_blocked_by_wash={pre_wash} pre_blocked_by_uptrend={pre_up} pre_blocked_by_extension={pre_ext} pre_blocked_by_breakout={pre_brk} pre_blocked_by_mfi_hot={pre_mfi} pre_blocked_by_other={pre_oth} delay_entries={delay_e} delay_missed={delay_m} retrace_entries={rt_e} retrace_missed={rt_m} entry_path_break_low={path_low} entry_path_reject_wash={path_wash} entries_quality_A={qa} entries_quality_B={qb} retest_touch_bars={rtouch} wash_ok_bars={wash} armed_bars={armed} entry_ready_bars={entry_ready} entries={entries}".format(
            env_on=env_on_bars,
            evals=entry_eval_bars,
            touch=entry_touch_bars,
            reject=entry_reject_bars,
            armed_reject=armed_and_reject_bars,
            armed_break=armed_and_break_low_bars,
            pre=entry_ready_pre_bars,
            pre_wash=pre_blocked_by_wash,
            pre_up=pre_blocked_by_uptrend,
            pre_ext=pre_blocked_by_extension,
            pre_brk=pre_blocked_by_breakout,
            pre_mfi=pre_blocked_by_mfi_hot,
            pre_oth=pre_blocked_by_other,
            delay_e=delay_entries,
            delay_m=delay_missed,
            rt_e=retrace_entries,
            rt_m=retrace_missed,
            path_low=entry_path_break_low,
            path_wash=entry_path_reject_wash,
            qa=entries_quality_A,
            qb=entries_quality_B,
            rtouch=retest_touch_bars,
            wash=wash_ok_bars,
            armed=armed_bars,
            entry_ready=entry_ready_bars,
            entries=stats["entries"],
        )
    )
    print("[BACKTEST] BLOCKS " + " ".join([f"{k}={v}" for k, v in block_counts.items()]))
    if trades_by_mfi_bucket:
        print("[BACKTEST] MFI_BUCKETS")
        for bucket in sorted(trades_by_mfi_bucket.keys()):
            trades = trades_by_mfi_bucket[bucket]
            wins = wins_by_mfi_bucket.get(bucket, 0)
            winrate = (wins / trades * 100.0) if trades > 0 else 0.0
            avg_mae = mae_by_mfi_bucket.get(bucket, 0.0) / trades if trades > 0 else 0.0
            net_r = net_by_mfi_bucket.get(bucket, 0.0)
            print(f"  {bucket}: trades={trades} winrate={winrate:.2f}% avg_mae={avg_mae:.4f} net_r={net_r:.3f}")

    if entries_by_path:
        print("[BACKTEST] ENTRY_PATHS")
        for path in sorted(entries_by_path.keys()):
            trades = entries_by_path.get(path, 0)
            wins = wins_by_path.get(path, 0)
            winrate = (wins / trades * 100.0) if trades > 0 else 0.0
            avg_mae = mae_by_path.get(path, 0.0) / trades if trades > 0 else 0.0
            net_r = net_by_path.get(path, 0.0)
            print(f"  {path}: trades={trades} winrate={winrate:.2f}% avg_mae={avg_mae:.4f} net_r={net_r:.3f}")

    if path_trigger_counts:
        print("[BACKTEST] PATH_TRIGGERS " + " ".join([f"{k}={v}" for k, v in path_trigger_counts.items()]))

    if shadow_trades:
        shadow_winrate = (shadow_wins / shadow_trades * 100.0) if shadow_trades > 0 else 0.0
        shadow_avg_mae = shadow_mae / shadow_trades if shadow_trades > 0 else 0.0
        print(
            f"[BACKTEST] SHADOW_A trades={shadow_trades} winrate={shadow_winrate:.2f}% avg_mae={shadow_avg_mae:.4f} net_r={shadow_net:.3f}"
        )


if __name__ == "__main__":
    run_backtest()
