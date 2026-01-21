#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import ccxt

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.swaggy_atlas_lab_v2.atlas_eval import evaluate_global_gate, evaluate_local
from engines.swaggy_atlas_lab_v2.broker_sim import BrokerSim
from engines.swaggy_atlas_lab_v2.config import AtlasConfig, BacktestConfig, SwaggyConfig
from engines.dtfx.engine import DTFXConfig
from engines.swaggy_atlas_lab_v2.data import (
    ensure_dir,
    fetch_ohlcv_all,
    slice_df,
    to_df,
    save_universe,
)
from engines.universe import build_universe_from_tickers
from engines.swaggy_atlas_lab_v2.policy import AtlasMode, apply_policy
from engines.swaggy_atlas_lab_v2.report import (
    build_summary,
    write_shadow_summary_json,
    write_summary_json,
    write_trades_csv,
)
from engines.swaggy_atlas_lab_v2.indicators import atr, ema
from engines.swaggy_atlas_lab_v2.swaggy_signal import SwaggySignalEngine


def _append_backtest_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_atlas_lab_v2", "backtest", f"backtest_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _append_backtest_entry_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_atlas_lab_v2", "backtest", f"backtest_entries_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _iso_kst_ms(ts_ms: int) -> str:
    tz = timezone(timedelta(hours=9))
    try:
        dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=tz)
        return dt.isoformat(timespec="seconds")
    except Exception:
        return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


def _append_swaggy_trade_json(payload: Dict[str, object]) -> None:
    try:
        path = os.path.join("logs", "swaggy_trades.jsonl")
        ensure_dir(os.path.dirname(path))
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    except Exception:
        pass


def _entry_quality_bucket(
    atlas_pass_hard: Optional[bool],
    confirm_pass: Optional[int],
    overext_dist_at_entry: Optional[float],
    trigger_combo: str,
    trigger_parts: Dict[str, float],
    strength_total: float,
    strength_min_req: float,
) -> tuple[str, List[str]]:
    reasons: List[str] = []
    if atlas_pass_hard:
        reasons.append("ATLAS_HARD_OK")
    if confirm_pass:
        reasons.append("CONFIRM_OK")
    if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry <= 1.0:
        reasons.append("LOW_OVEREXT")
    strong_combo = False
    combo_set = {c.strip().upper() for c in (trigger_combo or "").split("+") if c.strip()}
    if combo_set == {"SWEEP", "RECLAIM"}:
        strong_combo = True
    rej_strength = trigger_parts.get("REJECTION")
    if rej_strength is not None and rej_strength >= 0.56:
        strong_combo = True
    if strong_combo:
        reasons.append("TRIGGER_COMBO_STRONG")
    if strength_total >= (strength_min_req + 0.08):
        reasons.append("STRENGTH_ABOVE_MIN")

    if (
        atlas_pass_hard
        and confirm_pass
        and isinstance(overext_dist_at_entry, (int, float))
        and overext_dist_at_entry <= 1.0
        and strong_combo
        and strength_total >= (strength_min_req + 0.08)
    ):
        return "A", reasons
    if (
        atlas_pass_hard
        and confirm_pass
        and isinstance(overext_dist_at_entry, (int, float))
        and overext_dist_at_entry <= 1.35
    ):
        return "B", reasons
    fail_reasons = []
    if not atlas_pass_hard:
        fail_reasons.append("ATLAS_HARD_FAIL")
    if not confirm_pass:
        fail_reasons.append("CONFIRM_FAIL")
    if isinstance(overext_dist_at_entry, (int, float)) and overext_dist_at_entry > 1.35:
        fail_reasons.append("HIGH_OVEREXT")
    if not strong_combo:
        fail_reasons.append("WEAK_TRIGGER")
    if strength_total < (strength_min_req + 0.08):
        fail_reasons.append("LOW_STRENGTH")
    return "C", reasons + fail_reasons


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})


def parse_args():
    parser = argparse.ArgumentParser(description="Swaggy x Atlas Lab backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--mode", default="all", choices=("hard", "soft", "shadow", "off", "hybrid", "all"))
    parser.add_argument("--symbols", default="", help="comma-separated symbols")
    parser.add_argument("--symbols-file", default="", help="path to fixed symbol list")
    parser.add_argument("--universe", default="top50", help="topN (e.g. top50) or 'symbols'")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--anchor", default="BTC,ETH")
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--sl-pct", type=float, default=0.0)
    parser.add_argument("--fee", type=float, default=0.0)
    parser.add_argument("--slippage", type=float, default=0.0)
    parser.add_argument("--timeout-bars", type=int, default=0)
    parser.add_argument("--cooldown-min", type=int, default=0)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _parse_universe_arg(text: str) -> int:
    if text.lower().startswith("top"):
        try:
            return int(text.lower().replace("top", ""))
        except Exception:
            return 50
    return 0


def _overext_dist(df, side: str, cfg: SwaggyConfig) -> float:
    if df.empty or len(df) < cfg.overext_ema_len + 2:
        return 0.0
    ema_series = ema(df["close"], cfg.overext_ema_len)
    if ema_series.empty:
        return 0.0
    ema_val = float(ema_series.iloc[-1])
    last_price = float(df["close"].iloc[-1])
    atr_val = atr(df, cfg.touch_atr_len)
    if atr_val <= 0:
        return 0.0
    side = (side or "").upper()
    if side == "SHORT":
        return (ema_val - last_price) / atr_val
    return (last_price - ema_val) / atr_val


def main() -> None:
    args = parse_args()
    if args.sl_pct <= 0:
        raise SystemExit("--sl-pct is required")
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(args.days) * 24 * 60 * 60 * 1000
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    run_id = time.strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join("logs", "swaggy_atlas_lab_v2", "backtest")
    rep_dir = os.path.join("reports", "swaggy_atlas_lab_v2")
    ensure_dir(log_dir)
    ensure_dir(rep_dir)
    log_path = os.path.join(log_dir, f"bt_{run_id}.log")
    trades_path = os.path.join(rep_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(rep_dir, f"summary_{run_id}.json")
    shadow_path = os.path.join(rep_dir, f"shadow_{run_id}.json")
    universe_path = os.path.join(rep_dir, f"universe_{run_id}.json")

    bt_cfg = BacktestConfig(
        tp_pct=args.tp_pct,
        sl_pct=args.sl_pct,
        fee_rate=args.fee,
        slippage_pct=args.slippage,
        timeout_bars=args.timeout_bars,
        mode=args.mode,
    )
    sw_cfg = SwaggyConfig()
    at_cfg = AtlasConfig()
    if isinstance(args.cooldown_min, int) and args.cooldown_min > 0:
        sw_cfg.cooldown_min = int(args.cooldown_min)

    ex = _make_exchange()
    ex.load_markets()
    ltf_minutes = float(ex.parse_timeframe(sw_cfg.tf_ltf)) / 60.0
    anchor_symbols = [s.strip() for s in args.anchor.split(",") if s.strip()]
    symbols: List[str] = []
    if args.symbols_file.strip():
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            symbols = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    elif args.symbols.strip():
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        tickers = ex.fetch_tickers()
        dtfx_cfg = DTFXConfig()
        min_qv = max(dtfx_cfg.min_quote_volume_usdt, dtfx_cfg.low_liquidity_qv_usdt)
        anchors = []
        for s in dtfx_cfg.anchor_symbols or []:
            anchors.append(s if "/" in s else f"{s}/USDT:USDT")
        symbols = build_universe_from_tickers(
            tickers,
            symbols=list(tickers.keys()),
            min_quote_volume_usdt=min_qv,
            top_n=dtfx_cfg.universe_top_n,
            anchors=tuple(anchors),
        )
    source = "symbols_file" if args.symbols_file.strip() else ("symbols_arg" if args.symbols.strip() else "dtfx_universe")
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    save_universe(
        universe_path,
        symbols,
        {
            "source": source,
            "days": str(args.days),
            "start_ms": str(start_ms),
            "end_ms": str(end_ms),
        },
    )

    data_by_sym: Dict[str, Dict[str, object]] = {}
    tfs = [sw_cfg.tf_ltf, sw_cfg.tf_mtf, sw_cfg.tf_htf, sw_cfg.tf_htf2, sw_cfg.tf_d1, "3m"]
    for sym in symbols:
        tf_map: Dict[str, object] = {}
        for tf in tfs:
            rows = fetch_ohlcv_all(ex, sym, tf, start_ms, end_ms)
            tf_map[tf] = to_df(rows)
        data_by_sym[sym] = tf_map

    btc_data = fetch_ohlcv_all(ex, at_cfg.ref_symbol, sw_cfg.tf_mtf, start_ms, end_ms)
    btc_df = to_df(btc_data)

    modes = [
        AtlasMode.HARD,
        AtlasMode.SOFT,
        AtlasMode.SHADOW,
        AtlasMode.OFF,
        AtlasMode.HYBRID,
    ] if args.mode == "all" else [AtlasMode(args.mode)]
    trades: List[Dict] = []
    stats_by_key: Dict[tuple[str, str], Dict[str, float]] = {}
    trade_logs: Dict[tuple[str, str], List[dict]] = {}
    open_trades: Dict[tuple[str, str], Dict[str, object]] = {}
    last_close_by_sym: Dict[str, float] = {}
    last_ts_by_sym: Dict[str, int] = {}
    overext_by_key: Dict[tuple[str, str], Dict[str, int]] = {}
    d1_block_by_key: Dict[tuple[str, str], int] = {}
    entry_syms_by_mode: Dict[str, set] = {}
    last_day_exits_by_mode: Dict[str, int] = {}
    last_day_start_ms = end_ms - 24 * 60 * 60 * 1000

    with open(log_path, "a", encoding="utf-8") as log_fp:
        for mode in modes:
            run_line = f"[run] mode={mode.value} days={args.days} start_ms={start_ms} end_ms={end_ms}"
            log_fp.write(run_line + "\n")
            _append_backtest_log(run_line)
            engine = SwaggySignalEngine(sw_cfg)
            broker = BrokerSim(bt_cfg.tp_pct, bt_cfg.sl_pct, bt_cfg.fee_rate, bt_cfg.slippage_pct, bt_cfg.timeout_bars)
            for sym in symbols:
                sym_state = engine._state.setdefault(sym, {})
                df_ltf = data_by_sym[sym][sw_cfg.tf_ltf]
                df_mtf = data_by_sym[sym][sw_cfg.tf_mtf]
                df_htf = data_by_sym[sym][sw_cfg.tf_htf]
                df_htf2 = data_by_sym[sym][sw_cfg.tf_htf2]
                df_d1 = data_by_sym[sym][sw_cfg.tf_d1]
                if df_ltf is not None and not df_ltf.empty:
                    try:
                        last_close_by_sym[sym] = float(df_ltf.iloc[-1]["close"])
                        last_ts_by_sym[sym] = int(df_ltf.iloc[-1]["ts"])
                    except Exception:
                        pass
                for i in range(30, len(df_ltf)):
                    cur = df_ltf.iloc[i]
                    ts_ms = int(cur["ts"])
                    now_ts = ts_ms / 1000.0
                    d5 = df_ltf.iloc[: i + 1]
                    d3 = slice_df(data_by_sym[sym]["3m"], ts_ms)
                    d15 = slice_df(df_mtf, ts_ms)
                    d1h = slice_df(df_htf, ts_ms)
                    d4h = slice_df(df_htf2, ts_ms)
                    prev_phase = sym_state.get("phase")
                    d1d = slice_df(df_d1, ts_ms)
                    signal = engine.evaluate_symbol(sym, d4h, d1h, d15, d5, d3, d1d, now_ts)
                    debug = signal.debug if isinstance(signal.debug, dict) else {}
                    event_list = debug.get("events") if isinstance(debug.get("events"), list) else []
                    if event_list:
                        for event in event_list:
                            if not isinstance(event, dict):
                                continue
                            payload = {
                                "ts": _iso_kst_ms(ts_ms),
                                "event": event.get("event") or "SWAGGY_EVENT",
                                "engine": "SWAGGY_ATLAS_LAB",
                                "mode": mode.value,
                                "symbol": sym,
                                "side": event.get("side") or signal.side,
                                "ltf": sw_cfg.tf_ltf,
                                "mtf": sw_cfg.tf_mtf,
                                "htf": sw_cfg.tf_htf,
                                "htf2": sw_cfg.tf_htf2,
                                "cycle_id": ts_ms,
                                "range_id": event.get("range_id") or debug.get("touch_key"),
                            }
                            payload.update(event)
                            _append_swaggy_trade_json(payload)
                    new_phase = sym_state.get("phase")
                    key = (mode.value, sym)
                    overext_stats = overext_by_key.setdefault(
                        key,
                        {
                            "entry_ready_seen": 0,
                            "overext_block_count": 0,
                            "overext_recover_count": 0,
                        },
                    )
                    if isinstance(signal.reasons, list) and ("ENTRY_READY" in signal.reasons or "CHASE" in signal.reasons):
                        overext_stats["entry_ready_seen"] += 1
                    if isinstance(signal.reasons, list) and "D1_EMA7_DIST" in signal.reasons:
                        d1_key = (mode.value, sym)
                        d1_block_by_key[d1_key] = d1_block_by_key.get(d1_key, 0) + 1
                    if prev_phase != "CHASE" and new_phase == "CHASE":
                        dist = _overext_dist(d5, signal.side or "", sw_cfg)
                        thresh = sw_cfg.overext_atr_mult
                        sym_state["overext_blocked"] = True
                        line = (
                            f"OVEREXT_BLOCK ts={ts_ms} sym={sym} side={signal.side} "
                            f"dist={dist:.4f} thresh={thresh:.2f} state=ENTRY_READY->CHASE"
                        )
                        log_fp.write(line + "\n")
                        _append_backtest_log(line)
                        overext_stats["overext_block_count"] += 1
                    if prev_phase == "CHASE" and new_phase == "WAIT_TRIGGER":
                        chase_side = sym_state.get("chase_side") or signal.side or ""
                        dist = _overext_dist(d5, chase_side, sw_cfg)
                        thresh = sw_cfg.overext_atr_mult
                        line = (
                            f"OVEREXT_RECOVER ts={ts_ms} sym={sym} side={chase_side} "
                            f"dist={dist:.4f} thresh={thresh:.2f} state=CHASE->WAIT_TRIGGER"
                        )
                        log_fp.write(line + "\n")
                        _append_backtest_log(line)
                        overext_stats["overext_recover_count"] += 1
                    side = signal.side or ""
                    entry_px = signal.entry_px

                    if broker.has_position(sym):
                        trade = broker.on_bar(sym, ts_ms, float(cur["high"]), float(cur["low"]), float(cur["close"]), i)
                        if trade:
                            pnl_pct = broker.calc_pnl_pct(trade)
                            pnl_usdt = pnl_pct * trade.size_usdt
                            fee = trade.size_usdt * bt_cfg.fee_rate * 2
                            trades.append(
                                {
                                    "run_id": run_id,
                                    "mode": mode.value,
                                    "sym": sym,
                                    "side": trade.side,
                                    "entry_ts": trade.entry_ts,
                                    "entry_price": trade.entry_price,
                                    "exit_ts": trade.exit_ts,
                                    "exit_price": trade.exit_price,
                                    "exit_reason": trade.exit_reason,
                                    "pnl_usdt": pnl_usdt,
                                    "pnl_pct": pnl_pct,
                                    "policy_action": trade.policy_action,
                                    "overext_dist_at_entry": trade.overext_dist_at_entry,
                                    "overext_blocked": "Y" if trade.overext_blocked else "N",
                                    "fee": fee,
                                    "duration_bars": trade.bars,
                                    "mfe": trade.mfe,
                                    "mae": abs(trade.mae),
                                    "sw_strength": trade.sw_strength,
                                    "sw_reasons": trade.sw_reasons,
                                    "atlas_pass": trade.atlas_pass,
                                    "atlas_mult": trade.atlas_mult,
                                    "atlas_reasons": trade.atlas_reasons,
                                    "atlas_shadow_pass": trade.atlas_shadow_pass,
                                    "atlas_shadow_reasons": trade.atlas_shadow_reasons,
                                }
                            )
                            if trade.context and trade.exit_ts is not None:
                                payload = dict(trade.context)
                                entry_price = float(payload.get("entry_price") or 0.0)
                                atr14 = payload.get("atr14_ltf")
                                mfe_atr = None
                                mae_atr = None
                                if isinstance(atr14, (int, float)) and atr14 > 0 and entry_price > 0:
                                    mfe_atr = (trade.mfe * entry_price) / atr14
                                    mae_atr = (abs(trade.mae) * entry_price) / atr14
                                pnl_r = (pnl_pct / bt_cfg.sl_pct) if bt_cfg.sl_pct > 0 else None
                                payload.update(
                                    {
                                        "exit_ts": _iso_kst_ms(trade.exit_ts),
                                        "exit_price": trade.exit_price,
                                        "exit_reason": trade.exit_reason,
                                        "pnl_r": pnl_r,
                                        "pnl_pct": pnl_pct,
                                        "mfe_atr": mfe_atr,
                                        "mae_atr": mae_atr,
                                        "hold_bars_ltf": trade.bars,
                                    }
                                )
                                _append_swaggy_trade_json(payload)
                            log_fp.write(
                                f"EXIT ts={trade.exit_ts} sym={sym} pnl_usdt={pnl_usdt:.4f} "
                                f"pnl_pct={pnl_pct:.4f} reason={trade.exit_reason} duration_bars={trade.bars}\n"
                            )
                            _append_backtest_log(
                                f"EXIT ts={trade.exit_ts} sym={sym} pnl_usdt={pnl_usdt:.4f} "
                                f"pnl_pct={pnl_pct:.4f} reason={trade.exit_reason} duration_bars={trade.bars}"
                            )
                            stats = stats_by_key.setdefault(
                                key,
                                {
                                    "entries": 0,
                                    "exits": 0,
                                    "trades": 0,
                                    "wins": 0,
                                    "losses": 0,
                                    "tp": 0,
                                    "sl": 0,
                                    "mfe_sum": 0.0,
                                    "mae_sum": 0.0,
                                    "hold_sum": 0.0,
                                },
                            )
                            stats["trades"] += 1
                            stats["exits"] += 1
                            if pnl_pct >= 0:
                                stats["wins"] += 1
                            else:
                                stats["losses"] += 1
                            if trade.exit_reason == "TP":
                                stats["tp"] += 1
                            elif trade.exit_reason == "SL":
                                stats["sl"] += 1
                            stats["mfe_sum"] += trade.mfe
                            stats["mae_sum"] += abs(trade.mae)
                            stats["hold_sum"] += float(trade.bars) * ltf_minutes
                            if isinstance(trade.exit_ts, (int, float)) and trade.exit_ts >= last_day_start_ms:
                                last_day_exits_by_mode[mode.value] = last_day_exits_by_mode.get(mode.value, 0) + 1
                            entry_dt = ""
                            if isinstance(trade.entry_ts, (int, float)) and trade.entry_ts:
                                entry_dt = datetime.fromtimestamp(trade.entry_ts / 1000.0, tz=timezone.utc).strftime(
                                    "%Y-%m-%d %H:%M"
                                )
                            exit_dt = ""
                            if isinstance(trade.exit_ts, (int, float)) and trade.exit_ts:
                                exit_dt = datetime.fromtimestamp(trade.exit_ts / 1000.0, tz=timezone.utc).strftime(
                                    "%Y-%m-%d %H:%M"
                                )
                            trade_logs.setdefault(key, []).append(
                                {
                                    "entry_ts": trade.entry_ts or 0,
                                    "line": "[BACKTEST][EXIT] sym=%s mode=%s side=%s entry_dt=%s exit_dt=%s "
                                    "entry_px=%.6g exit_px=%.6g reason=%s"
                                    % (
                                        sym,
                                        mode.value,
                                        trade.side,
                                        entry_dt,
                                        exit_dt,
                                        trade.entry_price,
                                        trade.exit_price or 0.0,
                                        trade.exit_reason,
                                    ),
                                }
                            )
                        continue

                    if not signal.entry_ok or not side or entry_px is None:
                        continue

                    atlas = None
                    gate = None
                    if mode != AtlasMode.OFF:
                        btc_slice = slice_df(btc_df, ts_ms)
                        gate = evaluate_global_gate(btc_slice, at_cfg)
                        atlas = evaluate_local(sym, side, d15, btc_slice, gate, at_cfg)

                    shadow_pass = atlas.pass_hard if atlas else None
                    shadow_mult = atlas.atlas_mult if atlas else None
                    shadow_reasons = atlas.reasons if atlas else None

                    policy = apply_policy(mode, bt_cfg.base_usdt, atlas)
                    entry_line = (
                        "ENTRY ts=%d sym=%s side=%s mode=%s sw_ok=%s sw_strength=%.3f sw_reasons=%s "
                        "base_usdt=%.2f final_usdt=%.2f atlas_pass=%s atlas_mult=%s atlas_reasons=%s "
                        "atlas_shadow_pass=%s atlas_shadow_mult=%s atlas_shadow_reasons=%s policy_action=%s"
                        % (
                            ts_ms,
                            sym,
                            side,
                            mode.value,
                            "Y" if signal.entry_ok else "N",
                            signal.strength,
                            signal.reasons,
                            bt_cfg.base_usdt,
                            policy.final_usdt,
                            policy.atlas_pass if policy.atlas_pass is not None else "N/A",
                            policy.atlas_mult if policy.atlas_mult is not None else "N/A",
                            policy.atlas_reasons if policy.atlas_reasons is not None else "N/A",
                            shadow_pass if shadow_pass is not None else "N/A",
                            shadow_mult if shadow_mult is not None else "N/A",
                            shadow_reasons if shadow_reasons is not None else "N/A",
                            policy.policy_action if policy.policy_action is not None else "N/A",
                        )
                    )
                    log_fp.write(entry_line + "\n")
                    _append_backtest_log(entry_line)
                    if not policy.allow:
                        continue
                    dist_at_entry = _overext_dist(d5, side, sw_cfg)
                    dist_at_entry_abs = abs(dist_at_entry) if isinstance(dist_at_entry, (int, float)) else dist_at_entry
                    overext_blocked = bool(sym_state.get("overext_blocked"))
                    sym_state["overext_blocked"] = False
                    beta_val = atlas.metrics.get("beta") if atlas and atlas.metrics else None
                    if (
                        sw_cfg.skip_beta_mid
                        and isinstance(beta_val, (int, float))
                        and 1.0 <= float(beta_val) < 1.5
                    ):
                        line = f"SKIP ts={ts_ms} sym={sym} reason=SKIP_BETA_MID beta={beta_val:.4g}"
                        log_fp.write(line + "\n")
                        _append_backtest_log(line)
                        continue
                    if (
                        sw_cfg.skip_overext_mid
                        and isinstance(dist_at_entry_abs, (int, float))
                        and 1.1 <= float(dist_at_entry_abs) < 1.4
                    ):
                        line = f"SKIP ts={ts_ms} sym={sym} reason=SKIP_OVEREXT_MID overext={dist_at_entry_abs:.4g}"
                        log_fp.write(line + "\n")
                        _append_backtest_log(line)
                        continue
                    body_ratio = None
                    confirm_metrics = debug.get("confirm_metrics") if isinstance(debug, dict) else None
                    if isinstance(confirm_metrics, dict):
                        body_ratio = confirm_metrics.get("body_ratio")
                    if (
                        sw_cfg.skip_confirm_body
                        and isinstance(body_ratio, (int, float))
                        and float(body_ratio) < 0.60
                    ):
                        line = f"SKIP ts={ts_ms} sym={sym} reason=SKIP_CONFIRM_BODY body_ratio={body_ratio:.4g}"
                        log_fp.write(line + "\n")
                        _append_backtest_log(line)
                        continue
                    entry_quality, entry_quality_reasons = _entry_quality_bucket(
                        bool(atlas.pass_hard) if atlas and atlas.pass_hard is not None else False,
                        int(debug.get("confirm_pass") or 0),
                        dist_at_entry_abs,
                        str(debug.get("trigger_combo") or ""),
                        debug.get("trigger_parts") if isinstance(debug.get("trigger_parts"), dict) else {},
                        float(debug.get("strength_total") or 0.0),
                        float(debug.get("strength_min_req") or 0.0),
                    )
                    trade_context = {
                        "ts": _iso_kst_ms(ts_ms),
                        "event": "SWAGGY_TRADE",
                        "engine": "SWAGGY_ATLAS_LAB",
                        "mode": mode.value,
                        "symbol": sym,
                        "side": side,
                        "ltf": sw_cfg.tf_ltf,
                        "mtf": sw_cfg.tf_mtf,
                        "htf": sw_cfg.tf_htf,
                        "htf2": sw_cfg.tf_htf2,
                        "cycle_id": ts_ms,
                        "range_id": debug.get("touch_key"),
                        "entry_ts": _iso_kst_ms(ts_ms),
                        "entry_price": entry_px,
                        "atr14_ltf": debug.get("atr14_ltf"),
                        "atr14_htf": debug.get("atr14_htf"),
                        "ema20_ltf": debug.get("ema20_ltf"),
                        "ema20_htf": debug.get("ema20_htf"),
                        "level_type": debug.get("level_type"),
                        "level_price": debug.get("level_price"),
                        "level_score": debug.get("level_score"),
                        "touch_count": debug.get("touch_count"),
                        "level_age_bars": debug.get("level_age_bars"),
                        "touch_pct": debug.get("touch_pct"),
                        "touch_atr_mult": debug.get("touch_atr_mult"),
                        "touch_pass": int(debug.get("touch_pass") or 0),
                        "touch_fail_reason": debug.get("touch_fail_reason"),
                        "trigger_combo": debug.get("trigger_combo"),
                        "trigger_strength_best": debug.get("trigger_strength_best"),
                        "trigger_strength_min": debug.get("trigger_strength_min"),
                        "trigger_strength_avg": debug.get("trigger_strength_avg"),
                        "trigger_strength_used": debug.get("trigger_strength_used"),
                        "trigger_parts": debug.get("trigger_parts"),
                        "strength_total": debug.get("strength_total"),
                        "strength_min_req": debug.get("strength_min_req"),
                        "trigger_threshold_used": debug.get("trigger_threshold_used"),
                        "use_trigger_min": debug.get("use_trigger_min"),
                        "confirm_pass": int(debug.get("confirm_pass") or 0),
                        "confirm_fail_reason": debug.get("confirm_fail"),
                        "confirm_metrics": debug.get("confirm_metrics"),
                        "confirm_body_ratio": (debug.get("confirm_metrics") or {}).get("body_ratio")
                        if isinstance(debug.get("confirm_metrics"), dict)
                        else None,
                        "overext_ema_len": debug.get("overext_ema_len"),
                        "overext_atr_mult": debug.get("overext_atr_mult"),
                        "overext_dist_at_touch": debug.get("overext_dist_at_touch"),
                        "overext_dist_at_entry": dist_at_entry_abs,
                        "overext_blocked": 1 if overext_blocked else 0,
                        "overext_state": "CHASE" if overext_blocked else "OK",
                        "atlas_pass_soft": atlas.metrics.get("pass_soft") if atlas and atlas.metrics else None,
                        "atlas_pass_hard": atlas.metrics.get("pass_hard") if atlas and atlas.metrics else None,
                        "atlas_state": gate.get("reason") if gate else None,
                        "atlas_regime": atlas.metrics.get("regime") if atlas and atlas.metrics else None,
                        "atlas_rs": atlas.metrics.get("rs") if atlas and atlas.metrics else None,
                        "atlas_rs_z": atlas.metrics.get("rs_z") if atlas and atlas.metrics else None,
                        "atlas_corr": atlas.metrics.get("corr") if atlas and atlas.metrics else None,
                        "atlas_beta": atlas.metrics.get("beta") if atlas and atlas.metrics else None,
                        "atlas_vol_ratio": atlas.metrics.get("vol_ratio") if atlas and atlas.metrics else None,
                        "atlas_block_reason": policy.policy_action if policy and not policy.allow else None,
                        "entry_quality_bucket": entry_quality,
                        "entry_quality_reasons": entry_quality_reasons,
                    }
                    _append_backtest_entry_log(
                        "engine=swaggy_atlas_lab_v2 mode=%s symbol=%s side=%s entry=%.6g "
                        "final_usdt=%.2f sw_strength=%.3f sw_reasons=%s atlas_pass=%s atlas_mult=%s "
                        "atlas_reasons=%s atlas_shadow_pass=%s atlas_shadow_mult=%s atlas_shadow_reasons=%s policy_action=%s"
                        % (
                            mode.value,
                            sym,
                            side,
                            float(entry_px or 0.0),
                            float(policy.final_usdt or 0.0),
                            float(signal.strength or 0.0),
                            ",".join(signal.reasons or []),
                            policy.atlas_pass if policy.atlas_pass is not None else "N/A",
                            policy.atlas_mult if policy.atlas_mult is not None else "N/A",
                            ",".join(policy.atlas_reasons or []),
                            shadow_pass if shadow_pass is not None else "N/A",
                            shadow_mult if shadow_mult is not None else "N/A",
                            ",".join(shadow_reasons or []),
                            policy.policy_action if policy.policy_action is not None else "N/A",
                        )
                    )
                    broker.enter(
                        sym,
                        side,
                        ts_ms,
                        i,
                        entry_px,
                        policy.final_usdt,
                        sw_strength=signal.strength,
                        sw_reasons=signal.reasons,
                        atlas_pass=policy.atlas_pass,
                        atlas_mult=policy.atlas_mult,
                        atlas_reasons=policy.atlas_reasons,
                        atlas_shadow_pass=shadow_pass if mode == AtlasMode.SHADOW else None,
                        atlas_shadow_reasons=shadow_reasons if mode == AtlasMode.SHADOW else None,
                        policy_action=policy.policy_action,
                        overext_dist_at_entry=dist_at_entry_abs,
                        overext_blocked=overext_blocked,
                        context=trade_context,
                    )
                    entry_syms_by_mode.setdefault(mode.value, set()).add(sym)
                    stats = stats_by_key.setdefault(
                        key,
                        {
                            "entries": 0,
                            "exits": 0,
                            "trades": 0,
                            "wins": 0,
                            "losses": 0,
                            "tp": 0,
                            "sl": 0,
                            "mfe_sum": 0.0,
                            "mae_sum": 0.0,
                            "hold_sum": 0.0,
                        },
                    )
                    stats["entries"] += 1
                    # ENTRY summary is represented by OPEN/EXIT logs (avoid duplicate lines).
        for sym, trade in broker.positions.items():
            open_trades[(mode.value, sym)] = {
                "sym": sym,
                "mode": mode.value,
                "side": trade.side,
                "entry_price": trade.entry_price,
                "entry_ts": trade.entry_ts,
            }

    write_trades_csv(trades_path, trades)
    summary = build_summary(run_id, trades)
    if overext_by_key:
        by_mode: Dict[str, Dict[str, int]] = {}
        total = {"entry_ready_seen": 0, "overext_block_count": 0, "overext_recover_count": 0}
        for (mode, _sym), stats in overext_by_key.items():
            bucket = by_mode.setdefault(mode, {"entry_ready_seen": 0, "overext_block_count": 0, "overext_recover_count": 0})
            for key in ("entry_ready_seen", "overext_block_count", "overext_recover_count"):
                bucket[key] += int(stats.get(key) or 0)
                total[key] += int(stats.get(key) or 0)
        for bucket in list(by_mode.values()) + [total]:
            denom = float(bucket.get("entry_ready_seen") or 0)
            bucket["overext_block_rate"] = (bucket.get("overext_block_count", 0) / denom) if denom else 0.0
        summary["overext"] = {"by_mode": by_mode, "total": total}
    write_summary_json(summary_path, summary)
    shadow_payload = {}
    if isinstance(summary.get("modes"), dict) and "shadow" in summary["modes"]:
        shadow_payload = {
            "run_id": run_id,
            "shadow_ab": summary["modes"]["shadow"].get("shadow_ab"),
            "shadow_fail_by_reason": summary["modes"]["shadow"].get("shadow_fail_by_reason"),
        }
        write_shadow_summary_json(shadow_path, shadow_payload)
    if args.verbose:
        msg = f"[done] trades={trades_path} summary={summary_path} log={log_path}"
        if shadow_payload:
            msg = f"{msg} shadow={shadow_path}"
        print(msg)
    if stats_by_key:
        trades_by_key: Dict[tuple[str, str], List[Dict]] = {}
        for t in trades:
            key = (t.get("mode") or "", t.get("sym") or "")
            trades_by_key.setdefault(key, []).append(t)
        total_by_mode: Dict[str, Dict[str, float]] = {}
        multi_mode = len({k[0] for k in stats_by_key.keys()}) > 1
        for (mode, sym), stats in stats_by_key.items():
            trades_count = int(stats.get("trades") or 0)
            entries = int(stats.get("entries") or 0)
            exits = int(stats.get("exits") or 0)
            wins = int(stats.get("wins") or 0)
            losses = int(stats.get("losses") or 0)
            tp = int(stats.get("tp") or 0)
            sl = int(stats.get("sl") or 0)
            win_rate = (wins / trades_count * 100.0) if trades_count else 0.0
            avg_mfe = (stats.get("mfe_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_mae = (stats.get("mae_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_hold = (stats.get("hold_sum", 0.0) / trades_count) if trades_count else 0.0
            sym_trades = trades_by_key.get((mode, sym), [])
            long_wins = sum(1 for t in sym_trades if t.get("side") == "LONG" and float(t.get("pnl_pct") or 0.0) > 0)
            long_losses = sum(1 for t in sym_trades if t.get("side") == "LONG" and float(t.get("pnl_pct") or 0.0) <= 0)
            short_wins = sum(1 for t in sym_trades if t.get("side") == "SHORT" and float(t.get("pnl_pct") or 0.0) > 0)
            short_losses = sum(1 for t in sym_trades if t.get("side") == "SHORT" and float(t.get("pnl_pct") or 0.0) <= 0)
            label = f"{sym}@{mode}" if multi_mode else sym
            last_day_exits = last_day_exits_by_mode.get(mode, 0)
            base_usdt = float(bt_cfg.base_usdt or 0.0)
            tp_sum_usdt = tp * base_usdt * float(bt_cfg.tp_pct or 0.0)
            sl_sum_usdt = sl * base_usdt * float(bt_cfg.sl_pct or 0.0)
            net_sum_usdt = tp_sum_usdt - sl_sum_usdt
            entry_sym_count = len(entry_syms_by_mode.get(mode, set()))
            print(
                "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f last_day_exits=%d "
                "base_usdt=%.2f tp_sum=%.3f sl_sum=%.3f net_sum=%.3f entry_syms=%d"
                % (
                    label,
                    entries,
                    exits,
                    trades_count,
                    wins,
                    losses,
                    win_rate,
                    tp,
                    sl,
                    avg_mfe,
                    avg_mae,
                    avg_hold,
                    last_day_exits,
                    base_usdt,
                    tp_sum_usdt,
                    sl_sum_usdt,
                    net_sum_usdt,
                    entry_sym_count,
                )
            )
            entries_list = list(trade_logs.get((mode, sym), []))
            open_trade = open_trades.get((mode, sym))
            if isinstance(open_trade, dict):
                entry_dt = ""
                entry_ts = open_trade.get("entry_ts")
                if isinstance(entry_ts, (int, float)) and entry_ts > 0:
                    entry_dt = datetime.fromtimestamp(entry_ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                entry_px = open_trade.get("entry_price")
                side = (open_trade.get("side") or "").upper()
                last_px = last_close_by_sym.get(sym)
                last_ts = last_ts_by_sym.get(sym)
                last_dt = ""
                if isinstance(last_ts, (int, float)) and last_ts > 0:
                    last_dt = datetime.fromtimestamp(float(last_ts) / 1000.0, tz=timezone.utc).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                unrealized = None
                if isinstance(entry_px, (int, float)) and isinstance(last_px, (int, float)) and entry_px > 0:
                    if side == "SHORT":
                        unrealized = (float(entry_px) - float(last_px)) / float(entry_px) * 100.0
                    else:
                        unrealized = (float(last_px) - float(entry_px)) / float(entry_px) * 100.0
                last_disp = f"{float(last_px):.6g}" if isinstance(last_px, (int, float)) else "N/A"
                pnl_disp = f"{float(unrealized):.2f}%" if isinstance(unrealized, (int, float)) else "N/A"
                entries_list.append(
                    {
                        "entry_ts": entry_ts or 0,
                        "line": "[BACKTEST][OPEN] sym=%s mode=%s side=%s entry_dt=%s exit_dt=%s entry_px=%.6g "
                        "last_px=%s last_dt=%s unrealized_pct=%s"
                        % (
                            open_trade.get("sym"),
                            open_trade.get("mode"),
                            open_trade.get("side"),
                            entry_dt,
                            "",
                            float(entry_px or 0.0),
                            last_disp,
                            last_dt or "N/A",
                            pnl_disp,
                        ),
                    }
                )
            entries_list.sort(key=lambda item: item.get("entry_ts") or 0, reverse=True)
            for entry in entries_list:
                print(entry.get("line", ""))
            mode_total = total_by_mode.setdefault(
                mode,
                {
                    "entries": 0,
                    "exits": 0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "tp": 0,
                    "sl": 0,
                    "mfe_sum": 0.0,
                    "mae_sum": 0.0,
                    "hold_sum": 0.0,
                    "long_wins": 0,
                    "long_losses": 0,
                    "short_wins": 0,
                    "short_losses": 0,
                },
            )
            for key in ("trades", "wins", "losses", "tp", "sl", "entries", "exits"):
                mode_total[key] += int(stats.get(key) or 0)
            mode_total["mfe_sum"] += float(stats.get("mfe_sum") or 0.0)
            mode_total["mae_sum"] += float(stats.get("mae_sum") or 0.0)
            mode_total["hold_sum"] += float(stats.get("hold_sum") or 0.0)
            mode_total["long_wins"] += long_wins
            mode_total["long_losses"] += long_losses
            mode_total["short_wins"] += short_wins
            mode_total["short_losses"] += short_losses
        grand_total = {
            "entries": 0,
            "exits": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "tp": 0,
            "sl": 0,
            "mfe_sum": 0.0,
            "mae_sum": 0.0,
            "hold_sum": 0.0,
            "long_wins": 0,
            "long_losses": 0,
            "short_wins": 0,
            "short_losses": 0,
        }
        for mode, stats in total_by_mode.items():
            trades_count = int(stats.get("trades") or 0)
            entries = int(stats.get("entries") or 0)
            exits = int(stats.get("exits") or 0)
            wins = int(stats.get("wins") or 0)
            losses = int(stats.get("losses") or 0)
            tp = int(stats.get("tp") or 0)
            sl = int(stats.get("sl") or 0)
            win_rate = (wins / trades_count * 100.0) if trades_count else 0.0
            avg_mfe = (stats.get("mfe_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_mae = (stats.get("mae_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_hold = (stats.get("hold_sum", 0.0) / trades_count) if trades_count else 0.0
            label = f"TOTAL@{mode}" if multi_mode else "TOTAL"
            last_day_exits = last_day_exits_by_mode.get(mode, 0)
            base_usdt = float(bt_cfg.base_usdt or 0.0)
            tp_sum_usdt = tp * base_usdt * float(bt_cfg.tp_pct or 0.0)
            sl_sum_usdt = sl * base_usdt * float(bt_cfg.sl_pct or 0.0)
            net_sum_usdt = tp_sum_usdt - sl_sum_usdt
            entry_sym_count = len(entry_syms_by_mode.get(mode, set()))
            print(
                "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f last_day_exits=%d "
                "base_usdt=%.2f tp_sum=%.3f sl_sum=%.3f net_sum=%.3f entry_syms=%d"
                % (
                    label,
                    entries,
                    exits,
                    trades_count,
                    wins,
                    losses,
                    win_rate,
                    tp,
                    sl,
                    avg_mfe,
                    avg_mae,
                    avg_hold,
                    last_day_exits,
                    base_usdt,
                    tp_sum_usdt,
                    sl_sum_usdt,
                    net_sum_usdt,
                    entry_sym_count,
                )
            )
            for key in ("trades", "wins", "losses", "tp", "sl", "entries", "exits"):
                grand_total[key] += int(stats.get(key) or 0)
            grand_total["mfe_sum"] += float(stats.get("mfe_sum") or 0.0)
            grand_total["mae_sum"] += float(stats.get("mae_sum") or 0.0)
            grand_total["hold_sum"] += float(stats.get("hold_sum") or 0.0)
            for key in ("long_wins", "long_losses", "short_wins", "short_losses"):
                grand_total[key] += int(stats.get(key) or 0)
        total_trades = int(grand_total.get("trades") or 0)
        total_entries = int(grand_total.get("entries") or 0)
        total_exits = int(grand_total.get("exits") or 0)
        total_wins = int(grand_total.get("wins") or 0)
        total_losses = int(grand_total.get("losses") or 0)
        total_tp = int(grand_total.get("tp") or 0)
        total_sl = int(grand_total.get("sl") or 0)
        total_win_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
        total_avg_mfe = (grand_total.get("mfe_sum", 0.0) / total_trades) if total_trades else 0.0
        total_avg_mae = (grand_total.get("mae_sum", 0.0) / total_trades) if total_trades else 0.0
        total_avg_hold = (grand_total.get("hold_sum", 0.0) / total_trades) if total_trades else 0.0
        if multi_mode:
            base_usdt = float(bt_cfg.base_usdt or 0.0)
            total_tp_sum = total_tp * base_usdt * float(bt_cfg.tp_pct or 0.0)
            total_sl_sum = total_sl * base_usdt * float(bt_cfg.sl_pct or 0.0)
            total_net_sum = total_tp_sum - total_sl_sum
            total_entry_syms = len(set().union(*entry_syms_by_mode.values())) if entry_syms_by_mode else 0
            print(
                "[BACKTEST] TOTAL entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f last_day_exits=%d "
                "base_usdt=%.2f tp_sum=%.3f sl_sum=%.3f net_sum=%.3f entry_syms=%d"
                % (
                    total_entries,
                    total_exits,
                    total_trades,
                    total_wins,
                    total_losses,
                    total_win_rate,
                    total_tp,
                    total_sl,
                    total_avg_mfe,
                    total_avg_mae,
                    total_avg_hold,
                    sum(last_day_exits_by_mode.values()),
                    base_usdt,
                    total_tp_sum,
                    total_sl_sum,
                    total_net_sum,
                    total_entry_syms,
                )
            )
        if d1_block_by_key:
            by_mode: Dict[str, int] = {}
            total_blocks = 0
            for (mode, sym), count in sorted(d1_block_by_key.items()):
                line = f"[BACKTEST] {sym}@{mode} block=D1_EMA7_DIST count={count}"
                print(line)
                _append_backtest_log(line)
                by_mode[mode] = by_mode.get(mode, 0) + count
                total_blocks += count
            for mode, count in sorted(by_mode.items()):
                line = f"[BACKTEST] TOTAL@{mode} block=D1_EMA7_DIST count={count}"
                print(line)
                _append_backtest_log(line)
            line = f"[BACKTEST] TOTAL block=D1_EMA7_DIST count={total_blocks}"
            print(line)
            _append_backtest_log(line)
    else:
        base_usdt = float(bt_cfg.base_usdt or 0.0)
        print(
            "[BACKTEST] TOTAL entries=0 exits=0 trades=0 wins=0 losses=0 winrate=0.00%% tp=0 sl=0 "
            "avg_mfe=0.0000 avg_mae=0.0000 avg_hold=0.0 last_day_exits=0 "
            "base_usdt=%.2f tp_sum=0.000 sl_sum=0.000 net_sum=0.000 entry_syms=0"
            % base_usdt
        )


if __name__ == "__main__":
    main()
