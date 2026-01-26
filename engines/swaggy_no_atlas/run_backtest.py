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

from engines.swaggy_atlas_lab.broker_sim import BrokerSim
from engines.swaggy_atlas_lab.config import BacktestConfig, SwaggyConfig
from engines.dtfx.engine import DTFXConfig
from engines.swaggy_atlas_lab.data import (
    ensure_dir,
    fetch_ohlcv_all,
    slice_df,
    to_df,
    save_universe,
)
from engines.universe import build_universe_from_tickers
from engines.swaggy_atlas_lab.report import (
    build_summary,
    write_shadow_summary_json,
    write_summary_json,
    write_trades_csv,
)
from engines.swaggy_atlas_lab.indicators import atr, ema
from engines.swaggy_atlas_lab.swaggy_signal import SwaggySignalEngine

MAX_HOLD_BARS = 240
OVEREXT_ENTRY_MIN = -0.70


def _append_backtest_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_no_atlas", "backtest", f"backtest_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = datetime.fromtimestamp(time.time(), tz=timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _append_backtest_entry_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_no_atlas", "backtest", f"backtest_entries_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = datetime.fromtimestamp(time.time(), tz=timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance(
        {
            "apiKey": os.getenv("BACKTEST_BINANCE_API_KEY", ""),
            "secret": os.getenv("BACKTEST_BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Swaggy No Atlas backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--symbols", default="", help="comma-separated symbols")
    parser.add_argument("--symbols-file", default="", help="path to fixed symbol list")
    parser.add_argument("--universe", default="top50", help="topN (e.g. top50) or 'symbols'")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--anchor", default="BTC,ETH")
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--sl-pct", type=float, default=0.0)
    parser.add_argument("--base-usdt", type=float, default=10.0)
    parser.add_argument("--dca", choices=["on", "off"], default="off")
    parser.add_argument("--dca-thresholds", default="20,30,40", help="adverse move % thresholds, e.g. 20,30,40")
    parser.add_argument("--fee", type=float, default=0.0)
    parser.add_argument("--slippage", type=float, default=0.0)
    parser.add_argument("--timeout-bars", type=int, default=0)
    parser.add_argument("--cooldown-min", type=int, default=0)
    parser.add_argument("--overext-min", type=float, default=None)
    parser.add_argument("--d1-overext-atr", type=float, default=None)
    parser.add_argument("--last-day-entry", choices=["on", "off"], default="on")
    parser.add_argument("--last-day-entry-days", type=int, default=1)
    parser.add_argument("--entry-windows", default="", help="off windows (HH[:MM]-HH[:MM], comma-separated, UTC by default)")
    parser.add_argument("--entry-tz-offset", type=float, default=0.0, help="hours offset from UTC for entry windows (e.g. 9 for KST)")
    parser.add_argument("--sat-trade", choices=["on", "off"], default="on", help="allow entries on Saturday (KST)")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _load_runtime_overrides() -> dict:
    path = os.path.join(ROOT_DIR, "state.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _coerce_float(val) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val))
    except Exception:
        return None


def _parse_universe_arg(text: str) -> int:
    if text.lower().startswith("top"):
        try:
            return int(text.lower().replace("top", ""))
        except Exception:
            return 50
    return 0


def _parse_entry_windows(text: str) -> List[tuple[int, int]]:
    windows: List[tuple[int, int]] = []
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" not in part:
            continue
        start_raw, end_raw = part.split("-", 1)
        start_raw = start_raw.strip()
        end_raw = end_raw.strip()
        try:
            if ":" in start_raw:
                sh, sm = start_raw.split(":", 1)
            else:
                sh, sm = start_raw, "0"
            if ":" in end_raw:
                eh, em = end_raw.split(":", 1)
            else:
                eh, em = end_raw, "0"
            sh_i = int(sh)
            sm_i = int(sm)
            eh_i = int(eh)
            em_i = int(em)
        except Exception:
            continue
        if not (0 <= sh_i <= 23 and 0 <= eh_i <= 23 and 0 <= sm_i <= 59 and 0 <= em_i <= 59):
            continue
        windows.append((sh_i * 60 + sm_i, eh_i * 60 + em_i))
    return windows


def _in_entry_window(ts_ms: int, windows: List[tuple[int, int]], tz_offset_hours: float) -> bool:
    if not windows:
        return False
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    except Exception:
        return True
    if tz_offset_hours:
        dt = dt + timedelta(hours=tz_offset_hours)
    cur_min = dt.hour * 60 + dt.minute
    for start_min, end_min in windows:
        if start_min == end_min:
            return False
        if start_min < end_min:
            if start_min <= cur_min < end_min:
                return True
        else:
            if cur_min >= start_min or cur_min < end_min:
                return True
    return False


def _fmt_kst_ms(ts_ms: Optional[float]) -> str:
    if not isinstance(ts_ms, (int, float)) or ts_ms <= 0:
        return ""
    try:
        tz = timezone(timedelta(hours=9))
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _kst_dt_from_ms(ts_ms: Optional[float]) -> Optional[datetime]:
    if not isinstance(ts_ms, (int, float)) or ts_ms <= 0:
        return None
    try:
        tz = timezone(timedelta(hours=9))
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=tz)
    except Exception:
        return None


def _is_saturday_kst_ms(ts_ms: int) -> bool:
    try:
        tz = timezone(timedelta(hours=9))
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=tz)
        return dt.weekday() == 5
    except Exception:
        return False


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


def _d1_dist_atr(df, last_price: float, cfg: SwaggyConfig) -> Optional[float]:
    if df.empty or len(df) < max(cfg.d1_ema_len, cfg.d1_atr_len) + 2:
        return None
    ema_series = ema(df["close"], cfg.d1_ema_len)
    if ema_series.empty:
        return None
    ema_val = float(ema_series.iloc[-1])
    atr_val = atr(df, cfg.d1_atr_len)
    if atr_val <= 0:
        return None
    return abs(last_price - ema_val) / atr_val


def main() -> None:
    args = parse_args()
    runtime_overrides = _load_runtime_overrides()
    runtime_overext_min = _coerce_float(runtime_overrides.get("_swaggy_no_atlas_overext_min"))
    runtime_d1_overext = _coerce_float(runtime_overrides.get("_swaggy_d1_overext_atr_mult"))
    if args.overext_min is None and runtime_overext_min is not None:
        args.overext_min = float(runtime_overext_min)
    if args.overext_min is None:
        args.overext_min = OVEREXT_ENTRY_MIN
    if args.d1_overext_atr is None and runtime_d1_overext is not None:
        args.d1_overext_atr = float(runtime_d1_overext)
    if args.sl_pct <= 0:
        raise SystemExit("--sl-pct is required")
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(args.days) * 24 * 60 * 60 * 1000
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    run_id = time.strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join("logs", "swaggy_no_atlas", "backtest")
    ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"bt_{run_id}.log")
    trades_path = os.path.join(log_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(log_dir, f"summary_{run_id}.json")
    shadow_path = os.path.join(log_dir, f"shadow_{run_id}.json")
    universe_path = os.path.join(log_dir, f"universe_{run_id}.json")

    bt_cfg = BacktestConfig(
        tp_pct=args.tp_pct,
        sl_pct=args.sl_pct,
        fee_rate=args.fee,
        slippage_pct=args.slippage,
        timeout_bars=args.timeout_bars,
        base_usdt=args.base_usdt,
    )
    sw_cfg = SwaggyConfig()
    if isinstance(args.cooldown_min, int) and args.cooldown_min > 0:
        sw_cfg.cooldown_min = int(args.cooldown_min)
    if isinstance(args.d1_overext_atr, (int, float)):
        sw_cfg.d1_overext_atr_mult = float(args.d1_overext_atr)

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
    exit_pnl_by_reason: Dict[tuple[str, str], float] = {}
    for sym in symbols:
        tf_map: Dict[str, object] = {}
        for tf in tfs:
            rows = fetch_ohlcv_all(ex, sym, tf, start_ms, end_ms)
            tf_map[tf] = to_df(rows)
        data_by_sym[sym] = tf_map

    mode_name = "swaggy_no_atlas"
    trades: List[Dict] = []
    stats_by_key: Dict[tuple[str, str], Dict[str, float]] = {}
    trade_logs: Dict[tuple[str, str], List[dict]] = {}
    open_trades: Dict[tuple[str, str, str], Dict[str, object]] = {}
    overext_by_key: Dict[tuple[str, str], Dict[str, int]] = {}
    d1_block_by_key: Dict[tuple[str, str], int] = {}
    entry_syms_by_mode: Dict[str, set] = {}
    dca_adds_by_key: Dict[tuple[str, str], int] = {}
    dca_usdt_by_key: Dict[tuple[str, str], float] = {}
    last_close_by_sym: Dict[str, float] = {}
    last_ts_by_sym: Dict[str, int] = {}
    last_day_entry_days = int(args.last_day_entry_days or 1)
    if last_day_entry_days < 1:
        last_day_entry_days = 1
    last_day_start_ms = end_ms - last_day_entry_days * 24 * 60 * 60 * 1000
    last_day_exits_by_mode: Dict[str, int] = {}
    last_day_entry = str(args.last_day_entry or "on").lower()
    entry_windows = _parse_entry_windows(args.entry_windows)
    entry_tz_offset = float(args.entry_tz_offset or 0.0)
    sat_trade_enabled = str(args.sat_trade or "on").lower() == "on"
    dca_enabled = str(args.dca or "off").lower() == "on"
    dca_thresholds: List[float] = []
    for part in str(args.dca_thresholds or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            dca_thresholds.append(float(part))
        except Exception:
            pass
    if not dca_thresholds:
        dca_thresholds = [20.0, 30.0, 40.0]
    dca_max_adds = len(dca_thresholds)

    ensure_dir(os.path.dirname(log_path))
    with open(log_path, "a", encoding="utf-8") as log_fp:
        run_line = (
            f"[run] mode={mode_name} days={args.days} start_ms={start_ms} end_ms={end_ms} "
            f"overext_min={args.overext_min} d1_overext_atr={sw_cfg.d1_overext_atr_mult} "
            f"off_windows={args.entry_windows or 'NONE'} entry_tz_offset={entry_tz_offset:.2f}"
        )
        log_fp.write(run_line + "\n")
        _append_backtest_log(run_line)
        engine = SwaggySignalEngine(sw_cfg)
        broker = BrokerSim(
            bt_cfg.tp_pct,
            bt_cfg.sl_pct,
            bt_cfg.fee_rate,
            bt_cfg.slippage_pct,
            bt_cfg.timeout_bars,
            hedge_mode=True,
        )
        for sym in symbols:
            sym_state = engine._state.setdefault(sym, {})
            df_ltf = data_by_sym[sym][sw_cfg.tf_ltf]
            df_mtf = data_by_sym[sym][sw_cfg.tf_mtf]
            df_htf = data_by_sym[sym][sw_cfg.tf_htf]
            df_htf2 = data_by_sym[sym][sw_cfg.tf_htf2]
            df_d1 = data_by_sym[sym][sw_cfg.tf_d1]
            if not df_ltf.empty:
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
                new_phase = sym_state.get("phase")
                key = (mode_name, sym)
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
                    d1_key = (mode_name, sym)
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

                had_long = broker.has_position(sym, "LONG")
                had_short = broker.has_position(sym, "SHORT")
                for pos_side in ("LONG", "SHORT"):
                    if not broker.has_position(sym, pos_side):
                        continue
                    trade = broker.on_bar(
                        sym,
                        ts_ms,
                        float(cur["high"]),
                        float(cur["low"]),
                        float(cur["close"]),
                        i,
                        side=pos_side,
                    )
                    if trade and trade.exit_reason == "TIME":
                        trade.exit_reason = "TIMEOUT"
                    if trade:
                        pnl_pct = broker.calc_pnl_pct(trade)
                        pnl_usdt = pnl_pct * trade.size_usdt
                        fee = trade.size_usdt * bt_cfg.fee_rate * 2
                        trades.append(
                            {
                                "run_id": run_id,
                                "mode": mode_name,
                                "sym": sym,
                                "side": trade.side,
                                "entry_ts": trade.entry_ts,
                                "entry_price": trade.entry_price,
                                "exit_ts": trade.exit_ts,
                                "exit_price": trade.exit_price,
                                "exit_reason": trade.exit_reason,
                                "pnl_usdt": pnl_usdt,
                                "pnl_pct": pnl_pct,
                                "dca_adds": trade.dca_adds,
                                "dca_usdt": trade.dca_usdt,
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
                        if isinstance(trade.exit_ts, (int, float)) and trade.exit_ts >= last_day_start_ms:
                            last_day_exits_by_mode[mode_name] = last_day_exits_by_mode.get(mode_name, 0) + 1
                        log_fp.write(
                            f"EXIT ts={trade.exit_ts} sym={sym} pnl_usdt={pnl_usdt:.4f} "
                            f"pnl_pct={pnl_pct:.4f} reason={trade.exit_reason} duration_bars={trade.bars} "
                            f"dca_adds={int(trade.dca_adds or 0)} dca_usdt={float(trade.dca_usdt or 0.0):.2f}\n"
                        )
                        _append_backtest_log(
                            f"EXIT ts={trade.exit_ts} sym={sym} pnl_usdt={pnl_usdt:.4f} "
                            f"pnl_pct={pnl_pct:.4f} reason={trade.exit_reason} duration_bars={trade.bars} "
                            f"dca_adds={int(trade.dca_adds or 0)} dca_usdt={float(trade.dca_usdt or 0.0):.2f}"
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
                                "loss_pnl_sum": 0.0,
                                "dca_adds": 0,
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
                        stats.setdefault("loss_pnl_sum", 0.0)
                        if trade.exit_reason == "TP":
                            stats["tp"] += 1
                        elif trade.exit_reason == "SL":
                            stats["sl"] += 1
                        if pnl_usdt < 0:
                            stats["loss_pnl_sum"] += float(pnl_usdt)
                        reason_key = (mode_name, trade.exit_reason or "UNKNOWN")
                        exit_pnl_by_reason[reason_key] = exit_pnl_by_reason.get(reason_key, 0.0) + float(pnl_usdt)
                        stats["dca_adds"] += int(trade.dca_adds or 0)
                        stats["mfe_sum"] += trade.mfe
                        stats["mae_sum"] += abs(trade.mae)
                        stats["hold_sum"] += float(trade.bars) * ltf_minutes
                        entry_dt = _fmt_kst_ms(trade.entry_ts)
                        exit_dt = _fmt_kst_ms(trade.exit_ts)
                        trade_logs.setdefault(key, []).append(
                            {
                                "entry_ts": trade.entry_ts or 0,
                                "line": "[BACKTEST][EXIT] sym=%s mode=%s side=%s entry_dt=%s exit_dt=%s "
                                "entry_px=%.6g exit_px=%.6g reason=%s dca_adds=%d dca_usdt=%.2f"
                                % (
                                    sym,
                                    mode_name,
                                    trade.side,
                                    entry_dt,
                                    exit_dt,
                                    trade.entry_price,
                                    trade.exit_price or 0.0,
                                    trade.exit_reason,
                                    int(trade.dca_adds or 0),
                                    float(trade.dca_usdt or 0.0),
                                ),
                            }
                        )
                        continue
                    # DCA (Swaggy No Atlas): apply adverse move thresholds (configurable).
                    if dca_enabled:
                        trade_live = broker.get_position(sym, pos_side)
                        if trade_live:
                            adds_done = int(trade_live.dca_adds or 0)
                            if adds_done < dca_max_adds:
                                entry_ref = float(trade_live.entry_price or 0.0)
                                close_px = float(cur["close"])
                                adverse_pct = 0.0
                                if entry_ref > 0:
                                    if trade_live.side == "LONG":
                                        adverse_pct = (entry_ref - close_px) / entry_ref * 100.0
                                    else:
                                        adverse_pct = (close_px - entry_ref) / entry_ref * 100.0
                                if adverse_pct >= dca_thresholds[adds_done]:
                                    dca_usdt = float(bt_cfg.base_usdt or 0.0)
                                    if dca_usdt > 0:
                                        broker.add_position(
                                            sym,
                                            close_px,
                                    dca_usdt,
                                    float(cur["high"]),
                                    float(cur["low"]),
                                    side=trade_live.side,
                                    mode="dca",
                                )
                                        log_fp.write(
                                            "DCA ts=%d sym=%s side=%s adds=%d->%d entry=%.6g mark=%.6g usdt=%.2f\n"
                                            % (
                                                ts_ms,
                                                sym,
                                                trade_live.side,
                                                adds_done,
                                                adds_done + 1,
                                                entry_ref,
                                                close_px,
                                                dca_usdt,
                                            )
                                        )
                                    _append_backtest_log(
                                        "DCA ts=%d sym=%s side=%s adds=%d->%d entry=%.6g mark=%.6g usdt=%.2f"
                                        % (
                                            ts_ms,
                                            sym,
                                            trade_live.side,
                                            adds_done,
                                            adds_done + 1,
                                            entry_ref,
                                            close_px,
                                            dca_usdt,
                                        )
                                    )
                                    dca_adds_by_key[key] = dca_adds_by_key.get(key, 0) + 1
                                    dca_usdt_by_key[key] = dca_usdt_by_key.get(key, 0.0) + float(dca_usdt)

                if not signal.entry_ok or not side or entry_px is None:
                    continue
                if last_day_entry == "off" and ts_ms >= last_day_start_ms:
                    continue
                if not sat_trade_enabled and _is_saturday_kst_ms(ts_ms):
                    _append_backtest_log(
                        f"ENTRY_SKIP ts={ts_ms} sym={sym} side={side} reason=SATURDAY_OFF"
                    )
                    continue
                if _in_entry_window(ts_ms, entry_windows, entry_tz_offset):
                    _append_backtest_log(
                        f"ENTRY_SKIP ts={ts_ms} sym={sym} side={side} reason=OFF_WINDOW"
                    )
                    continue
                if (side == "LONG" and had_long) or (side == "SHORT" and had_short):
                    continue

                dist_at_entry = _overext_dist(d5, side, sw_cfg)
                if isinstance(dist_at_entry, (int, float)) and dist_at_entry < float(args.overext_min):
                    _append_backtest_log(
                        f"ENTRY_SKIP ts={ts_ms} sym={sym} side={side} reason=OVEREXT_DEEP dist={dist_at_entry:.4f}"
                    )
                    continue

                d1_dist = _d1_dist_atr(d1d, float(entry_px), sw_cfg) if isinstance(entry_px, (int, float)) else None
                entry_line = (
                    "ENTRY ts=%d sym=%s side=%s mode=%s sw_ok=%s sw_strength=%.3f sw_reasons=%s "
                    "base_usdt=%.2f final_usdt=%.2f policy_action=%s d1_dist_atr=%s"
                    % (
                        ts_ms,
                        sym,
                        side,
                        mode_name,
                        "Y" if signal.entry_ok else "N",
                        signal.strength,
                        signal.reasons,
                        bt_cfg.base_usdt,
                        bt_cfg.base_usdt,
                        "NO_ATLAS",
                        "N/A" if d1_dist is None else f"{d1_dist:.4f}",
                    )
                )
                log_fp.write(entry_line + "\n")
                _append_backtest_log(entry_line)
                overext_blocked = bool(sym_state.get("overext_blocked"))
                sym_state["overext_blocked"] = False
                _append_backtest_entry_log(
                    "engine=swaggy_no_atlas mode=%s symbol=%s side=%s entry=%.6g "
                    "final_usdt=%.2f sw_strength=%.3f sw_reasons=%s policy_action=%s d1_dist_atr=%s"
                    % (
                        mode_name,
                        sym,
                        side,
                        float(entry_px or 0.0),
                        float(bt_cfg.base_usdt or 0.0),
                        float(signal.strength or 0.0),
                        ",".join(signal.reasons or []),
                        "NO_ATLAS",
                        "N/A" if d1_dist is None else f"{d1_dist:.4f}",
                    )
                )
                broker.enter(
                    sym,
                    side,
                    ts_ms,
                    i,
                    entry_px,
                    bt_cfg.base_usdt,
                    sw_strength=signal.strength,
                    sw_reasons=signal.reasons,
                    policy_action="NO_ATLAS",
                    overext_dist_at_entry=dist_at_entry,
                    overext_blocked=overext_blocked,
                )
                entry_syms_by_mode.setdefault(mode_name, set()).add(sym)
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
                        "dca_adds": 0,
                        "mfe_sum": 0.0,
                        "mae_sum": 0.0,
                        "hold_sum": 0.0,
                    },
                )
                stats["entries"] += 1
                # ENTRY summary is represented by OPEN/EXIT logs (avoid duplicate lines).

    for pos_key, trade in broker.positions.items():
        sym = trade.sym
        open_trades[(mode_name, sym, trade.side)] = {
            "sym": sym,
            "mode": mode_name,
            "side": trade.side,
            "entry_price": trade.entry_price,
            "entry_ts": trade.entry_ts,
            "dca_adds": trade.dca_adds,
            "dca_usdt": trade.dca_usdt,
            "size_usdt": trade.size_usdt,
        }

    write_trades_csv(trades_path, trades)
    summary = build_summary(run_id, trades)
    open_stats_by_mode: Dict[str, Dict[str, float]] = {}
    for key_tuple, rec in open_trades.items():
        mode = key_tuple[0]
        sym = key_tuple[1]
        entry_px = rec.get("entry_price")
        size_usdt = rec.get("size_usdt")
        last_close = last_close_by_sym.get(sym)
        if not isinstance(entry_px, (int, float)) or not isinstance(size_usdt, (int, float)):
            continue
        if not isinstance(last_close, (int, float)) or last_close <= 0:
            continue
        side = (rec.get("side") or "").upper()
        if side == "SHORT":
            pnl_pct = (float(entry_px) - float(last_close)) / float(entry_px)
        else:
            pnl_pct = (float(last_close) - float(entry_px)) / float(entry_px)
        pnl_usdt = float(size_usdt) * pnl_pct
        bucket = open_stats_by_mode.setdefault(mode, {"count": 0.0, "notional": 0.0, "pnl": 0.0})
        bucket["count"] += 1.0
        bucket["notional"] += float(size_usdt)
        bucket["pnl"] += float(pnl_usdt)
    if open_stats_by_mode:
        summary["open_positions"] = open_stats_by_mode
    if exit_pnl_by_reason:
        per_mode_exit = {}
        for (mode, reason), pnl in exit_pnl_by_reason.items():
            bucket = per_mode_exit.setdefault(mode, {})
            bucket[reason] = float(pnl)
        summary["exit_pnl_by_reason"] = per_mode_exit
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
        total_lines: List[str] = []
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
            print(
                "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f dca_adds=%d dca_usdt=%.2f"
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
                    int(stats.get("dca_adds") or dca_adds_by_key.get((mode, sym), 0)),
                    float(dca_usdt_by_key.get((mode, sym), 0.0)),
                )
            )
            entries_list = list(trade_logs.get((mode, sym), []))
            open_trade_items = [
                rec for (m_key, s_key, _side), rec in open_trades.items()
                if m_key == mode and s_key == sym
            ]
            for open_trade in open_trade_items:
                if not isinstance(open_trade, dict):
                    continue
                entry_ts = open_trade.get("entry_ts")
                entry_dt = _fmt_kst_ms(entry_ts)
                entry_px = open_trade.get("entry_price")
                side = (open_trade.get("side") or "").upper()
                last_px = last_close_by_sym.get(sym)
                last_ts = last_ts_by_sym.get(sym)
                last_dt = _fmt_kst_ms(last_ts)
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
                        "last_px=%s last_dt=%s unrealized_pct=%s dca_adds=%s dca_usdt=%s"
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
                            open_trade.get("dca_adds"),
                            open_trade.get("dca_usdt"),
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
                    "dca_adds": 0,
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
            mode_total["dca_adds"] += int(stats.get("dca_adds") or 0)
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
            "loss_pnl_sum": 0.0,
            "dca_adds": 0,
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
            open_bucket = open_stats_by_mode.get(mode) or {}
            open_count = int(open_bucket.get("count") or 0)
            open_notional = float(open_bucket.get("notional") or 0.0)
            open_pnl = float(open_bucket.get("pnl") or 0.0)
            reason_order = ["TP", "SL", "UNKNOWN"]
            reason_items = []
            for reason in reason_order:
                key = (mode, reason)
                if key in exit_pnl_by_reason:
                    reason_items.append(f"{reason}={exit_pnl_by_reason.get(key, 0.0):.3f}")
            for (m_key, reason), pnl in sorted(exit_pnl_by_reason.items()):
                if m_key != mode or reason in reason_order:
                    continue
                reason_items.append(f"{reason}={pnl:.3f}")
            reason_text = " ".join(reason_items) if reason_items else "none"
            total_lines.append(
                "[BACKTEST] %s entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f last_day_exits=%d "
                "base_usdt=%.2f tp_sum=%.3f sl_sum=%.3f net_sum=%.3f entry_syms=%d "
                "open_count=%d open_notional=%.2f open_pnl=%.3f exit_pnl={%s}"
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
                    open_count,
                    open_notional,
                    open_pnl,
                    reason_text,
                )
            )
            for key in ("trades", "wins", "losses", "tp", "sl", "entries", "exits"):
                grand_total[key] += int(stats.get(key) or 0)
            grand_total["loss_pnl_sum"] += float(stats.get("loss_pnl_sum") or 0.0)
            grand_total["mfe_sum"] += float(stats.get("mfe_sum") or 0.0)
            grand_total["mae_sum"] += float(stats.get("mae_sum") or 0.0)
            grand_total["hold_sum"] += float(stats.get("hold_sum") or 0.0)
            grand_total["dca_adds"] += int(stats.get("dca_adds") or 0)
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
            total_open_count = sum(int(v.get("count") or 0) for v in open_stats_by_mode.values())
            total_open_notional = sum(float(v.get("notional") or 0.0) for v in open_stats_by_mode.values())
            total_open_pnl = sum(float(v.get("pnl") or 0.0) for v in open_stats_by_mode.values())
            total_reason_items = []
            reason_set = {r for (_m, r) in exit_pnl_by_reason.keys()}
            for reason in ["TP", "SL", "UNKNOWN"]:
                if reason not in reason_set:
                    continue
                pnl_sum = sum(
                    float(val)
                    for (m_key, r_key), val in exit_pnl_by_reason.items()
                    if r_key == reason
                )
                total_reason_items.append(f"{reason}={pnl_sum:.3f}")
            for reason in sorted(r for r in reason_set if r not in ("TP", "SL", "UNKNOWN")):
                pnl_sum = sum(
                    float(val)
                    for (m_key, r_key), val in exit_pnl_by_reason.items()
                    if r_key == reason
                )
                total_reason_items.append(f"{reason}={pnl_sum:.3f}")
            total_reason_text = " ".join(total_reason_items) if total_reason_items else "none"
            total_lines.append(
                "[BACKTEST] TOTAL entries=%d exits=%d trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f last_day_exits=%d "
                "base_usdt=%.2f tp_sum=%.3f sl_sum=%.3f net_sum=%.3f entry_syms=%d "
                "open_count=%d open_notional=%.2f open_pnl=%.3f exit_pnl={%s}"
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
                    total_open_count,
                    total_open_notional,
                    total_open_pnl,
                    total_reason_text,
                )
            )
        if args.verbose and d1_block_by_key:
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
    if stats_by_key:
        for line in total_lines:
            print(line)
        # KST time buckets (entries based on entry_ts)
        hour_stats: Dict[int, Dict[str, int]] = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
        dow_stats: Dict[int, Dict[str, int]] = {d: {"entries": 0, "tp": 0, "sl": 0} for d in range(7)}
        for tr in trades:
            entry_ts = tr.get("entry_ts")
            dt_kst = _kst_dt_from_ms(entry_ts)
            if dt_kst is None:
                continue
            hour = int(dt_kst.hour)
            dow = int(dt_kst.weekday())
            hour_stats[hour]["entries"] += 1
            dow_stats[dow]["entries"] += 1
            reason = (tr.get("exit_reason") or "").upper()
            if reason == "TP":
                hour_stats[hour]["tp"] += 1
                dow_stats[dow]["tp"] += 1
            elif reason == "SL":
                hour_stats[hour]["sl"] += 1
                dow_stats[dow]["sl"] += 1

        header = "[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate"
        print(header)
        _append_backtest_log(header)
        for h in range(24):
            e = hour_stats[h]["entries"]
            tp = hour_stats[h]["tp"]
            sl = hour_stats[h]["sl"]
            sl_rate = (sl / e * 100.0) if e else 0.0
            line = f"[BACKTEST] HOUR {h:02d} entries={e} tp={tp} sl={sl} sl_rate={sl_rate:.2f}%"
            print(line)
            _append_backtest_log(line)

        dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        header = "[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate"
        print(header)
        _append_backtest_log(header)
        for d in range(7):
            e = dow_stats[d]["entries"]
            tp = dow_stats[d]["tp"]
            sl = dow_stats[d]["sl"]
            sl_rate = (sl / e * 100.0) if e else 0.0
            line = f"[BACKTEST] DOW {dow_labels[d]} entries={e} tp={tp} sl={sl} sl_rate={sl_rate:.2f}%"
            print(line)
            _append_backtest_log(line)


if __name__ == "__main__":
    main()
