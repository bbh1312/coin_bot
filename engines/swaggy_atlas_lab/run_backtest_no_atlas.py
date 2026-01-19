#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Dict, List

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


def _append_backtest_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_atlas_lab", "backtest", f"backtest_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _append_backtest_entry_log(line: str) -> None:
    date_tag = time.strftime("%Y%m%d")
    path = os.path.join("logs", "swaggy_atlas_lab", "backtest", f"backtest_entries_{date_tag}.log")
    ensure_dir(os.path.dirname(path))
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} {line}\n")


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})


def parse_args():
    parser = argparse.ArgumentParser(description="Swaggy Lab backtest (no Atlas gate/policy)")
    parser.add_argument("--days", type=int, default=7)
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
    log_dir = os.path.join("logs", "swaggy_atlas_lab", "backtest")
    rep_dir = os.path.join("reports", "swaggy_atlas_lab")
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
    )
    sw_cfg = SwaggyConfig()
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

    mode_name = "no_atlas"
    trades: List[Dict] = []
    stats_by_key: Dict[tuple[str, str], Dict[str, float]] = {}
    overext_by_key: Dict[tuple[str, str], Dict[str, int]] = {}
    d1_block_by_key: Dict[tuple[str, str], int] = {}

    with open(log_path, "a", encoding="utf-8") as log_fp:
        run_line = f"[run] mode={mode_name} days={args.days} start_ms={start_ms} end_ms={end_ms}"
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

                if broker.has_position(sym):
                    trade = broker.on_bar(sym, ts_ms, float(cur["high"]), float(cur["low"]), float(cur["close"]), i)
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
                    continue

                if not signal.entry_ok or not side or entry_px is None:
                    continue

                entry_line = (
                    "ENTRY ts=%d sym=%s side=%s mode=%s sw_ok=%s sw_strength=%.3f sw_reasons=%s "
                    "base_usdt=%.2f final_usdt=%.2f policy_action=%s"
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
                    )
                )
                log_fp.write(entry_line + "\n")
                _append_backtest_log(entry_line)
                dist_at_entry = _overext_dist(d5, side, sw_cfg)
                overext_blocked = bool(sym_state.get("overext_blocked"))
                sym_state["overext_blocked"] = False
                _append_backtest_entry_log(
                    "engine=swaggy_atlas_lab mode=%s symbol=%s side=%s entry=%.6g "
                    "final_usdt=%.2f sw_strength=%.3f sw_reasons=%s policy_action=%s"
                    % (
                        mode_name,
                        sym,
                        side,
                        float(entry_px or 0.0),
                        float(bt_cfg.base_usdt or 0.0),
                        float(signal.strength or 0.0),
                        ",".join(signal.reasons or []),
                        "NO_ATLAS",
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
        for (mode, sym), stats in stats_by_key.items():
            trades_count = int(stats.get("trades") or 0)
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
            print(
                "[BACKTEST] %s trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f "
                "long_wins=%d long_losses=%d short_wins=%d short_losses=%d"
                % (
                    f"{sym}@{mode}",
                    trades_count,
                    wins,
                    losses,
                    win_rate,
                    tp,
                    sl,
                    avg_mfe,
                    avg_mae,
                    avg_hold,
                    long_wins,
                    long_losses,
                    short_wins,
                    short_losses,
                )
            )
            for t in trades_by_key.get((mode, sym), []):
                hold_min = float(t.get("duration_bars") or 0) * ltf_minutes
                pnl_pct = float(t.get("pnl_pct") or 0.0)
                outcome = "WIN" if pnl_pct > 0 else "LOSS"
                print(
                    "[ENTRY] sym=%s side=%s outcome=%s mode=%s entry_ts=%s exit_ts=%s reason=%s pnl_pct=%.4f hold_min=%.1f"
                    % (
                        t.get("sym"),
                        t.get("side"),
                        outcome,
                        t.get("mode"),
                        t.get("entry_ts"),
                        t.get("exit_ts"),
                        t.get("exit_reason"),
                        pnl_pct,
                        hold_min,
                    )
                )
            mode_total = total_by_mode.setdefault(
                mode,
                {
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
            for key in ("trades", "wins", "losses", "tp", "sl"):
                mode_total[key] += int(stats.get(key) or 0)
            mode_total["mfe_sum"] += float(stats.get("mfe_sum") or 0.0)
            mode_total["mae_sum"] += float(stats.get("mae_sum") or 0.0)
            mode_total["hold_sum"] += float(stats.get("hold_sum") or 0.0)
            mode_total["long_wins"] += long_wins
            mode_total["long_losses"] += long_losses
            mode_total["short_wins"] += short_wins
            mode_total["short_losses"] += short_losses
        grand_total = {
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
            wins = int(stats.get("wins") or 0)
            losses = int(stats.get("losses") or 0)
            tp = int(stats.get("tp") or 0)
            sl = int(stats.get("sl") or 0)
            win_rate = (wins / trades_count * 100.0) if trades_count else 0.0
            avg_mfe = (stats.get("mfe_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_mae = (stats.get("mae_sum", 0.0) / trades_count) if trades_count else 0.0
            avg_hold = (stats.get("hold_sum", 0.0) / trades_count) if trades_count else 0.0
            print(
                "[BACKTEST] %s trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f "
                "long_wins=%d long_losses=%d short_wins=%d short_losses=%d"
                % (
                    f"TOTAL@{mode}",
                    trades_count,
                    wins,
                    losses,
                    win_rate,
                    tp,
                    sl,
                    avg_mfe,
                    avg_mae,
                    avg_hold,
                    int(stats.get("long_wins") or 0),
                    int(stats.get("long_losses") or 0),
                    int(stats.get("short_wins") or 0),
                    int(stats.get("short_losses") or 0),
                )
            )
            for key in ("trades", "wins", "losses", "tp", "sl"):
                grand_total[key] += int(stats.get(key) or 0)
            grand_total["mfe_sum"] += float(stats.get("mfe_sum") or 0.0)
            grand_total["mae_sum"] += float(stats.get("mae_sum") or 0.0)
            grand_total["hold_sum"] += float(stats.get("hold_sum") or 0.0)
            for key in ("long_wins", "long_losses", "short_wins", "short_losses"):
                grand_total[key] += int(stats.get(key) or 0)
        total_trades = int(grand_total.get("trades") or 0)
        total_wins = int(grand_total.get("wins") or 0)
        total_losses = int(grand_total.get("losses") or 0)
        total_tp = int(grand_total.get("tp") or 0)
        total_sl = int(grand_total.get("sl") or 0)
        total_win_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
        total_avg_mfe = (grand_total.get("mfe_sum", 0.0) / total_trades) if total_trades else 0.0
        total_avg_mae = (grand_total.get("mae_sum", 0.0) / total_trades) if total_trades else 0.0
        total_avg_hold = (grand_total.get("hold_sum", 0.0) / total_trades) if total_trades else 0.0
        print(
            "[BACKTEST] TOTAL trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
            "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f "
            "long_wins=%d long_losses=%d short_wins=%d short_losses=%d"
            % (
                total_trades,
                total_wins,
                total_losses,
                total_win_rate,
                total_tp,
                total_sl,
                total_avg_mfe,
                total_avg_mae,
                total_avg_hold,
                int(grand_total.get("long_wins") or 0),
                int(grand_total.get("long_losses") or 0),
                int(grand_total.get("short_wins") or 0),
                int(grand_total.get("short_losses") or 0),
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


if __name__ == "__main__":
    main()
