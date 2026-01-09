#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import time
from typing import Dict, List

import ccxt

from engines.swaggy_atlas_lab.atlas_eval import evaluate_global_gate, evaluate_local
from engines.swaggy_atlas_lab.broker_sim import BrokerSim
from engines.swaggy_atlas_lab.config import AtlasConfig, BacktestConfig, SwaggyConfig
from engines.swaggy_atlas_lab.data import (
    build_universe_from_tickers,
    ensure_dir,
    fetch_ohlcv_all,
    parse_ts,
    slice_df,
    to_df,
    save_universe,
)
from engines.swaggy_atlas_lab.policy import AtlasMode, apply_policy
from engines.swaggy_atlas_lab.report import build_summary, write_summary_json, write_trades_csv
from engines.swaggy_atlas_lab.swaggy_signal import SwaggySignalEngine


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "swap"}})


def parse_args():
    parser = argparse.ArgumentParser(description="Swaggy x Atlas Lab backtest")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--mode", default="all", choices=("hard", "soft", "shadow", "off", "all"))
    parser.add_argument("--symbols", default="", help="comma-separated symbols")
    parser.add_argument("--symbols-file", default="", help="path to fixed symbol list")
    parser.add_argument("--universe", default="top50", help="topN (e.g. top50) or 'symbols'")
    parser.add_argument("--anchor", default="BTC,ETH")
    parser.add_argument("--tp", type=float, default=0.02)
    parser.add_argument("--sl", type=float, default=0.02)
    parser.add_argument("--fee", type=float, default=0.0)
    parser.add_argument("--slippage", type=float, default=0.0)
    parser.add_argument("--timeout-bars", type=int, default=0)
    return parser.parse_args()


def _parse_universe_arg(text: str) -> int:
    if text.lower().startswith("top"):
        try:
            return int(text.lower().replace("top", ""))
        except Exception:
            return 50
    return 0


def main() -> None:
    args = parse_args()
    start_ms = parse_ts(args.start)
    end_ms = parse_ts(args.end)
    if end_ms <= start_ms:
        raise SystemExit("end must be after start")

    run_id = time.strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join("logs", "swaggy_atlas_lab")
    rep_dir = os.path.join("reports", "swaggy_atlas_lab")
    ensure_dir(log_dir)
    ensure_dir(rep_dir)
    log_path = os.path.join(log_dir, f"bt_{run_id}.log")
    trades_path = os.path.join(rep_dir, f"trades_{run_id}.csv")
    summary_path = os.path.join(rep_dir, f"summary_{run_id}.json")
    universe_path = os.path.join(rep_dir, f"universe_{run_id}.json")

    bt_cfg = BacktestConfig(tp_pct=args.tp, sl_pct=args.sl, fee_rate=args.fee, slippage_pct=args.slippage, timeout_bars=args.timeout_bars, mode=args.mode)
    sw_cfg = SwaggyConfig()
    at_cfg = AtlasConfig()

    ex = _make_exchange()
    ex.load_markets()
    anchor_symbols = [s.strip() for s in args.anchor.split(",") if s.strip()]
    symbols: List[str] = []
    if args.symbols_file.strip():
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            symbols = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    elif args.symbols.strip():
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        top_n = _parse_universe_arg(args.universe)
        tickers = ex.fetch_tickers()
        symbols = build_universe_from_tickers(tickers, top_n or 50, anchor_symbols)
    source = "symbols_file" if args.symbols_file.strip() else ("symbols_arg" if args.symbols.strip() else "live_tickers")
    save_universe(universe_path, symbols, {"source": source, "start": args.start, "end": args.end})

    data_by_sym: Dict[str, Dict[str, object]] = {}
    tfs = [sw_cfg.tf_ltf, sw_cfg.tf_mtf, sw_cfg.tf_htf, sw_cfg.tf_htf2]
    for sym in symbols:
        tf_map: Dict[str, object] = {}
        for tf in tfs:
            rows = fetch_ohlcv_all(ex, sym, tf, start_ms, end_ms)
            tf_map[tf] = to_df(rows)
        data_by_sym[sym] = tf_map

    btc_data = fetch_ohlcv_all(ex, at_cfg.ref_symbol, sw_cfg.tf_mtf, start_ms, end_ms)
    btc_df = to_df(btc_data)

    modes = [AtlasMode.HARD, AtlasMode.SOFT, AtlasMode.SHADOW, AtlasMode.OFF] if args.mode == "all" else [AtlasMode(args.mode)]
    trades: List[Dict] = []

    with open(log_path, "a", encoding="utf-8") as log_fp:
        for mode in modes:
            log_fp.write(f"[run] mode={mode.value} start={args.start} end={args.end}\n")
            engine = SwaggySignalEngine(sw_cfg)
            broker = BrokerSim(bt_cfg.tp_pct, bt_cfg.sl_pct, bt_cfg.fee_rate, bt_cfg.slippage_pct, bt_cfg.timeout_bars)
            for sym in symbols:
                df_ltf = data_by_sym[sym][sw_cfg.tf_ltf]
                df_mtf = data_by_sym[sym][sw_cfg.tf_mtf]
                df_htf = data_by_sym[sym][sw_cfg.tf_htf]
                df_htf2 = data_by_sym[sym][sw_cfg.tf_htf2]
                for i in range(30, len(df_ltf)):
                    cur = df_ltf.iloc[i]
                    ts_ms = int(cur["ts"])
                    now_ts = ts_ms / 1000.0
                    d5 = df_ltf.iloc[: i + 1]
                    d15 = slice_df(df_mtf, ts_ms)
                    d1h = slice_df(df_htf, ts_ms)
                    d4h = slice_df(df_htf2, ts_ms)
                    signal = engine.evaluate_symbol(sym, d4h, d1h, d15, d5, now_ts)
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
                                    "pnl_usdt": pnl_usdt,
                                    "pnl_pct": pnl_pct,
                                    "fee": fee,
                                    "duration_bars": trade.bars,
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
                        continue

                    if not signal.entry_ok or not side or entry_px is None:
                        continue

                    atlas = None
                    if mode != AtlasMode.OFF:
                        btc_slice = slice_df(btc_df, ts_ms)
                        gate = evaluate_global_gate(btc_slice, at_cfg)
                        atlas = evaluate_local(sym, side, d15, btc_slice, gate, at_cfg)

                    policy = apply_policy(mode, bt_cfg.base_usdt, atlas)
                    log_fp.write(
                        "ENTRY ts=%d sym=%s side=%s mode=%s sw_ok=%s sw_strength=%.3f sw_reasons=%s "
                        "base_usdt=%.2f final_usdt=%.2f atlas_pass=%s atlas_mult=%s atlas_reasons=%s "
                        "atlas_shadow_pass=%s atlas_shadow_mult=%s atlas_shadow_reasons=%s\n"
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
                            policy.shadow_pass if policy.shadow_pass is not None else "N/A",
                            policy.shadow_mult if policy.shadow_mult is not None else "N/A",
                            policy.shadow_reasons if policy.shadow_reasons is not None else "N/A",
                        )
                    )
                    if not policy.allow:
                        continue
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
                        atlas_shadow_pass=policy.shadow_pass,
                        atlas_shadow_reasons=policy.shadow_reasons,
                    )

    write_trades_csv(trades_path, trades)
    summary = build_summary(run_id, trades)
    write_summary_json(summary_path, summary)
    print(f"[done] trades={trades_path} summary={summary_path} log={log_path}")


if __name__ == "__main__":
    main()
