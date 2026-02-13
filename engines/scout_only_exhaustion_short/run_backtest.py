from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

import ccxt
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engines.backtest_common import load_common_universe, log_warmup_info
from engines.scout_only_exhaustion_short.engine import ScoutOnlyExhaustionShortConfig


def _read_cached_csv(path: str) -> List[List[float]]:
    rows: List[List[float]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            _ = next(reader, None)
            for row in reader:
                if not row:
                    continue
                try:
                    rows.append([float(x) for x in row[:6]])
                except Exception:
                    continue
    except Exception:
        return []
    return rows


def _fetch_ohlcv_all(
    exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    cache_only: bool = False,
    common_warmup_dir: str | None = None,
    common_only: bool = False,
) -> List[List[float]]:
    if common_warmup_dir:
        fname = symbol.replace("/", "_").replace(":", "_")
        cached_path = os.path.join(common_warmup_dir, f"{fname}_{timeframe}.csv")
        if os.path.exists(cached_path):
            rows = _read_cached_csv(cached_path)
            return [r for r in rows if start_ms <= r[0] <= end_ms]
        if common_only:
            return []
    if cache_only or exchange is None:
        return []
    since = start_ms
    out: List[List[float]] = []
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
        if not batch:
            break
        out.extend(batch)
        last = int(batch[-1][0])
        if last == since:
            break
        since = last + 1
    return [r for r in out if start_ms <= r[0] <= end_ms]


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    ma_up = up.ewm(alpha=1.0 / length, adjust=False).mean()
    ma_down = down.ewm(alpha=1.0 / length, adjust=False).mean()
    rs = ma_up / ma_down.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(50.0)


def _mfi(df: pd.DataFrame, length: int = 14) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"].astype(float)
    sign = tp.diff().fillna(0.0)
    pos = mf.where(sign > 0.0, 0.0)
    neg = mf.where(sign < 0.0, 0.0)
    pos_sum = pos.rolling(length, min_periods=1).sum()
    neg_sum = neg.rolling(length, min_periods=1).sum().replace(0.0, np.nan)
    mfr = pos_sum / neg_sum
    out = 100.0 - (100.0 / (1.0 + mfr))
    return out.fillna(50.0)


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / max(int(length), 1), adjust=False).mean()


def _new_stats() -> Dict[str, float]:
    return {
        "entries": 0,
        "exits": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "net_sum": 0.0,
        "net_sum_usdt": 0.0,
        "fee_included_net_sum": 0.0,
        "fee_included_net_sum_usdt": 0.0,
        "worst_trade_pct": 0.0,
    }


def _minute_str(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        dt = dt + pd.Timedelta(hours=9)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "N/A"


def _dow_label(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        dt = dt + pd.Timedelta(hours=9)
        return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    except Exception:
        return "N/A"


def _fmt_line(tag: str, s: Dict[str, float], base_usdt: float, days: int) -> str:
    trades = int(s["trades"])
    winrate = (float(s["wins"]) / trades * 100.0) if trades > 0 else 0.0
    tpd = (trades / max(float(days), 1.0)) if days > 0 else 0.0
    return (
        f"[BACKTEST] {tag} entries={int(s['entries'])} exits={int(s['exits'])} trades={trades} "
        f"trades_per_day={tpd:.3f} wins={int(s['wins'])} losses={int(s['losses'])} "
        f"winrate={winrate:.2f}% worst_trade_pct={float(s.get('worst_trade_pct', 0.0)):.3f} "
        f"net_sum={float(s['net_sum']):.3f} net_sum_usdt={float(s['net_sum_usdt']):.3f} "
        f"fee_included_net_sum={float(s.get('fee_included_net_sum', 0.0)):.3f} "
        f"fee_included_net_sum_usdt={float(s.get('fee_included_net_sum_usdt', 0.0)):.3f} "
        f"base_usdt={base_usdt:.2f}"
    )


def run_backtest() -> None:
    cfg = ScoutOnlyExhaustionShortConfig()
    parser = argparse.ArgumentParser("scout_only_exhaustion_short backtest")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--total-window-days", type=int, default=14)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--exclude-symbols", type=str, default="")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--use-confirmed", action="store_true")

    parser.add_argument("--pump-rise-3bars-min", type=float, default=cfg.pump_rise_3bars_min)
    parser.add_argument("--pump-rise-1bar-min", type=float, default=cfg.pump_rise_1bar_min)
    parser.add_argument("--pump-mfi-min", type=float, default=cfg.pump_mfi_min)
    parser.add_argument("--pump-vol-mult-min", type=float, default=cfg.pump_vol_mult_min)
    parser.add_argument("--pump-bb-excess-mult", type=float, default=cfg.pump_bb_excess_mult)
    parser.add_argument("--pump-optional-min-score", type=int, default=cfg.pump_optional_min_score)
    parser.add_argument("--pump-use-score-mode", action="store_true", dest="pump_use_score_mode")
    parser.add_argument("--no-pump-use-score-mode", action="store_false", dest="pump_use_score_mode")
    parser.set_defaults(pump_use_score_mode=cfg.pump_use_score_mode)
    parser.add_argument("--vol-sma-len", type=int, default=cfg.vol_sma_len)
    parser.add_argument("--bb-len", type=int, default=cfg.bb_len)
    parser.add_argument("--bb-std", type=float, default=cfg.bb_std)
    parser.add_argument("--watch-bars", type=int, default=cfg.watch_bars)

    parser.add_argument("--ema-fast-len", type=int, default=cfg.ema_fast_len)
    parser.add_argument("--ema-slow-len", type=int, default=cfg.ema_slow_len)
    parser.add_argument("--rsi-len", type=int, default=cfg.rsi_len)
    parser.add_argument("--scout-size", type=float, default=cfg.scout_size)
    parser.add_argument("--scout-tp-atr-mult", type=float, default=cfg.scout_tp_atr_mult)
    parser.add_argument("--scout-tp-dynamic", action="store_true", dest="scout_tp_dynamic")
    parser.add_argument("--no-scout-tp-dynamic", action="store_false", dest="scout_tp_dynamic")
    parser.set_defaults(scout_tp_dynamic=cfg.scout_tp_dynamic)
    parser.add_argument("--scout-tp-dynamic-step", type=float, default=cfg.scout_tp_dynamic_step)
    parser.add_argument("--scout-tp-dynamic-max", type=float, default=cfg.scout_tp_dynamic_max)
    parser.add_argument("--scout-loss-cap-pct", type=float, default=cfg.scout_loss_cap_pct)

    parser.add_argument("--entry-usdt", type=float, default=20.0)
    parser.add_argument("--fee-bps", type=float, default=8.0)
    parser.add_argument("--slip-pct", type=float, default=0.001)
    parser.add_argument("--slip-mode", type=str, default="uniform", choices=["uniform", "conservative"])
    parser.add_argument("--base-usdt", type=float, default=1000.0)
    parser.add_argument("--trades-out", type=str, default="")
    parser.add_argument("--log-gates", action="store_true")
    args = parser.parse_args()

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    total_window_days = max(int(args.total_window_days), 1)
    start_ms = end_ms - int((total_window_days + args.days) * 24 * 60 * 60 * 1000)
    eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    log_warmup_info(lambda _x: None, total_window_days, total_window_days * 1440, args.days)

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", os.path.join("logs", "common_warmup", "ohlcv"))
    use_common = bool(args.common_only or args.cache_only or args.common_warmup_dir)
    universe = load_common_universe(args.universe, exchange, args.cache_only, top_n=args.top_n)
    if args.exclude_symbols:
        raw = args.exclude_symbols.replace(" ", "").replace(";", ",").replace("|", ",")
        exclude = {s for s in raw.split(",") if s}
        if exclude:
            universe = [s for s in universe if s not in exclude]
    if not universe:
        print("[BACKTEST] no_universe")
        return

    stats = _new_stats()
    per_symbol: Dict[str, Dict[str, float]] = {}
    trade_rows: List[dict] = []
    exit_logs: List[dict] = []
    hour_stats: Dict[int, Dict[str, int]] = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
    dow_stats: Dict[str, Dict[str, int]] = {d: {"entries": 0, "tp": 0, "sl": 0} for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}
    date_stats: Dict[str, Dict[str, float]] = {}
    exit_reason_stats: Dict[str, Dict[str, float]] = {}
    gate = {
        "pump_detected": 0,
        "watch_started": 0,
        "entry_scout": 0,
        "exit_tp": 0,
        "exit_loss_cap": 0,
    }

    for sym in universe:
        rows = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_ltf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            common_warmup_dir=common_dir if use_common else None,
            common_only=args.common_only,
        )
        if not rows:
            continue
        d3 = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        d3 = d3.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        if len(d3) < 60:
            continue

        close = d3["close"].astype(float)
        vol = d3["volume"].astype(float)
        d3["rsi"] = _rsi(close, int(args.rsi_len))
        d3["mfi"] = _mfi(d3, 14)
        d3["ema_fast"] = _ema(close, int(args.ema_fast_len))
        d3["ema_slow"] = _ema(close, int(args.ema_slow_len))
        d3["vol_sma"] = vol.rolling(int(args.vol_sma_len), min_periods=1).mean()
        d3["bb_mid"] = close.rolling(int(args.bb_len), min_periods=1).mean()
        d3["bb_std"] = close.rolling(int(args.bb_len), min_periods=1).std(ddof=0).fillna(0.0)
        d3["bb_up"] = d3["bb_mid"] + float(args.bb_std) * d3["bb_std"]
        d3["atr"] = _atr(d3, 14).fillna(0.0)

        sym_stats = _new_stats()
        watch = None
        pos = None
        max_i = len(d3) - 1 - (1 if args.use_confirmed else 0)

        for i in range(30, max_i):
            ts = int(d3.at[i, "ts"])
            if ts < eval_start_ms:
                continue

            o = float(d3.at[i, "open"])
            h = float(d3.at[i, "high"])
            l = float(d3.at[i, "low"])
            c = float(d3.at[i, "close"])
            v = float(d3.at[i, "volume"])
            c1 = float(d3.at[i - 1, "close"])
            c3 = float(d3.at[i - 3, "close"])
            rise_1 = (c / c1 - 1.0) if c1 > 0 else 0.0
            rise_3 = (c / c3 - 1.0) if c3 > 0 else 0.0
            mfi = float(d3.at[i, "mfi"])
            vol_sma = max(float(d3.at[i, "vol_sma"]), 1e-12)
            bb_mid = float(d3.at[i, "bb_mid"])
            bb_up = float(d3.at[i, "bb_up"])
            bb_excess = max(c - bb_up, 0.0)
            bb_width = max((bb_up - bb_mid) * 2.0, 1e-12)
            vol_mult = v / vol_sma

            pass_rise3 = rise_3 >= float(args.pump_rise_3bars_min)
            pass_rise1 = rise_1 >= float(args.pump_rise_1bar_min)
            pass_mfi = mfi >= float(args.pump_mfi_min)
            pass_vol = vol_mult >= float(args.pump_vol_mult_min)
            pass_bb = (bb_excess / bb_width) >= float(args.pump_bb_excess_mult)
            if bool(args.pump_use_score_mode):
                optional_score = int(pass_rise1) + int(pass_mfi) + int(pass_bb)
                pump_now = pass_rise3 and pass_vol and (optional_score >= max(int(args.pump_optional_min_score), 1))
            else:
                pump_now = pass_rise3 and pass_rise1 and pass_mfi and pass_vol and pass_bb

            if pump_now:
                optional_score = int(pass_rise1) + int(pass_mfi) + int(pass_bb)
                watch = {
                    "pump_idx": i,
                    "pump_high": h,
                    "watch_expire_i": i + max(int(args.watch_bars), 1),
                    "pump_score": optional_score,
                }
                gate["pump_detected"] += 1
                gate["watch_started"] += 1

            if pos is None and watch is not None:
                if i > int(watch["watch_expire_i"]):
                    watch = None
                    continue
                if i <= int(watch["pump_idx"]):
                    continue
                atr3 = max(float(d3.at[i, "atr"]), 1e-12)
                ema_weak = float(d3.at[i, "ema_fast"]) < float(d3.at[i, "ema_slow"])
                rsi_weak = float(d3.at[i, "rsi"]) < float(d3.at[i - 1, "rsi"])
                bb_reject = (h >= bb_up) and (c < bb_up)
                weak_now = (ema_weak or rsi_weak) and bb_reject and (c < c1)
                if weak_now:
                    tp_mult = float(args.scout_tp_atr_mult)
                    if bool(args.scout_tp_dynamic):
                        score = max(int(watch.get("pump_score", 0)), 0)
                        tp_mult = min(float(args.scout_tp_dynamic_max), tp_mult + (float(args.scout_tp_dynamic_step) * score))
                    pos = {
                        "entry_i": i,
                        "avg_entry": c,
                        "size": float(args.scout_size),
                        "tp": c - tp_mult * atr3,
                    }
                    sym_stats["entries"] += 1
                    stats["entries"] += 1
                    gate["entry_scout"] += 1
                    continue

            if pos is not None:
                hit_tp = l <= float(pos["tp"])
                pnl_now = ((float(pos["avg_entry"]) - c) / max(float(pos["avg_entry"]), 1e-12)) * 100.0
                hit_loss_cap = pnl_now <= (-abs(float(args.scout_loss_cap_pct)) * 100.0)

                if not (hit_tp or hit_loss_cap):
                    continue
                loss_cap_px = float(pos["avg_entry"]) * (1.0 + abs(float(args.scout_loss_cap_pct)))
                if hit_tp:
                    exit_px = float(pos["tp"])
                    gate["exit_tp"] += 1
                    exit_reason = "tp"
                else:
                    exit_px = c
                    gate["exit_loss_cap"] += 1
                    exit_reason = "loss_cap"
                    exit_px = min(float(exit_px), float(loss_cap_px))

                slip_rate = max(float(args.slip_pct), 0.0)
                apply_slip = True
                if args.slip_mode == "conservative":
                    apply_slip = (exit_reason != "tp")
                exit_px_eff = float(exit_px) * (1.0 + (slip_rate if apply_slip else 0.0))
                gross_pnl_pct = ((float(pos["avg_entry"]) - exit_px_eff) / max(float(pos["avg_entry"]), 1e-12)) * 100.0
                fee_pnl_pct = 2.0 * (max(float(args.fee_bps), 0.0) / 10000.0) * 100.0
                net_pnl_pct = gross_pnl_pct - fee_pnl_pct
                pnl_usdt = float(args.entry_usdt) * float(pos["size"]) * (gross_pnl_pct / 100.0)
                fee_pnl_usdt = float(args.entry_usdt) * float(pos["size"]) * (net_pnl_pct / 100.0)

                sym_stats["exits"] += 1
                sym_stats["trades"] += 1
                sym_stats["net_sum"] += gross_pnl_pct
                sym_stats["net_sum_usdt"] += pnl_usdt
                sym_stats["fee_included_net_sum"] += net_pnl_pct
                sym_stats["fee_included_net_sum_usdt"] += fee_pnl_usdt
                if int(sym_stats["trades"]) == 1:
                    sym_stats["worst_trade_pct"] = net_pnl_pct
                else:
                    sym_stats["worst_trade_pct"] = min(float(sym_stats["worst_trade_pct"]), net_pnl_pct)

                stats["exits"] += 1
                stats["trades"] += 1
                stats["net_sum"] += gross_pnl_pct
                stats["net_sum_usdt"] += pnl_usdt
                stats["fee_included_net_sum"] += net_pnl_pct
                stats["fee_included_net_sum_usdt"] += fee_pnl_usdt
                if int(stats["trades"]) == 1:
                    stats["worst_trade_pct"] = net_pnl_pct
                else:
                    stats["worst_trade_pct"] = min(float(stats["worst_trade_pct"]), net_pnl_pct)

                if net_pnl_pct >= 0:
                    sym_stats["wins"] += 1
                    stats["wins"] += 1
                    result_reason = "TP"
                else:
                    sym_stats["losses"] += 1
                    stats["losses"] += 1
                    result_reason = "SL"
                entry_ts_ms = int(d3.at[int(pos["entry_i"]), "ts"])
                day_key = _minute_str(entry_ts_ms).split(" ")[0]
                hour = int(_minute_str(entry_ts_ms).split(" ")[1].split(":")[0]) if _minute_str(entry_ts_ms) != "N/A" else -1
                dow = _dow_label(entry_ts_ms)
                if 0 <= hour <= 23:
                    hour_stats.setdefault(hour, {"entries": 0, "tp": 0, "sl": 0})
                    hour_stats[hour]["entries"] += 1
                    hour_stats[hour]["tp" if result_reason == "TP" else "sl"] += 1
                dow_stats.setdefault(dow, {"entries": 0, "tp": 0, "sl": 0})
                dow_stats[dow]["entries"] += 1
                dow_stats[dow]["tp" if result_reason == "TP" else "sl"] += 1
                date_stats.setdefault(day_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
                date_stats[day_key]["entries"] += 1
                date_stats[day_key]["tp" if result_reason == "TP" else "sl"] += 1
                date_stats[day_key]["net_sum"] += float(gross_pnl_pct)
                date_stats[day_key]["net_sum_usdt"] += float(pnl_usdt)
                ex_key = str(exit_reason or "unknown")
                exit_reason_stats.setdefault(ex_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
                exit_reason_stats[ex_key]["entries"] += 1
                exit_reason_stats[ex_key]["tp" if result_reason == "TP" else "sl"] += 1
                exit_reason_stats[ex_key]["net_sum"] += float(gross_pnl_pct)
                exit_reason_stats[ex_key]["net_sum_usdt"] += float(pnl_usdt)
                exit_logs.append(
                    {
                        "sym": sym,
                        "mode": "scout_only_exhaustion_short",
                        "side": "SHORT",
                        "entry_ts": entry_ts_ms,
                        "exit_ts": ts,
                        "entry_px": float(pos["avg_entry"]),
                        "exit_px": float(exit_px_eff),
                        "reason": result_reason,
                        "exit_reason": str(exit_reason),
                        "tp_px": float(pos["tp"]),
                        "loss_cap_px": float(loss_cap_px),
                        "gross_pnl_pct": float(gross_pnl_pct),
                        "net_pnl_pct": float(net_pnl_pct),
                        "gross_pnl_usdt": float(pnl_usdt),
                        "net_pnl_usdt": float(fee_pnl_usdt),
                        "slip_pct": float(args.slip_pct),
                        "fee_bps": float(args.fee_bps),
                    }
                )
                trade_rows.append(
                    {
                        "engine_id": "scout_only_exhaustion_short",
                        "symbol": sym,
                        "entry_ts": int(d3.at[int(pos["entry_i"]), "ts"]),
                        "exit_ts": ts,
                        "entry_px": float(pos["avg_entry"]),
                        "exit_px_raw": float(exit_px),
                        "exit_px_eff": float(exit_px_eff),
                        "pnl_pct_gross": float(gross_pnl_pct),
                        "pnl_pct_fee": float(net_pnl_pct),
                        "pnl_usdt_gross": float(pnl_usdt),
                        "pnl_usdt_fee": float(fee_pnl_usdt),
                        "exit_reason": str(exit_reason),
                    }
                )
                pos = None
                watch = None

        if int(sym_stats["trades"]) > 0:
            per_symbol[sym] = sym_stats

    total_line = _fmt_line("TOTAL", stats, float(args.base_usdt), int(args.days))
    for sym, s in sorted(per_symbol.items(), key=lambda x: x[1]["net_sum_usdt"], reverse=True):
        print(_fmt_line(sym, s, float(args.base_usdt), int(args.days)))
        sym_exits = [x for x in exit_logs if x["sym"] == sym]
        sym_exits.sort(key=lambda x: x["entry_ts"], reverse=True)
        for item in sym_exits:
            result = "WIN" if item.get("reason") == "TP" else "LOSS"
            print(
                "[BACKTEST][EXIT] "
                f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                f"entry_dt={_minute_str(item['entry_ts'])} exit_dt={_minute_str(item['exit_ts'])} "
                f"entry_px={item['entry_px']:.6f} exit_px={item['exit_px']:.6f} "
                f"tp_px={float(item.get('tp_px', 0.0)):.6f} loss_cap_px={float(item.get('loss_cap_px', 0.0)):.6f} "
                f"gross_pnl_pct={float(item.get('gross_pnl_pct', 0.0)):.3f} net_pnl_pct={float(item.get('net_pnl_pct', 0.0)):.3f} "
                f"gross_pnl_usdt={float(item.get('gross_pnl_usdt', 0.0)):.4f} net_pnl_usdt={float(item.get('net_pnl_usdt', 0.0)):.4f} "
                f"slip_pct={float(item.get('slip_pct', 0.0)):.4f} fee_bps={float(item.get('fee_bps', 0.0)):.1f} "
                f"reason={item['reason']} exit_reason={item['exit_reason']} result={result}"
            )
    print(total_line)

    print("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for hour in range(24):
        b = hour_stats.get(hour, {"entries": 0, "tp": 0, "sl": 0})
        entries = int(b["entries"])
        sl = int(b["sl"])
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={int(b['tp'])} sl={sl} sl_rate={sl_rate:.2f}%")

    print("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        b = dow_stats.get(dow, {"entries": 0, "tp": 0, "sl": 0})
        entries = int(b["entries"])
        sl = int(b["sl"])
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(f"[BACKTEST] DOW {dow} entries={entries} tp={int(b['tp'])} sl={sl} sl_rate={sl_rate:.2f}%")

    print("[BACKTEST] BY_EXIT_REASON reason entries tp sl sl_rate winrate net_sum net_sum_usdt")
    for reason in sorted(exit_reason_stats.keys()):
        b = exit_reason_stats[reason]
        entries = int(b["entries"])
        tp = int(b["tp"])
        sl = int(b["sl"])
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        winrate = (tp / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] EXIT_REASON {reason} entries={entries} tp={tp} sl={sl} sl_rate={sl_rate:.2f}% "
            f"winrate={winrate:.2f}% net_sum={float(b['net_sum']):.3f} net_sum_usdt={float(b['net_sum_usdt']):.3f}"
        )

    if date_stats:
        print("[BACKTEST] BY_DATE(KST) date entries tp sl sl_rate winrate net_sum net_sum_usdt")
        total_entries = total_tp = total_sl = 0
        total_net_sum = 0.0
        total_net_sum_usdt = 0.0
        for day_key in sorted(date_stats.keys()):
            b = date_stats[day_key]
            entries = int(b["entries"])
            tp = int(b["tp"])
            sl = int(b["sl"])
            trades = tp + sl
            sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
            winrate = (tp / trades * 100.0) if trades > 0 else 0.0
            net_sum = float(b["net_sum"])
            net_sum_usdt = float(b["net_sum_usdt"])
            total_entries += entries
            total_tp += tp
            total_sl += sl
            total_net_sum += net_sum
            total_net_sum_usdt += net_sum_usdt
            print(
                f"[BACKTEST] DATE {day_key} entries={entries} tp={tp} sl={sl} sl_rate={sl_rate:.2f}% "
                f"winrate={winrate:.2f}% net_sum={net_sum:.3f} net_sum_usdt={net_sum_usdt:.3f}"
            )
        total_trades = total_tp + total_sl
        total_sl_rate = (total_sl / total_entries * 100.0) if total_entries > 0 else 0.0
        total_winrate = (total_tp / total_trades * 100.0) if total_trades > 0 else 0.0
        print(
            f"[BACKTEST] DATE TOTAL entries={total_entries} tp={total_tp} sl={total_sl} "
            f"sl_rate={total_sl_rate:.2f}% winrate={total_winrate:.2f}% "
            f"net_sum={total_net_sum:.3f} net_sum_usdt={total_net_sum_usdt:.3f}"
        )

    if args.log_gates:
        print("[BACKTEST] GATES " + str(gate))
    if args.trades_out:
        try:
            with open(args.trades_out, "w", encoding="utf-8") as f:
                for row in trade_rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(f"[BACKTEST] trades_out={args.trades_out} rows={len(trade_rows)}")
        except Exception as exc:
            print(f"[BACKTEST] trades_out_error={exc}")


if __name__ == "__main__":
    run_backtest()
