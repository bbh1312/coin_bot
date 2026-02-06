#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import ccxt
import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engines.backtest_common import calc_warmup_window, load_common_universe
from engines.volume_profile import build_volume_profile


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
        "liquidity_sniper_v1",
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
    try:
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df.to_csv(path, index=False)
    except Exception:
        return


def _read_common_warmup(path: str) -> List[list]:
    if not path or not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        if df.empty or "ts" not in df.columns:
            return []
        return df[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    except Exception:
        return []


def _read_oi_csv(path: str) -> Optional[pd.DataFrame]:
    if not path or not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        if df.empty or "ts" not in df.columns:
            return None
        if "oi" not in df.columns:
            return None
        return df[["ts", "oi"]].copy()
    except Exception:
        return None


def _fetch_ohlcv_all(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
    use_common_warmup: bool = False,
    common_warmup_dir: str = "",
    cache_only: bool = False,
) -> List[list]:
    if use_common_warmup and common_warmup_dir:
        safe = _sanitize_symbol(symbol)
        warmup_path = os.path.join(common_warmup_dir, f"{safe}_{timeframe}.csv")
        warmup_rows = _read_common_warmup(warmup_path)
        if warmup_rows:
            return [r for r in warmup_rows if start_ms <= int(r[0]) <= end_ms]
        if cache_only:
            return []
    cache_path = _ohlcv_cache_path(ROOT_DIR, symbol, timeframe, start_ms, end_ms)
    cached = _read_ohlcv_cache(cache_path)
    if cached:
        return cached
    if cache_only:
        return []
    out: List[list] = []
    since = start_ms
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    last_ts = None
    while since < end_ms:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if ts > end_ms:
                continue
            if last_ts is None or ts > last_ts:
                out.append(row)
                last_ts = ts
        new_last = int(batch[-1][0])
        if last_ts is None or new_last == last_ts:
            since = new_last + tf_ms
        else:
            since = last_ts + tf_ms
        if len(batch) < 2:
            break
        time.sleep(max(exchange.rateLimit, 200) / 1000.0)
    if out:
        out = out[:-1]
    _write_ohlcv_cache(cache_path, out)
    return out


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    up_move = high.diff()
    down_move = low.diff().abs()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = pd.concat(
        [
            (high - low),
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    tr_smooth = tr.rolling(length).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(length).mean() / tr_smooth
    minus_di = 100 * pd.Series(minus_dm).rolling(length).mean() / tr_smooth
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).replace([np.inf, -np.inf], np.nan) * 100
    return dx.rolling(length).mean()


def _vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"].astype(float) + df["low"].astype(float) + df["close"].astype(float)) / 3.0
    vol = df["volume"].astype(float)
    pv = (typical * vol).cumsum()
    vv = vol.cumsum().replace(0, np.nan)
    return pv / vv


def _map_idx_by_ts(ts_arr: np.ndarray, ts: int) -> int:
    return int(np.searchsorted(ts_arr, ts, side="right") - 1)


def parse_args():
    p = argparse.ArgumentParser(description="Liquidity Sniper Backtest Runner")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--universe", type=str, default="common")
    p.add_argument("--use-confirmed", action="store_true")
    p.add_argument("--use-common-warmup", action="store_true")
    p.add_argument("--common-warmup-dir", type=str, default="")
    p.add_argument("--use-live-cache", action="store_true")
    p.add_argument("--cache-only", action="store_true")
    p.add_argument("--tf-exec", type=str, default="5m")
    p.add_argument("--tf-htf", type=str, default="1h")
    p.add_argument("--atr-len", type=int, default=14)
    p.add_argument("--adx-len", type=int, default=14)
    p.add_argument("--adx-max", type=float, default=40.0)
    p.add_argument("--vp-lookback-bars", type=int, default=288)
    p.add_argument("--vp-bin-pct", type=float, default=0.002)
    p.add_argument("--zone-atr-mult", type=float, default=0.25)
    p.add_argument("--cvd-lookback", type=int, default=4)
    p.add_argument("--cvd-drop-mult", type=float, default=1.5)
    p.add_argument("--oi-lookback", type=int, default=6)
    p.add_argument("--use-oi-filter", action="store_true")
    p.add_argument("--oi-rise-min", type=float, default=0.0)
    p.add_argument("--oi-dir", type=str, default="")
    p.add_argument("--oi-tf", type=str, default="")
    p.add_argument("--htf-ema-len", type=int, default=200)
    p.add_argument("--htf-mr-mult", type=float, default=0.96)
    p.add_argument("--htf-rsi-len", type=int, default=14)
    p.add_argument("--htf-rsi-allow", type=float, default=30.0)
    p.add_argument("--sl-pct", type=float, default=0.0055)
    p.add_argument("--sl-atr-buffer", type=float, default=0.1)
    p.add_argument("--tp-rr", type=float, default=1.2)
    p.add_argument("--tp1-pct", type=float, default=0.008)
    p.add_argument("--tp2-pct", type=float, default=0.015)
    p.add_argument("--tp1-size", type=float, default=0.8)
    p.add_argument("--tp2-size", type=float, default=0.2)
    p.add_argument("--trail-size", type=float, default=0.0)
    p.add_argument("--time-stop-bars", type=int, default=2)
    p.add_argument("--time-stop-profit-pct", type=float, default=0.0)
    p.add_argument("--vwap-eps", type=float, default=1.0)
    p.add_argument("--be-pct", type=float, default=0.005)
    p.add_argument("--be-fee-pct", type=float, default=0.0005)
    p.add_argument("--boredom-bars", type=int, default=0)
    p.add_argument("--boredom-size", type=float, default=0.0)
    p.add_argument("--trail-atr-mult", type=float, default=1.5)
    p.add_argument("--vol-spike-mult", type=float, default=1.5)
    p.add_argument("--entry-vol-mult", type=float, default=1.0)
    p.add_argument("--early-entry-size", type=float, default=0.5)
    p.add_argument("--pinbar-wick-ratio", type=float, default=0.6)
    p.add_argument("--pinbar-vol-mult", type=float, default=1.2)
    p.add_argument("--log-path", type=str, default="")
    return p.parse_args()


def main():
    args = parse_args()
    exchange = ccxt.binance({"options": {"defaultType": "future"}})
    use_live_cache = bool(args.use_live_cache)
    use_common = bool(args.use_common_warmup) or use_live_cache
    common_dir = args.common_warmup_dir or os.getenv("COMMON_WARMUP_CACHE_DIR", "")
    oi_dir = args.oi_dir or (os.path.join(common_dir, "oi") if common_dir else "")

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_exec = max(args.vp_lookback_bars + 20, 240)
    min_htf = max(args.htf_ema_len + 10, 168)
    min_bars_by_tf = {args.tf_exec: min_exec, args.tf_htf: min_htf}
    start_ms, eval_start_ms, _, _ = calc_warmup_window(
        int(args.days),
        end_ms,
        min_bars_by_tf,
    )

    symbols = load_common_universe(args.universe, exchange, args.cache_only)
    if not symbols:
        print("[BACKTEST] no symbols in universe")
        return

    symbol_data: Dict[str, Dict[str, List[list]]] = {}
    for sym in symbols:
        rows_exec = _fetch_ohlcv_all(
            exchange,
            sym,
            args.tf_exec,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        rows_htf = _fetch_ohlcv_all(
            exchange,
            sym,
            args.tf_htf,
            start_ms,
            end_ms,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            cache_only=args.cache_only,
        )
        if rows_exec and rows_htf:
            symbol_data[sym] = {"exec": rows_exec, "htf": rows_htf}

    stats = {
        "entries": 0,
        "exits": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
        "net_sum": 0.0,
    }
    gate_stats = {
        "htf_fail": 0,
        "adx_fail": 0,
        "zone_fail": 0,
        "cvd_fail": 0,
        "oi_fail": 0,
        "vwap_fail": 0,
        "momentum_fail": 0,
        "entry_fail": 0,
        "time_fail": 0,
    }

    for symbol, data in symbol_data.items():
        sym_stats = {
            "entries": 0,
            "exits": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "mfe_sum": 0.0,
            "mae_sum": 0.0,
            "hold_sum": 0.0,
            "net_sum": 0.0,
        }
        df_ex = pd.DataFrame(data["exec"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        df_htf = pd.DataFrame(data["htf"], columns=["ts", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
        if df_ex.empty or df_htf.empty:
            continue
        ts_ex = df_ex["ts"].astype(int).to_numpy()
        ts_htf = df_htf["ts"].astype(int).to_numpy()

        atr = _atr(df_ex, args.atr_len)
        adx_ltf = _adx(df_ex, args.adx_len)
        vwap_ltf = _vwap(df_ex)
        ema10 = _ema(df_ex["close"].astype(float), 10)
        htf_close = df_htf["close"].astype(float)
        htf_ema = _ema(htf_close, args.htf_ema_len)
        htf_rsi = _rsi(htf_close, args.htf_rsi_len)
        oi_df = None
        if args.use_oi_filter:
            oi_tf = args.oi_tf or args.tf_exec
            safe = _sanitize_symbol(symbol)
            oi_path = os.path.join(oi_dir, f"{safe}_{oi_tf}.csv")
            oi_df = _read_oi_csv(oi_path)

        delta = df_ex["volume"].astype(float) * np.sign(df_ex["close"].astype(float) - df_ex["open"].astype(float))
        cvd = delta.cumsum()

        trade = None
        zone_active = False
        zone_until = -1
        absorb_active = False
        absorb_until = -1
        absorb_low = None
        vp_levels: Dict[str, float] = {}

        for i in range(1, len(df_ex)):
            sig_idx = i - 1 if args.use_confirmed else i
            if sig_idx <= 0:
                continue
            ts = int(ts_ex[sig_idx])
            idx_htf = _map_idx_by_ts(ts_htf, ts)
            if idx_htf <= 0:
                continue

            if trade:
                high = float(df_ex.at[i, "high"])
                low = float(df_ex.at[i, "low"])
                close = float(df_ex.at[i, "close"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (high - trade["entry_px"]) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (trade["entry_px"] - low) / trade["entry_px"]))
                trade["max_high"] = max(trade["max_high"], high)
                trade["last_lows"].append(low)
                if len(trade["last_lows"]) > 3:
                    trade["last_lows"].pop(0)

                exit_reason = None
                exit_px = None
                if not trade.get("be_done") and trade["max_high"] >= trade["entry_px"] * (1 + args.be_pct):
                    trade["sl_price"] = trade["entry_px"] * (1 + args.be_fee_pct)
                    trade["be_done"] = True
                if low <= trade["sl_price"]:
                    exit_reason = "SL"
                    exit_px = trade["sl_price"]
                elif not trade["tp1_done"]:
                    if high >= trade["tp1_price"]:
                        trade["tp1_done"] = True
                        trade["realized"] += (trade["tp1_price"] - trade["entry_px"]) / trade["entry_px"] * trade["tp1_size"]
                        trade["remaining"] = max(0.0, trade["remaining"] - trade["tp1_size"])
                        trade["sl_price"] = trade["entry_px"] * (1 + args.be_fee_pct)
                elif trade["tp1_done"]:
                    if high >= trade["tp2_price"]:
                        if not trade.get("tp2_done"):
                            trade["tp2_done"] = True
                            trade["realized"] += (trade["tp2_price"] - trade["entry_px"]) / trade["entry_px"] * trade["tp2_size"]
                            trade["remaining"] = max(0.0, trade["remaining"] - trade["tp2_size"])
                            trade["trail_active"] = True
                            if trade["remaining"] <= 0:
                                exit_reason = "TP2"
                                exit_px = trade["tp2_price"]
                if exit_reason is None and not trade["tp1_done"] and trade["hold_bars"] >= args.boredom_bars:
                    if trade["max_high"] < trade["tp1_price"]:
                        trade["realized"] += (trade["entry_px"] - trade["entry_px"]) / trade["entry_px"] * min(trade["remaining"], args.boredom_size)
                        trade["remaining"] = max(0.0, trade["remaining"] - args.boredom_size)
                if trade.get("trail_active"):
                    trail_stop = trade["max_high"] - trade["atr_entry"] * args.trail_atr_mult
                    if close < trail_stop:
                        exit_reason = "TRAIL"
                        exit_px = close
                if exit_reason is None and trade["hold_bars"] >= args.time_stop_bars and not trade["tp1_done"]:
                    exit_reason = "TIME_STOP"
                    exit_px = close
                    gate_stats["time_fail"] += 1
                if exit_reason is None:
                    vwap_now = float(vwap_ltf.iloc[i]) if not np.isnan(vwap_ltf.iloc[i]) else None
                    if vwap_now is not None and close < vwap_now:
                        exit_reason = "VWAP_FAIL"
                        exit_px = close
                if exit_reason:
                    pnl_pct = trade["realized"] + (exit_px - trade["entry_px"]) / trade["entry_px"] * trade["remaining"]
                    stats["exits"] += 1
                    stats["trades"] += 1
                    stats["mfe_sum"] += trade["mfe"]
                    stats["mae_sum"] += trade["mae"]
                    stats["hold_sum"] += trade["hold_bars"]
                    stats["net_sum"] += pnl_pct
                    sym_stats["exits"] += 1
                    sym_stats["trades"] += 1
                    sym_stats["mfe_sum"] += trade["mfe"]
                    sym_stats["mae_sum"] += trade["mae"]
                    sym_stats["hold_sum"] += trade["hold_bars"]
                    sym_stats["net_sum"] += pnl_pct
                    if pnl_pct > 0:
                        stats["wins"] += 1
                        sym_stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                        sym_stats["losses"] += 1
                    trade = None
                continue

            atr_now = float(atr.iloc[sig_idx]) if not np.isnan(atr.iloc[sig_idx]) else 0.0
            if atr_now <= 0:
                continue

            htf_close_now = float(htf_close.iloc[idx_htf])
            ema_now = float(htf_ema.iloc[idx_htf])
            htf_rsi_now = float(htf_rsi.iloc[idx_htf]) if not np.isnan(htf_rsi.iloc[idx_htf]) else 50.0
            if (
                htf_close_now >= ema_now
                or htf_close_now < ema_now * args.htf_mr_mult
                or htf_rsi_now < args.htf_rsi_allow
            ):
                pass
            else:
                gate_stats["htf_fail"] += 1
                continue

            adx_now = float(adx_ltf.iloc[sig_idx]) if not np.isnan(adx_ltf.iloc[sig_idx]) else 0.0
            adx_prev = float(adx_ltf.iloc[sig_idx - 1]) if sig_idx > 0 and not np.isnan(adx_ltf.iloc[sig_idx - 1]) else adx_now
            if adx_now > args.adx_max and adx_now >= adx_prev:
                gate_stats["adx_fail"] += 1
                continue

            close_now = float(df_ex.at[sig_idx, "close"])
            low_now = float(df_ex.at[sig_idx, "low"])
            high_now = float(df_ex.at[sig_idx, "high"])

            if sig_idx >= args.vp_lookback_bars:
                vp_df = df_ex.iloc[sig_idx - args.vp_lookback_bars + 1 : sig_idx + 1]
                vp = build_volume_profile(
                    vp_df,
                    bin_size_pct=args.vp_bin_pct,
                    bin_size_abs=0.0,
                    value_area_pct=0.7,
                    min_zone_width_pct=0.0,
                    hvn_threshold_ratio=0.5,
                    lvn_threshold_ratio=0.2,
                )
                if vp is not None:
                    vp_levels = {"poc": float(vp.poc.low), "val": float(vp.val), "vah": float(vp.vah)}
            if not vp_levels:
                gate_stats["zone_fail"] += 1
                continue

            val = vp_levels["val"]
            zone_low = val - atr_now * args.zone_atr_mult
            zone_high = val + atr_now * args.zone_atr_mult
            if not zone_active:
                if low_now < val:
                    zone_active = True
                    zone_until = sig_idx + 50
                else:
                    gate_stats["zone_fail"] += 1
                    continue

            if zone_active and sig_idx > zone_until:
                zone_active = False
                absorb_active = False
                absorb_low = None
                continue

            if not absorb_active:
                if sig_idx < args.cvd_lookback:
                    continue
                delta_win = cvd.diff().iloc[sig_idx - args.cvd_lookback + 1 : sig_idx + 1]
                cvd_delta = float(delta_win.sum())
                neg = (-delta_win[delta_win < 0]).astype(float)
                avg_drop = float(neg.mean()) if not neg.empty else 0.0
                cvd_drop = -cvd_delta if cvd_delta < 0 else 0.0
                if not (cvd_delta < 0 and avg_drop > 0 and cvd_drop > avg_drop * args.cvd_drop_mult):
                    gate_stats["cvd_fail"] += 1
                    continue
                absorb_low = low_now
                absorb_active = True
                absorb_until = sig_idx + 15

            if absorb_active:
                if sig_idx > absorb_until:
                    absorb_active = False
                    gate_stats["entry_fail"] += 1
                    continue
                if args.use_oi_filter:
                    if oi_df is None or oi_df.empty:
                        gate_stats["oi_fail"] += 1
                        continue
                    oi_ts = oi_df["ts"].astype(int).to_numpy()
                    oi_idx = _map_idx_by_ts(oi_ts, ts)
                    if oi_idx <= 0:
                        gate_stats["oi_fail"] += 1
                        continue
                    oi_now = float(oi_df["oi"].iloc[oi_idx])
                    oi_prev = float(oi_df["oi"].iloc[max(0, oi_idx - args.oi_lookback)])
                    if oi_now < oi_prev + args.oi_rise_min:
                        gate_stats["oi_fail"] += 1
                        continue
                vwap_now = float(vwap_ltf.iloc[sig_idx]) if not np.isnan(vwap_ltf.iloc[sig_idx]) else None
                vol_mean20 = float(df_ex["volume"].rolling(20).mean().iloc[sig_idx]) if sig_idx >= 20 else 0.0
                lower_wick = min(df_ex.at[sig_idx, "open"], close_now) - low_now
                rng = max(1e-9, high_now - low_now)
                lower_wick_ratio = lower_wick / rng
                hl_ok = False
                if sig_idx >= 3:
                    low1 = float(df_ex.at[sig_idx - 1, "low"])
                    low2 = float(df_ex.at[sig_idx - 2, "low"])
                    low3 = float(df_ex.at[sig_idx - 3, "low"])
                    hl_ok = low3 < low2 < low1
                pinbar_ok = (
                    close_now > df_ex.at[sig_idx, "open"]
                    and lower_wick_ratio >= args.pinbar_wick_ratio
                    and vol_mean20 > 0
                    and float(df_ex.at[sig_idx, "volume"]) >= vol_mean20 * args.pinbar_vol_mult
                )
                vol_spike_ok = vol_mean20 > 0 and float(df_ex.at[sig_idx, "volume"]) >= vol_mean20 * args.vol_spike_mult
                if not vol_spike_ok:
                    gate_stats["entry_fail"] += 1
                    continue
                if vwap_now is None or close_now <= vwap_now * args.vwap_eps:
                    if not pinbar_ok and not hl_ok:
                        gate_stats["vwap_fail"] += 1
                        continue
                recent_high = max(
                    float(df_ex.at[sig_idx - 1, "high"]),
                    float(df_ex.at[sig_idx - 2, "high"]) if sig_idx >= 2 else float(df_ex.at[sig_idx - 1, "high"]),
                )
                if close_now <= recent_high:
                    gate_stats["momentum_fail"] += 1
                    continue

                entry_px = close_now
                entry_ts = int(ts_ex[sig_idx])
                if entry_ts < eval_start_ms:
                    absorb_active = False
                    continue
                sl_fixed = entry_px * (1 - args.sl_pct)
                sl_struct = (absorb_low - atr_now * args.sl_atr_buffer) if absorb_low is not None else sl_fixed
                sl_price = sl_fixed
                risk = max(1e-9, entry_px - sl_price)
                tp1_price = entry_px * (1 + args.tp1_pct)
                tp2_price = entry_px * (1 + args.tp2_pct)
                entry_size = args.early_entry_size if (pinbar_ok and (vwap_now is None or close_now <= vwap_now * args.vwap_eps)) else 1.0
                tp1_size = args.tp1_size * entry_size
                tp2_size = args.tp2_size * entry_size
                trail_size = max(0.0, entry_size - tp1_size - tp2_size)
                trade = {
                    "entry_px": entry_px,
                    "entry_ts": entry_ts,
                    "sl_price": sl_price,
                    "tp1_price": tp1_price,
                    "tp2_price": tp2_price,
                    "tp1_done": False,
                    "tp2_done": False,
                    "remaining": entry_size,
                    "risk": risk,
                    "atr_entry": atr_now,
                    "max_high": entry_px,
                    "hold_bars": 0,
                    "mfe": 0.0,
                    "mae": 0.0,
                    "realized": 0.0,
                    "tp1_size": tp1_size,
                    "tp2_size": tp2_size,
                    "trail_active": False,
                    "trail_size": trail_size,
                    "be_done": False,
                    "last_lows": [],
                }
                stats["entries"] += 1
                sym_stats["entries"] += 1
                absorb_active = False
                zone_active = False

        trades = sym_stats["trades"]
        wins = sym_stats["wins"]
        losses = sym_stats["losses"]
        winrate = (wins / trades * 100.0) if trades > 0 else 0.0
        avg_mfe = sym_stats["mfe_sum"] / trades if trades > 0 else 0.0
        avg_mae = sym_stats["mae_sum"] / trades if trades > 0 else 0.0
        avg_hold = sym_stats["hold_sum"] / trades if trades > 0 else 0.0
        net_sum = sym_stats["net_sum"]
        line = (
            f"[BACKTEST] {symbol} entries={sym_stats['entries']} exits={sym_stats['exits']} trades={trades} "
            f"wins={wins} losses={losses} winrate={winrate:.2f}% "
            f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} net_sum={net_sum:.3f}"
        )
        print(line)

    trades = stats["trades"]
    wins = stats["wins"]
    losses = stats["losses"]
    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats["mfe_sum"] / trades if trades > 0 else 0.0
    avg_mae = stats["mae_sum"] / trades if trades > 0 else 0.0
    avg_hold = stats["hold_sum"] / trades if trades > 0 else 0.0
    net_sum = stats["net_sum"]
    line = (
        f"[BACKTEST] TOTAL entries={stats['entries']} exits={stats['exits']} trades={trades} "
        f"wins={wins} losses={losses} winrate={winrate:.2f}% "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} net_sum={net_sum:.3f}"
    )
    print(line)

    gates_line = (
        "[GATES] "
        f"htf_fail={gate_stats['htf_fail']} "
        f"adx_fail={gate_stats['adx_fail']} "
        f"zone_fail={gate_stats['zone_fail']} "
        f"cvd_fail={gate_stats['cvd_fail']} "
        f"oi_fail={gate_stats['oi_fail']} "
        f"vwap_fail={gate_stats['vwap_fail']} "
        f"momentum_fail={gate_stats['momentum_fail']} "
        f"entry_fail={gate_stats['entry_fail']} "
        f"time_fail={gate_stats['time_fail']}"
    )
    print(gates_line)


if __name__ == "__main__":
    main()
