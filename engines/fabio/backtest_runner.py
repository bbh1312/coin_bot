import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import ccxt

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import cycle_cache
import engine_runner
from engines.fabio import fabio_entry_engine, atlas_fabio_engine
from engines.dtfx.engine import DTFXConfig
from engines.rsi.engine import RsiEngine
from engines.universe import build_universe_from_tickers


def _parse_datetime(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("datetime value is empty")
    if raw.isdigit():
        num = int(raw)
        if num > 1_000_000_000_000:
            return num
        return num * 1000
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        dt = datetime.strptime(raw, "%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _since_ms(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


def _tf_to_ms(tf: str) -> int:
    tf = (tf or "").lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60 * 1000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60 * 1000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60 * 1000
    return 0


def _warmup_start_ms(start_ms: int, tf: str, min_bars: int) -> int:
    tf_ms = _tf_to_ms(tf)
    if tf_ms <= 0:
        return start_ms
    lookback = max(0, int(min_bars) - 1)
    return max(0, start_ms - (lookback * tf_ms))


def _fetch_ohlcv_range(ex, symbol: str, tf: str, since_ms: int, end_ms: int) -> List[list]:
    out: List[list] = []
    tf_ms = _tf_to_ms(tf)
    since = since_ms
    while True:
        batch = ex.fetch_ohlcv(symbol, tf, since=since, limit=1000)
        if not batch:
            break
        out.extend(batch)
        last_ts = batch[-1][0]
        if last_ts >= end_ms:
            break
        if len(batch) < 2:
            break
        since = last_ts + max(tf_ms, 1)
        time.sleep(0.05)
    return [row for row in out if row[0] <= end_ms]


def _get_available_range(ex, symbol: str, tf: str) -> Tuple[Optional[int], Optional[int]]:
    try:
        first = ex.fetch_ohlcv(symbol, tf, since=0, limit=1)
    except Exception:
        first = []
    try:
        last = ex.fetch_ohlcv(symbol, tf, limit=1)
    except Exception:
        last = []
    start_ts = first[0][0] if first else None
    end_ts = last[0][0] if last else None
    return start_ts, end_ts


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _write_signal_header(path: str) -> None:
    if not path:
        return
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts",
                "symbol",
                "side",
                "entry_px",
                "sl_px",
                "tp_px",
                "strength",
                "reasons",
            ]
        )


def _append_signal(path: str, row: List[Any]) -> None:
    if not path:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def _write_trade_header(path: str) -> None:
    if not path:
        return
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "symbol",
                "side",
                "entry_ts",
                "entry_px",
                "exit_ts",
                "exit_px",
                "exit_reason",
                "pnl_pct",
                "pnl_usdt",
                "hold_min",
                "mfe",
                "mae",
                "sl_px",
                "tp_px",
            ]
        )


def _append_trade(path: str, row: List[Any]) -> None:
    if not path:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def _calc_pnl(side: str, entry_px: float, exit_px: float, fee_rate: float, slip_pct: float) -> float:
    side = (side or "").upper()
    if side == "SHORT":
        entry_fill = entry_px * (1 - slip_pct)
        exit_fill = exit_px * (1 + slip_pct)
        fees = entry_fill * fee_rate + exit_fill * fee_rate
        return (entry_fill - exit_fill - fees) / entry_fill if entry_fill > 0 else 0.0
    entry_fill = entry_px * (1 + slip_pct)
    exit_fill = exit_px * (1 - slip_pct)
    fees = entry_fill * fee_rate + exit_fill * fee_rate
    return (exit_fill - entry_fill - fees) / entry_fill if entry_fill > 0 else 0.0


@dataclass
class PendingEntry:
    symbol: str
    side: str
    signal_idx: int
    entry_idx: int
    entry_px: float
    sl_px: float
    tp_px: float
    strength: float
    reasons: List[str]


@dataclass
class TradeState:
    symbol: str
    side: str
    entry_idx: int
    entry_ts: int
    entry_px: float
    sl_px: float
    tp_px: float
    high_max: float
    low_min: float
    strength: float
    reasons: List[str]
    exit_idx: Optional[int] = None
    exit_ts: Optional[int] = None
    exit_px: Optional[float] = None
    exit_reason: Optional[str] = None


def _build_dir_hint(
    symbols: List[str],
    data_by_tf: Dict[Tuple[str, str], List[list]],
    ltf: str,
    mode: str,
    long_syms: Optional[List[str]] = None,
    short_syms: Optional[List[str]] = None,
    verbose: bool = False,
) -> Dict[str, str]:
    hint: Dict[str, str] = {}
    mode = (mode or "auto").lower()
    long_set = set(long_syms or [])
    short_set = set(short_syms or [])
    if long_set or short_set:
        for sym in symbols:
            if sym in long_set:
                hint[sym] = "LONG"
            elif sym in short_set:
                hint[sym] = "SHORT"
        return hint
    if mode == "long":
        return {s: "LONG" for s in symbols}
    if mode == "short":
        return {s: "SHORT" for s in symbols}
    for sym in symbols:
        data = data_by_tf.get((sym, ltf)) or []
        if len(data) < 2:
            hint[sym] = "LONG"
            if verbose:
                print(f"[backtest] {sym} dir_hint fallback=LONG (insufficient {ltf} data)")
            continue
        first = float(data[0][4])
        last = float(data[-1][4])
        hint[sym] = "LONG" if last >= first else "SHORT"
    return hint


def _min_required_bars(atlas_cfg, fabio_cfg) -> Dict[str, int]:
    min_map: Dict[str, int] = {}
    min_map[atlas_cfg.htf_tf] = max(atlas_cfg.supertrend_period + 2, 20) + 1
    min_map[atlas_cfg.ltf_tf] = max(atlas_cfg.atr_period + atlas_cfg.atr_sma_period, 70) + 1
    rsi_min = max(100, int(getattr(fabio_cfg, "rsi_len", 14)) + 2)
    atr_min = max(100, int(getattr(fabio_cfg, "atr_len", 14)) + 2)
    vol_min = max(50, int(getattr(fabio_cfg, "vol_sma_len", 20)) + 1)
    htf_min = max(61, vol_min)
    ltf_min = max(61, rsi_min, atr_min, vol_min, int(getattr(fabio_cfg, "long_retest_window", 12)) + 2)
    min_map[fabio_cfg.timeframe_htf] = max(min_map.get(fabio_cfg.timeframe_htf, 0), htf_min)
    min_map[fabio_cfg.timeframe_ltf] = max(min_map.get(fabio_cfg.timeframe_ltf, 0), ltf_min)
    if getattr(atlas_cfg, "d1_tf", None):
        d1_min = max(int(getattr(atlas_cfg, "d1_ema_len", 7)) + 2, int(getattr(atlas_cfg, "d1_atr_len", 14)) + 2)
        min_map[atlas_cfg.d1_tf] = max(min_map.get(atlas_cfg.d1_tf, 0), d1_min)
    if fabio_cfg.short_timeframe_primary:
        min_map[fabio_cfg.short_timeframe_primary] = max(
            min_map.get(fabio_cfg.short_timeframe_primary, 0), max(rsi_min, atr_min, vol_min, 20)
        )
    if fabio_cfg.short_timeframe_secondary:
        min_map[fabio_cfg.short_timeframe_secondary] = max(
            min_map.get(fabio_cfg.short_timeframe_secondary, 0), rsi_min
        )
    return min_map


def _build_atlasfabio_configs():
    atlas_cfg = atlas_fabio_engine.Config()
    fabio_cfg = fabio_entry_engine.Config()
    fabio_cfg_mid = fabio_entry_engine.Config()

    fabio_cfg.dist_to_ema20_max = engine_runner.ATLASFABIO_STRONG_DIST_MAX
    fabio_cfg.long_dist_to_ema20_max = engine_runner.ATLASFABIO_STRONG_DIST_MAX
    fabio_cfg.pullback_vol_ratio_max = engine_runner.ATLASFABIO_PULLBACK_VOL_MAX
    fabio_cfg.retest_touch_tol = engine_runner.ATLASFABIO_RETEST_TOUCH_TOL

    fabio_cfg_mid.timeframe_ltf = "5m"
    fabio_cfg_mid.dist_to_ema20_max = engine_runner.ATLASFABIO_MID_DIST_MAX
    fabio_cfg_mid.long_dist_to_ema20_max = engine_runner.ATLASFABIO_MID_DIST_MAX
    fabio_cfg_mid.pullback_vol_ratio_max = engine_runner.ATLASFABIO_MID_PULLBACK_VOL_MAX
    fabio_cfg_mid.retest_touch_tol = engine_runner.ATLASFABIO_MID_RETEST_TOUCH_TOL
    fabio_cfg_mid.trigger_vol_ratio_min = engine_runner.ATLASFABIO_MID_VOL_MULT

    return atlas_cfg, fabio_cfg, fabio_cfg_mid


def _run_backtest_for_symbol(
    symbol: str,
    data_by_tf: Dict[Tuple[str, str], List[list]],
    dir_hint: Dict[str, str],
    start_ms: int,
    end_ms: int,
    log_path: str,
    exchange,
    entry_mode: str,
    exit_mode: str,
    risk_pct: float,
    sl_r_mult: float,
    tp_r_mult: float,
    sl_pct: float,
    tp_pct: float,
    timeout_bars: int,
    fee_rate: float,
    slippage_pct: float,
    position_size_usdt: float,
    trade_cooldown_bars: int,
    out_signals: str,
    out_trades: str,
    verbose: bool,
) -> Optional[Dict[str, Any]]:
    atlas_cfg, fabio_cfg, fabio_cfg_mid = _build_atlasfabio_configs()
    ltf = fabio_cfg.timeframe_ltf
    ltf_data = data_by_tf.get((symbol, ltf)) or []
    if not ltf_data:
        if verbose:
            print(f"[backtest] {symbol} skip: missing {ltf} data")
        return None

    min_required = _min_required_bars(atlas_cfg, fabio_cfg)
    tfs = list({atlas_cfg.htf_tf, atlas_cfg.ltf_tf, fabio_cfg.timeframe_htf, fabio_cfg.timeframe_ltf,
                fabio_cfg.short_timeframe_primary, fabio_cfg.short_timeframe_secondary, getattr(atlas_cfg, "d1_tf", None)})
    tfs = [tf for tf in tfs if tf]

    idx_by_tf: Dict[str, int] = {tf: 0 for tf in tfs}
    state: Dict[str, dict] = {
        "_atlasfabio_funnel_log_path": log_path,
        "_atlasfabio_confirm_entry_mode": "confirm_only",
        "_atlasfabio_backtest_mode": True,
        "_atlasfabio_backtest_quiet": True,
    }
    cached_ex = engine_runner.CachedExchange(exchange)
    def _log(msg: str) -> None:
        try:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
        if verbose:
            print(msg)

    cycles_run = 0
    pending: Optional[PendingEntry] = None
    trade: Optional[TradeState] = None
    cooldown_until = -10**9
    ltf_ms = _tf_to_ms(fabio_cfg.timeframe_ltf)
    ltf_minutes = (ltf_ms / 60000.0) if ltf_ms > 0 else 0.0
    entries: List[Dict[str, Any]] = []
    trade_stats: Dict[str, float] = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "long_wins": 0,
        "long_losses": 0,
        "short_wins": 0,
        "short_losses": 0,
        "timeouts": 0,
        "tp": 0,
        "sl": 0,
        "mfe_sum": 0.0,
        "mae_sum": 0.0,
        "hold_sum": 0.0,
        "skipped_in_pos": 0,
        "skipped_cooldown": 0,
        "skipped_no_next": 0,
    }

    for idx, row in enumerate(ltf_data):
        ts = int(row[0])
        if ts < start_ms or ts > end_ms:
            continue
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        st = state.get(symbol, {})
        if isinstance(st, dict):
            st["in_pos"] = trade is not None
            if trade is not None:
                st["last_entry"] = trade.entry_ts / 1000.0
            state[symbol] = st

        if trade is not None:
            trade.high_max = max(trade.high_max, h)
            trade.low_min = min(trade.low_min, l)
            sl_hit = False
            tp_hit = False
            if trade.side == "SHORT":
                sl_hit = h >= trade.sl_px
                tp_hit = l <= trade.tp_px
            else:
                sl_hit = l <= trade.sl_px
                tp_hit = h >= trade.tp_px
            exit_reason = None
            exit_px = None
            if sl_hit and tp_hit:
                exit_reason = "SL"
                exit_px = trade.sl_px
            elif sl_hit:
                exit_reason = "SL"
                exit_px = trade.sl_px
            elif tp_hit:
                exit_reason = "TP"
                exit_px = trade.tp_px
            elif timeout_bars > 0 and (idx - trade.entry_idx) >= timeout_bars:
                exit_reason = "TIMEOUT"
                exit_px = c
            if exit_reason:
                trade.exit_idx = idx
                trade.exit_ts = ts
                trade.exit_px = float(exit_px)
                trade.exit_reason = exit_reason
                pnl_pct = _calc_pnl(trade.side, trade.entry_px, trade.exit_px, fee_rate, slippage_pct)
                pnl_usdt = pnl_pct * position_size_usdt
                holding_bars = idx - trade.entry_idx + 1
                hold_min = holding_bars * ltf_minutes
                mfe = (
                    (trade.entry_px - trade.low_min) / trade.entry_px
                    if trade.side == "SHORT"
                    else (trade.high_max - trade.entry_px) / trade.entry_px
                )
                mae = (
                    (trade.high_max - trade.entry_px) / trade.entry_px
                    if trade.side == "SHORT"
                    else (trade.entry_px - trade.low_min) / trade.entry_px
                )
                _log(
                    "ATLASFABIO_TRADE_EXIT "
                    f"sym={symbol} side={trade.side} reason={trade.exit_reason} exit_px={trade.exit_px} "
                    f"exit_ts={trade.exit_ts} pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} "
                    f"mae={mae:.4f} hold={hold_min:.1f}"
                )
                trade_stats["trades"] += 1
                if exit_reason == "TIMEOUT":
                    trade_stats["timeouts"] += 1
                if exit_reason == "TP":
                    trade_stats["tp"] += 1
                elif exit_reason == "SL":
                    trade_stats["sl"] += 1
                if pnl_pct >= 0:
                    trade_stats["wins"] += 1
                    if trade.side == "LONG":
                        trade_stats["long_wins"] += 1
                    else:
                        trade_stats["short_wins"] += 1
                else:
                    trade_stats["losses"] += 1
                    if trade.side == "LONG":
                        trade_stats["long_losses"] += 1
                    else:
                        trade_stats["short_losses"] += 1
                trade_stats["mfe_sum"] += mfe
                trade_stats["mae_sum"] += mae
                trade_stats["hold_sum"] += hold_min
                entries.append(
                    {
                        "symbol": symbol,
                        "side": trade.side,
                        "entry_ts": trade.entry_ts,
                        "exit_ts": trade.exit_ts,
                        "exit_reason": trade.exit_reason,
                        "pnl_pct": pnl_pct,
                        "hold_min": hold_min,
                    }
                )
                _append_trade(
                    out_trades,
                    [
                        symbol,
                        trade.side,
                        trade.entry_ts,
                        f"{trade.entry_px:.6g}",
                        trade.exit_ts,
                        f"{trade.exit_px:.6g}",
                        trade.exit_reason,
                        f"{pnl_pct:.6f}",
                        f"{pnl_usdt:.6f}",
                        f"{hold_min:.1f}",
                        f"{mfe:.6f}",
                        f"{mae:.6f}",
                        f"{trade.sl_px:.6g}",
                        f"{trade.tp_px:.6g}",
                    ],
                )
                trade = None
                if isinstance(st, dict):
                    st["in_pos"] = False
                    state[symbol] = st
                cooldown_until = idx + max(0, int(trade_cooldown_bars))

        if pending is not None and idx == pending.entry_idx:
            if trade is None:
                entry_px = o if entry_mode == "NEXT_OPEN" else pending.entry_px
                if exit_mode == "R_MULT":
                    if pending.side == "SHORT":
                        sl_px = entry_px * (1 + risk_pct * sl_r_mult)
                        tp_px = entry_px * (1 - risk_pct * tp_r_mult)
                    else:
                        sl_px = entry_px * (1 - risk_pct * sl_r_mult)
                        tp_px = entry_px * (1 + risk_pct * tp_r_mult)
                else:
                    if pending.side == "SHORT":
                        sl_px = entry_px * (1 + sl_pct)
                        tp_px = entry_px * (1 - tp_pct)
                    else:
                        sl_px = entry_px * (1 - sl_pct)
                        tp_px = entry_px * (1 + tp_pct)
                trade = TradeState(
                    symbol=symbol,
                    side=pending.side,
                    entry_idx=idx,
                    entry_ts=ts,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp_px=tp_px,
                    high_max=h,
                    low_min=l,
                    strength=pending.strength,
                    reasons=pending.reasons,
                )
                if isinstance(st, dict):
                    st["in_pos"] = True
                    st["last_entry"] = ts / 1000.0
                    state[symbol] = st
                _log(
                    "ATLASFABIO_TRADE_ENTRY "
                    f"sym={symbol} side={trade.side} entry_px={trade.entry_px} entry_ts={trade.entry_ts} "
                    f"sl={trade.sl_px} tp={trade.tp_px}"
                )
            pending = None

        entry_signal: Optional[Dict[str, Any]] = None

        def _on_entry_signal(**payload):
            nonlocal entry_signal
            entry_signal = payload

        for tf in tfs:
            data = data_by_tf.get((symbol, tf)) or []
            tf_idx = idx_by_tf.get(tf, 0)
            while tf_idx < len(data) and int(data[tf_idx][0]) <= ts:
                tf_idx += 1
            idx_by_tf[tf] = tf_idx
            cycle_cache.set_raw(symbol, tf, data[:tf_idx])

        if any(idx_by_tf.get(tf, 0) < min_required.get(tf, 0) for tf in tfs):
            continue

        cycle_cache.clear_cycle_cache(keep_raw=True)
        engine_runner.CURRENT_CYCLE_STATS = {}

        engine_runner._run_atlas_fabio_cycle(
            universe_structure=[symbol],
            cached_ex=cached_ex,
            state=state,
            fabio_cfg=fabio_cfg,
            fabio_cfg_mid=fabio_cfg_mid,
            atlas_cfg=atlas_cfg,
            active_positions_total=0,
            dir_hint=dir_hint,
            send_alert=lambda _: None,
            entry_callback=_on_entry_signal,
            now_ts=ts / 1000.0,
        )
        cycles_run += 1

        if entry_signal:
            side = str(entry_signal.get("side") or "").upper()
            strength = float(entry_signal.get("strength") or 0.0)
            reasons = entry_signal.get("reasons") or []
            if trade is not None or pending is not None:
                trade_stats["skipped_in_pos"] += 1
                _log(f"ATLASFABIO_TRADE_SKIP sym={symbol} reason=in_position")
                continue
            if idx <= cooldown_until:
                trade_stats["skipped_cooldown"] += 1
                _log(f"ATLASFABIO_TRADE_SKIP sym={symbol} reason=cooldown")
                continue
            if entry_mode == "NEXT_OPEN" and idx + 1 >= len(ltf_data):
                trade_stats["skipped_no_next"] += 1
                _log(f"ATLASFABIO_TRADE_SKIP sym={symbol} reason=no_next_bar")
                continue
            entry_px = c
            if exit_mode == "R_MULT":
                if side == "SHORT":
                    sl_px = entry_px * (1 + risk_pct * sl_r_mult)
                    tp_px = entry_px * (1 - risk_pct * tp_r_mult)
                else:
                    sl_px = entry_px * (1 - risk_pct * sl_r_mult)
                    tp_px = entry_px * (1 + risk_pct * tp_r_mult)
            else:
                if side == "SHORT":
                    sl_px = entry_px * (1 + sl_pct)
                    tp_px = entry_px * (1 - tp_pct)
                else:
                    sl_px = entry_px * (1 - sl_pct)
                    tp_px = entry_px * (1 + tp_pct)
            _append_signal(
                out_signals,
                [
                    ts,
                    symbol,
                    side,
                    f"{entry_px:.6g}",
                    f"{sl_px:.6g}",
                    f"{tp_px:.6g}",
                    f"{strength:.4f}",
                    ",".join(reasons),
                ],
            )
            if entry_mode == "SIGNAL_CLOSE":
                trade = TradeState(
                    symbol=symbol,
                    side=side,
                    entry_idx=idx,
                    entry_ts=ts,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp_px=tp_px,
                    high_max=h,
                    low_min=l,
                    strength=strength,
                    reasons=reasons,
                )
                if isinstance(st, dict):
                    st["in_pos"] = True
                    st["last_entry"] = ts / 1000.0
                    state[symbol] = st
                _log(
                    "ATLASFABIO_TRADE_ENTRY "
                    f"sym={symbol} side={trade.side} entry_px={trade.entry_px} entry_ts={trade.entry_ts} "
                    f"sl={trade.sl_px} tp={trade.tp_px}"
                )
            else:
                pending = PendingEntry(
                    symbol=symbol,
                    side=side,
                    signal_idx=idx,
                    entry_idx=idx + 1,
                    entry_px=entry_px,
                    sl_px=sl_px,
                    tp_px=tp_px,
                    strength=strength,
                    reasons=reasons,
                )
    if cycles_run == 0:
        if verbose:
            print(f"[backtest] {symbol} skip: insufficient warmup data in range")
    if trade is not None:
        last_idx = len(ltf_data) - 1
        last_ts = int(ltf_data[last_idx][0])
        last_close = float(ltf_data[last_idx][4])
        trade.exit_idx = last_idx
        trade.exit_ts = last_ts
        trade.exit_px = last_close
        trade.exit_reason = "EOT"
        pnl_pct = _calc_pnl(trade.side, trade.entry_px, last_close, fee_rate, slippage_pct)
        pnl_usdt = pnl_pct * position_size_usdt
        holding_bars = last_idx - trade.entry_idx + 1
        hold_min = holding_bars * ltf_minutes
        mfe = (
            (trade.entry_px - trade.low_min) / trade.entry_px
            if trade.side == "SHORT"
            else (trade.high_max - trade.entry_px) / trade.entry_px
        )
        mae = (
            (trade.high_max - trade.entry_px) / trade.entry_px
            if trade.side == "SHORT"
            else (trade.entry_px - trade.low_min) / trade.entry_px
        )
        _log(
            "ATLASFABIO_TRADE_EXIT "
            f"sym={symbol} side={trade.side} reason=EOT exit_px={last_close} exit_ts={last_ts} "
            f"pnl_pct={pnl_pct:.4f} mfe={mfe:.4f} mae={mae:.4f} hold={hold_min:.1f}"
        )
        trade_stats["trades"] += 1
        if pnl_pct >= 0:
            trade_stats["wins"] += 1
            if trade.side == "LONG":
                trade_stats["long_wins"] += 1
            else:
                trade_stats["short_wins"] += 1
        else:
            trade_stats["losses"] += 1
            if trade.side == "LONG":
                trade_stats["long_losses"] += 1
            else:
                trade_stats["short_losses"] += 1
        trade_stats["mfe_sum"] += mfe
        trade_stats["mae_sum"] += mae
        trade_stats["hold_sum"] += hold_min
        entries.append(
            {
                "symbol": symbol,
                "side": trade.side,
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "exit_reason": trade.exit_reason,
                "pnl_pct": pnl_pct,
                "hold_min": hold_min,
            }
        )
        _append_trade(
            out_trades,
            [
                symbol,
                trade.side,
                trade.entry_ts,
                f"{trade.entry_px:.6g}",
                trade.exit_ts,
                f"{trade.exit_px:.6g}",
                trade.exit_reason,
                f"{pnl_pct:.6f}",
                f"{pnl_usdt:.6f}",
                f"{hold_min:.1f}",
                f"{mfe:.6f}",
                f"{mae:.6f}",
                f"{trade.sl_px:.6g}",
                f"{trade.tp_px:.6g}",
            ],
        )
        trade = None
    win_rate = (trade_stats["wins"] / trade_stats["trades"]) if trade_stats["trades"] else 0.0
    avg_mfe = (trade_stats["mfe_sum"] / trade_stats["trades"]) if trade_stats["trades"] else 0.0
    avg_mae = (trade_stats["mae_sum"] / trade_stats["trades"]) if trade_stats["trades"] else 0.0
    avg_hold = (trade_stats["hold_sum"] / trade_stats["trades"]) if trade_stats["trades"] else 0.0
    _log(
        "[atlasfabio-trades] "
        f"sym={symbol} trades={trade_stats['trades']} wins={trade_stats['wins']} "
        f"losses={trade_stats['losses']} timeouts={trade_stats['timeouts']} "
        f"long_wins={trade_stats['long_wins']} long_losses={trade_stats['long_losses']} "
        f"short_wins={trade_stats['short_wins']} short_losses={trade_stats['short_losses']} "
        f"win_rate={win_rate:.4f} skip_in_pos={trade_stats['skipped_in_pos']} "
        f"skip_cooldown={trade_stats['skipped_cooldown']} skip_no_next={trade_stats['skipped_no_next']}"
    )
    return {
        "symbol": symbol,
        "trades": trade_stats["trades"],
        "wins": trade_stats["wins"],
        "losses": trade_stats["losses"],
        "long_wins": trade_stats["long_wins"],
        "long_losses": trade_stats["long_losses"],
        "short_wins": trade_stats["short_wins"],
        "short_losses": trade_stats["short_losses"],
        "timeouts": trade_stats["timeouts"],
        "tp": trade_stats["tp"],
        "sl": trade_stats["sl"],
        "win_rate": win_rate,
        "avg_mfe": avg_mfe,
        "avg_mae": avg_mae,
        "avg_hold": avg_hold,
        "entries": entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="AtlasFabio backtest runner")
    parser.add_argument("--symbols", default="", help="comma-separated symbols")
    parser.add_argument("--max-symbols", type=int, default=7)
    parser.add_argument("--days", type=int, default=7, help="lookback days from now")
    parser.add_argument("--direction", default="auto", choices=("auto", "long", "short"))
    parser.add_argument("--long", default="", help="comma-separated long symbols override")
    parser.add_argument("--short", default="", help="comma-separated short symbols override")
    parser.add_argument("--log-file", default="backtest.log", help="backtest funnel log path")
    parser.add_argument("--out-signals", default="atlasfabio_signals.csv")
    parser.add_argument("--out-trades", default="atlasfabio_trades.csv")
    parser.add_argument("--entry-mode", default="NEXT_OPEN", choices=("NEXT_OPEN", "SIGNAL_CLOSE"))
    parser.add_argument("--exit-mode", default="PCT", choices=("R_MULT", "PCT"))
    parser.add_argument("--risk-pct", type=float, default=0.01)
    parser.add_argument("--sl-r-mult", type=float, default=2.0)
    parser.add_argument("--tp-r-mult", type=float, default=2.0)
    parser.add_argument("--sl-pct", type=float, default=0.02)
    parser.add_argument("--tp-pct", type=float, default=0.02)
    parser.add_argument("--timeout-bars", type=int, default=32)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--slippage-pct", type=float, default=0.0)
    parser.add_argument("--position-size-usdt", type=float, default=100.0)
    parser.add_argument("--trade-cooldown-bars", type=int, default=12)
    parser.add_argument("--auto-range", action="store_true", help="auto clamp to available data range")
    parser.add_argument("--verbose", action="store_true", help="print debug logs")
    args = parser.parse_args()
    if engine_runner.rsi_engine is None:
        engine_runner.rsi_engine = RsiEngine()

    end_ms = _now_ms()
    start_ms = _since_ms(args.days)
    if end_ms <= start_ms:
        raise SystemExit("invalid days range")

    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        tickers = exchange.fetch_tickers()
        dtfx_cfg = DTFXConfig()
        anchors = []
        for s in dtfx_cfg.anchor_symbols or []:
            anchors.append(s if "/" in s else f"{s}/USDT:USDT")
        min_qv = max(dtfx_cfg.min_quote_volume_usdt, dtfx_cfg.low_liquidity_qv_usdt)
        symbols = build_universe_from_tickers(
            tickers,
            symbols=list(tickers.keys()),
            min_quote_volume_usdt=min_qv,
            top_n=dtfx_cfg.universe_top_n,
            anchors=tuple(anchors),
        )
    if isinstance(args.max_symbols, int) and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    if not symbols:
        raise SystemExit("symbols is required")

    base_dir = os.path.join("logs", "fabio", "backtest")
    os.makedirs(base_dir, exist_ok=True)
    log_path = os.path.join(base_dir, os.path.basename(args.log_file.strip() or "backtest.log"))
    out_signals = os.path.join(base_dir, os.path.basename(args.out_signals))
    out_trades = os.path.join(base_dir, os.path.basename(args.out_trades))
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[backtest] days={args.days} start_ms={start_ms} end_ms={end_ms} symbols={symbols}\n")
    _write_signal_header(out_signals)
    _write_trade_header(out_trades)

    cycle_cache.set_fetcher(None)

    engine_runner.ATLAS_FABIO_ENABLED = True
    engine_runner.ATLAS_FABIO_PAPER = True
    engine_runner.LIVE_TRADING = False
    engine_runner.LONG_LIVE_TRADING = False
    engine_runner.count_open_positions = lambda force=False: 0

    atlas_cfg, fabio_cfg, _ = _build_atlasfabio_configs()
    tfs = list({atlas_cfg.htf_tf, atlas_cfg.ltf_tf, fabio_cfg.timeframe_htf, fabio_cfg.timeframe_ltf,
                fabio_cfg.short_timeframe_primary, fabio_cfg.short_timeframe_secondary, getattr(atlas_cfg, "d1_tf", None)})
    tfs = [tf for tf in tfs if tf]

    min_required = _min_required_bars(atlas_cfg, fabio_cfg)
    data_by_tf: Dict[Tuple[str, str], List[list]] = {}
    for sym in symbols:
        for tf in tfs:
            fetch_start = _warmup_start_ms(start_ms, tf, min_required.get(tf, 0))
            if args.verbose:
                print(f"[backtest] fetching {sym} {tf}")
            data_by_tf[(sym, tf)] = _fetch_ohlcv_range(exchange, sym, tf, fetch_start, end_ms)
        ltf = fabio_cfg.timeframe_ltf
        ltf_data = data_by_tf.get((sym, ltf)) or []
        if not ltf_data:
            avail_start, avail_end = _get_available_range(exchange, sym, ltf)
            if avail_start and avail_end:
                start_str = datetime.fromtimestamp(avail_start / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                end_str = datetime.fromtimestamp(avail_end / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                if args.verbose:
                    print(f"[backtest] {sym} {ltf} available={start_str}~{end_str}")
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(f"[backtest] {sym} {ltf} available={start_str}~{end_str}\n")
                if args.auto_range:
                    new_start = max(start_ms, avail_start)
                    new_end = min(end_ms, avail_end)
                    if new_end > new_start:
                        if args.verbose:
                            print(f"[backtest] {sym} auto-range applied")
                        for tf in tfs:
                            fetch_start = _warmup_start_ms(new_start, tf, min_required.get(tf, 0))
                            data_by_tf[(sym, tf)] = _fetch_ohlcv_range(
                                exchange, sym, tf, fetch_start, new_end
                            )
                    else:
                        if args.verbose:
                            print(f"[backtest] {sym} auto-range skipped: no overlap")
            else:
                if args.verbose:
                    print(f"[backtest] {sym} {ltf} available=unknown")
        for tf in tfs:
            data = data_by_tf.get((sym, tf)) or []
            req = min_required.get(tf, 0)
            if args.verbose:
                print(f"[backtest] {sym} {tf} bars={len(data)} min_required={req}")

    dir_hint = _build_dir_hint(
        symbols,
        data_by_tf,
        fabio_cfg.timeframe_ltf,
        args.direction,
        long_syms=[s.strip() for s in args.long.split(",") if s.strip()],
        short_syms=[s.strip() for s in args.short.split(",") if s.strip()],
        verbose=args.verbose,
    )

    summary_rows: List[Dict[str, Any]] = []
    for sym in symbols:
        hint = dir_hint.get(sym)
        if not hint:
            if args.verbose:
                print(f"[backtest] {sym} skip: dir_hint missing")
            continue
        row = _run_backtest_for_symbol(
            sym,
            data_by_tf,
            dir_hint={sym: hint},
            start_ms=start_ms,
            end_ms=end_ms,
            log_path=log_path,
            exchange=exchange,
            entry_mode=args.entry_mode,
            exit_mode=args.exit_mode,
            risk_pct=args.risk_pct,
            sl_r_mult=args.sl_r_mult,
            tp_r_mult=args.tp_r_mult,
            sl_pct=args.sl_pct,
            tp_pct=args.tp_pct,
            timeout_bars=args.timeout_bars,
            fee_rate=args.fee_rate,
            slippage_pct=args.slippage_pct,
            position_size_usdt=args.position_size_usdt,
            trade_cooldown_bars=args.trade_cooldown_bars,
            out_signals=out_signals,
            out_trades=out_trades,
            verbose=args.verbose,
        )
        if row:
            summary_rows.append(row)

    if summary_rows:
        summary_rows.sort(key=lambda r: (r.get("win_rate") or 0.0), reverse=True)
        total_trades = 0
        total_wins = 0
        total_losses = 0
        total_tp = 0
        total_sl = 0
        total_mfe = 0.0
        total_mae = 0.0
        total_hold = 0.0
        total_long_wins = 0
        total_long_losses = 0
        total_short_wins = 0
        total_short_losses = 0
        for row in summary_rows:
            trades = int(row.get("trades") or 0)
            wins = int(row.get("wins") or 0)
            losses = int(row.get("losses") or 0)
            tp = int(row.get("tp") or 0)
            sl = int(row.get("sl") or 0)
            long_wins = int(row.get("long_wins") or 0)
            long_losses = int(row.get("long_losses") or 0)
            short_wins = int(row.get("short_wins") or 0)
            short_losses = int(row.get("short_losses") or 0)
            win_rate = (wins / trades * 100.0) if trades else 0.0
            avg_mfe = float(row.get("avg_mfe") or 0.0)
            avg_mae = float(row.get("avg_mae") or 0.0)
            avg_hold = float(row.get("avg_hold") or 0.0)
            total_trades += trades
            total_wins += wins
            total_losses += losses
            total_tp += tp
            total_sl += sl
            total_mfe += avg_mfe * trades
            total_mae += avg_mae * trades
            total_hold += avg_hold * trades
            total_long_wins += long_wins
            total_long_losses += long_losses
            total_short_wins += short_wins
            total_short_losses += short_losses
            print(
                "[BACKTEST] %s trades=%d wins=%d losses=%d winrate=%.2f%% tp=%d sl=%d "
                "avg_mfe=%.4f avg_mae=%.4f avg_hold=%.1f "
                "long_wins=%d long_losses=%d short_wins=%d short_losses=%d"
                % (
                    row.get("symbol"),
                    trades,
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
            for entry in row.get("entries") or []:
                pnl_pct = float(entry.get("pnl_pct") or 0.0)
                outcome = "WIN" if pnl_pct > 0 else "LOSS"
                print(
                    "[ENTRY] sym=%s side=%s outcome=%s entry_ts=%s exit_ts=%s reason=%s pnl_pct=%.4f hold_min=%.1f"
                    % (
                        entry.get("symbol"),
                        entry.get("side"),
                        outcome,
                        entry.get("entry_ts"),
                        entry.get("exit_ts"),
                        entry.get("exit_reason"),
                        pnl_pct,
                        float(entry.get("hold_min") or 0.0),
                    )
                )
        total_win_rate = (total_wins / total_trades * 100.0) if total_trades else 0.0
        total_avg_mfe = (total_mfe / total_trades) if total_trades else 0.0
        total_avg_mae = (total_mae / total_trades) if total_trades else 0.0
        total_avg_hold = (total_hold / total_trades) if total_trades else 0.0
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
                total_long_wins,
                total_long_losses,
                total_short_wins,
                total_short_losses,
            )
        )


if __name__ == "__main__":
    main()
