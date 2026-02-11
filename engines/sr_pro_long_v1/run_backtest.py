from __future__ import annotations

import argparse
import json
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engines.backtest_common import calc_warmup_window, load_common_universe, log_warmup_info
from engines.sr_pro_long_v1.engine import SrProLongV1Config
from engines.sr_pro_common import build_sr_zones


def _read_cached_csv(path: str) -> List[List[float]]:
    rows: List[List[float]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
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


def _write_cached_csv(path: str, rows: List[List[float]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("ts,open,high,low,close,volume\n")
        for row in rows:
            if not row or len(row) < 6:
                continue
            f.write(
                f"{int(row[0])},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}\n"
            )


def _fetch_ohlcv_all(
    exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    cache_only: bool = False,
    use_common_warmup: bool = False,
    common_warmup_dir: str | None = None,
    common_only: bool = False,
) -> List[List[float]]:
    if use_common_warmup and common_warmup_dir:
        fname = symbol.replace("/", "_").replace(":", "_")
        cached_path = os.path.join(common_warmup_dir, f"{fname}_{timeframe}.csv")
        if not os.path.exists(cached_path):
            # allow common_ohlcv_cache files with limit suffix (e.g. *_1h_480.csv)
            # prefer the largest fresh cache (match live window), otherwise newest
            try:
                prefix = f"{fname}_{timeframe}_"
                candidates = []
                tf_ms = _tf_to_minutes(timeframe) * 60 * 1000
                for name in os.listdir(common_warmup_dir):
                    if not name.startswith(prefix) or not name.endswith(".csv"):
                        continue
                    full = os.path.join(common_warmup_dir, name)
                    try:
                        mtime = os.path.getmtime(full)
                        lim = int(name[len(prefix):-4])
                        # read last ts cheaply (last line)
                        last_ts = 0
                        with open(full, "rb") as fh:
                            try:
                                fh.seek(-2, os.SEEK_END)
                                while fh.read(1) != b"\n":
                                    fh.seek(-2, os.SEEK_CUR)
                            except Exception:
                                fh.seek(0)
                            last_line = fh.readline().decode("utf-8").strip()
                        if last_line:
                            try:
                                last_ts = int(float(last_line.split(",")[0]))
                            except Exception:
                                last_ts = 0
                    except Exception:
                        continue
                    candidates.append((lim, last_ts, mtime, full))
                if candidates:
                    # prefer largest limit with fresh last_ts near end_ms
                    fresh = [c for c in candidates if c[1] and c[1] >= (end_ms - max(tf_ms * 2, 1))]
                    if fresh:
                        fresh.sort(key=lambda x: (x[0], x[1]), reverse=True)
                        cached_path = fresh[0][3]
                    else:
                        candidates.sort(key=lambda x: x[2], reverse=True)
                        cached_path = candidates[0][3]
            except Exception:
                pass
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
        last = batch[-1][0]
        if last == since:
            break
        since = last + 1
    return [r for r in out if start_ms <= r[0] <= end_ms]


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _tf_to_minutes(tf: str) -> int:
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    return 1


def _ensure_common_cache(
    exchange,
    symbols: List[str],
    timeframes: List[str],
    start_ms: int,
    end_ms: int,
    common_warmup_dir: str,
) -> None:
    if exchange is None:
        return
    for sym in symbols:
        fname = sym.replace("/", "_").replace(":", "_")
        for tf in timeframes:
            cached_path = os.path.join(common_warmup_dir, f"{fname}_{tf}.csv")
            rows = _read_cached_csv(cached_path) if os.path.exists(cached_path) else []
            if rows:
                ts_min = min(r[0] for r in rows)
                ts_max = max(r[0] for r in rows)
                if ts_min <= start_ms and ts_max >= end_ms:
                    continue
            fetched = _fetch_ohlcv_all(
                exchange,
                sym,
                tf,
                start_ms,
                end_ms,
                cache_only=False,
                use_common_warmup=False,
                common_warmup_dir=None,
                common_only=False,
            )
            if fetched:
                _write_cached_csv(cached_path, fetched)


def _ts_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt + pd.Timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")


def _minute_str(ts_ms: int) -> str:
    return _ts_kst(ts_ms)


def _fmt_summary_line(
    symbol: Optional[str],
    stats: Dict[str, float],
    base_usdt: float,
    last_day_exits: int,
    entry_syms: int,
) -> str:
    trades = int(stats.get("trades", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    entries = int(stats.get("entries", 0))
    exits = int(stats.get("exits", 0))
    tp = int(stats.get("tp", wins))
    sl = int(stats.get("sl", losses))
    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats.get("mfe_sum", 0.0) / trades if trades > 0 else 0.0
    avg_mae = stats.get("mae_sum", 0.0) / trades if trades > 0 else 0.0
    avg_hold = stats.get("hold_sum", 0.0) / trades if trades > 0 else 0.0
    tag = "TOTAL" if symbol is None else symbol
    return (
        f"[BACKTEST] {tag} entries={entries} exits={exits} trades={trades} "
        f"wins={wins} losses={losses} winrate={winrate:.2f}% "
        f"tp={tp} sl={sl} avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} "
        f"avg_hold={avg_hold:.1f} last_day_exits={last_day_exits} "
        f"base_usdt={base_usdt:.2f} tp_sum={stats.get('tp_sum', 0.0):.3f} "
        f"sl_sum={stats.get('sl_sum', 0.0):.3f} net_sum={stats.get('net_sum', 0.0):.3f} "
        f"net_sum_usdt={stats.get('net_sum_usdt', 0.0):.3f} entry_syms={entry_syms}"
    )


def _dow_label(dt: datetime) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]


@dataclass
class Zone:
    mid: float
    top: float
    bot: float
    side: int
    live: bool
    born: int
    start: int
    vol: float


def run_backtest() -> None:
    parser = argparse.ArgumentParser("sr_pro_long_v1 backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--use-live-cache", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--auto-fill-cache", action="store_true", default=False)
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--relaxed-lookback", type=int, default=10)
    parser.add_argument("--auto-relax", action="store_true")
    parser.add_argument("--atr-mult", type=float, default=1.0)
    parser.add_argument("--delta-len", type=int, default=2)
    parser.add_argument("--cluster-atr", type=float, default=1.5)
    parser.add_argument("--max-zones-per-side", type=int, default=8)
    parser.add_argument("--touch-mode", type=str, default="top", choices=["top", "mid"])
    parser.add_argument("--touch-use-close", action="store_true")
    parser.add_argument("--dvf-norm-min", type=float, default=0.2)
    parser.add_argument("--require-reject-close", action="store_true")
    parser.add_argument("--reject-mode", type=str, default="top", choices=["top", "mid"])
    parser.add_argument("--reject-source", type=str, default="1h", choices=["1h", "15m"])
    parser.add_argument("--ema200-filter", action="store_true", default=True)
    parser.add_argument("--no-ema200-filter", action="store_false", dest="ema200_filter")
    parser.add_argument("--ema-filter-len", type=int, default=200)
    parser.add_argument("--retest-bars", type=int, default=6)
    parser.add_argument("--retest-atr-mult", type=float, default=0.25)
    parser.add_argument("--retest-near-atr-mult", type=float, default=0.15)
    parser.add_argument("--retest-wick-max", type=float, default=0.4)
    parser.add_argument("--retest-dyn", action="store_true")
    parser.add_argument("--retest-dyn-th", type=float, default=0.6)
    parser.add_argument("--retest-dyn-bars", type=int, default=10)
    parser.add_argument("--shallow-atr-mult", type=float, default=0.35)
    parser.add_argument("--shallow-wick-max", type=float, default=0.35)
    parser.add_argument("--shallow-dvf-min", type=float, default=0.0)
    parser.add_argument("--entry-ema-len", type=int, default=7)
    parser.add_argument("--entry-atr-offset", type=float, default=0.15)
    parser.add_argument("--sl-buffer", type=float, default=0.01)
    parser.add_argument("--sl-atr-mult", type=float, default=0.4)
    parser.add_argument("--tp-atr-mult", type=float, default=0.0)
    parser.add_argument("--tp-atr-mult-weak", type=float, default=0.0)
    parser.add_argument("--tp-mult", type=float, default=1.015)
    parser.add_argument("--tp-mult-weak", type=float, default=1.015)
    parser.add_argument("--base-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-usdt", type=float, default=10.0)
    parser.add_argument("--freeze-zones", action="store_true")
    parser.add_argument("--total-window-days", type=int, default=14)
    parser.add_argument("--rolling-zones", action="store_true", default=True)
    parser.add_argument("--no-rolling-zones", action="store_false", dest="rolling_zones")
    parser.add_argument("--zones-snapshot-in", type=str, default="")
    parser.add_argument("--zones-snapshot-out", type=str, default="")
    parser.add_argument("--log-gates", action="store_true")
    parser.add_argument("--debug-zone", action="store_true")
    parser.add_argument("--block-hours", type=str, default="")
    args = parser.parse_args()

    cfg = SrProLongV1Config(
        lookback=args.lookback,
        relaxed_lookback=args.relaxed_lookback,
        auto_relax=args.auto_relax,
        atr_mult=args.atr_mult,
        delta_len=args.delta_len,
        cluster_atr=args.cluster_atr,
        max_zones_per_side=args.max_zones_per_side,
        sl_buffer=args.sl_buffer,
        tp_atr_mult=args.tp_atr_mult,
        tp_atr_mult_weak=args.tp_atr_mult_weak,
        tp_mult=args.tp_mult,
        entry_ema_len=args.entry_ema_len,
        entry_atr_offset=args.entry_atr_offset,
    )

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {cfg.tf_ltf: 120, cfg.tf_mtf: 120, cfg.tf_htf: 120}
    if args.total_window_days:
        if args.total_window_days < args.days:
            print("[BACKTEST] total_window_days must be >= days")
            return
        total_window_days = args.total_window_days
        warmup_days = max(0, total_window_days)
        warmup_minutes = warmup_days * 1440
        start_ms = end_ms - int((warmup_days + args.days) * 24 * 60 * 60 * 1000)
        eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    else:
        start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(args.days, end_ms, min_bars)

    if args.rolling_zones and not args.total_window_days:
        print("[BACKTEST] rolling_zones requires total_window_days")
        return

    if args.use_live_cache:
        args.cache_only = True
    if args.common_only:
        args.cache_only = True
    # Prefer live cycle cache for backtest parity when common-only/cache-only is used.
    use_common = bool(args.use_live_cache or args.common_only or args.common_warmup_dir or args.cache_only)
    common_dir = args.common_warmup_dir
    if not common_dir:
        live_cache_dir = os.getenv("COMMON_OHLCV_CACHE_DIR", os.path.join("logs", "common_ohlcv_cache"))
        if os.path.isdir(live_cache_dir) and os.listdir(live_cache_dir):
            common_dir = live_cache_dir
        else:
            common_dir = os.getenv("COMMON_WARMUP_CACHE_DIR", os.path.join("logs", "common_warmup", "ohlcv"))
    if not common_dir:
        common_dir = os.path.join("logs", "common_warmup", "ohlcv")
    universe = load_common_universe(args.universe, exchange, args.cache_only, top_n=args.top_n)
    if not universe:
        print("[BACKTEST] no_universe")
        return

    log_warmup_info(lambda _: None, warmup_days, warmup_minutes, args.days)

    if args.auto_fill_cache and use_common and common_dir:
        cache_ex = exchange
        if cache_ex is None:
            cache_ex = ccxt.binance({"enableRateLimit": True})
        _ensure_common_cache(
            cache_ex,
            universe,
            [cfg.tf_ltf, cfg.tf_mtf, cfg.tf_htf],
            start_ms,
            end_ms,
            common_dir,
        )

    data: Dict[str, Dict[str, pd.DataFrame]] = {}
    for sym in universe:
        rows_3m = _fetch_ohlcv_all(exchange, sym, cfg.tf_ltf, start_ms, end_ms, args.cache_only, use_common, common_dir, args.common_only)
        rows_15m = _fetch_ohlcv_all(exchange, sym, cfg.tf_mtf, start_ms, end_ms, args.cache_only, use_common, common_dir, args.common_only)
        rows_1h = _fetch_ohlcv_all(exchange, sym, cfg.tf_htf, start_ms, end_ms, args.cache_only, use_common, common_dir, args.common_only)
        if rows_3m and rows_15m and rows_1h:
            data[sym] = {
                "3m": pd.DataFrame(rows_3m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "15m": pd.DataFrame(rows_15m, columns=["ts", "open", "high", "low", "close", "volume"]),
                "1h": pd.DataFrame(rows_1h, columns=["ts", "open", "high", "low", "close", "volume"]),
            }

    if not data:
        print("[BACKTEST] no_data")
        return

    zones_snapshot_in: Dict[str, List[dict]] = {}
    if args.zones_snapshot_in:
        try:
            with open(args.zones_snapshot_in, "r", encoding="utf-8") as f:
                zones_snapshot_in = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[BACKTEST] zones_snapshot_in_error={exc}")
            zones_snapshot_in = {}

    zones_snapshot_out: Dict[str, List[dict]] = {}

    def _new_stats() -> Dict[str, float]:
        return {
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
            "net_sum": 0.0,
            "tp_sum": 0.0,
            "sl_sum": 0.0,
            "net_sum_usdt": 0.0,
            "tp_sum_usdt": 0.0,
            "sl_sum_usdt": 0.0,
        }

    def _tp_sl_pct_from_trade(trade: dict) -> tuple[float, float]:
        entry = float(trade.get("entry_px") or 0.0)
        tp = float(trade.get("tp_price") or 0.0)
        sl = float(trade.get("sl_price") or 0.0)
        tp_pct = (tp - entry) / entry * 100.0 if entry > 0 and tp > 0 else 0.0
        sl_pct = (entry - sl) / entry * 100.0 if entry > 0 and sl > 0 else 0.0
        return tp_pct, sl_pct

    stats = _new_stats()
    per_symbol_stats: Dict[str, Dict[str, float]] = {}
    gate_counts = {
        "zone_touch": 0,
        "hl_15m": 0,
        "break_3m": 0,
        "break_3m_strong": 0,
        "break_3m_weak": 0,
        "retest_seen": 0,
        "retest_pass_close": 0,
        "retest_pass_high": 0,
        "retest_fail_shallow": 0,
        "entry_by_pass_close": 0,
        "entry_by_pass_high": 0,
        "reject_pass_1h": 0,
        "reject_pass_15m": 0,
        "ema200_pass": 0,
        "time_block": 0,
    }

    def _load_entry_block_hours() -> set[int]:
        # prefer explicit args, else read from state.json or ENV ENTRY_BLOCK_HOURS
        if args.block_hours:
            try:
                return {int(h.strip()) for h in args.block_hours.split(",") if h.strip() != ""}
            except Exception:
                return set()
        try:
            if os.path.exists("state.json"):
                with open("state.json", "r", encoding="utf-8") as f:
                    st = json.load(f)
                raw = ""
                if isinstance(st, dict):
                    raw = str(st.get("_entry_block_hours") or st.get("entry_block_hours") or "")
                if raw:
                    return {int(h.strip()) for h in raw.split(",") if h.strip() != ""}
        except Exception:
            pass
        raw_env = os.getenv("ENTRY_BLOCK_HOURS", "").strip()
        if raw_env:
            try:
                return {int(h.strip()) for h in raw_env.split(",") if h.strip() != ""}
            except Exception:
                return set()
        return set()

    block_hours = _load_entry_block_hours()

    entries_by_day: Dict[str, int] = {}
    cooldown_until: Dict[tuple, int] = {}
    exit_logs: List[dict] = []
    open_logs: List[dict] = []
    entry_symbols: set[str] = set()
    ltf_minutes = _tf_to_minutes(cfg.tf_ltf)
    hour_stats: Dict[int, Dict[str, int]] = {}
    dow_stats: Dict[str, Dict[str, int]] = {}
    date_stats: Dict[str, Dict[str, float]] = {}
    end_ms_last = end_ms

    for sym, frames in data.items():
        sym_stats = _new_stats()
        per_symbol_stats[sym] = sym_stats
        df_3m = frames["3m"]
        df_15m = frames["15m"]
        df_1h = frames["1h"]
        if args.use_confirmed:
            df_3m = df_3m.iloc[:-1]
            df_15m = df_15m.iloc[:-1]
            df_1h = df_1h.iloc[:-1]

        if len(df_3m) < 10 or len(df_15m) < 5 or len(df_1h) < (cfg.lookback * 2 + 5):
            continue

        close_1h = df_1h["close"].astype(float)
        open_1h = df_1h["open"].astype(float)
        high_1h = df_1h["high"].astype(float)
        low_1h = df_1h["low"].astype(float)
        volume_1h = df_1h["volume"].astype(float)
        ema200_1h = _ema(close_1h, 200)
        dv = np.where(close_1h > open_1h, volume_1h, np.where(close_1h < open_1h, -volume_1h, 0.0))
        dv = pd.Series(dv, index=df_1h.index)
        dvf = _ema(dv, cfg.delta_len)
        vol_ema = _ema(volume_1h, cfg.delta_len)
        atr_3m = _atr(df_3m, 14)
        ema_3m_entry = _ema(df_3m["close"].astype(float), int(cfg.entry_ema_len))
        atr_15m = _atr(df_15m, 14)

        zones: List[Zone] = []
        if args.zones_snapshot_in and sym in zones_snapshot_in:
            zones = [
                Zone(
                    mid=float(z["mid"]),
                    top=float(z["top"]),
                    bot=float(z["bot"]),
                    side=int(z["side"]),
                    live=bool(z["live"]),
                    born=int(z["born"]),
                    start=int(z["start"]),
                    vol=float(z["vol"]),
                )
                for z in zones_snapshot_in.get(sym, [])
            ]

        window_bars_1h = int(args.total_window_days * 24) if args.total_window_days else 0

        # build zones from 1h history first
        if not zones and not args.rolling_zones:
            zone_end_idx = len(df_1h)
            if args.freeze_zones:
                zone_end_idx = int(np.searchsorted(df_1h["ts"].values, eval_start_ms, side="right"))
            zones_raw = build_sr_zones(df_1h.iloc[:zone_end_idx], cfg, window_bars=window_bars_1h)
            zones = [
                Zone(
                    mid=float(z["mid"]),
                    top=float(z["top"]),
                    bot=float(z["bot"]),
                    side=int(z["side"]),
                    live=True,
                    born=int(z["born"]),
                    start=int(z["start"]),
                    vol=float(z["vol"]),
                )
                for z in zones_raw
            ]

        if args.zones_snapshot_out:
            zones_snapshot_out[sym] = [
                {
                    "mid": z.mid,
                    "top": z.top,
                    "bot": z.bot,
                    "side": z.side,
                    "live": z.live,
                    "born": z.born,
                    "start": z.start,
                    "vol": z.vol,
                }
                for z in zones
            ]

        ts_1h = df_1h["ts"].values
        ts_15m = df_15m["ts"].values
        ts_3m = df_3m["ts"].values

        trade = None
        retest_active = False
        retest_level = 0.0
        retest_until = -1
        last_zone_end_idx = None

        for i3 in range(3, len(df_3m) - 1):
            ts = int(ts_3m[i3])
            if ts < eval_start_ms:
                continue
            cd_until = cooldown_until.get((sym, "LONG"))
            if isinstance(cd_until, int) and ts < cd_until:
                continue

            idx_1h = int(np.searchsorted(ts_1h, ts, side="right") - 1)
            if args.use_confirmed:
                idx_1h -= 1
            if idx_1h < 0:
                continue

            if args.rolling_zones and not zones_snapshot_in:
                if last_zone_end_idx != idx_1h:
                    zones_raw = build_sr_zones(df_1h.iloc[: idx_1h + 1], cfg, window_bars=window_bars_1h)
                    zones = [
                        Zone(
                            mid=float(z["mid"]),
                            top=float(z["top"]),
                            bot=float(z["bot"]),
                            side=int(z["side"]),
                            live=True,
                            born=int(z["born"]),
                            start=int(z["start"]),
                            vol=float(z["vol"]),
                        )
                        for z in zones_raw
                    ]
                    last_zone_end_idx = idx_1h

            close_1h_now = float(close_1h.iloc[idx_1h])
            for z in zones:
                if z.live and z.side == -1 and close_1h_now < z.bot:
                    z.live = False
                if z.live and z.side == 1 and close_1h_now > z.top:
                    z.live = False

            if trade:
                high_i = float(df_3m.at[i3, "high"])
                low_i = float(df_3m.at[i3, "low"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (high_i - trade["entry_px"]) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (trade["entry_px"] - low_i) / trade["entry_px"]))
                if low_i <= trade["sl_price"]:
                    exit_px = trade["sl_price"]
                    pnl_pct = (exit_px - trade["entry_px"]) / trade["entry_px"]
                    for bucket in (stats, sym_stats):
                        bucket["exits"] += 1
                        bucket["trades"] += 1
                        bucket["mfe_sum"] += trade["mfe"]
                        bucket["mae_sum"] += trade["mae"]
                        bucket["hold_sum"] += trade["hold_bars"] * ltf_minutes
                        bucket["net_sum"] += pnl_pct
                        bucket["sl_sum"] += pnl_pct
                        bucket["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                        bucket["sl_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                        bucket["losses"] += 1
                        bucket["sl"] = bucket.get("sl", 0) + 1
                    exit_logs.append(
                        {
                            "sym": sym,
                            "mode": "sr_pro_long_v1",
                            "side": "LONG",
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                            "entry_px": trade["entry_px"],
                            "exit_px": exit_px,
                            "reason": "SL",
                            "tp_pct": _tp_sl_pct_from_trade(trade)[0],
                            "sl_pct": _tp_sl_pct_from_trade(trade)[1],
                            "pnl_pct": pnl_pct,
                        }
                    )
                    cooldown_until[(sym, "LONG")] = ts + (60 * 60 * 1000)
                    trade = None
                elif high_i >= trade["tp_price"]:
                    exit_px = trade["tp_price"]
                    pnl_pct = (exit_px - trade["entry_px"]) / trade["entry_px"]
                    for bucket in (stats, sym_stats):
                        bucket["exits"] += 1
                        bucket["trades"] += 1
                        bucket["mfe_sum"] += trade["mfe"]
                        bucket["mae_sum"] += trade["mae"]
                        bucket["hold_sum"] += trade["hold_bars"] * ltf_minutes
                        bucket["net_sum"] += pnl_pct
                        bucket["tp_sum"] += pnl_pct
                        bucket["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                        bucket["tp_sum_usdt"] += pnl_pct * float(args.entry_usdt)
                        bucket["wins"] += 1
                        bucket["tp"] = bucket.get("tp", 0) + 1
                    exit_logs.append(
                        {
                            "sym": sym,
                            "mode": "sr_pro_long_v1",
                            "side": "LONG",
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                            "entry_px": trade["entry_px"],
                            "exit_px": exit_px,
                            "reason": "TP",
                            "tp_pct": _tp_sl_pct_from_trade(trade)[0],
                            "sl_pct": _tp_sl_pct_from_trade(trade)[1],
                            "pnl_pct": pnl_pct,
                        }
                    )
                    trade = None
                continue

            h1_high = float(high_1h.iloc[idx_1h])
            h1_low = float(low_1h.iloc[idx_1h])
            h1_close = float(close_1h.iloc[idx_1h])
            h1_touch_level = "mid" if args.touch_mode == "mid" else "top"
            dvf_norm = float(dvf.iloc[idx_1h]) / float(vol_ema.iloc[idx_1h]) if float(vol_ema.iloc[idx_1h]) > 0 else 0.0
            h1_touch_px = h1_close if args.touch_use_close else h1_low
            if args.ema200_filter:
                ema_len = max(1, int(args.ema_filter_len))
                ema_line = _ema(close_1h, ema_len)
                ema_now = float(ema_line.iloc[idx_1h])
                if h1_close <= ema_now:
                    if args.log_gates:
                        gate_counts["zone_touch"] += 1
                    continue
                if args.log_gates:
                    gate_counts["ema200_pass"] += 1

            support_candidates = [
                z
                for z in zones
                if z.live
                and z.side == -1
                and dvf_norm >= float(args.dvf_norm_min)
                and h1_touch_px <= (z.mid if h1_touch_level == "mid" else z.top)
                and h1_high >= z.bot
            ]
            if support_candidates and args.require_reject_close:
                reject_level = "mid" if args.reject_mode == "mid" else "top"
                if args.reject_source == "1h":
                    support_candidates = [
                        z for z in support_candidates if h1_close > (z.mid if reject_level == "mid" else z.top)
                    ]
                    if support_candidates and args.log_gates:
                        gate_counts["reject_pass_1h"] += 1
                else:
                    idx_15m_rej = int(np.searchsorted(ts_15m, ts, side="right") - 1)
                    if idx_15m_rej >= 0:
                        close_15m = float(df_15m.at[idx_15m_rej, "close"])
                        support_candidates = [
                            z for z in support_candidates if close_15m > (z.mid if reject_level == "mid" else z.top)
                        ]
                        if support_candidates and args.log_gates:
                            gate_counts["reject_pass_15m"] += 1
            if not support_candidates:
                if args.log_gates:
                    gate_counts["zone_touch"] += 1
                continue

            idx_15m = int(np.searchsorted(ts_15m, ts, side="right") - 1)
            if idx_15m < 2:
                continue
            l15_0 = float(df_15m.at[idx_15m, "low"])
            l15_1 = float(df_15m.at[idx_15m - 1, "low"])
            l15_2 = float(df_15m.at[idx_15m - 2, "low"])
            if not (l15_0 > l15_1 or l15_1 > l15_2):
                if args.log_gates:
                    gate_counts["hl_15m"] += 1
                continue
            if not (float(df_15m.at[idx_15m, "close"]) > float(df_15m.at[idx_15m, "open"])):
                if args.log_gates:
                    gate_counts["hl_15m"] += 1
                continue

            close_now = float(df_3m.at[i3, "close"])
            open_now = float(df_3m.at[i3, "open"])
            high_prev = [
                float(df_3m.at[i3 - 1, "high"]),
                float(df_3m.at[i3 - 2, "high"]),
                float(df_3m.at[i3 - 3, "high"]),
            ]
            high_max = max(high_prev)
            strong_break = close_now > high_max
            weak_break = (float(df_3m.at[i3, "high"]) > high_max) and (close_now <= high_max) and (close_now > open_now)
            if not strong_break and not weak_break:
                if args.log_gates:
                    gate_counts["break_3m"] += 1
                continue
            # time block (KST hours)
            hour_kst = int(_ts_kst(ts).split(" ")[1].split(":")[0])
            if block_hours and hour_kst in block_hours:
                if args.log_gates:
                    gate_counts["time_block"] += 1
                continue
            if args.log_gates:
                if strong_break:
                    gate_counts["break_3m_strong"] += 1
                elif weak_break:
                    gate_counts["break_3m_weak"] += 1

            retest_level = high_max
            retest_active = True
            retest_bars = max(1, int(args.retest_bars))
            if args.retest_dyn:
                atr3 = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                atr15 = float(atr_15m.iloc[idx_15m]) if not np.isnan(atr_15m.iloc[idx_15m]) else 0.0
                if atr15 > 0 and (atr3 / atr15) < float(args.retest_dyn_th):
                    retest_bars = max(retest_bars, int(args.retest_dyn_bars))
            retest_until = i3 + retest_bars
            if args.log_gates:
                gate_counts["retest_seen"] += 1

            if retest_active and i3 <= retest_until:
                low_now = float(df_3m.at[i3, "low"])
                high_now = float(df_3m.at[i3, "high"])
                rng = float(df_3m.at[i3, "high"]) - float(df_3m.at[i3, "low"])
                lower_wick = min(float(df_3m.at[i3, "open"]), float(df_3m.at[i3, "close"])) - float(df_3m.at[i3, "low"])
                lower_wick_ratio = (lower_wick / rng) if rng > 0 else 0.0
                atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                if low_now <= retest_level + (atr_now * float(args.retest_atr_mult)):
                    entry_ok = False
                    entry_reason = None
                    if close_now > retest_level:
                        entry_ok = True
                        entry_reason = "close_reclaim"
                        if args.log_gates:
                            gate_counts["retest_pass_close"] += 1
                    elif high_now > retest_level and close_now > open_now and lower_wick_ratio <= 0.40:
                        entry_ok = True
                        entry_reason = "high_sweep"
                        if args.log_gates:
                            gate_counts["retest_pass_high"] += 1
                    elif (not strong_break) and (low_now > retest_level - (atr_now * float(args.shallow_atr_mult))) and close_now > open_now and dvf_norm >= float(args.shallow_dvf_min) and lower_wick_ratio <= float(args.shallow_wick_max):
                        entry_ok = True
                        entry_reason = "shallow"
                        if args.log_gates:
                            gate_counts["retest_pass_high"] += 1
                    else:
                        continue

                    ema_entry = float(ema_3m_entry.iloc[i3]) if len(ema_3m_entry) > i3 and not np.isnan(ema_3m_entry.iloc[i3]) else None
                    entry_offset = float(cfg.entry_atr_offset)
                    entry_target = None
                    if isinstance(ema_entry, (int, float)) and atr_now > 0:
                        entry_target = float(ema_entry) - (atr_now * entry_offset)
                    if entry_target is None or low_now > entry_target:
                        continue
                    entry_px = float(close_now)
                    nearest = min(support_candidates, key=lambda z: abs(z.mid - entry_px))
                    if not (close_now >= nearest.mid or entry_px >= nearest.top - (atr_now * 0.2)):
                        if args.log_gates:
                            gate_counts["retest_fail_shallow"] += 1
                        continue
                    sl_raw = nearest.bot - (atr_now * float(args.sl_atr_mult))
                    sl_price = min(sl_raw, entry_px - (atr_now * 1.0))
                    tp_atr = float(args.tp_atr_mult_weak) if (not strong_break and float(args.tp_atr_mult_weak) > 0) else float(args.tp_atr_mult)
                    if tp_atr > 0:
                        tp_price = entry_px + (atr_now * tp_atr)
                    else:
                        tp_price = entry_px * (cfg.tp_mult if strong_break else float(args.tp_mult_weak))

                    trade = {
                        "entry_px": entry_px,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "hold_bars": 0,
                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                        "track": "strong" if strong_break else "weak",
                        "reason": entry_reason,
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    if args.log_gates:
                        if entry_reason == "close_reclaim":
                            gate_counts["entry_by_pass_close"] += 1
                        else:
                            gate_counts["entry_by_pass_high"] += 1
                    day_key = _ts_kst(trade["entry_ts"]).split(" ")[0]
                    entries_by_day[day_key] = entries_by_day.get(day_key, 0) + 1
                    date_stats.setdefault(
                        day_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0}
                    )
                    date_stats[day_key]["entries"] += 1
                    entry_symbols.add(sym)
                    dt_kst = datetime.fromtimestamp(trade["entry_ts"] / 1000.0, tz=timezone.utc) + pd.Timedelta(hours=9)
                    hour_bucket = dt_kst.hour
                    dow_bucket = _dow_label(dt_kst)
                    hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
                    dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
                    hour_stats[hour_bucket]["entries"] += 1
                    dow_stats[dow_bucket]["entries"] += 1
                    retest_active = False
            if retest_active and i3 > retest_until:
                retest_active = False

        if trade:
            last_idx = len(df_3m) - 2 if args.use_confirmed else len(df_3m) - 1
            last_idx = max(0, last_idx)
            last_px = float(df_3m.at[last_idx, "close"])
            last_ts = int(df_3m.at[last_idx, "ts"])
            unrealized_pct = (last_px - trade["entry_px"]) / trade["entry_px"] * 100.0
            open_logs.append(
                {
                    "sym": sym,
                    "mode": "sr_pro_long_v1",
                    "side": "LONG",
                    "entry_ts": trade["entry_ts"],
                    "entry_px": trade["entry_px"],
                    "last_px": last_px,
                    "last_ts": last_ts,
                    "unrealized_pct": unrealized_pct,
                }
            )

    # enrich exit stats by hour/dow using entry times
    for ex in exit_logs:
        dt_kst = datetime.fromtimestamp(ex["entry_ts"] / 1000.0, tz=timezone.utc) + pd.Timedelta(hours=9)
        hour_bucket = dt_kst.hour
        dow_bucket = _dow_label(dt_kst)
        day_bucket = dt_kst.strftime("%Y-%m-%d")
        hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
        dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
        date_stats.setdefault(day_bucket, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
        date_stats[day_bucket]["net_sum"] += float(ex.get("pnl_pct", 0.0))
        date_stats[day_bucket]["net_sum_usdt"] += float(ex.get("pnl_pct", 0.0)) * float(args.entry_usdt)
        if ex["reason"] == "TP":
            hour_stats[hour_bucket]["tp"] += 1
            dow_stats[dow_bucket]["tp"] += 1
            date_stats[day_bucket]["tp"] += 1
        elif ex["reason"] == "SL":
            hour_stats[hour_bucket]["sl"] += 1
            dow_stats[dow_bucket]["sl"] += 1
            date_stats[day_bucket]["sl"] += 1

    last_day_threshold = end_ms_last - (24 * 60 * 60 * 1000)

    for sym, sym_stats in per_symbol_stats.items():
        if not (sym_stats.get("entries", 0) or sym_stats.get("trades", 0)):
            continue
        last_day_exits = 0
        for ex in exit_logs:
            if ex["sym"] == sym and ex["exit_ts"] >= last_day_threshold:
                last_day_exits += 1
        print(
            _fmt_summary_line(
                sym,
                sym_stats,
                float(args.entry_usdt),
                last_day_exits,
                1 if sym_stats.get("entries", 0) > 0 else 0,
            )
        )
        # OPEN/EXIT logs (entry_dt desc)
        sym_items: List[dict] = []
        sym_items.extend([ex for ex in exit_logs if ex["sym"] == sym])
        sym_items.extend([op for op in open_logs if op["sym"] == sym])
        sym_items.sort(key=lambda x: x["entry_ts"], reverse=True)
        for item in sym_items:
            if "exit_ts" in item:
                tp_pct = item.get("tp_pct")
                sl_pct = item.get("sl_pct")
                tp_sl_text = ""
                if isinstance(tp_pct, (int, float)) and isinstance(sl_pct, (int, float)):
                    tp_sl_text = f" tp_pct={tp_pct:.2f} sl_pct={sl_pct:.2f}"
                result = "WIN" if item.get("reason") == "TP" else "LOSS" if item.get("reason") == "SL" else "OTHER"
                print(
                    "[BACKTEST][EXIT] "
                    f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                    f"entry_dt={_minute_str(item['entry_ts'])} exit_dt={_minute_str(item['exit_ts'])} "
                    f"entry_px={item['entry_px']:.6f} exit_px={item['exit_px']:.6f} reason={item['reason']} result={result}{tp_sl_text}"
                )
            else:
                print(
                    "[BACKTEST][OPEN] "
                    f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                    f"entry_dt={_minute_str(item['entry_ts'])} exit_dt= "
                    f"entry_px={item['entry_px']:.6f} last_px={item['last_px']:.6f} "
                    f"last_dt={_minute_str(item['last_ts'])} unrealized_pct={item['unrealized_pct']:.2f}%"
                )

    total_last_day_exits = sum(1 for ex in exit_logs if ex["exit_ts"] >= last_day_threshold)
    summary = _fmt_summary_line(
        None,
        stats,
        float(args.entry_usdt),
        total_last_day_exits,
        len(entry_symbols),
    )
    print(summary)
    if args.log_gates:
        print(f"[BACKTEST] GATES {gate_counts}")

    print("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for hour in range(24):
        bucket = hour_stats.get(hour, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl = bucket["sl"]
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={bucket['tp']} "
            f"sl={sl} sl_rate={sl_rate:.2f}%"
        )

    print("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        bucket = dow_stats.get(dow, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl = bucket["sl"]
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] DOW {dow} entries={entries} tp={bucket['tp']} "
            f"sl={sl} sl_rate={sl_rate:.2f}%"
        )

    if date_stats:
        print("[BACKTEST] BY_DATE(KST) date entries tp sl sl_rate winrate net_sum net_sum_usdt")
        total_entries = 0
        total_tp = 0
        total_sl = 0
        total_net_sum = 0.0
        total_net_sum_usdt = 0.0
        for day_key in sorted(date_stats.keys()):
            bucket = date_stats[day_key]
            entries = int(bucket.get("entries", 0))
            sl = int(bucket.get("sl", 0))
            sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
            tp = int(bucket.get("tp", 0))
            trades = tp + sl
            winrate = (tp / trades * 100.0) if trades > 0 else 0.0
            net_sum = float(bucket.get("net_sum", 0.0))
            net_sum_usdt = float(bucket.get("net_sum_usdt", 0.0))
            total_entries += entries
            total_tp += tp
            total_sl += sl
            total_net_sum += net_sum
            total_net_sum_usdt += net_sum_usdt
            print(
                f"[BACKTEST] DATE {day_key} entries={entries} tp={tp} "
                f"sl={sl} sl_rate={sl_rate:.2f}% winrate={winrate:.2f}% "
                f"net_sum={net_sum:.3f} net_sum_usdt={net_sum_usdt:.3f}"
            )
        total_trades = total_tp + total_sl
        total_sl_rate = (total_sl / total_entries * 100.0) if total_entries > 0 else 0.0
        total_winrate = (total_tp / total_trades * 100.0) if total_trades > 0 else 0.0
        print(
            f"[BACKTEST] DATE TOTAL entries={total_entries} tp={total_tp} sl={total_sl} "
            f"sl_rate={total_sl_rate:.2f}% winrate={total_winrate:.2f}% "
            f"net_sum={total_net_sum:.3f} net_sum_usdt={total_net_sum_usdt:.3f}"
        )

    if args.zones_snapshot_out:
        try:
            with open(args.zones_snapshot_out, "w", encoding="utf-8") as f:
                json.dump(zones_snapshot_out, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            print(f"[BACKTEST] zones_snapshot_out_error={exc}")


if __name__ == "__main__":
    run_backtest()
