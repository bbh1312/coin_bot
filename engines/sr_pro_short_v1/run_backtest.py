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
from engines.sr_pro_short_v1.engine import SrProShortV1Config
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
                        # fallback to newest mtime
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
    tf = (tf or "").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    return 0


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


def _pivot_high(series: pd.Series, left: int, right: int, idx: int) -> Optional[float]:
    if idx - left < 0 or idx + right >= len(series):
        return None
    window = series.iloc[idx - left : idx + right + 1]
    val = series.iloc[idx]
    if float(val) == float(window.max()):
        return float(val)
    return None


def _pivot_low(series: pd.Series, left: int, right: int, idx: int) -> Optional[float]:
    if idx - left < 0 or idx + right >= len(series):
        return None
    window = series.iloc[idx - left : idx + right + 1]
    val = series.iloc[idx]
    if float(val) == float(window.min()):
        return float(val)
    return None


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
    parser = argparse.ArgumentParser("sr_pro_short_v1 backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--exclude-symbols", type=str, default="")
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--relaxed-lookback", type=int, default=10)
    parser.add_argument("--auto-relax", action="store_true", default=True)
    parser.add_argument("--atr-mult", type=float, default=1.0)
    parser.add_argument("--delta-len", type=int, default=2)
    parser.add_argument("--cluster-atr", type=float, default=1.5)
    parser.add_argument("--max-zones-per-side", type=int, default=8)
    parser.add_argument("--touch-mode", type=str, default="bot", choices=["bot", "mid"])
    parser.add_argument("--touch-use-close", action="store_true")
    parser.add_argument("--dvf-norm-max", type=float, default=-0.05)
    parser.add_argument("--dvf-norm-immediate", type=float, default=-0.25)
    parser.add_argument("--dvf-norm-diff-th", type=float, default=-0.03)
    parser.add_argument("--dvf-confirm-bars", type=int, default=1)
    parser.add_argument("--require-reject-close", action="store_true")
    parser.add_argument("--reject-mode", type=str, default="bot", choices=["bot", "mid"])
    parser.add_argument("--reject-source", type=str, default="1h", choices=["1h", "15m"])
    parser.add_argument("--ema200-filter", action="store_true")
    parser.add_argument("--ema-filter-len", type=int, default=200)
    parser.add_argument("--block-hours", type=str, default="")
    parser.add_argument("--disable-weak", action="store_true")
    parser.add_argument("--retest-bars", type=int, default=6)
    parser.add_argument("--retest-atr-mult", type=float, default=0.6)
    parser.add_argument("--retest-above-atr-mult", type=float, default=0.25)
    parser.add_argument("--reclaim-up-atr-mult", type=float, default=0.1)
    parser.add_argument("--retest-timeout-bars", type=int, default=6)
    parser.add_argument("--retest-near-atr-mult", type=float, default=0.15)
    parser.add_argument("--retest-wick-max", type=float, default=0.35)
    parser.add_argument("--retest-dyn", action="store_true")
    parser.add_argument("--retest-dyn-th", type=float, default=0.6)
    parser.add_argument("--retest-dyn-bars", type=int, default=10)
    parser.add_argument("--shallow-atr-mult", type=float, default=0.25)
    parser.add_argument("--shallow-wick-max", type=float, default=0.35)
    parser.add_argument("--shallow-dvf-max", type=float, default=0.0)
    parser.add_argument("--big-bear-body-mult", type=float, default=1.2)
    parser.add_argument("--atr-filter-len", type=int, default=20)
    parser.add_argument("--atr-filter-mult", type=float, default=0.7)
    parser.add_argument("--ema60-15m-len", type=int, default=60)
    parser.add_argument("--ema120-15m-len", type=int, default=120)
    parser.add_argument("--ema-slope-min", type=float, default=0.001)
    parser.add_argument("--sl-buffer", type=float, default=0.01)
    parser.add_argument("--sl-atr-mult", type=float, default=0.5)
    parser.add_argument("--sl-cap-pct", type=float, default=0.02)
    parser.add_argument("--sl-cap-atr-mult", type=float, default=0.6)
    parser.add_argument("--tp-atr-mult", type=float, default=0.0)
    parser.add_argument("--tp-atr-mult-weak", type=float, default=0.0)
    parser.add_argument("--tp-mult", type=float, default=0.98)
    parser.add_argument("--tp-mult-weak", type=float, default=0.99)
    parser.add_argument("--base-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-usdt", type=float, default=10.0)
    parser.add_argument("--freeze-zones", action="store_true")
    parser.add_argument("--total-window-days", type=int, default=14)
    parser.add_argument("--rolling-zones", action="store_true")
    parser.add_argument("--zones-snapshot-in", type=str, default="")
    parser.add_argument("--zones-snapshot-out", type=str, default="")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-gates", action="store_true")
    parser.add_argument("--debug-break", action="store_true")
    parser.add_argument("--debug-retest", action="store_true")
    parser.add_argument("--require-retest-touch", action="store_true")
    parser.add_argument("--debug-zone", action="store_true")
    parser.add_argument("--ltf-sr-bias", action="store_true")
    parser.add_argument("--ltf-sr-lookback", type=int, default=60)
    args = parser.parse_args()
    if args.log_gates:
        args.verbose = True
    if args.verbose:
        args.log_gates = True

    def _count_cache_files(cache_dir: str) -> dict:
        out = {"3m": 0, "15m": 0, "1h": 0}
        if not cache_dir or not os.path.isdir(cache_dir):
            return out
        try:
            for name in os.listdir(cache_dir):
                if not name.endswith(".csv"):
                    continue
                if "_3m_" in name:
                    out["3m"] += 1
                elif "_15m_" in name:
                    out["15m"] += 1
                elif "_1h_" in name:
                    out["1h"] += 1
        except Exception:
            pass
        return out

    cfg = SrProShortV1Config(
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
    )

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    min_bars = {
        cfg.tf_ltf: 120,
        cfg.tf_mtf: 120,
        cfg.tf_htf: 120,
    }
    if args.total_window_days:
        if args.total_window_days <= 0:
            print("[BACKTEST] total_window_days must be > 0")
            return
        total_window_days = args.total_window_days
        warmup_days = total_window_days
        warmup_minutes = warmup_days * 1440
        start_ms = end_ms - int((total_window_days + args.days) * 24 * 60 * 60 * 1000)
        eval_start_ms = end_ms - int(args.days * 24 * 60 * 60 * 1000)
    else:
        start_ms, eval_start_ms, warmup_days, warmup_minutes = calc_warmup_window(
            args.days, end_ms, min_bars
        )

    if args.rolling_zones and not args.total_window_days:
        print("[BACKTEST] rolling_zones requires total_window_days")
        return

    # Prefer live cycle cache for backtest parity when common-only/cache-only is used.
    use_common = bool(args.common_warmup_dir) or args.common_only or args.cache_only
    common_dir = args.common_warmup_dir
    if not common_dir:
        common_dir = os.getenv("COMMON_WARMUP_CACHE_DIR", os.path.join("logs", "common_warmup", "ohlcv"))
    if use_common:
        counts = _count_cache_files(common_dir)
        print(
            f"[BACKTEST] cache_source=common "
            f"common_dir='{common_dir}' exists={os.path.isdir(common_dir)} "
            f"files_3m={counts['3m']} files_15m={counts['15m']} files_1h={counts['1h']} "
            f"cache_only={args.cache_only} common_only={args.common_only}"
        )
    universe = load_common_universe(
        args.universe, exchange, args.cache_only, top_n=args.top_n
    )
    if args.exclude_symbols:
        raw = args.exclude_symbols.replace(" ", "").replace(";", ",").replace("|", ",")
        exclude = {s for s in raw.split(",") if s}
        if exclude:
            universe = [s for s in universe if s not in exclude]
    if not universe:
        print("[BACKTEST] no_universe")
        return

    log_warmup_info(lambda _: None, warmup_days, warmup_minutes, args.days)

    data: Dict[str, Dict[str, pd.DataFrame]] = {}
    for sym in universe:
        rows_3m = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_ltf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        rows_15m = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_mtf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
        rows_1h = _fetch_ohlcv_all(
            exchange,
            sym,
            cfg.tf_htf,
            start_ms,
            end_ms,
            cache_only=args.cache_only,
            use_common_warmup=use_common,
            common_warmup_dir=common_dir,
            common_only=args.common_only,
        )
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

    stats = _new_stats()
    per_symbol_stats: Dict[str, Dict[str, float]] = {}
    exit_logs: List[dict] = []
    open_logs: List[dict] = []
    gate_counts = {
        "zone_touch": 0,
        "lh_15m": 0,
        "break_3m": 0,
        "break_3m_strong": 0,
        "break_3m_weak": 0,
        "retest_seen": 0,
        "retest_pass_close": 0,
        "retest_pass_low": 0,
        "retest_fail_far": 0,
        "retest_fail_shallow": 0,
        "retest_fail_no_touch": 0,
        "entry_by_pass_close": 0,
        "entry_by_pass_low": 0,
        "entry_by_big_bear": 0,
        "entry_by_dvf_accel": 0,
        "entry_by_dvf_slope": 0,
        "entry_by_timeout": 0,
        "atr_filter": 0,
        "ema_slope_block": 0,
        "reject_pass_1h": 0,
        "reject_pass_15m": 0,
        "ema200_pass": 0,
        "entries_strong": 0,
        "entries_weak": 0,
        "wins_strong": 0,
        "wins_weak": 0,
        "losses_strong": 0,
        "losses_weak": 0,
        "net_strong": 0.0,
        "net_weak": 0.0,
        "mfe_strong_sum": 0.0,
        "mfe_weak_sum": 0.0,
        "mae_strong_sum": 0.0,
        "mae_weak_sum": 0.0,
        "hold_strong_sum": 0.0,
        "hold_weak_sum": 0.0,
        "time_block": 0,
        "skip_stale_ts": 0,
        "ltf_sr_bias_block": 0,
    }
    def _parse_entry_block_hours_raw(raw: str) -> set[int]:
        if not isinstance(raw, str):
            return set()
        raw = raw.replace(" ", "").replace("|", ",").replace(";", ",")
        if not raw:
            return set()
        out: set[int] = set()
        for part in raw.split(","):
            if not part:
                continue
            if "~" in part:
                left = part.split("~", 1)[0]
                try:
                    hour = int(float(left.split(":", 1)[0]))
                except Exception:
                    continue
                if 0 <= hour <= 23:
                    out.add(hour)
                continue
            try:
                hour = int(float(part.split(":", 1)[0]))
            except Exception:
                continue
            if 0 <= hour <= 23:
                out.add(hour)
        return out

    def _load_entry_block_hours() -> set[int]:
        # prefer explicit args, else read from state.json or ENV ENTRY_BLOCK_HOURS
        if args.block_hours:
            hours = _parse_entry_block_hours_raw(args.block_hours)
            if hours:
                return hours
        # try state.json
        try:
            if os.path.exists("state.json"):
                with open("state.json", "r", encoding="utf-8") as f:
                    st = json.load(f)
                raw = ""
                if isinstance(st, dict):
                    raw = str(st.get("_entry_block_hours") or st.get("entry_block_hours") or "")
                if raw:
                    hours = _parse_entry_block_hours_raw(raw)
                    if hours:
                        return hours
        except Exception:
            pass
        # fallback to env
        raw_env = os.getenv("ENTRY_BLOCK_HOURS", "").strip()
        if raw_env:
            hours = _parse_entry_block_hours_raw(raw_env)
            if hours:
                return hours
        return set()

    block_hours = _load_entry_block_hours()

    entries_by_day: Dict[str, int] = {}
    cooldown_until: Dict[str, int] = {}
    entry_symbols: set[str] = set()
    ltf_minutes = _tf_to_minutes(cfg.tf_ltf)
    hour_stats: Dict[int, Dict[str, int]] = {}
    dow_stats: Dict[str, Dict[str, int]] = {}
    date_stats: Dict[str, Dict[str, float]] = {}
    end_ms_last = end_ms

    def _record_entry_ts(entry_ts: int) -> None:
        dt_kst = datetime.fromtimestamp(entry_ts / 1000.0, tz=timezone.utc) + pd.Timedelta(hours=9)
        hour_bucket = dt_kst.hour
        dow_bucket = _dow_label(dt_kst)
        hour_stats.setdefault(hour_bucket, {"entries": 0, "tp": 0, "sl": 0})
        dow_stats.setdefault(dow_bucket, {"entries": 0, "tp": 0, "sl": 0})
        hour_stats[hour_bucket]["entries"] += 1
        dow_stats[dow_bucket]["entries"] += 1
        day_key = dt_kst.strftime("%Y-%m-%d")
        entries_by_day[day_key] = entries_by_day.get(day_key, 0) + 1
        date_stats.setdefault(
            day_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0}
        )
        date_stats[day_key]["entries"] += 1

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

        # precompute 1h indicators for zones
        close_1h = df_1h["close"].astype(float)
        open_1h = df_1h["open"].astype(float)
        high_1h = df_1h["high"].astype(float)
        low_1h = df_1h["low"].astype(float)
        volume_1h = df_1h["volume"].astype(float)
        atr_1h = _atr(df_1h, 14)
        ema200_1h = _ema(close_1h, 200)
        dv = np.where(close_1h > open_1h, volume_1h, np.where(close_1h < open_1h, -volume_1h, 0.0))
        dv = pd.Series(dv, index=df_1h.index)
        dvf = _ema(dv, cfg.delta_len)
        vol_ema = _ema(volume_1h, cfg.delta_len)
        atr_3m = _atr(df_3m, 14)
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

        # build zones from 1h history first (TradingView pivot confirmed style)
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

        # map 1h index by ts for fast lookup
        ts_1h = df_1h["ts"].values
        ts_15m = df_15m["ts"].values
        ts_3m = df_3m["ts"].values

        trade = None
        retest_active = False
        retest_is_strong = False
        retest_level = 0.0
        retest_until = -1
        break_i3 = -1
        break_low_min = 0.0
        dvf_pending_active = False
        dvf_pending_until = -1
        dvf_pending_level = 0.0
        dvf_pending_type = None
        break_debug_count = 0
        last_zone_end_idx = None
        if args.log_gates:
            print(
                f"[BACKTEST_START_STATE] {sym} in_position=False cooldown=0 "
                f"zones_total={len(zones)} zones_res={len([z for z in zones if z.side==1])} "
                f"zones_sup={len([z for z in zones if z.side==-1])}"
            )

        for i3 in range(3, len(df_3m) - 1):
            ts = int(ts_3m[i3])
            if ts < eval_start_ms:
                continue
            if args.debug_retest and retest_active and i3 <= retest_until:
                try:
                    print(
                        "[BACKTEST][RET_STATE] "
                        f"sym={sym} ts={_ts_kst(ts)} i={i3} "
                        f"level={retest_level:.6f} until={retest_until} "
                        f"high={float(df_3m.at[i3, 'high']):.6f} "
                        f"low={float(df_3m.at[i3, 'low']):.6f} "
                        f"close={float(df_3m.at[i3, 'close']):.6f}"
                    )
                except Exception:
                    pass
            cd_until = cooldown_until.get(sym)
            if isinstance(cd_until, int) and ts < cd_until:
                continue
            # entry block hours (KST) - apply to all entry paths
            if block_hours:
                hour_kst = int(_ts_kst(ts).split(" ")[1].split(":")[0])
                if hour_kst in block_hours:
                    if args.log_gates:
                        gate_counts["time_block"] += 1
                    continue

            # resolve current 1h bar index
            idx_1h = int(np.searchsorted(ts_1h, ts, side="right") - 1)
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

            # invalidate zones
            close_1h_now = float(close_1h.iloc[idx_1h])
            for z in zones:
                if z.live and z.side == 1 and close_1h_now > z.top:
                    z.live = False
                if z.live and z.side == -1 and close_1h_now < z.bot:
                    z.live = False

            # handle existing trade
            if trade:
                high_i = float(df_3m.at[i3, "high"])
                low_i = float(df_3m.at[i3, "low"])
                trade["hold_bars"] += 1
                trade["mfe"] = max(trade["mfe"], max(0.0, (trade["entry_px"] - low_i) / trade["entry_px"]))
                trade["mae"] = max(trade["mae"], max(0.0, (high_i - trade["entry_px"]) / trade["entry_px"]))
                if high_i >= trade["sl_price"]:
                    exit_px = trade["sl_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    pnl_total = pnl_pct
                    for bucket in (stats, sym_stats):
                        bucket["exits"] += 1
                        bucket["trades"] += 1
                        bucket["mfe_sum"] += trade["mfe"]
                        bucket["mae_sum"] += trade["mae"]
                        bucket["hold_sum"] += trade["hold_bars"] * ltf_minutes
                        bucket["net_sum"] += pnl_total
                        bucket["sl_sum"] += pnl_total
                        bucket["net_sum_usdt"] += pnl_total * float(args.entry_usdt)
                        bucket["sl_sum_usdt"] += pnl_total * float(args.entry_usdt)
                        bucket["losses"] += 1
                        bucket["sl"] += 1
                        if trade.get("track") == "strong":
                            gate_counts["losses_strong"] += 1
                            gate_counts["net_strong"] += pnl_pct
                            gate_counts["mfe_strong_sum"] += trade["mfe"]
                            gate_counts["mae_strong_sum"] += trade["mae"]
                            gate_counts["hold_strong_sum"] += trade["hold_bars"]
                        elif trade.get("track") == "weak":
                            gate_counts["losses_weak"] += 1
                            gate_counts["net_weak"] += pnl_pct
                            gate_counts["mfe_weak_sum"] += trade["mfe"]
                            gate_counts["mae_weak_sum"] += trade["mae"]
                            gate_counts["hold_weak_sum"] += trade["hold_bars"]
                    exit_logs.append(
                        {
                            "sym": sym,
                            "mode": "sr_pro_short_v1",
                            "side": "SHORT",
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                            "entry_px": trade["entry_px"],
                            "exit_px": exit_px,
                            "reason": "SL",
                            "result": "LOSS",
                            "tp_pct": ((trade["entry_px"] - trade.get("tp_price", trade["entry_px"])) / trade["entry_px"] * 100.0) if trade["entry_px"] > 0 else 0.0,
                            "sl_pct": ((trade["sl_price"] - trade["entry_px"]) / trade["entry_px"] * 100.0) if trade["entry_px"] > 0 else 0.0,
                            "pnl_pct": pnl_pct,
                        }
                    )
                    cooldown_until[sym] = ts + (60 * 60 * 1000)
                    trade = None
                    continue
                elif low_i <= trade.get("tp_price", -1):
                    exit_px = trade["tp_price"]
                    pnl_pct = (trade["entry_px"] - exit_px) / trade["entry_px"]
                    pnl_total = pnl_pct
                    for bucket in (stats, sym_stats):
                        bucket["exits"] += 1
                        bucket["trades"] += 1
                        bucket["mfe_sum"] += trade["mfe"]
                        bucket["mae_sum"] += trade["mae"]
                        bucket["hold_sum"] += trade["hold_bars"] * ltf_minutes
                        bucket["net_sum"] += pnl_total
                        bucket["tp_sum"] += pnl_total
                        bucket["net_sum_usdt"] += pnl_total * float(args.entry_usdt)
                        bucket["tp_sum_usdt"] += pnl_total * float(args.entry_usdt)
                        bucket["wins"] += 1
                        bucket["tp"] += 1
                        if trade.get("track") == "strong":
                            gate_counts["wins_strong"] += 1
                            gate_counts["net_strong"] += pnl_pct
                            gate_counts["mfe_strong_sum"] += trade["mfe"]
                            gate_counts["mae_strong_sum"] += trade["mae"]
                            gate_counts["hold_strong_sum"] += trade["hold_bars"]
                        elif trade.get("track") == "weak":
                            gate_counts["wins_weak"] += 1
                            gate_counts["net_weak"] += pnl_pct
                            gate_counts["mfe_weak_sum"] += trade["mfe"]
                            gate_counts["mae_weak_sum"] += trade["mae"]
                            gate_counts["hold_weak_sum"] += trade["hold_bars"]
                    exit_logs.append(
                        {
                            "sym": sym,
                            "mode": "sr_pro_short_v1",
                            "side": "SHORT",
                            "entry_ts": trade["entry_ts"],
                            "exit_ts": ts,
                            "entry_px": trade["entry_px"],
                            "exit_px": exit_px,
                            "reason": "TP",
                            "result": "WIN",
                            "tp_pct": ((trade["entry_px"] - trade.get("tp_price", trade["entry_px"])) / trade["entry_px"] * 100.0) if trade["entry_px"] > 0 else 0.0,
                            "sl_pct": ((trade["sl_price"] - trade["entry_px"]) / trade["entry_px"] * 100.0) if trade["entry_px"] > 0 else 0.0,
                            "pnl_pct": pnl_pct,
                        }
                    )
                    trade = None
                    continue
                continue

            # 1h in-progress bar touching resistance zone with negative delta
            idx_1h_touch = idx_1h
            h1_high = float(high_1h.iloc[idx_1h_touch])
            h1_low = float(low_1h.iloc[idx_1h_touch])
            h1_close = float(close_1h.iloc[idx_1h_touch])
            dvf_norm = (
                float(dvf.iloc[idx_1h_touch]) / float(vol_ema.iloc[idx_1h_touch])
                if float(vol_ema.iloc[idx_1h_touch]) > 0
                else 0.0
            )
            dvf_prev = (
                float(dvf.iloc[idx_1h_touch - 1]) / float(vol_ema.iloc[idx_1h_touch - 1])
                if idx_1h_touch - 1 >= 0 and float(vol_ema.iloc[idx_1h_touch - 1]) > 0
                else dvf_norm
            )
            dvf_norm_diff = dvf_norm - dvf_prev
            h1_touch_px = h1_close if args.touch_use_close else h1_high
            if args.ema200_filter:
                ema_len = max(1, int(args.ema_filter_len))
                ema_line = _ema(close_1h, ema_len)
                ema_now = float(ema_line.iloc[idx_1h_touch])
                if h1_close >= ema_now:
                    if args.log_gates:
                        gate_counts["zone_touch"] += 1
                    continue
                if args.log_gates:
                    gate_counts["ema200_pass"] += 1
            resist_candidates = [
                z
                for z in zones
                if z.live
                and z.side == 1
                and dvf_norm <= float(args.dvf_norm_max)
                and h1_high >= z.bot
                and h1_low <= z.top
            ]
            if resist_candidates and args.require_reject_close:
                reject_level = "mid" if args.reject_mode == "mid" else "bot"
                if args.reject_source == "1h":
                    resist_candidates = [
                        z
                        for z in resist_candidates
                        if h1_close < (z.mid if reject_level == "mid" else z.bot)
                    ]
                    if resist_candidates and args.log_gates:
                        gate_counts["reject_pass_1h"] += 1
                else:
                    idx_15m_rej = int(np.searchsorted(ts_15m, ts, side="right") - 1)
                    if idx_15m_rej >= 0:
                        close_15m = float(df_15m.at[idx_15m_rej, "close"])
                        resist_candidates = [
                            z
                            for z in resist_candidates
                            if close_15m < (z.mid if reject_level == "mid" else z.bot)
                        ]
                        if resist_candidates and args.log_gates:
                            gate_counts["reject_pass_15m"] += 1
            if args.debug_zone and (i3 % 200 == 0):
                live_res = [z for z in zones if z.live and z.side == 1]
                live_sup = [z for z in zones if z.live and z.side == -1]
                nearest_res = min(live_res, key=lambda z: abs(z.mid - h1_touch_px)) if live_res else None
                if nearest_res:
                    print(
                        f"[DEBUG][ZONE] {sym} ts={_ts_kst(ts)} "
                        f"h1_touch_px={h1_touch_px:.6f} res_mid={nearest_res.mid:.6f} "
                        f"res_top={nearest_res.top:.6f} res_bot={nearest_res.bot:.6f} "
                        f"dvf_norm={dvf_norm:.4f} zones_res={len(live_res)} zones_sup={len(live_sup)}"
                    )
                else:
                    print(
                        f"[DEBUG][ZONE] {sym} ts={_ts_kst(ts)} "
                        f"h1_touch_px={h1_touch_px:.6f} res_mid=NONE "
                        f"dvf_norm={dvf_norm:.4f} zones_res={len(live_res)} zones_sup={len(live_sup)}"
                    )
            if not resist_candidates:
                # Touch is only valid on the latest confirmed 1h bar.
                # If current 1h does not touch, clear any prior touch/retest state.
                retest_active = False
                retest_level = 0.0
                retest_until = -1
                break_i3 = -1
                break_low_min = 0.0
                break_type = None
                if args.log_gates:
                    gate_counts["zone_touch"] += 1
                continue

            # 15m bearish + close below EMA20
            idx_15m = int(np.searchsorted(ts_15m, ts, side="right") - 1)
            if idx_15m < 2:
                continue
            h15_0 = float(df_15m.at[idx_15m, "high"])
            h15_1 = float(df_15m.at[idx_15m - 1, "high"])
            h15_2 = float(df_15m.at[idx_15m - 2, "high"])
            close15 = float(df_15m.at[idx_15m, "close"])
            open15 = float(df_15m.at[idx_15m, "open"])
            ema20_15m_ok = False
            try:
                if len(df_15m) >= 20:
                    ema20_15m = _ema(df_15m["close"], 20)
                    ema20_15m_ok = close15 < float(ema20_15m.iloc[idx_15m])
            except Exception:
                ema20_15m_ok = False
            if not ema20_15m_ok:
                if args.log_gates:
                    gate_counts["lh_15m"] += 1
                continue
            if not (close15 < open15):
                if args.log_gates:
                    gate_counts["lh_15m"] += 1
                continue
            # 15m EMA60/EMA120 slope filter (skip if steeply rising)
            try:
                ema60 = _ema(df_15m["close"], int(args.ema60_15m_len))
                ema120 = _ema(df_15m["close"], int(args.ema120_15m_len))
                if len(ema60) >= 2 and len(ema120) >= 2:
                    ema60_slope = (float(ema60.iloc[idx_15m]) - float(ema60.iloc[idx_15m - 1])) / float(ema60.iloc[idx_15m - 1])
                    ema120_slope = (float(ema120.iloc[idx_15m]) - float(ema120.iloc[idx_15m - 1])) / float(ema120.iloc[idx_15m - 1])
                    if ema60_slope > float(args.ema_slope_min) or ema120_slope > float(args.ema_slope_min):
                        if args.log_gates:
                            gate_counts["ema_slope_block"] += 1
                        continue
            except Exception:
                pass

            # 3m structure break: close < min(low[-3:])
            close_now = float(df_3m.at[i3, "close"])
            open_now = float(df_3m.at[i3, "open"])
            high_now = float(df_3m.at[i3, "high"])
            low_now = float(df_3m.at[i3, "low"])
            low_prev = [
                float(df_3m.at[i3 - 1, "low"]),
                float(df_3m.at[i3 - 2, "low"]),
                float(df_3m.at[i3 - 3, "low"]),
            ]
            low_min = min(low_prev)
            # sanity check: guard against corrupted/unsorted 3m rows
            try:
                max_prev = max(low_prev)
                min_prev = min(low_prev)
                if min_prev > 0 and (low_now > (max_prev * 3.0) or low_now < (min_prev / 3.0)):
                    if args.log_gates:
                        gate_counts["skip_stale_ts"] += 1
                    if args.debug_break and break_debug_count < 10:
                        print(
                            "[BACKTEST][ANOMALY] "
                            f"sym={sym} ts={_ts_kst(ts)} l0={low_now:.6f} "
                            f"l1={low_prev[0]:.6f} l2={low_prev[1]:.6f} l3={low_prev[2]:.6f} "
                            f"low_min={low_min:.6f}"
                        )
                        break_debug_count += 1
                    continue
            except Exception:
                pass
            strong_break = close_now < low_min
            weak_break = float(df_3m.at[i3, "low"]) <= low_min
            if args.disable_weak:
                weak_break = False
            if args.log_gates:
                if strong_break:
                    gate_counts["break_3m_strong"] += 1
                elif weak_break:
                    gate_counts["break_3m_weak"] += 1
            if not strong_break and not weak_break:
                if args.log_gates:
                    gate_counts["break_3m"] += 1
                if args.debug_break and break_debug_count < 10:
                    l1 = float(df_3m.at[i3 - 1, "low"])
                    l2 = float(df_3m.at[i3 - 2, "low"])
                    l3 = float(df_3m.at[i3 - 3, "low"])
                    l4 = float(df_3m.at[i3 - 4, "low"]) if i3 - 4 >= 0 else float("nan")
                    c0 = float(df_3m.at[i3, "close"])
                    print(
                        "[BACKTEST][BREAK_DEBUG] "
                        f"sym={sym} ts={_ts_kst(ts)} "
                        f"l0={float(df_3m.at[i3, 'low']):.6f} l1={l1:.6f} l2={l2:.6f} l3={l3:.6f} l4={l4:.6f} "
                        f"close0={c0:.6f} low_min={low_min:.6f} "
                        f"strong={int(strong_break)} weak={int(weak_break)} "
                        f"df_low_last={float(df_3m.at[i3, 'low']):.6f} "
                        f"df_close_last={float(df_3m.at[i3, 'close']):.6f} "
                        f"df_ts_last={int(df_3m.at[i3, 'ts'])}"
                    )
                    break_debug_count += 1
                continue
            # arm retest after break (before any immediate-entry tracks)
            if not retest_active or i3 > retest_until:
                retest_level = low_min
                retest_active = True
                retest_bars = int(args.retest_bars)
                if args.retest_dyn:
                    try:
                        atr3 = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                        atr15 = float(atr_15m.iloc[idx_15m]) if idx_15m >= 0 and not np.isnan(atr_15m.iloc[idx_15m]) else 0.0
                        if atr15 > 0 and (atr3 / atr15) < float(args.retest_dyn_th):
                            retest_bars = max(retest_bars, int(args.retest_dyn_bars))
                    except Exception:
                        pass
                retest_until = i3 + retest_bars
                break_i3 = i3
                break_low_min = low_min
                retest_is_strong = strong_break
                if args.debug_retest:
                    print(
                        "[BACKTEST][RET_ARM] "
                        f"sym={sym} ts={_ts_kst(ts)} level={retest_level:.6f} until={retest_until} "
                        f"low_min={low_min:.6f} strong={int(strong_break)} weak={int(weak_break)}"
                    )
            # ATR filter (skip low-volatility regime)
            try:
                atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                atr_ma = float(atr_3m.rolling(int(args.atr_filter_len)).mean().iloc[i3]) if len(atr_3m) >= int(args.atr_filter_len) else 0.0
                if atr_ma > 0 and atr_now <= (atr_ma * float(args.atr_filter_mult)):
                    if args.log_gates:
                        gate_counts["atr_filter"] += 1
                    continue
            except Exception:
                atr_now = 0.0
            def _apply_sl_cap(entry_px: float, sl_price: float, atr_now: float) -> float:
                cap_pct = float(args.sl_cap_pct)
                if entry_px > 0 and atr_now > 0:
                    cap_pct = max(cap_pct, (atr_now * float(args.sl_cap_atr_mult)) / entry_px)
                return min(sl_price, entry_px * (1.0 + cap_pct))

            def _ltf_sr_bias_pass(entry_px: float, track: str = "") -> bool:
                if not args.ltf_sr_bias:
                    return True
                lb = max(5, int(args.ltf_sr_lookback))
                start = max(0, i3 - lb + 1)
                seg = df_3m.iloc[start : i3 + 1]
                if seg.empty:
                    return True
                try:
                    sup = float(seg["low"].astype(float).min())
                    res = float(seg["high"].astype(float).max())
                except Exception:
                    return True
                if (not np.isfinite(sup)) or (not np.isfinite(res)) or res <= sup:
                    return True
                dist_res = abs(res - entry_px)
                dist_sup = abs(entry_px - sup)
                passed = dist_res <= dist_sup
                if not passed and args.log_gates:
                    gate_counts["ltf_sr_bias_block"] += 1
                if (not passed) and args.debug_break:
                    print(
                        "[BACKTEST][LTF_SR_BLOCK] "
                        f"sym={sym} ts={_ts_kst(ts)} track={track} "
                        f"entry={entry_px:.6f} sup={sup:.6f} res={res:.6f} "
                        f"dist_res={dist_res:.6f} dist_sup={dist_sup:.6f}"
                    )
                return passed

            retest_touch = False
            touch_reclaim_fail = False
            reclaim_up_buf = 0.0
            approach_tol = 0.0
            if retest_active and i3 <= retest_until:
                retest_touch_mult = float(args.retest_atr_mult) if retest_is_strong else 0.5
                reclaim_up_buf = atr_now * float(args.reclaim_up_atr_mult)
                approach_tol = atr_now * retest_touch_mult
                touch_approach = high_now >= (retest_level - approach_tol)
                touch_reclaim_fail = (
                    high_now >= (retest_level + reclaim_up_buf)
                    and close_now < retest_level
                    and close_now < float(df_3m.at[i3, "open"])
                )
                retest_touch = touch_approach or touch_reclaim_fail
                if args.debug_retest:
                    print(
                        "[BACKTEST][RET_TOUCH_EVAL] "
                        f"sym={sym} ts={_ts_kst(ts)} level={retest_level:.6f} "
                        f"high={high_now:.6f} low={low_now:.6f} close={close_now:.6f} "
                        f"atr3={atr_now:.6f} approach_tol={approach_tol:.6f} "
                        f"reclaim_up={reclaim_up_buf:.6f} touch={int(retest_touch)}"
                    )
                if retest_touch:
                    if args.log_gates:
                        gate_counts["retest_seen"] += 1
                    if args.debug_retest:
                        touch_type = "reclaim_fail" if touch_reclaim_fail else "approach"
                        print(
                            "[BACKTEST][RET_TOUCH] "
                            f"sym={sym} ts={_ts_kst(ts)} type={touch_type} "
                            f"level={retest_level:.6f} high={high_now:.6f} close={close_now:.6f} "
                            f"approach_tol={approach_tol:.6f} reclaim_up={reclaim_up_buf:.6f}"
                        )
            elif retest_active and i3 > retest_until:
                if args.log_gates:
                    gate_counts["retest_fail_far"] += 1
                retest_active = False

            # DVF pending confirm (next bar check)
            if dvf_pending_active and (not args.require_retest_touch or retest_touch):
                if i3 > dvf_pending_until:
                    dvf_pending_active = False
                    dvf_pending_type = None
                else:
                    if low_now < dvf_pending_level or close_now < dvf_pending_level:
                        entry_px = float(df_3m.at[i3 + 1, "open"])
                        if not _ltf_sr_bias_pass(entry_px, track=str(dvf_pending_type or "dvf_pending")):
                            continue
                        nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                        sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                        sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                        sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                        tp_price = entry_px * float(args.tp_mult)
                        track = "dvf_accel" if dvf_pending_type == "dvf_accel" else "dvf_slope"
                        trade = {
                            "entry_px": entry_px,
                            "sl_price": sl_price,
                            "tp_price": tp_price,
                            "mfe": 0.0,
                            "mae": 0.0,
                            "hold_bars": 0,
                            "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                            "track": track,
                        }
                        stats["entries"] += 1
                        sym_stats["entries"] += 1
                        entry_symbols.add(sym)
                        if args.log_gates:
                            if track == "dvf_accel":
                                gate_counts["entry_by_dvf_accel"] += 1
                            else:
                                gate_counts["entry_by_dvf_slope"] += 1
                        _record_entry_ts(trade["entry_ts"])
                        _log_signal_ctx(track, nearest, entry_px, atr_now, extra="pending_confirm=1")
                        dvf_pending_active = False
                        dvf_pending_type = None
                        retest_active = False
                        continue
            def _log_signal_ctx(track: str, nearest_zone, entry_px: float, atr_now: float | None = None, extra: str = "") -> None:
                try:
                    parts = [
                        "[BACKTEST][SIGNAL_CTX]",
                        f"sym={sym}",
                        f"track={track}",
                        f"h1_close={h1_close:.6f}",
                        f"h1_high={h1_high:.6f}",
                        f"h1_low={h1_low:.6f}",
                        f"dvf_norm={dvf_norm:.4f}",
                        f"zone_mid={nearest_zone.mid:.6f}",
                        f"zone_bot={nearest_zone.bot:.6f}",
                        f"zone_top={nearest_zone.top:.6f}",
                        f"h15_0={h15_0:.6f}",
                        f"h15_1={h15_1:.6f}",
                        f"h15_2={h15_2:.6f}",
                        f"c3={close_now:.6f}",
                        f"o3={float(df_3m.at[i3, 'open']):.6f}",
                        f"h3={high_now:.6f}",
                        f"l3={low_now:.6f}",
                        f"low_min={low_min:.6f}",
                        f"strong={int(strong_break)}",
                        f"weak={int(weak_break)}",
                        f"retest_level={retest_level:.6f}",
                        f"retest_until={retest_until}",
                        f"df_low_last={float(df_3m.at[i3, 'low']):.6f}",
                        f"df_close_last={float(df_3m.at[i3, 'close']):.6f}",
                        f"df_ts_last={int(df_3m.at[i3, 'ts'])}",
                    ]
                    if isinstance(atr_now, (int, float)):
                        parts.append(f"atr3={atr_now:.6f}")
                    if extra:
                        parts.append(extra)
                    print(" ".join(parts))
                except Exception:
                    pass
            # Big bear break candle -> immediate entry (skip retest)
            try:
                if args.require_retest_touch and not retest_touch:
                    if args.log_gates:
                        gate_counts["retest_fail_no_touch"] += 1
                    continue
                body = abs(close_now - open_now)
                bodies = (df_3m["close"] - df_3m["open"]).abs()
                avg_body = float(bodies.iloc[i3-6:i3].mean()) if i3 >= 6 else float(bodies.iloc[:i3].mean())
                if avg_body > 0 and close_now < open_now and body >= (avg_body * float(args.big_bear_body_mult)):
                    entry_px = float(df_3m.at[i3 + 1, "open"])
                    if not _ltf_sr_bias_pass(entry_px, track="big_bear"):
                        continue
                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                    sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                    sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                    sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                    tp_price = entry_px * float(args.tp_mult)
                    trade = {
                        "entry_px": entry_px,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "hold_bars": 0,
                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                        "track": "big_bear",
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    entry_symbols.add(sym)
                    if args.log_gates:
                        gate_counts["entry_by_big_bear"] += 1
                    _record_entry_ts(trade["entry_ts"])
                    _log_signal_ctx(
                        "big_bear",
                        nearest,
                        entry_px,
                        atr_now,
                        extra=f"body={body:.6f} avg_body={avg_body:.6f} body_mult={body/avg_body if avg_body>0 else 0:.3f}",
                    )
                    retest_active = False
                    continue
            except Exception:
                pass
            # DVF acceleration -> immediate entry (skip retest)
            try:
                if dvf_norm <= float(args.dvf_norm_immediate):
                    if args.require_retest_touch and not retest_touch:
                        if args.log_gates:
                            gate_counts["retest_fail_no_touch"] += 1
                        continue
                    if int(args.dvf_confirm_bars) > 0:
                        dvf_pending_active = True
                        dvf_pending_until = i3 + int(args.dvf_confirm_bars)
                        dvf_pending_level = low_min
                        dvf_pending_type = "dvf_accel"
                        continue
                    entry_px = float(df_3m.at[i3 + 1, "open"])
                    if not _ltf_sr_bias_pass(entry_px, track="dvf_accel"):
                        continue
                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                    sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                    sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                    sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                    tp_price = entry_px * float(args.tp_mult)
                    trade = {
                        "entry_px": entry_px,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "hold_bars": 0,
                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                        "track": "dvf_accel",
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    entry_symbols.add(sym)
                    if args.log_gates:
                        gate_counts["entry_by_dvf_accel"] += 1
                    _record_entry_ts(trade["entry_ts"])
                    _log_signal_ctx("dvf_accel", nearest, entry_px, atr_now)
                    retest_active = False
                    continue
            except Exception:
                pass
            if args.log_gates:
                if strong_break:
                    gate_counts["break_3m_strong"] += 1
                elif weak_break:
                    gate_counts["break_3m_weak"] += 1

            # DVF slope acceleration -> immediate entry (skip retest)
            try:
                if dvf_norm_diff <= float(args.dvf_norm_diff_th):
                    if args.require_retest_touch and not retest_touch:
                        if args.log_gates:
                            gate_counts["retest_fail_no_touch"] += 1
                        continue
                    if int(args.dvf_confirm_bars) > 0:
                        dvf_pending_active = True
                        dvf_pending_until = i3 + int(args.dvf_confirm_bars)
                        dvf_pending_level = low_min
                        dvf_pending_type = "dvf_slope"
                        continue
                    entry_px = float(df_3m.at[i3 + 1, "open"])
                    if not _ltf_sr_bias_pass(entry_px, track="dvf_slope"):
                        continue
                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                    sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                    sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                    sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                    tp_price = entry_px * float(args.tp_mult)
                    trade = {
                        "entry_px": entry_px,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "hold_bars": 0,
                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                        "track": "dvf_slope",
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    entry_symbols.add(sym)
                    if args.log_gates:
                        gate_counts["entry_by_dvf_slope"] += 1
                    _record_entry_ts(trade["entry_ts"])
                    _log_signal_ctx("dvf_slope", nearest, entry_px, atr_now)
                    retest_active = False
                    continue
            except Exception:
                pass

            if retest_active and i3 <= retest_until:
                high_now = float(df_3m.at[i3, "high"])
                low_now = float(df_3m.at[i3, "low"])
                atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0
                retest_touch_mult = float(args.retest_atr_mult) if strong_break else 0.5
                # retest_touch computed earlier in loop
                # timeout chase: if no retest and new low within N bars, enter
                if (
                    not retest_touch
                    and break_i3 >= 0
                    and (i3 - break_i3) <= int(args.retest_timeout_bars)
                    and low_now < break_low_min
                ):
                    if args.require_retest_touch:
                        if args.log_gates:
                            gate_counts["retest_fail_far"] += 1
                        retest_active = False
                        continue
                    entry_px = float(df_3m.at[i3 + 1, "open"])
                    if not _ltf_sr_bias_pass(entry_px, track="timeout"):
                        continue
                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                    sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                    sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                    sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                    tp_price = entry_px * float(args.tp_mult)
                    trade = {
                        "entry_px": entry_px,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "hold_bars": 0,
                        "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                        "track": "timeout",
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    entry_symbols.add(sym)
                    if args.log_gates:
                        gate_counts["entry_by_timeout"] += 1
                    _record_entry_ts(trade["entry_ts"])
                    _log_signal_ctx("timeout", nearest, entry_px, atr_now)
                    retest_active = False
                    continue

                if retest_touch:
                    rng = float(df_3m.at[i3, "high"]) - float(df_3m.at[i3, "low"])
                    upper_wick = float(df_3m.at[i3, "high"]) - max(float(df_3m.at[i3, "open"]), float(df_3m.at[i3, "close"]))
                    wick_ratio = (upper_wick / rng) if rng > 0 else 0.0
                    entry_px = float(df_3m.at[i3 + 1, "open"])
                    if not _ltf_sr_bias_pass(entry_px, track="retest"):
                        continue
                    nearest = min(resist_candidates, key=lambda z: abs(z.mid - entry_px))
                    sl_raw = nearest.top + (atr_now * float(args.sl_atr_mult))
                    sl_price = max(sl_raw, entry_px + (atr_now * 1.0))
                    sl_price = _apply_sl_cap(entry_px, sl_price, atr_now)
                    tp_price = entry_px * float(args.tp_mult)
                    if close_now < retest_level:
                        if args.log_gates:
                            gate_counts["retest_pass_close"] += 1
                    elif low_now < retest_level and close_now < float(df_3m.at[i3, "open"]) and wick_ratio <= float(args.retest_wick_max):
                        if args.log_gates:
                            gate_counts["retest_pass_low"] += 1
                    elif high_now < retest_level + (atr_now * float(args.shallow_atr_mult)) and close_now < float(df_3m.at[i3, "open"]) and dvf_norm <= float(args.shallow_dvf_max) and wick_ratio <= float(args.shallow_wick_max):
                        if args.log_gates:
                            gate_counts["retest_pass_low"] += 1
                    else:
                        continue
                if not retest_touch:
                    continue
                trade = {
                    "entry_px": entry_px,
                    "sl_price": sl_price,
                    "tp_price": tp_price,
                    "mfe": 0.0,
                    "mae": 0.0,
                    "hold_bars": 0,
                    "entry_ts": int(df_3m.at[i3 + 1, "ts"]),
                    "track": "strong" if strong_break else "weak",
                }
                stats["entries"] += 1
                sym_stats["entries"] += 1
                entry_symbols.add(sym)
                _record_entry_ts(trade["entry_ts"])
                _log_signal_ctx("strong" if strong_break else "weak", nearest, entry_px, atr_now)
                if args.log_gates:
                    if close_now < retest_level:
                        gate_counts["entry_by_pass_close"] += 1
                    else:
                        gate_counts["entry_by_pass_low"] += 1
                    if strong_break:
                        gate_counts["entries_strong"] += 1
                    else:
                        gate_counts["entries_weak"] += 1
                retest_active = False
                if i3 >= retest_until:
                    if args.log_gates:
                        gate_counts["retest_fail_far"] += 1
                    retest_active = False

        if trade:
            last_idx = len(df_3m) - 2 if args.use_confirmed else len(df_3m) - 1
            last_idx = max(0, last_idx)
            last_px = float(df_3m.at[last_idx, "close"])
            last_ts = int(df_3m.at[last_idx, "ts"])
            unrealized_pct = (trade["entry_px"] - last_px) / trade["entry_px"] * 100.0
            open_logs.append(
                {
                    "sym": sym,
                    "mode": "sr_pro_short_v1",
                    "side": "SHORT",
                    "entry_ts": trade["entry_ts"],
                    "entry_px": trade["entry_px"],
                    "last_px": last_px,
                    "last_ts": last_ts,
                    "unrealized_pct": unrealized_pct,
                }
            )

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
        else:
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
        sym_items: List[dict] = []
        sym_items.extend([ex for ex in exit_logs if ex["sym"] == sym])
        sym_items.extend([op for op in open_logs if op["sym"] == sym])
        sym_items.sort(key=lambda x: x["entry_ts"], reverse=True)
        for item in sym_items:
            if "exit_ts" in item:
                print(
                    "[BACKTEST][EXIT] "
                    f"sym={item['sym']} mode={item['mode']} side={item['side']} "
                    f"entry_dt={_minute_str(item['entry_ts'])} exit_dt={_minute_str(item['exit_ts'])} "
                    f"entry_px={item['entry_px']:.6f} exit_px={item['exit_px']:.6f} "
                    f"reason={item['reason']} result={item.get('result','')} "
                    f"tp_pct={item.get('tp_pct',0.0):.2f} sl_pct={item.get('sl_pct',0.0):.2f}"
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
    print(
        _fmt_summary_line(
            None,
            stats,
            float(args.entry_usdt),
            total_last_day_exits,
            len(entry_symbols),
        )
    )
    if args.verbose:
        print(
            "[BACKTEST] GATE_COUNTS "
            f"zone_touch={gate_counts['zone_touch']} "
            f"lh_15m={gate_counts['lh_15m']} "
            f"break_3m={gate_counts['break_3m']} "
            f"break_strong={gate_counts['break_3m_strong']} "
            f"break_weak={gate_counts['break_3m_weak']} "
            f"retest_seen={gate_counts['retest_seen']} "
            f"retest_pass_close={gate_counts['retest_pass_close']} "
            f"retest_pass_low={gate_counts['retest_pass_low']} "
            f"retest_fail_far={gate_counts['retest_fail_far']} "
            f"retest_fail_shallow={gate_counts['retest_fail_shallow']} "
            f"retest_fail_no_touch={gate_counts['retest_fail_no_touch']} "
            f"entry_by_dvf_accel={gate_counts['entry_by_dvf_accel']} "
            f"entry_by_dvf_slope={gate_counts['entry_by_dvf_slope']} "
            f"entry_by_timeout={gate_counts['entry_by_timeout']} "
            f"entry_by_pass_close={gate_counts['entry_by_pass_close']} "
            f"entry_by_pass_low={gate_counts['entry_by_pass_low']} "
            f"reject_pass_1h={gate_counts['reject_pass_1h']} "
            f"reject_pass_15m={gate_counts['reject_pass_15m']} "
            f"skip_stale_ts={gate_counts['skip_stale_ts']} "
            f"ema200_pass={gate_counts['ema200_pass']} "
            f"entries_strong={gate_counts['entries_strong']} "
            f"entries_weak={gate_counts['entries_weak']} "
            f"wins_strong={gate_counts['wins_strong']} "
            f"wins_weak={gate_counts['wins_weak']} "
            f"losses_strong={gate_counts['losses_strong']} "
            f"losses_weak={gate_counts['losses_weak']} "
            f"net_strong={gate_counts['net_strong']:.3f} "
            f"net_weak={gate_counts['net_weak']:.3f} "
            f"mfe_strong={gate_counts['mfe_strong_sum']:.3f} "
            f"mfe_weak={gate_counts['mfe_weak_sum']:.3f} "
            f"mae_strong={gate_counts['mae_strong_sum']:.3f} "
            f"mae_weak={gate_counts['mae_weak_sum']:.3f} "
            f"hold_strong={gate_counts['hold_strong_sum']:.1f} "
            f"hold_weak={gate_counts['hold_weak_sum']:.1f} "
            f"ltf_sr_bias_block={gate_counts['ltf_sr_bias_block']}"
        )

    print("[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate")
    for hour in range(24):
        bucket = hour_stats.get(hour, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl = bucket["sl"]
        exits = bucket["tp"] + sl
        sl_rate = (sl / exits * 100.0) if exits > 0 else 0.0
        print(
            f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={bucket['tp']} "
            f"sl={sl} sl_rate={sl_rate:.2f}%"
        )

    print("[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate")
    for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        bucket = dow_stats.get(dow, {"entries": 0, "tp": 0, "sl": 0})
        entries = bucket["entries"]
        sl = bucket["sl"]
        exits = bucket["tp"] + sl
        sl_rate = (sl / exits * 100.0) if exits > 0 else 0.0
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

    if args.verbose and entries_by_day:
        print("[BACKTEST] ENTRIES_BY_DAY")
        for day in sorted(entries_by_day.keys()):
            print(f"[BACKTEST] {day} entries={entries_by_day[day]}")

    if args.zones_snapshot_out:
        try:
            out_dir = os.path.dirname(args.zones_snapshot_out)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with open(args.zones_snapshot_out, "w", encoding="utf-8") as f:
                json.dump(zones_snapshot_out, f, ensure_ascii=False)
            print(
                f"[BACKTEST] zones_snapshot_out saved={args.zones_snapshot_out} "
                f"symbols={len(zones_snapshot_out)}"
            )
        except OSError as exc:
            print(f"[BACKTEST] zones_snapshot_out_error={exc}")


def _ts_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return (dt + pd.Timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    run_backtest()
