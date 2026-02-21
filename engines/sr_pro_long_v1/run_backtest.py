from __future__ import annotations

import argparse
import json
import csv
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from env_loader import load_env

load_env(os.path.join(ROOT, ".env"))


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


def _iso_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + pd.Timedelta(hours=9)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")


def _minute_str(ts_ms: int) -> str:
    return _ts_kst(ts_ms)


def _latest_even_day_0030_kst_anchor_ms(now_ms: int) -> int:
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).astimezone(kst)
    anchor_hour = max(
        0,
        min(
            23,
            int(
                os.getenv(
                    "SR_PRO_LONG_V1_FIXED_ANCHOR_KST_HOUR",
                    os.getenv("SR_PRO_LONG_V1_BT_FIXED_ANCHOR_KST_HOUR", "9"),
                )
                or 9
            ),
        ),
    )
    anchor_minute = max(
        0,
        min(
            59,
            int(
                os.getenv(
                    "SR_PRO_LONG_V1_FIXED_ANCHOR_KST_MINUTE",
                    os.getenv("SR_PRO_LONG_V1_BT_FIXED_ANCHOR_KST_MINUTE", "0"),
                )
                or 0
            ),
        ),
    )
    anchor_kst = now_kst.replace(hour=anchor_hour, minute=anchor_minute, second=0, microsecond=0)
    if now_kst < anchor_kst:
        anchor_kst -= timedelta(days=1)
    while (anchor_kst.day % 2) != 0:
        anchor_kst -= timedelta(days=1)
    return int(anchor_kst.astimezone(timezone.utc).timestamp() * 1000)


def _even_day_anchor_series_ms(eval_start_ms: int, end_ms: int) -> List[int]:
    if end_ms <= 0:
        return []
    first = _latest_even_day_0030_kst_anchor_ms(eval_start_ms)
    out: List[int] = []
    cur = first
    step = 2 * 24 * 60 * 60 * 1000
    while cur < end_ms:
        out.append(int(cur))
        cur += step
    if not out:
        out.append(int(first))
    return sorted(set(out))


def _latest_daily_0030_kst_anchor_ms(now_ms: int) -> int:
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).astimezone(kst)
    anchor_hour = max(
        0,
        min(
            23,
            int(
                os.getenv(
                    "SR_PRO_LONG_V1_FIXED_ANCHOR_KST_HOUR",
                    os.getenv("SR_PRO_LONG_V1_BT_FIXED_ANCHOR_KST_HOUR", "9"),
                )
                or 9
            ),
        ),
    )
    anchor_minute = max(
        0,
        min(
            59,
            int(
                os.getenv(
                    "SR_PRO_LONG_V1_FIXED_ANCHOR_KST_MINUTE",
                    os.getenv("SR_PRO_LONG_V1_BT_FIXED_ANCHOR_KST_MINUTE", "0"),
                )
                or 0
            ),
        ),
    )
    anchor_kst = now_kst.replace(hour=anchor_hour, minute=anchor_minute, second=0, microsecond=0)
    if now_kst < anchor_kst:
        anchor_kst -= timedelta(days=1)
    return int(anchor_kst.astimezone(timezone.utc).timestamp() * 1000)


def _daily_anchor_series_ms(eval_start_ms: int, end_ms: int) -> List[int]:
    if end_ms <= 0:
        return []
    first = _latest_daily_0030_kst_anchor_ms(eval_start_ms)
    out: List[int] = []
    cur = first
    step = 24 * 60 * 60 * 1000
    while cur < end_ms:
        out.append(int(cur))
        cur += step
    if not out:
        out.append(int(first))
    return sorted(set(out))


def _load_common_universe_from_snapshot_log(
    anchor_ms: int,
    top_n: int,
    logs_dir: str = os.path.join("logs", "common_universe"),
) -> tuple[List[str], str]:
    if not os.path.isdir(logs_dir):
        return [], ""
    candidates: List[tuple[float, str]] = []

    def _parse_name_ts(name: str) -> float:
        try:
            # Fallback-only: filename timestamp may drift from actual refresh timing.
            stem = str(name).removesuffix(".log")
            ts_part = stem.replace("common_universe_", "", 1)
            dt_kst = datetime.strptime(ts_part, "%Y%m%d_%H%M%S").replace(
                tzinfo=timezone(timedelta(hours=9))
            )
            return float(dt_kst.timestamp())
        except Exception:
            return 0.0

    try:
        for name in os.listdir(logs_dir):
            if not (name.startswith("common_universe_") and name.endswith(".log")):
                continue
            full = os.path.join(logs_dir, name)
            # Match live selector: filename timestamp first, then mtime fallback.
            ts_sec = _parse_name_ts(name)
            if ts_sec <= 0:
                try:
                    ts_sec = float(os.path.getmtime(full))
                except Exception:
                    continue
            candidates.append((ts_sec, full))
    except Exception:
        return [], ""

    if not candidates:
        return [], ""

    cutoff_sec = (anchor_ms / 1000.0) if isinstance(anchor_ms, (int, float)) and anchor_ms > 0 else 0.0
    before_or_eq = [c for c in candidates if c[0] <= cutoff_sec] if cutoff_sec > 0 else []
    if before_or_eq:
        before_or_eq.sort(key=lambda x: x[0], reverse=True)
        chosen = before_or_eq[0][1]
    else:
        candidates.sort(key=lambda x: x[0], reverse=True)
        chosen = candidates[0][1]

    out: List[str] = []
    try:
        with open(chosen, "r", encoding="utf-8") as f:
            for line in f:
                sym = line.strip()
                if not sym:
                    continue
                if sym.startswith("COMMON_UNIVERSE"):
                    continue
                out.append(sym)
    except Exception:
        return [], chosen
    if top_n > 0:
        out = out[:top_n]
    return out, chosen


def _build_fixed_anchor_replay_schedule(
    eval_start_ms: int,
    end_ms: int,
    top_n: int,
    cadence: str = "even",
    cadence_days: int = 1,
    cadence_phase: int = 0,
) -> List[dict]:
    cadence_key = str(cadence or "even").lower()
    # Include at least one anchor before eval_start so pre-anchor window
    # (e.g. 00:00~08:59 KST) uses the same fixed universe/zone anchor as live.
    lookback_days = max(2, int(cadence_days or 1) + 2)
    anchors_start_ms = int(eval_start_ms - lookback_days * 24 * 60 * 60 * 1000)
    if cadence_key == "daily":
        anchors = _daily_anchor_series_ms(anchors_start_ms, end_ms)
    else:
        anchors = _even_day_anchor_series_ms(anchors_start_ms, end_ms)
    cadence_days = max(1, int(cadence_days or 1))
    cadence_phase = max(0, int(cadence_phase or 0)) % cadence_days
    if cadence_days > 1 and anchors:
        filtered: List[int] = []
        for a_ms in anchors:
            dt_kst = datetime.fromtimestamp(int(a_ms) / 1000.0, tz=timezone.utc) + timedelta(hours=9)
            day = int(dt_kst.day)
            if ((day - 1 - cadence_phase) % cadence_days) == 0:
                filtered.append(int(a_ms))
        anchors = filtered if filtered else [int(anchors[0])]
    if not anchors:
        return []
    sched: List[dict] = []
    for i, a_ms in enumerate(anchors):
        end = int(end_ms if i + 1 >= len(anchors) else anchors[i + 1])
        if end <= int(eval_start_ms):
            continue
        start = int(max(eval_start_ms, a_ms))
        if end <= start:
            continue
        uni, src = _load_common_universe_from_snapshot_log(a_ms, top_n=top_n)
        sched.append(
            {
                "anchor_ms": int(a_ms),
                "start_ms": int(start),
                "end_ms": int(end),
                "universe": list(uni),
                "source": src,
            }
        )
    return sched


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
    cfg_live = SrProLongV1Config()
    parser = argparse.ArgumentParser("sr_pro_long_v1 backtest")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--universe", type=str, default="common")
    parser.add_argument("--use-confirmed", action="store_true", default=True)
    parser.add_argument("--no-use-confirmed", action="store_false", dest="use_confirmed")
    parser.add_argument("--use-live-cache", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--common-only", action="store_true")
    parser.add_argument("--live-parity", action="store_true", help="Force live-equivalent data/parity mode.")
    parser.add_argument(
        "--entry-model",
        type=str,
        default="auto",
        choices=["auto", "next_open", "signal_close"],
        help="Entry timing model: auto(live-parity=>signal_close), next_open(legacy), signal_close(immediate).",
    )
    parser.add_argument(
        "--bootstrap-bars",
        type=int,
        default=int(os.getenv("SR_PRO_LONG_V1_BOOTSTRAP_BARS", "1440") or 1440),
        help="Pre-roll bars for --live-parity (ltf bars).",
    )
    parser.add_argument("--auto-fill-cache", action="store_true", default=False)
    parser.add_argument("--common-warmup-dir", type=str, default="")
    parser.add_argument(
        "--top-n",
        type=int,
        default=int(
            os.getenv(
                "SR_PRO_LONG_V1_FIXED_UNIVERSE_TOP_N",
                os.getenv("COMMON_UNIVERSE_TOP_N", "50"),
            )
            or 50
        ),
    )
    parser.add_argument("--lookback", type=int, default=int(cfg_live.lookback))
    parser.add_argument("--relaxed-lookback", type=int, default=int(cfg_live.relaxed_lookback))
    parser.add_argument("--auto-relax", action="store_true", dest="auto_relax")
    parser.add_argument("--no-auto-relax", action="store_false", dest="auto_relax")
    parser.set_defaults(auto_relax=bool(cfg_live.auto_relax))
    parser.add_argument("--atr-mult", type=float, default=float(cfg_live.atr_mult))
    parser.add_argument("--delta-len", type=int, default=int(cfg_live.delta_len))
    parser.add_argument("--cluster-atr", type=float, default=float(cfg_live.cluster_atr))
    parser.add_argument("--max-zones-per-side", type=int, default=int(cfg_live.max_zones_per_side))
    parser.add_argument("--touch-mode", type=str, default=str(cfg_live.touch_mode), choices=["top", "mid"])
    parser.add_argument("--touch-use-close", action="store_true")
    parser.add_argument("--dvf-norm-min", type=float, default=float(cfg_live.dvf_norm_min))
    parser.add_argument("--require-reject-close", action="store_true")
    parser.add_argument("--reject-mode", type=str, default="top", choices=["top", "mid"])
    parser.add_argument("--reject-source", type=str, default="1h", choices=["1h", "15m"])
    parser.add_argument(
        "--zone-accept-mode",
        type=str,
        default=str(getattr(cfg_live, "zone_accept_mode", "off")),
        choices=["off", "mid", "top", "top_bull"],
        help="Support zone acceptance on latest 1h bar: off/mid/top/top_bull(close>open).",
    )
    parser.add_argument(
        "--ema200-filter",
        action="store_true",
        default=bool(getattr(cfg_live, "ema200_filter", True)),
    )
    parser.add_argument("--no-ema200-filter", action="store_false", dest="ema200_filter")
    parser.add_argument("--ema-filter-len", type=int, default=int(cfg_live.ema_filter_len))
    parser.add_argument("--retest-bars", type=int, default=int(cfg_live.retest_bars))
    parser.add_argument("--retest-atr-mult", type=float, default=float(cfg_live.retest_atr_mult))
    parser.add_argument("--retest-breakdown-block-atr", type=float, default=float(getattr(cfg_live, "retest_breakdown_block_atr", 0.03)))
    parser.add_argument("--retest-reclaim-min-atr", type=float, default=float(getattr(cfg_live, "retest_reclaim_min_atr", 0.0)))
    parser.add_argument("--retest-near-atr-mult", type=float, default=float(cfg_live.retest_near_atr_mult))
    parser.add_argument("--retest-wick-max", type=float, default=float(cfg_live.retest_wick_max))
    parser.add_argument("--retest-dyn", action="store_true", default=bool(cfg_live.retest_dyn))
    parser.add_argument("--retest-dyn-th", type=float, default=float(cfg_live.retest_dyn_th))
    parser.add_argument("--retest-dyn-bars", type=int, default=int(cfg_live.retest_dyn_bars))
    parser.add_argument("--pullback-lookback", type=int, default=int(cfg_live.pullback_lookback))
    parser.add_argument("--pullback-min-pct", type=float, default=float(cfg_live.pullback_min_pct))
    parser.add_argument("--pullback-min-atr", type=float, default=float(cfg_live.pullback_min_atr))
    parser.add_argument("--require-sweep-reclaim", action="store_true", default=bool(cfg_live.require_sweep_reclaim))
    parser.add_argument("--no-require-sweep-reclaim", action="store_false", dest="require_sweep_reclaim")
    parser.add_argument("--sweep-lookback", type=int, default=int(cfg_live.sweep_lookback))
    parser.add_argument("--sweep-tol-atr", type=float, default=float(cfg_live.sweep_tol_atr))
    parser.add_argument("--sweep-reclaim-wait-bars", type=int, default=0)
    parser.add_argument("--max-break-ext-atr", type=float, default=float(cfg_live.max_break_ext_atr))
    parser.add_argument("--shallow-atr-mult", type=float, default=float(cfg_live.shallow_atr_mult))
    parser.add_argument("--shallow-wick-max", type=float, default=float(cfg_live.shallow_wick_max))
    parser.add_argument("--shallow-dvf-min", type=float, default=float(cfg_live.shallow_dvf_min))
    parser.add_argument("--entry-ema-len", type=int, default=int(cfg_live.entry_ema_len))
    parser.add_argument("--entry-atr-offset", type=float, default=float(cfg_live.entry_atr_offset))
    parser.add_argument("--entry-atr-offset-weak", type=float, default=float(getattr(cfg_live, "entry_atr_offset_weak", cfg_live.entry_atr_offset)))
    parser.add_argument("--entry-ema-max-dev-pct", type=float, default=float(getattr(cfg_live, "entry_ema_max_dev_pct", 0.0)))
    parser.add_argument("--entry-immediate-on-close-reclaim", action="store_true", default=bool(getattr(cfg_live, "entry_immediate_on_close_reclaim", False)))
    parser.add_argument("--no-entry-immediate-on-close-reclaim", action="store_false", dest="entry_immediate_on_close_reclaim")
    parser.add_argument(
        "--immediate-entry-fill",
        type=str,
        default=os.getenv("SR_PRO_LONG_BT_IMMEDIATE_ENTRY_FILL", "close"),
        choices=["close", "next_open"],
        help="Fill price model for immediate entry: close(current confirmed bar) or next_open(next 3m open).",
    )
    parser.add_argument("--entry-immediate-max-chase-pct", type=float, default=float(getattr(cfg_live, "entry_immediate_max_chase_pct", 0.0)))
    parser.add_argument("--entry-candle-guard", action="store_true", default=bool(getattr(cfg_live, "entry_candle_guard", True)))
    parser.add_argument("--no-entry-candle-guard", action="store_false", dest="entry_candle_guard")
    parser.add_argument("--sl-buffer", type=float, default=float(cfg_live.sl_buffer))
    parser.add_argument("--sl-atr-mult", type=float, default=float(cfg_live.sl_atr_mult))
    parser.add_argument("--sl-cap-pct", type=float, default=float(getattr(cfg_live, "sl_cap_pct", 0.0)))
    parser.add_argument("--sl-cap-atr-mult", type=float, default=float(getattr(cfg_live, "sl_cap_atr_mult", 0.0)))
    parser.add_argument("--tp-atr-mult", type=float, default=0.0)
    parser.add_argument("--tp-atr-mult-weak", type=float, default=0.0)
    parser.add_argument("--tp-mult", type=float, default=float(cfg_live.tp_mult))
    parser.add_argument("--tp-mult-weak", type=float, default=float(cfg_live.tp_mult_weak))
    parser.add_argument("--strong-be-trigger-mult", type=float, default=float(getattr(cfg_live, "strong_be_trigger_mult", 0.0)))
    parser.add_argument("--strong-be-stop-buffer-pct", type=float, default=float(getattr(cfg_live, "strong_be_stop_buffer_pct", 0.0)))
    parser.add_argument("--base-usdt", type=float, default=1000.0)
    parser.add_argument("--entry-usdt", type=float, default=10.0)
    parser.add_argument("--freeze-zones", action="store_true")
    parser.add_argument("--total-window-days", type=int, default=14)
    parser.add_argument("--rolling-zones", action="store_true", default=True)
    parser.add_argument("--no-rolling-zones", action="store_false", dest="rolling_zones")
    parser.add_argument(
        "--fixed-even-day-0030-kst",
        action="store_true",
        help="Fix universe/zones to latest even-day configured KST anchor snapshot (default 09:00).",
    )
    parser.add_argument(
        "--fixed-even-day-0030-kst-replay",
        action="store_true",
        help="Replay mode: refresh anchor/universe every even-day configured KST anchor within eval window.",
    )
    parser.add_argument(
        "--fixed-daily-0030-kst-replay",
        action="store_true",
        help="Replay mode: refresh anchor/universe every day configured KST anchor within eval window.",
    )
    parser.add_argument("--zones-snapshot-in", type=str, default="")
    parser.add_argument("--zones-snapshot-out", type=str, default="")
    parser.add_argument("--log-gates", action="store_true")
    parser.add_argument("--debug-zone", action="store_true")
    parser.add_argument("--ltf-sr-bias", action="store_true")
    parser.add_argument("--ltf-sr-lookback", type=int, default=60)
    parser.add_argument("--debug-symbol", type=str, default="")
    parser.add_argument("--block-hours", type=str, default="")
    parser.add_argument("--cooldown-sec", type=int, default=-1)
    parser.add_argument(
        "--fixed-replay-cadence-days",
        type=int,
        default=int(
            os.getenv(
                "SR_PRO_LONG_V1_FIXED_ANCHOR_CADENCE_DAYS",
                os.getenv("SR_PRO_LONG_V1_BT_FIXED_REPLAY_CADENCE_DAYS", "2"),
            )
            or 2
        ),
    )
    parser.add_argument(
        "--fixed-replay-cadence-phase",
        type=int,
        default=int(
            os.getenv(
                "SR_PRO_LONG_V1_FIXED_ANCHOR_PHASE_DAYS",
                os.getenv("SR_PRO_LONG_V1_BT_FIXED_REPLAY_CADENCE_PHASE", "1"),
            )
            or 1
        ),
    )
    args = parser.parse_args()
    if bool(args.live_parity):
        args.use_live_cache = True
        args.cache_only = True
        args.common_only = True
        args.use_confirmed = True
    entry_model = str(args.entry_model or "auto").strip().lower()
    if entry_model == "auto":
        entry_model = "signal_close" if bool(args.live_parity) else "next_open"
    bt_fixed_default = os.getenv("SR_PRO_LONG_V1_BT_FIXED_ANCHOR", "1").strip().lower() not in ("0", "false", "off", "no")
    live_cadence_days = max(1, int(os.getenv("SR_PRO_LONG_V1_FIXED_ANCHOR_CADENCE_DAYS", "2") or 2))
    default_replay_cadence = "daily" if live_cadence_days == 1 else "even"
    bt_replay_cadence = os.getenv("SR_PRO_LONG_V1_BT_FIXED_REPLAY_CADENCE", default_replay_cadence).strip().lower()
    if (
        bt_fixed_default
        and args.universe in ("common", "common_universe")
        and not args.fixed_even_day_0030_kst
        and not args.fixed_even_day_0030_kst_replay
        and not args.fixed_daily_0030_kst_replay
    ):
        args.fixed_even_day_0030_kst = True
        if bt_replay_cadence == "even":
            args.fixed_even_day_0030_kst_replay = True
        else:
            args.fixed_daily_0030_kst_replay = True

    def _tf_ms(tf: str) -> int:
        try:
            if tf.endswith("m"):
                return int(tf[:-1]) * 60 * 1000
            if tf.endswith("h"):
                return int(tf[:-1]) * 60 * 60 * 1000
            if tf.endswith("d"):
                return int(tf[:-1]) * 24 * 60 * 60 * 1000
        except Exception:
            pass
        return 60 * 1000

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
        entry_atr_offset_weak=args.entry_atr_offset_weak,
    )

    exchange = None if args.cache_only else ccxt.binance({"enableRateLimit": True})
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    anchor_ms = 0
    if args.fixed_even_day_0030_kst:
        if args.fixed_daily_0030_kst_replay and not args.fixed_even_day_0030_kst_replay:
            anchor_ms = _latest_daily_0030_kst_anchor_ms(end_ms)
            anchor_mode = "daily_config_kst"
        else:
            anchor_ms = _latest_even_day_0030_kst_anchor_ms(end_ms)
            anchor_mode = "even_day_config_kst"
        print(
            f"[BACKTEST] fixed_anchor_mode={anchor_mode} "
            f"anchor_kst={_ts_kst(anchor_ms)} anchor_utc={datetime.fromtimestamp(anchor_ms/1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}"
        )
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
    if bool(args.live_parity):
        pre_bars = max(30, int(args.bootstrap_bars))
        start_ms = min(start_ms, int(eval_start_ms - (pre_bars * _tf_ms(cfg_live.tf_ltf))))
        print(
            f"[BACKTEST] live_parity=1 cache_only=1 common_only=1 use_confirmed=1 "
            f"bootstrap_bars={pre_bars}"
        )

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
    replay_schedule: List[dict] = []
    universe: List[str] = []
    if (
        args.fixed_even_day_0030_kst
        and (args.fixed_even_day_0030_kst_replay or args.fixed_daily_0030_kst_replay)
        and args.universe in ("common", "common_universe")
    ):
        cadence = "daily" if args.fixed_daily_0030_kst_replay else "even"
        replay_schedule = _build_fixed_anchor_replay_schedule(
            eval_start_ms=eval_start_ms,
            end_ms=end_ms,
            top_n=args.top_n,
            cadence=cadence,
            cadence_days=max(1, int(args.fixed_replay_cadence_days or 1)),
            cadence_phase=int(args.fixed_replay_cadence_phase or 0),
        )
        union_syms: List[str] = []
        seen_syms = set()
        for seg in replay_schedule:
            for s in seg.get("universe", []):
                if s in seen_syms:
                    continue
                seen_syms.add(s)
                union_syms.append(s)
        universe = union_syms
        print(
            f"[BACKTEST] fixed_replay_schedule cadence={cadence} "
            f"cadence_days={max(1, int(args.fixed_replay_cadence_days or 1))} "
            f"cadence_phase={max(0, int(args.fixed_replay_cadence_phase or 0)) % max(1, int(args.fixed_replay_cadence_days or 1))} "
            f"segments={len(replay_schedule)} "
            f"union_symbols={len(universe)}"
        )
        for seg in replay_schedule:
            print(
                f"[BACKTEST] fixed_replay_seg "
                f"anchor_kst={_ts_kst(int(seg['anchor_ms']))} "
                f"start_kst={_ts_kst(int(seg['start_ms']))} "
                f"end_kst={_ts_kst(int(seg['end_ms']))} "
                f"size={len(seg.get('universe', []))} "
                f"file='{seg.get('source', '')}'"
            )
    elif args.fixed_even_day_0030_kst and args.universe in ("common", "common_universe"):
        universe, chosen_file = _load_common_universe_from_snapshot_log(
            anchor_ms=anchor_ms,
            top_n=args.top_n,
        )
        print(
            f"[BACKTEST] fixed_universe_snapshot "
            f"file='{chosen_file}' size={len(universe)} top_n={args.top_n}"
        )
    if not universe:
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
        "pullback_fail": 0,
        "sweep_reclaim_fail": 0,
        "break_overheat": 0,
        "retest_seen": 0,
        "retest_pass_close": 0,
        "retest_pass_high": 0,
        "retest_fail_shallow": 0,
        "entry_by_pass_close": 0,
        "entry_by_pass_high": 0,
        "entry_immediate": 0,
        "entry_ema_dev_fail": 0,
        "reject_pass_1h": 0,
        "reject_pass_15m": 0,
        "zone_accept_pass": 0,
        "ema200_pass": 0,
        "time_block": 0,
        "ltf_sr_bias_block": 0,
    }

    def _load_admin_runtime_settings_from_db() -> dict:
        db_path = os.getenv("TRADES_DB_PATH", os.path.join("logs", "trades.db")).strip()
        if not db_path or not os.path.exists(db_path):
            return {}
        con = None
        try:
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cols = [r[1] for r in cur.execute("PRAGMA table_info(account_settings)").fetchall()]
            if not cols:
                return {}
            admin_id = None
            try:
                row = cur.execute(
                    "SELECT id FROM accounts WHERE name='admin' ORDER BY id ASC LIMIT 1"
                ).fetchone()
                if row:
                    admin_id = int(row[0])
            except Exception:
                admin_id = None
            if admin_id is None:
                try:
                    row = cur.execute(
                        "SELECT account_id FROM account_settings ORDER BY account_id ASC LIMIT 1"
                    ).fetchone()
                    if row:
                        admin_id = int(row[0])
                except Exception:
                    admin_id = None
            if admin_id is None:
                return {}
            row = cur.execute(
                "SELECT * FROM account_settings WHERE account_id = ? LIMIT 1",
                (admin_id,),
            ).fetchone()
            if not row:
                return {}
            return dict(zip(cols, row))
        except Exception:
            return {}
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    _runtime_db = _load_admin_runtime_settings_from_db()

    def _load_state_runtime_settings() -> dict:
        try:
            if not os.path.exists("state.json"):
                return {}
            with open("state.json", "r", encoding="utf-8") as f:
                st = json.load(f)
            return st if isinstance(st, dict) else {}
        except Exception:
            return {}

    _runtime_state = _load_state_runtime_settings()

    def _load_entry_block_hours() -> set[int]:
        # explicit args -> admin account_settings(DB) -> state.json -> ENV
        if args.block_hours:
            try:
                return {int(h.strip()) for h in args.block_hours.split(",") if h.strip() != ""}
            except Exception:
                return set()
        raw_db = str(_runtime_db.get("entry_block_hours") or "").strip()
        if raw_db:
            try:
                return {int(h.strip()) for h in raw_db.split(",") if h.strip() != ""}
            except Exception:
                pass
        raw_state = str(_runtime_state.get("_entry_block_hours") or _runtime_state.get("entry_block_hours") or "").strip()
        if raw_state:
            try:
                return {int(h.strip()) for h in raw_state.split(",") if h.strip() != ""}
            except Exception:
                pass
        raw_env = os.getenv("ENTRY_BLOCK_HOURS", "").strip()
        if raw_env:
            try:
                return {int(h.strip()) for h in raw_env.split(",") if h.strip() != ""}
            except Exception:
                return set()
        return set()

    def _load_exit_cooldown_sec() -> int:
        if int(args.cooldown_sec) >= 0:
            return int(args.cooldown_sec)
        raw_db_h = _runtime_db.get("exit_cooldown_h")
        if raw_db_h is not None:
            try:
                return max(0, int(float(raw_db_h) * 3600.0))
            except Exception:
                pass
        raw_h = _runtime_state.get("_exit_cooldown_hours", _runtime_state.get("exit_cooldown_h"))
        if raw_h is not None:
            try:
                return max(0, int(float(raw_h) * 3600.0))
            except Exception:
                pass
        raw_sec = _runtime_state.get("_cooldown_sec", _runtime_state.get("cooldown_sec"))
        if raw_sec is not None:
            try:
                return max(0, int(float(raw_sec)))
            except Exception:
                pass
        return 0

    block_hours = _load_entry_block_hours()
    cooldown_ms = max(0, _load_exit_cooldown_sec()) * 1000
    print(
        f"[BACKTEST] runtime_sync entry_block_hours="
        f"{','.join(str(h) for h in sorted(block_hours)) if block_hours else 'none'} "
        f"cooldown_sec={int(cooldown_ms/1000)} "
        f"entry_model={entry_model} "
        f"db_settings={int(bool(_runtime_db))} state_settings={int(bool(_runtime_state))}"
    )

    entries_by_day: Dict[str, int] = {}
    entry_reason_stats: Dict[str, Dict[str, float]] = {}
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
        dbg_on = bool(str(args.debug_symbol or "").strip()) and (sym.upper() == str(args.debug_symbol).strip().upper())
        dbg_counts = {
            "eval_bars": 0,
            "fail_cooldown": 0,
            "fail_time_block": 0,
            "fail_idx_1h": 0,
            "fail_idx_15m": 0,
            "fail_zone": 0,
            "fail_hl_15m": 0,
            "fail_break_3m": 0,
            "fail_retest": 0,
            "fail_entry_target": 0,
            "fail_nearest_zone": 0,
            "entries": 0,
            "exits": 0,
        } if dbg_on else None
        def _dbg(ts_ms: int, stage: str, extra: str = "") -> None:
            if not dbg_on:
                return
            try:
                msg = (
                    f"[BACKTEST][DBG] sym={sym} ts={_iso_kst(ts_ms)} stage={stage}"
                    + (f" {extra}" if extra else "")
                )
                print(msg)
            except Exception:
                pass
        df_3m = frames["3m"]
        df_15m = frames["15m"]
        df_1h = frames["1h"]
        if args.use_confirmed:
            # Align with live: only trim the tail bar when it is still forming.
            for tf_name, df_cur in (("3m", df_3m), ("15m", df_15m), ("1h", df_1h)):
                if df_cur.empty:
                    continue
                tf_ms = _tf_ms(tf_name)
                last_ts = int(df_cur.iloc[-1]["ts"])
                if last_ts and (end_ms - last_ts) < tf_ms:
                    if tf_name == "3m":
                        df_3m = df_cur.iloc[:-1]
                    elif tf_name == "15m":
                        df_15m = df_cur.iloc[:-1]
                    else:
                        df_1h = df_cur.iloc[:-1]

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
        sweep_state_active = False
        sweep_state_level = 0.0
        sweep_state_until = -1
        last_zone_end_idx = None
        replay_anchor_index = 0
        replay_anchor_ms_cur = 0
        replay_anchor_universe: set[str] = set()
        if replay_schedule:
            for idx, seg in enumerate(replay_schedule):
                if int(seg.get("start_ms", 0)) <= eval_start_ms < int(seg.get("end_ms", 0)):
                    replay_anchor_index = idx
                    break
            replay_anchor_ms_cur = int(replay_schedule[replay_anchor_index].get("anchor_ms", 0))
            replay_anchor_universe = set(replay_schedule[replay_anchor_index].get("universe", []))

        for i3 in range(3, len(df_3m) - 1):
            ts = int(ts_3m[i3])
            if ts < eval_start_ms:
                continue
            if replay_schedule:
                while (
                    replay_anchor_index + 1 < len(replay_schedule)
                    and ts >= int(replay_schedule[replay_anchor_index].get("end_ms", end_ms))
                ):
                    replay_anchor_index += 1
                    replay_anchor_ms_cur = int(replay_schedule[replay_anchor_index].get("anchor_ms", 0))
                    replay_anchor_universe = set(replay_schedule[replay_anchor_index].get("universe", []))
                if sym not in replay_anchor_universe and trade is None:
                    continue
            if dbg_on:
                dbg_counts["eval_bars"] += 1
            if sweep_state_active and i3 > sweep_state_until:
                sweep_state_active = False
            cd_until = cooldown_until.get((sym, "LONG"))
            if isinstance(cd_until, int) and ts < cd_until:
                if dbg_on:
                    dbg_counts["fail_cooldown"] += 1
                    _dbg(ts, "cooldown", f"cd_until={_iso_kst(cd_until)}")
                continue

            # i3 bar is confirmed when the next 3m bar opens.
            decision_ts = ts + _tf_ms(cfg.tf_ltf)
            if args.use_confirmed:
                idx_1h = int(np.searchsorted(ts_1h, decision_ts - _tf_ms(cfg.tf_htf), side="right") - 1)
            else:
                idx_1h = int(np.searchsorted(ts_1h, ts, side="right") - 1)
            if idx_1h < 0:
                if dbg_on:
                    dbg_counts["fail_idx_1h"] += 1
                    _dbg(ts, "idx_1h", "idx_1h<0")
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
            elif replay_schedule:
                zone_end_idx = int(np.searchsorted(df_1h["ts"].values, replay_anchor_ms_cur, side="right"))
                if zone_end_idx <= 0:
                    continue
                if last_zone_end_idx != zone_end_idx:
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
                    last_zone_end_idx = zone_end_idx

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
                # Strong-only protective stop raise: once price reaches trigger, lift stop near entry.
                if (
                    str(trade.get("track") or "").lower() == "strong"
                    and float(args.strong_be_trigger_mult) > 0
                    and float(args.strong_be_stop_buffer_pct) >= 0
                    and trade["entry_px"] > 0
                    and high_i >= (trade["entry_px"] * float(args.strong_be_trigger_mult))
                ):
                    be_stop = trade["entry_px"] * (1.0 - float(args.strong_be_stop_buffer_pct))
                    if be_stop > float(trade.get("sl_price", 0.0)):
                        trade["sl_price"] = be_stop
                sl_hit = low_i <= trade["sl_price"]
                tp_hit = high_i >= trade["tp_price"]
                # On same-bar TP/SL touch, force SL-first to avoid optimistic fills.
                if sl_hit:
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
                    er_key = str(trade.get("reason") or "unknown")
                    er_bucket = entry_reason_stats.setdefault(
                        er_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0}
                    )
                    er_bucket["sl"] += 1
                    er_bucket["net_sum"] += pnl_pct
                    er_bucket["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
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
                            "entry_reason": trade.get("reason"),
                            "entry_track": trade.get("track"),
                            "tp_pct": _tp_sl_pct_from_trade(trade)[0],
                            "sl_pct": _tp_sl_pct_from_trade(trade)[1],
                            "pnl_pct": pnl_pct,
                        }
                    )
                    if cooldown_ms > 0:
                        cooldown_until[(sym, "LONG")] = ts + cooldown_ms
                    if dbg_on:
                        dbg_counts["exits"] += 1
                        _dbg(ts, "exit_sl", f"entry_ts={_iso_kst(trade['entry_ts'])} exit_px={exit_px:.6f}")
                    trade = None
                elif tp_hit:
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
                    er_key = str(trade.get("reason") or "unknown")
                    er_bucket = entry_reason_stats.setdefault(
                        er_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0}
                    )
                    er_bucket["tp"] += 1
                    er_bucket["net_sum"] += pnl_pct
                    er_bucket["net_sum_usdt"] += pnl_pct * float(args.entry_usdt)
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
                            "entry_reason": trade.get("reason"),
                            "entry_track": trade.get("track"),
                            "tp_pct": _tp_sl_pct_from_trade(trade)[0],
                            "sl_pct": _tp_sl_pct_from_trade(trade)[1],
                            "pnl_pct": pnl_pct,
                        }
                    )
                    if dbg_on:
                        dbg_counts["exits"] += 1
                        _dbg(ts, "exit_tp", f"entry_ts={_iso_kst(trade['entry_ts'])} exit_px={exit_px:.6f}")
                    trade = None
                continue

            h1_high = float(high_1h.iloc[idx_1h])
            h1_low = float(low_1h.iloc[idx_1h])
            h1_open = float(open_1h.iloc[idx_1h])
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
                    if dbg_on:
                        _dbg(ts, "ema200", f"h1_close={h1_close:.6f} ema200={ema_now:.6f}")
                    continue
                if args.log_gates:
                    gate_counts["ema200_pass"] += 1

            # Only accept zones touched by the latest confirmed 1h bar
            h1_ts = int(df_1h.at[idx_1h, "ts"]) if "ts" in df_1h.columns else 0
            for z in zones:
                if not z.live:
                    setattr(z, "last_touch_ts", 0)
                    continue
                touched_now = (
                    z.side == -1
                    and h1_touch_px <= (z.mid if h1_touch_level == "mid" else z.top)
                    and h1_high >= z.bot
                )
                setattr(z, "last_touch_ts", h1_ts if touched_now else 0)
            support_candidates = [
                z
                for z in zones
                if z.live
                and z.side == -1
                and dvf_norm >= float(args.dvf_norm_min)
                and h1_touch_px <= (z.mid if h1_touch_level == "mid" else z.top)
                and h1_high >= z.bot
                and getattr(z, "last_touch_ts", 0) == h1_ts
            ]
            if support_candidates and args.zone_accept_mode != "off":
                def _zone_accept_ok(z: Zone) -> bool:
                    if args.zone_accept_mode == "mid":
                        return h1_close >= z.mid
                    if args.zone_accept_mode == "top":
                        return h1_close >= z.top
                    if args.zone_accept_mode == "top_bull":
                        return h1_close >= z.top and h1_close > h1_open
                    return True

                support_candidates = [z for z in support_candidates if _zone_accept_ok(z)]
                if support_candidates and args.log_gates:
                    gate_counts["zone_accept_pass"] += 1
            if support_candidates and args.require_reject_close:
                reject_level = "mid" if args.reject_mode == "mid" else "top"
                if args.reject_source == "1h":
                    support_candidates = [
                        z for z in support_candidates if h1_close > (z.mid if reject_level == "mid" else z.top)
                    ]
                    if support_candidates and args.log_gates:
                        gate_counts["reject_pass_1h"] += 1
                else:
                    if args.use_confirmed:
                        idx_15m_rej = int(np.searchsorted(ts_15m, decision_ts - _tf_ms(cfg.tf_mtf), side="right") - 1)
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
                if dbg_on:
                    dbg_counts["fail_zone"] += 1
                    try:
                        live_sup = [z for z in zones if z.live and z.side == -1]
                        near = min(live_sup, key=lambda z: abs(z.mid - h1_touch_px)) if live_sup else None
                        near_txt = (
                            f"near_mid={near.mid:.6f} near_bot={near.bot:.6f} near_top={near.top:.6f} "
                            f"touch_px={h1_touch_px:.6f} h1_high={h1_high:.6f} h1_low={h1_low:.6f} "
                            f"touch_cond={int((h1_touch_px <= near.top) and (h1_high >= near.bot))}"
                        ) if near else "near=none"
                    except Exception:
                        near_txt = "near=err"
                    _dbg(
                        ts,
                        "zone",
                        f"support_candidates=0 live_sup={len([z for z in zones if z.live and z.side == -1])} "
                        f"dvf_norm={dvf_norm:.4f} idx_1h={idx_1h} h1_ts={_iso_kst(h1_ts)} {near_txt}",
                    )
                if args.log_gates:
                    gate_counts["zone_touch"] += 1
                continue

            if args.use_confirmed:
                idx_15m = int(np.searchsorted(ts_15m, decision_ts - _tf_ms(cfg.tf_mtf), side="right") - 1)
            else:
                idx_15m = int(np.searchsorted(ts_15m, ts, side="right") - 1)
            if idx_15m < 2:
                if dbg_on:
                    dbg_counts["fail_idx_15m"] += 1
                    _dbg(ts, "idx_15m", f"idx_15m={idx_15m}")
                continue
            l15_0 = float(df_15m.at[idx_15m, "low"])
            l15_1 = float(df_15m.at[idx_15m - 1, "low"])
            l15_2 = float(df_15m.at[idx_15m - 2, "low"])
            if not (l15_0 > l15_1 or l15_1 > l15_2):
                if dbg_on:
                    dbg_counts["fail_hl_15m"] += 1
                    _dbg(ts, "hl_15m", f"cond=HL_FAIL l0={l15_0:.6f} l1={l15_1:.6f} l2={l15_2:.6f}")
                if args.log_gates:
                    gate_counts["hl_15m"] += 1
                continue
            if not (float(df_15m.at[idx_15m, "close"]) > float(df_15m.at[idx_15m, "open"])):
                if dbg_on:
                    dbg_counts["fail_hl_15m"] += 1
                    _dbg(
                        ts,
                        "hl_15m",
                        f"cond=BULL_FAIL c15={float(df_15m.at[idx_15m, 'close']):.6f} o15={float(df_15m.at[idx_15m, 'open']):.6f}",
                    )
                if args.log_gates:
                    gate_counts["hl_15m"] += 1
                continue

            close_now = float(df_3m.at[i3, "close"])
            open_now = float(df_3m.at[i3, "open"])
            low_now = float(df_3m.at[i3, "low"])
            high_now = float(df_3m.at[i3, "high"])
            atr_now = float(atr_3m.iloc[i3]) if not np.isnan(atr_3m.iloc[i3]) else 0.0

            pullback_lb = max(5, int(args.pullback_lookback))
            pb_start = max(0, i3 - pullback_lb + 1)
            recent_peak = float(df_3m.iloc[pb_start : i3 + 1]["high"].astype(float).max())
            dd_abs = max(0.0, recent_peak - low_now)
            dd_pct = (dd_abs / recent_peak) if recent_peak > 0 else 0.0
            dd_atr = (dd_abs / atr_now) if atr_now > 0 else 0.0
            if dd_pct < float(args.pullback_min_pct) and dd_atr < float(args.pullback_min_atr):
                if args.log_gates:
                    gate_counts["pullback_fail"] += 1
                if dbg_on:
                    _dbg(ts, "pullback", f"dd_pct={dd_pct:.4f} dd_atr={dd_atr:.2f} peak={recent_peak:.6f} low={low_now:.6f}")
                continue

            if bool(args.require_sweep_reclaim):
                if sweep_state_active:
                    if close_now > sweep_state_level:
                        sweep_state_active = False
                    else:
                        if args.log_gates:
                            gate_counts["sweep_reclaim_fail"] += 1
                        continue
                sw_lb = max(3, int(args.sweep_lookback))
                sw_start = max(0, i3 - sw_lb)
                prior_lows = df_3m.iloc[sw_start:i3]["low"].astype(float)
                prior_low = float(prior_lows.min()) if len(prior_lows) > 0 else low_now
                sweep_tol = atr_now * float(args.sweep_tol_atr) if atr_now > 0 else 0.0
                sweep_ok = low_now <= (prior_low - sweep_tol)
                reclaim_ok = close_now > prior_low
                if not (sweep_ok and reclaim_ok):
                    if sweep_ok and int(args.sweep_reclaim_wait_bars) > 0:
                        sweep_state_active = True
                        sweep_state_level = prior_low
                        sweep_state_until = i3 + max(1, int(args.sweep_reclaim_wait_bars))
                    if args.log_gates:
                        gate_counts["sweep_reclaim_fail"] += 1
                    if dbg_on:
                        _dbg(ts, "sweep_reclaim", f"sweep={int(sweep_ok)} reclaim={int(reclaim_ok)} prior_low={prior_low:.6f} low={low_now:.6f} close={close_now:.6f}")
                    continue

            high_prev = [
                float(df_3m.at[i3 - 1, "high"]),
                float(df_3m.at[i3 - 2, "high"]),
                float(df_3m.at[i3 - 3, "high"]),
            ]
            high_max = max(high_prev)
            strong_break = close_now > high_max
            weak_break = (high_now > high_max) and (close_now <= high_max) and (close_now > open_now)
            if not strong_break and not weak_break:
                if dbg_on:
                    dbg_counts["fail_break_3m"] += 1
                    _dbg(ts, "break_3m", f"strong=0 weak=0 c3={close_now:.6f} o3={open_now:.6f} high_max={high_max:.6f}")
                if args.log_gates:
                    gate_counts["break_3m"] += 1
                continue
            if atr_now > 0 and float(args.max_break_ext_atr) > 0:
                break_ext_atr = max(0.0, close_now - high_max) / atr_now
                if break_ext_atr > float(args.max_break_ext_atr):
                    if args.log_gates:
                        gate_counts["break_overheat"] += 1
                    if dbg_on:
                        _dbg(ts, "break_overheat", f"break_ext_atr={break_ext_atr:.2f} max={float(args.max_break_ext_atr):.2f}")
                    continue
            # time block (KST hours)
            hour_kst = int(_ts_kst(ts).split(" ")[1].split(":")[0])
            if block_hours and hour_kst in block_hours:
                if dbg_on:
                    dbg_counts["fail_time_block"] += 1
                    _dbg(ts, "time_block", f"hour={hour_kst}")
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
                rng = float(df_3m.at[i3, "high"]) - float(df_3m.at[i3, "low"])
                lower_wick = min(float(df_3m.at[i3, "open"]), float(df_3m.at[i3, "close"])) - float(df_3m.at[i3, "low"])
                lower_wick_ratio = (lower_wick / rng) if rng > 0 else 0.0
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
                        if dbg_on:
                            dbg_counts["fail_retest"] += 1
                            _dbg(
                                ts,
                                "retest",
                                f"pattern_fail strong={int(strong_break)} weak={int(weak_break)} "
                                f"close={close_now:.6f} open={open_now:.6f} high={high_now:.6f} low={low_now:.6f} "
                                f"retest_level={retest_level:.6f}",
                            )
                        continue
                    reclaim_min_atr = float(args.retest_reclaim_min_atr)
                    if reclaim_min_atr > 0:
                        reclaim_margin = close_now - retest_level
                        if reclaim_margin < (atr_now * reclaim_min_atr):
                            if args.log_gates:
                                gate_counts["retest_fail_shallow"] += 1
                            if dbg_on:
                                _dbg(
                                    ts,
                                    "retest_reclaim_gate",
                                    (
                                        f"margin={reclaim_margin:.6f} "
                                        f"need={(atr_now * reclaim_min_atr):.6f} "
                                        f"atr3={atr_now:.6f} level={retest_level:.6f} close={close_now:.6f}"
                                    ),
                                )
                            continue
                    breakdown_block_atr = float(args.retest_breakdown_block_atr)
                    if breakdown_block_atr > 0:
                        breakdown_th = retest_level - (atr_now * breakdown_block_atr)
                        if low_now < breakdown_th:
                            if args.log_gates:
                                gate_counts["retest_fail_shallow"] += 1
                            if dbg_on:
                                _dbg(
                                    ts,
                                    "retest_breakdown_block",
                                    f"low={low_now:.6f} th={breakdown_th:.6f} level={retest_level:.6f} atr3={atr_now:.6f}",
                                )
                            continue

                    ema_entry = float(ema_3m_entry.iloc[i3]) if len(ema_3m_entry) > i3 and not np.isnan(ema_3m_entry.iloc[i3]) else None
                    entry_offset = float(cfg.entry_atr_offset if strong_break else getattr(cfg, "entry_atr_offset_weak", cfg.entry_atr_offset))
                    entry_target = None
                    immediate_entry = bool(getattr(args, "entry_immediate_on_close_reclaim", False)) and (entry_reason == "close_reclaim")
                    if isinstance(ema_entry, (int, float)) and atr_now > 0:
                        entry_target = float(ema_entry) - (atr_now * entry_offset)
                    if (not immediate_entry) and (entry_target is None or low_now > entry_target):
                        if dbg_on:
                            dbg_counts["fail_entry_target"] += 1
                            _dbg(
                                ts,
                                "entry_target",
                                f"low={low_now:.6f} target={(entry_target if isinstance(entry_target,(int,float)) else float('nan')):.6f} "
                                f"ema_entry={(ema_entry if isinstance(ema_entry,(int,float)) else float('nan')):.6f} atr3={atr_now:.6f}",
                            )
                        continue
                    if bool(args.entry_candle_guard):
                        # Avoid filling while 3m candle still pushes down.
                        if not (close_now >= open_now):
                            if dbg_on:
                                dbg_counts["fail_entry_target"] += 1
                                _dbg(
                                    ts,
                                    "entry_candle_guard",
                                    f"close={close_now:.6f} open={open_now:.6f} target={float(entry_target):.6f}",
                                )
                            continue
                    max_dev_pct = float(getattr(args, "entry_ema_max_dev_pct", 0.0) or 0.0)
                    if (
                        max_dev_pct > 0
                        and isinstance(ema_entry, (int, float))
                        and float(ema_entry) > 0
                    ):
                        ema_dev_pct = (close_now - float(ema_entry)) / float(ema_entry)
                        if ema_dev_pct > max_dev_pct:
                            if args.log_gates:
                                gate_counts["entry_ema_dev_fail"] += 1
                            if dbg_on:
                                _dbg(
                                    ts,
                                    "entry_ema_dev",
                                    (
                                        f"close={close_now:.6f} ema={float(ema_entry):.6f} "
                                        f"dev_pct={ema_dev_pct*100.0:.2f} max={max_dev_pct*100.0:.2f}"
                                    ),
                                )
                            continue
                    next_idx = i3 + 1
                    if next_idx >= len(df_3m):
                        continue
                    entry_ts_ms = int(df_3m.at[next_idx, "ts"])
                    if entry_model == "signal_close":
                        entry_px = float(close_now)
                        entry_ts_ms = int(ts)
                        if args.log_gates:
                            gate_counts["entry_immediate"] += 1
                    elif immediate_entry:
                        chase_cap = float(getattr(args, "entry_immediate_max_chase_pct", 0.0) or 0.0)
                        if (
                            chase_cap > 0
                            and isinstance(ema_entry, (int, float))
                            and float(ema_entry) > 0
                        ):
                            chase_pct = (close_now - float(ema_entry)) / float(ema_entry)
                            if chase_pct > chase_cap:
                                if dbg_on:
                                    dbg_counts["fail_entry_target"] += 1
                                    _dbg(
                                        ts,
                                        "entry_immediate_chase",
                                        (
                                            f"close={close_now:.6f} ema={float(ema_entry):.6f} "
                                            f"chase_pct={chase_pct*100.0:.2f} max={chase_cap*100.0:.2f}"
                                        ),
                                    )
                                continue
                        if str(getattr(args, "immediate_entry_fill", "close")).lower() == "next_open":
                            entry_px = float(df_3m.at[next_idx, "open"])
                            entry_ts_ms = int(df_3m.at[next_idx, "ts"])
                        else:
                            entry_px = float(close_now)
                            entry_ts_ms = int(ts)
                        if args.log_gates:
                            gate_counts["entry_immediate"] += 1
                    else:
                        entry_px = float(entry_target)
                    if args.ltf_sr_bias:
                        lb = max(5, int(args.ltf_sr_lookback))
                        start = max(0, i3 - lb + 1)
                        seg = df_3m.iloc[start : i3 + 1]
                        if not seg.empty:
                            try:
                                sup = float(seg["low"].astype(float).min())
                                res = float(seg["high"].astype(float).max())
                            except Exception:
                                sup = None
                                res = None
                            if (
                                isinstance(sup, (int, float))
                                and isinstance(res, (int, float))
                                and np.isfinite(sup)
                                and np.isfinite(res)
                                and res > sup
                            ):
                                dist_sup = abs(entry_px - sup)
                                dist_res = abs(res - entry_px)
                                if dist_sup > dist_res:
                                    if args.log_gates:
                                        gate_counts["ltf_sr_bias_block"] += 1
                                    if args.debug_zone:
                                        print(
                                            "[BACKTEST][LTF_SR_BLOCK] "
                                            f"sym={sym} ts={_ts_kst(ts)} track={entry_reason or 'retest'} "
                                            f"entry={entry_px:.6f} sup={sup:.6f} res={res:.6f} "
                                            f"dist_sup={dist_sup:.6f} dist_res={dist_res:.6f}"
                                        )
                                    continue
                    nearest = min(support_candidates, key=lambda z: abs(z.mid - entry_px))
                    if not (close_now >= nearest.mid or entry_px >= nearest.top - (atr_now * 0.2)):
                        if dbg_on:
                            try:
                                print(
                                    "[BACKTEST] SR_PRO_LONG_NEAREST_FAIL "
                                    f"sym={sym} ts={_iso_kst(ts)} "
                                    f"close_3m={close_now:.6f} entry={entry_px:.6f} "
                                    f"zone_mid={nearest.mid:.6f} zone_top={nearest.top:.6f} zone_bot={nearest.bot:.6f} "
                                    f"atr3={atr_now:.6f} cond_rhs={(nearest.top - (atr_now * 0.2)):.6f}"
                                )
                            except Exception:
                                pass
                        if dbg_on:
                            dbg_counts["fail_nearest_zone"] += 1
                        if args.log_gates:
                            gate_counts["retest_fail_shallow"] += 1
                        continue
                    sl_raw = nearest.bot - (atr_now * float(args.sl_atr_mult))
                    sl_price = min(sl_raw, entry_px - (atr_now * 1.0))
                    cap_pct = max(
                        float(args.sl_cap_pct),
                        ((atr_now * float(args.sl_cap_atr_mult)) / entry_px) if entry_px > 0 else 0.0,
                    )
                    if cap_pct > 0 and entry_px > 0:
                        sl_price = max(sl_price, entry_px * (1.0 - cap_pct))
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
                        "entry_ts": int(entry_ts_ms),
                        "track": "strong" if strong_break else "weak",
                        "reason": entry_reason,
                    }
                    stats["entries"] += 1
                    sym_stats["entries"] += 1
                    if dbg_on:
                        dbg_counts["entries"] += 1
                        _dbg(
                            ts,
                            "entry",
                            f"entry_ts={_iso_kst(trade['entry_ts'])} reason={entry_reason or 'unknown'} "
                            f"entry_px={entry_px:.6f} tp={tp_price:.6f} sl={sl_price:.6f} "
                            f"strong={int(strong_break)} weak={int(weak_break)}",
                        )
                    if args.log_gates:
                        if entry_reason == "close_reclaim":
                            gate_counts["entry_by_pass_close"] += 1
                        else:
                            gate_counts["entry_by_pass_high"] += 1
                    er_key = str(entry_reason or "unknown")
                    er_bucket = entry_reason_stats.setdefault(
                        er_key, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0}
                    )
                    er_bucket["entries"] += 1
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
        if dbg_on and dbg_counts is not None:
            print(
                "[BACKTEST] SR_PRO_LONG_DEBUG_SUMMARY "
                f"sym={sym} eval_bars={dbg_counts['eval_bars']} "
                f"fail_cooldown={dbg_counts['fail_cooldown']} fail_time_block={dbg_counts['fail_time_block']} "
                f"fail_idx_1h={dbg_counts['fail_idx_1h']} fail_idx_15m={dbg_counts['fail_idx_15m']} "
                f"fail_zone={dbg_counts['fail_zone']} fail_hl_15m={dbg_counts['fail_hl_15m']} "
                f"fail_break_3m={dbg_counts['fail_break_3m']} fail_retest={dbg_counts['fail_retest']} "
                f"fail_entry_target={dbg_counts['fail_entry_target']} fail_nearest_zone={dbg_counts['fail_nearest_zone']} "
                f"entries={dbg_counts['entries']} exits={dbg_counts['exits']} open={1 if trade else 0}"
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

    print("[BACKTEST] BY_ENTRY_REASON reason entries tp sl sl_rate winrate net_sum net_sum_usdt")
    for reason in sorted(entry_reason_stats.keys()):
        bucket = entry_reason_stats.get(reason, {"entries": 0, "tp": 0, "sl": 0, "net_sum": 0.0, "net_sum_usdt": 0.0})
        entries = int(bucket.get("entries", 0))
        tp_cnt = int(bucket.get("tp", 0))
        sl_cnt = int(bucket.get("sl", 0))
        sl_rate = (sl_cnt / entries * 100.0) if entries > 0 else 0.0
        winrate = (tp_cnt / entries * 100.0) if entries > 0 else 0.0
        print(
            f"[BACKTEST] ENTRY_REASON {reason} entries={entries} tp={tp_cnt} sl={sl_cnt} "
            f"sl_rate={sl_rate:.2f}% winrate={winrate:.2f}% net_sum={float(bucket.get('net_sum', 0.0)):.3f} "
            f"net_sum_usdt={float(bucket.get('net_sum_usdt', 0.0)):.3f}"
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
