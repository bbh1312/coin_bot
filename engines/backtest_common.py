from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

from engines.universe import build_universe_from_tickers

TF_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


def calc_warmup_window(
    eval_days: int,
    now_ms: int,
    min_bars_by_tf: Dict[str, int],
) -> Tuple[int, int, int, int]:
    eval_start_ms = int((datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc) - timedelta(days=eval_days)).timestamp() * 1000)
    warmup_minutes = 0
    for tf, min_bars in min_bars_by_tf.items():
        minutes = TF_MINUTES.get(tf, 1) * int(min_bars)
        if minutes > warmup_minutes:
            warmup_minutes = minutes
    warmup_days = int((warmup_minutes + 1439) // 1440) if warmup_minutes > 0 else 0
    start_ms = eval_start_ms - int(warmup_minutes * 60 * 1000)
    return start_ms, eval_start_ms, warmup_days, warmup_minutes


def load_common_universe(
    universe_arg: str,
    exchange,
    cache_only: bool,
    min_quote_volume_usdt: float = 8_000_000.0,
    top_n: int = 50,
) -> List[str]:
    universe: List[str] = []
    if universe_arg in ("common", "common_universe"):
        latest_path = os.path.join("logs", "common_universe", "latest.txt")
        if os.path.exists(latest_path):
            with open(latest_path, "r", encoding="utf-8") as f:
                universe = [line.strip() for line in f.read().splitlines() if line.strip()]
        if not universe and not cache_only and exchange is not None:
            tickers = exchange.fetch_tickers()
            universe = build_universe_from_tickers(
                tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
            )
    elif universe_arg.startswith("top"):
        latest_path = os.path.join("logs", "common_universe", "latest.txt")
        if cache_only and os.path.exists(latest_path):
            with open(latest_path, "r", encoding="utf-8") as f:
                universe = [line.strip() for line in f.read().splitlines() if line.strip()]
        if not universe:
            if exchange is None:
                return []
            tickers = exchange.fetch_tickers()
            universe = build_universe_from_tickers(
                tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
            )
        try:
            n = int(universe_arg.replace("top", ""))
            universe = universe[:n]
        except Exception:
            pass
    else:
        if exchange is None:
            return []
        tickers = exchange.fetch_tickers()
        universe = build_universe_from_tickers(
            tickers, min_quote_volume_usdt=min_quote_volume_usdt, top_n=top_n
        )
    # Restrict to USDT-margined symbols to avoid BadSymbol for non-USDT markets.
    universe = [sym for sym in universe if sym.endswith("/USDT") or "/USDT:" in sym]
    return universe


def log_warmup_info(log_fn, warmup_days: int, warmup_minutes: int, eval_days: int) -> None:
    line = f"[BACKTEST] WARMUP auto days={warmup_days} minutes={warmup_minutes} eval_days={eval_days}"
    try:
        print(line)
    except Exception:
        pass
    try:
        log_fn(line)
    except Exception:
        pass


def format_backtest_summary(symbol: Optional[str], stats: Dict[str, float]) -> str:
    trades = int(stats.get("trades", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    entries = int(stats.get("entries", 0))
    exits = int(stats.get("exits", 0))
    winrate = (wins / trades * 100.0) if trades > 0 else 0.0
    avg_mfe = stats.get("mfe_sum", 0.0) / trades if trades > 0 else 0.0
    avg_mae = stats.get("mae_sum", 0.0) / trades if trades > 0 else 0.0
    avg_hold = stats.get("hold_sum", 0.0) / trades if trades > 0 else 0.0
    net_sum = stats.get("net_sum", 0.0)
    tp_sum = stats.get("tp_sum", 0.0)
    sl_sum = stats.get("sl_sum", 0.0)
    net_sum_usdt = stats.get("net_sum_usdt", 0.0)
    tp_sum_usdt = stats.get("tp_sum_usdt", 0.0)
    sl_sum_usdt = stats.get("sl_sum_usdt", 0.0)
    tag = "TOTAL" if symbol is None else symbol
    return (
        f"[BACKTEST] {tag} entries={entries} exits={exits} trades={trades} "
        f"wins={wins} losses={losses} winrate={winrate:.2f}% "
        f"avg_mfe={avg_mfe:.4f} avg_mae={avg_mae:.4f} avg_hold={avg_hold:.1f} "
        f"tp_sum={tp_sum:.3f} sl_sum={sl_sum:.3f} net_sum={net_sum:.3f} "
        f"tp_sum_usdt={tp_sum_usdt:.3f} sl_sum_usdt={sl_sum_usdt:.3f} net_sum_usdt={net_sum_usdt:.3f}"
    )


def _to_kst(ts_ms: int) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(timezone(timedelta(hours=9)))
    except Exception:
        return None


def print_time_summaries(trades: List[dict], log_fn=None) -> None:
    if not trades:
        return
    stats_by_hour = {h: {"entries": 0, "tp": 0, "sl": 0} for h in range(24)}
    stats_by_dow = {d: {"entries": 0, "tp": 0, "sl": 0} for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}
    for tr in trades:
        ts = tr.get("entry_ts")
        if ts is None:
            continue
        dt = _to_kst(int(ts))
        if dt is None:
            continue
        hour = dt.hour
        dow = dt.strftime("%a")
        if hour in stats_by_hour:
            stats_by_hour[hour]["entries"] += 1
        if dow in stats_by_dow:
            stats_by_dow[dow]["entries"] += 1
        result = str(tr.get("result") or tr.get("reason") or "").upper()
        if result in ("TP", "WIN"):
            if hour in stats_by_hour:
                stats_by_hour[hour]["tp"] += 1
            if dow in stats_by_dow:
                stats_by_dow[dow]["tp"] += 1
        elif result in ("SL", "LOSS"):
            if hour in stats_by_hour:
                stats_by_hour[hour]["sl"] += 1
            if dow in stats_by_dow:
                stats_by_dow[dow]["sl"] += 1
        else:
            pnl = tr.get("pnl_pct")
            try:
                pnl_val = float(pnl)
            except Exception:
                pnl_val = None
            if pnl_val is not None:
                if pnl_val > 0:
                    if hour in stats_by_hour:
                        stats_by_hour[hour]["tp"] += 1
                    if dow in stats_by_dow:
                        stats_by_dow[dow]["tp"] += 1
                elif pnl_val < 0:
                    if hour in stats_by_hour:
                        stats_by_hour[hour]["sl"] += 1
                    if dow in stats_by_dow:
                        stats_by_dow[dow]["sl"] += 1

    header = "[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate"
    try:
        print(header)
    except Exception:
        pass
    if callable(log_fn):
        log_fn(header)
    for hour in range(24):
        entries = stats_by_hour[hour]["entries"]
        tp = stats_by_hour[hour]["tp"]
        sl = stats_by_hour[hour]["sl"]
        sl_rate = (sl / entries * 100.0) if entries > 0 else 0.0
        line = f"[BACKTEST] HOUR {hour:02d} entries={entries} tp={tp} sl={sl} sl_rate={sl_rate:.2f}%"
        try:
            print(line)
        except Exception:
            pass
        if callable(log_fn):
            log_fn(line)


def print_trades_by_symbol(trades: List[dict], log_fn=None) -> None:
    if not trades:
        return
    trades_sorted = sorted(trades, key=lambda t: (t.get("symbol") or "", int(t.get("entry_ts") or 0)))
    header = "[BACKTEST] TRADES(KST) symbol result pnl_pct entry_ts exit_ts"
    try:
        print(header)
    except Exception:
        pass
    if callable(log_fn):
        log_fn(header)
    for tr in trades_sorted:
        symbol = tr.get("symbol") or "UNKNOWN"
        result = tr.get("result") or tr.get("reason") or "NA"
        pnl = tr.get("pnl_pct")
        try:
            pnl_val = float(pnl)
            pnl_str = f"{pnl_val:.2f}%"
        except Exception:
            pnl_str = "N/A"
        entry_ts = tr.get("entry_ts")
        exit_ts = tr.get("exit_ts")
        entry_dt = _to_kst(int(entry_ts)) if isinstance(entry_ts, (int, float)) else None
        exit_dt = _to_kst(int(exit_ts)) if isinstance(exit_ts, (int, float)) else None
        entry_str = entry_dt.strftime("%Y-%m-%d %H:%M") if entry_dt else "N/A"
        exit_str = exit_dt.strftime("%Y-%m-%d %H:%M") if exit_dt else "N/A"
        line = f"[BACKTEST] TRADE {symbol} result={result} pnl_pct={pnl_str} entry_ts={entry_str} exit_ts={exit_str}"
        try:
            print(line)
        except Exception:
            pass
        if callable(log_fn):
            log_fn(line)
