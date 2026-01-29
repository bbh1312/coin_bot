from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

STATE_FILE = os.path.join(ROOT_DIR, "state.json")
LOG_DIR = os.path.join(ROOT_DIR, "logs", "adv_trend_alert")

from env_loader import load_env
import cycle_cache
from atlas_test.data_feed import fetch_ohlcv
from atlas_test.notifier_telegram import send_message
from executor import exchange
from engines.universe import build_universe_from_tickers

ADV_TREND_ADX_MIN = float(os.getenv("ADV_TREND_ADX_MIN", "25"))
ADV_TREND_MFI_LONG_MAX = float(os.getenv("ADV_TREND_MFI_LONG_MAX", "80"))
ADV_TREND_MFI_SHORT_MIN = float(os.getenv("ADV_TREND_MFI_SHORT_MIN", "20"))
ADV_TREND_ADX_LEN = int(os.getenv("ADV_TREND_ADX_LEN", "14"))
ADV_TREND_MFI_LEN = int(os.getenv("ADV_TREND_MFI_LEN", "14"))
ADV_TREND_EMA_LEN = int(os.getenv("ADV_TREND_EMA_LEN", "200"))
ADV_TREND_SUPER_ATR_LEN = int(os.getenv("ADV_TREND_SUPER_ATR_LEN", "10"))
ADV_TREND_SUPER_MULT = float(os.getenv("ADV_TREND_SUPER_MULT", "3.0"))
ADV_TREND_MIN_STOP_ATR = float(os.getenv("ADV_TREND_MIN_STOP_ATR", "0.5"))

PER_SYMBOL_SLEEP = float(os.getenv("ADV_TREND_ALERT_SLEEP", "0.05"))
INTERVAL_SEC = int(os.getenv("ADV_TREND_ALERT_INTERVAL_SEC", str(15 * 60)))
INTERVAL_OFFSET_SEC = int(os.getenv("ADV_TREND_ALERT_OFFSET_SEC", str(5 * 60)))


def _fmt_kst_now() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S KST")


def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    base_dir = os.path.dirname(STATE_FILE) or "."
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = f"{STATE_FILE}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=True)
    os.replace(tmp_path, STATE_FILE)


def _append_log(msg: str) -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        day = datetime.now(tz=timezone(timedelta(hours=9))).strftime("%Y%m%d")
        path = os.path.join(LOG_DIR, f"adv_trend_alert_{day}.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        return


def _log_kst(msg: str) -> None:
    line = f"{_fmt_kst_now()} {msg}"
    print(line)
    _append_log(line)


def _sleep_to_boundary(interval_sec: int, offset_sec: int = 0) -> None:
    now = time.time()
    if interval_sec <= 0:
        time.sleep(1)
        return
    base = now - float(offset_sec)
    next_ts = (int(base // interval_sec) + 1) * interval_sec + float(offset_sec)
    time.sleep(max(1, next_ts - now))


def _set_cache(symbol: str, tf: str, data: list) -> bool:
    if not data:
        return False
    formatted = [[c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"]] for c in data]
    cycle_cache.set_raw(symbol, tf, formatted)
    return True


def _fetch_and_cache(symbol: str, tf: str, limit: int) -> bool:
    ohlcv = fetch_ohlcv(exchange, symbol, tf, limit, drop_last=True)
    return _set_cache(symbol, tf, ohlcv)


def _normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        return ""
    raw = symbol.strip().upper()
    if not raw:
        return ""
    if raw.endswith("/USDT") and not raw.endswith("/USDT:USDT"):
        return f"{raw}:USDT"
    if "/" in raw and not raw.endswith(":USDT"):
        return raw
    if raw.endswith("USDT") and "/" not in raw:
        base = raw[:-4]
        return f"{base}/USDT:USDT"
    return raw


def _build_fallback_universe() -> list[str]:
    try:
        tickers = exchange.fetch_tickers()
        top_n = int(os.getenv("ADV_TREND_UNIVERSE_TOP_N", "30"))
        min_qv = float(os.getenv("ADV_TREND_MIN_QV", "5000000"))
        return build_universe_from_tickers(
            tickers,
            min_quote_volume_usdt=min_qv,
            top_n=top_n,
        )
    except Exception:
        return []


def _build_universe_union(state: dict) -> list[str]:
    keys = [
        "_universe",
        "_swaggy_universe",
        "_adv_trend_universe",
        "_dtfx_universe",
        "_atlas_rs_fail_short_universe",
    ]
    union = []
    for key in keys:
        vals = state.get(key)
        if isinstance(vals, list):
            union.extend(vals)
    out = []
    seen = set()
    for sym in union:
        norm = _normalize_symbol(sym)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _adv_atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _adv_mfi(df: pd.DataFrame, length: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"]
    tp_diff = tp.diff()
    pos_mf = mf.where(tp_diff > 0, 0.0)
    neg_mf = mf.where(tp_diff < 0, 0.0).abs()
    pos_sum = pos_mf.rolling(length).sum()
    neg_sum = neg_mf.rolling(length).sum()
    ratio = pos_sum / neg_sum.replace(0, float("nan"))
    mfi = 100 - (100 / (1 + ratio))
    return mfi.fillna(0.0)


def _adv_adx(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr.replace(0, float("nan")))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float("nan"))) * 100
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()
    return adx.fillna(0.0)


def _adv_supertrend(df: pd.DataFrame, atr_len: int, mult: float) -> tuple[pd.Series, pd.Series]:
    atr = _adv_atr(df, atr_len)
    src = df["close"]
    upper = src + (mult * atr)
    lower = src - (mult * atr)
    final_upper = upper.copy()
    final_lower = lower.copy()
    trend = pd.Series(index=df.index, dtype="int")
    for i in range(len(df)):
        if i == 0:
            trend.iloc[i] = 1
            continue
        if upper.iloc[i] < final_upper.iloc[i - 1] or df["close"].iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]
        if lower.iloc[i] > final_lower.iloc[i - 1] or df["close"].iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]
        if trend.iloc[i - 1] == 1:
            trend.iloc[i] = -1 if df["close"].iloc[i] < final_lower.iloc[i] else 1
        else:
            trend.iloc[i] = 1 if df["close"].iloc[i] > final_upper.iloc[i] else -1
    st_line = pd.Series(index=df.index, dtype="float")
    for i in range(len(df)):
        st_line.iloc[i] = final_lower.iloc[i] if trend.iloc[i] == 1 else final_upper.iloc[i]
    return st_line, trend


def _adv_eval_15m(df_15m: pd.DataFrame, df_4h: Optional[pd.DataFrame] = None) -> Optional[dict]:
    if df_15m.empty:
        return None
    if len(df_15m) < max(ADV_TREND_SUPER_ATR_LEN + 5, ADV_TREND_ADX_LEN + 5, ADV_TREND_MFI_LEN + 5):
        return None
    try:
        ema200 = float("nan")
        if df_4h is not None and not df_4h.empty and len(df_4h) >= ADV_TREND_EMA_LEN:
            ema200 = ema(df_4h["close"], ADV_TREND_EMA_LEN).iloc[-1]
        mfi_val = _adv_mfi(df_15m, ADV_TREND_MFI_LEN).iloc[-1]
        adx_val = _adv_adx(df_15m, ADV_TREND_ADX_LEN).iloc[-1]
        st_line, st_trend = _adv_supertrend(df_15m, ADV_TREND_SUPER_ATR_LEN, ADV_TREND_SUPER_MULT)
        st_px = float(st_line.iloc[-1])
        trend_dir = int(st_trend.iloc[-1])
        prev_trend_dir = int(st_trend.iloc[-2]) if len(st_trend) >= 2 else 0
        close_px = float(df_15m["close"].iloc[-1])
        atr_val = float(_adv_atr(df_15m, ADV_TREND_SUPER_ATR_LEN).iloc[-1])
    except Exception:
        return None
    return {
        "ema200": float(ema200),
        "mfi": float(mfi_val),
        "adx": float(adx_val),
        "st_px": st_px,
        "trend_dir": trend_dir,
        "prev_trend_dir": prev_trend_dir,
        "close_px": close_px,
        "atr": atr_val,
        "ts": int(df_15m["ts"].iloc[-1]) if "ts" in df_15m.columns else None,
    }


def _build_alert_message(symbol: str, side: str, signal: dict) -> str:
    sym = symbol.replace("/USDT:USDT", "")
    side_key = (side or "").upper()
    side_kr = "롱" if side_key == "LONG" else "숏"
    st_px = signal.get("st_px")
    close_px = signal.get("close_px")
    adx_val = signal.get("adx")
    mfi_val = signal.get("mfi")
    ema200 = signal.get("ema200")
    atr_val = signal.get("atr")
    candle_ts = signal.get("ts")
    candle_str = "n/a"
    if isinstance(candle_ts, (int, float)) and candle_ts > 0:
        try:
            kst = timezone(timedelta(hours=9))
            candle_dt = datetime.fromtimestamp(float(candle_ts) / 1000.0, tz=kst)
            candle_str = candle_dt.strftime("%Y-%m-%d %H:%M KST")
        except Exception:
            candle_str = "n/a"
    return (
        "📣 Advanced Trend Alert (15m)\n"
        f"시간: {_fmt_kst_now()}\n"
        "----\n"
        f"{sym} / {side_kr}\n"
        f"캔들: {candle_str}\n"
        f"close={close_px:.6g} st={st_px:.6g} atr={atr_val:.6g}\n"
        f"ema200(4h)={ema200:.6g} adx={adx_val:.2f} mfi={mfi_val:.2f}"
    )


def main() -> None:
    load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN_SUPER_TREND", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_SUPER_TREND", "").strip()
    if not token or not chat_id:
        _log_kst("[adv-trend-alert] missing TELEGRAM_BOT_TOKEN_SUPER_TREND or TELEGRAM_CHAT_ID_SUPER_TREND")
        return

    exchange.load_markets()
    ltf_limit = max(ADV_TREND_EMA_LEN, 500)
    htf_limit = max(ADV_TREND_EMA_LEN, 220)

    _log_kst("[adv-trend-alert] start")
    while True:
        state = _load_state()
        universe = _build_universe_union(state)
        used_fallback = False
        if not universe:
            universe = _build_fallback_universe()
            if not universe:
                _log_kst("[adv-trend-alert] empty universe; retry next cycle")
                _sleep_to_boundary(INTERVAL_SEC, INTERVAL_OFFSET_SEC)
                continue
            _log_kst(f"[adv-trend-alert] fallback universe={len(universe)}")
            used_fallback = True
        if not state.get("_adv_trend_alert_last"):
            if not used_fallback:
                _log_kst(f"[adv-trend-alert] universe={len(universe)}")
        signal_bucket = state.get("_adv_trend_alert_last")
        if not isinstance(signal_bucket, dict):
            signal_bucket = {}
            state["_adv_trend_alert_last"] = signal_bucket
        for symbol in universe:
            try:
                _fetch_and_cache(symbol, "15m", ltf_limit)
                _fetch_and_cache(symbol, "4h", htf_limit)
                df_15m = cycle_cache.get_df(symbol, "15m", limit=ltf_limit, force=True)
                df_4h = cycle_cache.get_df(symbol, "4h", limit=htf_limit, force=True)
            except Exception:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if df_15m.empty or len(df_15m) < 20:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            signal = _adv_eval_15m(df_15m, df_4h)
            if not signal:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            trend_dir = signal.get("trend_dir")
            prev_trend_dir = signal.get("prev_trend_dir")
            if trend_dir is None or prev_trend_dir is None:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            flip_long = trend_dir == 1 and prev_trend_dir <= 0
            flip_short = trend_dir == -1 and prev_trend_dir >= 0
            if not flip_long and not flip_short:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if flip_long and flip_short:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            side = "LONG" if flip_long else "SHORT"
            close_px = signal.get("close_px")
            st_px = signal.get("st_px")
            atr_val = signal.get("atr")
            if not isinstance(close_px, (int, float)) or not isinstance(st_px, (int, float)):
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if not isinstance(atr_val, (int, float)) or atr_val <= 0:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            risk_per_unit = abs(float(close_px) - float(st_px))
            if risk_per_unit <= 0:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if risk_per_unit < (float(atr_val) * ADV_TREND_MIN_STOP_ATR):
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "LONG" and st_px >= close_px:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "SHORT" and st_px <= close_px:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            adx_val = signal.get("adx")
            mfi_val = signal.get("mfi")
            if isinstance(adx_val, (int, float)) and adx_val < ADV_TREND_ADX_MIN:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "LONG" and isinstance(mfi_val, (int, float)) and mfi_val > ADV_TREND_MFI_LONG_MAX:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            if side == "SHORT" and isinstance(mfi_val, (int, float)) and mfi_val < ADV_TREND_MFI_SHORT_MIN:
                time.sleep(PER_SYMBOL_SLEEP)
                continue

            sig_ts = signal.get("ts")
            key = f"{symbol}:{side}"
            last_ts = signal_bucket.get(key)
            if isinstance(sig_ts, (int, float)) and last_ts == sig_ts:
                time.sleep(PER_SYMBOL_SLEEP)
                continue
            signal_bucket[key] = sig_ts if isinstance(sig_ts, (int, float)) else time.time()
            state["_adv_trend_alert_last"] = signal_bucket
            _save_state(state)

            msg = _build_alert_message(symbol, side, signal)
            send_message(token, chat_id, msg)
            _log_kst(f"ADV_TREND_ALERT sym={symbol} side={side} st={st_px:.6g} close={close_px:.6g}")
            time.sleep(PER_SYMBOL_SLEEP)
        _sleep_to_boundary(INTERVAL_SEC, INTERVAL_OFFSET_SEC)


if __name__ == "__main__":
    main()
