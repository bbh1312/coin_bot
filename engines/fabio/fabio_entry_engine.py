import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

import cycle_cache


@dataclass
class Config:
    timeframe_htf: str = "4h"
    timeframe_ltf: str = "15m"
    limit: int = 400

    ema_fast: int = 7
    ema_slow: int = 20
    atr_len: int = 14
    vol_sma_len: int = 20

    regime_min_ema_spread_pct: float = 0.4
    ltf_max_extension_pct: float = 3.0
    retest_buffer_pct: float = 0.15
    pullback_vol_ratio_max: float = 1.3
    trigger_vol_ratio_min: float = 1.05
    early_ema_dist_pct: float = 0.0015
    early_retest_buffer_pct: float = 0.15
    early_wick_ratio: float = 1.2
    early_lookback_high: int = 20
    retest_window: int = 8
    retest_touch_tol: float = 0.0015
    retest_reject_tol: float = 0.00040
    dist_to_ema20_max: float = 0.006
    long_retest_window: int = 12
    long_touch_tol: float = 0.003
    long_reclaim_tol: float = 0.0003
    long_dist_to_ema20_max: float = 0.006
    short_retest_recent: int = 6
    short_lower_high_lookback: int = 5
    short_upper_wick_ratio: float = 1.5
    short_upper_wick_chain_len: int = 2
    short_reject_wick_ratio: float = 0.4
    short_rsi_high: float = 60.0
    short_rsi_low: float = 50.0
    short_rsi_downturn_high: float = 55.0
    short_rsi_downturn_strict: bool = True
    short_timeframe_primary: str = "5m"
    short_timeframe_secondary: str = "3m"
    short_limit: int = 200
    short_dist_to_ema20_max: float = 0.015
    short_fast_drop_pct: float = 3.0
    short_fast_drop_lookback: int = 5
    short_vol_ratio_min: float = 1.0
    short_vol_required: bool = False
    short_impulse_up_pct: float = 1.0
    short_impulse_cooldown_bars: int = 3
    short_impulse_atr_mult: float = 1.2
    rsi_len: int = 14
    trend_cont_rsi_min: float = 52.0
    trend_cont_low_tol_pct: float = 0.002
    trend_cont_lookback_high: int = 20
    trend_cont_body_atr_max: float = 1.6
    trend_cont_min_closes: int = 2
    trend_cont_hold_bars: int = 5
    trend_cont_hold_ratio_min: float = 0.6
    pullback_only: bool = True
    location_required: bool = True
    bb_len: int = 20
    bb_mult: float = 2.0
    location_long_touch_pct: float = 0.002
    location_short_touch_pct: float = 0.002
    location_ema7_atr_mult: float = 0.6
    location_bb_long_max: float = 0.60
    location_bb_short_min: float = 0.40


def ema_list(values, n: int):
    if len(values) < n:
        return []
    k = 2 / (n + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] * (1 - k) + v * k)
    return out


def sma_list(values, n: int):
    if len(values) < n:
        return []
    out = [None] * (n - 1)
    for i in range(n - 1, len(values)):
        out.append(sum(values[i - n + 1:i + 1]) / n)
    return out


def bollinger_last(values, n: int, k: float) -> Optional[tuple]:
    if len(values) < n or n <= 1:
        return None
    window = values[-n:]
    mean = sum(window) / n
    var = sum((v - mean) ** 2 for v in window) / n
    std = var ** 0.5
    upper = mean + k * std
    lower = mean - k * std
    return mean, upper, lower


def atr_list(highs, lows, closes, n: int):
    if len(highs) < n + 1 or len(lows) < n + 1 or len(closes) < n + 1:
        return []
    trs = []
    for i in range(len(highs)):
        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        trs.append(tr)
    out = [None] * (n - 1)
    for i in range(n - 1, len(trs)):
        out.append(sum(trs[i - n + 1:i + 1]) / n)
    return out


def rsi_list(values, n: int):
    if len(values) < n + 1:
        return []
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n
    out = [None] * n
    for i in range(n, len(deltas)):
        if i == n:
            ag = avg_gain
            al = avg_loss
        else:
            ag = (ag * (n - 1) + gains[i]) / n
            al = (al * (n - 1) + losses[i]) / n
        if al == 0:
            out.append(100.0)
        else:
            rs = ag / al
            out.append(100 - (100 / (1 + rs)))
    return out


def _tf_to_ms(tf: str) -> int:
    tf = (tf or "").lower()
    if tf.endswith("m"):
        try:
            return int(tf[:-1]) * 60 * 1000
        except Exception:
            return 0
    if tf.endswith("h"):
        try:
            return int(tf[:-1]) * 60 * 60 * 1000
        except Exception:
            return 0
    if tf.endswith("d"):
        try:
            return int(tf[:-1]) * 24 * 60 * 60 * 1000
        except Exception:
            return 0
    return 0


def macd_list(values, fast: int = 12, slow: int = 26, signal: int = 9):
    if len(values) < slow + signal + 5:
        return [], [], []
    fast_ema = ema_list(values, fast)
    slow_ema = ema_list(values, slow)
    m = min(len(fast_ema), len(slow_ema))
    macd_line = [a - b for a, b in zip(fast_ema[-m:], slow_ema[-m:])]
    sig_line = ema_list(macd_line, signal)
    hlen = min(len(macd_line), len(sig_line))
    macd_line = macd_line[-hlen:]
    sig_line = sig_line[-hlen:]
    hist = [a - b for a, b in zip(macd_line, sig_line)]
    return macd_line, sig_line, hist


def _get_bucket_with_key(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    bucket = state.get(key)
    if not isinstance(bucket, dict):
        bucket = {}
        state[key] = bucket
    return bucket


def _get_early_bucket(state: Dict[str, Any]) -> Dict[str, Any]:
    bucket = state.get("_fabio_early")
    if not isinstance(bucket, dict):
        bucket = {}
        state["_fabio_early"] = bucket
    return bucket


def evaluate_early_short(symbol: str, state: Dict[str, Any], cfg: Optional[Config] = None) -> Dict[str, Any]:
    cfg = cfg or Config()
    res = {"ok": False, "reason": None}
    df = cycle_cache.get_df(symbol, cfg.timeframe_ltf, cfg.limit)
    if df.empty or len(df) < 80:
        return res
    df = df.iloc[:-1]
    if len(df) < 80:
        return res
    closes = df["close"].tolist()
    highs = df["high"].tolist()
    lows = df["low"].tolist()
    opens = df["open"].tolist()
    ema20 = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("ema", cfg.ema_slow), lambda: ema_list(closes, cfg.ema_slow)
    )
    ema60 = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("ema", 60), lambda: ema_list(closes, 60)
    )
    if not ema20 or not ema60:
        return res
    ema20_v = float(ema20[-1])
    ema60_v = float(ema60[-1])
    if ema60_v == 0:
        return res
    dist = abs(ema20_v - ema60_v) / ema60_v
    if dist < cfg.early_ema_dist_pct:
        return res

    lookback = max(5, int(cfg.early_lookback_high))
    recent_high = max(highs[-(lookback + 1):-1])
    last_high = float(highs[-1])
    last_close = float(closes[-1])
    last_open = float(opens[-1])
    retest_fail = (recent_high >= ema20_v) and (last_high >= ema20_v * (1 - cfg.early_retest_buffer_pct / 100.0)) and (last_close < ema20_v)

    body = abs(last_close - last_open)
    upper = last_high - max(last_open, last_close)
    wick_reject = upper >= max(body, 1e-9) * cfg.early_wick_ratio

    macd_line, sig_line, hist = cycle_cache.get_ind(
        symbol,
        cfg.timeframe_ltf,
        ("macd", 12, 26, 9),
        lambda: macd_list(closes, 12, 26, 9),
    )
    momentum_weak = len(hist) >= 2 and (hist[-1] < hist[-2])

    if retest_fail and (wick_reject or momentum_weak):
        res["ok"] = True
        res["reason"] = "early_short"
        bucket = _get_early_bucket(state)
        bucket[symbol] = {"ts": time.time(), "reason": res["reason"]}
    return res


def evaluate_symbol(
    symbol: str,
    ex,
    state: Dict[str, Any],
    cfg: Optional[Config] = None,
    bucket_key: str = "_fabio",
    side_hint: Optional[str] = None,
) -> Dict[str, Any]:
    cfg = cfg or Config()
    res = {
        "long": False,
        "short": False,
        "reason": None,
        "status": "ok",
        "block_reason": None,
        "confirm_ok": None,
        "trigger_ok": None,
        "trigger_price_ok": None,
        "retest_ok": None,
        "dist_ok": None,
        "vol_ok": None,
        "entry_ready": None,
        "price_ok": None,
        "price_ok_strict": None,
        "price_ok_soft": None,
        "price_close": None,
        "price_ema7": None,
        "price_ema20": None,
        "price_ema60": None,
        "ema7_hold_ratio": None,
        "ema7_hold_ok": None,
        "trend_struct_ok": None,
        "price_between_ema20_ema60": None,
        "trend_cont_trigger": None,
        "fast_drop": None,
        "rsi": None,
        "vol_ratio": None,
        "dist_pct": None,
        "trigger_tf": cfg.timeframe_ltf,
        "signal_side": None,
        "signal_trigger_ok": False,
        "signal_strength": 0.0,
        "signal_reasons": [],
        "signal_factors": {},
        "signal_weights": {},
        "trigger_factors": {},
        "trigger_hard": False,
        "trigger_soft": False,
        "trigger_mode": "NONE",
        "vol_now": None,
        "vol_avg": None,
        "vol_ratio": None,
        "dist_to_ema20": None,
        "atr_now": None,
        "retest_strict": None,
        "location_ok": None,
        "bb_pos": None,
    }
    bucket = _get_bucket_with_key(state, bucket_key)
    sym = bucket.setdefault(symbol, {"mode": "IDLE", "last_ts": None})
    side_hint = (side_hint or "").upper()
    if side_hint not in ("LONG", "SHORT"):
        side_hint = None

    htf = cycle_cache.get_df(symbol, cfg.timeframe_htf, cfg.limit)
    ltf = cycle_cache.get_df(symbol, cfg.timeframe_ltf, cfg.limit)
    if htf.empty or ltf.empty:
        res["status"] = "warmup"
        res["block_reason"] = "warmup"
        return res

    if len(htf) < 60 or len(ltf) < 60:
        res["status"] = "warmup"
        res["block_reason"] = "warmup"
        return res

    htf = htf[:-1]
    ltf = ltf[:-1]
    if len(htf) < 2 or len(ltf) < 2:
        return res

    last = ltf.iloc[-1]
    last_ts = sym.get("last_ts")
    if last_ts == int(last.ts):
        res["block_reason"] = "dup_candle"
        return res
    sym["last_ts"] = int(last.ts)

    htf_closes = htf["close"].tolist()
    htf_highs = htf["high"].tolist()
    htf_lows = htf["low"].tolist()
    htf_vols = htf["volume"].tolist()
    ltf_closes = ltf["close"].tolist()
    ltf_highs = ltf["high"].tolist()
    ltf_lows = ltf["low"].tolist()
    ltf_opens = ltf["open"].tolist()
    ltf_vols = ltf["volume"].tolist()

    ema7_htf = cycle_cache.get_ind(
        symbol, cfg.timeframe_htf, ("ema", cfg.ema_fast), lambda: ema_list(htf_closes, cfg.ema_fast)
    )
    ema20_htf = cycle_cache.get_ind(
        symbol, cfg.timeframe_htf, ("ema", cfg.ema_slow), lambda: ema_list(htf_closes, cfg.ema_slow)
    )
    ema60_htf = cycle_cache.get_ind(
        symbol, cfg.timeframe_htf, ("ema", 60), lambda: ema_list(htf_closes, 60)
    )
    ema7_ltf = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("ema", cfg.ema_fast), lambda: ema_list(ltf_closes, cfg.ema_fast)
    )
    ema20_ltf = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("ema", cfg.ema_slow), lambda: ema_list(ltf_closes, cfg.ema_slow)
    )
    ema60_ltf = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("ema", 60), lambda: ema_list(ltf_closes, 60)
    )
    vol_sma_htf = cycle_cache.get_ind(
        symbol, cfg.timeframe_htf, ("vol_sma", cfg.vol_sma_len), lambda: sma_list(htf_vols, cfg.vol_sma_len)
    )
    vol_sma_ltf = cycle_cache.get_ind(
        symbol, cfg.timeframe_ltf, ("vol_sma", cfg.vol_sma_len), lambda: sma_list(ltf_vols, cfg.vol_sma_len)
    )

    if not (ema7_htf and ema20_htf and ema7_ltf and ema20_ltf and ema60_ltf and vol_sma_ltf):
        return res

    h_ema7 = float(ema7_htf[-1])
    h_ema20 = float(ema20_htf[-1])
    h_close = float(htf_closes[-1])
    h_ema60 = float(ema60_htf[-1]) if ema60_htf else None
    spread = abs(h_ema7 - h_ema20) / h_ema20 * 100.0 if h_ema20 else 0.0
    if spread < cfg.regime_min_ema_spread_pct:
        sym["mode"] = "IDLE"

    regime = (
        "LONG"
        if h_ema7 > h_ema20 and h_close >= h_ema20
        else "SHORT"
        if h_ema7 < h_ema20 and h_close <= h_ema20
        else "NONE"
    )
    if sym.get("mode") == "WAIT_LONG" and regime != "LONG":
        sym["mode"] = "IDLE"
    if sym.get("mode") == "WAIT_SHORT" and regime != "SHORT":
        sym["mode"] = "IDLE"
    if side_hint == "LONG" and sym.get("mode") == "WAIT_SHORT":
        sym["mode"] = "IDLE"
    if side_hint == "SHORT" and sym.get("mode") == "WAIT_LONG":
        sym["mode"] = "IDLE"

    l_ema20 = float(ema20_ltf[-1])
    l_ema60 = float(ema60_ltf[-1])
    l_ema7 = float(ema7_ltf[-1])
    l_close = float(last.close)
    l_low = float(last.low)
    l_high = float(last.high)
    l_open = float(last.open)
    l_vol = float(last.volume)
    l_vol_sma = float(vol_sma_ltf[-1]) if vol_sma_ltf[-1] is not None else 0.0
    l_vol_ratio = (l_vol / l_vol_sma) if l_vol_sma > 0 else 0.0
    ext = abs(l_close - l_ema20) / l_ema20 * 100.0 if l_ema20 else 0.0
    if ext > cfg.ltf_max_extension_pct:
        res["block_reason"] = "chase"
        return res

    if sym.get("mode") == "IDLE" and regime in ("LONG", "SHORT"):
        if (
            regime == "LONG"
            and (side_hint in (None, "LONG"))
            and l_low <= l_ema20 * (1 + cfg.retest_buffer_pct / 100.0)
            and l_vol_ratio <= cfg.pullback_vol_ratio_max
        ):
            sym["mode"] = "WAIT_LONG"
        if (
            regime == "SHORT"
            and (side_hint in (None, "SHORT"))
            and l_high >= l_ema20 * (1 - cfg.retest_buffer_pct / 100.0)
            and l_vol_ratio <= cfg.pullback_vol_ratio_max
        ):
            sym["mode"] = "WAIT_SHORT"
        if sym.get("mode") == "IDLE":
            if l_vol_ratio > cfg.pullback_vol_ratio_max:
                res["block_reason"] = "pullback_vol"
            else:
                res["block_reason"] = "retest"

    if sym.get("mode") == "WAIT_LONG" and side_hint != "SHORT":
        ema20_slope = None
        ema60_slope = None
        if len(ema20_ltf) >= 2 and len(ema60_ltf) >= 2:
            ema20_slope = float(ema20_ltf[-1]) - float(ema20_ltf[-2])
            ema60_slope = float(ema60_ltf[-1]) - float(ema60_ltf[-2])
        slope_ok = (
            (ema20_slope is not None and ema60_slope is not None)
            and (ema20_slope > 0)
            and (ema60_slope >= 0)
        )
        above2_ok = False
        if len(ltf_closes) >= 2 and len(ema20_ltf) >= 2:
            above2_ok = (float(ltf_closes[-1]) > float(ema20_ltf[-1])) and (float(ltf_closes[-2]) > float(ema20_ltf[-2]))
        confirm_ok = slope_ok or above2_ok
        dist_ok = False
        dist_pct = None
        dist_ratio = None
        if l_ema20 > 0:
            dist = (l_close - l_ema20) / l_ema20
            dist_ok = dist <= cfg.long_dist_to_ema20_max
            dist_ratio = dist
            dist_pct = dist * 100.0
        retest_ok = False
        if len(ltf_closes) >= 2 and len(ema20_ltf) >= 2 and len(ltf_lows) >= 2 and len(ltf_opens) >= 2:
            start = max(0, len(ltf_closes) - int(cfg.long_retest_window))
            touch_idx = None
            for i in range(len(ltf_closes) - 1, start - 1, -1):
                try:
                    ema20_i = float(ema20_ltf[i])
                    low_i = float(ltf_lows[i])
                except Exception:
                    continue
                if ema20_i <= 0:
                    continue
                if low_i <= ema20_i * (1 + cfg.long_touch_tol):
                    touch_idx = i
                    break
            if touch_idx is not None:
                for j in range(touch_idx + 1, len(ltf_closes)):
                    try:
                        ema20_j = float(ema20_ltf[j])
                        close_j = float(ltf_closes[j])
                        open_j = float(ltf_opens[j])
                    except Exception:
                        continue
                    if ema20_j <= 0:
                        continue
                    reclaim = close_j >= ema20_j * (1 + cfg.long_reclaim_tol)
                    bullish = close_j > open_j
                    if reclaim and bullish:
                        retest_ok = True
                        break
        retest_strict = False
        if len(ltf_closes) >= 2 and len(ema20_ltf) >= 2 and len(ltf_lows) >= 2:
            prev_low = float(ltf_lows[-2])
            ema20_prev = float(ema20_ltf[-2])
            if ema20_prev > 0:
                touched = prev_low <= ema20_prev * (1 + cfg.location_long_touch_pct)
                reclaimed = l_close > l_ema20
                retest_strict = bool(touched and reclaimed)
        vol_ok = l_vol_ratio >= cfg.trigger_vol_ratio_min
        trigger_price_ok = (l_close > l_ema7 and l_close > l_open)
        trigger_ok = (trigger_price_ok and vol_ok)
        price_ok_strict = bool(trigger_price_ok)
        price_ok_soft = (l_close > l_ema7)
        hold_bars = max(2, int(cfg.trend_cont_hold_bars))
        hold_count = 0
        hold_total = 0
        for i in range(max(0, len(ltf_closes) - hold_bars), len(ltf_closes)):
            try:
                close_i = float(ltf_closes[i])
                ema7_i = float(ema7_ltf[i])
            except Exception:
                continue
            hold_total += 1
            if close_i >= ema7_i:
                hold_count += 1
        ema7_hold_ratio = (hold_count / hold_total) if hold_total > 0 else 0.0
        ema7_hold_ok = ema7_hold_ratio >= cfg.trend_cont_hold_ratio_min
        trend_struct_ok = False
        struct_bars = max(3, int(cfg.trend_cont_min_closes) + 1)
        prev_highs = ltf_highs[-(struct_bars + 1):-1] if len(ltf_highs) > struct_bars else ltf_highs[:-1]
        prev_lows = ltf_lows[-(struct_bars + 1):-1] if len(ltf_lows) > struct_bars else ltf_lows[:-1]
        if prev_highs and prev_lows:
            try:
                trend_struct_ok = (l_high >= max(prev_highs)) and (l_low >= min(prev_lows))
            except Exception:
                trend_struct_ok = False
        price_between_ema20_ema60 = None
        if l_ema20 and l_ema60:
            low_band = min(l_ema20, l_ema60)
            high_band = max(l_ema20, l_ema60)
            price_between_ema20_ema60 = low_band <= l_close <= high_band
        fast_drop = False
        trend_cont_trigger = False
        atr_vals = atr_list(ltf_highs, ltf_lows, ltf_closes, cfg.atr_len)
        atr_now = atr_vals[-1] if atr_vals else None
        bb_vals = bollinger_last(ltf_closes, cfg.bb_len, cfg.bb_mult)
        bb_pos = None
        bb_ok = False
        if bb_vals:
            _, bb_up, bb_dn = bb_vals
            if bb_up != bb_dn:
                bb_pos = (l_close - bb_dn) / (bb_up - bb_dn)
                bb_ok = bb_pos <= cfg.location_bb_long_max
        ema20_touch_ok = l_low <= l_ema20 * (1 + cfg.location_long_touch_pct)
        ema7_dist_ok = False
        if isinstance(atr_now, (int, float)) and atr_now > 0:
            ema7_dist_ok = abs(l_close - l_ema7) <= float(atr_now) * cfg.location_ema7_atr_mult
        location_hits = sum(1 for v in (ema20_touch_ok, ema7_dist_ok, bb_ok) if v)
        location_ok = location_hits >= 2
        body = abs(l_close - l_open)
        body_ok = True
        if isinstance(atr_now, (int, float)) and atr_now > 0:
            body_ok = body <= (atr_now * cfg.trend_cont_body_atr_max)
        rsi_len = getattr(cfg, "rsi_len", 14)
        rsi_vals = rsi_list(ltf_closes, rsi_len)
        rsi_last = rsi_vals[-1] if rsi_vals else None
        rsi_ok = isinstance(rsi_last, (int, float)) and rsi_last >= cfg.trend_cont_rsi_min
        low_tol = 1 - float(cfg.trend_cont_low_tol_pct)
        min_closes = max(2, int(cfg.trend_cont_min_closes))
        last_closes = ltf_closes[-min_closes:] if len(ltf_closes) >= min_closes else ltf_closes
        last_lows = ltf_lows[-min_closes:] if len(ltf_lows) >= min_closes else ltf_lows
        ema7_last = l_ema7
        consecutive_ok = (
            len(last_closes) >= min_closes
            and all(c >= ema7_last for c in last_closes)
            and all(l >= ema7_last * low_tol for l in last_lows)
            and rsi_ok
        )
        lookback = max(5, int(cfg.trend_cont_lookback_high))
        prev_highs = ltf_highs[-(lookback + 1):-1] if len(ltf_highs) > lookback else ltf_highs[:-1]
        breakout_ok = bool(prev_highs) and (l_high >= max(prev_highs)) and (l_close >= l_ema7)
        touch_ok = (l_low <= l_ema7 * (1 + cfg.long_touch_tol)) and (l_close >= l_ema7)
        if dist_ok and body_ok and (not fast_drop) and (consecutive_ok or breakout_ok or touch_ok):
            trend_cont_trigger = True
        retest_ok_final = bool(retest_ok and retest_strict) if cfg.pullback_only else bool(retest_ok)
        location_ok_final = bool(location_ok) if cfg.location_required else True
        entry_ready = confirm_ok and retest_ok_final and dist_ok and vol_ok and location_ok_final
        res["confirm_ok"] = bool(confirm_ok)
        res["retest_ok"] = bool(retest_ok_final)
        res["retest_strict"] = bool(retest_strict)
        res["dist_ok"] = bool(dist_ok)
        res["vol_ok"] = bool(vol_ok)
        res["trigger_ok"] = bool(trigger_ok)
        res["trigger_price_ok"] = bool(trigger_price_ok)
        res["price_ok"] = bool(trigger_price_ok)
        res["price_ok_strict"] = bool(price_ok_strict)
        res["price_ok_soft"] = bool(price_ok_soft)
        res["price_close"] = float(l_close)
        res["price_ema7"] = float(l_ema7)
        res["price_ema20"] = float(l_ema20)
        res["price_ema60"] = float(l_ema60)
        res["ema7_hold_ratio"] = float(ema7_hold_ratio)
        res["ema7_hold_ok"] = bool(ema7_hold_ok)
        res["trend_struct_ok"] = bool(trend_struct_ok)
        res["price_between_ema20_ema60"] = (
            bool(price_between_ema20_ema60) if price_between_ema20_ema60 is not None else None
        )
        res["trend_cont_trigger"] = bool(trend_cont_trigger)
        res["fast_drop"] = False
        res["entry_ready"] = bool(entry_ready)
        res["rsi"] = rsi_last
        res["vol_ratio"] = l_vol_ratio
        res["vol_now"] = l_vol
        res["vol_avg"] = l_vol_sma
        res["dist_to_ema20"] = dist_ratio
        res["atr_now"] = atr_now
        res["location_ok"] = bool(location_ok_final)
        res["bb_pos"] = bb_pos
        res["dist_pct"] = dist_pct
        res["dist_ratio"] = dist_ratio
        res["dist_mode"] = "ratio"
        trigger_rsi = bool(rsi_ok)
        trigger_retest = bool(retest_strict)
        trigger_vol = bool(vol_ok)
        trigger_struct = bool(confirm_ok)
        trigger_ema = bool(l_close > l_ema7)
        hard_trigger = trigger_rsi or (trigger_retest and trigger_struct) or (trigger_vol and trigger_struct)
        soft_trigger = (trigger_ema and trigger_struct) or (trigger_retest and trigger_ema) or (trigger_vol and trigger_ema) or trigger_struct
        trigger_mode = "HARD" if hard_trigger else ("SOFT" if soft_trigger else "NONE")
        res["trigger_factors"] = {
            "rsi": trigger_rsi,
            "retest": trigger_retest,
            "vol": trigger_vol,
            "struct": trigger_struct,
            "ema": trigger_ema,
        }
        res["trigger_hard"] = bool(hard_trigger)
        res["trigger_soft"] = bool(soft_trigger)
        res["trigger_mode"] = trigger_mode
        signal_trigger_ok = bool(hard_trigger or soft_trigger)
        ema_align_ok = (l_close > l_ema20) and (l_ema7 > l_ema20)
        rsi_reversal_ok = isinstance(rsi_last, (int, float)) and rsi_last >= cfg.trend_cont_rsi_min
        structure_ok = bool(confirm_ok)
        weights = {
            "ema_align": 2,
            "rsi_reversal": 2,
            "vol": 2,
            "retest": 2,
            "structure": 1,
            "dist": 1,
        }
        factors = {
            "ema_align": ema_align_ok,
            "rsi_reversal": rsi_reversal_ok,
            "vol": bool(vol_ok),
            "retest": bool(retest_ok_final),
            "structure": structure_ok,
            "dist": bool(dist_ok),
        }
        denom = sum(weights.values()) or 1
        score = sum(weights[k] for k, v in factors.items() if v)
        strength = max(0.0, min(1.0, float(score) / float(denom)))
        res["signal_side"] = "LONG"
        res["signal_trigger_ok"] = signal_trigger_ok
        res["signal_strength"] = strength
        res["signal_reasons"] = [k for k, v in factors.items() if v]
        res["signal_factors"] = factors
        res["signal_weights"] = weights
        if (
            l_close > l_ema7
            and l_close > l_open
            and vol_ok
            and confirm_ok
            and retest_ok_final
            and dist_ok
            and location_ok_final
        ):
            sym["mode"] = "IDLE"
            res["long"] = True
            res["reason"] = "fabio_long|retest"
        else:
            if not retest_ok_final:
                res["block_reason"] = "retest"
            elif not location_ok_final:
                res["block_reason"] = "no_location"
            elif not dist_ok:
                res["block_reason"] = "chase"
            elif l_vol_ratio < cfg.trigger_vol_ratio_min:
                res["block_reason"] = "volume"
            elif not confirm_ok:
                res["block_reason"] = "regime"

    if sym.get("mode") == "WAIT_SHORT" and side_hint != "LONG":
        short_tf = cfg.short_timeframe_primary or cfg.timeframe_ltf
        short_limit = int(cfg.short_limit) if cfg.short_limit else cfg.limit
        stf = cycle_cache.get_df(symbol, short_tf, short_limit)
        if stf.empty or len(stf) < 20:
            stf = ltf
            short_tf = cfg.timeframe_ltf
        stf = stf.iloc[:-1]
        if len(stf) < 2:
            bucket[symbol] = sym
            res["block_reason"] = "warmup"
            return res
        stf_closes = stf["close"].tolist()
        stf_highs = stf["high"].tolist()
        stf_lows = stf["low"].tolist()
        stf_opens = stf["open"].tolist()
        stf_vols = stf["volume"].tolist()
        ema7_stf = cycle_cache.get_ind(
            symbol, short_tf, ("ema", cfg.ema_fast), lambda: ema_list(stf_closes, cfg.ema_fast)
        )
        ema20_stf = cycle_cache.get_ind(
            symbol, short_tf, ("ema", cfg.ema_slow), lambda: ema_list(stf_closes, cfg.ema_slow)
        )
        ema60_stf = cycle_cache.get_ind(
            symbol, short_tf, ("ema", 60), lambda: ema_list(stf_closes, 60)
        )
        vol_sma_stf = cycle_cache.get_ind(
            symbol, short_tf, ("vol_sma", cfg.vol_sma_len), lambda: sma_list(stf_vols, cfg.vol_sma_len)
        )
        if not (ema20_stf and ema60_stf and ema7_stf and vol_sma_stf):
            bucket[symbol] = sym
            res["block_reason"] = "warmup"
            return res

        s_ema7 = float(ema7_stf[-1])
        s_ema20 = float(ema20_stf[-1])
        s_ema60 = float(ema60_stf[-1])
        s_close = float(stf_closes[-1])
        s_open = float(stf_opens[-1])
        s_high = float(stf_highs[-1])
        s_low = float(stf_lows[-1])
        s_vol = float(stf_vols[-1])
        s_vol_sma = float(vol_sma_stf[-1]) if vol_sma_stf[-1] is not None else 0.0
        s_vol_ratio = (s_vol / s_vol_sma) if s_vol_sma > 0 else 0.0

        regime_ok = False
        if h_ema60 is not None and h_ema60 > 0:
            if (h_close < h_ema60) or (h_ema20 < h_ema60):
                regime_ok = True
        if s_ema20 < s_ema60:
            regime_ok = True
        if not regime_ok:
            bucket[symbol] = sym
            res["block_reason"] = "regime"
            return res

        dist_ok = False
        dist_pct = None
        dist_ratio = None
        if s_ema20 > 0:
            dist = abs(s_ema20 - s_close) / s_ema20
            dist_ok = dist <= cfg.short_dist_to_ema20_max
            dist_ratio = dist
            dist_pct = dist * 100.0

        fast_drop = False
        lookback = max(2, int(cfg.short_fast_drop_lookback))
        if len(stf_closes) > lookback:
            prev = float(stf_closes[-lookback - 1])
            if prev > 0:
                drop_pct = (s_close / prev - 1.0) * 100.0
                if drop_pct <= -abs(cfg.short_fast_drop_pct):
                    fast_drop = True

        lower_high = False
        if len(stf_highs) >= cfg.short_lower_high_lookback + 1:
            prev_max = max(stf_highs[-(cfg.short_lower_high_lookback + 1):-1])
            lower_high = stf_highs[-1] < prev_max

        upper_wick_chain = False
        wick_hits = 0
        if len(stf_closes) >= cfg.short_upper_wick_chain_len:
            for i in range(-cfg.short_upper_wick_chain_len, 0):
                try:
                    o = float(stf_opens[i])
                    c = float(stf_closes[i])
                    h = float(stf_highs[i])
                except Exception:
                    continue
                body = abs(c - o)
                if body <= 0:
                    continue
                upper = h - max(o, c)
                if (upper / body) >= cfg.short_upper_wick_ratio and c < o:
                    wick_hits += 1
            upper_wick_chain = wick_hits >= cfg.short_upper_wick_chain_len

        rsi_len = getattr(cfg, "rsi_len", 14)
        rsi_vals = rsi_list(stf_closes, rsi_len)
        rsi_downturn = False
        rsi_prev = None
        rsi_now = None
        if len(rsi_vals) >= 2 and rsi_vals[-1] is not None and rsi_vals[-2] is not None:
            rsi_prev = float(rsi_vals[-2])
            rsi_now = float(rsi_vals[-1])
            rsi_downturn = (
                rsi_prev >= cfg.short_rsi_downturn_high
                and rsi_now < rsi_prev
                and (not cfg.short_rsi_downturn_strict or rsi_now < cfg.short_rsi_downturn_high)
            )

        retest_reject_ok = False
        if len(stf_closes) >= 2 and len(ema20_stf) >= 2:
            start = max(0, len(stf_closes) - int(cfg.retest_window))
            last_reject_idx = None
            for i in range(start, len(stf_closes)):
                try:
                    ema20_i = float(ema20_stf[i])
                    high_i = float(stf_highs[i])
                    close_i = float(stf_closes[i])
                except Exception:
                    continue
                if ema20_i <= 0:
                    continue
                touched = high_i >= ema20_i * (1 - cfg.retest_touch_tol)
                rejected = close_i <= ema20_i * (1 - cfg.retest_reject_tol)
                if touched and rejected:
                    last_reject_idx = i
            if last_reject_idx is not None:
                recent_threshold = max(0, len(stf_closes) - int(cfg.short_retest_recent))
                if last_reject_idx >= recent_threshold:
                    retest_reject_ok = True
        retest_strict = False
        if len(stf_closes) >= 2 and len(ema20_stf) >= 2 and len(stf_highs) >= 2:
            prev_high = float(stf_highs[-2])
            ema20_prev = float(ema20_stf[-2])
            if ema20_prev > 0:
                touched = prev_high >= ema20_prev * (1 - cfg.location_short_touch_pct)
                reclaimed = s_close < s_ema20
                retest_strict = bool(touched and reclaimed)

        trigger_ok = False
        trigger_price_ok = s_close < s_ema20
        reject_candle = False
        body = abs(s_close - s_open)
        if body > 0:
            upper = s_high - max(s_open, s_close)
            wick_ratio = upper / body
            if wick_ratio >= cfg.short_reject_wick_ratio and s_close <= s_open and s_close < s_ema20:
                reject_candle = True
        trigger_ok = rsi_downturn and reject_candle

        if cfg.short_timeframe_secondary:
            sec_tf = cfg.short_timeframe_secondary
            sec_df = cycle_cache.get_df(symbol, sec_tf, short_limit)
            if not sec_df.empty:
                sec_df = sec_df.iloc[:-1]
                if len(sec_df) >= 2:
                    rsi_len = getattr(cfg, "rsi_len", 14)
                    sec_rsi = rsi_list(sec_df["close"].tolist(), rsi_len)
                    if len(sec_rsi) >= 2 and sec_rsi[-1] is not None and sec_rsi[-2] is not None:
                        if (sec_rsi[-2] >= cfg.short_rsi_downturn_high) and (sec_rsi[-1] < sec_rsi[-2]):
                            if not cfg.short_rsi_downturn_strict or sec_rsi[-1] < cfg.short_rsi_downturn_high:
                                rsi_downturn = True
        trigger_ok = rsi_downturn and reject_candle

        vol_ok = s_vol_ratio >= cfg.short_vol_ratio_min
        structure_ok = retest_reject_ok or lower_high or upper_wick_chain
        atr_vals = atr_list(stf_highs, stf_lows, stf_closes, cfg.atr_len)
        atr_now = atr_vals[-1] if atr_vals else None
        bb_vals = bollinger_last(stf_closes, cfg.bb_len, cfg.bb_mult)
        bb_pos = None
        bb_ok = False
        if bb_vals:
            _, bb_up, bb_dn = bb_vals
            if bb_up != bb_dn:
                bb_pos = (s_close - bb_dn) / (bb_up - bb_dn)
                bb_ok = bb_pos >= cfg.location_bb_short_min
        ema20_touch_ok = s_high >= s_ema20 * (1 - cfg.location_short_touch_pct)
        ema7_dist_ok = False
        if isinstance(atr_now, (int, float)) and atr_now > 0:
            ema7_dist_ok = abs(s_close - s_ema7) <= float(atr_now) * cfg.location_ema7_atr_mult
        location_hits = sum(1 for v in (ema20_touch_ok, ema7_dist_ok, bb_ok) if v)
        location_ok = location_hits >= 2
        impulse_block = False
        block_until = sym.get("short_block_until_ts")
        s_ts = None
        if "ts" in stf.columns:
            try:
                s_ts = int(stf["ts"].iloc[-1])
            except Exception:
                s_ts = None
        tf_ms = _tf_to_ms(short_tf)
        if block_until and s_ts and s_ts < block_until:
            impulse_block = True
        elif len(stf_closes) >= 2:
            prev_close = float(stf_closes[-2])
            pct_change = ((s_close / prev_close - 1.0) * 100.0) if prev_close > 0 else 0.0
            crossed = prev_close <= s_ema20 and s_close > s_ema20
            impulse_up = pct_change >= cfg.short_impulse_up_pct and (s_close > s_ema20 or crossed)
            if impulse_up and cfg.short_impulse_atr_mult > 0:
                atr_vals = atr_list(stf_highs, stf_lows, stf_closes, cfg.atr_len)
                if len(atr_vals) >= 1 and atr_vals[-1] is not None:
                    rng = s_high - s_low
                    impulse_up = rng >= float(atr_vals[-1]) * cfg.short_impulse_atr_mult
            if impulse_up and tf_ms > 0 and s_ts:
                sym["short_block_until_ts"] = s_ts + tf_ms * max(1, int(cfg.short_impulse_cooldown_bars))
                impulse_block = True
        retest_ok_final = bool(retest_reject_ok and retest_strict) if cfg.pullback_only else bool(retest_reject_ok)
        location_ok_final = bool(location_ok) if cfg.location_required else True
        entry_ready = (
            regime_ok
            and structure_ok
            and trigger_ok
            and trigger_price_ok
            and dist_ok
            and (vol_ok or not cfg.short_vol_required)
            and (not fast_drop)
            and (not impulse_block)
            and retest_ok_final
            and location_ok_final
        )

        res["confirm_ok"] = bool(structure_ok)
        res["retest_ok"] = bool(retest_ok_final)
        res["retest_strict"] = bool(retest_strict)
        res["dist_ok"] = bool(dist_ok)
        res["vol_ok"] = bool(vol_ok)
        res["trigger_ok"] = bool(trigger_ok)
        res["trigger_price_ok"] = bool(trigger_price_ok)
        res["price_ok"] = bool(trigger_price_ok)
        res["price_ok_strict"] = bool(trigger_price_ok)
        res["price_ok_soft"] = None
        res["price_close"] = float(s_close)
        res["price_ema7"] = None
        res["price_ema20"] = float(s_ema20)
        res["rsi_downturn"] = bool(rsi_downturn)
        res["reject_candle"] = bool(reject_candle)
        res["fast_drop"] = bool(fast_drop)
        res["impulse_block"] = bool(impulse_block)
        res["entry_ready"] = bool(entry_ready)
        res["rsi"] = rsi_vals[-1] if rsi_vals else None
        res["vol_ratio"] = s_vol_ratio
        res["vol_now"] = s_vol
        res["vol_avg"] = s_vol_sma
        res["dist_to_ema20"] = dist_ratio
        res["atr_now"] = atr_now
        res["location_ok"] = bool(location_ok_final)
        res["bb_pos"] = bb_pos
        res["dist_pct"] = dist_pct
        res["dist_ratio"] = dist_ratio
        res["dist_mode"] = "ratio"
        trigger_rsi = bool(rsi_downturn)
        trigger_retest = bool(retest_strict)
        trigger_vol = bool(vol_ok)
        trigger_struct = bool(structure_ok)
        trigger_ema = bool(s_close < s_ema20)
        hard_trigger = trigger_rsi or (trigger_retest and trigger_struct) or (trigger_vol and trigger_struct)
        soft_trigger = (trigger_ema and trigger_struct) or (trigger_retest and trigger_ema) or (trigger_vol and trigger_ema) or trigger_struct
        trigger_mode = "HARD" if hard_trigger else ("SOFT" if soft_trigger else "NONE")
        res["trigger_factors"] = {
            "rsi": trigger_rsi,
            "retest": trigger_retest,
            "vol": trigger_vol,
            "struct": trigger_struct,
            "ema": trigger_ema,
        }
        res["trigger_hard"] = bool(hard_trigger)
        res["trigger_soft"] = bool(soft_trigger)
        res["trigger_mode"] = trigger_mode
        ema_align_ok = s_close < s_ema20
        rsi_reversal_ok = bool(rsi_downturn)
        structure_ok = bool(structure_ok)
        weights = {
            "ema_align": 2,
            "rsi_reversal": 2,
            "vol": 2,
            "retest": 2,
            "structure": 1,
            "dist": 1,
        }
        factors = {
            "ema_align": ema_align_ok,
            "rsi_reversal": rsi_reversal_ok,
            "vol": bool(vol_ok),
            "retest": bool(retest_ok_final),
            "structure": structure_ok,
            "dist": bool(dist_ok),
        }
        denom = sum(weights.values()) or 1
        score = sum(weights[k] for k, v in factors.items() if v)
        strength = max(0.0, min(1.0, float(score) / float(denom)))
        res["signal_side"] = "SHORT"
        res["signal_trigger_ok"] = bool(hard_trigger or soft_trigger)
        res["signal_strength"] = strength
        res["signal_reasons"] = [k for k, v in factors.items() if v]
        res["signal_factors"] = factors
        res["signal_weights"] = weights
        if entry_ready:
            sym["mode"] = "IDLE"
            res["short"] = True
            res["reason"] = "fabio_short|retest"
        else:
            if not regime_ok:
                res["block_reason"] = "regime"
            elif impulse_block:
                res["block_reason"] = "impulse_up"
            elif not retest_ok_final:
                res["block_reason"] = "retest"
            elif not location_ok_final:
                res["block_reason"] = "no_location"
            elif not dist_ok or fast_drop:
                res["block_reason"] = "chase"
            elif not rsi_downturn:
                res["block_reason"] = "rsi_not_downturn"
            elif not reject_candle:
                res["block_reason"] = "no_reject_candle"
            elif not structure_ok or not trigger_ok or not trigger_price_ok:
                res["block_reason"] = "retest"
            elif cfg.short_vol_required and not vol_ok:
                res["block_reason"] = "volume"

    bucket[symbol] = sym
    return res
