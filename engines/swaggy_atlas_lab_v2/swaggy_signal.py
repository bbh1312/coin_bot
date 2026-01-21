from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from engines.swaggy_atlas_lab_v2.config import SwaggyConfig
from engines.swaggy_atlas_lab_v2.indicators import VPLevels, atr, build_vp_levels, detect_regime, ema


@dataclass
class SwaggySignal:
    entry_ok: bool
    side: Optional[str]
    strength: float
    reasons: List[str]
    trigger: str
    entry_px: Optional[float]
    debug: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Level:
    price: float
    kind: str
    low: Optional[float] = None
    high: Optional[float] = None
    ts: Optional[float] = None


@dataclass
class Trigger:
    side: str
    kind: str
    level: Level
    strength: float
    evidence: Dict[str, float]


def _cluster_levels(levels: List[Level], cluster_pct: float) -> List[Level]:
    if not levels:
        return []
    levels = sorted(levels, key=lambda x: x.price)
    clustered: List[Level] = []
    bucket = [levels[0]]
    for lv in levels[1:]:
        anchor = bucket[-1]
        dist = abs(lv.price - anchor.price) / anchor.price if anchor.price else 0.0
        if dist <= cluster_pct:
            bucket.append(lv)
        else:
            clustered.append(_merge_bucket(bucket))
            bucket = [lv]
    if bucket:
        clustered.append(_merge_bucket(bucket))
    return clustered


def _level_key(level: Level, tick_size: float) -> int:
    if tick_size and tick_size > 0:
        return int(round(level.price / tick_size))
    return int(round(level.price * 1e8))


def _level_type(kind: str) -> str:
    kind = (kind or "").upper()
    if "VP" in kind:
        return "VP"
    if "SWING" in kind:
        return "SWING"
    if "ROUND" in kind:
        return "ROUND"
    return kind or "UNKNOWN"


def _ts_to_sec(ts_val: float) -> float:
    if ts_val > 1e12:
        return float(ts_val) / 1000.0
    return float(ts_val)


def _level_age_bars(df_htf: pd.DataFrame, level_ts: Optional[float]) -> Optional[int]:
    if level_ts is None or df_htf.empty or "ts" not in df_htf.columns:
        return None
    try:
        level_ts = float(level_ts)
    except Exception:
        return None
    ts_vals = df_htf["ts"].tolist()
    count = 0
    for ts_val in ts_vals:
        try:
            ts_sec = _ts_to_sec(float(ts_val))
        except Exception:
            continue
        if ts_sec >= level_ts:
            count += 1
    return count if count > 0 else None


def _merge_bucket(bucket: List[Level]) -> Level:
    prices = [b.price for b in bucket]
    kinds = {b.kind for b in bucket}
    low = min(b.low for b in bucket if b.low is not None) if any(b.low is not None for b in bucket) else None
    high = max(b.high for b in bucket if b.high is not None) if any(b.high is not None for b in bucket) else None
    kind = "+".join(sorted(kinds))
    return Level(price=float(sum(prices) / len(prices)), kind=kind, low=low, high=high)


def _swing_levels(df: pd.DataFrame, lookback: int = 60) -> List[Level]:
    if df.empty or len(df) < 5:
        return []
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    ts_list = df["ts"].tolist() if "ts" in df.columns else None
    levels: List[Level] = []
    start = max(1, len(df) - lookback - 2)
    for i in range(start, len(df) - 1):
        ts_val = None
        if ts_list is not None:
            try:
                ts_val = float(ts_list[i]) / 1000.0
            except Exception:
                ts_val = None
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            levels.append(Level(price=highs[i], kind="SWING_HIGH", ts=ts_val))
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            levels.append(Level(price=lows[i], kind="SWING_LOW", ts=ts_val))
    return levels


def _round_levels(price: float, tick_size: float, count: int = 6) -> List[Level]:
    if price <= 0 or tick_size <= 0:
        return []
    step = tick_size * 250
    base = round(price / step) * step
    levels = []
    for i in range(-count, count + 1):
        levels.append(Level(price=base + step * i, kind="ROUND"))
    return levels


def build_levels(
    df_1h: pd.DataFrame,
    vp: Optional[VPLevels],
    last_price: float,
    level_cluster_pct: float,
    tick_size: float,
    df_mtf: Optional[pd.DataFrame] = None,
    swing_lookback_1h: int = 60,
    swing_lookback_mtf: int = 32,
    max_dist_pct: float = 0.06,
    level_cap: int = 25,
) -> List[Level]:
    levels: List[Level] = []
    swing_1h = _swing_levels(df_1h, lookback=swing_lookback_1h)
    swing_mtf = _swing_levels(df_mtf, lookback=swing_lookback_mtf) if df_mtf is not None and not df_mtf.empty else []
    levels.extend(swing_1h)
    levels.extend(swing_mtf)
    levels.extend(_round_levels(last_price, tick_size))
    if vp:
        levels.append(Level(price=vp.poc, kind="VP_POC", low=vp.profile.poc.low, high=vp.profile.poc.high))
        levels.append(Level(price=vp.vah, kind="VP_VAH"))
        levels.append(Level(price=vp.val, kind="VP_VAL"))
        for b in vp.profile.hvn:
            levels.append(Level(price=(b.low + b.high) / 2.0, kind="VP_HVN", low=b.low, high=b.high))
        for b in vp.profile.lvn:
            levels.append(Level(price=(b.low + b.high) / 2.0, kind="VP_LVN", low=b.low, high=b.high))
    levels = [lv for lv in levels if lv.price > 0]
    levels = _cluster_levels(levels, level_cluster_pct)
    if last_price > 0 and max_dist_pct and max_dist_pct > 0:
        levels = [
            lv for lv in levels
            if abs(lv.price - last_price) / last_price <= max_dist_pct
        ]
    if level_cap and level_cap > 0:
        levels = sorted(levels, key=lambda lv: abs(lv.price - last_price))[:level_cap]
    return levels


def nearest_level(levels: List[Level], price: float) -> Optional[Level]:
    best = None
    best_dist = None
    for lv in levels:
        dist = abs(price - lv.price)
        if best_dist is None or dist < best_dist:
            best = lv
            best_dist = dist
    return best


def _vol_sma(df: pd.DataFrame, length: int = 20) -> float:
    if df.empty:
        return 0.0
    vols = df["volume"].astype(float)
    if len(vols) < length:
        return float(vols.mean())
    return float(vols.rolling(length).mean().iloc[-1])


def detect_reclaim(df: pd.DataFrame, level: Level, touch_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 4:
        return None
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    c1_close = float(c1["close"])
    c2_close = float(c2["close"])
    c3_close = float(c3["close"])
    if (
        c1_close < level.price * (1 - touch_eps)
        and c2_close >= level.price * (1 - touch_eps)
        and c3_close >= level.price * (1 - touch_eps)
    ):
        return Trigger(
            "long",
            "RECLAIM",
            level,
            0.0,
            {"c1_close": c1_close, "c2_close": c2_close, "c3_close": c3_close},
        )
    if (
        c1_close > level.price * (1 + touch_eps)
        and c2_close <= level.price * (1 + touch_eps)
        and c3_close <= level.price * (1 + touch_eps)
    ):
        return Trigger(
            "short",
            "RECLAIM",
            level,
            0.0,
            {"c1_close": c1_close, "c2_close": c2_close, "c3_close": c3_close},
        )
    return None


def detect_rejection_wick(df: pd.DataFrame, level: Level, wick_ratio: float, vol_mult: float) -> Optional[Trigger]:
    if df.empty:
        return None
    last = df.iloc[-1]
    high = float(last["high"])
    low = float(last["low"])
    close = float(last["close"])
    open_ = float(last["open"])
    vol = float(last["volume"])
    vr = _vol_sma(df)
    if vr <= 0 or vol < vr * vol_mult:
        return None
    candle_range = max(1e-9, high - low)
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    if high >= level.price and upper_wick / candle_range >= wick_ratio and close < level.price:
        return Trigger(
            "short",
            "REJECTION",
            level,
            0.0,
            {"wick_ratio": upper_wick / candle_range, "vol": vol, "vol_sma": vr},
        )
    if low <= level.price and lower_wick / candle_range >= wick_ratio and close > level.price:
        return Trigger(
            "long",
            "REJECTION",
            level,
            0.0,
            {"wick_ratio": lower_wick / candle_range, "vol": vol, "vol_sma": vr},
        )
    return None


def detect_sweep_and_return(df: pd.DataFrame, level: Level, sweep_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 2:
        return None
    prev = df.iloc[-2]
    last = df.iloc[-1]
    prev_high = float(prev["high"])
    prev_low = float(prev["low"])
    last_high = float(last["high"])
    last_low = float(last["low"])
    last_close = float(last["close"])
    if (prev_high >= level.price * (1 + sweep_eps) or last_high >= level.price * (1 + sweep_eps)) and last_close < level.price:
        return Trigger("short", "SWEEP", level, 0.0, {"last_close": last_close})
    if (prev_low <= level.price * (1 - sweep_eps) or last_low <= level.price * (1 - sweep_eps)) and last_close > level.price:
        return Trigger("long", "SWEEP", level, 0.0, {"last_close": last_close})
    return None


def detect_breakout_retest(df: pd.DataFrame, level: Level, hold_eps: float) -> Optional[Trigger]:
    if df.empty or len(df) < 4:
        return None
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    c1_close = float(c1["close"])
    c2_low = float(c2["low"])
    c2_high = float(c2["high"])
    c3_close = float(c3["close"])
    hold_tol = hold_eps * 0.5
    if c1_close > level.price and c2_low <= level.price and c3_close > level.price * (1 + hold_tol):
        return Trigger("long", "RETEST", level, 0.0, {"c1": c1_close, "c3": c3_close})
    if c1_close < level.price and c2_high >= level.price and c3_close < level.price * (1 - hold_tol):
        return Trigger("short", "RETEST", level, 0.0, {"c1": c1_close, "c3": c3_close})
    return None


def dist_to_level_ok(price: float, level: Level, max_dist: float) -> Tuple[bool, str]:
    if level.price <= 0:
        return False, "dist"
    dist = abs(price - level.price) / level.price
    if dist > max_dist:
        return False, "dist"
    return True, ""


def in_lvn_gap(last_price: float, vp: Optional[VPLevels]) -> Tuple[bool, str]:
    if vp is None:
        return True, ""
    for low, high in vp.lvn_gaps:
        if low <= last_price <= high:
            return False, "lvn_gap"
    return True, ""


def expansion_bar(df: pd.DataFrame, atr_len: int, expansion_mult: float) -> Tuple[bool, str]:
    if df.empty or len(df) < atr_len + 2:
        return True, ""
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < atr_len:
        return True, ""
    atr_val = float(sum(trs[-atr_len:]) / atr_len)
    last_range = highs[-1] - lows[-1]
    if atr_val > 0 and last_range > atr_val * expansion_mult:
        return False, "expansion"
    return True, ""


def cooldown_ok(now_ts: float, last_ts: Optional[float], cooldown_min: int) -> Tuple[bool, str]:
    if not last_ts:
        return True, ""
    if (now_ts - last_ts) < cooldown_min * 60:
        return False, "cooldown"
    return True, ""


def regime_ok(regime: str, side: str, allow_countertrend: bool, range_short_allowed: bool) -> Tuple[bool, str]:
    if allow_countertrend:
        return True, ""
    if regime == "bull" and side == "SHORT":
        return False, "regime"
    if regime == "bear" and side == "LONG":
        return False, "regime"
    if regime == "range" and side == "SHORT" and not range_short_allowed:
        return False, "regime"
    return True, ""


def _strength_components(trigger: Trigger, last_price: float, regime: str, cfg: SwaggyConfig) -> Dict[str, float]:
    wick_score = float(trigger.evidence.get("wick_ratio", 0.0))
    reclaim_score = 1.0 if trigger.kind in ("RECLAIM", "RETEST") else 0.0
    vol_ratio = 0.0
    if "vol" in trigger.evidence and "vol_sma" in trigger.evidence:
        vol = float(trigger.evidence["vol"])
        vol_sma = float(trigger.evidence["vol_sma"])
        if vol_sma > 0:
            vol_ratio = vol / vol_sma
    if cfg.vol_mult > 1:
        volume_score = min(1.0, max(0.0, (vol_ratio - 1.0) / (cfg.vol_mult - 1.0)))
    else:
        volume_score = min(1.0, vol_ratio)
    dist_pct = abs(last_price - trigger.level.price) / trigger.level.price if trigger.level.price else 1.0
    distance_score = max(0.0, 1.0 - (dist_pct / cfg.max_dist)) if cfg.max_dist > 0 else 0.0
    if regime == "range":
        regime_score = 0.5
    elif regime == "bull" and trigger.side == "long":
        regime_score = 1.0
    elif regime == "bear" and trigger.side == "short":
        regime_score = 1.0
    else:
        regime_score = 0.0
    return {
        "wick_score": float(wick_score),
        "reclaim_score": float(reclaim_score),
        "volume_score": float(volume_score),
        "distance_score": float(distance_score),
        "regime_score": float(regime_score),
    }


def _compute_strength(trigger: Trigger, last_price: float, regime: str, cfg: SwaggyConfig) -> float:
    comps = _strength_components(trigger, last_price, regime, cfg)
    strength = (
        0.25 * comps["wick_score"]
        + 0.25 * comps["reclaim_score"]
        + 0.20 * comps["volume_score"]
        + 0.20 * comps["distance_score"]
        + 0.10 * comps["regime_score"]
    )
    return max(0.0, min(1.0, strength))


class SwaggySignalEngine:
    def __init__(self, config: Optional[SwaggyConfig] = None):
        self.config = config or SwaggyConfig()
        self._state: Dict[str, Dict[str, Any]] = {}

    def evaluate_symbol(
        self,
        sym: str,
        candles_4h: pd.DataFrame,
        candles_1h: pd.DataFrame,
        candles_15m: pd.DataFrame,
        candles_5m: pd.DataFrame,
        candles_3m: pd.DataFrame,
        candles_1d: pd.DataFrame,
        now_ts: float,
    ) -> SwaggySignal:
        cfg = self.config
        state = self._state.setdefault(sym, {})
        events: List[Dict[str, Any]] = []
        if candles_5m.empty or len(candles_5m) < 31:
            return SwaggySignal(False, None, 0.0, ["NO_DATA"], "-", None)
        last = candles_5m.iloc[-1]
        last_price = float(last["close"])
        tick_size = cfg.tick_size if cfg.tick_size > 0 else max(last_price * 0.0001, 1e-8)

        regime_df = candles_4h if not candles_4h.empty else candles_1h
        regime_state = detect_regime(regime_df, lookback=cfg.regime_lookback, k=cfg.regime_fractal_k)
        if state.get("phase") == "COOLDOWN":
            until_ts = float(state.get("cooldown_until", 0.0) or 0.0)
            if now_ts < until_ts:
                return SwaggySignal(False, None, 0.0, ["COOLDOWN"], "-", None, debug={"regime": regime_state.regime})
            state["phase"] = "WAIT_TOUCH"

        vp = build_vp_levels(
            candles_1h,
            cfg.bin_size_pct,
            cfg.bin_size_abs if cfg.bin_size_abs > 0 else cfg.tick_size,
            cfg.value_area_pct,
            cfg.min_zone_width_pct,
            cfg.hvn_threshold_ratio,
            cfg.lvn_threshold_ratio,
            cfg.lvn_ratio,
        )
        levels = build_levels(
            candles_1h,
            vp,
            last_price,
            cfg.level_cluster_pct,
            tick_size,
            df_mtf=candles_15m,
            swing_lookback_1h=cfg.swing_lookback_1h,
            swing_lookback_mtf=cfg.swing_lookback_mtf,
            max_dist_pct=cfg.level_max_dist_pct,
            level_cap=cfg.level_cap,
        )
        if not levels:
            return SwaggySignal(False, None, 0.0, ["NO_LEVEL"], "-", None, debug={"regime": regime_state.regime})

        target_level = nearest_level(levels, last_price)
        if not target_level:
            return SwaggySignal(False, None, 0.0, ["NO_LEVEL"], "-", None, debug={"regime": regime_state.regime})

        level_type = _level_type(target_level.kind)
        level_age_bars = _level_age_bars(candles_1h, target_level.ts)

        phase = state.get("phase") or "WAIT_TOUCH"
        if phase == "CHASE":
            chase_ok, _ = _overextension_ok(candles_5m, state.get("chase_side"), cfg)
            if not chase_ok:
                return SwaggySignal(False, None, 0.0, ["CHASE"], "-", None, debug={"regime": regime_state.regime})
            state["phase"] = "WAIT_TRIGGER"
        if phase == "WAIT_TOUCH":
            touch_hit, touch_meta = _is_level_touch(candles_5m, target_level, cfg, regime_state.regime)
            if not touch_hit:
                return SwaggySignal(False, None, 0.0, ["NO_TOUCH"], "-", None, debug={"regime": regime_state.regime})
            state["phase"] = "WAIT_TRIGGER"
            touch_key = _level_key(target_level, tick_size)
            if state.get("touch_key") == touch_key:
                state["touch_count"] = int(state.get("touch_count", 0) or 0) + 1
            else:
                state["touch_count"] = 1
            state["touch_key"] = touch_key
            state["touch_level"] = {"price": target_level.price, "kind": target_level.kind, "ts": target_level.ts}
            state["touch_ts"] = now_ts
            _, dist_long = _overextension_ok(candles_5m, "LONG", cfg)
            _, dist_short = _overextension_ok(candles_5m, "SHORT", cfg)
            dist_long_abs = abs(dist_long) if isinstance(dist_long, (int, float)) else None
            dist_short_abs = abs(dist_short) if isinstance(dist_short, (int, float)) else None
            state["touch_overext"] = {"LONG": dist_long_abs, "SHORT": dist_short_abs}
            state["touch_meta"] = touch_meta
            dist_pct = float(touch_meta.get("min_dist_pct_to_level") or 0.0)
            level_score = max(0.0, 1.0 - (dist_pct / cfg.max_dist)) if cfg.max_dist > 0 else None
            touch_atr_mult = None
            try:
                atr_val = float(touch_meta.get("atr") or 0.0)
                tol = float(touch_meta.get("touch_tol") or 0.0)
                if atr_val > 0:
                    touch_atr_mult = tol / atr_val
            except Exception:
                touch_atr_mult = None
            state["touch_pct"] = touch_meta.get("min_dist_pct_to_level")
            state["touch_atr_mult"] = touch_atr_mult
            overext_touch = dist_long_abs if dist_long_abs is not None else dist_short_abs
            events.append(
                {
                    "event": "SWAGGY_TOUCH",
                    "level_type": level_type,
                    "level_price": target_level.price,
                    "level_score": level_score,
                    "touch_count": state.get("touch_count"),
                    "level_age_bars": level_age_bars,
                    "touch_pct": state.get("touch_pct"),
                    "touch_atr_mult": touch_atr_mult,
                    "touch_pass": 1,
                    "touch_fail_reason": "",
                    "overext_dist_at_touch": overext_touch,
                    "range_id": touch_key,
                }
            )

        touch_meta = state.get("touch_level") or {}
        touch_price = touch_meta.get("price")
        touch_level = target_level if touch_price is None else target_level

        reclaim = detect_reclaim(candles_5m, touch_level, cfg.touch_eps)
        rejection = detect_rejection_wick(candles_5m, touch_level, cfg.wick_ratio, cfg.vol_mult)
        sweep = detect_sweep_and_return(candles_5m, touch_level, cfg.sweep_eps)
        retest = detect_breakout_retest(candles_5m, touch_level, cfg.hold_eps)
        candidates = [c for c in [reclaim, rejection, sweep, retest] if c is not None]
        if not candidates:
            return SwaggySignal(False, None, 0.0, ["NO_TRIGGER"], "-", None, debug={"regime": regime_state.regime})
        trigger_combo = "+".join(sorted({c.kind.upper() for c in candidates}))

        for trig in candidates:
            trig.strength = _compute_strength(trig, last_price, regime_state.regime, cfg)

        best = max(candidates, key=lambda x: x.strength)
        strength_comps = _strength_components(best, last_price, regime_state.regime, cfg)
        side = best.side.upper()
        strength = float(best.strength)
        trigger_parts = {t.kind.upper(): float(t.strength or 0.0) for t in candidates}
        trigger_strength_best = float(best.strength or 0.0)
        trigger_strength_min = min(trigger_parts.values()) if trigger_parts else 0.0
        trigger_strength_avg = (
            sum(trigger_parts.values()) / len(trigger_parts) if trigger_parts else 0.0
        )

        entry_min = _entry_min_for_regime(regime_state.regime, cfg)
        min_by_kind = _min_strength_for_trigger(best.kind, cfg)
        trigger_threshold_used = max(cfg.weak_cut, entry_min, min_by_kind)
        trigger_strength_used = trigger_strength_min if cfg.use_trigger_min else trigger_strength_best
        if best.kind != "SWEEP" and trigger_strength_used < trigger_threshold_used:
            return SwaggySignal(
                False,
                side,
                strength,
                ["WEAK_SIGNAL"],
                best.kind,
                None,
                debug={"regime": regime_state.regime, "trigger_threshold_used": trigger_threshold_used},
            )
        events.append(
            {
                "event": "SWAGGY_TRIGGER",
                "trigger_combo": "+".join(sorted({t.kind.upper() for t in candidates})),
                "trigger_strength_best": trigger_strength_best,
                "trigger_strength_min": trigger_strength_min,
                "trigger_strength_avg": trigger_strength_avg,
                "trigger_parts": trigger_parts,
                "strength_total": strength,
                "strength_min_req": trigger_threshold_used,
                "trigger_threshold_used": trigger_threshold_used,
                "trigger_strength_used": trigger_strength_used,
            }
        )

        allow_countertrend = cfg.allow_countertrend
        ok_regime, _ = regime_ok(regime_state.regime, side, allow_countertrend, cfg.range_short_allowed)
        if not ok_regime:
            return SwaggySignal(False, side, strength, ["REGIME_BLOCK"], best.kind, None, debug={"regime": regime_state.regime})
        side_key = side.upper()
        last_ts_key = f"last_signal_ts_{side_key}"
        last_ts = state.get(last_ts_key)
        if last_ts is None:
            last_ts = state.get("last_signal_ts")
        ok_cooldown, _ = cooldown_ok(now_ts, last_ts, cfg.cooldown_min)
        if not ok_cooldown:
            return SwaggySignal(False, side, strength, ["COOLDOWN"], best.kind, None, debug={"regime": regime_state.regime})
        ok_gap, _ = in_lvn_gap(last_price, vp)
        if not ok_gap:
            return SwaggySignal(False, side, strength, ["LVN_GAP"], best.kind, None, debug={"regime": regime_state.regime})
        ok_exp, _ = expansion_bar(candles_5m, cfg.touch_atr_len, cfg.expansion_atr_mult)
        if not ok_exp:
            return SwaggySignal(False, side, strength, ["EXPANSION_BAR"], best.kind, None, debug={"regime": regime_state.regime})
        ok_dist, _ = dist_to_level_ok(last_price, best.level, cfg.max_dist)
        if not ok_dist:
            return SwaggySignal(False, side, strength, ["DIST_FAIL"], best.kind, None, debug={"regime": regime_state.regime})

        d1_dist_atr = 0.0
        if not candles_1d.empty and len(candles_1d) >= max(cfg.d1_ema_len, cfg.d1_atr_len) + 2:
            ema7_series = ema(candles_1d["close"], cfg.d1_ema_len)
            d1_ema = float(ema7_series.iloc[-1]) if not ema7_series.empty else 0.0
            d1_atr = atr(candles_1d, cfg.d1_atr_len)
            if d1_atr > 0:
                d1_dist_atr = abs(last_price - d1_ema) / d1_atr
        if d1_dist_atr > cfg.d1_overext_atr_mult:
            return SwaggySignal(
                False,
                side,
                strength,
                ["D1_EMA7_DIST"],
                best.kind,
                None,
                debug={"regime": regime_state.regime, "d1_dist_atr": d1_dist_atr},
            )

        chase_ok, chase_dist = _overextension_ok(candles_5m, side, cfg)
        chase_dist_abs = abs(chase_dist) if isinstance(chase_dist, (int, float)) else None
        if not chase_ok:
            state["phase"] = "CHASE"
            state["chase_side"] = side
            events.append(
                {
                    "event": "SWAGGY_CONFIRM",
                    "confirm_pass": 0,
                    "confirm_fail_reason": "OVEREXT_CHASE",
                    "confirm_metrics": {},
                    "overext_blocked": 1,
                    "overext_state": "CHASE",
                    "overext_dist_at_entry": chase_dist_abs,
                }
            )
            return SwaggySignal(
                False,
                side,
                strength,
                ["CHASE"],
                best.kind,
                None,
                debug={"regime": regime_state.regime, "chase_dist": chase_dist, "events": events},
            )

        if candles_3m.empty or len(candles_3m) < 9:
            events.append(
                {
                    "event": "SWAGGY_CONFIRM",
                    "confirm_pass": 0,
                    "confirm_fail_reason": "EMA7_WARMUP",
                    "confirm_metrics": {},
                    "overext_blocked": 0,
                    "overext_state": "OK",
                }
            )
            return SwaggySignal(
                False,
                side,
                strength,
                ["EMA7_WARMUP"],
                best.kind,
                None,
                debug={"regime": regime_state.regime, "confirm_pass": 0, "confirm_fail": "EMA7_WARMUP", "events": events},
            )
        ema7_series = ema(candles_3m["close"], 7)
        if not ema7_series.empty:
            ema7_val = float(ema7_series.iloc[-1])
            last3 = candles_3m.iloc[-1]
            hi3 = float(last3["high"])
            lo3 = float(last3["low"])
            op3 = float(last3["open"])
            cl3 = float(last3["close"])
            rng3 = max(1e-9, hi3 - lo3)
            body_ratio = abs(cl3 - op3) / rng3
            if side == "LONG":
                opp_wick = (hi3 - max(op3, cl3)) / rng3
                close_pos = (cl3 - lo3) / rng3
            else:
                opp_wick = (min(op3, cl3) - lo3) / rng3
                close_pos = (hi3 - cl3) / rng3
            confirm_metrics = {
                "body_ratio": body_ratio,
                "opp_wick_ratio": opp_wick,
                "close_pos": close_pos,
                "ema7": ema7_val,
            }
            if side == "LONG" and last_price > ema7_val:
                events.append(
                    {
                        "event": "SWAGGY_CONFIRM",
                        "confirm_pass": 0,
                        "confirm_fail_reason": "EMA7_DIR",
                        "confirm_metrics": confirm_metrics,
                        "overext_blocked": 0,
                        "overext_state": "OK",
                    }
                )
                return SwaggySignal(
                    False,
                    side,
                    strength,
                    ["EMA7_DIR"],
                    best.kind,
                    None,
                    debug={
                        "regime": regime_state.regime,
                        "ema7": ema7_val,
                        "confirm_pass": 0,
                        "confirm_fail": "EMA7_DIR",
                        "confirm_metrics": confirm_metrics,
                        "events": events,
                    },
                )
            if side == "SHORT" and last_price < ema7_val:
                events.append(
                    {
                        "event": "SWAGGY_CONFIRM",
                        "confirm_pass": 0,
                        "confirm_fail_reason": "EMA7_DIR",
                        "confirm_metrics": confirm_metrics,
                        "overext_blocked": 0,
                        "overext_state": "OK",
                    }
                )
                return SwaggySignal(
                    False,
                    side,
                    strength,
                    ["EMA7_DIR"],
                    best.kind,
                    None,
                    debug={
                        "regime": regime_state.regime,
                        "ema7": ema7_val,
                        "confirm_pass": 0,
                        "confirm_fail": "EMA7_DIR",
                        "confirm_metrics": confirm_metrics,
                        "events": events,
                    },
                )
            events.append(
                {
                    "event": "SWAGGY_CONFIRM",
                    "confirm_pass": 1,
                    "confirm_fail_reason": "",
                    "confirm_metrics": confirm_metrics,
                    "overext_blocked": 0,
                    "overext_state": "OK",
                }
            )
            state["confirm_metrics"] = confirm_metrics

        state[last_ts_key] = now_ts
        state["last_signal_ts"] = now_ts
        state["phase"] = "COOLDOWN"
        state["cooldown_until"] = now_ts + cfg.cooldown_min * 60
        entry_px = last_price
        touch_overext = state.get("touch_overext") or {}
        level_ts = (state.get("touch_level") or {}).get("ts")
        level_age = (now_ts - float(level_ts)) if isinstance(level_ts, (int, float)) else None
        atr14_ltf = atr(candles_5m, 14)
        atr14_htf = atr(candles_1h, 14)
        ema20_ltf = ema(candles_5m["close"], 20)
        ema20_htf = ema(candles_1h["close"], 20)
        return SwaggySignal(
            True,
            side,
            strength,
            ["ENTRY_READY"],
            best.kind,
            entry_px,
            debug={
                "regime": regime_state.regime,
                "level_score": strength_comps.get("distance_score"),
                "touch_count": state.get("touch_count"),
                "touch_key": state.get("touch_key"),
                "level_age_sec": level_age,
                "trigger_combo": trigger_combo,
                "trigger_parts": trigger_parts,
                "trigger_strength_best": trigger_strength_best,
                "trigger_strength_min": trigger_strength_min,
                "trigger_strength_avg": trigger_strength_avg,
                "trigger_strength_used": trigger_strength_used,
                "strength_total": strength,
                "strength_min_req": trigger_threshold_used,
                "trigger_threshold_used": trigger_threshold_used,
                "use_trigger_min": 1 if cfg.use_trigger_min else 0,
                "confirm_pass": 1,
                "confirm_fail": "",
                "confirm_metrics": state.get("confirm_metrics") or {},
                "overext_dist_at_touch": touch_overext.get(side),
                "overext_dist_at_entry": chase_dist_abs,
                "overext_ema_len": cfg.overext_ema_len,
                "overext_atr_mult": cfg.overext_atr_mult,
                "level_type": level_type,
                "level_price": target_level.price,
                "level_age_bars": level_age_bars,
                "touch_pct": state.get("touch_pct"),
                "touch_atr_mult": state.get("touch_atr_mult"),
                "touch_pass": 1,
                "touch_fail_reason": "",
                "atr14_ltf": atr14_ltf,
                "atr14_htf": atr14_htf,
                "ema20_ltf": float(ema20_ltf.iloc[-1]) if not ema20_ltf.empty else None,
                "ema20_htf": float(ema20_htf.iloc[-1]) if not ema20_htf.empty else None,
                "events": events,
            },
        )


def _min_strength_for_trigger(kind: str, cfg: SwaggyConfig) -> float:
    if kind == "SWEEP":
        return cfg.min_strength_sweep
    if kind == "RECLAIM":
        return cfg.min_strength_reclaim
    if kind == "REJECTION":
        return cfg.min_strength_rejection
    if kind == "RETEST":
        return cfg.min_strength_retest
    return cfg.entry_min


def _entry_min_for_regime(regime: str, cfg: SwaggyConfig) -> float:
    if regime == "range":
        return cfg.entry_min_range
    if regime == "bear":
        return cfg.entry_min_bear
    if regime == "bull":
        return cfg.entry_min_bull
    return cfg.entry_min


def _overextension_ok(df: pd.DataFrame, side: Optional[str], cfg: SwaggyConfig) -> tuple[bool, float]:
    if cfg.overext_atr_mult <= 0:
        return True, 0.0
    if df.empty or len(df) < cfg.overext_ema_len + 2:
        return True, 0.0
    last_price = float(df["close"].iloc[-1])
    ema_series = ema(df["close"], cfg.overext_ema_len)
    if ema_series.empty:
        return True, 0.0
    ema_val = float(ema_series.iloc[-1])
    atr_val = atr(df, cfg.touch_atr_len)
    if atr_val <= 0:
        return True, 0.0
    side = (side or "").upper()
    if side == "SHORT":
        dist = (ema_val - last_price) / atr_val
    else:
        dist = (last_price - ema_val) / atr_val
    return (dist <= cfg.overext_atr_mult), float(dist)


def _is_level_touch(
    df: pd.DataFrame,
    level: Level,
    cfg: SwaggyConfig,
    regime: str,
) -> tuple[bool, Dict[str, Any]]:
    if df.empty or level.price <= 0:
        return False, {}
    last = df.iloc[-1]
    close = float(last["close"])
    high = float(last["high"])
    low = float(last["low"])
    band_low = level.low if level.low is not None else level.price
    band_high = level.high if level.high is not None else level.price
    atr_val = atr(df, cfg.touch_atr_len)
    touch_pct_min = _touch_pct_min_for_regime(regime, cfg)
    extra_tol = max(atr_val * cfg.touch_eps_atr_mult, level.price * cfg.touch_eps_pct)
    tol = max(level.price * touch_pct_min, atr_val * cfg.touch_atr_mult, extra_tol)
    if level.low is None and level.high is None:
        band_low = level.price - tol
        band_high = level.price + tol
    near_wick_dist = min(abs(high - level.price), abs(low - level.price))
    wick_touch = near_wick_dist <= tol
    close_touch = abs(close - level.price) <= tol
    dist_pct_close = abs(close - level.price) / level.price if level.price else 0.0
    dist_pct_wick_min = min(abs(close - level.price), abs(high - level.price), abs(low - level.price)) / level.price if level.price else 0.0
    meta = {
        "level_price": level.price,
        "band_low": band_low,
        "band_high": band_high,
        "atr": atr_val,
        "min_dist_pct_to_level": dist_pct_wick_min,
        "dist_pct_close": dist_pct_close,
        "touch_tol": tol,
    }
    return (wick_touch or close_touch), meta


def _touch_pct_min_for_regime(regime: str, cfg: SwaggyConfig) -> float:
    if regime == "bull":
        return cfg.touch_pct_min_bull
    if regime == "bear":
        return cfg.touch_pct_min_bear
    if regime == "range":
        return cfg.touch_pct_min_range
    if regime == "chaos":
        return cfg.touch_pct_min_chaos
    return cfg.touch_pct_min
