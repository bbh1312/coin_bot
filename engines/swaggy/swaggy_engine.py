from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from collections import deque

import cycle_cache
from engines.base import BaseEngine, EngineContext
from engines.swaggy.filters import (
    cooldown_ok,
    dist_to_level_ok,
    expansion_bar,
    in_lvn_gap,
    regime_ok,
)
from engines.swaggy.htf_structure import RegimeState, detect_regime
from engines.swaggy.levels import Level, build_levels, nearest_level, _round_levels
from engines.swaggy.manage import manage_position
from engines.swaggy.risk import RiskPlan, build_risk_plan
from engines.swaggy.triggers import (
    Trigger,
    detect_breakout_retest,
    detect_reclaim,
    detect_rejection_wick,
    detect_sweep_and_return,
)
from engines.swaggy.vp_profile import VPLevels, build_vp_levels
from engines.universe import build_universe_from_tickers


@dataclass
class SwaggyConfig:
    name: str = "swaggy"
    tf_htf: str = "1h"
    tf_htf2: str = "4h"
    tf_mtf: str = "15m"
    tf_ltf: str = "5m"
    vp_lookback_1h: int = 240
    ltf_limit: int = 400
    level_cluster_pct: float = 0.0022
    touch_eps: float = 0.0015
    touch_pct_min: float = 0.0002
    touch_pct_min_bull: float = 0.0010
    touch_pct_min_bear: float = 0.0012
    touch_pct_min_range: float = 0.0016
    touch_pct_min_chaos: float = 0.0016
    universe_blacklist: tuple = ("BTC/USDT:USDT", "ETH/USDT:USDT")
    touch_atr_mult: float = 0.15
    touch_atr_len: int = 14
    touch_eps_atr_mult: float = 0.05
    touch_eps_pct: float = 0.0005
    max_dist: float = 0.005
    sweep_eps: float = 0.0010
    hold_eps: float = 0.0010
    wick_ratio: float = 0.45
    vol_mult: float = 1.2
    invalid_eps: float = 0.002
    expansion_atr_mult: float = 1.5
    time_stop_bars_5m: int = 12
    cooldown_min: int = 20
    allow_countertrend: bool = False
    range_short_allowed: bool = False
    tp1_partial: float = 0.5
    tp1_fallback_r: float = 1.5
    tick_size: float = 0.001
    regime_lookback: int = 30
    regime_fractal_k: int = 2
    min_quote_volume_usdt: float = 30_000_000.0
    universe_top_n: int = 40
    bin_size_pct: float = 0.1
    bin_size_abs: float = 0.0
    value_area_pct: float = 0.70
    min_zone_width_pct: float = 0.15
    hvn_threshold_ratio: float = 0.55
    lvn_threshold_ratio: float = 0.18
    lvn_ratio: float = 0.35
    min_strength_sweep: float = 0.55
    min_strength_reclaim: float = 0.40
    min_strength_rejection: float = 0.50
    min_strength_retest: float = 0.45
    min_strength_sweep_confirm: float = 0.30
    confirm_window: int = 3
    weak_cut: float = 0.32
    entry_min: float = 0.40
    entry_min_bull: float = 0.40
    entry_min_range: float = 0.45
    entry_min_bear: float = 0.40
    strong_min: float = 0.55
    sweep_confirm_gate: float = 0.42
    sweep_confirm_min: float = 0.38
    range_confirm_bars: int = 3
    short_bull_min_strength: float = 0.55
    short_bull_wick_ratio: float = 0.70
    short_bull_vol_mult: float = 1.3
    max_dist_reclaim: float = 0.0025
    max_dist_retest: float = 0.0020
    short_max_dist_reclaim: float = 0.0025
    short_max_dist_retest: float = 0.0020
    short_max_dist_rejection: float = 0.0025
    short_max_dist_sweep: float = 0.0015
    reclaim_ext_atr_mult: float = 0.60
    symbol_entry_cooldown_min: int = 30
    swing_lookback_1h: int = 60
    swing_lookback_mtf: int = 32
    level_recent_secs: int = 14400
    level_max_dist_pct: float = 0.06
    level_cap: int = 25
    no_trigger_log_sample_pct: float = 0.10
    sweep_log_sample_pct: float = 0.10
    # frequency control
    symbol_cooldown_bars: int = 3
    zone_reentry_limit: int = 1
    post_sl_cooldown_bars: int = 5
    # TP / SL
    sl_mode: str = "FIXED_PCT"  # FIXED_PCT | ZONE_BASED | INVALID_EPS
    sl_buffer_atr: float = 0.25
    sl_pct: float = 0.02
    tp_pct: float = 0.02


@dataclass
class SwaggyDecision:
    sym: str
    side: Optional[str]
    entry_ready: int
    entry_px: Optional[float]
    sl_px: Optional[float]
    tp1_px: Optional[float]
    tp2_px: Optional[float]
    gate_stage: str = ""
    gate_fail: str = ""
    gate_fail_detail: str = ""
    reason_codes: List[str] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, int] = field(default_factory=dict)
    evidence: Dict[str, Any] = field(default_factory=dict)


class SwaggyEngine(BaseEngine):
    name = "swaggy"

    def __init__(self, config: Optional[SwaggyConfig] = None):
        self.config = config or SwaggyConfig()
        self._state: Dict[str, Dict[str, Any]] = {}
        self._trigger_counts: Dict[str, int] = {}
        self._filter_counts: Dict[str, int] = {}
        self._gate_fail_counts: Dict[str, int] = {}
        self._gate_stage_counts: Dict[str, int] = {}
        self._ready_fail_counts: Dict[str, int] = {}
        self._no_level_samples: List[Dict[str, Any]] = []
        self._stats: Dict[str, int] = {}

    def begin_cycle(self) -> None:
        self._trigger_counts = {}
        self._filter_counts = {}
        self._gate_fail_counts = {}
        self._gate_stage_counts = {}
        self._ready_fail_counts = {}
        self._no_level_samples = []
        self._stats = {
            "touch": 0,
            "trigger": 0,
            "entry_ready": 0,
            "reject": 0,
            "reclaim": 0,
            "bull_short_candidate": 0,
            "bull_short_blocked_early": 0,
        }

    def get_trigger_counts(self) -> Dict[str, int]:
        return dict(self._trigger_counts)

    def get_filter_counts(self) -> Dict[str, int]:
        return dict(self._filter_counts)

    def get_gate_fail_counts(self) -> Dict[str, int]:
        return dict(self._gate_fail_counts)

    def get_gate_stage_counts(self) -> Dict[str, int]:
        return dict(self._gate_stage_counts)

    def get_ready_fail_counts(self) -> Dict[str, int]:
        return dict(self._ready_fail_counts)

    def get_no_level_samples(self) -> List[Dict[str, Any]]:
        return list(self._no_level_samples)

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def build_universe(self, ctx: EngineContext) -> List[str]:
        shared = ctx.state.get("_universe")
        if isinstance(shared, list) and shared:
            return [s for s in shared if s not in self.config.universe_blacklist]
        tickers = ctx.state.get("_tickers")
        if not isinstance(tickers, dict):
            return []
        symbols = ctx.state.get("_symbols")
        sym_iter = symbols if isinstance(symbols, list) else list(tickers.keys())
        universe = build_universe_from_tickers(
            tickers,
            symbols=sym_iter,
            min_quote_volume_usdt=self.config.min_quote_volume_usdt,
            top_n=self.config.universe_top_n,
        )
        return [s for s in universe if s not in self.config.universe_blacklist]

    def on_tick(self, ctx: EngineContext, symbol: str) -> Optional[SwaggyDecision]:
        cfg = self.config
        atlas_gate = {}
        if ctx and isinstance(ctx.state, dict):
            atlas_gate = ctx.state.get("_atlas_swaggy_gate") or {}
        if isinstance(atlas_gate, dict) and atlas_gate:
            if not atlas_gate.get("trade_allowed"):
                self._filter_counts["atlas_block"] = self._filter_counts.get("atlas_block", 0) + 1
                decision = SwaggyDecision(
                    sym=symbol,
                    side=None,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["ATLAS_BLOCK"],
                    debug={"regime": atlas_gate.get("regime"), "reason": "atlas_block", "atlas": atlas_gate},
                    evidence={"atlas": atlas_gate},
                )
                self._annotate_gate(decision)
                return decision
        df_5m = cycle_cache.get_df(symbol, cfg.tf_ltf, cfg.ltf_limit)
        if df_5m.empty or len(df_5m) < 31:
            return None
        df_5m = df_5m.iloc[:-1]
        df_15m = cycle_cache.get_df(symbol, cfg.tf_mtf, 200)
        if not df_15m.empty and len(df_15m) > 1:
            df_15m = df_15m.iloc[:-1]
        df_1h = cycle_cache.get_df(symbol, cfg.tf_htf, cfg.vp_lookback_1h)
        if not df_1h.empty and len(df_1h) > 1:
            df_1h = df_1h.iloc[:-1]
        df_4h = cycle_cache.get_df(symbol, cfg.tf_htf2, 200)
        if not df_4h.empty and len(df_4h) > 1:
            df_4h = df_4h.iloc[:-1]
        position_state = {}
        if ctx and isinstance(ctx.state, dict):
            position_state = ctx.state.get(symbol, {}) or {}
        decision = self.evaluate_symbol(
            sym=symbol,
            candles_4h=df_4h,
            candles_1h=df_1h,
            candles_15m=df_15m,
            candles_5m=df_5m,
            position_state=position_state,
            engine_config=cfg,
            now_ts=ctx.now_ts,
        )
        if isinstance(atlas_gate, dict) and atlas_gate and isinstance(decision, SwaggyDecision):
            allow_long = bool(atlas_gate.get("allow_long"))
            allow_short = bool(atlas_gate.get("allow_short"))
            long_requires_exception = bool(atlas_gate.get("long_requires_exception"))
            side = (decision.side or "").upper()
            if side == "LONG" and not allow_long and not long_requires_exception:
                self._filter_counts["atlas_side_block"] = self._filter_counts.get("atlas_side_block", 0) + 1
                if decision.entry_ready:
                    self._stats["entry_ready"] = max(0, int(self._stats.get("entry_ready", 0)) - 1)
                decision.entry_ready = 0
                decision.reason_codes = ["ATLAS_SIDE_BLOCK"]
                decision.debug = {**(decision.debug or {}), "reason": "atlas_side_block", "atlas": atlas_gate}
                decision.evidence = {**(decision.evidence or {}), "atlas": atlas_gate}
            elif side == "SHORT" and not allow_short:
                self._filter_counts["atlas_side_block"] = self._filter_counts.get("atlas_side_block", 0) + 1
                if decision.entry_ready:
                    self._stats["entry_ready"] = max(0, int(self._stats.get("entry_ready", 0)) - 1)
                decision.entry_ready = 0
                decision.reason_codes = ["ATLAS_SIDE_BLOCK"]
                decision.debug = {**(decision.debug or {}), "reason": "atlas_side_block", "atlas": atlas_gate}
                decision.evidence = {**(decision.evidence or {}), "atlas": atlas_gate}
        if isinstance(decision, SwaggyDecision):
            self._annotate_gate(decision)
        return decision

    def evaluate_symbol(
        self,
        sym: str,
        candles_4h: pd.DataFrame,
        candles_1h: pd.DataFrame,
        candles_15m: pd.DataFrame,
        candles_5m: pd.DataFrame,
        position_state: Dict[str, Any],
        engine_config: SwaggyConfig,
        now_ts: float,
    ) -> SwaggyDecision:
        cfg = engine_config
        state = self._state.setdefault(sym, position_state or {})
        last = candles_5m.iloc[-1]
        last_price = float(last["close"])
        tick_size = cfg.tick_size
        tick_size_fallback = False
        if not isinstance(tick_size, (int, float)) or tick_size <= 0:
            tick_size = max(last_price * 0.0001, 1e-8)
            tick_size_fallback = True
        bar_idx = len(candles_5m) - 1
        if position_state:
            state.update(position_state)
        regime_df = candles_4h if not candles_4h.empty else candles_1h
        regime_state = detect_regime(regime_df, lookback=cfg.regime_lookback, k=cfg.regime_fractal_k)
        regime_state = _apply_range_hysteresis(state, regime_state, cfg.range_confirm_bars)
        if state.get("in_pos"):
            state["phase"] = "IN_POSITION"
        phase = state.get("phase") or "WAIT_TOUCH"
        if phase == "COOLDOWN":
            until_ts = float(state.get("cooldown_until", 0.0) or 0.0)
            if now_ts < until_ts:
                remaining = max(0.0, until_ts - now_ts)
                evidence = {
                    "cooldown_key": "symbol",
                    "cooldown_until": until_ts,
                    "cooldown_remaining_sec": remaining,
                    "last_entry_ts": state.get("last_entry_ts"),
                }
                _inject_regime_evidence(evidence, regime_state, cfg)
                return SwaggyDecision(
                    sym=sym,
                    side=None,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["COOLDOWN"],
                    debug={"regime": regime_state.regime, "reason": "cooldown"},
                    evidence=evidence,
                )
            state["phase"] = "WAIT_TOUCH"
            phase = "WAIT_TOUCH"
        bin_abs = cfg.bin_size_abs if cfg.bin_size_abs > 0 else cfg.tick_size
        vp = build_vp_levels(
            candles_1h,
            cfg.bin_size_pct,
            bin_abs,
            cfg.value_area_pct,
            cfg.min_zone_width_pct,
            cfg.hvn_threshold_ratio,
            cfg.lvn_threshold_ratio,
            cfg.lvn_ratio,
        )
        levels, level_meta = build_levels(
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
            return_meta=True,
        )
        level_attempts = [level_meta]
        if not levels:
            for dist_cap in (0.12, 0.20):
                levels, retry_meta = build_levels(
                    candles_1h,
                    vp,
                    last_price,
                    cfg.level_cluster_pct,
                    tick_size,
                    df_mtf=candles_15m,
                    swing_lookback_1h=cfg.swing_lookback_1h,
                    swing_lookback_mtf=cfg.swing_lookback_mtf,
                    max_dist_pct=dist_cap,
                    level_cap=cfg.level_cap,
                    return_meta=True,
                )
                level_attempts.append(retry_meta)
                if levels:
                    break
        if not levels:
            raw_levels = level_meta.get("raw_levels", [])
            if raw_levels:
                levels = sorted(raw_levels, key=lambda lv: abs(lv.price - last_price))[:12]
        if not levels:
            levels = _round_levels(last_price, tick_size)
        if not levels:
            no_level_reason = "NO_LEVEL_FILTERED_ALL"
            if len(candles_1h) == 0:
                no_level_reason = "NO_LEVEL_EMPTY_1H"
            elif len(candles_15m) == 0:
                no_level_reason = "NO_LEVEL_EMPTY_15M"
            elif tick_size_fallback:
                no_level_reason = "NO_LEVEL_TICKSIZE_0"
            else:
                if (not vp) and (level_meta.get("swing_1h_raw", 0) == 0 and level_meta.get("swing_mtf_raw", 0) == 0):
                    no_level_reason = "NO_LEVEL_VP_NONE_AND_SWING_EMPTY"
            attempts_str = " | ".join(
                f"dist={m.get('max_dist_pct')} before={m.get('before_cluster')} "
                f"after_cluster={m.get('after_cluster')} after_dist={m.get('after_dist')} after_cap={m.get('after_cap')}"
                for m in level_attempts
            )
            print(
                "[swaggy-levels] "
                f"sym={sym} len1h={len(candles_1h)} len15m={len(candles_15m)} "
                f"tick_size={tick_size} last_price={last_price:.6g} vp_none={1 if not vp else 0} "
                f"before={level_meta.get('before_cluster')} after_cluster={level_meta.get('after_cluster')} "
                f"after_dist={level_meta.get('after_dist')} after_cap={level_meta.get('after_cap')} "
                f"attempts={attempts_str} reason={no_level_reason}"
            )
            sample = {
                "sym": sym,
                "len_1h": len(candles_1h),
                "len_15m": len(candles_15m),
                "tick_size": tick_size,
                "last_price": last_price,
                "vp_none": 1 if not vp else 0,
                "before_cluster": level_meta.get("before_cluster"),
                "after_cluster": level_meta.get("after_cluster"),
                "after_dist": level_meta.get("after_dist"),
                "after_cap": level_meta.get("after_cap"),
                "reason": no_level_reason,
            }
            if len(self._no_level_samples) < 20:
                self._no_level_samples.append(sample)
            return SwaggyDecision(
                sym=sym,
                side=None,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=[no_level_reason],
                debug={"regime": regime_state.regime, "reason": "no_level", "level_meta": sample},
            )
        last_ts = None
        if "ts" in candles_5m.columns:
            try:
                last_ts = float(candles_5m["ts"].iloc[-1]) / 1000.0
            except Exception:
                last_ts = None
        target_level = nearest_level(levels, last_price, now_ts=last_ts, recent_secs=cfg.level_recent_secs)
        if state.get("phase") == "IN_POSITION":
            manage = manage_position(state, last_price, bar_idx, cfg.time_stop_bars_5m)
            if manage:
                state["last_stop_ts"] = now_ts
                return SwaggyDecision(
                    sym=sym,
                    side=None,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["IN_POSITION"],
                    debug={"manage": manage.action, "info": manage.info},
                )
            return SwaggyDecision(
                sym=sym,
                side=None,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["IN_POSITION"],
                debug={"regime": regime_state.regime, "reason": "in_position"},
            )

        touch_hit = False
        touch_meta: Dict[str, Any] = {}
        if target_level:
            touch_hit, touch_meta = _is_level_touch(candles_5m, target_level, cfg, regime_state.regime)
        if target_level and touch_hit:
            self._stats["touch"] = self._stats.get("touch", 0) + 1
            state["phase"] = "WAIT_TRIGGER"
            state["touch_level"] = {"price": target_level.price, "kind": target_level.kind}

        if state.get("phase") != "WAIT_TRIGGER":
            _inject_regime_evidence(touch_meta, regime_state, cfg)
            if levels:
                min_dist_pct_among = _min_dist_pct_among_levels(candles_5m.iloc[-1], levels)
                touch_meta["min_dist_pct_among_levels"] = min_dist_pct_among
            return SwaggyDecision(
                sym=sym,
                side=None,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["NO_TOUCH"],
                debug={
                    "regime": regime_state.regime,
                    "reason": "no_touch",
                    "touch_meta": touch_meta,
                },
                evidence=touch_meta,
            )

        trigger = None
        trigger_debug: Dict[str, Any] = {}
        touch_meta = state.get("touch_level") or {}
        touch_price = touch_meta.get("price")
        touch_level = _level_by_price(levels, touch_price, cfg.level_cluster_pct) if touch_price else target_level
        if touch_level:
            trigger, trigger_debug = self._detect_trigger(
                candles_5m,
                touch_level,
                cfg,
                last_price,
                regime_state.regime,
            )
        if trigger is None:
            base_evidence: Dict[str, Any] = {}
            _inject_regime_evidence(base_evidence, regime_state, cfg)
            cand_report = _trigger_candidate_report(
                candles_5m,
                touch_level,
                cfg,
                last_price,
                regime_state.regime,
                state,
                touch_meta,
            )
            cand_report["sym"] = sym
            cand_report["tf"] = cfg.tf_ltf
            cand_report["touch_count"] = 1
            cand_report["trigger_candidates"] = {
                "sweep": int(bool(cand_report.get("sweep_ok"))),
                "reclaim": int(bool(cand_report.get("reclaim_ok"))),
                "reject": int(bool(cand_report.get("rejection_ok"))),
                "retest": int(bool(cand_report.get("retest_ok"))),
            }
            cand_report["no_trigger_fail"] = _no_trigger_fail_code(cand_report)
            cand_report["no_trigger_log_sample"] = _should_sample_log(
                sym, bar_idx, cfg.no_trigger_log_sample_pct
            )
            base_evidence.update(cand_report)
            return SwaggyDecision(
                sym=sym,
                side=None,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["NO_TRIGGER"],
                debug={
                    "regime": regime_state.regime,
                    "reason": "no_trigger",
                    "trigger_debug": trigger_debug,
                },
                evidence=base_evidence,
            )
        self._stats["trigger"] = self._stats.get("trigger", 0) + 1
        self._trigger_counts[trigger.kind] = self._trigger_counts.get(trigger.kind, 0) + 1
        if trigger.kind == "RECLAIM":
            self._stats["reclaim"] = self._stats.get("reclaim", 0) + 1
        if trigger.kind == "REJECTION":
            self._stats["reject"] = self._stats.get("reject", 0) + 1

        if regime_state.regime == "bull" and trigger.side == "short":
            self._stats["bull_short_candidate"] = self._stats.get("bull_short_candidate", 0) + 1
            self._stats["bull_short_blocked_early"] = self._stats.get("bull_short_blocked_early", 0) + 1
            evidence = dict(trigger.evidence or {})
            evidence["reason"] = "BULL_SHORT_BLOCKED_EARLY"
            evidence["trigger"] = trigger.kind
            evidence["strength"] = trigger.strength
            ltf_regime = detect_regime(candles_5m, lookback=cfg.regime_lookback, k=cfg.regime_fractal_k).regime
            evidence["ltf_trend"] = ltf_regime
            evidence["htf_trend"] = regime_state.regime
            evidence["early_block_reason"] = "bull_short_block"
            evidence.update(_bull_short_regime_inputs(candles_4h, candles_1h, last_price))
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["BULL_SHORT_BLOCKED_EARLY"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "BULL_SHORT_BLOCKED_EARLY",
                    trigger_debug,
                ),
                filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 1, "weak_signal": 0, "sweep_confirm": 0},
                evidence=evidence,
            )

        short_bull_exception = False
        if regime_state.regime == "bull" and trigger.side == "short":
            ok, info = _short_bull_exception_info(trigger, regime_state, cfg, trigger_debug)
            if not ok:
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["SHORT_BULL_BLOCK"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "SHORT_BULL_BLOCK",
                        trigger_debug,
                    ),
                    filters={
                        "dist": 0,
                        "lvn_gap": 0,
                        "expansion": 0,
                        "cooldown": 0,
                        "regime": 1,
                        "weak_signal": 0,
                        "sweep_confirm": 0,
                        "short_bull_block": 1,
                    },
                    evidence=info,
                )
            trigger.evidence = {**(trigger.evidence or {}), **info}
            short_bull_exception = True

        _update_recent_triggers(state, bar_idx, trigger)
        has_confirm = False
        if trigger.kind == "SWEEP":
            state["last_sweep_ts"] = now_ts
            state["last_sweep_bar"] = bar_idx
            state["last_sweep_level"] = trigger.level.price
            state["last_sweep_side"] = trigger.side
            state["pending_sweep"] = {
                "ts": now_ts,
                "level": trigger.level.price,
                "side": trigger.side,
            }
            evidence = dict(trigger.evidence or {})
            evidence["sweep_observe"] = 1
            evidence["sweep_level"] = trigger.level.price
            evidence["sweep_side"] = trigger.side
            evidence["strength"] = trigger.strength
            has_confirm, confirm_src, confirm_strength, confirm_age, confirm_type = _has_confirm(
                state,
                trigger.side,
                cfg.confirm_window,
                "SWEEP",
                last_price,
                trigger.level.price,
                trigger.strength,
            )
            evidence["confirm_src"] = confirm_src
            evidence["confirm_type"] = confirm_type
            evidence["confirm_age"] = confirm_age
            evidence["confirm_strength"] = confirm_strength
            evidence["confirm_window"] = cfg.confirm_window
            evidence["confirm_gate"] = cfg.sweep_confirm_gate
            evidence["confirm_min"] = cfg.sweep_confirm_min
            evidence["same_bar_confirm_allowed"] = True
            evidence["confirm_found"] = int(has_confirm)
            sweep_metrics = _sweep_observe_metrics(
                candles_5m,
                trigger.level.price,
                trigger.side,
                cfg,
                confirm_age,
                cfg.confirm_window,
            )
            evidence.update(sweep_metrics)
            evidence["confirm_window_n"] = cfg.confirm_window
            evidence["elapsed_n"] = confirm_age
            evidence["rsi_slope"] = None
            evidence["structure_ok"] = int(has_confirm)
            evidence["sweep_fail_reason"] = _sweep_fail_reason(
                sweep_metrics,
                confirm_age,
                cfg.confirm_window,
                confirm_strength,
                cfg.sweep_confirm_min,
            )
            evidence["sweep_log_sample"] = _should_sample_log(
                sym, bar_idx, cfg.sweep_log_sample_pct
            )
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["SWEEP_OBSERVE"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "SWEEP_OBSERVE",
                    trigger_debug,
                ),
                filters={
                    "dist": 0,
                    "lvn_gap": 0,
                    "expansion": 0,
                    "cooldown": 0,
                    "regime": 0,
                    "weak_signal": 0,
                    "sweep_confirm": 0,
                    "short_bull_block": 0,
                    "reentry_block": 0,
                    "short_dist": 0,
                },
                evidence=evidence,
            )
        min_by_kind = _min_strength_for_trigger(trigger.kind, cfg)
        if trigger.side == "short" and regime_state.regime == "bear":
            if trigger.kind == "RECLAIM":
                min_by_kind = max(0.0, min_by_kind - 0.02)
            elif trigger.kind == "REJECTION":
                min_by_kind = max(0.0, min_by_kind - 0.02)
        if trigger.kind != "SWEEP" and trigger.strength < cfg.weak_cut:
            evidence = dict(trigger.evidence or {})
            evidence["min_strength"] = cfg.weak_cut
            evidence["min_strength_required"] = cfg.weak_cut
            evidence["strength"] = trigger.strength
            evidence["trigger"] = trigger.kind
            evidence["strength_components"] = _strength_components(trigger, last_price, regime_state.regime, cfg)
            evidence["delta"] = float(trigger.strength) - float(cfg.weak_cut)
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["WEAK_SIGNAL"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "WEAK_SIGNAL",
                    trigger_debug,
                ),
                filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 1, "sweep_confirm": 0},
                evidence=evidence,
            )
        entry_min = _entry_min_for_regime(regime_state.regime, cfg)
        if trigger.kind != "SWEEP" and trigger.strength < entry_min:
            evidence = dict(trigger.evidence or {})
            evidence["min_strength"] = entry_min
            evidence["min_strength_required"] = entry_min
            evidence["strength"] = trigger.strength
            evidence["trigger"] = trigger.kind
            evidence["strength_components"] = _strength_components(trigger, last_price, regime_state.regime, cfg)
            evidence["delta"] = float(trigger.strength) - float(entry_min)
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["WEAK_SIGNAL"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "WEAK_SIGNAL",
                    trigger_debug,
                ),
                filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 1, "sweep_confirm": 0},
                evidence=evidence,
            )

        if trigger.kind != "SWEEP" and trigger.strength < min_by_kind:
            evidence = dict(trigger.evidence or {})
            evidence["min_strength"] = min_by_kind
            evidence["min_strength_required"] = min_by_kind
            evidence["strength"] = trigger.strength
            evidence["trigger"] = trigger.kind
            evidence["strength_components"] = _strength_components(trigger, last_price, regime_state.regime, cfg)
            evidence["delta"] = float(trigger.strength) - float(min_by_kind)
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["WEAK_SIGNAL"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "WEAK_SIGNAL",
                    trigger_debug,
                ),
                filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 1, "sweep_confirm": 0},
                evidence=evidence,
            )

        if trigger.kind == "RECLAIM":
            atr = _atr(candles_5m, cfg.touch_atr_len)
            zone_high = trigger.level.high if trigger.level.high is not None else trigger.level.price
            zone_low = trigger.level.low if trigger.level.low is not None else trigger.level.price
            if trigger.side == "long":
                if (last_price - zone_high) > (atr * cfg.reclaim_ext_atr_mult):
                    evidence = dict(trigger.evidence or {})
                    evidence["atr"] = atr
                    evidence["zone_high"] = zone_high
                    evidence["ext_atr_mult"] = cfg.reclaim_ext_atr_mult
                    evidence["ext_dist"] = last_price - zone_high
                    _inject_regime_evidence(evidence, regime_state, cfg)
                    return SwaggyDecision(
                        sym=sym,
                        side=trigger.side,
                        entry_ready=0,
                        entry_px=None,
                        sl_px=None,
                        tp1_px=None,
                        tp2_px=None,
                        reason_codes=["RECLAIM_TOO_EXTENDED"],
                        debug=self._debug_payload(
                            last_price,
                            regime_state.regime,
                            trigger,
                            vp,
                            "RECLAIM_TOO_EXTENDED",
                            trigger_debug,
                        ),
                        filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 0, "sweep_confirm": 0},
                        evidence=evidence,
                    )
            elif trigger.side == "short":
                if (zone_low - last_price) > (atr * cfg.reclaim_ext_atr_mult):
                    evidence = dict(trigger.evidence or {})
                    evidence["atr"] = atr
                    evidence["zone_low"] = zone_low
                    evidence["ext_atr_mult"] = cfg.reclaim_ext_atr_mult
                    evidence["ext_dist"] = zone_low - last_price
                    _inject_regime_evidence(evidence, regime_state, cfg)
                    return SwaggyDecision(
                        sym=sym,
                        side=trigger.side,
                        entry_ready=0,
                        entry_px=None,
                        sl_px=None,
                        tp1_px=None,
                        tp2_px=None,
                        reason_codes=["RECLAIM_TOO_EXTENDED"],
                        debug=self._debug_payload(
                            last_price,
                            regime_state.regime,
                            trigger,
                            vp,
                            "RECLAIM_TOO_EXTENDED",
                            trigger_debug,
                        ),
                        filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 0, "sweep_confirm": 0},
                        evidence=evidence,
                    )

        if regime_state.regime == "range" and trigger.side == "long":
            dist_pct = abs(last_price - trigger.level.price) / trigger.level.price if trigger.level.price else 1.0
            if not (trigger.kind == "RECLAIM" and trigger.strength >= cfg.entry_min_range and dist_pct <= cfg.max_dist_reclaim):
                evidence = dict(trigger.evidence or {})
                evidence["range_long_gate"] = 1
                evidence["strength"] = trigger.strength
                evidence["dist_pct"] = dist_pct
                _inject_regime_evidence(evidence, regime_state, cfg)
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["RANGE_LONG_GATE"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "RANGE_LONG_GATE",
                        trigger_debug,
                    ),
                    filters={"dist": 0, "lvn_gap": 0, "expansion": 0, "cooldown": 0, "regime": 0, "weak_signal": 1, "sweep_confirm": 0},
                    evidence=evidence,
                )

        if cfg.symbol_entry_cooldown_min > 0:
            last_entry_ts = state.get("last_entry_ts")
            cooldown_until = None
            if isinstance(last_entry_ts, (int, float)):
                cooldown_until = float(last_entry_ts) + (cfg.symbol_entry_cooldown_min * 60)
            if cooldown_until is not None and now_ts < cooldown_until:
                evidence = dict(trigger.evidence or {})
                evidence["entry_cooldown_min"] = cfg.symbol_entry_cooldown_min
                evidence["last_entry_ts"] = last_entry_ts
                evidence["cooldown_until"] = cooldown_until
                _inject_regime_evidence(evidence, regime_state, cfg)
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["REENTRY_BLOCK"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "REENTRY_BLOCK",
                        trigger_debug,
                    ),
                    filters={
                        "dist": 0,
                        "lvn_gap": 0,
                        "expansion": 0,
                        "cooldown": 0,
                        "regime": 0,
                        "weak_signal": 0,
                        "sweep_confirm": 0,
                        "short_bull_block": 0,
                        "reentry_block": 1,
                        "short_dist": 0,
                    },
                    evidence=evidence,
                )

        if trigger.side == "short":
            dist_pct = abs(last_price - trigger.level.price) / trigger.level.price if trigger.level.price else 1.0
            max_dist = _max_short_dist_for_trigger(trigger.kind, cfg)
            if max_dist is None or max_dist <= 0 or dist_pct > max_dist:
                evidence = dict(trigger.evidence or {})
                evidence["dist_pct"] = dist_pct
                evidence["dist_cap"] = max_dist
                _inject_regime_evidence(evidence, regime_state, cfg)
                self._filter_counts["short_dist"] = self._filter_counts.get("short_dist", 0) + 1
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["SHORT_DIST_TOO_FAR"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "SHORT_DIST_TOO_FAR",
                        trigger_debug,
                    ),
                    filters={
                        "dist": 0,
                        "lvn_gap": 0,
                        "expansion": 0,
                        "cooldown": 0,
                        "regime": 0,
                        "weak_signal": 0,
                        "sweep_confirm": 0,
                        "short_bull_block": 0,
                        "reentry_block": 0,
                        "short_dist": 1,
                    },
                    evidence=evidence,
                )

        allow_countertrend = cfg.allow_countertrend or short_bull_exception
        dist_price = last_price
        if trigger.kind in ("REJECTION", "SWEEP"):
            high = float(candles_5m.iloc[-1]["high"])
            low = float(candles_5m.iloc[-1]["low"])
            close = float(candles_5m.iloc[-1]["close"])
            dist_price = _nearest_price_to_level(trigger.level.price, high, low, close)
        filters = [
            regime_ok(regime_state.regime, trigger.side.upper(), allow_countertrend, cfg.range_short_allowed),
            cooldown_ok(now_ts, state.get("last_signal_ts"), cfg.cooldown_min),
            in_lvn_gap(last_price, vp),
            expansion_bar(candles_5m, 14, cfg.expansion_atr_mult),
            dist_to_level_ok(dist_price, trigger.level, _max_dist_for_trigger(trigger.kind, cfg)),
        ]
        filter_failed = next((f for f in filters if not f.ok), None)
        dist_pct = abs(dist_price - trigger.level.price) / trigger.level.price if trigger.level.price else 0.0
        dist_max = _max_dist_for_trigger(trigger.kind, cfg)
        filter_bits = {
            "regime": 1 if filters[0].reason == "regime" and not filters[0].ok else 0,
            "cooldown": 1 if filters[1].reason == "cooldown" and not filters[1].ok else 0,
            "lvn_gap": 1 if filters[2].reason == "lvn_gap" and not filters[2].ok else 0,
            "expansion": 1 if filters[3].reason == "expansion" and not filters[3].ok else 0,
            "dist": 1 if filters[4].reason == "dist" and not filters[4].ok else 0,
            "weak_signal": 0,
            "sweep_confirm": 1 if (trigger.kind == "SWEEP" and has_confirm) else 0,
            "short_bull_block": 0,
            "reentry_block": 0,
            "short_dist": 0,
        }
        if filter_failed:
            reason = _reason_label(filter_failed.reason)
            self._filter_counts[filter_failed.reason] = self._filter_counts.get(filter_failed.reason, 0) + 1
            evidence = dict(trigger.evidence or {})
            if reason == "REGIME_BLOCK":
                evidence["regime"] = regime_state.regime
                evidence["range_short_disabled"] = (regime_state.regime == "range" and not cfg.range_short_allowed)
            if reason == "EXPANSION_BAR":
                try:
                    highs = candles_5m["high"].astype(float).tolist()
                    lows = candles_5m["low"].astype(float).tolist()
                    closes = candles_5m["close"].astype(float).tolist()
                    trs = []
                    for i in range(1, len(highs)):
                        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                        trs.append(tr)
                    if len(trs) >= 14:
                        atr = float(sum(trs[-14:]) / 14)
                        last_range = float(highs[-1] - lows[-1])
                        evidence["expansion_atr"] = atr
                        evidence["expansion_last_range"] = last_range
                        evidence["expansion_mult"] = cfg.expansion_atr_mult
                except Exception:
                    pass
            evidence["touch_dist_pct"] = dist_pct
            evidence["dist_max_for_trigger"] = dist_max
            evidence["dist_fail"] = int(filter_bits.get("dist", 0))
            evidence["level_kind"] = trigger.level.kind
            if isinstance(touch_meta, dict):
                evidence["tol_pct"] = touch_meta.get("touch_tol_pct")
                evidence["min_dist_pct_to_level"] = touch_meta.get("min_dist_pct_to_level")
            _inject_regime_evidence(evidence, regime_state, cfg)
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=[reason],
                debug=self._debug_payload(last_price, regime_state.regime, trigger, vp, reason, trigger_debug),
                filters=filter_bits,
                evidence=evidence,
            )

        sl_atr = _atr(candles_5m, cfg.touch_atr_len)
        risk = build_risk_plan(
            trigger.side,
            last_price,
            trigger.level,
            levels,
            cfg.invalid_eps,
            sl_mode=cfg.sl_mode,
            sl_atr=sl_atr,
            sl_buffer_atr=cfg.sl_buffer_atr,
            sl_pct=cfg.sl_pct,
            tp_pct=cfg.tp_pct,
        )
        if risk is None:
            return SwaggyDecision(
                sym=sym,
                side=trigger.side,
                entry_ready=0,
                entry_px=None,
                sl_px=None,
                tp1_px=None,
                tp2_px=None,
                reason_codes=["OTHER_FILTER_FAIL"],
                debug=self._debug_payload(
                    last_price,
                    regime_state.regime,
                    trigger,
                    vp,
                    "OTHER_FILTER_FAIL",
                    trigger_debug,
                ),
                filters=filter_bits,
                evidence=trigger.evidence,
            )
        if risk.tp1_px is None:
            fallback_tp1 = _fallback_tp1(trigger.side, risk.entry_px, risk.sl_px, cfg.tp1_fallback_r)
            if fallback_tp1 is None:
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["OTHER_FILTER_FAIL"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "OTHER_FILTER_FAIL",
                        trigger_debug,
                    ),
                    filters=filter_bits,
                    evidence=trigger.evidence,
                )
            risk.tp1_px = fallback_tp1
            trigger.evidence = dict(trigger.evidence or {})
            trigger.evidence["tp1_fallback"] = True
            trigger.evidence["tp1_fallback_r"] = cfg.tp1_fallback_r
        _inject_regime_evidence(trigger.evidence, regime_state, cfg)

        zone_id = _zone_id(trigger.level)
        if cfg.symbol_cooldown_bars > 0:
            last_entry_bar = state.get("last_entry_bar")
            if isinstance(last_entry_bar, int):
                bars_since = bar_idx - last_entry_bar
                if bars_since < cfg.symbol_cooldown_bars:
                    bars_left = cfg.symbol_cooldown_bars - bars_since
                    evidence = dict(trigger.evidence or {})
                    evidence["bars_left"] = bars_left
                    evidence["cooldown_bars"] = cfg.symbol_cooldown_bars
                    _inject_regime_evidence(evidence, regime_state, cfg)
                    print(f"[swaggy] ENTRY_SKIP reason=COOLDOWN sym={sym} bars_left={bars_left}")
                    return SwaggyDecision(
                        sym=sym,
                        side=trigger.side,
                        entry_ready=0,
                        entry_px=None,
                        sl_px=None,
                        tp1_px=None,
                        tp2_px=None,
                        reason_codes=["COOLDOWN"],
                        debug=self._debug_payload(
                            last_price,
                            regime_state.regime,
                            trigger,
                            vp,
                            "COOLDOWN",
                            trigger_debug,
                        ),
                        filters=filter_bits,
                        evidence=evidence,
                    )
        if cfg.zone_reentry_limit > 0 and zone_id:
            last_zone_id = state.get("last_zone_id")
            zone_count = int(state.get("zone_entry_count", 0) or 0)
            if last_zone_id == zone_id and zone_count >= cfg.zone_reentry_limit:
                evidence = dict(trigger.evidence or {})
                evidence["zone_id"] = zone_id
                evidence["zone_entry_count"] = zone_count
                evidence["zone_reentry_limit"] = cfg.zone_reentry_limit
                _inject_regime_evidence(evidence, regime_state, cfg)
                print(f"[swaggy] ENTRY_SKIP reason=ZONE_REENTRY sym={sym}")
                return SwaggyDecision(
                    sym=sym,
                    side=trigger.side,
                    entry_ready=0,
                    entry_px=None,
                    sl_px=None,
                    tp1_px=None,
                    tp2_px=None,
                    reason_codes=["ZONE_REENTRY"],
                    debug=self._debug_payload(
                        last_price,
                        regime_state.regime,
                        trigger,
                        vp,
                        "ZONE_REENTRY",
                        trigger_debug,
                    ),
                    filters=filter_bits,
                    evidence=evidence,
                )
        if cfg.post_sl_cooldown_bars > 0:
            last_sl_ts = state.get("swaggy_last_sl_ts")
            if isinstance(last_sl_ts, (int, float)):
                bars_since = int((now_ts - float(last_sl_ts)) / 300.0)
                if bars_since < cfg.post_sl_cooldown_bars:
                    bars_left = cfg.post_sl_cooldown_bars - bars_since
                    evidence = dict(trigger.evidence or {})
                    evidence["bars_left"] = bars_left
                    evidence["post_sl_cooldown_bars"] = cfg.post_sl_cooldown_bars
                    _inject_regime_evidence(evidence, regime_state, cfg)
                    print(f"[swaggy] SL_COOLDOWN_ACTIVE sym={sym} bars_left={bars_left}")
                    return SwaggyDecision(
                        sym=sym,
                        side=trigger.side,
                        entry_ready=0,
                        entry_px=None,
                        sl_px=None,
                        tp1_px=None,
                        tp2_px=None,
                        reason_codes=["SL_COOLDOWN"],
                        debug=self._debug_payload(
                            last_price,
                            regime_state.regime,
                            trigger,
                            vp,
                            "SL_COOLDOWN",
                            trigger_debug,
                        ),
                        filters=filter_bits,
                        evidence=evidence,
                    )
        state["last_signal_ts"] = now_ts
        state["last_entry_ts"] = now_ts
        state["last_entry_bar"] = bar_idx
        state["phase"] = "COOLDOWN"
        state["cooldown_until"] = now_ts + cfg.cooldown_min * 60
        if zone_id:
            last_zone_id = state.get("last_zone_id")
            if last_zone_id == zone_id:
                state["zone_entry_count"] = int(state.get("zone_entry_count", 0) or 0) + 1
            else:
                state["zone_entry_count"] = 1
            state["last_zone_id"] = zone_id
        state.update({
            "in_pos": False,
            "side": trigger.side,
            "entry_px": risk.entry_px,
            "sl_px": risk.sl_px,
            "tp1_px": risk.tp1_px,
            "tp2_px": risk.tp2_px,
            "entry_bar": bar_idx,
            "tp1_hit": False,
        })
        self._stats["entry_ready"] = self._stats.get("entry_ready", 0) + 1
        return SwaggyDecision(
            sym=sym,
            side=trigger.side,
            entry_ready=1,
            entry_px=risk.entry_px,
            sl_px=risk.sl_px,
            tp1_px=risk.tp1_px,
            tp2_px=risk.tp2_px,
            reason_codes=[],
            debug=self._debug_payload(
                last_price,
                regime_state.regime,
                trigger,
                vp,
                "ENTRY_READY",
                trigger_debug,
            ),
            filters=filter_bits,
            evidence=trigger.evidence,
        )

    def apply_external_gate(self, decision: SwaggyDecision, gate_fail: str, detail: str = "") -> None:
        if not isinstance(decision, SwaggyDecision):
            return
        if decision.entry_ready:
            self._stats["entry_ready"] = max(0, int(self._stats.get("entry_ready", 0)) - 1)
        decision.entry_ready = 0
        decision.gate_stage = "S5_ATLAS"
        decision.gate_fail = gate_fail
        decision.gate_fail_detail = detail
        if gate_fail:
            self._gate_fail_counts[gate_fail] = self._gate_fail_counts.get(gate_fail, 0) + 1
            self._ready_fail_counts[gate_fail] = self._ready_fail_counts.get(gate_fail, 0) + 1
        self._gate_stage_counts["S5_ATLAS"] = self._gate_stage_counts.get("S5_ATLAS", 0) + 1

    def _annotate_gate(self, decision: SwaggyDecision) -> None:
        stage, gate_fail, detail = _classify_gate(decision)
        decision.gate_stage = stage
        decision.gate_fail = gate_fail
        decision.gate_fail_detail = detail
        if stage:
            self._gate_stage_counts[stage] = self._gate_stage_counts.get(stage, 0) + 1
        if gate_fail:
            self._gate_fail_counts[gate_fail] = self._gate_fail_counts.get(gate_fail, 0) + 1
        trigger = (decision.debug or {}).get("trigger")
        if not decision.entry_ready and trigger and trigger != "-":
            if gate_fail:
                self._ready_fail_counts[gate_fail] = self._ready_fail_counts.get(gate_fail, 0) + 1

    def _detect_trigger(
        self,
        df: pd.DataFrame,
        level: Level,
        cfg: SwaggyConfig,
        last_price: float,
        regime: str,
    ) -> tuple[Optional[Trigger], Dict[str, Any]]:
        reclaim = detect_reclaim(df, level, cfg.touch_eps)
        rejection = detect_rejection_wick(df, level, cfg.wick_ratio, cfg.vol_mult)
        sweep = detect_sweep_and_return(df, level, cfg.sweep_eps)
        retest = detect_breakout_retest(df, level, cfg.hold_eps)
        candidates = [reclaim, rejection, sweep, retest]
        scored = []
        for trig in candidates:
            if trig is None:
                continue
            trig.strength = self._compute_strength(trig, last_price, regime, cfg)
            scored.append(trig)
        if regime == "bull":
            scored = [
                t for t in scored
                if not (t.side == "short" and t.kind in ("SWEEP", "RETEST"))
            ]
            scored = [
                t for t in scored
                if not (t.side == "short" and t.kind == "REJECTION" and t.strength < 0.55)
            ]
        debug = _trigger_debug(
            df,
            level,
            reclaim,
            rejection,
            sweep,
            retest,
            cfg,
            last_price,
            regime,
            candle_ref="closed",
        )
        if not scored:
            return None, debug
        non_sweep = [t for t in scored if t.kind != "SWEEP"]
        if non_sweep:
            best = max(non_sweep, key=lambda x: x.strength)
        else:
            best = max(scored, key=lambda x: x.strength)
        return best, debug

    def _compute_strength(
        self,
        trigger: Trigger,
        last_price: float,
        regime: str,
        cfg: SwaggyConfig,
    ) -> float:
        comps = _strength_components(trigger, last_price, regime, cfg)
        strength = (
            0.25 * comps["wick_score"]
            + 0.25 * comps["reclaim_score"]
            + 0.20 * comps["volume_score"]
            + 0.20 * comps["distance_score"]
            + 0.10 * comps["regime_score"]
        )
        return max(0.0, min(1.0, strength))

    def _debug_payload(
        self,
        last_price: float,
        regime: str,
        trigger: Trigger,
        vp: Optional[VPLevels],
        reason: str,
        trigger_debug: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        vp_meta: Dict[str, Any] = {}
        if vp:
            vp_meta = {
                "poc": vp.poc,
                "val": vp.val,
                "vah": vp.vah,
                "hvn": len(vp.profile.hvn),
                "lvn": len(vp.profile.lvn),
            }
        dist_pct = abs(last_price - trigger.level.price) / trigger.level.price if trigger.level.price else 0.0
        return {
            "px": last_price,
            "regime": regime,
            "level": trigger.level.price,
            "trigger": trigger.kind,
            "side": trigger.side,
            "strength": trigger.strength,
            "dist_pct": dist_pct,
            "reason": reason,
            "vp": vp_meta,
            "trigger_debug": trigger_debug or {},
        }


def _classify_gate(decision: SwaggyDecision) -> tuple[str, str, str]:
    if not isinstance(decision, SwaggyDecision):
        return "", "", ""
    if decision.entry_ready:
        return "S7_ENTRY_READY", "", ""
    reason = (decision.reason_codes or ["-"])[0]
    debug = decision.debug or {}
    evidence = decision.evidence or {}
    trigger = debug.get("trigger")
    strength = debug.get("strength")
    dist_pct = evidence.get("dist_pct", debug.get("dist_pct"))
    if reason.startswith("NO_LEVEL"):
        return "S0_NO_LEVEL", reason, ""
    if reason == "NO_TOUCH":
        return "S1_NO_TOUCH", "NO_TOUCH", ""
    if reason == "NO_TRIGGER":
        fail = evidence.get("no_trigger_fail") or "NO_TRIGGER"
        return "S2_NO_TRIGGER", str(fail), ""
    if reason in ("ATLAS_BLOCK", "ATLAS_SIDE_BLOCK", "ATLAS_LONG_NO_EXCEPTION", "ATLAS_LONG_BLOCK_QUALITY", "ATLAS_SHORT_BLOCK_QUALITY"):
        fail_map = {
            "ATLAS_BLOCK": "ATLAS_BLOCK_GLOBAL",
            "ATLAS_SIDE_BLOCK": "ATLAS_BLOCK_SIDE",
            "ATLAS_LONG_NO_EXCEPTION": "ATLAS_LONG_NO_EXCEPTION",
            "ATLAS_LONG_BLOCK_QUALITY": "ATLAS_LONG_BLOCK_QUALITY",
            "ATLAS_SHORT_BLOCK_QUALITY": "ATLAS_SHORT_BLOCK_QUALITY",
        }
        atlas = evidence.get("atlas") or {}
        detail = (
            f"reason={atlas.get('reason')} allow_long={atlas.get('allow_long')} "
            f"allow_short={atlas.get('allow_short')} long_req_exc={atlas.get('long_requires_exception')}"
        )
        return "S5_ATLAS", fail_map.get(reason, reason), detail
    if reason == "SWEEP_OBSERVE":
        sweep_fail = evidence.get("sweep_fail_reason") or "SWEEP_NO_CONFIRM"
        detail = (
            f"strength={_fmt_float(strength)} trigger={trigger} "
            f"confirm={_fmt_float(evidence.get('confirm_strength'))} age={evidence.get('confirm_age')}"
        )
        return "S6_TRIGGER_GATES", str(sweep_fail), detail
    if reason == "WEAK_SIGNAL":
        min_strength = evidence.get("min_strength")
        detail = f"strength={_fmt_float(strength)} min={_fmt_float(min_strength)} trigger={trigger}"
        return "S6_TRIGGER_GATES", "TRIGGER_MIN_FAIL", detail
    if reason == "WEAK_SWEEP_NO_CONFIRM":
        detail = f"strength={_fmt_float(strength)} trigger={trigger}"
        return "S6_TRIGGER_GATES", "SWEEP_NO_CONFIRM", detail
    if reason == "WEAK_SWEEP_CONF_TOO_WEAK":
        detail = (
            f"strength={_fmt_float(strength)} confirm={_fmt_float(evidence.get('confirm_strength'))} "
            f"gate={_fmt_float(evidence.get('confirm_gate'))} min={_fmt_float(evidence.get('confirm_min'))}"
        )
        return "S6_TRIGGER_GATES", "SWEEP_CONF_TOO_WEAK", detail
    if reason == "SHORT_DIST_TOO_FAR":
        detail = f"dist_pct={_fmt_float(dist_pct)} cap={_fmt_float(evidence.get('dist_cap'))} trigger={trigger}"
        return "S6_TRIGGER_GATES", "DIST_CAP_FAIL", detail
    if reason == "BULL_SHORT_BLOCKED_EARLY":
        detail = (
            f"ema20={_fmt_float(evidence.get('ema20'))} ema60={_fmt_float(evidence.get('ema60'))} "
            f"ema20_gt_ema60={evidence.get('ema20_gt_ema60')} "
            f"slope20={_fmt_float(evidence.get('slope20'))} slope60={_fmt_float(evidence.get('slope60'))} "
            f"dist_ema60_pct={_fmt_float(evidence.get('dist_to_ema60_pct'))}"
        )
        return "S4_LOCAL_FILTERS", "BULL_SHORT_BLOCKED_EARLY", detail
    if reason in ("RANGE_LONG_GATE", "REENTRY_BLOCK", "SHORT_BULL_BLOCK", "ZONE_REENTRY", "SL_COOLDOWN", "RECLAIM_TOO_EXTENDED"):
        detail = f"strength={_fmt_float(strength)} dist_pct={_fmt_float(dist_pct)} trigger={trigger}"
        return "S4_LOCAL_FILTERS", reason, detail
    if reason in ("LVN_GAP", "DIST_FAIL", "EXPANSION_BAR", "COOLDOWN", "REGIME_BLOCK", "OTHER_FILTER_FAIL"):
        detail = f"strength={_fmt_float(strength)} dist_pct={_fmt_float(dist_pct)} trigger={trigger}"
        return "S4_LOCAL_FILTERS", reason, detail
    detail = f"strength={_fmt_float(strength)} dist_pct={_fmt_float(dist_pct)} trigger={trigger}"
    return "S6_TRIGGER_GATES", reason, detail


def _fmt_float(val: Any) -> str:
    try:
        return f"{float(val):.4f}"
    except Exception:
        return "N/A"


def _is_level_touch(df: pd.DataFrame, level: Level, cfg: SwaggyConfig, regime: str) -> tuple[bool, Dict[str, Any]]:
    if df.empty or level.price <= 0:
        return False, {}
    last = df.iloc[-1]
    close = float(last["close"])
    high = float(last["high"])
    low = float(last["low"])
    band_low = level.low if level.low is not None else level.price
    band_high = level.high if level.high is not None else level.price
    atr = _atr(df, cfg.touch_atr_len)
    touch_pct_min = _touch_pct_min_for_regime(regime, cfg)
    extra_tol = max(atr * cfg.touch_eps_atr_mult, level.price * cfg.touch_eps_pct)
    tol = max(level.price * touch_pct_min, atr * cfg.touch_atr_mult, extra_tol)
    if level.low is None and level.high is None:
        band_low = level.price - tol
        band_high = level.price + tol
    near_wick_dist = min(abs(high - level.price), abs(low - level.price))
    near_wick = near_wick_dist <= tol
    wick_touch = near_wick
    close_touch = abs(close - level.price) <= tol
    dist_pct_close = abs(close - level.price) / level.price if level.price else 0.0
    dist_pct_wick_min = min(abs(close - level.price), abs(high - level.price), abs(low - level.price)) / level.price if level.price else 0.0
    meta = {
        "best_level_kind": level.kind,
        "best_level_price": level.price,
        "best_level_band_low": band_low,
        "best_level_band_high": band_high,
        "level_kind": level.kind,
        "level_price": level.price,
        "band_low": band_low,
        "band_high": band_high,
        "atr": atr,
        "min_dist_pct_to_level": dist_pct_wick_min,
        "dist_pct_close": dist_pct_close,
        "dist_pct_wick_min": dist_pct_wick_min,
        "touch_tol": tol,
        "touch_tol_pct": (tol / level.price) if level.price else 0.0,
        "extra_tol": extra_tol,
        "touch_pct_min": touch_pct_min,
        "touch_atr_mult": cfg.touch_atr_mult,
        "touch_eps_atr_mult": cfg.touch_eps_atr_mult,
        "touch_eps_pct": cfg.touch_eps_pct,
        "low": low,
        "high": high,
        "close": close,
        "wick_touch": int(wick_touch),
        "close_touch": int(close_touch),
        "near_wick_dist": near_wick_dist,
    }
    return (wick_touch or close_touch), meta


def _atr(df: pd.DataFrame, length: int) -> float:
    if df.empty or len(df) < length + 2:
        return 0.0
    highs = df["high"].astype(float).tolist()
    lows = df["low"].astype(float).tolist()
    closes = df["close"].astype(float).tolist()
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < length:
        return 0.0
    return float(sum(trs[-length:]) / length)


def _level_by_price(levels: List[Level], price: float, cluster_pct: float) -> Optional[Level]:
    if price is None:
        return None
    best = None
    best_dist = None
    for lv in levels:
        dist = abs(price - lv.price) / lv.price if lv.price else 1.0
        if best_dist is None or dist < best_dist:
            best = lv
            best_dist = dist
    if best and best_dist is not None and best_dist <= cluster_pct:
        return best
    return None


def _zone_id(level: Level) -> str:
    if not isinstance(level, Level):
        return ""
    low = "na"
    high = "na"
    if isinstance(level.low, (int, float)):
        low = f"{level.low:.6g}"
    if isinstance(level.high, (int, float)):
        high = f"{level.high:.6g}"
    return f"{level.kind}:{level.price:.6g}:{low}:{high}"


def _reason_label(reason: str) -> str:
    mapping = {
        "lvn_gap": "LVN_GAP",
        "dist": "DIST_FAIL",
        "regime": "REGIME_BLOCK",
        "expansion": "EXPANSION_BAR",
        "cooldown": "COOLDOWN",
    }
    return mapping.get(reason, "OTHER_FILTER_FAIL")


def _min_strength_for_trigger(kind: str, cfg: SwaggyConfig) -> float:
    if kind == "SWEEP":
        return cfg.min_strength_sweep
    if kind == "RECLAIM":
        return cfg.min_strength_reclaim
    if kind == "REJECTION":
        return cfg.min_strength_rejection
    if kind == "RETEST":
        return cfg.min_strength_retest
    return 0.0


def _entry_min_for_regime(regime: str, cfg: SwaggyConfig) -> float:
    if regime == "range":
        return cfg.entry_min_range
    if regime == "bear":
        return cfg.entry_min_bear
    if regime == "bull":
        return cfg.entry_min_bull
    return cfg.entry_min


def _max_dist_for_trigger(kind: str, cfg: SwaggyConfig) -> float:
    if kind == "RECLAIM":
        return cfg.max_dist_reclaim
    if kind == "RETEST":
        return cfg.max_dist_retest
    return cfg.max_dist


def _max_short_dist_for_trigger(kind: str, cfg: SwaggyConfig) -> Optional[float]:
    if kind == "RECLAIM":
        return cfg.short_max_dist_reclaim
    if kind == "RETEST":
        return cfg.short_max_dist_retest
    if kind == "REJECTION":
        return cfg.short_max_dist_rejection
    if kind == "SWEEP":
        return cfg.short_max_dist_sweep
    return cfg.max_dist


def _fallback_tp1(side: str, entry: float, sl: float, r_mult: float) -> Optional[float]:
    risk = abs(entry - sl)
    if risk <= 0 or r_mult <= 0:
        return None
    if side.upper() == "LONG":
        return entry + risk * r_mult
    return entry - risk * r_mult


def _inject_regime_evidence(evidence: Dict[str, Any], regime_state: RegimeState, cfg: SwaggyConfig) -> None:
    evidence["regime"] = regime_state.regime
    evidence["regime_score"] = regime_state.regime_score
    evidence["range_reason"] = regime_state.range_reason
    evidence["range_short_disabled"] = (regime_state.regime == "range" and not cfg.range_short_allowed)


def _touch_pct_min_for_regime(regime: str, cfg: SwaggyConfig) -> float:
    if regime == "bull":
        return cfg.touch_pct_min_bull
    if regime == "bear":
        return cfg.touch_pct_min_bear
    if regime == "range":
        return cfg.touch_pct_min_range
    return cfg.touch_pct_min_chaos


def _nearest_price_to_level(level_price: float, high: float, low: float, close: float) -> float:
    d_high = abs(high - level_price)
    d_low = abs(low - level_price)
    d_close = abs(close - level_price)
    if d_high <= d_low and d_high <= d_close:
        return high
    if d_low <= d_close:
        return low
    return close


def _min_dist_pct_among_levels(last_row: pd.Series, levels: List[Level]) -> Optional[float]:
    try:
        close = float(last_row["close"])
        high = float(last_row["high"])
        low = float(last_row["low"])
    except Exception:
        return None
    best = None
    for lv in levels:
        if not lv.price:
            continue
        dist = min(abs(close - lv.price), abs(high - lv.price), abs(low - lv.price)) / lv.price
        if best is None or dist < best:
            best = dist
    return best


def _tf_to_sec(tf: str) -> Optional[int]:
    if not tf:
        return None
    text = str(tf).strip().lower()
    if text.endswith("m"):
        try:
            return int(text[:-1]) * 60
        except Exception:
            return None
    if text.endswith("h"):
        try:
            return int(text[:-1]) * 3600
        except Exception:
            return None
    if text.endswith("d"):
        try:
            return int(text[:-1]) * 86400
        except Exception:
            return None
    return None


def _apply_range_hysteresis(state: Dict[str, Any], regime_state: RegimeState, confirm_bars: int) -> RegimeState:
    if confirm_bars <= 1:
        return regime_state
    if regime_state.regime == "range":
        count = int(state.get("range_count", 0) or 0) + 1
        state["range_count"] = count
        last_non_range = state.get("last_non_range")
        if count < confirm_bars and last_non_range in ("bull", "bear"):
            return RegimeState(
                last_non_range,
                regime_state.last_swing_high,
                regime_state.last_swing_low,
                regime_state.regime_score,
                "range_pending",
            )
        return regime_state
    state["range_count"] = 0
    state["last_non_range"] = regime_state.regime
    return regime_state


def _short_bull_exception_info(
    trigger: Trigger,
    regime_state: RegimeState,
    cfg: SwaggyConfig,
    trigger_debug: Optional[Dict[str, Any]] = None,
) -> tuple[bool, Dict[str, Any]]:
    evidence = dict(trigger.evidence or {})
    wick_ratio = float(evidence.get("wick_ratio") or 0.0)
    vol = float(evidence.get("vol") or 0.0)
    vol_sma = float(evidence.get("vol_sma") or 0.0)
    if trigger_debug:
        wick_ratio = float(trigger_debug.get("wick_ratio") or wick_ratio)
        vol = float(trigger_debug.get("vol") or vol)
        vol_sma = float(trigger_debug.get("vol_sma") or vol_sma)
    type_ok = trigger.kind in ("REJECTION", "RECLAIM")
    strength_ok = trigger.strength >= cfg.short_bull_min_strength
    wick_ok = wick_ratio >= cfg.short_bull_wick_ratio
    vol_ok = vol_sma > 0 and vol >= vol_sma * cfg.short_bull_vol_mult
    ok = type_ok and strength_ok and (wick_ok or vol_ok)
    evidence.update({
        "regime": regime_state.regime,
        "short_bull_exception": ok,
        "short_bull_block_subreason": "",
        "trigger": trigger.kind,
        "strength": trigger.strength,
        "wick_ratio": wick_ratio,
        "vol": vol,
        "vol_sma": vol_sma,
        "short_bull_min_strength": cfg.short_bull_min_strength,
        "short_bull_wick_ratio": cfg.short_bull_wick_ratio,
        "short_bull_vol_mult": cfg.short_bull_vol_mult,
    })
    if not ok:
        if not type_ok:
            evidence["short_bull_block_subreason"] = "TYPE_FAIL"
        elif not strength_ok:
            evidence["short_bull_block_subreason"] = "STRENGTH_FAIL"
        elif not (wick_ok or vol_ok):
            evidence["short_bull_block_subreason"] = "WICK_VOL_FAIL"
    return ok, evidence


def _update_recent_triggers(state: Dict[str, Any], bar_idx: int, trigger: Trigger) -> None:
    max_len = 20
    recent = state.get("recent_triggers")
    if isinstance(recent, deque):
        recent = list(recent)
    if not isinstance(recent, list):
        recent = []
    recent.append({
        "bar": bar_idx,
        "trigger": trigger.kind,
        "side": trigger.side,
        "strength": trigger.strength,
    })
    if len(recent) > max_len:
        recent = recent[-max_len:]
    state["recent_triggers"] = recent


def _has_confirm(
    state: Dict[str, Any],
    side: str,
    window: int,
    trigger_kind: str,
    last_close: float,
    level_price: float,
    trigger_strength: float,
) -> tuple[bool, str, float, Optional[int], str]:
    recent = state.get("recent_triggers")
    if isinstance(last_close, (int, float)) and level_price > 0 and trigger_kind == "SWEEP":
        if side == "long" and last_close > level_price:
            return True, "SAME_BAR_RECLAIM", trigger_strength, 0, "SAME_BAR_RECLAIM"
        if side == "short" and last_close < level_price:
            return True, "SAME_BAR_RECLAIM", trigger_strength, 0, "SAME_BAR_RECLAIM"
    if isinstance(recent, deque):
        recent = list(recent)
    if not isinstance(recent, list) or not recent:
        return False, "none", 0.0, None, "none"
    cur_bar = recent[-1]["bar"] if recent else None
    if cur_bar is None:
        return False, "none", 0.0, None, "none"
    for item in reversed(recent):
        if item.get("trigger") not in ("RECLAIM", "REJECTION", "RETEST"):
            continue
        if item.get("side") != side:
            continue
        diff = cur_bar - int(item.get("bar", 0))
        if diff <= window:
            kind = item.get("trigger")
            return True, f"{kind}@-{diff}", float(item.get("strength") or 0.0), diff, str(kind)
        break
    return False, "none", 0.0, None, "none"


def _trigger_debug(
    df: pd.DataFrame,
    level: Level,
    reclaim: Optional[Trigger],
    rejection: Optional[Trigger],
    sweep: Optional[Trigger],
    retest: Optional[Trigger],
    cfg: SwaggyConfig,
    last_price: float,
    regime: str,
    candle_ref: str,
) -> Dict[str, Any]:
    if df.empty:
        return {}
    last = df.iloc[-1]
    close = float(last["close"])
    open_ = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])
    c1_close = c2_close = c3_close = None
    if len(df) >= 3:
        c1_close = float(df.iloc[-3]["close"])
        c2_close = float(df.iloc[-2]["close"])
        c3_close = float(df.iloc[-1]["close"])
    candle_range = max(1e-9, high - low)
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    wick_ratio = max(upper_wick, lower_wick) / candle_range
    vols = df["volume"].astype(float)
    vol = float(vols.iloc[-1])
    vol_sma = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(vols.mean())
    debug = {
        "level": level.price,
        "reclaim_ok": 1 if reclaim else 0,
        "rejection_ok": 1 if rejection else 0,
        "sweep_ok": 1 if sweep else 0,
        "retest_ok": 1 if retest else 0,
        "candle_ref": candle_ref,
        "c1_close": c1_close,
        "c2_close": c2_close,
        "c3_close": c3_close,
        "wick_ratio": wick_ratio,
        "vol": vol,
        "vol_sma": vol_sma,
    }
    debug.update(_trigger_candidate_report(df, level, cfg, last_price, regime, None))
    return debug


def _trigger_candidate_report(
    df: pd.DataFrame,
    level: Optional[Level],
    cfg: SwaggyConfig,
    last_price: float,
    regime: str,
    state: Optional[Dict[str, Any]],
    touch_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if df.empty or level is None:
        return {}
    last = df.iloc[-1]
    close = float(last["close"])
    high = float(last["high"])
    low = float(last["low"])
    c1 = df.iloc[-3] if len(df) >= 3 else df.iloc[-1]
    c2 = df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
    c3 = df.iloc[-1]
    c1_close = float(c1["close"])
    c2_close = float(c2["close"])
    c3_close = float(c3["close"])
    c2_low = float(c2["low"])
    c2_high = float(c2["high"])
    band_low = level.low if level.low is not None else level.price
    band_high = level.high if level.high is not None else level.price
    tol = None
    dist_pct_to_level = None
    if isinstance(touch_meta, dict):
        tol = touch_meta.get("touch_tol")
        dist_pct_to_level = touch_meta.get("min_dist_pct_to_level")

    reclaim_long = c1_close < level.price * (1 - cfg.touch_eps)
    reclaim_long_c2 = c2_close >= level.price * (1 - cfg.touch_eps)
    reclaim_long_c3 = c3_close >= level.price * (1 - cfg.touch_eps)
    reclaim_short = c1_close > level.price * (1 + cfg.touch_eps)
    reclaim_short_c2 = c2_close <= level.price * (1 + cfg.touch_eps)
    reclaim_short_c3 = c3_close <= level.price * (1 + cfg.touch_eps)
    reclaim_ok = (reclaim_long and reclaim_long_c2 and reclaim_long_c3) or (reclaim_short and reclaim_short_c2 and reclaim_short_c3)
    reclaim_reason = "ok"
    if not reclaim_ok:
        if reclaim_long or reclaim_short:
            if reclaim_long:
                if not reclaim_long_c2:
                    reclaim_reason = "c2_not_reclaim"
                elif not reclaim_long_c3:
                    reclaim_reason = "c3_not_hold"
            else:
                if not reclaim_short_c2:
                    reclaim_reason = "c2_not_reclaim"
                elif not reclaim_short_c3:
                    reclaim_reason = "c3_not_hold"
        else:
            reclaim_reason = "c1_not_far_enough"

    hold_tol = cfg.hold_eps * 0.5
    retest_long = c1_close > level.price
    retest_long_c2 = c2_low <= level.price
    retest_long_c3 = c3_close > level.price * (1 + hold_tol)
    retest_short = c1_close < level.price
    retest_short_c2 = c2_high >= level.price
    retest_short_c3 = c3_close < level.price * (1 - hold_tol)
    retest_ok = (retest_long and retest_long_c2 and retest_long_c3) or (retest_short and retest_short_c2 and retest_short_c3)
    retest_reason = "ok"
    if not retest_ok:
        if retest_long or retest_short:
            if retest_long:
                if not retest_long_c2:
                    retest_reason = "c2_not_touch"
                elif not retest_long_c3:
                    retest_reason = "c3_not_hold"
            else:
                if not retest_short_c2:
                    retest_reason = "c2_not_touch"
                elif not retest_short_c3:
                    retest_reason = "c3_not_hold"
        else:
            retest_reason = "c1_not_break"

    vol = float(last["volume"])
    vol_sma = float(df["volume"].astype(float).rolling(20).mean().iloc[-1]) if len(df) >= 20 else float(df["volume"].astype(float).mean())
    candle_range = max(1e-9, high - low)
    upper_wick = high - max(float(last["open"]), close)
    lower_wick = min(float(last["open"]), close) - low
    wick_ratio = max(upper_wick, lower_wick) / candle_range
    rejection_vol_ok = vol_sma > 0 and vol >= vol_sma * cfg.vol_mult
    rejection_ok = False
    rejection_reason = "ok"
    if rejection_vol_ok:
        if high >= level.price and upper_wick / candle_range >= cfg.wick_ratio and close < level.price:
            rejection_ok = True
        elif low <= level.price and lower_wick / candle_range >= cfg.wick_ratio and close > level.price:
            rejection_ok = True
        else:
            rejection_reason = "wick_or_close_fail"
    else:
        rejection_reason = "vol_too_low"

    sweep_breach = (high >= level.price * (1 + cfg.sweep_eps)) or (low <= level.price * (1 - cfg.sweep_eps))
    sweep_ok = False
    sweep_reason = "ok"
    sweep_side = None
    if sweep_breach:
        if (high >= level.price * (1 + cfg.sweep_eps) and close < level.price) or (
            low <= level.price * (1 - cfg.sweep_eps) and close > level.price
        ):
            sweep_ok = True
            sweep_side = "short" if close < level.price else "long"
        else:
            sweep_reason = "no_return"
    else:
        sweep_reason = "no_breach"
    has_confirm = 0
    confirm_src = "none"
    confirm_strength = 0.0
    confirm_type = "none"
    confirm_age = None
    if sweep_ok:
        tmp_trigger = Trigger(sweep_side or "long", "SWEEP", level, 0.0, {})
        tmp_trigger.strength = _compute_strength_simple(tmp_trigger, last_price, regime, cfg)
        if state is not None:
            has_confirm, confirm_src, confirm_strength, confirm_age, confirm_type = _has_confirm(
                state,
                sweep_side or "long",
                cfg.confirm_window,
                "SWEEP",
                last_price,
                level.price,
                tmp_trigger.strength,
            )

    reclaim_strength = None
    retest_strength = None
    rejection_strength = None
    sweep_strength = None
    if reclaim_ok:
        reclaim_side = "long" if reclaim_long and reclaim_long_c2 and reclaim_long_c3 else "short"
        trig = Trigger(reclaim_side, "RECLAIM", level, 0.0, {})
        reclaim_strength = _compute_strength_simple(trig, last_price, regime, cfg)
    if retest_ok:
        retest_side = "long" if retest_long and retest_long_c2 and retest_long_c3 else "short"
        trig = Trigger(retest_side, "RETEST", level, 0.0, {})
        retest_strength = _compute_strength_simple(trig, last_price, regime, cfg)
    if rejection_ok:
        rejection_side = "short" if close < level.price else "long"
        trig = Trigger(
            rejection_side,
            "REJECTION",
            level,
            0.0,
            {"wick_ratio": wick_ratio, "vol": vol, "vol_sma": vol_sma},
        )
        rejection_strength = _compute_strength_simple(trig, last_price, regime, cfg)
    if sweep_ok:
        trig = Trigger(sweep_side or "long", "SWEEP", level, 0.0, {})
        sweep_strength = _compute_strength_simple(trig, last_price, regime, cfg)

    best_strength = max(
        [s for s in [reclaim_strength, retest_strength, rejection_strength, sweep_strength] if s is not None],
        default=None,
    )
    best_candidate = None
    if best_strength is not None:
        if best_strength == reclaim_strength:
            best_candidate = "RECLAIM"
        elif best_strength == retest_strength:
            best_candidate = "RETEST"
        elif best_strength == rejection_strength:
            best_candidate = "REJECTION"
        elif best_strength == sweep_strength:
            best_candidate = "SWEEP"
    why_none = "all_candidates_failed"
    if any(v for v in [reclaim_ok, retest_ok, rejection_ok, sweep_ok]):
        why_none = "candidates_ok_but_filtered"
    best_fail_reason = "no_candidate"
    if best_candidate == "RECLAIM":
        best_fail_reason = "" if reclaim_ok else reclaim_reason
    elif best_candidate == "RETEST":
        best_fail_reason = "" if retest_ok else retest_reason
    elif best_candidate == "REJECTION":
        best_fail_reason = "" if rejection_ok else rejection_reason
    elif best_candidate == "SWEEP":
        best_fail_reason = "" if sweep_ok else sweep_reason
    return {
        "candles_used": {
            "c1": {"close": c1_close, "high": float(c1["high"]), "low": float(c1["low"])},
            "c2": {"close": c2_close, "high": c2_high, "low": c2_low},
            "c3": {"close": c3_close, "high": high, "low": low},
        },
        "close": close,
        "high": high,
        "low": low,
        "level_kind": level.kind,
        "level_price": level.price,
        "band_low": band_low,
        "band_high": band_high,
        "tol": tol,
        "dist_pct_to_level": dist_pct_to_level,
        "reclaim_ok": int(reclaim_ok),
        "reclaim_strength": reclaim_strength,
        "reclaim_fail_reason": "" if reclaim_ok else reclaim_reason,
        "reclaim_min_required": cfg.min_strength_reclaim,
        "retest_ok": int(retest_ok),
        "retest_strength": retest_strength,
        "retest_fail_reason": "" if retest_ok else retest_reason,
        "retest_min_required": cfg.min_strength_retest,
        "rejection_ok": int(rejection_ok),
        "rejection_strength": rejection_strength,
        "wick_ratio": wick_ratio,
        "rejection_fail_reason": "" if rejection_ok else rejection_reason,
        "rejection_min_required": cfg.min_strength_rejection,
        "sweep_ok": int(sweep_ok),
        "sweep_strength": sweep_strength,
        "sweep_min_required": cfg.min_strength_sweep,
        "has_confirm": int(has_confirm),
        "confirm_type": confirm_type,
        "confirm_strength": confirm_strength,
        "confirm_src": confirm_src,
        "confirm_age": confirm_age,
        "confirm_window": cfg.confirm_window,
        "confirm_gate": cfg.sweep_confirm_gate,
        "confirm_min": cfg.sweep_confirm_min,
        "sweep_fail_reason": "" if sweep_ok else sweep_reason,
        "best_candidate": best_candidate,
        "best_strength": best_strength,
        "best_fail_reason": best_fail_reason,
        "why_none": why_none,
    }


def _compute_strength_simple(trigger: Trigger, last_price: float, regime: str, cfg: SwaggyConfig) -> float:
    comps = _strength_components(trigger, last_price, regime, cfg)
    strength = (
        0.25 * comps["wick_score"]
        + 0.25 * comps["reclaim_score"]
        + 0.20 * comps["volume_score"]
        + 0.20 * comps["distance_score"]
        + 0.10 * comps["regime_score"]
    )
    return max(0.0, min(1.0, strength))


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


def _should_sample_log(symbol: str, bar_idx: int, sample_pct: float) -> bool:
    if sample_pct <= 0:
        return False
    if sample_pct >= 1:
        return True
    bucket = int(abs(hash(f"{symbol}:{bar_idx}")) % 1000)
    return bucket < int(sample_pct * 1000)


def _no_trigger_fail_code(cand_report: Dict[str, Any]) -> str:
    if not cand_report:
        return "NO_TRIGGER__NO_CANDIDATE"
    best = cand_report.get("best_candidate")
    if best == "SWEEP" and not cand_report.get("has_confirm"):
        return "NO_TRIGGER__SWEEP_SEEN_FAIL_CONFIRM"
    if best == "RECLAIM":
        strength = cand_report.get("reclaim_strength")
        min_req = cand_report.get("reclaim_min_required")
        if strength is not None and min_req is not None and float(strength) < float(min_req):
            return "NO_TRIGGER__RECLAIM_SEEN_FAIL_MIN"
    if best == "REJECTION":
        strength = cand_report.get("rejection_strength")
        min_req = cand_report.get("rejection_min_required")
        if strength is not None and min_req is not None and float(strength) < float(min_req):
            return "NO_TRIGGER__REJECTION_SEEN_FAIL_MIN"
    if best == "RETEST":
        strength = cand_report.get("retest_strength")
        min_req = cand_report.get("retest_min_required")
        if strength is not None and min_req is not None and float(strength) < float(min_req):
            return "NO_TRIGGER__RETEST_SEEN_FAIL_MIN"
    if cand_report.get("sweep_ok") and not cand_report.get("has_confirm"):
        return "NO_TRIGGER__SWEEP_SEEN_FAIL_CONFIRM"
    if cand_report.get("reclaim_ok") and cand_report.get("reclaim_strength") is not None:
        if float(cand_report.get("reclaim_strength")) < float(cand_report.get("reclaim_min_required") or 0):
            return "NO_TRIGGER__RECLAIM_SEEN_FAIL_MIN"
    if cand_report.get("rejection_ok") and cand_report.get("rejection_strength") is not None:
        if float(cand_report.get("rejection_strength")) < float(cand_report.get("rejection_min_required") or 0):
            return "NO_TRIGGER__REJECTION_SEEN_FAIL_MIN"
    if cand_report.get("retest_ok") and cand_report.get("retest_strength") is not None:
        if float(cand_report.get("retest_strength")) < float(cand_report.get("retest_min_required") or 0):
            return "NO_TRIGGER__RETEST_SEEN_FAIL_MIN"
    return "NO_TRIGGER__NO_CANDIDATE"


def _ema_last(values: pd.Series, span: int) -> tuple[Optional[float], Optional[float]]:
    if values is None or values.empty or len(values) < span + 2:
        return None, None
    ema = values.ewm(span=span, adjust=False).mean()
    try:
        return float(ema.iloc[-1]), float(ema.iloc[-2])
    except Exception:
        return None, None


def _bull_short_regime_inputs(
    candles_4h: pd.DataFrame,
    candles_1h: pd.DataFrame,
    last_price: float,
) -> Dict[str, Any]:
    df = candles_4h if not candles_4h.empty else candles_1h
    if df.empty or "close" not in df.columns:
        return {
            "ema20": None,
            "ema60": None,
            "ema20_gt_ema60": None,
            "slope20": None,
            "slope60": None,
            "dist_to_ema60_pct": None,
        }
    closes = df["close"].astype(float)
    ema20, ema20_prev = _ema_last(closes, 20)
    ema60, ema60_prev = _ema_last(closes, 60)
    slope20 = None
    slope60 = None
    if ema20 is not None and ema20_prev not in (None, 0):
        slope20 = (ema20 - ema20_prev) / ema20_prev
    if ema60 is not None and ema60_prev not in (None, 0):
        slope60 = (ema60 - ema60_prev) / ema60_prev
    dist_to_ema60 = None
    if ema60 not in (None, 0):
        dist_to_ema60 = (last_price - ema60) / ema60
    return {
        "ema20": ema20,
        "ema60": ema60,
        "ema20_gt_ema60": None if (ema20 is None or ema60 is None) else int(ema20 > ema60),
        "slope20": slope20,
        "slope60": slope60,
        "dist_to_ema60_pct": dist_to_ema60,
    }


def _sweep_observe_metrics(
    df: pd.DataFrame,
    level_price: float,
    side: str,
    cfg: SwaggyConfig,
    confirm_age: Optional[int],
    confirm_window: int,
) -> Dict[str, Any]:
    if df.empty or level_price <= 0:
        return {
            "sweep_ts": None,
            "confirm_deadline_ts": None,
            "sweep_direction": None,
            "sweep_side": side,
            "sweep_level_price": level_price,
            "sweep_wick_depth_pct": None,
            "reclaim_close_dist_pct": None,
            "vol_ratio": None,
            "body_ratio": None,
            "wick_ratio": None,
            "confirm_reclaim_ok": None,
            "confirm_body_ok": None,
            "confirm_wick_ok": None,
            "confirm_vol_ok": None,
            "confirm_window_ok": None,
        }
    last = df.iloc[-1]
    high = float(last["high"])
    low = float(last["low"])
    close = float(last["close"])
    open_ = float(last["open"])
    ts_val = None
    if "ts" in last:
        try:
            ts_val = int(last["ts"])
        except Exception:
            ts_val = None
    sweep_direction = "UPPER" if side == "short" else "LOWER"
    sweep_wick_depth_pct = None
    if side == "short":
        sweep_wick_depth_pct = max(0.0, (high - level_price) / level_price)
    else:
        sweep_wick_depth_pct = max(0.0, (level_price - low) / level_price)
    reclaim_close_dist_pct = abs(close - level_price) / level_price
    candle_range = max(1e-9, high - low)
    body_ratio = abs(close - open_) / candle_range
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    wick_ratio = max(upper_wick, lower_wick) / candle_range
    vols = df["volume"].astype(float)
    vol = float(vols.iloc[-1])
    vol_sma = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(vols.mean())
    vol_ratio = vol / vol_sma if vol_sma > 0 else None
    confirm_reclaim_ok = (close < level_price) if side == "short" else (close > level_price)
    confirm_body_ok = body_ratio >= 0.3
    confirm_wick_ok = wick_ratio >= cfg.wick_ratio
    confirm_vol_ok = vol_ratio is not None and vol_ratio >= cfg.vol_mult
    confirm_window_ok = confirm_age is not None and confirm_age <= confirm_window
    deadline_ts = None
    if ts_val is not None:
        tf_sec = _tf_to_sec(cfg.tf_ltf)
        if tf_sec:
            deadline_ts = ts_val + int(confirm_window * tf_sec * 1000)
    return {
        "sweep_ts": ts_val,
        "confirm_deadline_ts": deadline_ts,
        "sweep_direction": sweep_direction,
        "sweep_side": side,
        "sweep_level_price": level_price,
        "sweep_wick_depth_pct": sweep_wick_depth_pct,
        "reclaim_close_dist_pct": reclaim_close_dist_pct,
        "vol_ratio": vol_ratio,
        "body_ratio": body_ratio,
        "wick_ratio": wick_ratio,
        "confirm_reclaim_ok": int(confirm_reclaim_ok),
        "confirm_body_ok": int(confirm_body_ok),
        "confirm_wick_ok": int(confirm_wick_ok),
        "confirm_vol_ok": int(confirm_vol_ok),
        "confirm_window_ok": int(confirm_window_ok),
    }


def _sweep_fail_reason(
    metrics: Dict[str, Any],
    confirm_age: Optional[int],
    confirm_window: int,
    confirm_strength: float,
    confirm_min: float,
) -> str:
    if metrics.get("confirm_reclaim_ok") in (0, None):
        return "SWEEP_NO_RECLAIM"
    if confirm_age is not None and confirm_age > confirm_window:
        return "SWEEP_LATE_RECLAIM"
    if confirm_strength is not None and confirm_strength < confirm_min:
        return "SWEEP_RECLAIM_BUT_WEAK"
    if metrics.get("confirm_wick_ok") in (0, None):
        return "SWEEP_RECLAIM_BUT_INVALID_WICK"
    if metrics.get("confirm_vol_ok") in (0, None):
        return "SWEEP_RECLAIM_BUT_WEAK"
    return "SWEEP_RECLAIM_BUT_WEAK"
