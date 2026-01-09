from __future__ import annotations

import os
import sys
import time
from collections import Counter, deque
from typing import Any, Dict, Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import ccxt
import pandas as pd
import requests

import cycle_cache
from env_loader import load_env
from engines.base import EngineContext
from engines.swaggy.event_writer import JsonlWriter, now_ms
from engines.swaggy.logs import (
    format_cut_top,
    format_debug,
    format_zone_stats,
)
from engines.swaggy.swaggy_engine import SwaggyConfig, SwaggyEngine


def _make_exchange() -> ccxt.Exchange:
    return ccxt.binance({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
        "timeout": 30_000,
        "rateLimit": 200,
    })


def _send_telegram(bot_token: str, chat_id: str, text: str) -> bool:
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text[:3800],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if not r.ok:
            print("[swaggy-telegram] failed:", r.status_code, r.text)
        return r.ok
    except Exception as e:
        print("[swaggy-telegram] error:", e)
        return False


def _make_fetcher(ex: ccxt.Exchange):
    def _fetch(symbol: str, tf: str, limit: int) -> Optional[list]:
        try:
            return ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        except Exception:
            return None
    return _fetch


def _df_from_raw(raw: list) -> pd.DataFrame:
    return pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])


def _slice_by_ts(df: pd.DataFrame, ts_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["ts"] <= ts_ms]


def _replay_symbol(sym: str, cfg: SwaggyConfig) -> None:
    ex = _make_exchange()
    raw_5m = ex.fetch_ohlcv(sym, timeframe=cfg.tf_ltf, limit=cfg.ltf_limit)
    raw_15m = ex.fetch_ohlcv(sym, timeframe=cfg.tf_mtf, limit=200)
    raw_1h = ex.fetch_ohlcv(sym, timeframe=cfg.tf_htf, limit=cfg.vp_lookback_1h)
    raw_4h = ex.fetch_ohlcv(sym, timeframe=cfg.tf_htf2, limit=200)
    df_5m = _df_from_raw(raw_5m)
    df_15m = _df_from_raw(raw_15m)
    df_1h = _df_from_raw(raw_1h)
    df_4h = _df_from_raw(raw_4h)
    engine = SwaggyEngine(cfg)
    engine.begin_cycle()
    state: Dict[str, Any] = {}
    entry_count = 0
    cycle = 0
    summary_every = int(os.getenv("SWAGGY_SUMMARY_EVERY", "50"))
    window_eval = int(os.getenv("SWAGGY_WINDOW_EVAL", "500"))
    window_reason = int(os.getenv("SWAGGY_WINDOW_REASON", "2000"))
    cut_mode = _cut_mode()
    debug_on = os.getenv("SWAGGY_DEBUG", "0").strip() == "1"
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(ROOT_DIR, "logs", "swaggy", f"swaggy_replay_{sym.replace('/', '').replace(':', '')}_{date_tag}.jsonl")
    writer = JsonlWriter(log_path)
    reason_window = deque(maxlen=window_reason)
    gate_fail_window = deque(maxlen=window_reason)
    gate_stage_window = deque(maxlen=window_reason)
    ready_fail_window = deque(maxlen=window_reason)
    trigger_window = deque(maxlen=window_eval)
    strength_window = deque(maxlen=window_eval)
    funnel_window = deque(maxlen=window_reason)
    for i in range(30, len(df_5m)):
        cycle += 1
        cur = df_5m.iloc[i]
        ts_ms = int(cur["ts"])
        now_ts = ts_ms / 1000.0
        d5 = df_5m.iloc[: i + 1]
        d15 = _slice_by_ts(df_15m, ts_ms)
        d1h = _slice_by_ts(df_1h, ts_ms)
        d4h = _slice_by_ts(df_4h, ts_ms)
        print(f"[swaggy-scan] mode=replay tf={cfg.tf_ltf} cycle={cycle} ts={ts_ms} syms=1 evaluated=1")
        decision = engine.evaluate_symbol(
            sym=sym,
            candles_4h=d4h,
            candles_1h=d1h,
            candles_15m=d15,
            candles_5m=d5,
            position_state=state,
            engine_config=cfg,
            now_ts=now_ts,
        )
        reason = _decision_reason(decision)
        dbg = decision.debug or {}
        trigger_kind = dbg.get("trigger", "-")
        strength = dbg.get("strength", 0.0) if isinstance(dbg.get("strength"), (int, float)) else 0.0
        dist_pct = dbg.get("dist_pct", 0.0) if isinstance(dbg.get("dist_pct"), (int, float)) else 0.0
        regime = dbg.get("regime", "-")
        in_lvn_gap = 1 if decision.filters.get("lvn_gap") == 1 else 0
        trigger_debug = (dbg.get("trigger_debug") or {}) if isinstance(dbg.get("trigger_debug"), dict) else {}
        if debug_on and trigger_debug:
            print(
                "[swaggy-trigger-debug] "
                f"sym={sym} side={decision.side} px={last_price(cur):.6g} level={trigger_debug.get('level', '-')}\n"
                f"trigger_candidate="
                f"reclaim={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection={trigger_debug.get('rejection_ok', 0)} "
                f"sweep={trigger_debug.get('sweep_ok', 0)} "
                f"retest={trigger_debug.get('retest_ok', 0)}\n"
                f"candle_ref={trigger_debug.get('candle_ref', '-')}"
                f" c1_close={trigger_debug.get('c1_close')} c2_close={trigger_debug.get('c2_close')} "
                f"c3_close={trigger_debug.get('c3_close')}\n"
                f"reclaim_ok={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection_ok={trigger_debug.get('rejection_ok', 0)} "
                f"sweep_ok={trigger_debug.get('sweep_ok', 0)}\n"
                f"wick_ratio={trigger_debug.get('wick_ratio')} vol={trigger_debug.get('vol')} "
                f"vol_sma={trigger_debug.get('vol_sma')}"
            )
        trigger_debug = (dbg.get("trigger_debug") or {}) if isinstance(dbg.get("trigger_debug"), dict) else {}
        if debug_on and trigger_debug:
            print(
                "[swaggy-trigger-debug] "
                f"sym={sym} side={decision.side} px={last_price(cur):.6g} level={trigger_debug.get('level', '-')}\n"
                f"trigger_candidate="
                f"reclaim={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection={trigger_debug.get('rejection_ok', 0)} "
                f"sweep={trigger_debug.get('sweep_ok', 0)} "
                f"retest={trigger_debug.get('retest_ok', 0)}\n"
                f"candle_ref={trigger_debug.get('candle_ref', '-')}"
                f" c1_close={trigger_debug.get('c1_close')} c2_close={trigger_debug.get('c2_close')} "
                f"c3_close={trigger_debug.get('c3_close')}\n"
                f"reclaim_ok={trigger_debug.get('reclaim_ok', 0)} "
                f"rejection_ok={trigger_debug.get('rejection_ok', 0)} "
                f"sweep_ok={trigger_debug.get('sweep_ok', 0)}\n"
                f"wick_ratio={trigger_debug.get('wick_ratio')} vol={trigger_debug.get('vol')} "
                f"vol_sma={trigger_debug.get('vol_sma')}"
            )
        funnel = _funnel_counts(decision, trigger_kind)
        print(
            "[swaggy-funnel] "
            f"total={funnel['total']} in_pos={funnel['in_pos']} cooldown={funnel['cooldown']} "
            f"no_level={funnel['no_level']} no_touch={funnel['no_touch']} "
            f"touched={funnel['touched']} no_trigger={funnel['no_trigger']} "
            f"trigger={funnel['trigger']} filter_fail={funnel['filter_fail']} "
            f"filters_pass={funnel['filters_pass']} entry_ready={funnel['entry_ready']}"
        )
        reason_window.append(reason)
        gate_fail_window.append(decision.gate_fail or reason)
        gate_stage_window.append(decision.gate_stage or "")
        if not decision.entry_ready and decision.gate_fail:
            ready_fail_window.append(decision.gate_fail)
        funnel_window.append(funnel)
        if trigger_kind in {"SWEEP", "RECLAIM", "REJECTION", "RETEST"}:
            trigger_window.append(trigger_kind)
            strength_window.append(float(strength))
        if decision.entry_ready:
            entry_count += 1
            level = dbg.get("level", "-")
            bucket = _strength_bucket(float(strength))
            print(
                f"[swaggy-entry] sym={sym} side={decision.side} px={last_price(cur):.6g} level={level}\n"
                f"trigger={trigger_kind} strength={strength:.2f} bucket={bucket} "
                f"dist_pct={_fmt_dist(dist_pct)} regime={regime} in_lvn_gap={in_lvn_gap}\n"
                f"entry={decision.entry_px:.6g} sl={decision.sl_px:.6g} "
                f"tp1={decision.tp1_px} tp2={decision.tp2_px} reason=ENTRY_READY"
            )
            writer.write(_entry_event(
                sym,
                cfg,
                "replay",
                cycle,
                decision,
                trigger_kind,
                strength,
                bucket,
                dist_pct,
                regime,
                in_lvn_gap,
                ts_ms,
            ))
        elif debug_on or _should_log_cut(cut_mode, decision):
            extra = ""
            if reason == "NO_TRIGGER" and isinstance(decision.evidence, dict):
                if decision.evidence.get("no_trigger_log_sample"):
                    extra = " " + _fmt_no_trigger(decision.evidence)
            if reason == "SWEEP_OBSERVE" and isinstance(decision.evidence, dict):
                if decision.evidence.get("sweep_log_sample"):
                    extra = " " + _fmt_sweep_observe(decision.evidence)
            if reason == "WEAK_SIGNAL" and isinstance(decision.evidence, dict):
                extra = " " + _fmt_weak_signal(decision.evidence)
            if reason == "COOLDOWN" and isinstance(decision.evidence, dict):
                extra = " " + _fmt_cooldown(decision.evidence)
            if reason == "BULL_SHORT_BLOCKED_EARLY" and isinstance(decision.evidence, dict):
                print(
                    "[early-bull-short] "
                    f"sym={sym} ltf={decision.evidence.get('ltf_trend')} "
                    f"htf={decision.evidence.get('htf_trend')} "
                    f"reason={decision.evidence.get('early_block_reason')}"
                )
            print(
                f"[swaggy-cut] sym={sym} side={decision.side} px={last_price(cur):.6g} "
                f"level={dbg.get('level', '-')}\n"
                f"trigger={trigger_kind} strength={strength:.2f} dist_pct={_fmt_dist(dist_pct)}\n"
                f"filters: dist={decision.filters.get('dist', 0)} lvn_gap={decision.filters.get('lvn_gap', 0)} "
                f"expansion={decision.filters.get('expansion', 0)} cooldown={decision.filters.get('cooldown', 0)} "
                f"regime={decision.filters.get('regime', 0)}\n"
                f"reason={reason} gate_stage={decision.gate_stage} gate_fail={decision.gate_fail} "
                f"gate_detail={decision.gate_fail_detail}{extra}"
            )
            if _should_write_cut(cut_mode, decision):
                writer.write(_cut_event(
                    sym,
                    cfg,
                    "replay",
                    cycle,
                    decision,
                    trigger_kind,
                    strength,
                    dist_pct,
                    regime,
                    in_lvn_gap,
                    ts_ms,
                ))
        elif debug_on:
            print(
                format_debug(
                    sym,
                    last_price(cur),
                    dbg.get("regime", "-"),
                    float(dbg.get("level", 0.0) or 0.0),
                    dbg.get("vp", {}) if isinstance(dbg.get("vp"), dict) else {},
                    dbg.get("trigger", "-"),
                    dbg.get("side", "-"),
                    float(strength or 0.0),
                    float(dist_pct or 0.0),
                    1 if decision.reason_codes == ["LVN_GAP"] else 0,
                    1 if decision.reason_codes == ["EXPANSION_BAR"] else 0,
                    f"ENTRY_READY={decision.entry_ready}",
                    reason,
                )
            )
        state.update(engine._state.get(sym, {}))
        if cycle % summary_every == 0:
            _print_summary(
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
            )
            writer.write(_summary_event(
                cfg,
                "replay",
                cycle,
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
                ts_ms,
            ))
    if entry_count == 0:
        print(
            format_cut_top(
                engine.get_trigger_counts(),
                engine.get_filter_counts(),
                engine.get_gate_fail_counts(),
                engine.get_gate_stage_counts(),
            )
        )
    print(format_zone_stats(engine.get_stats(), engine.get_ready_fail_counts()))


def _scan_live(cfg: SwaggyConfig) -> None:
    ex = _make_exchange()
    cycle_cache.set_fetcher(_make_fetcher(ex))
    engine = SwaggyEngine(cfg)
    state: Dict[str, Any] = {}
    cycle = 0
    summary_every = int(os.getenv("SWAGGY_SUMMARY_EVERY", "50"))
    window_eval = int(os.getenv("SWAGGY_WINDOW_EVAL", "500"))
    window_reason = int(os.getenv("SWAGGY_WINDOW_REASON", "2000"))
    cut_mode = _cut_mode()
    debug_on = os.getenv("SWAGGY_DEBUG", "0").strip() == "1"
    date_tag = time.strftime("%Y%m%d")
    log_path = os.path.join(ROOT_DIR, "logs", "swaggy", f"swaggy_live_{date_tag}.jsonl")
    writer = JsonlWriter(log_path)
    reason_window = deque(maxlen=window_reason)
    gate_fail_window = deque(maxlen=window_reason)
    gate_stage_window = deque(maxlen=window_reason)
    ready_fail_window = deque(maxlen=window_reason)
    trigger_window = deque(maxlen=window_eval)
    strength_window = deque(maxlen=window_eval)
    funnel_window = deque(maxlen=window_reason)
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN_SWAGGY", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID_SWAGGY", "").strip()
    while True:
        cycle += 1
        engine.begin_cycle()
        tickers = ex.fetch_tickers()
        ctx = EngineContext(exchange=ex, state={"_tickers": tickers}, now_ts=time.time(), logger=print, config=cfg)
        universe = engine.build_universe(ctx)
        scan_line = f"[swaggy-scan] mode=live tf={cfg.tf_ltf} cycle={cycle} ts={now_ms()} syms={len(tickers)} evaluated={len(universe)}"
        if len(universe) < len(tickers):
            scan_line = f"{scan_line} batch_limit={len(universe)}"
        print(scan_line)
        entry_count = 0
        funnel = Counter({
            "total": 0,
            "in_pos": 0,
            "cooldown": 0,
            "no_level": 0,
            "no_touch": 0,
            "touched": 0,
            "no_trigger": 0,
            "trigger": 0,
            "filter_fail": 0,
            "filters_pass": 0,
            "entry_ready": 0,
        })
        for sym in universe:
            decision = engine.on_tick(ctx, sym)
            if not decision:
                continue
            dbg = decision.debug or {}
            trigger_kind = dbg.get("trigger", "-")
            strength = dbg.get("strength", 0.0) if isinstance(dbg.get("strength"), (int, float)) else 0.0
            level = dbg.get("level", "-")
            dist_pct = dbg.get("dist_pct", 0.0) if isinstance(dbg.get("dist_pct"), (int, float)) else 0.0
            regime = dbg.get("regime", "-")
            in_lvn_gap = 1 if decision.filters.get("lvn_gap") == 1 else 0
            trigger_debug = (dbg.get("trigger_debug") or {}) if isinstance(dbg.get("trigger_debug"), dict) else {}
            if debug_on and trigger_debug:
                print(
                    "[swaggy-trigger-debug] "
                    f"sym={sym} side={decision.side} px={decision.debug.get('px', 0.0):.6g} level={trigger_debug.get('level', '-')}\n"
                    f"trigger_candidate="
                    f"reclaim={trigger_debug.get('reclaim_ok', 0)} "
                    f"rejection={trigger_debug.get('rejection_ok', 0)} "
                    f"sweep={trigger_debug.get('sweep_ok', 0)} "
                    f"retest={trigger_debug.get('retest_ok', 0)}\n"
                    f"candle_ref={trigger_debug.get('candle_ref', '-')}"
                    f" c1_close={trigger_debug.get('c1_close')} c2_close={trigger_debug.get('c2_close')} "
                    f"c3_close={trigger_debug.get('c3_close')}\n"
                    f"reclaim_ok={trigger_debug.get('reclaim_ok', 0)} "
                    f"rejection_ok={trigger_debug.get('rejection_ok', 0)} "
                    f"sweep_ok={trigger_debug.get('sweep_ok', 0)}\n"
                    f"wick_ratio={trigger_debug.get('wick_ratio')} vol={trigger_debug.get('vol')} "
                    f"vol_sma={trigger_debug.get('vol_sma')}"
                )
            if decision.entry_ready:
                entry_count += 1
                bucket = _strength_bucket(float(strength))
                line = (
                    f"[swaggy-entry] sym={sym} side={decision.side} px={decision.debug.get('px', 0.0):.6g} level={level}\n"
                    f"trigger={trigger_kind} strength={float(strength):.2f} bucket={bucket} "
                    f"dist_pct={_fmt_dist(dist_pct)} regime={regime} in_lvn_gap={in_lvn_gap}\n"
                    f"entry={decision.entry_px:.6g} sl={decision.sl_px:.6g} "
                    f"tp1={decision.tp1_px} tp2={decision.tp2_px} reason=ENTRY_READY"
                )
                print(line)
                _send_telegram(
                    bot_token,
                    chat_id,
                    (
                        f"[SWAGGY] ENTRY_READY\n"
                        f"sym={sym}\nside={decision.side}\n"
                        f"level={level}\ntrigger={trigger_kind}\nstrength={strength}\n"
                        f"entry={decision.entry_px:.6g}\nsl={decision.sl_px:.6g}\n"
                        f"tp1={decision.tp1_px}\n"
                        f"tp2={decision.tp2_px}\n"
                    ),
                )
                writer.write(_entry_event(
                    sym,
                    cfg,
                    "live",
                    cycle,
                    decision,
                    trigger_kind,
                    float(strength),
                    bucket,
                    dist_pct,
                    regime,
                    in_lvn_gap,
                    now_ms(),
                ))
            elif decision and debug_on:
                dbg = decision.debug or {}
                print(
                    format_debug(
                        sym,
                        float(dbg.get("px", 0.0) or 0.0) or 0.0,
                        dbg.get("regime", "-"),
                        float(dbg.get("level", 0.0) or 0.0),
                        dbg.get("vp", {}) if isinstance(dbg.get("vp"), dict) else {},
                        dbg.get("trigger", "-"),
                        dbg.get("side", "-"),
                        float(dbg.get("strength", 0.0) or 0.0),
                        float(dbg.get("dist_pct", 0.0) or 0.0),
                        1 if decision.reason_codes == ["LVN_GAP"] else 0,
                        1 if decision.reason_codes == ["EXPANSION_BAR"] else 0,
                        f"ENTRY_READY={decision.entry_ready}",
                        _decision_reason(decision),
                    )
                )
            if decision and not decision.entry_ready and (debug_on or _should_log_cut(cut_mode, decision)):
                dbg = decision.debug or {}
                trigger_kind = dbg.get("trigger", "-")
                strength = float(dbg.get("strength", 0.0) or 0.0)
                dist_pct = float(dbg.get("dist_pct", 0.0) or 0.0)
                reason = _decision_reason(decision)
                extra = ""
                if reason == "NO_TRIGGER" and isinstance(decision.evidence, dict):
                    extra = " " + _fmt_no_trigger(decision.evidence)
                print(
                    f"[swaggy-cut] sym={sym} side={decision.side} px={decision.debug.get('px', 0.0):.6g} "
                    f"level={dbg.get('level', '-')}\n"
                    f"trigger={trigger_kind} strength={strength:.2f} dist_pct={_fmt_dist(dist_pct)}\n"
                    f"filters: dist={decision.filters.get('dist', 0)} lvn_gap={decision.filters.get('lvn_gap', 0)} "
                    f"expansion={decision.filters.get('expansion', 0)} cooldown={decision.filters.get('cooldown', 0)} "
                    f"regime={decision.filters.get('regime', 0)}\n"
                    f"reason={reason} gate_stage={decision.gate_stage} gate_fail={decision.gate_fail} "
                    f"gate_detail={decision.gate_fail_detail}{extra}"
                )
            _accumulate_funnel(funnel, decision, (decision.debug or {}).get("trigger", "-"))
            reason = _decision_reason(decision)
            reason_window.append(reason)
            gate_fail_window.append(decision.gate_fail or reason)
            gate_stage_window.append(decision.gate_stage or "")
            if not decision.entry_ready and decision.gate_fail:
                ready_fail_window.append(decision.gate_fail)
            funnel_window.append(_funnel_counts(decision, (decision.debug or {}).get("trigger", "-")))
            if trigger_kind in {"SWEEP", "RECLAIM", "REJECTION", "RETEST"}:
                trigger_window.append(trigger_kind)
                strength_window.append(float(strength))
            if not decision.entry_ready and _should_write_cut(cut_mode, decision):
                writer.write(_cut_event(
                    sym,
                    cfg,
                    "live",
                    cycle,
                    decision,
                    trigger_kind,
                    float(strength),
                    float(dist_pct),
                    regime,
                    1 if decision.filters.get("lvn_gap") == 1 else 0,
                    now_ms(),
                ))
            state.update(engine._state.get(sym, {}))
        print(
            "[swaggy-funnel] "
            f"total={funnel['total']} in_pos={funnel['in_pos']} cooldown={funnel['cooldown']} "
            f"no_level={funnel['no_level']} no_touch={funnel['no_touch']} "
            f"touched={funnel['touched']} no_trigger={funnel['no_trigger']} "
            f"trigger={funnel['trigger']} filter_fail={funnel['filter_fail']} "
            f"filters_pass={funnel['filters_pass']} entry_ready={funnel['entry_ready']}"
        )
        if cycle % summary_every == 0:
            _print_summary(
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
            )
            writer.write(_summary_event(
                cfg,
                "live",
                cycle,
                reason_window,
                trigger_window,
                strength_window,
                funnel_window,
                gate_fail_window,
                gate_stage_window,
                ready_fail_window,
                window_eval,
                window_reason,
                now_ms(),
            ))
        time.sleep(5)


def main() -> None:
    load_env()
    mode = os.getenv("SWAGGY_MODE", "replay").strip().lower()
    sym = os.getenv("SWAGGY_SYMBOL", "BTC/USDT:USDT").strip()
    cfg = SwaggyConfig()
    if mode == "replay":
        _replay_symbol(sym, cfg)
    else:
        _scan_live(cfg)


def last_price(row) -> float:
    try:
        return float(row["close"])
    except Exception:
        return 0.0


def _strength_bucket(val: float) -> str:
    if val < 0.40:
        return "weak"
    if val < 0.55:
        return "mid"
    return "strong"


def _decision_reason(decision) -> str:
    if decision.entry_ready:
        return "ENTRY_READY"
    return (decision.reason_codes or ["-"])[0]


def _fmt_dist(val: float) -> str:
    if not isinstance(val, (int, float)):
        return "0"
    return f"{val:.6f}"


def _cut_mode() -> int:
    raw = os.getenv("SWAGGY_LOG_CUTS", "1").strip()
    try:
        val = int(raw)
    except Exception:
        val = 0
    if val < 0:
        return 0
    if val > 2:
        return 2
    return val


def _is_filter_cut(decision) -> bool:
    reason = _decision_reason(decision)
    if reason in {
        "LVN_GAP",
        "DIST_FAIL",
        "REGIME_BLOCK",
        "EXPANSION_BAR",
        "OTHER_FILTER_FAIL",
        "WEAK_SIGNAL",
        "SWEEP_OBSERVE",
        "WEAK_SWEEP_NO_CONFIRM",
        "WEAK_SWEEP_CONF_TOO_WEAK",
        "SHORT_BULL_BLOCK",
        "REENTRY_BLOCK",
        "SHORT_DIST_TOO_FAR",
        "RANGE_LONG_GATE",
        "ATLAS_BLOCK",
        "ATLAS_SIDE_BLOCK",
        "ATLAS_LONG_NO_EXCEPTION",
        "ATLAS_LONG_BLOCK_QUALITY",
        "ATLAS_SHORT_BLOCK_QUALITY",
    }:
        return True
    return any(decision.filters.values())


def _should_write_cut(cut_mode: int, decision) -> bool:
    if cut_mode == 0:
        return False
    if cut_mode == 2:
        return True
    return _is_filter_cut(decision)


def _should_log_cut(cut_mode: int, decision) -> bool:
    return _should_write_cut(cut_mode, decision)


def _funnel_counts(decision, trigger_kind: str) -> Dict[str, int]:
    funnel = {
        "total": 1,
        "in_pos": 0,
        "cooldown": 0,
        "no_level": 0,
        "no_touch": 0,
        "touched": 0,
        "no_trigger": 0,
        "trigger": 0,
        "filter_fail": 0,
        "filters_pass": 0,
        "entry_ready": 0,
    }
    reason = (decision.reason_codes or ["-"])[0]
    if reason == "IN_POSITION":
        funnel["in_pos"] = 1
        return funnel
    if reason == "COOLDOWN":
        funnel["cooldown"] = 1
        return funnel
    if reason == "NO_LEVEL":
        funnel["no_level"] = 1
        return funnel
    if reason == "NO_TOUCH":
        funnel["no_touch"] = 1
        return funnel
    if reason == "NO_TRIGGER":
        funnel["no_trigger"] = 1
        funnel["touched"] = 1
        return funnel
    funnel["touched"] = 1
    if trigger_kind and trigger_kind != "-":
        funnel["trigger"] = 1
    if decision.entry_ready:
        funnel["entry_ready"] = 1
        funnel["filters_pass"] = 1
        return funnel
    funnel["filter_fail"] = 1
    return funnel


def _accumulate_funnel(funnel: Counter, decision, trigger_kind: str) -> None:
    counts = _funnel_counts(decision, trigger_kind)
    for k, v in counts.items():
        funnel[k] += v


def _print_summary(
    reason_window,
    trigger_window,
    strength_window,
    funnel_window,
    gate_fail_window,
    gate_stage_window,
    ready_fail_window,
    window_eval,
    window_reason,
) -> None:
    reason_counts = Counter(reason_window)
    filtered_counts = Counter({k: v for k, v in reason_counts.items() if k != "ENTRY_READY"})
    top5 = ",".join([f"{k}:{v}" for k, v in filtered_counts.most_common(5)])
    print(
        "[swaggy-reason-top] "
        f"window={window_reason} eval={len(reason_window)} filter_fail="
        f"{sum(filtered_counts.get(r, 0) for r in ['LVN_GAP','DIST_FAIL','REGIME_BLOCK','EXPANSION_BAR','COOLDOWN','OTHER_FILTER_FAIL'])} "
        f"top5={top5}"
    )
    gate_fail_counts = Counter([g for g in gate_fail_window if g and g != "ENTRY_READY"])
    gate_stage_counts = Counter([g for g in gate_stage_window if g])
    ready_fail_counts = Counter([g for g in ready_fail_window if g])
    gate_fail_top = " | ".join([f"{k}={v}" for k, v in gate_fail_counts.most_common(6)])
    gate_stage_top = " ".join([f"{k}={v}" for k, v in gate_stage_counts.most_common(6)])
    ready_fail_top = " | ".join([f"{k}={v}" for k, v in ready_fail_counts.most_common(6)])
    if gate_fail_top:
        print(f"[swaggy-gate-fail-top] window={window_reason} {gate_fail_top}")
    if gate_stage_top:
        print(f"[swaggy-gate-stage-hist] window={window_reason} {gate_stage_top}")
    if ready_fail_top:
        print(f"[swaggy-ready-fail-top] window={window_reason} {ready_fail_top}")
    trigger_counts = Counter(trigger_window)
    print(
        "[swaggy-trigger-dist] "
        f"window={window_eval} entry={len(trigger_window)} "
        f"RECLAIM={trigger_counts.get('RECLAIM', 0)} "
        f"REJECTION={trigger_counts.get('REJECTION', 0)} "
        f"SWEEP={trigger_counts.get('SWEEP', 0)} "
        f"RETEST={trigger_counts.get('RETEST', 0)}"
    )
    hist = _strength_hist(strength_window)
    print(
        "[swaggy-strength-hist] "
        f"window={window_eval} entry={len(strength_window)}\n"
        f"0.00-0.39={hist['b1']} 0.40-0.64={hist['b2']} "
        f"0.65-0.79={hist['b3']} 0.80-1.00={hist['b4']} "
        f"avg={hist['avg']:.2f} med={hist['med']:.2f}"
    )


def _strength_hist(vals) -> Dict[str, float]:
    if not vals:
        return {"b1": 0, "b2": 0, "b3": 0, "b4": 0, "avg": 0.0, "med": 0.0}
    sorted_vals = sorted(vals)
    b1 = len([v for v in sorted_vals if v < 0.40])
    b2 = len([v for v in sorted_vals if 0.40 <= v < 0.65])
    b3 = len([v for v in sorted_vals if 0.65 <= v < 0.80])
    b4 = len([v for v in sorted_vals if v >= 0.80])
    avg = sum(sorted_vals) / len(sorted_vals)
    med = sorted_vals[len(sorted_vals) // 2]
    return {"b1": b1, "b2": b2, "b3": b3, "b4": b4, "avg": avg, "med": med}


def _entry_event(
    sym: str,
    cfg: SwaggyConfig,
    mode: str,
    cycle: int,
    decision,
    trigger_kind: str,
    strength: float,
    bucket: str,
    dist_pct: float,
    regime: str,
    in_lvn_gap: int,
    ts_ms: int,
) -> Dict[str, Any]:
    size_mult = 1.0
    if isinstance(decision.evidence, dict):
        size_mult = float(decision.evidence.get("size_mult", 1.0) or 1.0)
    return {
        "ts": ts_ms,
        "event": "ENTRY_SIGNAL",
        "engine": "swaggy",
        "mode": mode,
        "cycle": cycle,
        "sym": sym,
        "tf": cfg.tf_ltf,
        "side": decision.side,
        "px": decision.debug.get("px"),
        "level": decision.debug.get("level"),
        "trigger": trigger_kind,
        "strength": strength,
        "strength_bucket": bucket,
        "dist_pct": _fmt_dist(dist_pct),
        "regime": regime,
        "in_lvn_gap": in_lvn_gap,
        "entry": decision.entry_px,
        "sl": decision.sl_px,
        "tp1": decision.tp1_px,
        "tp2": decision.tp2_px,
        "order": {
            "entry_px": decision.entry_px,
            "sl_px": decision.sl_px,
            "tp1_px": decision.tp1_px,
            "qty": decision.evidence.get("order_qty") if isinstance(decision.evidence, dict) else None,
            "usdt": decision.evidence.get("order_usdt") if isinstance(decision.evidence, dict) else None,
            "size_mult": size_mult,
            "client_tag": decision.evidence.get("client_tag") if isinstance(decision.evidence, dict) else "swaggy",
        },
        "reason": "ENTRY_READY",
        "gate_stage": decision.gate_stage,
        "gate_fail": decision.gate_fail,
        "gate_fail_detail": decision.gate_fail_detail,
        "filters": decision.filters,
        "evidence": decision.evidence,
    }


def _cut_event(
    sym: str,
    cfg: SwaggyConfig,
    mode: str,
    cycle: int,
    decision,
    trigger_kind: str,
    strength: float,
    dist_pct: float,
    regime: str,
    in_lvn_gap: int,
    ts_ms: int,
) -> Dict[str, Any]:
    reason = (decision.reason_codes or ["-"])[0]
    return {
        "ts": ts_ms,
        "event": "CUT_EVENT",
        "engine": "swaggy",
        "mode": mode,
        "cycle": cycle,
        "sym": sym,
        "tf": cfg.tf_ltf,
        "side": decision.side,
        "px": decision.debug.get("px"),
        "level": decision.debug.get("level"),
        "trigger": trigger_kind,
        "strength": strength,
        "dist_pct": _fmt_dist(dist_pct),
        "regime": regime,
        "in_lvn_gap": in_lvn_gap,
        "reason": reason,
        "gate_stage": decision.gate_stage,
        "gate_fail": decision.gate_fail,
        "gate_fail_detail": decision.gate_fail_detail,
        "filters": _normalize_filters(decision.filters),
        "evidence": decision.evidence,
    }


def _summary_event(
    cfg: SwaggyConfig,
    mode: str,
    cycle: int,
    reason_window,
    trigger_window,
    strength_window,
    funnel_window,
    gate_fail_window,
    gate_stage_window,
    ready_fail_window,
    window_eval: int,
    window_reason: int,
    ts_ms: int,
) -> Dict[str, Any]:
    reason_counts = Counter(reason_window)
    filtered_counts = Counter({k: v for k, v in reason_counts.items() if k != "ENTRY_READY"})
    trigger_counts = Counter(trigger_window)
    hist = _strength_hist(strength_window)
    funnel = Counter()
    for row in funnel_window:
        funnel.update(row)
    gate_fail_counts = Counter([g for g in gate_fail_window if g and g != "ENTRY_READY"])
    gate_stage_counts = Counter([g for g in gate_stage_window if g])
    ready_fail_counts = Counter([g for g in ready_fail_window if g])
    return {
        "ts": ts_ms,
        "event": "SUMMARY",
        "engine": "swaggy",
        "mode": mode,
        "cycle": cycle,
        "tf": cfg.tf_ltf,
        "window_eval": window_eval,
        "window_reason": window_reason,
        "funnel": dict(funnel),
        "reason_top": dict(filtered_counts.most_common(5)),
        "gate_fail_top": dict(gate_fail_counts.most_common(8)),
        "gate_stage_hist": dict(gate_stage_counts),
        "ready_fail_top": dict(ready_fail_counts.most_common(8)),
        "trigger_dist": dict(trigger_counts),
        "strength_hist": hist,
    }


def _normalize_filters(filters: Dict[str, int]) -> Dict[str, int]:
    return {
        "dist": int(filters.get("dist", 0)),
        "lvn_gap": int(filters.get("lvn_gap", 0)),
        "expansion": int(filters.get("expansion", 0)),
        "cooldown": int(filters.get("cooldown", 0)),
        "regime": int(filters.get("regime", 0)),
        "weak_signal": int(filters.get("weak_signal", 0)),
        "sweep_confirm": int(filters.get("sweep_confirm", 0)),
        "short_bull_block": int(filters.get("short_bull_block", 0)),
        "reentry_block": int(filters.get("reentry_block", 0)),
        "short_dist": int(filters.get("short_dist", 0)),
    }


def _fmt_no_trigger(evidence: Dict[str, Any]) -> str:
    cand = evidence
    return (
        "no_trigger "
        f"sym={cand.get('sym')} tf={cand.get('tf')} last_close={cand.get('close')} "
        f"touch_count={cand.get('touch_count')} "
        f"trigger_candidates={cand.get('trigger_candidates')} "
        f"level={cand.get('level_price')} band=[{cand.get('band_low')},{cand.get('band_high')}] "
        f"tol={cand.get('tol')} dist_pct={cand.get('dist_pct_to_level')} "
        f"reclaim_str={cand.get('reclaim_strength')} min={cand.get('reclaim_min_required')} "
        f"fail={cand.get('reclaim_fail_reason')} "
        f"retest_str={cand.get('retest_strength')} min={cand.get('retest_min_required')} "
        f"fail={cand.get('retest_fail_reason')} "
        f"rejection_str={cand.get('rejection_strength')} min={cand.get('rejection_min_required')} "
        f"fail={cand.get('rejection_fail_reason')} "
        f"sweep_str={cand.get('sweep_strength')} min={cand.get('sweep_min_required')} "
        f"fail={cand.get('sweep_fail_reason')} "
        f"confirm={cand.get('has_confirm')} conf_type={cand.get('confirm_type')} "
        f"best={cand.get('best_candidate')} best_strength={cand.get('best_strength')} "
        f"best_fail={cand.get('best_fail_reason')} why_none={cand.get('why_none')} "
        f"no_trigger_fail={cand.get('no_trigger_fail')}"
    )


def _fmt_sweep_observe(evidence: Dict[str, Any]) -> str:
    return (
        "sweep_observe "
        f"sweep_dir={evidence.get('sweep_direction')} sweep_side={evidence.get('sweep_side')} "
        f"sweep_ts={evidence.get('sweep_ts')} confirm_deadline_ts={evidence.get('confirm_deadline_ts')} "
        f"confirm_reclaim_ok={evidence.get('confirm_reclaim_ok')} "
        f"confirm_body_ok={evidence.get('confirm_body_ok')} "
        f"confirm_wick_ok={evidence.get('confirm_wick_ok')} "
        f"confirm_vol_ok={evidence.get('confirm_vol_ok')} "
        f"confirm_window_ok={evidence.get('confirm_window_ok')} "
        f"sweep_level_price={evidence.get('sweep_level_price')} "
        f"sweep_wick_depth_pct={evidence.get('sweep_wick_depth_pct')} "
        f"reclaim_close_dist_pct={evidence.get('reclaim_close_dist_pct')} "
        f"confirm_window_n={evidence.get('confirm_window_n')} elapsed_n={evidence.get('elapsed_n')} "
        f"vol_ratio={evidence.get('vol_ratio')} rsi_slope={evidence.get('rsi_slope')} "
        f"structure_ok={evidence.get('structure_ok')} "
        f"sweep_fail_reason={evidence.get('sweep_fail_reason')}"
    )


def _fmt_weak_signal(evidence: Dict[str, Any]) -> str:
    return (
        "weak_signal "
        f"strength={evidence.get('strength')} min_required={evidence.get('min_strength_required')} "
        f"components={evidence.get('strength_components')}"
    )


def _fmt_cooldown(evidence: Dict[str, Any]) -> str:
    return (
        "cooldown "
        f"key={evidence.get('cooldown_key')} remaining_sec={evidence.get('cooldown_remaining_sec')} "
        f"until={evidence.get('cooldown_until')} last_entry_ts={evidence.get('last_entry_ts')}"
    )


if __name__ == "__main__":
    main()
