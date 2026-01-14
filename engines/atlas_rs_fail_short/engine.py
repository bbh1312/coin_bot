from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

import cycle_cache
from atlas_test.atlas_engine import compute_atlas_local, _score_from_atlas, atlas_swaggy_cfg, compute_global_gate
from engines.base import BaseEngine, EngineContext, Signal
from engines.atlas_rs_fail_short.config import AtlasRsFailShortConfig


@dataclass
class AtlasRsFailShortSignal:
    symbol: str
    entry_ready: bool
    entry_price: Optional[float]
    size_mult: float
    meta: Dict[str, Any]


class AtlasRsFailShortEngine(BaseEngine):
    name: str = "atlas_rs_fail_short"

    def __init__(self, config: Optional[AtlasRsFailShortConfig] = None):
        self.config = config or AtlasRsFailShortConfig()

    def build_universe(self, ctx: EngineContext) -> list[str]:
        tickers = ctx.state.get("_tickers")
        if not isinstance(tickers, dict):
            return []
        min_qv = 8_000_000.0
        exclude_top_abs = 20
        exclude_top_qv = 20
        top_by_qv = 30
        pct_map = {}
        qv_map = {}
        for sym, t in tickers.items():
            if not t:
                continue
            qv = t.get("quoteVolume")
            pct = t.get("percentage")
            if qv is None or pct is None:
                continue
            try:
                qv_val = float(qv)
                pct_val = float(pct)
            except Exception:
                continue
            if qv_val < min_qv:
                continue
            qv_map[sym] = qv_val
            pct_map[sym] = pct_val
        if not qv_map:
            return []
        sorted_abs = sorted(pct_map.items(), key=lambda x: abs(x[1]), reverse=True)
        excluded = {sym for sym, _ in sorted_abs[:exclude_top_abs]}
        candidates = [(sym, qv) for sym, qv in qv_map.items() if sym not in excluded]
        candidates.sort(key=lambda x: x[1], reverse=True)
        if exclude_top_qv > 0:
            candidates = candidates[exclude_top_qv:]
        return [sym for sym, _ in candidates[:top_by_qv]]

    def on_tick(self, ctx: EngineContext, symbol: str) -> Optional[Signal]:
        sig = self.evaluate_symbol(ctx, symbol)
        if not sig or not sig.entry_ready or sig.entry_price is None:
            return None
        return Signal(
            symbol=symbol,
            side="SHORT",
            pattern="ENTRY_READY",
            entry_price=float(sig.entry_price),
            sl=0.0,
            targets=[],
            meta=sig.meta,
        )

    def evaluate_symbol(self, ctx: EngineContext, symbol: str) -> Optional[AtlasRsFailShortSignal]:
        cfg = self.config
        state = ctx.state if isinstance(ctx.state, dict) else {}
        now_ts = float(ctx.now_ts or 0.0) or pd.Timestamp.utcnow().timestamp()

        bucket = state.setdefault("_atlas_rs_fail_short", {})
        rec = bucket.get(symbol) if isinstance(bucket.get(symbol), dict) else {}
        rec.setdefault("state", "IDLE")
        rec.setdefault("last_bar_ts", 0)
        rec.setdefault("pullback_ts", 0)
        rec.setdefault("pullback_high", None)
        rec.setdefault("pullback_id", None)
        rec.setdefault("pullback_anchor", None)
        rec.setdefault("last_swing_high", None)
        rec.setdefault("cooldown_until_ts", 0)
        rec.setdefault("last_entry_pullback_id", None)
        rec.setdefault("last_entry_ts", 0)
        rec.setdefault("fade_id", None)
        rec.setdefault("last_entry_fade_id", None)
        state_transition = ""

        def _set_state(new_state: str) -> None:
            nonlocal state_transition
            old_state = rec.get("state")
            if old_state != new_state:
                state_transition = f"{old_state}->{new_state}"
                rec["state"] = new_state

        def _emit_log(
            entry_ready: int,
            block_reason: str,
            atlas_data: Optional[dict],
            htf_ok: Optional[bool],
            ltf_pullback: int,
            fail_confirm: int,
            confirm_type: str,
            size_mult: float,
        ) -> None:
            logger = getattr(ctx, "logger", None)
            if not logger:
                return
            atlas_data = atlas_data or {}
            atlas_parts = _score_parts(atlas_data, atlas_swaggy_cfg)
            state_label = _atlas_state(atlas_data)
            sym_short = symbol.replace("/USDT:USDT", "").replace(":USDT", "")
            line = (
                "[{ts}] [atlas-test] idx=1 sym={sym} state={state} score={score} "
                "regime={regime} dir={direction} part=R{sr}/RS{srsi}/I{sind}/V{svol} "
                "rs={rs} rs_z={rsz} rs_slow={rsl} rs_z_slow={rszl} "
                "corr={corr} beta={beta} corr_slow={corrs} beta_slow={betas} "
                "vol={vol} "
                "engine=atlas_rs_fail_short htf_ok={htf} ltf_pullback={pb} fail_confirm={fc} "
                "confirm={confirm} entry_ready={entry} size_mult={mult} block_reason={reason}"
            ).format(
                ts=_fmt_now_kst(),
                sym=sym_short,
                state=state_label,
                score=int(atlas_data.get("score") or 0),
                regime=atlas_data.get("regime") or "n/a",
                direction=atlas_data.get("dir") or "n/a",
                sr=atlas_parts.get("score_regime"),
                srsi=atlas_parts.get("score_rs"),
                sind=atlas_parts.get("score_indep"),
                svol=atlas_parts.get("score_vol"),
                rs=_fmt_val_atlas(atlas_data.get("rs")),
                rsz=_fmt_val_atlas(atlas_data.get("rs_z")),
                rsl=_fmt_val_atlas(atlas_data.get("rs_slow")),
                rszl=_fmt_val_atlas(atlas_data.get("rs_z_slow")),
                corr=_fmt_val_atlas(atlas_data.get("corr")),
                beta=_fmt_val_atlas(atlas_data.get("beta")),
                corrs=_fmt_val_atlas(atlas_data.get("corr_slow")),
                betas=_fmt_val_atlas(atlas_data.get("beta_slow")),
                vol=_fmt_val_atlas(atlas_data.get("vol"), "{:.2f}"),
                htf=_fmt_flag(htf_ok),
                pb=ltf_pullback,
                fc=fail_confirm,
                confirm=confirm_type or "n/a",
                entry=entry_ready,
                mult=_fmt_val_atlas(size_mult),
                reason=block_reason or "n/a",
            )
            logger(line)

        def _blocked(reason: str, atlas_data: Optional[dict] = None, htf_ok: Optional[bool] = None) -> AtlasRsFailShortSignal:
            _emit_log(
                entry_ready=0,
                block_reason=reason,
                atlas_data=atlas_data,
                htf_ok=htf_ok,
                ltf_pullback=0,
                fail_confirm=0,
                confirm_type="",
                size_mult=1.0,
            )
            meta = {
                "block_reason": reason,
                "atlas": atlas_data or {},
                "state_transition": state_transition,
                "pullback_id": rec.get("pullback_id"),
            }
            return AtlasRsFailShortSignal(
                symbol=symbol,
                entry_ready=False,
                entry_price=None,
                size_mult=1.0,
                meta=meta,
            )

        st = state.get(symbol, {}) if isinstance(state.get(symbol), dict) else {}
        if st.get("in_pos"):
            _set_state("IDLE")
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            return _blocked("IN_POSITION")

        # 공통 재진입 쿨다운만 사용

        min_bars_ready = max(cfg.ema_len + 5, 40)
        cached_hit = False
        try:
            cached_hit = cycle_cache.get_raw(symbol, cfg.ltf_tf) is not None
        except Exception:
            cached_hit = False
        df_ltf = cycle_cache.get_df(symbol, cfg.ltf_tf, cfg.ltf_limit)
        if df_ltf is None or df_ltf.empty or len(df_ltf) < min_bars_ready:
            _blocked(f"DATA_MISSING len={0 if df_ltf is None else len(df_ltf)} limit={cfg.ltf_limit} cached={1 if cached_hit else 0}")
            df_retry = None
            try:
                df_retry = cycle_cache.get_df(symbol, cfg.ltf_tf, cfg.ltf_limit, force=True)
            except Exception:
                df_retry = None
            if df_retry is None or df_retry.empty or len(df_retry) < min_bars_ready:
                return _blocked("DATA_MISSING")
            df_ltf = df_retry
        df_ltf = df_ltf.iloc[:-1]
        if df_ltf.empty or len(df_ltf) < min_bars_ready:
            _blocked(f"DATA_MISSING len={len(df_ltf)} limit={cfg.ltf_limit} cached={1 if cached_hit else 0}")
            df_retry = None
            try:
                df_retry = cycle_cache.get_df(symbol, cfg.ltf_tf, cfg.ltf_limit, force=True)
            except Exception:
                df_retry = None
            if df_retry is None or df_retry.empty or len(df_retry) < min_bars_ready:
                return _blocked("DATA_MISSING")
            df_ltf = df_retry.iloc[:-1]
        last_ts = int(df_ltf["ts"].iloc[-1])
        if last_ts == rec.get("last_bar_ts"):
            return None
        rec["last_bar_ts"] = last_ts

        gate = compute_global_gate()
        atlas_local = compute_atlas_local(symbol, gate, atlas_swaggy_cfg)
        score = _score_from_atlas(gate, atlas_local, atlas_swaggy_cfg)
        atlas_data = {
            "regime": gate.get("regime"),
            "dir": _dir_or_none(atlas_local.get("symbol_direction")),
            "score": _num_or_none(score),
            "rs": _num_or_none(atlas_local.get("rs")),
            "rs_z": _num_or_none(atlas_local.get("rs_z")),
            "rs_slow": _num_or_none(atlas_local.get("rs_slow")),
            "rs_z_slow": _num_or_none(atlas_local.get("rs_z_slow")),
            "corr": _num_or_none(atlas_local.get("corr")),
            "beta": _num_or_none(atlas_local.get("beta")),
            "corr_slow": _num_or_none(atlas_local.get("corr_slow")),
            "beta_slow": _num_or_none(atlas_local.get("beta_slow")),
            "vol": _num_or_none(atlas_local.get("vol_ratio")),
            "allow_long": atlas_local.get("allow_long"),
            "allow_short": atlas_local.get("allow_short"),
        }
        snapshot = state.setdefault("_atlas_rs_fail_short_snapshot", {})
        if isinstance(snapshot, dict):
            snapshot[symbol] = {"dir": atlas_data.get("dir"), "ts": last_ts}
        if not atlas_data.get("dir"):
            _set_state("IDLE")
            bucket[symbol] = rec
            return _blocked("ATLAS_DIR_MISSING", atlas_data)
        if not atlas_data.get("regime") or atlas_data.get("score") is None:
            _set_state("IDLE")
            bucket[symbol] = rec
            return _blocked("ATLAS_MISSING_REQUIRED", atlas_data)
        score_val = float(atlas_data["score"])
        if atlas_data.get("regime") == "bull_extreme" and score_val >= cfg.bull_extreme_score_max:
            _set_state("IDLE")
            bucket[symbol] = rec
            return _blocked("REGIME_BULL_EXTREME", atlas_data)
        regime = atlas_data.get("regime")
        bull_soft_allow = regime == "bull" and score_val < 80
        if (not bull_soft_allow) and regime not in cfg.allow_regimes:
            _set_state("IDLE")
            bucket[symbol] = rec
            return _blocked("REGIME_NOT_ALLOWED", atlas_data)
        if not (atlas_data.get("dir") == "BEAR" or score_val <= cfg.dir_score_max):
            _set_state("IDLE")
            bucket[symbol] = rec
            return _blocked("ATLAS_DIR_SCORE_BLOCK", atlas_data)

        close_series = df_ltf["close"].astype(float)
        high_series = df_ltf["high"].astype(float)
        low_series = df_ltf["low"].astype(float)
        open_series = df_ltf["open"].astype(float)
        vol_series = df_ltf["volume"].astype(float)
        min_bars = max(
            cfg.ema_len,
            cfg.atr_len + 2,
            cfg.rsi_len + 2,
            cfg.bb_len + 2,
            cfg.macd_slow + cfg.macd_signal + 2,
            cfg.vol_sma_len + 2,
        )
        if len(df_ltf) < (min_bars + 5):
            return _blocked("DATA_MISSING", atlas_data)

        ema20 = _ema(close_series, cfg.ema_len)
        atr_ltf = _atr(df_ltf, cfg.atr_len)
        rsi_series = _rsi_wilder(close_series, cfg.rsi_len)
        macd_hist = _macd_hist(close_series, cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
        bb_vals = _bollinger(close_series, cfg.bb_len, cfg.bb_mult)
        quote_vol = vol_series * close_series
        vol_sma = _sma(quote_vol, cfg.vol_sma_len)
        if (
            ema20 is None
            or atr_ltf is None
            or rsi_series is None
            or macd_hist is None
            or bb_vals is None
            or vol_sma is None
        ):
            return _blocked("DATA_MISSING", atlas_data)

        ema20_now = float(ema20.iloc[-1])
        atr_now = float(atr_ltf.iloc[-1])
        rsi_now = float(rsi_series.iloc[-1])
        rsi_prev = float(rsi_series.iloc[-2]) if len(rsi_series) > 1 else rsi_now
        hist_now = float(macd_hist.iloc[-1])
        hist_prev = float(macd_hist.iloc[-2]) if len(macd_hist) > 1 else hist_now
        bb_mid, bb_upper, bb_lower = bb_vals
        bb_width = bb_upper - bb_lower
        vol_sma_now = float(vol_sma.iloc[-1])
        if any(pd.isna(v) for v in (ema20_now, atr_now, rsi_now, rsi_prev, hist_now, hist_prev, bb_upper, bb_lower, vol_sma_now)):
            return _blocked("DATA_MISSING", atlas_data)
        if bb_width <= 0:
            return _blocked("DATA_MISSING", atlas_data)

        last = df_ltf.iloc[-1]
        open_now = float(last["open"])
        high_now = float(last["high"])
        low_now = float(last["low"])
        close_now = float(last["close"])
        wick_ratio = _upper_wick_ratio(open_now, high_now, low_now, close_now)

        fade_id = f"{last_ts}:fade"
        rec["fade_id"] = fade_id
        rec["pullback_id"] = fade_id

        if close_now <= 0 or atr_now <= 0:
            return _blocked("DATA_MISSING", atlas_data)
        if atr_now / close_now < cfg.atr_min_ratio:
            return _blocked("ATR_TOO_LOW", atlas_data)
        if cfg.min_quote_volume > 0 and vol_sma_now < cfg.min_quote_volume:
            return _blocked("VOLUME_TOO_LOW", atlas_data)

        high_minus_ema20 = high_now - ema20_now
        location_ok = close_now > ema20_now and high_now >= (ema20_now + (atr_now * cfg.fade_atr_mult))
        if (
            high_minus_ema20 < (atr_now * cfg.fade_atr_min_mult)
            or high_minus_ema20 > (atr_now * cfg.fade_atr_max_mult)
        ):
            return _blocked("LOCATION_RANGE_FAIL", atlas_data)
        if not location_ok:
            return _blocked("LOCATION_FAIL", atlas_data)

        bb_touch = False
        if bb_width > 0:
            bb_touch = high_now >= (bb_upper - (bb_width * cfg.bb_upper_eps))
        if cfg.bb_touch_required and not bb_touch:
            return _blocked("BB_TOUCH_MISSING", atlas_data)

        if rsi_now >= cfg.rsi_block:
            return _blocked("RSI_OVERHEATED", atlas_data)
        if not (cfg.rsi_min <= rsi_now <= cfg.rsi_max):
            return _blocked("RSI_RANGE_FAIL", atlas_data)

        rsi_down = rsi_now < rsi_prev
        macd_decay = hist_now < hist_prev
        bearish_body = close_now < open_now
        wick_ok = wick_ratio >= cfg.wick_ratio_min
        trigger_hits = sum(1 for v in (wick_ok, rsi_down, macd_decay, bearish_body) if v)
        if trigger_hits < cfg.trigger_min:
            return _blocked("TRIGGER_NOT_MET", atlas_data)
        if cfg.trigger_exact and trigger_hits != cfg.trigger_min:
            return _blocked("TRIGGER_COUNT_MISMATCH", atlas_data)

        if rec.get("last_entry_fade_id") == fade_id:
            return _blocked("DUP_FADE", atlas_data)

        _set_state("ENTRY_READY")
        entry_transition = state_transition
        rec["last_entry_fade_id"] = fade_id
        bucket[symbol] = rec

        risk_tags = []
        if score_val < cfg.min_score:
            risk_tags.append("SCORE_LOW")
        rsz = atlas_data.get("rs_z")
        if isinstance(rsz, (int, float)) and rsz <= -3.0:
            risk_tags.append("RS_EXTREME")
        vol_ratio = atlas_data.get("vol")
        if isinstance(vol_ratio, (int, float)) and vol_ratio >= 10:
            risk_tags.append("VOL_HIGH")
        if bb_touch:
            risk_tags.append("BB_TOUCH")
        if macd_decay:
            risk_tags.append("MACD_DECAY")

        triggers = []
        if wick_ok:
            triggers.append("WICK")
        if rsi_down:
            triggers.append("RSI_DOWN")
        if macd_decay:
            triggers.append("MACD_DECAY")
        if bearish_body:
            triggers.append("BEAR_BODY")
        confirm_type = "+".join(triggers)
        trigger_bits = "WICK={w};RSI={r};MACD={m};BEAR={b}".format(
            w=int(wick_ok),
            r=int(rsi_down),
            m=int(macd_decay),
            b=int(bearish_body),
        )

        meta = {
            "engine": "atlas_rs_fail_short",
            "symbol": symbol,
            "side": "SHORT",
            "timeframe": cfg.ltf_tf,
            "reason": "INTRADAY_FADE",
            "entry_price": close_now,
            "entry_ts": last_ts,
            "size_mult": 1.0,
            "risk_tags": risk_tags,
            "pullback_id": fade_id,
            "state_transition": entry_transition,
            "atlas": {
                "regime": atlas_data.get("regime"),
                "dir": atlas_data.get("dir"),
                "score": atlas_data.get("score"),
                "rs_z": atlas_data.get("rs_z"),
                "rs_z_slow": atlas_data.get("rs_z_slow"),
                "beta": atlas_data.get("beta"),
                "vol": atlas_data.get("vol"),
            },
            "tech": {
                "ema20": ema20_now,
                "atr": atr_now,
                "rsi": rsi_now,
                "rsi_prev": rsi_prev,
                "macd_hist": hist_now,
                "macd_hist_prev": hist_prev,
                "bb_upper": bb_upper,
                "bb_lower": bb_lower,
                "bb_width": bb_width,
                "wick_ratio": wick_ratio,
                "trigger_hits": trigger_hits,
                "confirm_type": confirm_type,
                "trigger_bits": trigger_bits,
                "high_minus_ema20": high_minus_ema20,
            },
        }
        _emit_log(
            entry_ready=1,
            block_reason="",
            atlas_data=atlas_data,
            htf_ok=None,
            ltf_pullback=0,
            fail_confirm=0,
            confirm_type=confirm_type,
            size_mult=1.0,
        )
        rec["state"] = "IDLE"
        bucket[symbol] = rec
        return AtlasRsFailShortSignal(
            symbol=symbol,
            entry_ready=True,
            entry_price=close_now,
            size_mult=1.0,
            meta=meta,
        )


def _ema(series: pd.Series, span: int) -> Optional[pd.Series]:
    if series.empty or span <= 0:
        return None
    return series.ewm(span=span, adjust=False).mean()

def _sma(series: pd.Series, window: int) -> Optional[pd.Series]:
    if series.empty or window <= 0:
        return None
    return series.rolling(window).mean()

def _rsi_wilder(close: pd.Series, period: int) -> Optional[pd.Series]:
    if close.empty or period <= 0:
        return None
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.astype(float).fillna(0.0)

def _macd_hist(close: pd.Series, fast: int, slow: int, signal: int) -> Optional[pd.Series]:
    if close.empty or fast <= 0 or slow <= 0 or signal <= 0:
        return None
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd - sig

def _bollinger(close: pd.Series, window: int, mult: float) -> Optional[tuple[float, float, float]]:
    if close.empty or window <= 1:
        return None
    mean = close.rolling(window).mean()
    std = close.rolling(window).std()
    if mean.empty or std.empty:
        return None
    mid = float(mean.iloc[-1])
    dev = float(std.iloc[-1]) * mult
    return mid, mid + dev, mid - dev

def _atr(df: pd.DataFrame, n: int = 14) -> Optional[pd.Series]:
    if df.empty or len(df) < (n + 2):
        return None
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = (high - low).abs().to_frame("hl")
    tr["hc"] = (high - prev_close).abs()
    tr["lc"] = (low - prev_close).abs()
    tr_val = tr.max(axis=1)
    return tr_val.rolling(n).mean()


def _upper_wick_ratio(open_px: float, high_px: float, low_px: float, close_px: float) -> float:
    rng = max(0.0, high_px - low_px)
    if rng <= 0:
        return 0.0
    upper = high_px - max(open_px, close_px)
    return max(0.0, upper / rng)

def _body_ratio(open_px: float, high_px: float, low_px: float, close_px: float) -> float:
    rng = max(0.0, high_px - low_px)
    if rng <= 0:
        return 0.0
    body = abs(close_px - open_px)
    return max(0.0, body / rng)

def _num_or_none(val: Any) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)
    return None

def _dir_or_none(val: Any) -> Optional[str]:
    if not isinstance(val, str):
        return None
    v = val.upper()
    if v in ("BULL", "BEAR", "NEUTRAL"):
        return v
    return None


def _fmt_flag(val: Optional[bool]) -> str:
    if val is None:
        return "N/A"
    return "1" if val else "0"


def _fmt_val(val: Any) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, float):
        return f"{val:.4g}"
    return str(val)

def _fmt_val_atlas(val: Any, fmt: str = "{:.4f}") -> str:
    if isinstance(val, (int, float)):
        return fmt.format(val)
    return "n/a"

def _fmt_now_kst() -> str:
    kst = pd.Timestamp.now(tz="Asia/Seoul")
    return kst.strftime("%Y-%m-%d %H:%M:%S KST")

def _atlas_state(atlas_data: Dict[str, Any]) -> str:
    allow_long = int(atlas_data.get("allow_long", 0) or 0)
    allow_short = int(atlas_data.get("allow_short", 0) or 0)
    if allow_long and not allow_short:
        return "STRONG_BULL"
    if allow_short and not allow_long:
        return "STRONG_BEAR"
    return "NO_TRADE"

def _score_parts(atlas_data: Dict[str, Any], cfg) -> Dict[str, int]:
    regime = atlas_data.get("regime")
    score_regime = 25 if regime in ("bull", "bear") else (15 if regime == "chaos_vol" else 5)
    rs = atlas_data.get("rs")
    rs_z = atlas_data.get("rs_z")
    rs_slow = atlas_data.get("rs_slow")
    rs_z_slow = atlas_data.get("rs_z_slow")
    rs_pass = (
        (isinstance(rs, (int, float)) and rs >= cfg.rs_pass)
        or (isinstance(rs_z, (int, float)) and rs_z >= cfg.rs_z_pass)
        or (isinstance(rs_slow, (int, float)) and rs_slow >= cfg.rs_pass)
        or (isinstance(rs_z_slow, (int, float)) and rs_z_slow >= cfg.rs_z_pass)
    )
    corr = atlas_data.get("corr")
    beta = atlas_data.get("beta")
    corr_slow = atlas_data.get("corr_slow")
    beta_slow = atlas_data.get("beta_slow")
    indep_pass = (
        (isinstance(corr, (int, float)) and corr <= cfg.indep_corr)
        or (isinstance(beta, (int, float)) and beta <= cfg.indep_beta)
        or (isinstance(corr_slow, (int, float)) and corr_slow <= cfg.indep_corr)
        or (isinstance(beta_slow, (int, float)) and beta_slow <= cfg.indep_beta)
    )
    vol_ratio = atlas_data.get("vol")
    vol_pass = isinstance(vol_ratio, (int, float)) and vol_ratio >= cfg.vol_pass
    return {
        "score_regime": score_regime,
        "score_rs": 25 if rs_pass else 0,
        "score_indep": 25 if indep_pass else 0,
        "score_vol": 25 if vol_pass else 0,
    }
