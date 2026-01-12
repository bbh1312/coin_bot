from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

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
        shared = ctx.state.get("_universe")
        tickers = ctx.state.get("_tickers")
        if isinstance(shared, list) and shared:
            return list(shared)
        if isinstance(tickers, dict):
            return list(tickers.keys())
        return []

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

        def _blocked(reason: str, atlas_data: Optional[dict] = None, htf_ok: Optional[bool] = None) -> None:
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

        st = state.get(symbol, {}) if isinstance(state.get(symbol), dict) else {}
        if st.get("in_pos"):
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("IN_POSITION")
            return None

        if now_ts < float(rec.get("cooldown_until_ts") or 0.0):
            _blocked("COOLDOWN")
            return None

        df_ltf = cycle_cache.get_df(symbol, cfg.ltf_tf, cfg.ltf_limit)
        if df_ltf.empty or len(df_ltf) < max(cfg.ltf_ema_slow + 5, 40):
            _blocked("DATA_MISSING")
            return None
        df_ltf = df_ltf.iloc[:-1]
        if df_ltf.empty or len(df_ltf) < max(cfg.ltf_ema_slow + 5, 40):
            _blocked("DATA_MISSING")
            return None
        last_ts = int(df_ltf["ts"].iloc[-1])
        if last_ts == rec.get("last_bar_ts"):
            return None
        rec["last_bar_ts"] = last_ts

        df_htf = cycle_cache.get_df(symbol, cfg.htf_tf, cfg.htf_limit)
        if df_htf.empty or len(df_htf) < max(cfg.htf_ema_slow + 5, 40):
            _blocked("HTF_DATA_MISSING")
            bucket[symbol] = rec
            return None
        df_htf = df_htf.iloc[:-1]
        if df_htf.empty or len(df_htf) < max(cfg.htf_ema_slow + 5, 40):
            _blocked("HTF_DATA_MISSING")
            bucket[symbol] = rec
            return None

        ema_fast_htf = _ema(df_htf["close"], cfg.htf_ema_fast)
        ema_slow_htf = _ema(df_htf["close"], cfg.htf_ema_slow)
        if ema_fast_htf is None or ema_slow_htf is None:
            _blocked("HTF_DATA_MISSING")
            bucket[symbol] = rec
            return None
        htf_fast_now = float(ema_fast_htf.iloc[-1])
        htf_slow_now = float(ema_slow_htf.iloc[-1])
        htf_ok = True
        if cfg.htf_allow_fast_le_slow:
            htf_ok = htf_fast_now <= htf_slow_now
        if not htf_ok:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("HTF_UPTREND_BLOCK", htf_ok=htf_ok)
            return None

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
        if not atlas_data.get("dir") or atlas_data["dir"] != "BEAR":
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("ATLAS_NOT_BEAR", atlas_data, htf_ok=htf_ok)
            return None
        if not atlas_data.get("regime") or atlas_data.get("rs_z") is None or atlas_data.get("score") is None:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("ATLAS_MISSING_REQUIRED", atlas_data, htf_ok=htf_ok)
            return None
        if float(atlas_data["score"]) < cfg.min_score:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("SCORE_TOO_LOW", atlas_data, htf_ok=htf_ok)
            return None
        rsz = atlas_data.get("rs_z")
        if rsz > cfg.rsz_enter:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("RSZ_TOO_HIGH", atlas_data, htf_ok=htf_ok)
            return None
        rsz_slow = atlas_data.get("rs_z_slow")
        if isinstance(rsz_slow, (int, float)) and rsz_slow > cfg.rsz_slow_max:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("RSZ_SLOW_TOO_HIGH", atlas_data, htf_ok=htf_ok)
            return None
        if atlas_data.get("regime") not in cfg.allow_regimes:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("REGIME_NOT_ALLOWED", atlas_data, htf_ok=htf_ok)
            return None
        beta = atlas_data.get("beta")
        if beta is not None and abs(beta) > cfg.beta_abs_max:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("BETA_TOO_HIGH", atlas_data, htf_ok=htf_ok)
            return None
        vol_ratio = atlas_data.get("vol")
        if vol_ratio is not None and vol_ratio < cfg.vol_min:
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            _blocked("VOL_TOO_LOW", atlas_data, htf_ok=htf_ok)
            return None

        ema_fast_ltf = _ema(df_ltf["close"], cfg.ltf_ema_fast)
        ema_slow_ltf = _ema(df_ltf["close"], cfg.ltf_ema_slow)
        atr_ltf = _atr(df_ltf, 14)
        if ema_fast_ltf is None or ema_slow_ltf is None:
            _blocked("DATA_MISSING", atlas_data, htf_ok=htf_ok)
            return None
        ema20 = float(ema_fast_ltf.iloc[-1])
        ema60 = float(ema_slow_ltf.iloc[-1])
        atr_now = float(atr_ltf.iloc[-1]) if atr_ltf is not None and not atr_ltf.empty else None
        last = df_ltf.iloc[-1]
        open_now = float(last["open"])
        high_now = float(last["high"])
        low_now = float(last["low"])
        close_now = float(last["close"])
        wick_ratio = _upper_wick_ratio(open_now, high_now, low_now, close_now)

        pullback_anchor = None
        pullback_hit = False
        upper_zone = max(ema20, ema60)
        if high_now >= upper_zone:
            pullback_hit = True
            pullback_anchor = "EMA60" if high_now >= ema60 else "EMA20"
        pullback_active = rec.get("state") in ("PULLBACK", "FAIL_CONFIRM")

        if rec.get("state") == "IDLE" and pullback_hit:
            rec["state"] = "PULLBACK"
            rec["pullback_ts"] = last_ts
            rec["pullback_high"] = high_now
            rec["pullback_anchor"] = pullback_anchor
            rec["pullback_id"] = f"{last_ts}:{pullback_anchor or 'EMA'}"
            swing_lookback = max(5, int(cfg.pullback_swing_lookback))
            highs_window = df_ltf["high"].astype(float).tolist()
            if len(highs_window) > 1:
                prior = highs_window[-(swing_lookback + 1):-1]
                rec["last_swing_high"] = max(prior) if prior else None
            else:
                rec["last_swing_high"] = None
            pullback_active = True

        if pullback_active:
            if rec.get("pullback_high") is None:
                rec["pullback_high"] = high_now
            else:
                rec["pullback_high"] = max(float(rec.get("pullback_high") or 0.0), high_now)
            last_swing_high = rec.get("last_swing_high")
            if isinstance(last_swing_high, (int, float)) and high_now > last_swing_high * (1 + cfg.pullback_break_eps):
                rec["state"] = "IDLE"
                rec["pullback_ts"] = 0
                rec["pullback_high"] = None
                rec["pullback_id"] = None
                rec["pullback_anchor"] = None
                rec["last_swing_high"] = None
                bucket[symbol] = rec
                _blocked("PULLBACK_BREAKS_HIGH", atlas_data, htf_ok=htf_ok)
                return None

        if pullback_active and rec.get("state") == "PULLBACK":
            rec["state"] = "FAIL_CONFIRM"

        timeout_bars = int(cfg.pullback_timeout_bars)
        if pullback_active and timeout_bars > 0 and rec.get("pullback_ts"):
            try:
                idx = df_ltf.index[df_ltf["ts"] == rec.get("pullback_ts")]
                if len(idx) > 0:
                    bars_since = len(df_ltf) - 1 - int(idx[-1])
                    if bars_since >= timeout_bars:
                        rec["state"] = "IDLE"
                        rec["pullback_ts"] = 0
                        rec["pullback_high"] = None
                        rec["pullback_id"] = None
                        bucket[symbol] = rec
                        _blocked("CONFIRM_TIMEOUT", atlas_data, htf_ok=htf_ok)
                        return None
            except Exception:
                pass

        confirm_close_ok = close_now < ema20
        confirm_wick_ok = wick_ratio >= cfg.wick_ratio_min
        body_ratio = _body_ratio(open_now, high_now, low_now, close_now)
        bearish_body_ok = close_now < open_now and body_ratio >= cfg.body_ratio_min
        last_swing_high = rec.get("last_swing_high")
        lower_high_ok = (
            isinstance(last_swing_high, (int, float))
            and isinstance(rec.get("pullback_high"), (int, float))
            and float(rec.get("pullback_high")) <= float(last_swing_high) * (1 + cfg.pullback_anchor_eps)
        )
        confirm_ok = pullback_active and confirm_close_ok and confirm_wick_ok and (lower_high_ok or bearish_body_ok)
        confirm_type = ""
        if confirm_ok:
            confirm_type = "close_below_ema20+wick+lh" if lower_high_ok else "close_below_ema20+wick+body"

        size_mult = 1.0
        if isinstance(rsz, (int, float)) and rsz < -3.0:
            size_mult *= 0.7
        if isinstance(vol_ratio, (int, float)) and vol_ratio > 10:
            size_mult *= 0.7

        if confirm_ok:
            pullback_id = rec.get("pullback_id")
            if pullback_id and pullback_id == rec.get("last_entry_pullback_id"):
                _emit_log(
                    entry_ready=0,
                    block_reason="DUP_PULLBACK",
                    atlas_data=atlas_data,
                    htf_ok=htf_ok,
                    ltf_pullback=1,
                    fail_confirm=0,
                    confirm_type=confirm_type,
                    size_mult=size_mult,
                )
                rec["state"] = "IDLE"
                rec["pullback_ts"] = 0
                rec["pullback_high"] = None
                rec["pullback_id"] = None
                bucket[symbol] = rec
                return None
            rec["last_entry_pullback_id"] = pullback_id
            rec["state"] = "ENTRY_READY"
            rec["cooldown_until_ts"] = now_ts + cfg.cooldown_minutes * 60
            bucket[symbol] = rec
            risk_tags = []
            if isinstance(rsz, (int, float)) and rsz < -3.0:
                risk_tags.append("RS_EXTREME")
            if isinstance(vol_ratio, (int, float)) and vol_ratio > 10:
                risk_tags.append("VOL_HIGH")
            meta = {
                "engine": "atlas_rs_fail_short",
                "symbol": symbol,
                "side": "SHORT",
                "timeframe": cfg.ltf_tf,
                "reason": "ATLAS_DIR_BEAR+RS_WEAK+PULLBACK_FAIL",
                "entry_price": close_now,
                "entry_ts": last_ts,
                "size_mult": size_mult,
                "risk_tags": risk_tags,
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
                    "ema20": ema20,
                    "ema60": ema60,
                    "pullback_high": rec.get("pullback_high"),
                    "confirm_type": confirm_type,
                    "wick_ratio": wick_ratio,
                    "body_ratio": body_ratio,
                    "atr": atr_now,
                },
            }
            _emit_log(
                entry_ready=1,
                block_reason="",
                atlas_data=atlas_data,
                htf_ok=htf_ok,
                ltf_pullback=1,
                fail_confirm=1,
                confirm_type=confirm_type,
                size_mult=size_mult,
            )
            rec["state"] = "IDLE"
            rec["pullback_ts"] = 0
            rec["pullback_high"] = None
            rec["pullback_id"] = None
            bucket[symbol] = rec
            return AtlasRsFailShortSignal(
                symbol=symbol,
                entry_ready=True,
                entry_price=close_now,
                size_mult=size_mult,
                meta=meta,
            )

        block_reason = "NO_PULLBACK_TOUCH"
        if pullback_active:
            block_reason = "CONFIRM_NOT_MET"
        _emit_log(
            entry_ready=0,
            block_reason=block_reason,
            atlas_data=atlas_data,
            htf_ok=htf_ok,
            ltf_pullback=1 if pullback_active else 0,
            fail_confirm=1 if pullback_active else 0,
            confirm_type=confirm_type,
            size_mult=size_mult,
        )
        bucket[symbol] = rec
        return AtlasRsFailShortSignal(
            symbol=symbol,
            entry_ready=False,
            entry_price=None,
            size_mult=size_mult,
            meta={"block_reason": block_reason, "atlas": atlas_data},
        )


def _ema(series: pd.Series, span: int) -> Optional[pd.Series]:
    if series.empty or span <= 0:
        return None
    return series.ewm(span=span, adjust=False).mean()

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
