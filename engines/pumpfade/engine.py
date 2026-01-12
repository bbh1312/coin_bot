from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

import cycle_cache
from engines.base import BaseEngine, EngineContext, Signal
from engines.pumpfade.config import PumpFadeConfig


@dataclass
class PumpFadeSignal:
    symbol: str
    entry_ready: bool
    entry_price: Optional[float]
    entry_zone: Optional[tuple[float, float]]
    reasons: List[str]
    meta: Dict[str, Any]


class PumpFadeEngine(BaseEngine):
    name: str = "pumpfade"

    def __init__(self, config: Optional[PumpFadeConfig] = None):
        self.config = config or PumpFadeConfig()

    def build_universe(self, ctx: EngineContext) -> List[str]:
        shared = ctx.state.get("_universe")
        tickers = ctx.state.get("_tickers")
        if isinstance(shared, list) and shared:
            return list(shared)
        if isinstance(tickers, dict):
            return list(tickers.keys())
        return []

    def on_tick(self, ctx: EngineContext, symbol: str) -> Optional[Signal]:
        cfg = self.config
        sig = self.evaluate_symbol(ctx, symbol, cfg)
        if not sig or not sig.entry_ready or not sig.entry_price:
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

    def evaluate_symbol(
        self,
        ctx: EngineContext,
        symbol: str,
        cfg: PumpFadeConfig,
    ) -> Optional[PumpFadeSignal]:
        df15 = cycle_cache.get_df(symbol, cfg.tf_trigger, limit=max(80, cfg.lookback_hh + 10))
        if df15.empty or len(df15) < max(60, cfg.lookback_hh + 3):
            return None
        df15 = df15.iloc[:-1]
        if len(df15) < max(60, cfg.lookback_hh + 3):
            return None

        failure_idx = len(df15) - 2
        confirm_idx = len(df15) - 1
        if failure_idx < 2 or confirm_idx <= failure_idx:
            return None

        tickers = ctx.state.get("_tickers") if isinstance(ctx.state, dict) else None
        if not self._pump_candidate(symbol, tickers, cfg):
            return None

        closes = df15["close"].astype(float).tolist()
        highs = df15["high"].astype(float).tolist()
        lows = df15["low"].astype(float).tolist()
        opens = df15["open"].astype(float).tolist()
        vols = df15["volume"].astype(float).tolist()

        ema7 = self._ema_series(df15["close"], cfg.ema_fast)
        ema20 = self._ema_series(df15["close"], cfg.ema_mid)
        ema60 = self._ema_series(df15["close"], cfg.ema_slow)
        atr15 = self._atr_series(df15, cfg.atr_len)
        if ema7 is None or ema20 is None or ema60 is None or atr15 is None:
            return None
        if len(ema20) <= confirm_idx or len(atr15) <= confirm_idx:
            return None

        atr_now = float(atr15.iloc[confirm_idx])
        ema20_now = float(ema20.iloc[confirm_idx])
        close_now = closes[confirm_idx]
        dist_to_ema20 = abs(close_now - ema20_now)
        if atr_now > 0 and dist_to_ema20 > atr_now * cfg.dist_to_ema20_atr_mult:
            return None
        if cfg.ema20_slope_lookback > 0 and atr_now > 0:
            slope_idx = max(0, confirm_idx - cfg.ema20_slope_lookback)
            if slope_idx < confirm_idx:
                ema20_slope = float(ema20.iloc[confirm_idx]) - float(ema20.iloc[slope_idx])
                if ema20_slope > atr_now * cfg.ema20_slope_atr_mult_max:
                    return None

        hh_start = max(0, confirm_idx - cfg.lookback_hh)
        hh_end = confirm_idx
        hh_window = highs[hh_start:hh_end]
        if not hh_window:
            return None
        hh_n = max(hh_window)
        retest_low = hh_n - (atr_now * cfg.retest_atr_high)
        retest_high = hh_n + (atr_now * cfg.retest_atr_low)
        fail_high = highs[failure_idx]
        fail_low = lows[failure_idx]
        in_retest = retest_low <= fail_high <= retest_high or fail_high >= retest_low
        if not in_retest:
            return None

        prior_start = max(0, failure_idx - cfg.lookback_hh)
        prior_hh = max(highs[prior_start:failure_idx])
        failure_reasons = []
        failure_ok = False
        tiny_eps = cfg.failure_eps
        fail_open = opens[failure_idx]
        fail_close = closes[failure_idx]
        body = max(1e-9, abs(fail_close - fail_open))
        upper_wick = fail_high - max(fail_open, fail_close)
        atr_fail = float(atr15.iloc[failure_idx]) if len(atr15) > failure_idx else atr_now
        if atr_fail > 0 and body < atr_fail * cfg.min_body_atr_mult:
            return None
        if (
            fail_high >= prior_hh * (1 - tiny_eps)
            and fail_close <= prior_hh
            and upper_wick >= body * cfg.failure_wick_mult
            and fail_close <= fail_open
        ):
            failure_ok = True
            failure_reasons.append("upper_wick_fail")
        mid = (fail_high + fail_low) * 0.5
        if (
            fail_high > prior_hh
            and fail_close < prior_hh
            and fail_close < mid
        ):
            failure_ok = True
            failure_reasons.append("break_fail_close_weak")
        if not failure_ok:
            return None

        confirm_ok = False
        confirm_type = ""
        confirm_close = closes[confirm_idx]
        if cfg.confirm_use_low_break and confirm_close < fail_low:
            confirm_ok = True
            confirm_type = "LOW_BREAK"
        if cfg.confirm_use_ema7:
            ema7_now = float(ema7.iloc[confirm_idx])
            if confirm_close < ema7_now and not confirm_ok:
                confirm_ok = True
                confirm_type = "EMA7"
        if not confirm_ok:
            return None

        vol_failure = vols[failure_idx]
        vol_prev = vols[failure_idx - 1] if failure_idx - 1 >= 0 else vol_failure
        peak_start = max(0, failure_idx - max(1, int(cfg.vol_peak_lookback)))
        vol_peak = max(vols[peak_start:failure_idx + 1])
        if vol_peak > 0 and vol_failure >= vol_peak * cfg.vol_peak_block:
            return None
        vol_ok = False
        if vol_peak > 0 and vol_failure <= vol_peak * cfg.vol_peak_mult_ok:
            vol_ok = True
        if vol_prev > 0 and vol_failure <= vol_prev * cfg.vol_prev_mult_ok:
            vol_ok = True
        if not vol_ok:
            return None

        rsi_now = None
        rsi_prev = None
        rsi_turn = False
        rsi_strong = False
        rsi_series = self._rsi_series(df15["close"], cfg.rsi_len)
        if rsi_series is not None and len(rsi_series) > confirm_idx:
            rsi_now = float(rsi_series.iloc[confirm_idx])
            rsi_prev = float(rsi_series.iloc[confirm_idx - 1])
            rsi_turn = rsi_prev > rsi_now
            rsi_strong = rsi_now < cfg.rsi_strong_threshold
        if cfg.rsi_turn_required and not rsi_turn:
            return None
        if cfg.rsi_strong_required and not rsi_strong:
            return None

        hist_increasing = False
        hist_now = None
        macd_hist = self._macd_hist(df15["close"], cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
        if macd_hist is not None and len(macd_hist) > confirm_idx:
            hist_now = float(macd_hist.iloc[confirm_idx])
            hist_prev = float(macd_hist.iloc[confirm_idx - 1])
            hist_increasing = hist_now > hist_prev
        if cfg.macd_block_increasing and hist_increasing and isinstance(hist_now, (int, float)) and hist_now > 0:
            return None

        if cfg.confirm_use_5m:
            df5 = cycle_cache.get_df(symbol, cfg.tf_confirm, limit=60)
            if df5.empty or len(df5) < 20:
                return None
            df5 = df5.iloc[:-1]
            if len(df5) < 20:
                return None
            ema7_5m = self._ema_series(df5["close"], cfg.ema_fast)
            if ema7_5m is None or len(ema7_5m) < 1:
                return None
            last5_close = float(df5["close"].iloc[-1])
            if last5_close >= float(ema7_5m.iloc[-1]):
                return None

        entry_low = (fail_high + fail_low) * 0.5
        entry_high = fail_high
        entry_price = entry_low
        ema7_now = float(ema7.iloc[confirm_idx])
        if entry_low <= ema7_now <= entry_high:
            entry_price = ema7_now

        reasons = [
            "retest_zone",
            "failure_candle",
            "confirm_break",
            "vol_weaken",
        ] + failure_reasons
        if rsi_turn:
            reasons.append("rsi_turn")
        if rsi_strong:
            reasons.append("rsi_strong")
        if hist_increasing:
            reasons.append("macd_increasing")

        meta = {
            "reasons": reasons,
            "hh_n": hh_n,
            "prior_hh": prior_hh,
            "retest_low": retest_low,
            "retest_high": retest_high,
            "failure_high": fail_high,
            "failure_low": fail_low,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "entry_price": entry_price,
            "vol_failure": vol_failure,
            "vol_peak": vol_peak,
            "vol_peak_lookback": cfg.vol_peak_lookback,
            "vol_prev": vol_prev,
            "vol_ok": vol_ok,
            "rsi": rsi_now,
            "rsi_prev": rsi_prev,
            "rsi_turn": rsi_turn,
            "rsi_strong": rsi_strong,
            "macd_hist_increasing": hist_increasing,
            "confirm_close": confirm_close,
            "confirm_close_lt_fail_low": confirm_close < fail_low,
            "confirm_close_lt_ema7": confirm_close < float(ema7.iloc[confirm_idx]),
            "confirm_type": confirm_type,
            "atr15": atr_now,
        }
        return PumpFadeSignal(
            symbol=symbol,
            entry_ready=True,
            entry_price=entry_price,
            entry_zone=(entry_low, entry_high),
            reasons=reasons,
            meta=meta,
        )

    def _filter_gainers(self, symbols: List[str], tickers: Optional[dict]) -> List[str]:
        if not tickers:
            return symbols
        out = []
        for sym in symbols:
            t = tickers.get(sym)
            if not t:
                continue
            try:
                pct = float(t.get("percentage") or 0.0)
            except Exception:
                pct = 0.0
            if pct > 0:
                out.append(sym)
        return out

    def _pump_candidate(self, symbol: str, tickers: Optional[dict], cfg: PumpFadeConfig) -> bool:
        hits = 0
        pct_24h = None
        if isinstance(tickers, dict):
            t = tickers.get(symbol)
            if t and t.get("percentage") is not None:
                try:
                    pct_24h = float(t.get("percentage"))
                except Exception:
                    pct_24h = None
        if pct_24h is not None and pct_24h >= cfg.pct_24h_min:
            hits += 1

        pct_6h = self._pct_change(symbol, cfg.tf_trigger, bars=24)
        if pct_6h is not None and pct_6h >= cfg.pct_6h_min:
            hits += 1

        range_3h = self._range_pct(symbol, cfg.tf_trigger, bars=12)
        if range_3h is not None and range_3h >= cfg.range_3h_min:
            hits += 1

        vol_1h = self._vol_spike(symbol, "1h", mult=cfg.vol_1h_mult)
        if vol_1h:
            hits += 1

        return hits >= cfg.pump_min_hits

    def _pct_change(self, symbol: str, tf: str, bars: int) -> Optional[float]:
        df = cycle_cache.get_df(symbol, tf, limit=bars + 5)
        if df.empty or len(df) < bars + 1:
            return None
        df = df.iloc[:-1]
        if len(df) < bars + 1:
            return None
        last = float(df["close"].iloc[-1])
        prev = float(df["close"].iloc[-(bars + 1)])
        if prev <= 0:
            return None
        return (last / prev - 1.0) * 100.0

    def _range_pct(self, symbol: str, tf: str, bars: int) -> Optional[float]:
        df = cycle_cache.get_df(symbol, tf, limit=bars + 5)
        if df.empty or len(df) < bars:
            return None
        df = df.iloc[:-1]
        if len(df) < bars:
            return None
        highs = df["high"].astype(float).iloc[-bars:]
        lows = df["low"].astype(float).iloc[-bars:]
        hh = float(highs.max())
        ll = float(lows.min())
        if ll <= 0:
            return None
        return (hh - ll) / ll * 100.0

    def _vol_spike(self, symbol: str, tf: str, mult: float) -> bool:
        df = cycle_cache.get_df(symbol, tf, limit=40)
        if df.empty or len(df) < 25:
            return False
        df = df.iloc[:-1]
        if len(df) < 25:
            return False
        vol_now = float(df["volume"].iloc[-1])
        vol_sma = float(df["volume"].iloc[-21:-1].mean())
        if vol_sma <= 0:
            return False
        return vol_now >= vol_sma * mult

    @staticmethod
    def _ema_series(values: pd.Series, length: int) -> Optional[pd.Series]:
        if values is None or len(values) < length + 2:
            return None
        return values.ewm(span=length, adjust=False).mean()

    @staticmethod
    def _atr_series(df: pd.DataFrame, length: int) -> Optional[pd.Series]:
        if df.empty or len(df) < length + 2:
            return None
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat(
            [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        return tr.rolling(length).mean()

    @staticmethod
    def _rsi_series(values: pd.Series, length: int) -> Optional[pd.Series]:
        if values is None or len(values) < length + 2:
            return None
        delta = values.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
        avg_loss = avg_loss.replace(0, float("nan"))
        rs = (avg_gain / avg_loss).astype(float)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(0.0)

    @staticmethod
    def _macd_hist(values: pd.Series, fast: int, slow: int, signal: int) -> Optional[pd.Series]:
        if values is None or len(values) < slow + signal + 5:
            return None
        ema_fast = values.ewm(span=fast, adjust=False).mean()
        ema_slow = values.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        sig = macd.ewm(span=signal, adjust=False).mean()
        return macd - sig
