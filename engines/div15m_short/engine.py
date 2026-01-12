from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd

from engines.base import BaseEngine
from engines.div15m_short.config import Div15mShortConfig
from engines.div15m_short.pivots import find_recent_pivot_highs
from engines.div15m_short.scorer import score_divergence
from engines.div15m_short.state import CandidateState, SymbolState


@dataclass
class Div15mShortEvent:
    ts: int
    symbol: str
    event: str
    entry_px: float
    p1_idx: int
    p2_idx: int
    high1: float
    high2: float
    rsi1: float
    rsi2: float
    score: float
    reasons: str


class Div15mShortEngine(BaseEngine):
    name: str = "div15m_short"

    def __init__(self, config: Optional[Div15mShortConfig] = None):
        self.config = config or Div15mShortConfig()
        self.symbol_states: Dict[str, SymbolState] = {}
        self.counters: Dict[str, int] = {
            "bars": 0,
            "pivots_found": 0,
            "candidate": 0,
            "confirm_ok": 0,
            "trigger_ok": 0,
            "skipped_cooldown": 0,
            "skipped_dup": 0,
            "confirm_fail_no_ob": 0,
            "confirm_fail_no_reject": 0,
            "confirm_fail_spike": 0,
            "regime_block": 0,
            "macd_block": 0,
        }

    @staticmethod
    def _rsi_wilder(close: pd.Series, period: int) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = avg_loss.replace(0, float("nan"))
        rs = (avg_gain / avg_loss).astype(float)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(0.0)

    @staticmethod
    def _ema(values: pd.Series, length: int) -> pd.Series:
        return values.ewm(span=length, adjust=False).mean()

    @staticmethod
    def _macd_hist(close: pd.Series, fast: int, slow: int, signal: int) -> pd.Series:
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal, adjust=False).mean()
        return macd - macd_signal

    def _ensure_state(self, symbol: str) -> SymbolState:
        st = self.symbol_states.get(symbol)
        if st is None:
            st = SymbolState()
            self.symbol_states[symbol] = st
        return st

    def _check_spike(self, df: pd.DataFrame, p2_idx: int) -> bool:
        if self.config.SPIKE_VOL_X <= 0:
            return False
        lb = self.config.SPIKE_VOL_LOOKBACK
        if p2_idx < lb:
            return False
        vol = float(df["volume"].iloc[p2_idx])
        avg = float(df["volume"].iloc[p2_idx - lb : p2_idx].mean())
        return avg > 0 and vol >= avg * self.config.SPIKE_VOL_X

    def _confirm_rsi_ob(self, rsi: pd.Series, p2_idx: int) -> bool:
        w = self.config.OB_LOOKBACK_W
        start = max(0, p2_idx - w)
        window = rsi.iloc[start : p2_idx + 1]
        if window.empty:
            return False
        return float(window.max()) >= self.config.RSI_OB

    def _confirm_reject(self, close: pd.Series, ema_fast: pd.Series, p2_idx: int, idx: int) -> bool:
        end = min(idx, p2_idx + self.config.CONFIRM_WINDOW)
        if end <= p2_idx:
            return False
        for i in range(p2_idx + 1, end + 1):
            if float(close.iloc[i]) < float(ema_fast.iloc[i]):
                return True
        return False

    def _trigger_ok(self, df: pd.DataFrame, ema_slow: pd.Series, idx: int) -> Tuple[bool, str]:
        if self.config.TRIGGER_MODE.upper() == "B":
            if idx < 3:
                return False, "B"
            prev_low = float(df["low"].iloc[idx - 3 : idx].min())
            return float(df["close"].iloc[idx]) < prev_low, "B"
        return float(df["close"].iloc[idx]) < float(ema_slow.iloc[idx]), "A"

    def on_bar(
        self,
        symbol: str,
        df: pd.DataFrame,
        idx: int,
        rsi: pd.Series,
        ema_fast: pd.Series,
        ema_slow: pd.Series,
        log_fn,
    ) -> Optional[Div15mShortEvent]:
        self.counters["bars"] += 1
        st = self._ensure_state(symbol)
        highs = df["high"].tolist()
        if idx < (self.config.RSI_LEN + self.config.WARMUP_BARS):
            return None

        p1_idx, p2_idx, pivot_count = find_recent_pivot_highs(
            highs,
            idx=idx,
            left_right=self.config.PIVOT_L,
            lookback=self.config.LOOKBACK_BARS,
            min_gap=self.config.MIN_PIVOT_GAP,
        )
        if pivot_count:
            self.counters["pivots_found"] += 1

        if idx <= st.cooldown_until:
            self.counters["skipped_cooldown"] += 1
            if idx == st.last_signal_idx + 1:
                log_fn(f"DIV15S_SKIP sym={symbol} reason=cooldown")
            return None

        if p1_idx is None or p2_idx is None:
            return None

        if p2_idx == st.last_p2_idx and st.active is None:
            self.counters["skipped_dup"] += 1
            log_fn(f"DIV15S_SKIP sym={symbol} reason=dup_pivot")
            return None

        high1 = float(df["high"].iloc[p1_idx])
        high2 = float(df["high"].iloc[p2_idx])
        if st.active is not None and st.active.p2_idx == p2_idx:
            return None

        if high1 <= 0:
            return None
        hh_pct = (high2 - high1) / high1
        rsi1 = float(rsi.iloc[p1_idx])
        rsi2 = float(rsi.iloc[p2_idx])
        rsi_down = rsi1 - rsi2
        if rsi1 <= 0.0 or rsi2 <= 0.0:
            return None

        if not (high2 > high1 * (1 + self.config.MIN_HH_PCT) and rsi_down >= self.config.MIN_RSI_DOWN):
            return None

        score, _, _ = score_divergence(
            high1,
            high2,
            rsi1,
            rsi2,
            target_hh=self.config.MIN_HH_PCT,
            target_rsi=self.config.MIN_RSI_DOWN,
        )

        self.counters["candidate"] += 1
        log_fn(
            "DIV15S_CANDIDATE "
            f"sym={symbol} p1={p1_idx} p2={p2_idx} "
            f"high1={high1} high2={high2} rsi1={rsi1} rsi2={rsi2} "
            f"hh_pct={hh_pct:.6f} rsi_down={rsi_down:.4f} score={score:.4f}"
        )
        candidate = CandidateState(
            p1_idx=p1_idx,
            p2_idx=p2_idx,
            high1=high1,
            high2=high2,
            rsi1=rsi1,
            rsi2=rsi2,
            hh_pct=hh_pct,
            rsi_down=rsi_down,
            score=score,
            confirm_window_end=p2_idx + self.config.CONFIRM_WINDOW,
            rsi_ob_ok=self._confirm_rsi_ob(rsi, p2_idx),
            spike_block=self._check_spike(df, p2_idx),
        )

        st.active = candidate
        st.last_p2_idx = p2_idx

        return None

    def process_candidate(
        self,
        symbol: str,
        df: pd.DataFrame,
        idx: int,
        rsi: pd.Series,
        ema_fast: pd.Series,
        ema_slow: pd.Series,
        ema_regime: pd.Series,
        log_fn,
        macd_hist: Optional[pd.Series] = None,
    ) -> Optional[Div15mShortEvent]:
        st = self._ensure_state(symbol)
        candidate = st.active
        if candidate is None:
            return None
        if idx <= candidate.p2_idx:
            return None

        if candidate.spike_block and not candidate.confirm_logged:
            candidate.confirm_fail = ["spike_block"]
            self.counters["confirm_fail_spike"] += 1
            log_fn(f"DIV15S_CONFIRM sym={symbol} ok=N fail=[spike_block]")
            candidate.confirm_logged = True
            st.active = None
            return None

        if not candidate.rsi_ob_ok and not candidate.confirm_logged:
            candidate.confirm_fail = ["no_ob"]
            self.counters["confirm_fail_no_ob"] += 1
            log_fn(f"DIV15S_CONFIRM sym={symbol} ok=N fail=[no_ob]")
            candidate.confirm_logged = True
            st.active = None
            return None

        close = df["close"]

        if not candidate.ema_reject_found:
            candidate.ema_reject_found = self._confirm_reject(close, ema_fast, candidate.p2_idx, idx)

        if candidate.ema_reject_found and not candidate.confirm_logged:
            candidate.confirm_ok = True
            self.counters["confirm_ok"] += 1
            log_fn(f"DIV15S_CONFIRM sym={symbol} ok=Y reasons=[rsi_ob, ema7_reject]")
            candidate.confirm_logged = True

        if not candidate.confirm_ok and idx >= candidate.confirm_window_end and not candidate.confirm_logged:
            candidate.confirm_fail = ["no_reject"]
            self.counters["confirm_fail_no_reject"] += 1
            log_fn(f"DIV15S_CONFIRM sym={symbol} ok=N fail=[no_reject]")
            candidate.confirm_logged = True
            st.active = None
            return None

        if not candidate.confirm_ok:
            return None

        if idx >= 2:
            high0 = float(df["high"].iloc[idx])
            high1 = float(df["high"].iloc[idx - 1])
            high2 = float(df["high"].iloc[idx - 2])
            if high0 > high1 > high2:
                if not candidate.regime_block_logged:
                    self.counters["regime_block"] = self.counters.get("regime_block", 0) + 1
                    log_fn(f"DIV15S_SKIP sym={symbol} reason=higher_highs")
                    candidate.regime_block_logged = True
                return None

        if idx >= 1 and len(ema_regime) > idx:
            ema_now = float(ema_regime.iloc[idx])
            lookback = max(1, int(self.config.EMA_REGIME_SLOPE_LOOKBACK))
            prev_idx = max(0, idx - lookback)
            ema_prev = float(ema_regime.iloc[prev_idx])
            slope = ema_now - ema_prev
            close_now = float(df["close"].iloc[idx])
            if close_now >= ema_now or slope > 0:
                if not candidate.regime_block_logged:
                    self.counters["regime_block"] = self.counters.get("regime_block", 0) + 1
                    log_fn(f"DIV15S_SKIP sym={symbol} reason=regime_filter")
                    candidate.regime_block_logged = True
                return None

        if self.config.MACD_HIST_FILTER:
            if macd_hist is None:
                macd_hist = self._macd_hist(close, self.config.MACD_FAST, self.config.MACD_SLOW, self.config.MACD_SIGNAL)
            if idx < 1 or len(macd_hist) <= idx:
                return None
            hist_now = float(macd_hist.iloc[idx])
            hist_prev = float(macd_hist.iloc[idx - 1])
            macd_ok = hist_now < hist_prev and hist_prev > 0
            if not macd_ok:
                if not candidate.macd_block_logged:
                    self.counters["macd_block"] += 1
                    log_fn(f"DIV15S_SKIP sym={symbol} reason=macd_hist")
                    candidate.macd_block_logged = True
                return None

        trigger_ok, trig_mode = self._trigger_ok(df, ema_slow, idx)
        if trigger_ok and not candidate.trigger_logged:
            ts = int(df["ts"].iloc[idx])
            entry_px = float(df["close"].iloc[idx])
            log_fn(f"DIV15S_TRIGGER sym={symbol} trigger={trig_mode} entry_px={entry_px} entry_ts={ts}")
            candidate.trigger_logged = True
            st.last_signal_idx = idx
            if self.config.SIGNAL_COOLDOWN_BARS > 0:
                st.cooldown_until = idx + self.config.SIGNAL_COOLDOWN_BARS
            self.counters["trigger_ok"] += 1
            event = Div15mShortEvent(
                ts=ts,
                symbol=symbol,
                event="ENTRY_READY",
                entry_px=entry_px,
                p1_idx=candidate.p1_idx,
                p2_idx=candidate.p2_idx,
                high1=candidate.high1,
                high2=candidate.high2,
                rsi1=candidate.rsi1,
                rsi2=candidate.rsi2,
                score=candidate.score,
                reasons=f"trigger={trig_mode}",
            )
            st.active = None
            return event
        return None
