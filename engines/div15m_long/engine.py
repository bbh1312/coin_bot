from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd

from engines.base import BaseEngine
from engines.div15m_long.config import Div15mConfig
from engines.div15m_long.pivots import find_recent_pivot_lows
from engines.div15m_long.scorer import score_divergence
from engines.div15m_long.state import CandidateState, SymbolState


@dataclass
class Div15mEvent:
    ts: int
    symbol: str
    event: str
    entry_px: float
    p1_idx: int
    p2_idx: int
    low1: float
    low2: float
    rsi1: float
    rsi2: float
    score: float
    reasons: str


class Div15mLongEngine(BaseEngine):
    name: str = "div15m_long"

    def __init__(self, config: Optional[Div15mConfig] = None):
        self.config = config or Div15mConfig()
        self.symbol_states: Dict[str, SymbolState] = {}
        self.counters: Dict[str, int] = {
            "bars": 0,
            "pivots_found": 0,
            "candidate": 0,
            "confirm_ok": 0,
            "trigger_ok": 0,
            "skipped_cooldown": 0,
            "skipped_dup": 0,
            "confirm_fail_no_os": 0,
            "confirm_fail_no_reclaim": 0,
            "confirm_fail_spike": 0,
            "regime_block": 0,
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

    def _ema(self, values: pd.Series, length: int) -> pd.Series:
        return values.ewm(span=length, adjust=False).mean()

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

    def _confirm_rsi_os(self, rsi: pd.Series, p2_idx: int) -> bool:
        w = self.config.OS_LOOKBACK_W
        start = max(0, p2_idx - w)
        window = rsi.iloc[start : p2_idx + 1]
        if window.empty:
            return False
        return float(window.min()) <= self.config.RSI_OS

    def _confirm_reclaim(self, close: pd.Series, ema_fast: pd.Series, p2_idx: int, idx: int) -> bool:
        end = min(idx, p2_idx + self.config.CONFIRM_WINDOW)
        if end <= p2_idx:
            return False
        for i in range(p2_idx + 1, end + 1):
            if float(close.iloc[i]) > float(ema_fast.iloc[i]):
                return True
        return False

    def _trigger_ok(self, df: pd.DataFrame, ema_slow: pd.Series, idx: int) -> Tuple[bool, str]:
        if self.config.TRIGGER_MODE.upper() == "B":
            if idx < 3:
                return False, "B"
            prev_high = float(df["high"].iloc[idx - 3 : idx].max())
            return float(df["close"].iloc[idx]) > prev_high, "B"
        return float(df["close"].iloc[idx]) > float(ema_slow.iloc[idx]), "A"

    def on_bar(
        self,
        symbol: str,
        df: pd.DataFrame,
        idx: int,
        rsi: pd.Series,
        ema_fast: pd.Series,
        ema_slow: pd.Series,
        log_fn,
    ) -> Optional[Div15mEvent]:
        self.counters["bars"] += 1
        st = self._ensure_state(symbol)
        lows = df["low"].tolist()
        if idx < (self.config.RSI_LEN + self.config.WARMUP_BARS):
            return None

        p1_idx, p2_idx, pivot_count = find_recent_pivot_lows(
            lows,
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
                log_fn(f"DIV15_SKIP sym={symbol} reason=cooldown")
            return None

        if p1_idx is None or p2_idx is None:
            return None

        if p2_idx == st.last_p2_idx and st.active is None:
            self.counters["skipped_dup"] += 1
            log_fn(f"DIV15_SKIP sym={symbol} reason=dup_pivot")
            return None

        low1 = float(df["low"].iloc[p1_idx])
        low2 = float(df["low"].iloc[p2_idx])
        if st.active is not None and st.active.p2_idx == p2_idx:
            return None

        if low1 <= 0:
            return None
        ll_pct = (low1 - low2) / low1
        rsi1 = float(rsi.iloc[p1_idx])
        rsi2 = float(rsi.iloc[p2_idx])
        rsi_up = rsi2 - rsi1
        if rsi1 <= 0.0 or rsi2 <= 0.0:
            return None

        if not (low2 < low1 * (1 - self.config.MIN_LL_PCT) and rsi_up >= self.config.MIN_RSI_UP):
            return None

        score, _, _ = score_divergence(
            low1,
            low2,
            rsi1,
            rsi2,
            target_ll=self.config.MIN_LL_PCT,
            target_rsi=self.config.MIN_RSI_UP,
        )

        self.counters["candidate"] += 1
        log_fn(
            "DIV15_CANDIDATE "
            f"sym={symbol} p1={p1_idx} p2={p2_idx} "
            f"low1={low1} low2={low2} rsi1={rsi1} rsi2={rsi2} "
            f"ll_pct={ll_pct:.6f} rsi_up={rsi_up:.4f} score={score:.4f}"
        )
        candidate = CandidateState(
            p1_idx=p1_idx,
            p2_idx=p2_idx,
            low1=low1,
            low2=low2,
            rsi1=rsi1,
            rsi2=rsi2,
            ll_pct=ll_pct,
            rsi_up=rsi_up,
            score=score,
            confirm_window_end=p2_idx + self.config.CONFIRM_WINDOW,
            rsi_os_ok=self._confirm_rsi_os(rsi, p2_idx),
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
    ) -> Optional[Div15mEvent]:
        st = self._ensure_state(symbol)
        candidate = st.active
        if candidate is None:
            return None
        if idx <= candidate.p2_idx:
            return None

        if candidate.spike_block and not candidate.confirm_logged:
            candidate.confirm_fail = ["spike_block"]
            self.counters["confirm_fail_spike"] += 1
            log_fn(f"DIV15_CONFIRM sym={symbol} ok=N fail=[spike_block]")
            candidate.confirm_logged = True
            st.active = None
            return None

        if not candidate.rsi_os_ok and not candidate.confirm_logged:
            candidate.confirm_fail = ["no_os"]
            self.counters["confirm_fail_no_os"] += 1
            log_fn(f"DIV15_CONFIRM sym={symbol} ok=N fail=[no_os]")
            candidate.confirm_logged = True
            st.active = None
            return None

        close = df["close"]

        if not candidate.ema_reclaim_found:
            candidate.ema_reclaim_found = self._confirm_reclaim(close, ema_fast, candidate.p2_idx, idx)

        if candidate.ema_reclaim_found and not candidate.confirm_logged:
            candidate.confirm_ok = True
            self.counters["confirm_ok"] += 1
            log_fn(f"DIV15_CONFIRM sym={symbol} ok=Y reasons=[rsi_os, ema7_reclaim]")
            candidate.confirm_logged = True

        if not candidate.confirm_ok and idx >= candidate.confirm_window_end and not candidate.confirm_logged:
            candidate.confirm_fail = ["no_reclaim"]
            self.counters["confirm_fail_no_reclaim"] += 1
            log_fn(f"DIV15_CONFIRM sym={symbol} ok=N fail=[no_reclaim]")
            candidate.confirm_logged = True
            st.active = None
            return None

        if not candidate.confirm_ok:
            return None

        if idx >= 2:
            low0 = float(df["low"].iloc[idx])
            low1 = float(df["low"].iloc[idx - 1])
            low2 = float(df["low"].iloc[idx - 2])
            if low0 < low1 < low2:
                if not candidate.regime_block_logged:
                    self.counters["regime_block"] = self.counters.get("regime_block", 0) + 1
                    log_fn(f"DIV15_SKIP sym={symbol} reason=lower_lows")
                    candidate.regime_block_logged = True
                return None

        if idx >= 1 and len(ema_regime) > idx:
            ema_now = float(ema_regime.iloc[idx])
            ema_prev = float(ema_regime.iloc[idx - 1])
            slope = ema_now - ema_prev
            close_now = float(df["close"].iloc[idx])
            if close_now <= ema_now or slope < 0:
                if not candidate.regime_block_logged:
                    self.counters["regime_block"] = self.counters.get("regime_block", 0) + 1
                    log_fn(f"DIV15_SKIP sym={symbol} reason=regime_filter")
                    candidate.regime_block_logged = True
                return None

        trigger_ok, trig_mode = self._trigger_ok(df, ema_slow, idx)
        if trigger_ok and not candidate.trigger_logged:
            ts = int(df["ts"].iloc[idx])
            entry_px = float(df["close"].iloc[idx])
            log_fn(f"DIV15_TRIGGER sym={symbol} trigger={trig_mode} entry_px={entry_px} entry_ts={ts}")
            candidate.trigger_logged = True
            st.last_signal_idx = idx
            if self.config.SIGNAL_COOLDOWN_BARS > 0:
                st.cooldown_until = idx + self.config.SIGNAL_COOLDOWN_BARS
            self.counters["trigger_ok"] += 1
            event = Div15mEvent(
                ts=ts,
                symbol=symbol,
                event="ENTRY_READY",
                entry_px=entry_px,
                p1_idx=candidate.p1_idx,
                p2_idx=candidate.p2_idx,
                low1=candidate.low1,
                low2=candidate.low2,
                rsi1=candidate.rsi1,
                rsi2=candidate.rsi2,
                score=candidate.score,
                reasons=f"trigger={trig_mode}",
            )
            st.active = None
            return event
        return None
