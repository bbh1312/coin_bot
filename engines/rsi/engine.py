from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import cycle_cache
from engines.base import BaseEngine, EngineContext
from engines.rsi.config import RsiConfig


@dataclass
class RsiScanResult:
    ready_entry: bool
    ok_tf: bool
    rsi5m_downturn: bool
    rsi3m_downturn: bool
    rsis: Dict[str, Optional[float]]
    rsis_prev: Dict[str, Optional[float]]
    r3m_val: Optional[float]
    vol_ok: bool
    vol_cur: float
    vol_avg: float
    struct_ok: bool
    struct_metrics: Dict[str, Any]
    check_status: List[Tuple[str, bool]]
    pass_count: int
    miss_count: int
    trigger_ok: bool
    spike_ready: bool
    struct_ready: bool
    impulse_block: bool


class RsiEngine(BaseEngine):
    name: str = "rsi"

    def __init__(self, config: Optional[RsiConfig] = None):
        self.config = config or RsiConfig()
        self._rsi1h_ttl_cache: Dict[str, Tuple[float, Optional[float]]] = {}
        self._cycle_stats: Optional[Dict[str, Any]] = None

    def set_cycle_stats(self, stats: Optional[Dict[str, Any]]) -> None:
        self._cycle_stats = stats

    def _stat_inc(self, key: str, amount: int = 1) -> None:
        if self._cycle_stats is None:
            return
        self._cycle_stats[key] = int(self._cycle_stats.get(key, 0) or 0) + amount

    def _fetch_df(self, symbol: str, tf: str, limit: int) -> pd.DataFrame:
        df = cycle_cache.get_df(symbol, tf, limit)
        if not df.empty:
            if self._cycle_stats is not None:
                hits_by_tf = self._cycle_stats.setdefault("cache_hits_by_tf", {})
                hits_by_tf[tf] = hits_by_tf.get(tf, 0) + 1
            return df
        if self._cycle_stats is not None:
            miss_by_tf = self._cycle_stats.setdefault("cache_miss_by_tf", {})
            miss_by_tf[tf] = miss_by_tf.get(tf, 0) + 1
        return pd.DataFrame()

    @staticmethod
    def rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = avg_loss.replace(0, float("nan"))
        rs = (avg_gain / avg_loss).astype(float)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(0.0)

    def _get_rsi_series(self, symbol: str, tf: str, length: int) -> pd.Series:
        def _compute():
            df = self._fetch_df(symbol, tf, limit=self.config.rsi_default_limit)
            if len(df) < max(20, length + 2):
                return pd.Series(dtype="float64")
            return self.rsi_wilder(df["close"], length)

        return cycle_cache.get_ind(symbol, tf, ("rsi_series", length), _compute)

    def fetch_rsi(self, symbol: str, tf: str) -> Optional[float]:
        try:
            series = self._get_rsi_series(symbol, tf, self.config.rsi_len)
            if len(series) < 1:
                return None
            return float(series.iloc[-1])
        except Exception:
            return None

    def fetch_rsi_1h(self, symbol: str) -> Optional[float]:
        now = time.time()
        ts, val = self._rsi1h_ttl_cache.get(symbol, (0.0, None))
        if val is not None and (now - ts) <= self.config.rsi1h_ttl_sec:
            self._stat_inc("rsi1h_ttl_hit")
            return val
        self._stat_inc("rsi1h_ttl_miss")
        df = self._fetch_df(symbol, "1h", limit=self.config.rsi_1h_limit)
        if len(df) < max(20, self.config.rsi_len + 2):
            return None
        try:
            rsi_val = float(self.rsi_wilder(df["close"], self.config.rsi_len).iloc[-1])
        except Exception:
            return None
        self._rsi1h_ttl_cache[symbol] = (now, rsi_val)
        return rsi_val

    def fetch_rsi_last2(self, symbol: str, tf: str) -> Tuple[Optional[float], Optional[float]]:
        try:
            series = self._get_rsi_series(symbol, tf, self.config.rsi_len)
            if len(series) < 2:
                return None, None
            prev = float(series.iloc[-2])
            last = float(series.iloc[-1])
            return prev, last
        except Exception:
            return None, None

    def fetch_rsi_last3(self, symbol: str, tf: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        try:
            series = self._get_rsi_series(symbol, tf, self.config.rsi_len)
            if len(series) < 3:
                return None, None, None
            r3 = float(series.iloc[-3])
            r2 = float(series.iloc[-2])
            r1 = float(series.iloc[-1])
            return r3, r2, r1
        except Exception:
            return None, None, None

    def volume_surge_5m(
        self,
        symbol: str,
        return_metrics: bool = False,
        df: Optional[pd.DataFrame] = None,
    ):
        local_df = df if df is not None else self._fetch_df(
            symbol,
            "5m",
            limit=max(22, self.config.vol_surge_lookback + 2),
        )
        if len(local_df) < self.config.vol_surge_lookback + 2:
            return (False, 0.0, 0.0) if return_metrics else False
        try:
            cur = float(local_df["volume"].iloc[-1])
            avg = float(local_df["volume"].iloc[-(self.config.vol_surge_lookback + 1):-1].mean())
            res = avg > 0 and cur >= avg * self.config.vol_surge_mult
            return (res, cur, avg) if return_metrics else res
        except Exception:
            return (False, 0.0, 0.0) if return_metrics else False

    def structural_rejection_5m(self, symbol: str, df: Optional[pd.DataFrame] = None):
        local_df = df.tail(6) if df is not None else self._fetch_df(symbol, "5m", limit=6)
        if len(local_df) < 3:
            return False, {}
        highs = local_df["high"].iloc[-3:].tolist()
        lows = local_df["low"].iloc[-3:].tolist()
        opens = local_df["open"].iloc[-3:].tolist()
        closes = local_df["close"].iloc[-3:].tolist()

        lower_highs = highs[-3] > highs[-2] and highs[-2] > highs[-1]

        def upper_wick_ratio(idx):
            high = highs[idx]
            low = lows[idx]
            o = opens[idx]
            c = closes[idx]
            rng = high - low
            if rng <= 0:
                return 0.0
            upper = high - max(o, c)
            return max(0.0, upper / rng)

        upper_wick_ratio_1 = upper_wick_ratio(-1)
        upper_wick_ratio_2 = upper_wick_ratio(-2)
        wick_reject = upper_wick_ratio_1 >= 0.30 and upper_wick_ratio_2 >= 0.30

        metrics = {
            "highs": highs,
            "lower_highs": lower_highs,
            "upper_wick_ratio_1": upper_wick_ratio_1,
            "upper_wick_ratio_2": upper_wick_ratio_2,
            "wick_reject": wick_reject,
        }
        return (lower_highs or wick_reject), metrics

    def ema20_touch_and_side_5m(self, symbol: str, df: Optional[pd.DataFrame] = None):
        local_df = df if df is not None else self._fetch_df(symbol, "5m", limit=120)
        if len(local_df) < 25:
            return False, False, {}
        try:
            ema20 = self._ema(local_df["close"], 20)
            idx = -2
            last_high = float(local_df["high"].iloc[idx])
            last_close = float(local_df["close"].iloc[idx])
            last_ema20 = float(ema20.iloc[idx])
            threshold_touch = last_ema20 * (1 + self.config.ema20_exit_buffer)
            touched = last_high >= threshold_touch
            close_above = last_close >= last_ema20
            metrics = {
                "high": last_high,
                "close": last_close,
                "ema20": last_ema20,
                "threshold": threshold_touch,
                "ts": float(local_df["ts"].iloc[idx]) / 1000.0 if "ts" in local_df else None,
            }
            return touched, close_above, metrics
        except Exception:
            return False, False, {}

    @staticmethod
    def _ema(values: pd.Series, length: int) -> pd.Series:
        return values.ewm(span=length, adjust=False).mean()

    @staticmethod
    def _atr(df: pd.DataFrame, length: int) -> pd.Series:
        high = df["high"]
        low = df["low"]
        close = df["close"]
        prev_close = close.shift(1)
        tr = pd.concat(
            [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        return tr.rolling(length).mean()

    def build_universe(self, ctx: EngineContext) -> List[str]:
        tickers = ctx.state.get("_tickers")
        if not isinstance(tickers, dict):
            return []
        shared = ctx.state.get("_universe")
        symbols = ctx.state.get("_symbols")
        sym_iter = symbols if isinstance(symbols, list) else list(tickers.keys())
        base_syms = shared if isinstance(shared, list) and shared else sym_iter
        candidates: List[Tuple[str, float]] = []
        for sym in base_syms:
            t = tickers.get(sym)
            if not t:
                continue
            pct = t.get("percentage")
            qv = t.get("quoteVolume")
            if pct is None or qv is None:
                continue
            try:
                pct = float(pct)
                qv = float(qv)
            except Exception:
                continue
            if qv < self.config.min_quote_volume_usdt:
                continue
            if pct <= 0:
                continue
            candidates.append((sym, pct))
        candidates.sort(key=lambda x: x[1], reverse=True)
        out: List[str] = []
        for sym in self.config.anchors:
            if sym not in out:
                out.append(sym)
        for sym, _ in candidates:
            if sym in out:
                continue
            out.append(sym)
            if self.config.universe_top_n and len(out) >= self.config.universe_top_n:
                break
        return out

    def scan_symbol(
        self,
        symbol: str,
        pass_counts: Dict[str, int],
        logger: Any,
    ) -> Optional[RsiScanResult]:
        cfg = self.config
        rsis: Dict[str, Optional[float]] = {}
        rsis_prev: Dict[str, Optional[float]] = {}
        ok_tf = True
        rsi5m_downturn = False
        rsi3m_downturn = False
        r3m_val: Optional[float] = None
        check_status: List[Tuple[str, bool]] = []

        r1h = self.fetch_rsi_1h(symbol)
        rsis["1h"] = r1h
        passed_1h = (r1h is not None) and (r1h >= cfg.thresholds["1h"])
        check_status.append(("1h", passed_1h))
        if passed_1h:
            pass_counts["1h"] += 1

        df_15m = self._fetch_df(symbol, "15m", limit=cfg.rsi_default_limit)
        if len(df_15m) >= 20:
            try:
                r15 = float(self.rsi_wilder(df_15m["close"], cfg.rsi_len).iloc[-1])
            except Exception:
                r15 = None
        else:
            r15 = None
        rsis["15m"] = r15
        passed_15m = (r15 is not None) and (r15 >= cfg.thresholds["15m"])
        check_status.append(("15m", passed_15m))
        if not passed_15m:
            ok_tf = False
        else:
            pass_counts["15m"] += 1

        r5 = self.fetch_rsi(symbol, "5m")
        rsis["5m"] = r5
        passed_5m = (r5 is not None) and (r5 >= cfg.thresholds["5m"])
        check_status.append(("5m", passed_5m))
        if not passed_5m:
            ok_tf = False
        else:
            pass_counts["5m"] += 1

        r5_prev, r5_last = self.fetch_rsi_last2(symbol, "5m")
        rsis_prev["5m"] = r5_prev
        if r5_last is not None:
            rsis["5m"] = r5_last
        rsi5m_downturn = (r5_prev is not None and r5_last is not None and r5_prev > r5_last)

        r3, r2, r1 = self.fetch_rsi_last3(symbol, "3m")
        rsis_prev["3m"] = r2
        rsis["3m"] = r1
        r3m_val = r3
        rsi3m_downturn = (
            (r2 is not None and r1 is not None and r2 >= 85.0 and r2 > r1)
            or (r3 is not None and r2 is not None and r3 >= 85.0 and r3 > r2)
        )
        check_status.append(("3m_downturn", rsi3m_downturn))

        df_5m_once = self._fetch_df(symbol, "5m", limit=cfg.rsi_default_limit)
        vol_ok, vol_cur, vol_avg = self.volume_surge_5m(symbol, return_metrics=True, df=df_5m_once)
        check_status.append(("vol_surge", vol_ok))

        struct_ok, struct_metrics = self.structural_rejection_5m(symbol, df=df_5m_once)
        check_status.append(("struct_5m", struct_ok))

        impulse_block = False
        ema7 = ema20 = ema60 = atr20 = close_5m = None
        if len(df_5m_once) >= 60:
            try:
                ema7 = float(self._ema(df_5m_once["close"], 7).iloc[-1])
                ema20 = float(self._ema(df_5m_once["close"], 20).iloc[-1])
                ema60 = float(self._ema(df_5m_once["close"], 60).iloc[-1])
                atr20 = float(self._atr(df_5m_once, 20).iloc[-1])
                close_5m = float(df_5m_once["close"].iloc[-1])
            except Exception:
                ema7 = ema20 = ema60 = atr20 = close_5m = None
        if (
            close_5m is not None
            and ema7 is not None
            and ema20 is not None
            and ema60 is not None
            and atr20 is not None
        ):
            impulse_block = (
                close_5m > ema7
                and (ema7 - ema20) > atr20 * 0.6
                and (ema20 - ema60) > atr20 * 0.6
            )
        check_status.append(("impulse_ok", not impulse_block))

        pass_count = sum(1 for _, p in check_status if p)
        miss_count = len(check_status) - pass_count
        trigger_ok = (rsi3m_downturn or struct_ok)
        spike_ready = (
            (rsis.get("3m") is not None)
            and (rsis["3m"] >= 85.0)
            and rsi3m_downturn
            and vol_ok
            and passed_1h
            and passed_15m
        )
        struct_ready = ok_tf and vol_ok and struct_ok and rsi5m_downturn and (not impulse_block)
        ready_entry = spike_ready or struct_ready

        if (not ready_entry) and pass_count >= 3 and miss_count <= 2:
            missing = [name for name, p in check_status if not p]
            rsi_info_parts = []
            for tf in cfg.tf_check_order:
                val = rsis.get(tf)
                if tf == "3m":
                    prev = rsis_prev.get(tf)
                    r3disp = (r3m_val if r3m_val is not None else float("nan"))
                    r2disp = (prev if prev is not None else float("nan"))
                    r1disp = (val if val is not None else float("nan"))
                    rsi_info_parts.append(
                        f"3m:{r3disp:.2f},{r2disp:.2f},{r1disp:.2f}/{cfg.thresholds['3m']:.0f}"
                    )
                else:
                    rsi_info_parts.append(
                        f"{tf}:{(val if val is not None else float('nan')):.2f}/{cfg.thresholds[tf]:.0f}"
                    )
            rsi_info = " | ".join(rsi_info_parts)
            vol_info = f"vol cur:{vol_cur:.2f} avg:{vol_avg:.2f} mult:{cfg.vol_surge_mult}"
            struct_info = (
                f"struct lower_highs:{struct_metrics.get('lower_highs')} "
                f"wick_reject:{struct_metrics.get('wick_reject')} "
                f"5m_down:{rsi5m_downturn}"
            )
            logger(
                f"[근접] {symbol} 통과 {pass_count}/{len(check_status)} 미달 {missing} | "
                f"{rsi_info} | {vol_info} | {struct_info}"
            )

        return RsiScanResult(
            ready_entry=ready_entry,
            ok_tf=ok_tf,
            rsi5m_downturn=rsi5m_downturn,
            rsi3m_downturn=rsi3m_downturn,
            rsis=rsis,
            rsis_prev=rsis_prev,
            r3m_val=r3m_val,
            vol_ok=vol_ok,
            vol_cur=float(vol_cur),
            vol_avg=float(vol_avg),
            struct_ok=struct_ok,
            struct_metrics=struct_metrics,
            check_status=check_status,
            pass_count=pass_count,
            miss_count=miss_count,
            trigger_ok=trigger_ok,
            spike_ready=spike_ready,
            struct_ready=struct_ready,
            impulse_block=impulse_block,
        )
