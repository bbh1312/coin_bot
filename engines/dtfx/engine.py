from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

import cycle_cache
from engines.base import BaseEngine, EngineContext, Signal
from engines.dtfx.config import (
    MIN_QVOL_24H_USDT,
    UNIVERSE_TOP_N,
    LOW_LIQUIDITY_QVOL_24H_USDT,
    ANCHOR_SYMBOLS,
    MAJOR_SYMBOLS,
    get_default_params,
)
from engines.dtfx.core.types import Candle
from engines.dtfx.dtfx_long import DTFXLongEngine
from engines.dtfx.dtfx_short import DTFXShortEngine
from engines.universe import build_universe_from_tickers


@dataclass
class DTFXConfig:
    tf_ltf: str = "1m"
    tf_mtf: str = "15m"
    tf_htf: str = "1h"
    ltf_limit: int = 200
    mtf_limit: int = 200
    htf_limit: int = 200
    universe_top_n: int = UNIVERSE_TOP_N
    min_quote_volume_usdt: float = MIN_QVOL_24H_USDT
    low_liquidity_qv_usdt: float = LOW_LIQUIDITY_QVOL_24H_USDT
    anchor_symbols: List[str] = None
    major_symbols: List[str] = None
    enable_long: bool = True
    enable_short: bool = True


def _normalize_symbol_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    out = []
    seen = set()
    for item in items:
        s = str(item).strip().upper()
        if not s:
            continue
        s = s.split("/")[0]
        s = s.split(":")[0]
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _df_to_candles(df: pd.DataFrame) -> List[Candle]:
    if df.empty:
        return []
    records = df.to_dict("records")
    candles = []
    for row in records:
        try:
            candles.append(
                Candle(
                    ts=int(row["ts"]),
                    o=float(row["open"]),
                    h=float(row["high"]),
                    l=float(row["low"]),
                    c=float(row["close"]),
                    v=float(row["volume"]),
                )
            )
        except Exception:
            continue
    return candles


class DTFXEngine(BaseEngine):
    name = "dtfx"

    def __init__(self, config: Optional[DTFXConfig] = None):
        self.config = config or DTFXConfig()
        if self.config.anchor_symbols is None:
            self.config.anchor_symbols = list(ANCHOR_SYMBOLS)
        if self.config.major_symbols is None:
            self.config.major_symbols = list(MAJOR_SYMBOLS)
        self._params_base = get_default_params()
        self._params_base["major_symbols"] = set(_normalize_symbol_list(self.config.major_symbols))
        self._params_base["anchor_symbols"] = set(_normalize_symbol_list(self.config.anchor_symbols))
        self._tfs = {"ltf": self.config.tf_ltf, "mtf": self.config.tf_mtf, "htf": self.config.tf_htf}
        self._long_engine: Optional[DTFXLongEngine] = None
        self._short_engine: Optional[DTFXShortEngine] = None

    def on_start(self, ctx: EngineContext) -> None:
        self._ensure_engines(ctx.exchange)
        ctx.logger(f"[dtfx] engine start name={self.name}")

    def _ensure_engines(self, exchange: Any) -> None:
        if self._long_engine is None:
            params = dict(self._params_base)
            self._long_engine = DTFXLongEngine(params, dict(self._tfs), exchange)
        if self._short_engine is None:
            params = dict(self._params_base)
            self._short_engine = DTFXShortEngine(params, dict(self._tfs), exchange)

    def build_universe(self, ctx: EngineContext) -> List[str]:
        shared = ctx.state.get("_universe")
        if isinstance(shared, list) and shared:
            return list(shared)
        tickers = ctx.state.get("_tickers")
        if not isinstance(tickers, dict):
            return []
        symbols = ctx.state.get("_symbols")
        sym_iter = symbols if isinstance(symbols, list) else list(tickers.keys())
        min_qv = max(self.config.min_quote_volume_usdt, self.config.low_liquidity_qv_usdt)
        anchors = []
        for s in self.config.anchor_symbols:
            if "/" in s:
                anchors.append(s)
            else:
                anchors.append(f"{s}/USDT:USDT")
        return build_universe_from_tickers(
            tickers,
            symbols=sym_iter,
            min_quote_volume_usdt=min_qv,
            top_n=self.config.universe_top_n,
            anchors=tuple(anchors),
        )

    def scan_symbol(self, ctx: EngineContext, symbol: str) -> List[Signal]:
        self._ensure_engines(ctx.exchange)
        df_ltf = cycle_cache.get_df(symbol, self.config.tf_ltf, self.config.ltf_limit)
        df_mtf = cycle_cache.get_df(symbol, self.config.tf_mtf, self.config.mtf_limit)
        df_htf = cycle_cache.get_df(symbol, self.config.tf_htf, self.config.htf_limit)
        if df_ltf.empty or df_mtf.empty or df_htf.empty:
            return []
        df_ltf = df_ltf.iloc[:-1]
        df_mtf = df_mtf.iloc[:-1]
        df_htf = df_htf.iloc[:-1]
        ltf = _df_to_candles(df_ltf)
        mtf = _df_to_candles(df_mtf)
        htf = _df_to_candles(df_htf)
        if not ltf or not mtf or not htf:
            return []
        signals: List[Signal] = []
        if self.config.enable_long and self._long_engine:
            sig = self._long_engine.on_candle(symbol, ltf, mtf, htf)
            base_sig = self._to_base_signal(sig, "LONG")
            if base_sig:
                signals.append(base_sig)
        if self.config.enable_short and self._short_engine:
            sig = self._short_engine.on_candle(symbol, ltf, mtf, htf)
            base_sig = self._to_base_signal(sig, "SHORT")
            if base_sig:
                signals.append(base_sig)
        return signals

    def _to_base_signal(self, dtfx_signal: Any, side: str) -> Optional[Signal]:
        if not dtfx_signal:
            return None
        event = getattr(dtfx_signal, "event", "") or ""
        if not event.startswith("ENTRY_READY"):
            return None
        symbol = getattr(dtfx_signal, "symbol", None)
        context = getattr(dtfx_signal, "context", None) or {}
        dtfx_engine_name = getattr(dtfx_signal, "engine", "dtfx")
        dtfx_state = getattr(dtfx_signal, "state", "")
        dtfx_tf_ltf = getattr(dtfx_signal, "tf_ltf", self.config.tf_ltf)
        dtfx_tf_mtf = getattr(dtfx_signal, "tf_mtf", self.config.tf_mtf)
        dtfx_tf_htf = getattr(dtfx_signal, "tf_htf", self.config.tf_htf)
        touch = context.get("touch") if isinstance(context, dict) else None
        entry_price = None
        if isinstance(touch, dict):
            entry_price = touch.get("price")
        if not isinstance(entry_price, (int, float)):
            entry_price = None
        return Signal(
            symbol=symbol or "",
            side=side,
            pattern=event,
            entry_price=float(entry_price) if isinstance(entry_price, (int, float)) else 0.0,
            sl=0.0,
            targets=[],
            meta={
                "dtfx": context,
                "dtfx_event": event,
                "dtfx_engine": dtfx_engine_name,
                "dtfx_state": dtfx_state,
                "dtfx_tf_ltf": dtfx_tf_ltf,
                "dtfx_tf_mtf": dtfx_tf_mtf,
                "dtfx_tf_htf": dtfx_tf_htf,
            },
        )

    def on_tick(self, ctx: EngineContext, symbol: str) -> Optional[Signal]:
        signals = self.scan_symbol(ctx, symbol)
        return signals[0] if signals else None
