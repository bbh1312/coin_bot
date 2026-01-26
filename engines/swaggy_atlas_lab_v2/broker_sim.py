from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Trade:
    sym: str
    side: str
    entry_ts: int
    entry_index: int
    entry_price: float
    size_usdt: float
    sl_price: float
    tp_price: float
    sw_strength: float = 0.0
    sw_reasons: Optional[list[str]] = None
    atlas_pass: Optional[bool] = None
    atlas_mult: Optional[float] = None
    atlas_reasons: Optional[list[str]] = None
    atlas_shadow_pass: Optional[bool] = None
    atlas_shadow_reasons: Optional[list[str]] = None
    policy_action: Optional[str] = None
    overext_dist_at_entry: Optional[float] = None
    overext_blocked: Optional[bool] = None
    exit_ts: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    mfe: float = 0.0
    mae: float = 0.0
    bars: int = 0
    context: Optional[Dict[str, Any]] = None


class BrokerSim:
    def __init__(
        self,
        tp_pct: float,
        sl_pct: float,
        fee_rate: float,
        slippage_pct: float,
        timeout_bars: int,
        hedge_mode: bool = False,
    ):
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.fee_rate = fee_rate
        self.slippage_pct = slippage_pct
        self.timeout_bars = timeout_bars
        self.hedge_mode = hedge_mode
        self.positions: Dict[object, Trade] = {}

    def _key(self, sym: str, side: Optional[str]) -> object:
        if not self.hedge_mode:
            return sym
        side_key = (side or "").upper()
        return (sym, side_key)

    def has_position(self, sym: str, side: Optional[str] = None) -> bool:
        if not self.hedge_mode:
            return sym in self.positions
        if side:
            return self._key(sym, side) in self.positions
        for key in self.positions.keys():
            if isinstance(key, tuple) and key[0] == sym:
                return True
        return False

    def enter(
        self,
        sym: str,
        side: str,
        ts: int,
        entry_index: int,
        entry_px: float,
        size_usdt: float,
        tp_pct: Optional[float] = None,
        sl_pct: Optional[float] = None,
        sw_strength: float = 0.0,
        sw_reasons: Optional[list[str]] = None,
        atlas_pass: Optional[bool] = None,
        atlas_mult: Optional[float] = None,
        atlas_reasons: Optional[list[str]] = None,
        atlas_shadow_pass: Optional[bool] = None,
        atlas_shadow_reasons: Optional[list[str]] = None,
        policy_action: Optional[str] = None,
        overext_dist_at_entry: Optional[float] = None,
        overext_blocked: Optional[bool] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Trade:
        side = side.upper()
        tp_used = self.tp_pct if tp_pct is None else float(tp_pct)
        sl_used = self.sl_pct if sl_pct is None else float(sl_pct)
        if side == "LONG":
            sl_px = entry_px * (1 - sl_used)
            tp_px = entry_px * (1 + tp_used)
        else:
            sl_px = entry_px * (1 + sl_used)
            tp_px = entry_px * (1 - tp_used)
        trade = Trade(
            sym=sym,
            side=side,
            entry_ts=ts,
            entry_index=entry_index,
            entry_price=entry_px,
            size_usdt=size_usdt,
            sl_price=sl_px,
            tp_price=tp_px,
            sw_strength=sw_strength,
            sw_reasons=sw_reasons,
            atlas_pass=atlas_pass,
            atlas_mult=atlas_mult,
            atlas_reasons=atlas_reasons,
            atlas_shadow_pass=atlas_shadow_pass,
            atlas_shadow_reasons=atlas_shadow_reasons,
            policy_action=policy_action,
            overext_dist_at_entry=overext_dist_at_entry,
            overext_blocked=overext_blocked,
            context=context,
        )
        self.positions[self._key(sym, side)] = trade
        return trade

    def get_trade(self, sym: str, side: Optional[str] = None) -> Optional[Trade]:
        return self.positions.get(self._key(sym, side)) if side or self.hedge_mode else self.positions.get(sym)

    def on_bar(
        self,
        sym: str,
        ts: int,
        high: float,
        low: float,
        close: float,
        bar_index: int,
        side: Optional[str] = None,
    ) -> Optional[Trade]:
        trade = self.positions.get(self._key(sym, side)) if side or self.hedge_mode else self.positions.get(sym)
        if trade is None:
            return None
        trade.bars = max(0, bar_index - trade.entry_index + 1)
        if trade.side == "LONG":
            trade.mfe = max(trade.mfe, (high - trade.entry_price) / trade.entry_price)
            trade.mae = min(trade.mae, (low - trade.entry_price) / trade.entry_price)
            sl_hit = low <= trade.sl_price
            tp_hit = high >= trade.tp_price
        else:
            trade.mfe = max(trade.mfe, (trade.entry_price - low) / trade.entry_price)
            trade.mae = min(trade.mae, (trade.entry_price - high) / trade.entry_price)
            sl_hit = high >= trade.sl_price
            tp_hit = low <= trade.tp_price
        exit_reason = None
        exit_price = None
        if sl_hit and tp_hit:
            exit_reason = "SL"
            exit_price = trade.sl_price
        elif sl_hit:
            exit_reason = "SL"
            exit_price = trade.sl_price
        elif tp_hit:
            exit_reason = "TP"
            exit_price = trade.tp_price
        elif self.timeout_bars > 0 and trade.bars >= self.timeout_bars:
            exit_reason = "TIME"
            exit_price = close
        if exit_reason:
            trade.exit_ts = ts
            trade.exit_price = float(exit_price)
            trade.exit_reason = exit_reason
            self.positions.pop(self._key(sym, trade.side), None)
            return trade
        return None

    def calc_pnl_pct(self, trade: Trade) -> float:
        if trade.exit_price is None or trade.entry_price <= 0:
            return 0.0
        if trade.side == "LONG":
            entry_fill = trade.entry_price * (1 + self.slippage_pct)
            exit_fill = trade.exit_price * (1 - self.slippage_pct)
            fees = entry_fill * self.fee_rate + exit_fill * self.fee_rate
            return (exit_fill - entry_fill - fees) / entry_fill
        entry_fill = trade.entry_price * (1 - self.slippage_pct)
        exit_fill = trade.exit_price * (1 + self.slippage_pct)
        fees = entry_fill * self.fee_rate + exit_fill * self.fee_rate
        return (entry_fill - exit_fill - fees) / entry_fill if entry_fill > 0 else 0.0
