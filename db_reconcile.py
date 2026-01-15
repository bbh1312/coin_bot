import time
from typing import Iterable, Optional

import db_recorder as dbrec


def _to_ts(val) -> Optional[float]:
    try:
        val = float(val)
    except Exception:
        return None
    return val / 1000.0 if val > 1e12 else val


def _income_id_from_row(row: dict) -> Optional[str]:
    if not isinstance(row, dict):
        return None
    iid = row.get("tranId") or row.get("id") or row.get("incomeId")
    if iid:
        return str(iid)
    ts = row.get("time")
    symbol = row.get("symbol")
    income = row.get("income")
    if ts and symbol and income is not None:
        return f"{ts}:{symbol}:{income}"
    return None


def sync_income(exchange, since_ts: Optional[float] = None) -> int:
    if not dbrec.ENABLED:
        return 0
    start_ms = int(float(since_ts) * 1000) if since_ts else None
    now_ms = int(time.time() * 1000)
    fetched = 0
    if start_ms is None:
        start_ms = now_ms - (3 * 86400 * 1000)
    since = start_ms
    while True:
        params = {
            "incomeType": "REALIZED_PNL",
            "startTime": since,
            "endTime": now_ms,
            "limit": 1000,
        }
        try:
            if hasattr(exchange, "fapiPrivateGetIncome"):
                batch = exchange.fapiPrivateGetIncome(params)
            else:
                batch = exchange.fapiPrivate_get_income(params)
        except Exception:
            break
        if not batch:
            break
        last_ts = None
        for row in batch:
            if not isinstance(row, dict):
                continue
            ts = row.get("time")
            ts_val = _to_ts(ts) or time.time()
            if ts_val < (start_ms / 1000.0):
                continue
            income_id = _income_id_from_row(row)
            symbol = row.get("symbol") or ""
            side = row.get("positionSide") or row.get("side")
            income = row.get("income")
            income_type = row.get("incomeType")
            asset = row.get("asset")
            dbrec.record_income(
                income_id=income_id,
                symbol=symbol,
                side=side,
                income=income,
                income_type=income_type,
                asset=asset,
                ts=ts_val,
                raw=row,
            )
            fetched += 1
            try:
                ts_ms = int(float(ts))
                if last_ts is None or ts_ms > last_ts:
                    last_ts = ts_ms
            except Exception:
                continue
        if last_ts is None:
            break
        since = last_ts + 1
        if last_ts >= now_ms:
            break
    return fetched


def sync_exchange_state(exchange, since_ts: Optional[float] = None, symbols: Optional[Iterable[str]] = None) -> dict:
    stats = {"orders": 0, "fills": 0, "positions": 0}
    if not dbrec.ENABLED:
        return stats
    sym_list = list(symbols) if symbols else []
    try:
        positions = exchange.fetch_positions(sym_list) if sym_list else exchange.fetch_positions()
    except Exception:
        positions = []
    for p in positions or []:
        info = p.get("info") or {}
        symbol = p.get("symbol") or info.get("symbol")
        if not symbol:
            continue
        side = info.get("positionSide") or p.get("positionSide")
        qty = info.get("positionAmt") or p.get("positionAmt") or p.get("contracts")
        avg_entry = info.get("entryPrice") or p.get("entryPrice")
        unreal = info.get("unRealizedProfit") or p.get("unrealizedPnl")
        realized = info.get("realizedPnl") or p.get("realizedPnl")
        ts = _to_ts(info.get("updateTime") or p.get("timestamp")) or time.time()
        dbrec.record_position_snapshot(
            symbol=symbol,
            side=side,
            qty=qty,
            avg_entry=avg_entry,
            unreal_pnl=unreal,
            realized_pnl=realized,
            ts=ts,
            source="sync_exchange_state",
        )
        stats["positions"] += 1

    if sym_list:
        open_orders = []
        for sym in sym_list:
            try:
                open_orders.extend(exchange.fetch_open_orders(sym) or [])
            except Exception:
                continue
    else:
        try:
            open_orders = exchange.fetch_open_orders()
        except Exception:
            open_orders = []
    for o in open_orders or []:
        symbol = o.get("symbol")
        side = o.get("side") or (o.get("info") or {}).get("side")
        order_id = o.get("id") or (o.get("info") or {}).get("orderId")
        ts = _to_ts(o.get("timestamp")) or time.time()
        dbrec.record_order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=o.get("type"),
            status=o.get("status") or "open",
            price=o.get("price"),
            qty=o.get("amount"),
            ts=ts,
            engine=None,
            client_order_id=o.get("clientOrderId") or (o.get("info") or {}).get("clientOrderId"),
            raw=o,
        )
        stats["orders"] += 1

    since_ms = int(float(since_ts) * 1000) if since_ts else None
    if sym_list:
        symbols_iter = sym_list
    else:
        symbols_iter = [None]
    for sym in symbols_iter:
        try:
            trades = exchange.fetch_my_trades(sym, since=since_ms) if sym else exchange.fetch_my_trades(since=since_ms)
        except Exception:
            trades = []
        for tr in trades or []:
            symbol = tr.get("symbol") or sym
            side = tr.get("side")
            order_id = tr.get("order") or tr.get("orderId")
            fill_id = tr.get("id") or tr.get("tradeId")
            ts = _to_ts(tr.get("timestamp")) or time.time()
            fee_obj = tr.get("fee") or {}
            fee = fee_obj.get("cost") if isinstance(fee_obj, dict) else None
            fee_asset = fee_obj.get("currency") if isinstance(fee_obj, dict) else None
            dbrec.record_fill(
                fill_id=fill_id,
                order_id=order_id,
                symbol=symbol,
                side=side,
                price=tr.get("price"),
                qty=tr.get("amount"),
                fee=fee,
                fee_asset=fee_asset,
                ts=ts,
                raw=tr,
            )
            stats["fills"] += 1
    return stats


def reconcile_positions(exchange, symbols: Optional[Iterable[str]] = None) -> dict:
    return sync_exchange_state(exchange, since_ts=None, symbols=symbols)


def backfill_fills(exchange, since_ts: Optional[float], symbols: Optional[Iterable[str]] = None) -> dict:
    return sync_exchange_state(exchange, since_ts=since_ts, symbols=symbols)


def backfill_orders(exchange, since_ts: Optional[float], symbols: Optional[Iterable[str]] = None) -> dict:
    return sync_exchange_state(exchange, since_ts=since_ts, symbols=symbols)
