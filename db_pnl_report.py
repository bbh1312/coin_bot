import argparse
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class PositionState:
    qty: float = 0.0
    avg_entry: float = 0.0
    realized_pnl: float = 0.0
    realized_pnl_net: float = 0.0
    trade_count: int = 0
    qty_sum: float = 0.0
    notional_sum: float = 0.0
    fee_usdt: float = 0.0
    fee_other: float = 0.0


def _to_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def _income_rows(conn: sqlite3.Connection, since_ts: float):
    cur = conn.cursor()
    try:
        return cur.execute(
            """
            SELECT ts, symbol, side, income, income_type, asset, raw_json
            FROM income
            WHERE ts >= ? AND income_type = 'REALIZED_PNL'
            ORDER BY ts ASC
            """,
            (since_ts,),
        ).fetchall()
    except Exception:
        return []

def _fill_side_map(conn: sqlite3.Connection, since_ts: float) -> Dict[str, str]:
    cur = conn.cursor()
    try:
        rows = cur.execute(
            "SELECT fill_id, side FROM fills WHERE ts >= ?",
            (since_ts,),
        ).fetchall()
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for fill_id, side in rows:
        if fill_id:
            out[str(fill_id)] = str(side or "").lower()
    return out


def _load_fills(conn: sqlite3.Connection, since_ts: float):
    cur = conn.cursor()
    return cur.execute(
        """
        SELECT ts, symbol, side, price, qty, fee, fee_asset, raw_json
        FROM fills
        WHERE ts >= ?
        ORDER BY ts ASC
        """,
        (since_ts,),
    ).fetchall()


def _extract_trade_side(raw: dict, side_field: Optional[str]) -> Optional[str]:
    if isinstance(raw, dict):
        side = raw.get("side")
        if isinstance(side, str):
            side = side.lower()
            if side in ("buy", "sell"):
                return side
        is_buyer = raw.get("isBuyer")
        if is_buyer is True:
            return "buy"
        if is_buyer is False:
            return "sell"
    if isinstance(side_field, str):
        sf = side_field.lower()
        if sf in ("buy", "sell"):
            return sf
    return None


def _extract_position_side(raw: dict, side_field: Optional[str]) -> Optional[str]:
    if isinstance(raw, dict):
        ps = raw.get("positionSide") or (raw.get("info") or {}).get("positionSide")
        if isinstance(ps, str) and ps.upper() in ("LONG", "SHORT"):
            return ps.upper()
    if isinstance(side_field, str) and side_field.upper() in ("LONG", "SHORT"):
        return side_field.upper()
    return None


def _apply_long(ps: PositionState, trade_side: str, price: float, qty: float, fee_usdt: float):
    if trade_side == "buy":
        new_qty = ps.qty + qty
        if new_qty > 0:
            ps.avg_entry = (ps.avg_entry * ps.qty + price * qty) / new_qty if ps.qty > 0 else price
        ps.qty = new_qty
    elif trade_side == "sell":
        closed = min(qty, ps.qty)
        if closed > 0:
            delta = (price - ps.avg_entry) * closed
            ps.realized_pnl += delta
            ps.realized_pnl_net += delta
        ps.qty = max(ps.qty - closed, 0.0)
    ps.realized_pnl_net -= fee_usdt


def _apply_short(ps: PositionState, trade_side: str, price: float, qty: float, fee_usdt: float):
    if trade_side == "sell":
        new_qty = ps.qty + qty
        if new_qty > 0:
            ps.avg_entry = (ps.avg_entry * ps.qty + price * qty) / new_qty if ps.qty > 0 else price
        ps.qty = new_qty
    elif trade_side == "buy":
        closed = min(qty, ps.qty)
        if closed > 0:
            delta = (ps.avg_entry - price) * closed
            ps.realized_pnl += delta
            ps.realized_pnl_net += delta
        ps.qty = max(ps.qty - closed, 0.0)
    ps.realized_pnl_net -= fee_usdt


def _apply_net(ps: PositionState, trade_side: str, price: float, qty: float, fee_usdt: float):
    if trade_side == "buy":
        if ps.qty >= 0:
            new_qty = ps.qty + qty
            ps.avg_entry = (ps.avg_entry * ps.qty + price * qty) / new_qty if ps.qty > 0 else price
            ps.qty = new_qty
        else:
            closed = min(qty, abs(ps.qty))
            if closed > 0:
                delta = (ps.avg_entry - price) * closed
                ps.realized_pnl += delta
                ps.realized_pnl_net += delta
            ps.qty += closed
            remaining = qty - closed
            if remaining > 0:
                ps.avg_entry = price
                ps.qty = remaining
    elif trade_side == "sell":
        if ps.qty <= 0:
            new_qty = abs(ps.qty) + qty
            ps.avg_entry = (ps.avg_entry * abs(ps.qty) + price * qty) / new_qty if ps.qty < 0 else price
            ps.qty = -new_qty
        else:
            closed = min(qty, ps.qty)
            if closed > 0:
                delta = (price - ps.avg_entry) * closed
                ps.realized_pnl += delta
                ps.realized_pnl_net += delta
            ps.qty -= closed
            remaining = qty - closed
            if remaining > 0:
                ps.avg_entry = price
                ps.qty = -remaining
    ps.realized_pnl_net -= fee_usdt


def build_report(db_path: str, since_sec: int, out_path: str, daily_out_path: str):
    since_ts = time.time() - since_sec
    conn = sqlite3.connect(db_path)
    income_rows = _income_rows(conn, since_ts)
    rows = _load_fills(conn, since_ts) if not income_rows else []
    fill_side_by_id = _fill_side_map(conn, since_ts) if income_rows else {}

    states: Dict[Tuple[str, str], PositionState] = {}
    daily: Dict[Tuple[str, str, str], PositionState] = {}

    if income_rows:
        for ts, symbol, side_field, income, income_type, asset, raw_json in income_rows:
            if not symbol:
                continue
            side = (side_field or "").upper()
            if side not in ("LONG", "SHORT"):
                side = "NET"
            pnl = _to_float(income)
            if pnl is None:
                continue
            trade_id = None
            # Map tradeId -> side using fills
            if side == "NET":
                raw_obj = None
                if raw_json:
                    try:
                        raw_obj = json.loads(raw_json)
                    except Exception:
                        raw_obj = None
                if isinstance(raw_obj, dict):
                    trade_id = raw_obj.get("tradeId") or raw_obj.get("id")
                if trade_id and str(trade_id) in fill_side_by_id:
                    fside = fill_side_by_id.get(str(trade_id))
                    if fside == "sell":
                        side = "LONG"
                    elif fside == "buy":
                        side = "SHORT"
            key = (symbol, side)
            ps = states.setdefault(key, PositionState())
            ps.trade_count += 1
            ps.realized_pnl += pnl
            ps.realized_pnl_net += pnl
            day = time.strftime("%Y-%m-%d", time.gmtime(float(ts) + 9 * 3600))
            dkey = (day, symbol, side)
            dps = daily.setdefault(dkey, PositionState())
            dps.trade_count += 1
            dps.realized_pnl += pnl
            dps.realized_pnl_net += pnl
        # Skip fill-based aggregation when income is available.
        rows = []

    for ts, symbol, side_field, price, qty, fee, fee_asset, raw_json in rows:
        p = _to_float(price)
        q = _to_float(qty)
        if p is None or q is None or q <= 0:
            continue
        raw = None
        if raw_json:
            try:
                raw = json.loads(raw_json)
            except Exception:
                raw = None
        trade_side = _extract_trade_side(raw, side_field)
        if trade_side not in ("buy", "sell"):
            continue
        pos_side = _extract_position_side(raw, side_field)
        pos_key = pos_side if pos_side in ("LONG", "SHORT") else "NET"
        key = (symbol, pos_key)
        ps = states.setdefault(key, PositionState())
        ps.trade_count += 1
        ps.qty_sum += q
        ps.notional_sum += p * q
        fee_usdt = 0.0
        if fee_asset and str(fee_asset).upper() == "USDT":
            fee_usdt = _to_float(fee) or 0.0
            ps.fee_usdt += fee_usdt
        elif fee is not None:
            ps.fee_other += _to_float(fee) or 0.0

        day = time.strftime("%Y-%m-%d", time.gmtime(float(ts) + 9 * 3600))
        dkey = (day, symbol, pos_key)
        dps = daily.setdefault(dkey, PositionState())
        dps.trade_count += 1
        dps.qty_sum += q
        dps.notional_sum += p * q
        if fee_asset and str(fee_asset).upper() == "USDT":
            dps.fee_usdt += fee_usdt
        elif fee is not None:
            dps.fee_other += _to_float(fee) or 0.0

        if pos_key == "LONG":
            _apply_long(ps, trade_side, p, q, fee_usdt)
            _apply_long(dps, trade_side, p, q, fee_usdt)
        elif pos_key == "SHORT":
            _apply_short(ps, trade_side, p, q, fee_usdt)
            _apply_short(dps, trade_side, p, q, fee_usdt)
        else:
            _apply_net(ps, trade_side, p, q, fee_usdt)
            _apply_net(dps, trade_side, p, q, fee_usdt)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("symbol,pos_side,realized_pnl,realized_pnl_net,trade_count,qty_sum,notional_sum,fee_usdt,fee_other\n")
        for (symbol, pos_side), ps in sorted(states.items()):
            f.write(
                f"{symbol},{pos_side},{ps.realized_pnl:.6f},{ps.realized_pnl_net:.6f},{ps.trade_count},{ps.qty_sum:.8f},{ps.notional_sum:.8f},{ps.fee_usdt:.6f},{ps.fee_other:.6f}\n"
            )
    total = PositionState()
    for ps in states.values():
        total.realized_pnl += ps.realized_pnl
        total.realized_pnl_net += ps.realized_pnl_net
        total.trade_count += ps.trade_count
        total.qty_sum += ps.qty_sum
        total.notional_sum += ps.notional_sum
        total.fee_usdt += ps.fee_usdt
        total.fee_other += ps.fee_other

    with open(daily_out_path, "w", encoding="utf-8") as f:
        f.write("day,symbol,pos_side,realized_pnl,realized_pnl_net,trade_count,qty_sum,notional_sum,fee_usdt,fee_other\n")
        for (day, symbol, pos_side), ps in sorted(daily.items()):
            f.write(
                f"{day},{symbol},{pos_side},{ps.realized_pnl:.6f},{ps.realized_pnl_net:.6f},{ps.trade_count},{ps.qty_sum:.8f},{ps.notional_sum:.8f},{ps.fee_usdt:.6f},{ps.fee_other:.6f}\n"
            )
    daily_total: Dict[str, PositionState] = {}
    for (day, _symbol, _pos_side), ps in daily.items():
        d = daily_total.setdefault(day, PositionState())
        d.realized_pnl += ps.realized_pnl
        d.realized_pnl_net += ps.realized_pnl_net
        d.trade_count += ps.trade_count
        d.qty_sum += ps.qty_sum
        d.notional_sum += ps.notional_sum
        d.fee_usdt += ps.fee_usdt
        d.fee_other += ps.fee_other
    total_out = os.path.join(os.path.dirname(out_path), "db_pnl_total_last3d.csv")
    with open(total_out, "w", encoding="utf-8") as f:
        f.write("label,realized_pnl,realized_pnl_net,trade_count,qty_sum,notional_sum,fee_usdt,fee_other\n")
        f.write(
            f"TOTAL,{total.realized_pnl:.6f},{total.realized_pnl_net:.6f},{total.trade_count},{total.qty_sum:.8f},{total.notional_sum:.8f},{total.fee_usdt:.6f},{total.fee_other:.6f}\n"
        )
        for day in sorted(daily_total.keys()):
            ps = daily_total[day]
            f.write(
                f"{day},{ps.realized_pnl:.6f},{ps.realized_pnl_net:.6f},{ps.trade_count},{ps.qty_sum:.8f},{ps.notional_sum:.8f},{ps.fee_usdt:.6f},{ps.fee_other:.6f}\n"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="logs/trades.db")
    parser.add_argument("--since-sec", type=int, default=3 * 86400)
    parser.add_argument("--out", default="reports/db_pnl_last3d.csv")
    parser.add_argument("--out-daily", default="reports/db_pnl_daily_last3d.csv")
    args = parser.parse_args()
    build_report(args.db, args.since_sec, args.out, args.out_daily)
    print({"out": args.out, "out_daily": args.out_daily})


if __name__ == "__main__":
    main()
