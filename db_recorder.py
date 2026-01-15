import json
import os
import sqlite3
import threading
import time
from typing import Any, Optional


_LOCK = threading.Lock()
_CONN = None

_ENV_ENABLED = str(os.getenv("DB_RECORDING", "")).lower() in ("1", "true", "yes")
_ENV_PATH = os.getenv("DB_PATH", "").strip()
ENABLED = _ENV_ENABLED or bool(_ENV_PATH)
DB_PATH = _ENV_PATH or ("logs/trades.db" if ENABLED else "")


def _get_conn() -> Optional[sqlite3.Connection]:
    global _CONN
    if not ENABLED or not DB_PATH:
        return None
    with _LOCK:
        if _CONN is not None:
            return _CONN
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        _init_db(conn)
        _CONN = conn
        return _CONN


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS engine_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            ts REAL,
            symbol TEXT,
            side TEXT,
            engine TEXT,
            reason TEXT,
            meta_json TEXT
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            created_at TEXT,
            ts REAL,
            symbol TEXT,
            side TEXT,
            order_type TEXT,
            status TEXT,
            price REAL,
            qty REAL,
            engine TEXT,
            client_order_id TEXT,
            raw_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fills (
            fill_id TEXT PRIMARY KEY,
            created_at TEXT,
            ts REAL,
            order_id TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            qty REAL,
            fee REAL,
            fee_asset TEXT,
            raw_json TEXT
        );
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            ts REAL,
            symbol TEXT,
            side TEXT,
            qty REAL,
            avg_entry REAL,
            unreal_pnl REAL,
            realized_pnl REAL,
            source TEXT
        );
        CREATE TABLE IF NOT EXISTS cancels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            ts REAL,
            order_id TEXT,
            symbol TEXT,
            side TEXT,
            reason TEXT,
            raw_json TEXT
        );
        CREATE TABLE IF NOT EXISTS income (
            income_id TEXT PRIMARY KEY,
            created_at TEXT,
            ts REAL,
            symbol TEXT,
            side TEXT,
            income REAL,
            income_type TEXT,
            asset TEXT,
            raw_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_engine_signals_ts_symbol ON engine_signals (ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_orders_ts_symbol ON orders (ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_orders_symbol_status ON orders (symbol, status);
        CREATE INDEX IF NOT EXISTS idx_fills_ts_symbol ON fills (ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills (order_id);
        CREATE INDEX IF NOT EXISTS idx_positions_ts_symbol ON positions (ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_cancels_ts_symbol ON cancels (ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_income_ts_symbol ON income (ts, symbol);
        """
    )
    for table in ("engine_signals", "orders", "fills", "positions", "cancels"):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN created_at TEXT")
        except Exception:
            pass
    try:
        conn.execute("ALTER TABLE income ADD COLUMN created_at TEXT")
    except Exception:
        pass
    conn.commit()


def _to_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def _json(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return None

def _kst_str(ts: Optional[float]) -> str:
    base = float(ts if ts is not None else time.time())
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(base + 9 * 3600))


def record_engine_signal(
    symbol: str,
    side: str,
    engine: Optional[str],
    reason: Optional[str],
    meta: Optional[dict],
    ts: Optional[float] = None,
) -> None:
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            """
            INSERT INTO engine_signals (created_at, ts, symbol, side, engine, reason, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (_kst_str(ts_val), ts_val, symbol, side, engine, reason, _json(meta)),
        )
        conn.commit()


def record_order(
    order_id: Optional[str],
    symbol: str,
    side: Optional[str],
    order_type: Optional[str],
    status: Optional[str],
    price: Any,
    qty: Any,
    ts: Optional[float] = None,
    engine: Optional[str] = None,
    client_order_id: Optional[str] = None,
    raw: Any = None,
) -> None:
    if not order_id:
        return
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            """
            INSERT INTO orders (order_id, created_at, ts, symbol, side, order_type, status, price, qty, engine, client_order_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                created_at=excluded.created_at,
                ts=excluded.ts,
                symbol=excluded.symbol,
                side=excluded.side,
                order_type=excluded.order_type,
                status=excluded.status,
                price=excluded.price,
                qty=excluded.qty,
                engine=excluded.engine,
                client_order_id=excluded.client_order_id,
                raw_json=excluded.raw_json
            """,
            (
                str(order_id),
                _kst_str(ts_val),
                ts_val,
                symbol,
                side,
                order_type,
                status,
                _to_float(price),
                _to_float(qty),
                engine,
                client_order_id,
                _json(raw),
            ),
        )
        conn.commit()


def record_fill(
    fill_id: Optional[str],
    order_id: Optional[str],
    symbol: str,
    side: Optional[str],
    price: Any,
    qty: Any,
    fee: Any,
    fee_asset: Optional[str],
    ts: Optional[float] = None,
    raw: Any = None,
) -> None:
    if not fill_id:
        return
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            """
            INSERT INTO fills (fill_id, created_at, ts, order_id, symbol, side, price, qty, fee, fee_asset, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fill_id) DO UPDATE SET
                created_at=excluded.created_at,
                ts=excluded.ts,
                order_id=excluded.order_id,
                symbol=excluded.symbol,
                side=excluded.side,
                price=excluded.price,
                qty=excluded.qty,
                fee=excluded.fee,
                fee_asset=excluded.fee_asset,
                raw_json=excluded.raw_json
            """,
            (
                str(fill_id),
                _kst_str(ts_val),
                ts_val,
                str(order_id) if order_id else None,
                symbol,
                side,
                _to_float(price),
                _to_float(qty),
                _to_float(fee),
                fee_asset,
                _json(raw),
            ),
        )
        conn.commit()


def record_position_snapshot(
    symbol: str,
    side: Optional[str],
    qty: Any,
    avg_entry: Any,
    unreal_pnl: Any,
    realized_pnl: Any,
    ts: Optional[float] = None,
    source: Optional[str] = None,
) -> None:
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            """
            INSERT INTO positions (created_at, ts, symbol, side, qty, avg_entry, unreal_pnl, realized_pnl, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _kst_str(ts_val),
                ts_val,
                symbol,
                side,
                _to_float(qty),
                _to_float(avg_entry),
                _to_float(unreal_pnl),
                _to_float(realized_pnl),
                source,
            ),
        )
        conn.commit()


def record_cancel(
    order_id: Optional[str],
    symbol: str,
    side: Optional[str],
    ts: Optional[float] = None,
    reason: Optional[str] = None,
    raw: Any = None,
) -> None:
    if not order_id:
        return
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            "INSERT INTO cancels (created_at, ts, order_id, symbol, side, reason, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (_kst_str(ts_val), ts_val, str(order_id), symbol, side, reason, _json(raw)),
        )
        conn.commit()


def record_income(
    income_id: Optional[str],
    symbol: str,
    side: Optional[str],
    income: Any,
    income_type: Optional[str],
    asset: Optional[str],
    ts: Optional[float] = None,
    raw: Any = None,
) -> None:
    if not income_id:
        return
    conn = _get_conn()
    if conn is None:
        return
    ts_val = float(ts if ts is not None else time.time())
    with _LOCK:
        conn.execute(
            """
            INSERT INTO income (income_id, created_at, ts, symbol, side, income, income_type, asset, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(income_id) DO UPDATE SET
                created_at=excluded.created_at,
                ts=excluded.ts,
                symbol=excluded.symbol,
                side=excluded.side,
                income=excluded.income,
                income_type=excluded.income_type,
                asset=excluded.asset,
                raw_json=excluded.raw_json
            """,
            (
                str(income_id),
                _kst_str(ts_val),
                ts_val,
                symbol,
                side,
                _to_float(income),
                income_type,
                asset,
                _json(raw),
            ),
        )
        conn.commit()


def record_fills_from_order(order: dict, symbol: str, side: Optional[str]) -> None:
    trades = order.get("trades") if isinstance(order.get("trades"), list) else []
    if not trades:
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        fills = info.get("fills") if isinstance(info.get("fills"), list) else []
        trades = fills
    if not trades:
        return
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    for tr in trades:
        if not isinstance(tr, dict):
            continue
        fill_id = tr.get("id") or tr.get("tradeId")
        price = tr.get("price") or tr.get("avgPrice") or tr.get("fillPrice")
        qty = tr.get("amount") or tr.get("qty") or tr.get("executedQty")
        fee = None
        fee_asset = None
        fee_obj = tr.get("fee")
        if isinstance(fee_obj, dict):
            fee = fee_obj.get("cost")
            fee_asset = fee_obj.get("currency")
        else:
            fee = tr.get("commission") or tr.get("fee")
            fee_asset = tr.get("commissionAsset")
        ts = tr.get("timestamp") or tr.get("time")
        if isinstance(ts, (int, float)):
            ts = float(ts) / 1000.0 if ts > 1e12 else float(ts)
        record_fill(
            fill_id=fill_id,
            order_id=order_id,
            symbol=symbol,
            side=side,
            price=price,
            qty=qty,
            fee=fee,
            fee_asset=fee_asset,
            ts=ts,
            raw=tr,
        )
