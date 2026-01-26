import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.getenv("ACCOUNTS_DB_PATH", "logs/trades.db")

DEFAULT_SETTINGS = {
    "entry_pct": 30.0,
    "dry_run": 1,
    "auto_exit": 0,
    "max_positions": 12,
    "leverage": 10,
    "margin_mode": "cross",
    "exit_cooldown_h": 2.0,
    "long_tp_pct": 3.0,
    "long_sl_pct": 3.0,
    "short_tp_pct": 3.0,
    "short_sl_pct": 3.0,
    "dca_enabled": 1,
    "dca_pct": 2.0,
    "dca1_pct": 30.0,
    "dca2_pct": 30.0,
    "dca3_pct": 30.0,
}


def _connect() -> sqlite3.Connection:
    base_dir = os.path.dirname(DB_PATH) or "."
    os.makedirs(base_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                api_key TEXT NOT NULL,
                api_secret TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS account_settings (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id),
                entry_pct REAL NOT NULL,
                dry_run INTEGER NOT NULL DEFAULT 1,
                auto_exit INTEGER NOT NULL DEFAULT 0,
                max_positions INTEGER NOT NULL DEFAULT 7,
                leverage INTEGER NOT NULL DEFAULT 10,
                margin_mode TEXT NOT NULL DEFAULT "cross",
                exit_cooldown_h REAL NOT NULL DEFAULT 2.0,
                long_tp_pct REAL NOT NULL DEFAULT 3.0,
                long_sl_pct REAL NOT NULL DEFAULT 3.0,
                short_tp_pct REAL NOT NULL DEFAULT 3.0,
                short_sl_pct REAL NOT NULL DEFAULT 3.0,
                dca_enabled INTEGER NOT NULL DEFAULT 1,
                dca_pct REAL NOT NULL DEFAULT 2.0,
                dca1_pct REAL NOT NULL DEFAULT 30.0,
                dca2_pct REAL NOT NULL DEFAULT 30.0,
                dca3_pct REAL NOT NULL DEFAULT 30.0,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS telegram_bots (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id),
                bot_token TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                last_update_id INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_accounts_name ON accounts (name);
            CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts (is_active);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _fetch_one(conn: sqlite3.Connection, query: str, params: Tuple[Any, ...]) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(query, params)
    row = cur.fetchone()
    return row


def _fetch_all(conn: sqlite3.Connection, query: str, params: Tuple[Any, ...]) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(query, params)
    return cur.fetchall()


def ensure_default_account(name: str = "admin") -> Optional[int]:
    init_db()
    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "")
    if not api_key or not api_secret:
        return None
    conn = _connect()
    try:
        row = _fetch_one(conn, "SELECT id FROM accounts WHERE name = ?", (name,))
        if row:
            return int(row["id"])
        now = _now_str()
        cur = conn.execute(
            """
            INSERT INTO accounts (name, api_key, api_secret, is_active, created_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            """,
            (name, api_key, api_secret, now, now),
        )
        account_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO account_settings (
                account_id, entry_pct, dry_run, auto_exit, max_positions, leverage, margin_mode,
                exit_cooldown_h, long_tp_pct, long_sl_pct, short_tp_pct, short_sl_pct,
                dca_enabled, dca_pct, dca1_pct, dca2_pct, dca3_pct, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                DEFAULT_SETTINGS["entry_pct"],
                DEFAULT_SETTINGS["dry_run"],
                DEFAULT_SETTINGS["auto_exit"],
                DEFAULT_SETTINGS["max_positions"],
                DEFAULT_SETTINGS["leverage"],
                DEFAULT_SETTINGS["margin_mode"],
                DEFAULT_SETTINGS["exit_cooldown_h"],
                DEFAULT_SETTINGS["long_tp_pct"],
                DEFAULT_SETTINGS["long_sl_pct"],
                DEFAULT_SETTINGS["short_tp_pct"],
                DEFAULT_SETTINGS["short_sl_pct"],
                DEFAULT_SETTINGS["dca_enabled"],
                DEFAULT_SETTINGS["dca_pct"],
                DEFAULT_SETTINGS["dca1_pct"],
                DEFAULT_SETTINGS["dca2_pct"],
                DEFAULT_SETTINGS["dca3_pct"],
                now,
                now,
            ),
        )
        if tg_token and tg_chat:
            conn.execute(
                """
                INSERT INTO telegram_bots (account_id, bot_token, chat_id, last_update_id, created_at, updated_at)
                VALUES (?, ?, ?, 0, ?, ?)
                """,
                (account_id, tg_token, tg_chat, now, now),
            )
        conn.commit()
        return account_id
    finally:
        conn.close()


def list_active_accounts() -> List[Dict[str, Any]]:
    init_db()
    conn = _connect()
    try:
        rows = _fetch_all(conn, "SELECT * FROM accounts WHERE is_active = 1 ORDER BY id ASC", ())
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_account_settings(account_id: int) -> Dict[str, Any]:
    init_db()
    conn = _connect()
    try:
        row = _fetch_one(
            conn,
            "SELECT * FROM account_settings WHERE account_id = ?",
            (account_id,),
        )
        if row:
            return dict(row)
        return {}
    finally:
        conn.close()


def get_telegram_bot(account_id: int) -> Dict[str, Any]:
    init_db()
    conn = _connect()
    try:
        row = _fetch_one(conn, "SELECT * FROM telegram_bots WHERE account_id = ?", (account_id,))
        if row:
            return dict(row)
        return {}
    finally:
        conn.close()


def update_telegram_last_update_id(account_id: int, update_id: int) -> None:
    init_db()
    conn = _connect()
    try:
        now = _now_str()
        conn.execute(
            """
            UPDATE telegram_bots
            SET last_update_id = ?, updated_at = ?
            WHERE account_id = ?
            """,
            (int(update_id), now, int(account_id)),
        )
        conn.commit()
    finally:
        conn.close()


def update_telegram_chat_id(account_id: int, chat_id: str) -> None:
    init_db()
    conn = _connect()
    try:
        now = _now_str()
        conn.execute(
            """
            UPDATE telegram_bots
            SET chat_id = ?, updated_at = ?
            WHERE account_id = ?
            """,
            (str(chat_id), now, int(account_id)),
        )
        conn.commit()
    finally:
        conn.close()
