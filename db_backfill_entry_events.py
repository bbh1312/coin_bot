import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

from env_loader import load_env
import db_recorder as dbrec


def _parse_entry_ts(val):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            return None
    return None


def main():
    load_env()
    if not dbrec.ENABLED:
        print("DB recording disabled.")
        return
    start_date_str = os.getenv("BACKFILL_START_DATE", "2026-01-12")
    try:
        start_ts = datetime.strptime(f"{start_date_str} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        print(f"Invalid BACKFILL_START_DATE={start_date_str}")
        return
    existing = set()
    try:
        db_path = getattr(dbrec, "DB_PATH", "") or "logs/trades.db"
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """
            SELECT ts, symbol, side
            FROM events
            WHERE event_type = 'ENTRY' AND ts >= ?
            """,
            (float(start_ts),),
        ).fetchall()
        conn.close()
        existing = {(int(r[0]), r[1], (r[2] or "").upper()) for r in rows if r and r[1]}
    except Exception:
        existing = set()
    entry_dir = os.path.join("logs", "entry")
    paths = []
    if os.path.isdir(entry_dir):
        for name in sorted(os.listdir(entry_dir)):
            if name.startswith("entry_events-") and name.endswith(".log"):
                paths.append(os.path.join(entry_dir, name))
    legacy_path = os.path.join("logs", "entry_events.log")
    if os.path.exists(legacy_path):
        paths.append(legacy_path)
    if not paths:
        print("No entry_events logs found.")
        return
    inserted = 0
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    ts_val = _parse_entry_ts(payload.get("entry_ts"))
                    if not isinstance(ts_val, (int, float)) or ts_val < start_ts:
                        continue
                    symbol = payload.get("symbol") or ""
                    side = (payload.get("side") or "").upper()
                    engine = payload.get("engine") or "unknown"
                    key = (int(ts_val), symbol, side)
                    if key in existing:
                        continue
                    dbrec.record_event(
                        symbol=symbol,
                        side=side,
                        event_type="ENTRY",
                        source=engine,
                        qty=payload.get("qty"),
                        avg_entry=payload.get("entry_price"),
                        price=payload.get("entry_price"),
                        meta={
                            "engine": engine,
                            "source": "entry_events",
                            "entry_order_id": payload.get("entry_order_id"),
                        },
                        ts=ts_val,
                    )
                    inserted += 1
        except Exception:
            continue
    print({"inserted": inserted})


if __name__ == "__main__":
    main()
