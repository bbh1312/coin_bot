#!/usr/bin/env python3
import json
import os
import time
from datetime import datetime


ENTRY_DIR = os.path.join("logs", "entry")
ENTRY_LEGACY = os.path.join("logs", "entry_events.log")
MANAGE_QUEUE = os.path.join("logs", "manage_queue.jsonl")
STATE_PATH = "state.json"


def _load_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _entry_ts_to_float(val):
    if isinstance(val, (int, float)):
        return float(val)
    if not isinstance(val, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val, fmt).timestamp()
        except Exception:
            continue
    return None


def _is_valid_engine(engine):
    if not engine:
        return False
    key = str(engine).upper()
    return key not in ("MANUAL", "UNKNOWN")


def _pick_latest(current, new):
    if not current:
        return new
    if not new:
        return current
    return new if new.get("ts", 0) >= current.get("ts", 0) else current


def _load_entry_events():
    rows = []
    if os.path.isdir(ENTRY_DIR):
        for name in sorted(os.listdir(ENTRY_DIR)):
            if name.startswith("entry_events-") and name.endswith(".log"):
                rows.extend(_load_jsonl(os.path.join(ENTRY_DIR, name)))
    if os.path.exists(ENTRY_LEGACY):
        rows.extend(_load_jsonl(ENTRY_LEGACY))
    events = {}
    for row in rows:
        sym = row.get("symbol")
        side = (row.get("side") or "").upper()
        eng = row.get("engine")
        if not sym or not side or not _is_valid_engine(eng):
            continue
        ts = _entry_ts_to_float(row.get("entry_ts")) or 0.0
        key = (sym, side)
        rec = {"engine": str(eng).upper(), "reason": None, "ts": ts}
        events[key] = _pick_latest(events.get(key), rec)
    return events


def _load_manage_queue():
    rows = _load_jsonl(MANAGE_QUEUE)
    events = {}
    for row in rows:
        if row.get("type") != "entry":
            continue
        sym = row.get("symbol")
        side = (row.get("side") or "").upper()
        eng = row.get("engine")
        reason = row.get("reason")
        if not sym or not side or not _is_valid_engine(eng):
            continue
        ts = float(row.get("ts") or 0.0)
        key = (sym, side)
        rec = {"engine": str(eng).upper(), "reason": reason, "ts": ts}
        events[key] = _pick_latest(events.get(key), rec)
    return events


def main():
    if not os.path.exists(STATE_PATH):
        print("state.json not found")
        return
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)
    trade_log = state.get("_trade_log") or []
    entry_events = _load_entry_events()
    queue_events = _load_manage_queue()

    updated = []
    for tr in trade_log:
        if tr.get("status") != "open":
            continue
        sym = tr.get("symbol")
        side = (tr.get("side") or "").upper()
        if not sym or not side:
            continue
        engine_label = tr.get("engine_label")
        if _is_valid_engine(engine_label):
            continue
        key = (sym, side)
        rec = entry_events.get(key) or queue_events.get(key)
        if not rec:
            continue
        engine = rec.get("engine")
        reason = rec.get("reason")
        tr["engine_label"] = engine
        meta = tr.get("meta") or {}
        meta["engine"] = engine
        if reason:
            meta["reason"] = reason
        tr["meta"] = meta
        updated.append(f"{sym}|{side}=>{engine}")

    if not updated:
        print("no updates")
        return

    backup = f"{STATE_PATH}.bak.{time.strftime('%Y%m%d-%H%M%S')}"
    try:
        os.replace(STATE_PATH, backup)
    except Exception:
        with open(backup, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"updated {len(updated)} open trades")
    for item in updated:
        print(item)
    print(f"backup={backup}")


if __name__ == "__main__":
    main()
