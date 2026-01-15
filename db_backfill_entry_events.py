import json
import os
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
                    dbrec.record_engine_signal(
                        symbol=payload.get("symbol") or "",
                        side=payload.get("side") or "",
                        engine=payload.get("engine") or "",
                        reason=(payload.get("engine") or "").lower(),
                        meta=payload,
                        ts=ts_val,
                    )
                    inserted += 1
        except Exception:
            continue
    print({"inserted": inserted})


if __name__ == "__main__":
    main()
