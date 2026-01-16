import json
import os
import threading
import time
import uuid
from typing import Dict, List, Tuple

_QUEUE_LOCK = threading.Lock()


def _default_path() -> str:
    return os.getenv("MANAGE_QUEUE_PATH", os.path.join("logs", "manage_queue.jsonl"))


def enqueue_request(payload: Dict[str, object], path: str = "") -> str:
    if not path:
        path = _default_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    req_id = str(payload.get("id") or uuid.uuid4())
    payload = dict(payload)
    payload["id"] = req_id
    payload["ts"] = float(payload.get("ts") or time.time())
    line = json.dumps(payload, ensure_ascii=False)
    with _QUEUE_LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    return req_id


def read_requests_from_offset(offset: int, path: str = "") -> Tuple[int, List[Dict[str, object]]]:
    if not path:
        path = _default_path()
    if not os.path.exists(path):
        return offset, []
    out: List[Dict[str, object]] = []
    with open(path, "r", encoding="utf-8") as f:
        try:
            f.seek(offset)
        except Exception:
            f.seek(0)
            offset = 0
        while True:
            line = f.readline()
            if not line:
                break
            offset = f.tell()
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                out.append(payload)
    return offset, out
