import json
import os
from typing import Dict, Any


def _default_state() -> Dict[str, Any]:
    return {
        "global": {"last_summary_ts": 0},
        "symbols": {},
    }


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return _default_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        data.setdefault("global", {"last_summary_ts": 0})
        data.setdefault("symbols", {})
        return data
    except Exception:
        return _default_state()


def save_state(path: str, state: Dict[str, Any]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        return
