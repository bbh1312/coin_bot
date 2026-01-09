import json
import os
import time
from typing import Any, Dict


class JsonlWriter:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path

    def write(self, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()


def now_ms() -> int:
    return int(time.time() * 1000)
