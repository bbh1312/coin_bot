import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import numpy as np # Added

# Assuming Signal is defined elsewhere, e.g., in types.py
class Signal:
    def __init__(self, ts: int, engine: str, symbol: str, tf_ltf: str, tf_mtf: str, tf_htf: str, event: str, state: str, context: Dict[str, Any]):
        self.ts = ts
        self.engine = engine
        self.symbol = symbol
        self.tf_ltf = tf_ltf
        self.tf_mtf = tf_mtf
        self.tf_htf = tf_htf
        self.event = event
        self.state = state
        self.context = context

LOG_DIR = os.path.join("logs", "dtfx")

def _convert_numpy_types(obj):
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_numpy_types(elem) for elem in obj]
    return obj

def _env_or_default(key: str, default: str) -> str:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()

def _with_dtfx_prefix(prefix: str) -> str:
    clean = str(prefix or "").strip()
    if not clean:
        clean = "engine"
    if clean.startswith("dtfx_"):
        return clean
    return f"dtfx_{clean}"


def _build_log_entry(signal: Signal, mode: str) -> dict:
    converted_context = _convert_numpy_types(signal.context)
    return {
        "ts": signal.ts,
        "engine": signal.engine,
        "symbol": signal.symbol,
        "tf_ltf": signal.tf_ltf,
        "tf_mtf": signal.tf_mtf,
        "tf_htf": signal.tf_htf,
        "event": signal.event,
        "state": signal.state,
        "context": converted_context,
        "mode": mode,
    }


class JsonlLogger:
    def __init__(
        self,
        log_dir: str = LOG_DIR,
        prefix: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        self.log_dir = log_dir
        self.prefix = prefix or _env_or_default("DTFX_LOG_PREFIX", "signal")
        self.mode = mode or _env_or_default("DTFX_LOG_MODE", "log_only")
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        self.log_file_path = ""
        self._update_log_file_path() # 초기 파일 경로 설정
        self.error_log_path = os.path.join(self.log_dir, "error.log") # Added

    def _update_log_file_path(self):
        today = datetime.utcnow().strftime("%Y%m%d")
        self.log_file_path = os.path.join(self.log_dir, f"{_with_dtfx_prefix(self.prefix)}_{today}.jsonl")

    def _log_internal_error(self, msg: str): # Made _log_error a method
        try:
            os.makedirs(self.log_dir, exist_ok=True)
            with open(self.error_log_path, "a", encoding="utf-8") as f:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{ts}] {msg}\n")
        except Exception:
            pass

    def log(self, signal: Signal):
        # Add a print statement to console for every logged signal
        filters = signal.context.get("filters", {}) if isinstance(signal.context, dict) else {}
        symbol_base = filters.get("symbol_base") or signal.symbol.split("/")[0].split(":")[0]
        is_anchor = filters.get("is_anchor")
        print(
            f"[SIGNAL LOGGED] Engine: {signal.engine} | Event: {signal.event} | Symbol: {signal.symbol} "
            f"| State: {signal.state} base={symbol_base} is_anchor={bool(is_anchor)}"
        )

        if self.mode == "backtest":
            if not self.log_file_path.endswith(f"{datetime.utcnow().strftime('%Y%m%d')}.jsonl"):
                self._update_log_file_path()
            log_entry = _build_log_entry(signal, self.mode)
            try:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            except Exception as e:
                self._log_internal_error(
                    f"Error writing signal to log file {self.log_file_path}: {e}. Signal: {signal.symbol} {signal.event}"
                )
                print(f"Error writing to log file: {e}") # Keep this console print for immediate feedback.
            return

        event = str(signal.event or "")
        prefix = "signal" if event.startswith("ENTRY_READY") else "engine"
        mode = "signal" if prefix == "signal" else "engine"
        log_entry = _build_log_entry(signal, mode)
        today = datetime.utcnow().strftime("%Y%m%d")
        log_path = os.path.join(self.log_dir, f"{_with_dtfx_prefix(prefix)}_{today}.jsonl")
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            self._log_internal_error(
                f"Error writing signal to log file {log_path}: {e}. Signal: {signal.symbol} {signal.event}"
            )
            print(f"Error writing to log file: {e}") # Keep this console print for immediate feedback.

    def log_entry(self, payload: dict, prefix: Optional[str] = None, mode: Optional[str] = None) -> None:
        if not isinstance(payload, dict):
            return
        current_prefix = prefix or self.prefix
        current_mode = mode or self.mode
        today = datetime.utcnow().strftime("%Y%m%d")
        log_path = os.path.join(self.log_dir, f"{_with_dtfx_prefix(current_prefix)}_{today}.jsonl")
        payload = dict(payload)
        payload["mode"] = current_mode
        payload["context"] = _convert_numpy_types(payload.get("context", {}))
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as e:
            self._log_internal_error(f"Error writing log file {log_path}: {e}")
            print(f"Error writing to log file: {e}")

_logger_instance = None

def get_logger() -> JsonlLogger:
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = JsonlLogger()
    return _logger_instance


def write_dtfx_log(payload: dict, prefix: str, mode: str) -> None:
    logger = get_logger()
    logger.log_entry(payload, prefix=prefix, mode=mode)
