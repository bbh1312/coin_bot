"""ws_manager.py
Binance Futures WebSocket helper for 5m kline streams (manage-mode only).

특징
- 관리 대상 심볼만 대상으로 5m kline 스트림을 수집
- 최신 klines 캐시(DataFrame) 제공
- 웹소켓 미존재/실패 시 사용하지 않음(자동 폴백)
"""
import json
import threading
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd

try:
    import websocket  # websocket-client
except Exception:
    websocket = None

# --- 상태 ---
_watch_symbols: set = set()
_lock = threading.Lock()
_stop_event = threading.Event()
_reconnect_event = threading.Event()
_thread: Optional[threading.Thread] = None
_running: bool = False
_kline_cache: Dict[str, List[dict]] = {}  # stream_symbol -> list of kline dicts
_last_error: Optional[str] = None

BINANCE_FSTREAM_WS = "wss://fstream.binance.com/stream"
KLINE_INTERVAL = "5m"
MAX_KLINE_KEEP = 240  # 최대 보존 캔들 수


def is_available() -> bool:
    return websocket is not None


def is_running() -> bool:
    return _running


def _sym_to_stream(sym: str) -> str:
    """ccxt 심볼(BTC/USDT:USDT)을 binance stream 심볼(btcusdt)로 변환."""
    try:
        base = sym.split(":")[0]
        base = base.replace("/", "")
        return base.lower()
    except Exception:
        return ""


def set_watch_symbols(symbols: Iterable[str]) -> None:
    """감시 대상 심볼 설정. 변경 시 재연결 트리거."""
    global _watch_symbols
    with _lock:
        new_set = set(_sym_to_stream(s) for s in symbols if _sym_to_stream(s))
        if new_set != _watch_symbols:
            _watch_symbols = new_set
            _reconnect_event.set()


def stop():
    global _running
    _stop_event.set()
    _reconnect_event.set()
    _running = False


def start() -> bool:
    """백그라운드 스레드 시작."""
    global _thread, _running, _stop_event, _reconnect_event
    if not is_available():
        return False
    if _running:
        return True
    _stop_event.clear()
    _reconnect_event.clear()
    _thread = threading.Thread(target=_run_loop, name="ws-manager", daemon=True)
    _thread.start()
    _running = True
    return True


def get_5m_df(symbol: str, limit: int = 120) -> Optional[pd.DataFrame]:
    """관리 심볼용 5m kline DF 반환. 데이터 없으면 None."""
    stream_sym = _sym_to_stream(symbol)
    data = _kline_cache.get(stream_sym)
    if not data:
        return None
    try:
        df = pd.DataFrame(data[-limit:])
        return df
    except Exception:
        return None


def last_error() -> Optional[str]:
    return _last_error


def _run_loop():
    global _last_error
    backoff = 1.0
    while not _stop_event.is_set():
        # 감시 대상이 없으면 대기
        with _lock:
            targets = list(_watch_symbols)
        if not targets:
            time.sleep(2.0)
            continue

        streams = "/".join(f"{s}@kline_{KLINE_INTERVAL}" for s in targets)
        url = f"{BINANCE_FSTREAM_WS}?streams={streams}"

        def on_message(ws, message):
            try:
                payload = json.loads(message)
                data = payload.get("data", {})
                stream = payload.get("stream") or ""
                k = data.get("k") or {}
                if k.get("i") != KLINE_INTERVAL:
                    return
                stream_sym = stream.split("@")[0]
                entry = {
                    "ts": k.get("t"),
                    "open": float(k.get("o")),
                    "high": float(k.get("h")),
                    "low": float(k.get("l")),
                    "close": float(k.get("c")),
                    "volume": float(k.get("q") or k.get("v") or 0.0),
                    "closed": bool(k.get("x")),
                }
                arr = _kline_cache.setdefault(stream_sym, [])
                # 동일 open time 대체
                found = False
                for idx, ex in enumerate(arr):
                    if ex.get("ts") == entry["ts"]:
                        arr[idx] = entry
                        found = True
                        break
                if not found:
                    arr.append(entry)
                if len(arr) > MAX_KLINE_KEEP:
                    del arr[:-MAX_KLINE_KEEP]
            except Exception as e:
                _last_error = str(e)

        def on_error(ws, error):
            nonlocal backoff
            _last_error = str(error)
            backoff = min(backoff * 1.5, 30.0)

        def on_close(ws, *args):
            pass

        def on_open(ws):
            nonlocal backoff
            backoff = 1.0

        ws_app = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )

        # run_forever는 내부에서 블로킹; reconnect 이벤트/stop 이벤트로 종료 유도
        ws_thread = threading.Thread(target=ws_app.run_forever, kwargs={"ping_interval": 15, "ping_timeout": 10}, daemon=True)
        ws_thread.start()

        # 재연결/중지 감시
        while ws_thread.is_alive() and not _stop_event.is_set():
            if _reconnect_event.wait(timeout=1.0):
                _reconnect_event.clear()
                try:
                    ws_app.close()
                except Exception:
                    pass
                break
        # 종료 시 약간 대기 후 재시도
        time.sleep(backoff)

    # stop 플래그 set 후 종료
    try:
        _kline_cache.clear()
    except Exception:
        pass
    _running = False
