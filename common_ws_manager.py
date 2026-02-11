"""common_ws_manager.py
Binance Futures WebSocket helper for common-universe OHLCV (multi-tf).

특징
- 공통 유니버스 심볼 + 다중 타임프레임 kline 스트림 수집
- close 확정봉만 처리
- on_close_hook으로 외부 처리(예: common_warmup csv append)
"""
import json
import threading
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple

try:
    import websocket  # websocket-client
except Exception:
    websocket = None

BINANCE_FSTREAM_WS = "wss://fstream.binance.com/stream"
MAX_KEEP = 7200

_watch_symbols: List[str] = []
_watch_tfs: List[str] = []
_sym_map: Dict[str, str] = {}  # stream_sym -> ccxt symbol
_lock = threading.Lock()
_stop_event = threading.Event()
_reconnect_event = threading.Event()
_thread: Optional[threading.Thread] = None
_running: bool = False
_last_error: Optional[str] = None
_on_close_hook: Optional[Callable[[str, str, list], None]] = None
_kline_cache: Dict[Tuple[str, str], List[list]] = {}  # (ccxt_sym, tf) -> list[ohlcv]


def is_available() -> bool:
    return websocket is not None


def is_running() -> bool:
    return _running


def last_error() -> Optional[str]:
    return _last_error


def _sym_to_stream(sym: str) -> str:
    try:
        base = sym.split(":")[0]
        base = base.replace("/", "")
        return base.lower()
    except Exception:
        return ""


def set_on_close_hook(hook: Optional[Callable[[str, str, list], None]]) -> None:
    global _on_close_hook
    _on_close_hook = hook


def set_watch(symbols: Iterable[str], tfs: Iterable[str]) -> None:
    global _watch_symbols, _watch_tfs, _sym_map
    with _lock:
        _watch_symbols = [s for s in symbols if s]
        _watch_tfs = [tf for tf in tfs if tf]
        _sym_map = {_sym_to_stream(s): s for s in _watch_symbols if _sym_to_stream(s)}
        _reconnect_event.set()


def stop() -> None:
    global _running
    _stop_event.set()
    _reconnect_event.set()
    _running = False


def start() -> bool:
    global _thread, _running
    if not is_available():
        return False
    if _running:
        return True
    _stop_event.clear()
    _reconnect_event.clear()
    _thread = threading.Thread(target=_run_loop, name="common-ws", daemon=True)
    _thread.start()
    _running = True
    return True


def get_cache(symbol: str, tf: str, limit: int = 500) -> Optional[List[list]]:
    key = (symbol, tf)
    arr = _kline_cache.get(key)
    if not arr:
        return None
    return arr[-limit:]


def _run_loop() -> None:
    global _last_error, _running
    backoff = 1.0
    while not _stop_event.is_set():
        with _lock:
            syms = list(_sym_map.keys())
            tfs = list(_watch_tfs)
        if not syms or not tfs:
            time.sleep(2.0)
            continue
        streams = "/".join(f"{s}@kline_{tf}" for s in syms for tf in tfs)
        url = f"{BINANCE_FSTREAM_WS}?streams={streams}"

        def on_message(ws, message):
            try:
                payload = json.loads(message)
                data = payload.get("data", {})
                stream = payload.get("stream") or ""
                k = data.get("k") or {}
                tf = k.get("i")
                if not tf:
                    return
                if not k.get("x"):
                    return  # only closed
                stream_sym = stream.split("@")[0]
                ccxt_sym = _sym_map.get(stream_sym)
                if not ccxt_sym:
                    return
                ohlcv = [
                    int(k.get("t")),
                    float(k.get("o")),
                    float(k.get("h")),
                    float(k.get("l")),
                    float(k.get("c")),
                    float(k.get("q") or k.get("v") or 0.0),
                ]
                key = (ccxt_sym, tf)
                arr = _kline_cache.setdefault(key, [])
                if arr and arr[-1][0] == ohlcv[0]:
                    arr[-1] = ohlcv
                else:
                    arr.append(ohlcv)
                if len(arr) > MAX_KEEP:
                    del arr[:-MAX_KEEP]
                if _on_close_hook:
                    _on_close_hook(ccxt_sym, tf, ohlcv)
                if tf == "1h":
                    try:
                        print(f"[common-ws] 1h closed sym={ccxt_sym} ts={ohlcv[0]}")
                    except Exception:
                        pass
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
            url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open
        )
        ws_thread = threading.Thread(
            target=ws_app.run_forever, kwargs={"ping_interval": 15, "ping_timeout": 10}, daemon=True
        )
        ws_thread.start()

        while ws_thread.is_alive() and not _stop_event.is_set():
            if _reconnect_event.wait(timeout=1.0):
                _reconnect_event.clear()
                try:
                    ws_app.close()
                except Exception:
                    pass
                break
        time.sleep(backoff)
    _running = False
