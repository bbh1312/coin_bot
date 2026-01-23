"""executor.py
Binance USDT-M Perpetual (swap) execution helpers for SHORT-only strategy.

User settings applied:
- Leverage: 10x
- Entry USDT: 20
- DCA max adds: 3
- Each DCA size: 2% of available USDT
- DCA trigger by entry-price move (adverse vs entry):
  - 1st add when adverse move >= 30%
  - 2nd add when adverse move >= 30%
  - 3rd add when adverse move >= 30%

Security:
- Use environment variables (DO NOT hardcode keys):
  BINANCE_API_KEY, BINANCE_API_SECRET
- DRY_RUN=1 (default) / DRY_RUN=0 to go live
"""

import os
import random
import time
import threading
from contextlib import contextmanager
from typing import Optional, Dict

import ccxt
from env_loader import load_env
try:
    import db_recorder as dbrec
except Exception:
    dbrec = None

DEFAULT_LEVERAGE = 10
BASE_ENTRY_USDT = 40.0
DCA_MAX_ADDS = 3
DCA_ENABLED = True
DCA_PCT = 2.0

DCA_FIRST_PCT = 30.0   # price up vs entry (against short)
DCA_SECOND_PCT = 30.0
DCA_THIRD_PCT = 30.0

load_env()
API_KEY = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
# POSITION_MODE: 'auto' | 'hedge' | 'oneway'
# 기본값을 'hedge'로 고정
POSITION_MODE = os.environ.get("POSITION_MODE", "hedge").lower().strip()

# --- Position TTL cache (to reduce REST calls) ---
POS_TTL_SEC = 30.0  # fetch_positions 호출 간 최소 간격을 늘려 레이트리밋 완화

def _normalize_symbol_key(symbol: str) -> str:
    if not isinstance(symbol, str):
        return symbol
    if symbol.endswith("/USDT") and not symbol.endswith("/USDT:USDT"):
        return f"{symbol}:USDT"
    return symbol

def _alt_symbol_key(symbol: str) -> Optional[str]:
    if not isinstance(symbol, str):
        return None
    if symbol.endswith("/USDT:USDT"):
        return symbol.replace("/USDT:USDT", "/USDT")
    if symbol.endswith("/USDT"):
        return symbol.replace("/USDT", "/USDT:USDT")
    return None

# --- Balance TTL cache (USDT available) ---
_BAL_TTL_SEC = 5.0

# 전역 레이트리밋 백오프 (engine_runner와 공유 목적)
GLOBAL_BACKOFF_UNTIL = 0.0
_BACKOFF_SECS = 0.0


class ExecutorContext:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        dry_run: bool,
        position_mode: str,
        default_leverage: int,
        base_entry_usdt: float,
        dca_enabled: bool,
        dca_pct: float,
        dca_first_pct: float,
        dca_second_pct: float,
        dca_third_pct: float,
    ) -> None:
        self.exchange = ccxt.binance(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        self.dry_run = bool(dry_run)
        self.position_mode = (position_mode or "hedge").lower().strip()
        self.default_leverage = int(default_leverage)
        self.base_entry_usdt = float(base_entry_usdt)
        self.dca_enabled = bool(dca_enabled)
        self.dca_pct = float(dca_pct)
        self.dca_first_pct = float(dca_first_pct)
        self.dca_second_pct = float(dca_second_pct)
        self.dca_third_pct = float(dca_third_pct)

        self.pos_cache = {}
        self.pos_long_cache = {}
        self.pos_all_cache = {"ts": 0.0, "positions_by_symbol": {}}
        self.pos_ttl_sec = POS_TTL_SEC

        self.bal_ttl_sec = _BAL_TTL_SEC
        self.bal_cache = {"ts": 0.0, "available": None}
        self.bal_lock = threading.Lock()

        self.posmode_cache_ts = 0.0
        self.posmode_cache_val = False
        self.posmode_ttl = 60.0

        self.global_backoff_until = 0.0
        self.backoff_secs = 0.0

    def apply_globals(self) -> None:
        self.dry_run = bool(DRY_RUN)
        self.position_mode = (POSITION_MODE or "hedge").lower().strip()
        self.default_leverage = int(DEFAULT_LEVERAGE)
        self.base_entry_usdt = float(BASE_ENTRY_USDT)
        self.dca_enabled = bool(DCA_ENABLED)
        self.dca_pct = float(DCA_PCT)
        self.dca_first_pct = float(DCA_FIRST_PCT)
        self.dca_second_pct = float(DCA_SECOND_PCT)
        self.dca_third_pct = float(DCA_THIRD_PCT)


_THREAD_CTX = threading.local()
_DEFAULT_CTX = ExecutorContext(
    api_key=API_KEY,
    api_secret=API_SECRET,
    dry_run=DRY_RUN,
    position_mode=POSITION_MODE,
    default_leverage=DEFAULT_LEVERAGE,
    base_entry_usdt=BASE_ENTRY_USDT,
    dca_enabled=DCA_ENABLED,
    dca_pct=DCA_PCT,
    dca_first_pct=DCA_FIRST_PCT,
    dca_second_pct=DCA_SECOND_PCT,
    dca_third_pct=DCA_THIRD_PCT,
)


def _get_ctx() -> ExecutorContext:
    ctx = getattr(_THREAD_CTX, "current", None)
    if ctx is None:
        ctx = _DEFAULT_CTX
    if ctx is _DEFAULT_CTX:
        ctx.apply_globals()
    return ctx


@contextmanager
def use_context(ctx: ExecutorContext):
    prev = getattr(_THREAD_CTX, "current", None)
    _THREAD_CTX.current = ctx
    try:
        yield
    finally:
        _THREAD_CTX.current = prev


class AccountExecutor:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        dry_run: bool,
        position_mode: str,
        default_leverage: int,
        base_entry_usdt: float,
        dca_enabled: bool,
        dca_pct: float,
        dca_first_pct: float,
        dca_second_pct: float,
        dca_third_pct: float,
    ) -> None:
        self.ctx = ExecutorContext(
            api_key=api_key,
            api_secret=api_secret,
            dry_run=dry_run,
            position_mode=position_mode,
            default_leverage=default_leverage,
            base_entry_usdt=base_entry_usdt,
            dca_enabled=dca_enabled,
            dca_pct=dca_pct,
            dca_first_pct=dca_first_pct,
            dca_second_pct=dca_second_pct,
            dca_third_pct=dca_third_pct,
        )

    @contextmanager
    def activate(self):
        with use_context(self.ctx):
            yield

    def set_dry_run(self, flag: bool) -> bool:
        with self.activate():
            return set_dry_run(flag)

    def refresh_positions_cache(self, force: bool = False) -> bool:
        with self.activate():
            return refresh_positions_cache(force=force)

    def count_open_positions(self, force: bool = False) -> Optional[int]:
        with self.activate():
            return count_open_positions(force=force)

    def list_open_position_symbols(self, force: bool = False) -> Dict[str, set]:
        with self.activate():
            return list_open_position_symbols(force=force)

    def get_available_usdt(self, ttl_sec: float = _BAL_TTL_SEC) -> Optional[float]:
        with self.activate():
            return get_available_usdt(ttl_sec=ttl_sec)

    def get_short_position_amount(self, symbol: str) -> float:
        with self.activate():
            return get_short_position_amount(symbol)

    def get_long_position_amount(self, symbol: str) -> float:
        with self.activate():
            return get_long_position_amount(symbol)

    def get_short_roi_pct(self, symbol: str):
        with self.activate():
            return get_short_roi_pct(symbol)

    def get_short_position_detail(self, symbol: str) -> dict:
        with self.activate():
            return get_short_position_detail(symbol)

    def get_long_position_detail(self, symbol: str) -> dict:
        with self.activate():
            return get_long_position_detail(symbol)

    def short_market(self, symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated") -> dict:
        with self.activate():
            return short_market(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)

    def short_limit(self, *args, **kwargs) -> dict:
        with self.activate():
            return short_limit(*args, **kwargs)

    def long_market(self, symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated") -> dict:
        with self.activate():
            return long_market(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)

    def close_short_market(self, symbol: str) -> dict:
        with self.activate():
            return close_short_market(symbol)

    def close_long_market(self, symbol: str) -> dict:
        with self.activate():
            return close_long_market(symbol)

    def cancel_open_orders(self, symbol: str) -> dict:
        with self.activate():
            return cancel_open_orders(symbol)

    def cancel_stop_orders(self, symbol: str) -> dict:
        with self.activate():
            return cancel_stop_orders(symbol)

    def cancel_conditional_by_side(self, symbol: str, side: str) -> dict:
        with self.activate():
            return cancel_conditional_by_side(symbol, side)

    def dca_short_if_needed(self, symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
        with self.activate():
            return dca_short_if_needed(symbol, adds_done=adds_done, margin_mode=margin_mode)

    def dca_long_if_needed(self, symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
        with self.activate():
            return dca_long_if_needed(symbol, adds_done=adds_done, margin_mode=margin_mode)

def trigger_rate_limit_backoff(msg: str = ""):
    """429/-1003 감지 시 외부에서 호출해 전역 백오프를 공유."""
    global GLOBAL_BACKOFF_UNTIL, _BACKOFF_SECS
    ctx = _get_ctx()
    ctx.backoff_secs = 5.0 if ctx.backoff_secs <= 0 else min(ctx.backoff_secs * 1.5, 30.0)
    ctx.global_backoff_until = time.time() + ctx.backoff_secs
    if ctx is _DEFAULT_CTX:
        _BACKOFF_SECS = ctx.backoff_secs
        GLOBAL_BACKOFF_UNTIL = ctx.global_backoff_until
    print(f"[rate-limit/executor] 백오프 {ctx.backoff_secs:.1f}s (사유: {str(msg)[:80]})")

def get_global_backoff_until() -> float:
    ctx = _get_ctx()
    if ctx is _DEFAULT_CTX:
        return float(GLOBAL_BACKOFF_UNTIL or 0.0)
    return float(ctx.global_backoff_until or 0.0)

def _extract_usdt_available(balance: dict) -> Optional[float]:
    if not isinstance(balance, dict):
        return None
    try:
        usdt = balance.get("USDT") or {}
        if isinstance(usdt, dict):
            for key in ("free", "available", "total"):
                val = usdt.get(key)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
    except Exception:
        pass
    try:
        free_map = balance.get("free") or {}
        val = free_map.get("USDT")
        if isinstance(val, (int, float)) and val > 0:
            return float(val)
    except Exception:
        pass
    info = balance.get("info")
    if isinstance(info, dict):
        for key in ("availableBalance", "available_balance", "available"):
            val = info.get(key)
            try:
                val = float(val)
            except Exception:
                val = None
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    if isinstance(info, list):
        for row in info:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset") or row.get("currency") or "").upper()
            if asset != "USDT":
                continue
            for key in ("availableBalance", "available", "free", "balance"):
                val = row.get(key)
                try:
                    val = float(val)
                except Exception:
                    val = None
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
    return None

def get_available_usdt(ttl_sec: float = _BAL_TTL_SEC) -> Optional[float]:
    ctx = _get_ctx()
    now = time.time()
    with ctx.bal_lock:
        ts = float(ctx.bal_cache.get("ts", 0.0) or 0.0)
        cached = ctx.bal_cache.get("available")
        if cached is not None and (now - ts) <= ttl_sec:
            return float(cached)
    try:
        balance = ctx.exchange.fetch_balance({"type": "swap"})
    except Exception:
        try:
            balance = ctx.exchange.fetch_balance({"type": "future"})
        except Exception:
            try:
                balance = ctx.exchange.fetch_balance()
            except Exception:
                return None
    available = _extract_usdt_available(balance)
    if isinstance(available, (int, float)) and available > 0:
        with ctx.bal_lock:
            ctx.bal_cache["ts"] = now
            ctx.bal_cache["available"] = float(available)
    return available


def _calc_dca_usdt() -> Optional[float]:
    ctx = _get_ctx()
    available = get_available_usdt()
    if not isinstance(available, (int, float)) or available <= 0:
        return None
    usdt = float(available) * (ctx.dca_pct / 100.0)
    if usdt <= 0:
        return None
    return usdt

exchange = _DEFAULT_CTX.exchange

def _db_record_order(action: str, symbol: str, side: str, res: Optional[dict], status_override: Optional[str] = None) -> None:
    if not dbrec or not isinstance(res, dict):
        return
    order = res.get("order") if isinstance(res.get("order"), dict) else {}
    order_id = res.get("order_id") or order.get("id") or (order.get("info") or {}).get("orderId")
    status = order.get("status") or status_override or res.get("status") or "ok"
    price = order.get("price") or order.get("average") or res.get("price") or res.get("last")
    qty = order.get("amount") or res.get("amount")
    ts = order.get("timestamp") or order.get("lastTradeTimestamp")
    if isinstance(ts, (int, float)):
        ts = float(ts) / 1000.0 if ts > 1e12 else float(ts)
    else:
        ts = time.time()
    try:
        dbrec.record_order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=action,
            status=status,
            price=price,
            qty=qty,
            ts=ts,
            engine=None,
            client_order_id=order.get("clientOrderId") or (order.get("info") or {}).get("clientOrderId"),
            raw=order if order else res,
        )
        if order:
            dbrec.record_fills_from_order(order, symbol=symbol, side=side)
    except Exception:
        return

def is_hedge_mode() -> bool:
    """Return True if account is in Hedge mode.
    Priority: env override -> auto-detect (live only) -> default False.
    """
    ctx = _get_ctx()
    # Explicit override by env
    if ctx.position_mode == "hedge":
        return True
    if ctx.position_mode == "oneway":
        return False
    # DRY_RUN 여부와 관계없이 설정/감지 결과를 사용
    now = time.time()
    if (now - ctx.posmode_cache_ts) <= ctx.posmode_ttl:
        return ctx.posmode_cache_val
    try:
        # ccxt raw method name variants
        if hasattr(ctx.exchange, "fapiPrivateGetPositionSideDual"):
            resp = ctx.exchange.fapiPrivateGetPositionSideDual()
        else:
            resp = ctx.exchange.fapiPrivate_get_positionsidedual()
        val = resp.get("dualSidePosition")
        if isinstance(val, str):
            val = val.lower() in ("true", "1", "yes")
        hedge = bool(val)
    except Exception:
        hedge = False
    ctx.posmode_cache_ts = now
    ctx.posmode_cache_val = hedge
    return hedge

def ensure_ready():
    # DRY_RUN이면 키 없이도 마켓 로드는 시도
    ctx = _get_ctx()
    if ctx.dry_run:
        try:
            ctx.exchange.load_markets()
            return
        except Exception:
            pass
    if not ctx.exchange.apiKey or not ctx.exchange.secret:
        raise RuntimeError("환경변수 BINANCE_API_KEY / BINANCE_API_SECRET 설정 필요")
    ctx.exchange.load_markets()

def set_dry_run(flag: bool) -> bool:
    """런타임에 DRY_RUN 토글. True=dry-run, False=live"""
    global DRY_RUN
    DRY_RUN = bool(flag)
    _DEFAULT_CTX.dry_run = DRY_RUN
    return DRY_RUN

def set_leverage_and_margin(symbol: str, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated"):
    ctx = _get_ctx()
    try:
        ctx.exchange.set_margin_mode(margin_mode, symbol)
    except Exception as e:
        print("[margin_mode] warn:", e)
    try:
        ctx.exchange.set_leverage(leverage, symbol)
    except Exception as e:
        print("[leverage] warn:", e)

def _get_position_cached(symbol: str):
    """Return position object for symbol using TTL cache.
    Cache entry: symbol -> (ts, position_or_None)
    """
    now = time.time()
    # 1) 전체 포지션 캐시 우선 사용
    ctx = _get_ctx()
    try:
        ts_all = float(ctx.pos_all_cache.get("ts", 0.0) or 0.0)
        if (now - ts_all) <= ctx.pos_ttl_sec:
            pos_map = ctx.pos_all_cache.get("positions_by_symbol") or {}
            positions = pos_map.get(symbol)
            if positions is None:
                alt = _alt_symbol_key(symbol)
                positions = pos_map.get(alt) if alt else None
            pos = _select_short_position(positions or []) if positions else None
            ctx.pos_cache[symbol] = (now, pos)
            return pos
    except Exception:
        pass
    try:
        ts, pos = ctx.pos_cache.get(symbol, (0.0, None))
    except Exception:
        ts, pos = 0.0, None
    if (now - ts) <= ctx.pos_ttl_sec:
        return pos
    ensure_ready()
    try:
        positions = ctx.exchange.fetch_positions([symbol])
    except Exception as e:
        msg = str(e)
        print("[fetch_positions] warn:", e)
        if ("429" in msg) or ("-1003" in msg):
            trigger_rate_limit_backoff(msg)
        # keep previous cache if exists
        ctx.pos_cache[symbol] = (now, pos)
        return pos
    if not positions:
        alt = _alt_symbol_key(symbol)
        if alt:
            try:
                positions = exchange.fetch_positions([alt])
            except Exception as e:
                msg = str(e)
                print("[fetch_positions] warn:", e)
                if ("429" in msg) or ("-1003" in msg):
                    trigger_rate_limit_backoff(msg)
                _POS_CACHE[symbol] = (now, pos)
                return pos
    found = _select_short_position(positions)
    ctx.pos_cache[symbol] = (now, found)
    return found

def _get_long_position_cached(symbol: str):
    """Return long position object for symbol using TTL cache."""
    now = time.time()
    try:
        ctx = _get_ctx()
        ts_all = float(ctx.pos_all_cache.get("ts", 0.0) or 0.0)
        if (now - ts_all) <= ctx.pos_ttl_sec:
            pos_map = ctx.pos_all_cache.get("positions_by_symbol") or {}
            positions = pos_map.get(symbol)
            if positions is None:
                alt = _alt_symbol_key(symbol)
                positions = pos_map.get(alt) if alt else None
            pos = _select_long_position(positions or []) if positions else None
            ctx.pos_long_cache[symbol] = (now, pos)
            return pos
    except Exception:
        pass
    try:
        ctx = _get_ctx()
        ts, pos = ctx.pos_long_cache.get(symbol, (0.0, None))
    except Exception:
        ts, pos = 0.0, None
    if (now - ts) <= ctx.pos_ttl_sec:
        return pos
    ensure_ready()
    try:
        positions = ctx.exchange.fetch_positions([symbol])
    except Exception as e:
        msg = str(e)
        print("[fetch_positions] warn:", e)
        if ("429" in msg) or ("-1003" in msg):
            trigger_rate_limit_backoff(msg)
        ctx.pos_long_cache[symbol] = (now, pos)
        return pos
    if not positions:
        alt = _alt_symbol_key(symbol)
        if alt:
            try:
                positions = exchange.fetch_positions([alt])
            except Exception as e:
                msg = str(e)
                print("[fetch_positions] warn:", e)
                if ("429" in msg) or ("-1003" in msg):
                    trigger_rate_limit_backoff(msg)
                _POS_LONG_CACHE[symbol] = (now, pos)
                return pos
    found = _select_long_position(positions)
    ctx.pos_long_cache[symbol] = (now, found)
    return found

def _select_short_position(positions) -> Optional[dict]:
    """positions 리스트에서 숏 포지션만 반환."""
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    for p in positions:
        if hedge:
            info = p.get("info") or {}
            side = info.get("positionSide") or p.get("positionSide")
            if str(side).upper() != "SHORT":
                continue
            # hedge: 0수량 슬롯은 스킵
            try:
                pa = float(info.get("positionAmt") or p.get("positionAmt") or 0.0)
            except Exception:
                pa = 0.0
            if abs(pa) <= 0:
                continue
        else:
            # oneway: 음수 수량(숏)만 인정, 0은 스킵
            try:
                pa = float((p.get("info") or {}).get("positionAmt") or p.get("positionAmt") or 0.0)
            except Exception:
                pa = 0.0
            if pa >= 0:
                continue
        return p
    return None

def _select_long_position(positions) -> Optional[dict]:
    """positions 리스트에서 롱 포지션만 반환."""
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    for p in positions:
        if hedge:
            info = p.get("info") or {}
            side = info.get("positionSide") or p.get("positionSide")
            if str(side).upper() != "LONG":
                continue
            try:
                pa = float(info.get("positionAmt") or p.get("positionAmt") or 0.0)
            except Exception:
                pa = 0.0
            if abs(pa) <= 0:
                continue
        else:
            try:
                pa = float((p.get("info") or {}).get("positionAmt") or p.get("positionAmt") or 0.0)
            except Exception:
                pa = 0.0
            if pa <= 0:
                continue
        return p
    return None

def refresh_positions_cache(force: bool = False) -> bool:
    """전체 포지션을 한 번에 가져와 심볼별 캐시에 저장."""
    ctx = _get_ctx()
    now = time.time()
    try:
        ts_all = float(ctx.pos_all_cache.get("ts", 0.0) or 0.0)
        if (not force) and (now - ts_all) <= ctx.pos_ttl_sec:
            return True
    except Exception:
        pass
    ensure_ready()
    try:
        positions = ctx.exchange.fetch_positions()
    except Exception as e:
        msg = str(e)
        print("[fetch_positions] warn:", e)
        if ("429" in msg) or ("-1003" in msg):
            trigger_rate_limit_backoff(msg)
        return False
    pos_map = {}
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        sym_norm = _normalize_symbol_key(sym)
        pos_map.setdefault(sym_norm, []).append(p)
    ctx.pos_all_cache["ts"] = now
    ctx.pos_all_cache["positions_by_symbol"] = pos_map
    return True


def count_open_positions(force: bool = False) -> Optional[int]:
    """현재 열린 포지션 수(롱/숏 합산)를 반환한다."""
    if not refresh_positions_cache(force=force):
        return None
    ctx = _get_ctx()
    pos_map = ctx.pos_all_cache.get("positions_by_symbol") or {}
    cnt = 0
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    for positions in pos_map.values():
        if not positions:
            continue
        if hedge:
            for p in positions:
                if _is_long_position(p) or _is_short_position(p):
                    cnt += 1
        else:
            for p in positions:
                if _is_long_position(p) or _is_short_position(p):
                    cnt += 1
                    break
    return cnt

def list_open_position_symbols(force: bool = False) -> Dict[str, set]:
    """현재 열린 포지션 심볼을 롱/숏으로 반환한다."""
    ctx = _get_ctx()
    if not refresh_positions_cache(force=force):
        pos_map = ctx.pos_all_cache.get("positions_by_symbol") or {}
        if not pos_map:
            return {"long": set(), "short": set()}
    else:
        pos_map = ctx.pos_all_cache.get("positions_by_symbol") or {}
    longs = set()
    shorts = set()
    for sym, positions in pos_map.items():
        if not positions:
            continue
        sym = _normalize_symbol_key(sym)
        for p in positions:
            if _is_long_position(p):
                longs.add(sym)
            if _is_short_position(p):
                shorts.add(sym)
    return {"long": longs, "short": shorts}

def _position_size_abs(p) -> float:
    info = p.get("info") or {}
    try:
        pa = float(info.get("positionAmt") or p.get("positionAmt") or 0.0)
    except Exception:
        pa = 0.0
    if pa == 0:
        try:
            pa = float(p.get("contracts") or 0.0)
        except Exception:
            pa = 0.0
    try:
        return abs(float(pa))
    except Exception:
        return 0.0

def _is_short_position(p) -> bool:
    if not p:
        return False
    info = p.get("info") or {}
    size_abs = _position_size_abs(p)
    if size_abs <= 0:
        return False
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    if hedge:
        side = info.get("positionSide") or p.get("positionSide")
        return str(side).upper() == "SHORT"
    # oneway: positionAmt 부호로 판정
    try:
        pos_amt = float(info.get("positionAmt") or p.get("positionAmt") or 0.0)
    except Exception:
        pos_amt = 0.0
    return pos_amt < 0

def _is_long_position(p) -> bool:
    if not p:
        return False
    info = p.get("info") or {}
    size_abs = _position_size_abs(p)
    if size_abs <= 0:
        return False
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    if hedge:
        side = info.get("positionSide") or p.get("positionSide")
        return str(side).upper() == "LONG"
    try:
        pos_amt = float(info.get("positionAmt") or p.get("positionAmt") or 0.0)
    except Exception:
        pos_amt = 0.0
    return pos_amt > 0

def _find_position(symbol: str):
    return _get_position_cached(symbol)

def _find_long_position(symbol: str):
    return _get_long_position_cached(symbol)

def get_short_position_amount(symbol: str) -> float:
    p = _find_position(symbol)
    if not p:
        return 0.0
    if not _is_short_position(p):
        return 0.0
    amt = _position_size_abs(p)
    # noisy sampling log disabled
    return amt

def get_long_position_amount(symbol: str) -> float:
    p = _find_long_position(symbol)
    if not p:
        return 0.0
    if not _is_long_position(p):
        return 0.0
    return _position_size_abs(p)

def get_short_roi_pct(symbol: str):
    """Leveraged ROI% approximation for SHORT:
    ROI% ~= ((entry - mark) / entry) * leverage * 100
    (negative = loss)
    """
    p = _find_position(symbol)
    if not p:
        return None
    if not _is_short_position(p):
        return None
    info = p.get("info") or {}

    entry = info.get("entryPrice") or info.get("avgPrice") or p.get("entryPrice")
    mark = info.get("markPrice") or p.get("markPrice")
    lev = info.get("leverage") or p.get("leverage") or DEFAULT_LEVERAGE
    try:
        entry = float(entry); mark = float(mark); lev = float(lev)
    except Exception:
        return None
    if entry <= 0:
        return None
    return ((entry - mark) / entry) * lev * 100.0

def get_short_position_detail(symbol: str) -> dict:
    """숏 포지션 상세 정보 및 손익(USDT) 추정치를 반환한다."""
    p = _find_position(symbol)
    if not p or not _is_short_position(p):
        return {}
    info = p.get("info") or {}

    entry = info.get("entryPrice") or info.get("avgPrice") or p.get("entryPrice")
    mark = info.get("markPrice") or p.get("markPrice")
    lev = info.get("leverage") or p.get("leverage") or DEFAULT_LEVERAGE
    qty = _position_size_abs(p)

    try:
        entry = float(entry); mark = float(mark); lev = float(lev); qty = float(qty)
    except Exception:
        return {}

    if entry <= 0 or qty <= 0:
        return {}

    notional = entry * qty
    margin = notional / lev if lev > 0 else None
    pnl = (entry - mark) * qty if mark > 0 else None
    roi = (pnl / margin * 100.0) if (pnl is not None and margin not in (None, 0)) else None

    return {
        "entry": entry,
        "mark": mark,
        "leverage": lev,
        "qty": qty,
        "notional": notional,
        "margin": margin,
        "pnl": pnl,
        "roi": roi,
    }

def get_long_position_detail(symbol: str) -> dict:
    """롱 포지션 상세 정보 및 손익(USDT) 추정치를 반환한다."""
    p = _find_long_position(symbol)
    if not p or not _is_long_position(p):
        return {}
    info = p.get("info") or {}

    entry = info.get("entryPrice") or info.get("avgPrice") or p.get("entryPrice")
    mark = info.get("markPrice") or p.get("markPrice")
    lev = info.get("leverage") or p.get("leverage") or DEFAULT_LEVERAGE
    qty = _position_size_abs(p)

    try:
        entry = float(entry); mark = float(mark); lev = float(lev); qty = float(qty)
    except Exception:
        return {}

    if entry <= 0 or qty <= 0:
        return {}

    notional = entry * qty
    margin = notional / lev if lev > 0 else None
    pnl = (mark - entry) * qty if mark > 0 else None
    roi = (pnl / margin * 100.0) if (pnl is not None and margin not in (None, 0)) else None

    return {
        "entry": entry,
        "mark": mark,
        "leverage": lev,
        "qty": qty,
        "notional": notional,
        "margin": margin,
        "pnl": pnl,
        "roi": roi,
    }

def should_dca_by_price(entry: float, mark: float, adds_done: int) -> bool:
    """Adverse move vs entry for short. Triggers when mark price rises by configured %."""
    ctx = _get_ctx()
    if entry <= 0:
        return False
    adverse_pct = (mark - entry) / entry * 100.0
    if adds_done <= 0:
        return adverse_pct >= ctx.dca_first_pct
    if adds_done == 1:
        return adverse_pct >= ctx.dca_second_pct
    return adverse_pct >= ctx.dca_third_pct

def should_dca_by_price_long(entry: float, mark: float, adds_done: int) -> bool:
    """Adverse move vs entry for long. Triggers when mark price falls by configured %."""
    ctx = _get_ctx()
    if entry <= 0:
        return False
    adverse_pct = (entry - mark) / entry * 100.0
    if adds_done <= 0:
        return adverse_pct >= ctx.dca_first_pct
    if adds_done == 1:
        return adverse_pct >= ctx.dca_second_pct
    return adverse_pct >= ctx.dca_third_pct

def short_market(symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated") -> dict:
    ctx = _get_ctx()
    ensure_ready()
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = ctx.exchange.market(symbol)
    last = float(ctx.exchange.fetch_ticker(symbol)["last"])
    # usdt_amount를 증거금으로 해석하고 레버리지를 곱해 명목가로 변환
    notional = float(usdt_amount) * float(leverage)
    amount = float(ctx.exchange.amount_to_precision(symbol, notional / last))

    # 최소 수량/명목 조건 확인
    limits = market.get("limits") or {}
    min_qty = None
    min_notional = None
    try:
        min_qty = float((limits.get("amount") or {}).get("min"))
    except Exception:
        min_qty = None
    try:
        min_notional = float((limits.get("cost") or {}).get("min"))
    except Exception:
        min_notional = None

    # 필터에서 보조 추출 (Binance notional/lot size)
    try:
        for f in market.get("info", {}).get("filters", []) or []:
            ftype = str(f.get("filterType") or "").upper()
            if min_notional is None and ftype in ("MIN_NOTIONAL", "MIN_NOTIONAL_FILTER", "NOTIONAL"):
                val = f.get("notional") or f.get("minNotional")
                try:
                    min_notional = float(val)
                except Exception:
                    pass
            if min_qty is None and ftype in ("MARKET_LOT_SIZE", "LOT_SIZE"):
                val = f.get("minQty")
                try:
                    min_qty = float(val)
                except Exception:
                    pass
    except Exception:
        pass

    notional_after = amount * last
    if amount <= 0:
        return {"status": "skip", "reason": "amount_zero", "symbol": symbol}
    if min_qty is not None and amount < min_qty:
        return {"status": "skip", "reason": "below_min_qty", "symbol": symbol, "amount": amount, "min_qty": min_qty}
    if min_notional is not None and notional_after < min_notional:
        return {
            "status": "skip",
            "reason": "below_min_notional",
            "symbol": symbol,
            "notional": notional_after,
            "min_notional": min_notional,
        }
    if ctx.dry_run:
        res = {
            "status": "dry_run",
            "action": "short_market",
            "symbol": symbol,
            "amount": amount,
            "last": last,
            "usdt": usdt_amount,
            "lev": leverage,
            "order_id": None,
        }
        _db_record_order("short_market", symbol, "SHORT", res, status_override="dry_run")
        return res
    params = {}
    if is_hedge_mode():
        params["positionSide"] = "SHORT"
    order = ctx.exchange.create_market_sell_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {
        "status": "ok",
        "action": "short_market",
        "order": order,
        "order_id": order_id,
        "amount": amount,
        "last": last,
        "usdt": usdt_amount,
        "lev": leverage,
    }
    _db_record_order("short_market", symbol, "SHORT", res)
    return res

def short_limit(
    symbol: str,
    price: float,
    usdt_amount: float = BASE_ENTRY_USDT,
    leverage: int = DEFAULT_LEVERAGE,
    margin_mode: str = "isolated",
) -> dict:
    ctx = _get_ctx()
    ensure_ready()
    if price <= 0:
        return {"status": "skip", "reason": "price_unavailable", "symbol": symbol}
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = ctx.exchange.market(symbol)
    notional = float(usdt_amount) * float(leverage)
    amount = float(ctx.exchange.amount_to_precision(symbol, notional / float(price)))

    limits = market.get("limits") or {}
    min_qty = None
    min_notional = None
    try:
        min_qty = float((limits.get("amount") or {}).get("min"))
    except Exception:
        min_qty = None
    try:
        min_notional = float((limits.get("cost") or {}).get("min"))
    except Exception:
        min_notional = None
    try:
        for f in market.get("info", {}).get("filters", []) or []:
            ftype = str(f.get("filterType") or "").upper()
            if min_notional is None and ftype in ("MIN_NOTIONAL", "MIN_NOTIONAL_FILTER", "NOTIONAL"):
                val = f.get("notional") or f.get("minNotional")
                try:
                    min_notional = float(val)
                except Exception:
                    pass
            if min_qty is None and ftype in ("MARKET_LOT_SIZE", "LOT_SIZE"):
                val = f.get("minQty")
                try:
                    min_qty = float(val)
                except Exception:
                    pass
    except Exception:
        pass

    notional_after = amount * float(price)
    if amount <= 0:
        return {"status": "skip", "reason": "amount_zero", "symbol": symbol}
    if min_qty is not None and amount < min_qty:
        return {"status": "skip", "reason": "below_min_qty", "symbol": symbol, "amount": amount, "min_qty": min_qty}
    if min_notional is not None and notional_after < min_notional:
        return {
            "status": "skip",
            "reason": "below_min_notional",
            "symbol": symbol,
            "notional": notional_after,
            "min_notional": min_notional,
        }
    if ctx.dry_run:
        res = {
            "status": "dry_run",
            "action": "short_limit",
            "symbol": symbol,
            "amount": amount,
            "price": price,
            "usdt": usdt_amount,
            "lev": leverage,
            "order_id": None,
        }
        _db_record_order("short_limit", symbol, "SHORT", res, status_override="dry_run")
        return res
    params = {}
    if is_hedge_mode():
        params["positionSide"] = "SHORT"
    order = ctx.exchange.create_limit_sell_order(symbol, amount, price, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {
        "status": "ok",
        "action": "short_limit",
        "order": order,
        "order_id": order_id,
        "amount": amount,
        "price": price,
        "usdt": usdt_amount,
        "lev": leverage,
    }
    _db_record_order("short_limit", symbol, "SHORT", res)
    return res

def long_market(symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated") -> dict:
    ctx = _get_ctx()
    ensure_ready()
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = ctx.exchange.market(symbol)
    last = float(ctx.exchange.fetch_ticker(symbol)["last"])
    notional = float(usdt_amount) * float(leverage)
    amount = float(ctx.exchange.amount_to_precision(symbol, notional / last))

    limits = market.get("limits") or {}
    min_qty = None
    min_notional = None
    try:
        min_qty = float((limits.get("amount") or {}).get("min"))
    except Exception:
        min_qty = None
    try:
        min_notional = float((limits.get("cost") or {}).get("min"))
    except Exception:
        min_notional = None
    try:
        for f in market.get("info", {}).get("filters", []) or []:
            ftype = str(f.get("filterType") or "").upper()
            if min_notional is None and ftype in ("MIN_NOTIONAL", "MIN_NOTIONAL_FILTER", "NOTIONAL"):
                val = f.get("notional") or f.get("minNotional")
                try:
                    min_notional = float(val)
                except Exception:
                    pass
            if min_qty is None and ftype in ("MARKET_LOT_SIZE", "LOT_SIZE"):
                val = f.get("minQty")
                try:
                    min_qty = float(val)
                except Exception:
                    pass
    except Exception:
        pass

    notional_after = amount * last
    if amount <= 0:
        return {"status": "skip", "reason": "amount_zero", "symbol": symbol}
    if min_qty is not None and amount < min_qty:
        return {"status": "skip", "reason": "below_min_qty", "symbol": symbol, "amount": amount, "min_qty": min_qty}
    if min_notional is not None and notional_after < min_notional:
        return {
            "status": "skip",
            "reason": "below_min_notional",
            "symbol": symbol,
            "notional": notional_after,
            "min_notional": min_notional,
        }
    if ctx.dry_run:
        res = {
            "status": "dry_run",
            "action": "long_market",
            "symbol": symbol,
            "amount": amount,
            "last": last,
            "usdt": usdt_amount,
            "lev": leverage,
            "order_id": None,
        }
        _db_record_order("long_market", symbol, "LONG", res, status_override="dry_run")
        return res
    params = {}
    if is_hedge_mode():
        params["positionSide"] = "LONG"
    order = ctx.exchange.create_market_buy_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {
        "status": "ok",
        "action": "long_market",
        "order": order,
        "order_id": order_id,
        "amount": amount,
        "last": last,
        "usdt": usdt_amount,
        "lev": leverage,
    }
    _db_record_order("long_market", symbol, "LONG", res)
    return res

def close_short_market(symbol: str) -> dict:
    ctx = _get_ctx()
    ensure_ready()
    amount = get_short_position_amount(symbol)
    if amount <= 0:
        return {"status": "skip", "reason": "no_short_position", "symbol": symbol}
    amount = float(ctx.exchange.amount_to_precision(symbol, amount))
    if ctx.dry_run:
        res = {"status": "dry_run", "action": "close_short_market", "symbol": symbol, "amount": amount, "order_id": None}
        _db_record_order("close_short_market", symbol, "SHORT", res, status_override="dry_run")
        return res
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    # hedge 모드에서는 reduceOnly를 보내지 않고 positionSide만 지정
    params = {"positionSide": "SHORT"} if hedge else {"reduceOnly": True}
    order = ctx.exchange.create_market_buy_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {"status": "ok", "action": "close_short_market", "order": order, "order_id": order_id, "amount": amount}
    _db_record_order("close_short_market", symbol, "SHORT", res)
    return res

def close_long_market(symbol: str) -> dict:
    ctx = _get_ctx()
    ensure_ready()
    amount = get_long_position_amount(symbol)
    if amount <= 0:
        return {"status": "skip", "reason": "no_long_position", "symbol": symbol}
    amount = float(ctx.exchange.amount_to_precision(symbol, amount))
    if ctx.dry_run:
        res = {"status": "dry_run", "action": "close_long_market", "symbol": symbol, "amount": amount, "order_id": None}
        _db_record_order("close_long_market", symbol, "LONG", res, status_override="dry_run")
        return res
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    params = {"positionSide": "LONG"} if hedge else {"reduceOnly": True}
    order = ctx.exchange.create_market_sell_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {"status": "ok", "action": "close_long_market", "order": order, "order_id": order_id, "amount": amount}
    _db_record_order("close_long_market", symbol, "LONG", res)
    return res

def cancel_open_orders(symbol: str) -> dict:
    ctx = _get_ctx()
    ensure_ready()
    if ctx.dry_run:
        return {"status": "dry_run", "action": "cancel_open_orders", "symbol": symbol}
    try:
        orders = ctx.exchange.fetch_open_orders(symbol)
        canceled = 0
        for o in orders:
            try:
                ctx.exchange.cancel_order(o["id"], symbol)
                canceled += 1
                if dbrec:
                    try:
                        dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=None, ts=time.time(), reason="cancel_open_orders", raw=o)
                    except Exception:
                        pass
            except Exception:
                pass
        return {"status": "ok", "action": "cancel_open_orders", "symbol": symbol, "canceled": canceled}
    except Exception as e:
        return {"status": "warn", "action": "cancel_open_orders", "symbol": symbol, "error": str(e)}

def cancel_stop_orders(symbol: str) -> dict:
    """
    Cancel stop-loss related orders only (skip take-profit).
    """
    ctx = _get_ctx()
    ensure_ready()
    if ctx.dry_run:
        return {"status": "dry_run", "action": "cancel_stop_orders", "symbol": symbol}
    try:
        orders = []
        try:
            orders = ctx.exchange.fetch_open_orders(symbol)
        except Exception:
            orders = []
        # Some exchanges separate conditional/stop orders; try extra fetches.
        extra_orders = []
        for params in ({"type": "stop"}, {"stop": True}, {"trigger": True}):
            try:
                extra_orders.extend(ctx.exchange.fetch_open_orders(symbol, params))
            except Exception:
                continue
        if extra_orders:
            orders = orders + extra_orders
        # Deduplicate by id
        seen_ids = set()
        deduped = []
        for o in orders:
            oid = o.get("id")
            if oid in seen_ids:
                continue
            seen_ids.add(oid)
            deduped.append(o)
        orders = deduped
        canceled = 0
        scanned = 0
        stop_count = 0
        for o in orders:
            scanned += 1
            try:
                info = o.get("info") or {}
                otype = (o.get("type") or info.get("type") or "").upper()
                if "TAKE_PROFIT" in otype:
                    continue
                is_stop = "STOP" in otype
                if not is_stop:
                    if info.get("stopPrice") or info.get("triggerPrice") or o.get("stopPrice") or o.get("triggerPrice"):
                        is_stop = True
                if not is_stop:
                    continue
                stop_count += 1
                params = {}
                pos_side = info.get("positionSide") or o.get("positionSide")
                if pos_side:
                    params["positionSide"] = pos_side
                try:
                    ctx.exchange.cancel_order(o["id"], symbol, params)
                    canceled += 1
                    if dbrec:
                        try:
                            dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_stop_orders", raw=o)
                        except Exception:
                            pass
                except Exception:
                    try:
                        ctx.exchange.cancel_order(o["id"], symbol)
                        canceled += 1
                        if dbrec:
                            try:
                                dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_stop_orders", raw=o)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
        if stop_count > 0 and canceled == 0:
            for params in ({"type": "STOP_MARKET"}, {"type": "stop"}, {"stop": True}, {"reduceOnly": True}):
                try:
                    ctx.exchange.cancel_all_orders(symbol, params)
                except Exception:
                    continue
        return {
            "status": "ok",
            "action": "cancel_stop_orders",
            "symbol": symbol,
            "scanned": scanned,
            "canceled": canceled,
            "stop_count": stop_count,
        }
    except Exception as e:
        return {"status": "warn", "action": "cancel_stop_orders", "symbol": symbol, "error": str(e)}

def cancel_conditional_by_side(symbol: str, side: str) -> dict:
    """
    Cancel conditional reduce-only/closePosition orders for a specific positionSide.
    """
    ctx = _get_ctx()
    ensure_ready()
    if ctx.dry_run:
        return {"status": "dry_run", "action": "cancel_conditional_by_side", "symbol": symbol, "side": side}
    orders = []
    seen_ids = set()
    fetch_errors = []
    for params in ({}, {"type": "stop"}, {"stop": True}, {"trigger": True}, {"reduceOnly": True}):
        try:
            batch = ctx.exchange.fetch_open_orders(symbol, params) if params else ctx.exchange.fetch_open_orders(symbol)
        except Exception as e:
            fetch_errors.append(str(e))
            continue
        for o in batch or []:
            oid = o.get("id")
            if oid and oid in seen_ids:
                continue
            if oid:
                seen_ids.add(oid)
            orders.append(o)
    if not orders and fetch_errors:
        return {
            "status": "warn",
            "action": "cancel_conditional_by_side",
            "symbol": symbol,
            "side": side,
            "error": fetch_errors[-1],
        }
    conditional_types = {
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    }
    side = (side or "").upper()
    canceled = 0
    targets = 0
    for o in orders:
        info = o.get("info") or {}
        otype = (o.get("type") or info.get("type") or "").upper()
        if otype not in conditional_types:
            continue
        pos_side = (info.get("positionSide") or o.get("positionSide") or "").upper()
        if pos_side and side and pos_side != side:
            continue
        reduce_only = bool(o.get("reduceOnly") or str(info.get("reduceOnly")).lower() == "true")
        close_position = str(info.get("closePosition")).lower() == "true"
        if not (reduce_only or close_position):
            continue
        targets += 1
        params = {}
        if pos_side:
            params["positionSide"] = pos_side
        try:
            ctx.exchange.cancel_order(o["id"], symbol, params)
            canceled += 1
            if dbrec:
                try:
                    dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_conditional_by_side", raw=o)
                except Exception:
                    pass
        except Exception:
            try:
                ctx.exchange.cancel_order(o["id"], symbol)
                canceled += 1
                if dbrec:
                    try:
                        dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_conditional_by_side", raw=o)
                    except Exception:
                        pass
            except Exception:
                pass
    return {
        "status": "ok",
        "action": "cancel_conditional_by_side",
        "symbol": symbol,
        "side": side,
        "targets": targets,
        "canceled": canceled,
        "fetched": len(orders),
    }

def dca_short_if_needed(symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
    ctx = _get_ctx()
    if not ctx.dca_enabled:
        return {"status": "skip", "reason": "dca_disabled", "symbol": symbol}
    if adds_done >= DCA_MAX_ADDS:
        return {"status": "skip", "reason": "max_adds_reached", "symbol": symbol, "adds_done": adds_done}
    p = _find_position(symbol)
    if not p:
        return {"status": "skip", "reason": "no_short_position", "symbol": symbol}
    if not _is_short_position(p):
        return {"status": "skip", "reason": "not_short_position", "symbol": symbol}
    info = p.get("info") or {}

    entry = info.get("entryPrice") or info.get("avgPrice") or p.get("entryPrice")
    mark = info.get("markPrice") or p.get("markPrice")
    try:
        entry = float(entry); mark = float(mark)
    except Exception:
        return {"status": "skip", "reason": "price_unavailable", "symbol": symbol}

    if not should_dca_by_price(entry, mark, adds_done):
        return {
            "status": "skip",
            "reason": "not_triggered",
            "symbol": symbol,
            "adds_done": adds_done,
            "entry": entry,
            "mark": mark,
        }

    dca_usdt = _calc_dca_usdt()
    if not isinstance(dca_usdt, (int, float)):
        return {
            "status": "skip",
            "reason": "balance_unavailable",
            "symbol": symbol,
            "adds_done": adds_done,
        }
    res = short_market(symbol, usdt_amount=dca_usdt, leverage=ctx.default_leverage, margin_mode=margin_mode)
    res.update({
        "adds_done_before": adds_done,
        "adds_done_after": adds_done + 1,
        "dca_usdt": dca_usdt,
        "entry": entry,
        "mark": mark,
    })
    return res

def dca_long_if_needed(symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
    ctx = _get_ctx()
    if not ctx.dca_enabled:
        return {"status": "skip", "reason": "dca_disabled", "symbol": symbol}
    if adds_done >= DCA_MAX_ADDS:
        return {"status": "skip", "reason": "max_adds_reached", "symbol": symbol, "adds_done": adds_done}
    p = _find_long_position(symbol)
    if not p:
        return {"status": "skip", "reason": "no_long_position", "symbol": symbol}
    if not _is_long_position(p):
        return {"status": "skip", "reason": "not_long_position", "symbol": symbol}
    info = p.get("info") or {}

    entry = info.get("entryPrice") or info.get("avgPrice") or p.get("entryPrice")
    mark = info.get("markPrice") or p.get("markPrice")
    try:
        entry = float(entry); mark = float(mark)
    except Exception:
        return {"status": "skip", "reason": "price_unavailable", "symbol": symbol}

    if not should_dca_by_price_long(entry, mark, adds_done):
        return {
            "status": "skip",
            "reason": "not_triggered",
            "symbol": symbol,
            "adds_done": adds_done,
            "entry": entry,
            "mark": mark,
        }

    dca_usdt = _calc_dca_usdt()
    if not isinstance(dca_usdt, (int, float)):
        return {
            "status": "skip",
            "reason": "balance_unavailable",
            "symbol": symbol,
            "adds_done": adds_done,
        }
    res = long_market(symbol, usdt_amount=dca_usdt, leverage=ctx.default_leverage, margin_mode=margin_mode)
    res.update({
        "adds_done_before": adds_done,
        "adds_done_after": adds_done + 1,
        "dca_usdt": dca_usdt,
        "entry": entry,
        "mark": mark,
    })
    return res
