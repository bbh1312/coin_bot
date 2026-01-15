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
_POS_CACHE = {}
_POS_LONG_CACHE = {}
_POS_ALL_CACHE = {"ts": 0.0, "positions_by_symbol": {}}

# --- Balance TTL cache (USDT available) ---
_BAL_TTL_SEC = 5.0
_BAL_CACHE = {"ts": 0.0, "available": None}
_BAL_LOCK = threading.Lock()

# 전역 레이트리밋 백오프 (engine_runner와 공유 목적)
GLOBAL_BACKOFF_UNTIL = 0.0
_BACKOFF_SECS = 0.0

def trigger_rate_limit_backoff(msg: str = ""):
    """429/-1003 감지 시 외부에서 호출해 전역 백오프를 공유."""
    global GLOBAL_BACKOFF_UNTIL, _BACKOFF_SECS
    _BACKOFF_SECS = 5.0 if _BACKOFF_SECS <= 0 else min(_BACKOFF_SECS * 1.5, 30.0)
    GLOBAL_BACKOFF_UNTIL = time.time() + _BACKOFF_SECS
    print(f"[rate-limit/executor] 백오프 {_BACKOFF_SECS:.1f}s (사유: {str(msg)[:80]})")

def get_global_backoff_until() -> float:
    return float(GLOBAL_BACKOFF_UNTIL or 0.0)

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
    now = time.time()
    with _BAL_LOCK:
        ts = float(_BAL_CACHE.get("ts", 0.0) or 0.0)
        cached = _BAL_CACHE.get("available")
        if cached is not None and (now - ts) <= ttl_sec:
            return float(cached)
    try:
        balance = exchange.fetch_balance({"type": "swap"})
    except Exception:
        try:
            balance = exchange.fetch_balance({"type": "future"})
        except Exception:
            try:
                balance = exchange.fetch_balance()
            except Exception:
                return None
    available = _extract_usdt_available(balance)
    if isinstance(available, (int, float)) and available > 0:
        with _BAL_LOCK:
            _BAL_CACHE["ts"] = now
            _BAL_CACHE["available"] = float(available)
    return available


def _calc_dca_usdt() -> Optional[float]:
    available = get_available_usdt()
    if not isinstance(available, (int, float)) or available <= 0:
        return None
    usdt = float(available) * (DCA_PCT / 100.0)
    if usdt <= 0:
        return None
    return usdt

exchange = ccxt.binance({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
})

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

# --- Position mode (Hedge vs One-way) detection cache ---
_POSMODE_CACHE_TS = 0.0
_POSMODE_CACHE_VAL = False  # False=oneway, True=hedge
_POSMODE_TTL = 60.0

def is_hedge_mode() -> bool:
    """Return True if account is in Hedge mode.
    Priority: env override -> auto-detect (live only) -> default False.
    """
    global _POSMODE_CACHE_TS, _POSMODE_CACHE_VAL
    # Explicit override by env
    if POSITION_MODE == "hedge":
        return True
    if POSITION_MODE == "oneway":
        return False
    # DRY_RUN 여부와 관계없이 설정/감지 결과를 사용
    now = time.time()
    if (now - _POSMODE_CACHE_TS) <= _POSMODE_TTL:
        return _POSMODE_CACHE_VAL
    try:
        # ccxt raw method name variants
        if hasattr(exchange, "fapiPrivateGetPositionSideDual"):
            resp = exchange.fapiPrivateGetPositionSideDual()
        else:
            resp = exchange.fapiPrivate_get_positionsidedual()
        val = resp.get("dualSidePosition")
        if isinstance(val, str):
            val = val.lower() in ("true", "1", "yes")
        hedge = bool(val)
    except Exception:
        hedge = False
    _POSMODE_CACHE_TS = now
    _POSMODE_CACHE_VAL = hedge
    return hedge

def ensure_ready():
    # DRY_RUN이면 키 없이도 마켓 로드는 시도
    if DRY_RUN:
        try:
            exchange.load_markets()
            return
        except Exception:
            pass
    if not API_KEY or not API_SECRET:
        raise RuntimeError("환경변수 BINANCE_API_KEY / BINANCE_API_SECRET 설정 필요")
    exchange.load_markets()

def set_dry_run(flag: bool) -> bool:
    """런타임에 DRY_RUN 토글. True=dry-run, False=live"""
    global DRY_RUN
    DRY_RUN = bool(flag)
    return DRY_RUN

def set_leverage_and_margin(symbol: str, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated"):
    try:
        exchange.set_margin_mode(margin_mode, symbol)
    except Exception as e:
        print("[margin_mode] warn:", e)
    try:
        exchange.set_leverage(leverage, symbol)
    except Exception as e:
        print("[leverage] warn:", e)

def _get_position_cached(symbol: str):
    """Return position object for symbol using TTL cache.
    Cache entry: symbol -> (ts, position_or_None)
    """
    now = time.time()
    # 1) 전체 포지션 캐시 우선 사용
    try:
        ts_all = float(_POS_ALL_CACHE.get("ts", 0.0) or 0.0)
        if (now - ts_all) <= POS_TTL_SEC:
            pos_map = _POS_ALL_CACHE.get("positions_by_symbol") or {}
            pos = _select_short_position(pos_map.get(symbol) or []) if symbol in pos_map else None
            _POS_CACHE[symbol] = (now, pos)
            return pos
    except Exception:
        pass
    try:
        ts, pos = _POS_CACHE.get(symbol, (0.0, None))
    except Exception:
        ts, pos = 0.0, None
    if (now - ts) <= POS_TTL_SEC:
        return pos
    ensure_ready()
    try:
        positions = exchange.fetch_positions([symbol])
    except Exception as e:
        msg = str(e)
        print("[fetch_positions] warn:", e)
        if ("429" in msg) or ("-1003" in msg):
            trigger_rate_limit_backoff(msg)
        # keep previous cache if exists
        _POS_CACHE[symbol] = (now, pos)
        return pos
    found = _select_short_position(positions)
    _POS_CACHE[symbol] = (now, found)
    return found

def _get_long_position_cached(symbol: str):
    """Return long position object for symbol using TTL cache."""
    now = time.time()
    try:
        ts_all = float(_POS_ALL_CACHE.get("ts", 0.0) or 0.0)
        if (now - ts_all) <= POS_TTL_SEC:
            pos_map = _POS_ALL_CACHE.get("positions_by_symbol") or {}
            pos = _select_long_position(pos_map.get(symbol) or []) if symbol in pos_map else None
            _POS_LONG_CACHE[symbol] = (now, pos)
            return pos
    except Exception:
        pass
    try:
        ts, pos = _POS_LONG_CACHE.get(symbol, (0.0, None))
    except Exception:
        ts, pos = 0.0, None
    if (now - ts) <= POS_TTL_SEC:
        return pos
    ensure_ready()
    try:
        positions = exchange.fetch_positions([symbol])
    except Exception as e:
        msg = str(e)
        print("[fetch_positions] warn:", e)
        if ("429" in msg) or ("-1003" in msg):
            trigger_rate_limit_backoff(msg)
        _POS_LONG_CACHE[symbol] = (now, pos)
        return pos
    found = _select_long_position(positions)
    _POS_LONG_CACHE[symbol] = (now, found)
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
    now = time.time()
    try:
        ts_all = float(_POS_ALL_CACHE.get("ts", 0.0) or 0.0)
        if (not force) and (now - ts_all) <= POS_TTL_SEC:
            return True
    except Exception:
        pass
    ensure_ready()
    try:
        positions = exchange.fetch_positions()
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
        pos_map.setdefault(sym, []).append(p)
    _POS_ALL_CACHE["ts"] = now
    _POS_ALL_CACHE["positions_by_symbol"] = pos_map
    return True


def count_open_positions(force: bool = False) -> Optional[int]:
    """현재 열린 포지션 수(롱/숏 합산)를 반환한다."""
    if not refresh_positions_cache(force=force):
        return None
    pos_map = _POS_ALL_CACHE.get("positions_by_symbol") or {}
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
    if not refresh_positions_cache(force=force):
        return {"long": set(), "short": set()}
    pos_map = _POS_ALL_CACHE.get("positions_by_symbol") or {}
    longs = set()
    shorts = set()
    for sym, positions in pos_map.items():
        if not positions:
            continue
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
    if random.random() < 0.05:
        info = p.get("info") or {}
        raw_amt = info.get("positionAmt") or p.get("positionAmt") or p.get("contracts")
        print(f"[pos-sample] sym={symbol} raw_amt={raw_amt} parsed={amt:.6f}")
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
    if entry <= 0:
        return False
    adverse_pct = (mark - entry) / entry * 100.0
    if adds_done <= 0:
        return adverse_pct >= DCA_FIRST_PCT
    if adds_done == 1:
        return adverse_pct >= DCA_SECOND_PCT
    return adverse_pct >= DCA_THIRD_PCT

def should_dca_by_price_long(entry: float, mark: float, adds_done: int) -> bool:
    """Adverse move vs entry for long. Triggers when mark price falls by configured %."""
    if entry <= 0:
        return False
    adverse_pct = (entry - mark) / entry * 100.0
    if adds_done <= 0:
        return adverse_pct >= DCA_FIRST_PCT
    if adds_done == 1:
        return adverse_pct >= DCA_SECOND_PCT
    return adverse_pct >= DCA_THIRD_PCT

def short_market(symbol: str, usdt_amount: float = BASE_ENTRY_USDT, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "isolated") -> dict:
    ensure_ready()
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = exchange.market(symbol)
    last = float(exchange.fetch_ticker(symbol)["last"])
    # usdt_amount를 증거금으로 해석하고 레버리지를 곱해 명목가로 변환
    notional = float(usdt_amount) * float(leverage)
    amount = float(exchange.amount_to_precision(symbol, notional / last))

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
    if DRY_RUN:
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
    order = exchange.create_market_sell_order(symbol, amount, params=params)
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
    ensure_ready()
    if price <= 0:
        return {"status": "skip", "reason": "price_unavailable", "symbol": symbol}
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = exchange.market(symbol)
    notional = float(usdt_amount) * float(leverage)
    amount = float(exchange.amount_to_precision(symbol, notional / float(price)))

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
    if DRY_RUN:
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
    order = exchange.create_limit_sell_order(symbol, amount, price, params=params)
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
    ensure_ready()
    set_leverage_and_margin(symbol, leverage=leverage, margin_mode=margin_mode)
    market = exchange.market(symbol)
    last = float(exchange.fetch_ticker(symbol)["last"])
    notional = float(usdt_amount) * float(leverage)
    amount = float(exchange.amount_to_precision(symbol, notional / last))

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
    if DRY_RUN:
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
    order = exchange.create_market_buy_order(symbol, amount, params=params)
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
    ensure_ready()
    amount = get_short_position_amount(symbol)
    if amount <= 0:
        return {"status": "skip", "reason": "no_short_position", "symbol": symbol}
    amount = float(exchange.amount_to_precision(symbol, amount))
    if DRY_RUN:
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
    order = exchange.create_market_buy_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {"status": "ok", "action": "close_short_market", "order": order, "order_id": order_id, "amount": amount}
    _db_record_order("close_short_market", symbol, "SHORT", res)
    return res

def close_long_market(symbol: str) -> dict:
    ensure_ready()
    amount = get_long_position_amount(symbol)
    if amount <= 0:
        return {"status": "skip", "reason": "no_long_position", "symbol": symbol}
    amount = float(exchange.amount_to_precision(symbol, amount))
    if DRY_RUN:
        res = {"status": "dry_run", "action": "close_long_market", "symbol": symbol, "amount": amount, "order_id": None}
        _db_record_order("close_long_market", symbol, "LONG", res, status_override="dry_run")
        return res
    hedge = False
    try:
        hedge = is_hedge_mode()
    except Exception:
        hedge = False
    params = {"positionSide": "LONG"} if hedge else {"reduceOnly": True}
    order = exchange.create_market_sell_order(symbol, amount, params=params)
    order_id = order.get("id") or (order.get("info") or {}).get("orderId")
    res = {"status": "ok", "action": "close_long_market", "order": order, "order_id": order_id, "amount": amount}
    _db_record_order("close_long_market", symbol, "LONG", res)
    return res

def cancel_open_orders(symbol: str) -> dict:
    ensure_ready()
    if DRY_RUN:
        return {"status": "dry_run", "action": "cancel_open_orders", "symbol": symbol}
    try:
        orders = exchange.fetch_open_orders(symbol)
        canceled = 0
        for o in orders:
            try:
                exchange.cancel_order(o["id"], symbol)
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
    ensure_ready()
    if DRY_RUN:
        return {"status": "dry_run", "action": "cancel_stop_orders", "symbol": symbol}
    try:
        orders = []
        try:
            orders = exchange.fetch_open_orders(symbol)
        except Exception:
            orders = []
        # Some exchanges separate conditional/stop orders; try extra fetches.
        extra_orders = []
        for params in ({"type": "stop"}, {"stop": True}, {"trigger": True}):
            try:
                extra_orders.extend(exchange.fetch_open_orders(symbol, params))
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
                    exchange.cancel_order(o["id"], symbol, params)
                    canceled += 1
                    if dbrec:
                        try:
                            dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_stop_orders", raw=o)
                        except Exception:
                            pass
                except Exception:
                    try:
                        exchange.cancel_order(o["id"], symbol)
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
                    exchange.cancel_all_orders(symbol, params)
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
    ensure_ready()
    if DRY_RUN:
        return {"status": "dry_run", "action": "cancel_conditional_by_side", "symbol": symbol, "side": side}
    orders = []
    seen_ids = set()
    fetch_errors = []
    for params in ({}, {"type": "stop"}, {"stop": True}, {"trigger": True}, {"reduceOnly": True}):
        try:
            batch = exchange.fetch_open_orders(symbol, params) if params else exchange.fetch_open_orders(symbol)
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
            exchange.cancel_order(o["id"], symbol, params)
            canceled += 1
            if dbrec:
                try:
                    dbrec.record_cancel(order_id=o.get("id"), symbol=symbol, side=pos_side, ts=time.time(), reason="cancel_conditional_by_side", raw=o)
                except Exception:
                    pass
        except Exception:
            try:
                exchange.cancel_order(o["id"], symbol)
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
    if not DCA_ENABLED:
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
    res = short_market(symbol, usdt_amount=dca_usdt, leverage=DEFAULT_LEVERAGE, margin_mode=margin_mode)
    res.update({
        "adds_done_before": adds_done,
        "adds_done_after": adds_done + 1,
        "dca_usdt": dca_usdt,
        "entry": entry,
        "mark": mark,
    })
    return res

def dca_long_if_needed(symbol: str, adds_done: int, margin_mode: str = "isolated") -> dict:
    if not DCA_ENABLED:
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
    res = long_market(symbol, usdt_amount=dca_usdt, leverage=DEFAULT_LEVERAGE, margin_mode=margin_mode)
    res.update({
        "adds_done_before": adds_done,
        "adds_done_after": adds_done + 1,
        "dca_usdt": dca_usdt,
        "entry": entry,
        "mark": mark,
    })
    return res
