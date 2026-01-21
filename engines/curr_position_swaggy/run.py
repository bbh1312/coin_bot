from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from env_loader import load_env
import cycle_cache
from atlas_test.data_feed import fetch_ohlcv
from atlas_test.notifier_telegram import send_message
from engines.curr_position_swaggy.config import CurrPositionSwaggyConfig
from engines.swaggy_no_atlas.engine import SwaggyNoAtlasEngine
from engines.swaggy_no_atlas.config import SwaggyNoAtlasConfig
from executor import exchange, list_open_position_symbols, get_long_position_detail, get_short_position_detail


def _fmt_kst_now() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S KST")


def _sleep_to_boundary(interval_sec: int) -> None:
    now = time.time()
    next_ts = (int(now // interval_sec) + 1) * interval_sec
    time.sleep(max(1, next_ts - now))


def _build_pos_side_map() -> Dict[str, str]:
    pos = list_open_position_symbols(force=True)
    longs = pos.get("long") or set()
    shorts = pos.get("short") or set()
    symbols = set(longs) | set(shorts)
    out: Dict[str, str] = {}
    for sym in symbols:
        has_long = sym in longs
        has_short = sym in shorts
        if has_long and has_short:
            out[sym] = "LONG+SHORT"
        elif has_long:
            out[sym] = "LONG"
        elif has_short:
            out[sym] = "SHORT"
    return out


def _side_kr(side: str) -> str:
    side = (side or "").upper()
    if side == "LONG":
        return "롱"
    if side == "SHORT":
        return "숏"
    if side == "LONG+SHORT":
        return "롱+숏"
    return side or "N/A"


def _fmt_roi(val: object) -> str:
    if isinstance(val, (int, float)):
        return f"{val:.2f}%"
    return "n/a"


def _pos_roi_text(symbol: str, pos_side: str) -> str:
    pos_side = (pos_side or "").upper()
    if pos_side == "LONG":
        roi = (get_long_position_detail(symbol) or {}).get("roi")
        return _fmt_roi(roi)
    if pos_side == "SHORT":
        roi = (get_short_position_detail(symbol) or {}).get("roi")
        return _fmt_roi(roi)
    if pos_side == "LONG+SHORT":
        roi_l = (get_long_position_detail(symbol) or {}).get("roi")
        roi_s = (get_short_position_detail(symbol) or {}).get("roi")
        return f"롱 {_fmt_roi(roi_l)} / 숏 {_fmt_roi(roi_s)}"
    return "n/a"


def _build_message(entries: list[dict]) -> str:
    lines = [
        "[CURR-POS-SWAGGY] 15m 진입 판단",
        f"시간: {_fmt_kst_now()}",
        "----",
    ]
    for item in entries:
        sym = item["symbol"].replace("/USDT:USDT", "")
        pos_side = item["pos_side"]
        roi_text = _pos_roi_text(item["symbol"], pos_side)
        entry_px = item.get("entry_px")
        entry_px_str = f"{float(entry_px):.6g}" if isinstance(entry_px, (int, float)) else "n/a"
        lines.append(
            f"{sym} / { _side_kr(pos_side) } / ROI: {roi_text} : "
            f"{_side_kr(item['side'])} 판단(진입가: {entry_px_str})"
        )
    return "\n".join(lines)


def _set_cache(symbol: str, tf: str, data: list) -> bool:
    if not data:
        return False
    formatted = [[c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"]] for c in data]
    cycle_cache.set_raw(symbol, tf, formatted)
    return True


def _fetch_and_cache(symbol: str, tf: str, limit: int) -> bool:
    ohlcv = fetch_ohlcv(exchange, symbol, tf, limit, drop_last=True)
    return _set_cache(symbol, tf, ohlcv)


def main() -> None:
    load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN_CURR_POSITION", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_CURR_POSITION", "").strip()
    if not token or not chat_id:
        print("[curr-pos-swaggy] missing TELEGRAM_BOT_TOKEN_CURR_POSITION or TELEGRAM_CHAT_ID_CURR_POSITION")
        return

    cfg = CurrPositionSwaggyConfig()
    swaggy_cfg = SwaggyNoAtlasConfig()
    swaggy_engine = SwaggyNoAtlasEngine(swaggy_cfg)
    ltf_limit = max(int(swaggy_cfg.ltf_limit), 120)
    mtf_limit = 200
    htf_limit = max(int(swaggy_cfg.vp_lookback_1h), 120)
    htf2_limit = 200
    d1_limit = 120
    tf_3m_limit = 30
    exchange.load_markets()

    print("[curr-pos-swaggy] start (15m)")
    while True:
        pos_map = _build_pos_side_map()
        if not pos_map:
            print("[curr-pos-swaggy] no open positions")
            _sleep_to_boundary(cfg.interval_sec)
            continue
        entries: list[dict] = []
        for sym, pos_side in sorted(pos_map.items()):
            if not _fetch_and_cache(sym, swaggy_cfg.tf_ltf, ltf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_mtf, mtf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_htf, htf_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_htf2, htf2_limit):
                continue
            if not _fetch_and_cache(sym, swaggy_cfg.tf_d1, d1_limit):
                continue
            if not _fetch_and_cache(sym, "3m", tf_3m_limit):
                continue

            df_5m = cycle_cache.get_df(sym, swaggy_cfg.tf_ltf, limit=ltf_limit)
            df_15m = cycle_cache.get_df(sym, swaggy_cfg.tf_mtf, limit=mtf_limit)
            df_1h = cycle_cache.get_df(sym, swaggy_cfg.tf_htf, limit=htf_limit)
            df_4h = cycle_cache.get_df(sym, swaggy_cfg.tf_htf2, limit=htf2_limit)
            df_1d = cycle_cache.get_df(sym, swaggy_cfg.tf_d1, limit=d1_limit)
            df_3m = cycle_cache.get_df(sym, "3m", limit=tf_3m_limit)
            if df_5m.empty or df_15m.empty or df_1h.empty or df_4h.empty or df_3m.empty:
                continue

            signal = swaggy_engine.evaluate_symbol(
                sym,
                df_4h,
                df_1h,
                df_15m,
                df_5m,
                df_3m,
                df_1d,
                time.time(),
            )
            if signal.entry_ok and signal.side:
                entries.append(
                    {
                        "symbol": sym,
                        "pos_side": pos_side,
                        "side": signal.side.upper(),
                        "entry_px": signal.entry_px,
                    }
                )
        if not entries:
            print("[curr-pos-swaggy] no entry-ready symbols")
            _sleep_to_boundary(cfg.interval_sec)
            continue
        msg = _build_message(entries)
        ok = send_message(token, chat_id, msg)
        print(f"[curr-pos-swaggy] sent={ok} count={len(entries)}")
        _sleep_to_boundary(cfg.interval_sec)


if __name__ == "__main__":
    main()
