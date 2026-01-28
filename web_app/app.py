from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, jsonify, redirect, render_template, request, url_for
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from engine_runner import (
    _build_account_contexts,
    _build_daily_report,
    _engine_label_from_reason,
    _get_open_trade,
    _log_trade_entry,
    _order_id_from_res,
    load_state,
    load_state_from,
    save_state,
    save_state_to,
    LIVE_TRADING,
    LONG_LIVE_TRADING,
    AUTO_EXIT_ENABLED,
    MAX_OPEN_POSITIONS,
    USDT_PER_TRADE,
    BASE_ENTRY_USDT,
    SWAGGY_ATLAS_LAB_ENABLED,
    SWAGGY_ATLAS_LAB_V2_ENABLED,
    SWAGGY_NO_ATLAS_ENABLED,
    ADV_TREND_ENABLED,
    SWAGGY_ATLAS_LAB_OFF_WINDOWS,
    SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS,
    SWAGGY_NO_ATLAS_OFF_WINDOWS,
    SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN,
    SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED,
    SWAGGY_D1_OVEREXT_ATR_MULT,
    SATURDAY_TRADE_ENABLED,
    DTFX_ENABLED,
    ATLAS_RS_FAIL_SHORT_ENABLED,
    RSI_ENABLED,
    LOSS_HEDGE_ENGINE_ENABLED,
    DCA_ENABLED,
    DCA_PCT,
    DCA_FIRST_PCT,
    DCA_SECOND_PCT,
    DCA_THIRD_PCT,
    EXIT_COOLDOWN_HOURS,
    send_telegram,
    AUTO_EXIT_LONG_TP_PCT,
    AUTO_EXIT_LONG_SL_PCT,
    AUTO_EXIT_SHORT_TP_PCT,
    AUTO_EXIT_SHORT_SL_PCT,
    ENGINE_EXIT_OVERRIDES,
)
import accounts_db
from executor import AccountExecutor
from account_context import AccountContext, AccountSettings

_FOLLOWER_POS_CACHE = {"ts": 0.0, "items": [], "groups": {}}
_FOLLOWER_POS_TTL_SEC = float(os.getenv("FOLLOWER_POS_TTL_SEC", "0"))
_WEB_MAX_WORKERS = int(os.getenv("WEB_MAX_WORKERS", "8"))

def _run_parallel_tasks(tasks: list) -> list:
    if not tasks:
        return []
    max_workers = min(len(tasks), max(1, _WEB_MAX_WORKERS))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(fn): meta for fn, meta in tasks}
        for fut in as_completed(future_map):
            meta = future_map.get(fut) or {}
            try:
                results.append(fut.result())
            except Exception as e:
                results.append({"status": "fail", "error": str(e), **meta})
    return results

COMMAND_DEFS = [
    {"cmd": "/live", "key": "_live_trading", "label": "Live Shorts", "type": "toggle"},
    {"cmd": "/long_live", "key": "_long_live", "label": "Live Longs", "type": "toggle"},
    {"cmd": "/auto_exit", "key": "_auto_exit", "label": "Auto Exit", "type": "toggle"},
    {"cmd": "/sat_trade", "key": "_sat_trade", "label": "토요일 진입", "type": "toggle"},
    {"cmd": "/swaggy_atlas_lab", "key": "_swaggy_atlas_lab_enabled", "label": "Swaggy Atlas Lab", "type": "toggle"},
    {"cmd": "/swaggy_atlas_lab_v2", "key": "_swaggy_atlas_lab_v2_enabled", "label": "Swaggy Atlas Lab V2", "type": "toggle"},
    {"cmd": "/swaggy_no_atlas", "key": "_swaggy_no_atlas_enabled", "label": "Swaggy No Atlas", "type": "toggle"},
    {"cmd": "/adv_trend", "key": "_adv_trend_enabled", "label": "Triple-Check Engine", "type": "toggle"},
    {"cmd": "/loss_hedge_engine", "key": "_loss_hedge_engine_enabled", "label": "손실방지엔진", "type": "toggle"},
    {"cmd": "/loss_hedge_interval", "key": "_loss_hedge_interval_min", "label": "손실방지 체크 주기(분)", "type": "int", "step": 1},
    {"cmd": "/swaggy_atlas_lab_off", "key": "_swaggy_atlas_lab_off_windows", "label": "Swaggy Lab Off Windows", "type": "text"},
    {"cmd": "/swaggy_atlas_lab_v2_off", "key": "_swaggy_atlas_lab_v2_off_windows", "label": "Swaggy Lab V2 Off Windows", "type": "text"},
    {"cmd": "/swaggy_no_atlas_off", "key": "_swaggy_no_atlas_off_windows", "label": "Swaggy No Atlas Off Windows", "type": "text"},
    {"cmd": "/swaggy_no_atlas_overext", "key": "_swaggy_no_atlas_overext_min", "label": "No Atlas Overext Min", "type": "number", "step": 0.01},
    {"cmd": "/swaggy_no_atlas_overext_on", "key": "_swaggy_no_atlas_overext_min_enabled", "label": "No Atlas Overext Min On/Off", "type": "toggle"},
    {"cmd": "/swaggy_d1_overext", "key": "_swaggy_d1_overext_atr_mult", "label": "Swaggy D1 Overext ATR", "type": "number", "step": 0.1},
    {"cmd": "/dtfx", "key": "_dtfx_enabled", "label": "DTFX", "type": "toggle"},
    {"cmd": "/atlas_rs_fail_short", "key": "_atlas_rs_fail_short_enabled", "label": "Atlas RS Fail Short", "type": "toggle"},
    {"cmd": "/rsi", "key": "_rsi_enabled", "label": "RSI", "type": "toggle"},
    {"cmd": "/dca", "key": "_dca_enabled", "label": "DCA", "type": "toggle"},
    {"cmd": "/dca_pct", "key": "_dca_pct", "label": "DCA 진입 금액(%)", "type": "number", "step": 0.1},
    {"cmd": "/dca1", "key": "_dca_first_pct", "label": "DCA1(%)", "type": "number", "step": 0.1},
    {"cmd": "/dca2", "key": "_dca_second_pct", "label": "DCA2(%)", "type": "number", "step": 0.1},
    {"cmd": "/dca3", "key": "_dca_third_pct", "label": "DCA3(%)", "type": "number", "step": 0.1},
    {"cmd": "/exit_cd_h", "key": "_exit_cooldown_hours", "label": "재진입 시간(h)", "type": "number", "step": 0.1},
    {"cmd": "/entry_usdt", "key": "_entry_usdt", "label": "진입 금액(%)", "type": "number", "step": 0.1},
    {"cmd": "/l_exit_tp", "key": "_auto_exit_long_tp_pct", "label": "롱 TP (%)", "type": "number", "step": 0.1},
    {"cmd": "/l_exit_sl", "key": "_auto_exit_long_sl_pct", "label": "롱 SL (%)", "type": "number", "step": 0.1},
    {"cmd": "/s_exit_tp", "key": "_auto_exit_short_tp_pct", "label": "숏 TP (%)", "type": "number", "step": 0.1},
    {"cmd": "/s_exit_sl", "key": "_auto_exit_short_sl_pct", "label": "숏 SL (%)", "type": "number", "step": 0.1},
    {"cmd": "/engine_exit", "key": "_engine_exit_overrides", "label": "엔진 TP/SL JSON", "type": "json"},
    {"cmd": "/max_pos", "key": "_max_open_positions", "label": "최대 포지션 수", "type": "int", "step": 1},
]
COMMANDS_BY_CMD = {d["cmd"]: d for d in COMMAND_DEFS}
COMMANDS_BY_KEY = {d["key"]: d for d in COMMAND_DEFS}


def _build_account_contexts_web(include_inactive: bool = False) -> list[AccountContext]:
    accounts_db.ensure_default_account("admin")
    accounts = accounts_db.list_all_accounts() if include_inactive else accounts_db.list_active_accounts()
    contexts: list[AccountContext] = []
    position_mode = os.getenv("POSITION_MODE", "hedge").lower().strip()
    admin_account_id = None
    for acct in accounts:
        if str(acct.get("name")) == "admin":
            admin_account_id = int(acct["id"])
            break
    admin_settings_raw = accounts_db.get_account_settings(admin_account_id) if admin_account_id else {}
    defaults = accounts_db.DEFAULT_SETTINGS
    for acct in accounts:
        account_id = int(acct["id"])
        settings_raw = admin_settings_raw

        def _get_setting(key: str, default_val: object) -> object:
            if key in settings_raw and settings_raw.get(key) is not None:
                return settings_raw.get(key)
            return default_val

        settings = AccountSettings(
            entry_pct=float(_get_setting("entry_pct", defaults["entry_pct"])),
            dry_run=_coerce_bool(_get_setting("dry_run", defaults["dry_run"])),
            auto_exit=_coerce_bool(_get_setting("auto_exit", defaults["auto_exit"])),
            max_positions=int(_get_setting("max_positions", defaults["max_positions"])),
            leverage=int(_get_setting("leverage", defaults["leverage"])),
            margin_mode=str(_get_setting("margin_mode", defaults["margin_mode"])),
            exit_cooldown_h=float(_get_setting("exit_cooldown_h", defaults["exit_cooldown_h"])),
            long_tp_pct=float(_get_setting("long_tp_pct", defaults["long_tp_pct"])),
            long_sl_pct=float(_get_setting("long_sl_pct", defaults["long_sl_pct"])),
            short_tp_pct=float(_get_setting("short_tp_pct", defaults["short_tp_pct"])),
            short_sl_pct=float(_get_setting("short_sl_pct", defaults["short_sl_pct"])),
            dca_enabled=_coerce_bool(_get_setting("dca_enabled", defaults["dca_enabled"])),
            dca_pct=float(_get_setting("dca_pct", defaults["dca_pct"])),
            dca1_pct=float(_get_setting("dca1_pct", defaults["dca1_pct"])),
            dca2_pct=float(_get_setting("dca2_pct", defaults["dca2_pct"])),
            dca3_pct=float(_get_setting("dca3_pct", defaults["dca3_pct"])),
        )
        state_path = "state.json" if str(acct.get("name")) == "admin" else f"state_{account_id}.json"
        executor = AccountExecutor(
            api_key=str(acct.get("api_key") or ""),
            api_secret=str(acct.get("api_secret") or ""),
            dry_run=settings.dry_run,
            position_mode=position_mode,
            default_leverage=settings.leverage,
            base_entry_usdt=BASE_ENTRY_USDT,
            dca_enabled=settings.dca_enabled,
            dca_pct=settings.dca_pct,
            dca_first_pct=settings.dca1_pct,
            dca_second_pct=settings.dca2_pct,
            dca_third_pct=settings.dca3_pct,
            account_name=str(acct.get("name") or account_id),
        )
        contexts.append(
            AccountContext(
                account_id=account_id,
                name=str(acct.get("name") or account_id),
                api_key=str(acct.get("api_key") or ""),
                api_secret=str(acct.get("api_secret") or ""),
                settings=settings,
                executor=executor,
                telegram=None,
                state_path=state_path,
                meta={"db": acct},
            )
        )
    return contexts

def _sync_admin_settings_from_state(state: dict) -> None:
    if not isinstance(state, dict):
        return
    try:
        admin_id = accounts_db.get_account_id_by_name("admin")
    except Exception:
        admin_id = None
    if not admin_id:
        return
    mapping = {
        "_entry_usdt": ("entry_pct", float),
        "_auto_exit": ("auto_exit", int),
        "_max_open_positions": ("max_positions", int),
        "_exit_cooldown_hours": ("exit_cooldown_h", float),
        "_auto_exit_long_tp_pct": ("long_tp_pct", float),
        "_auto_exit_long_sl_pct": ("long_sl_pct", float),
        "_auto_exit_short_tp_pct": ("short_tp_pct", float),
        "_auto_exit_short_sl_pct": ("short_sl_pct", float),
        "_dca_enabled": ("dca_enabled", int),
        "_dca_pct": ("dca_pct", float),
        "_dca_first_pct": ("dca1_pct", float),
        "_dca_second_pct": ("dca2_pct", float),
        "_dca_third_pct": ("dca3_pct", float),
    }
    for state_key, (db_key, cast) in mapping.items():
        if state_key not in state:
            continue
        val = state.get(state_key)
        if val is None:
            continue
        try:
            if cast is int:
                val = int(bool(val)) if isinstance(val, bool) else int(val)
            else:
                val = cast(val)
        except Exception:
            continue
        try:
            accounts_db.update_account_setting(admin_id, db_key, val)
        except Exception:
            pass
    if "_live_trading" in state:
        try:
            dry_run_val = 0 if bool(state.get("_live_trading")) else 1
            accounts_db.update_account_setting(admin_id, "dry_run", int(dry_run_val))
        except Exception:
            pass

app = Flask(__name__, template_folder="templates")
DEFAULTS = {
    "_live_trading": LIVE_TRADING,
    "_long_live": LONG_LIVE_TRADING,
    "_auto_exit": AUTO_EXIT_ENABLED,
    "_sat_trade": SATURDAY_TRADE_ENABLED,
    "_max_open_positions": MAX_OPEN_POSITIONS,
    "_entry_usdt": USDT_PER_TRADE,
    "_swaggy_atlas_lab_enabled": SWAGGY_ATLAS_LAB_ENABLED,
    "_swaggy_atlas_lab_v2_enabled": SWAGGY_ATLAS_LAB_V2_ENABLED,
    "_swaggy_no_atlas_enabled": SWAGGY_NO_ATLAS_ENABLED,
    "_adv_trend_enabled": ADV_TREND_ENABLED,
    "_loss_hedge_engine_enabled": LOSS_HEDGE_ENGINE_ENABLED,
    "_loss_hedge_interval_min": 15,
    "_swaggy_atlas_lab_off_windows": SWAGGY_ATLAS_LAB_OFF_WINDOWS,
    "_swaggy_atlas_lab_v2_off_windows": SWAGGY_ATLAS_LAB_V2_OFF_WINDOWS,
    "_swaggy_no_atlas_off_windows": SWAGGY_NO_ATLAS_OFF_WINDOWS,
    "_swaggy_no_atlas_overext_min": SWAGGY_NO_ATLAS_OVEREXT_ENTRY_MIN,
    "_swaggy_no_atlas_overext_min_enabled": SWAGGY_NO_ATLAS_OVEREXT_MIN_ENABLED,
    "_swaggy_d1_overext_atr_mult": SWAGGY_D1_OVEREXT_ATR_MULT,
    "_dtfx_enabled": DTFX_ENABLED,
    "_rsi_enabled": RSI_ENABLED,
    "_dca_enabled": DCA_ENABLED,
    "_dca_pct": DCA_PCT,
    "_dca_first_pct": DCA_FIRST_PCT,
    "_dca_second_pct": DCA_SECOND_PCT,
    "_dca_third_pct": DCA_THIRD_PCT,
    "_exit_cooldown_hours": EXIT_COOLDOWN_HOURS,
    "_atlas_rs_fail_short_enabled": ATLAS_RS_FAIL_SHORT_ENABLED,
    "_auto_exit_long_tp_pct": AUTO_EXIT_LONG_TP_PCT,
    "_auto_exit_long_sl_pct": AUTO_EXIT_LONG_SL_PCT,
    "_auto_exit_short_tp_pct": AUTO_EXIT_SHORT_TP_PCT,
    "_auto_exit_short_sl_pct": AUTO_EXIT_SHORT_SL_PCT,
    "_engine_exit_overrides": ENGINE_EXIT_OVERRIDES,
}


@app.route("/positions")
def follower_positions():
    items, groups = _build_follower_positions()
    total = len(items)
    admin_total = len(groups.get("admin") or [])
    follower_total = len(groups.get("followers") or [])
    user_filters = []
    seen = set()
    for row in items:
        name = row.get("account")
        if not name or name in seen or str(name) == "admin":
            continue
        seen.add(name)
        user_filters.append(str(name))
    user_filters.sort()
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    notice = request.args.get("notice")
    return render_template(
        "positions.html",
        items=items,
        total=total,
        admin_total=admin_total,
        follower_total=follower_total,
        user_filters=user_filters,
        updated_at=updated_at,
        notice=notice,
    )


@app.route("/manual_entry", methods=["GET"])
def manual_entry():
    notice = request.args.get("notice")
    try:
        accounts = _list_accounts()
    except Exception:
        accounts = []
    return render_template("manual_entry.html", notice=notice, accounts=accounts)


def _extract_fill(res: dict) -> tuple[object, object]:
    if not isinstance(res, dict):
        return None, None
    fill_price = res.get("last")
    qty = res.get("amount")
    order = res.get("order") if isinstance(res.get("order"), dict) else {}
    if fill_price is None:
        fill_price = order.get("average") or order.get("price")
    if qty is None:
        qty = order.get("amount")
    return fill_price, qty


@app.post("/manual_entry")
def manual_entry_submit():
    symbol = _normalize_symbol(request.form.get("symbol") or "")
    side = (request.form.get("side") or "").strip().upper()
    try:
        entry_pct = float(request.form.get("entry_pct") or 0)
    except Exception:
        entry_pct = 0.0
    if not symbol or side not in ("LONG", "SHORT") or entry_pct <= 0:
        return redirect(url_for("manual_entry", notice="입력값을 확인해주세요."))
    if entry_pct > 100:
        entry_pct = 100.0

    selected_accounts = request.form.getlist("accounts")
    try:
        contexts = _build_account_contexts_web(include_inactive=True)
    except Exception:
        contexts = []
    if selected_accounts and "ALL" not in selected_accounts:
        contexts = [c for c in contexts if str(c.name) in set(selected_accounts)]

    def _entry_task(acct: AccountContext) -> dict:
        try:
            settings = _load_account_settings(int(acct.account_id))
        except Exception:
            settings = accounts_db.DEFAULT_SETTINGS
        executor = acct.executor
        usdt_amount = _entry_usdt_available(executor, entry_pct)
        if usdt_amount <= 0:
            return {"account": acct.name, "status": "skip", "reason": "no_usdt"}
        try:
            with executor.activate():
                if side == "LONG":
                    res = executor.long_market(
                        symbol,
                        usdt_amount=usdt_amount,
                        leverage=int(settings.get("leverage") or 10),
                        margin_mode=str(settings.get("margin_mode") or "cross"),
                    )
                else:
                    res = executor.short_market(
                        symbol,
                        usdt_amount=usdt_amount,
                        leverage=int(settings.get("leverage") or 10),
                        margin_mode=str(settings.get("margin_mode") or "cross"),
                    )
            entry_price, qty = _extract_fill(res)
            entry_order_id = _order_id_from_res(res)
            state_path = _state_path_for_account(acct.meta.get("db") or {"id": acct.account_id, "name": acct.name})
            state = load_state_from(state_path)
            _log_trade_entry(
                state,
                side=side,
                symbol=symbol,
                entry_ts=time.time(),
                entry_price=entry_price if isinstance(entry_price, (int, float)) else None,
                qty=qty if isinstance(qty, (int, float)) else None,
                usdt=usdt_amount,
                entry_order_id=entry_order_id,
                meta={"reason": "관리자수동진입", "engine": "관리자수동진입"},
            )
            save_state_to(state, state_path)
            return {"account": acct.name, "status": "ok"}
        except Exception as e:
            return {"account": acct.name, "status": "fail", "error": str(e)[:80]}

    def _task_for(acct: AccountContext):
        return lambda: _entry_task(acct)

    tasks = [(_task_for(c), {"account": c.name}) for c in contexts]
    results_raw = _run_parallel_tasks(tasks)
    ok = sum(1 for r in results_raw if r.get("status") == "ok")
    fail = sum(1 for r in results_raw if r.get("status") == "fail")
    results = []
    for r in results_raw:
        acct_name = r.get("account") or r.get("acct") or "unknown"
        status = r.get("status")
        if status == "ok":
            results.append(f"{acct_name}: ok")
        elif status == "skip":
            results.append(f"{acct_name}: skip({r.get('reason')})")
        else:
            results.append(f"{acct_name}: fail({r.get('error')})")

    msg = f"관리자수동진입 {symbol} {side} pct={entry_pct:.2f}% ok={ok} fail={fail}"
    try:
        send_telegram(msg, allow_early=True)
    except Exception:
        pass
    notice = msg + " | " + ", ".join(results[:6]) + ("..." if len(results) > 6 else "")
    return redirect(url_for("manual_entry", notice=notice))


@app.post("/positions/close")
def close_positions():
    symbol = _normalize_symbol(request.form.get("symbol") or "")
    side = (request.form.get("side") or "").strip().upper()
    if not symbol or side not in ("LONG", "SHORT"):
        return redirect(url_for("follower_positions", notice="잘못된 요청"))
    try:
        contexts = _build_account_contexts_web(include_inactive=True)
    except Exception:
        contexts = []
    def _close_task(acct: AccountContext) -> dict:
        try:
            with acct.executor.activate():
                if side == "LONG":
                    amt = acct.executor.get_long_position_amount(symbol)
                    if not isinstance(amt, (int, float)) or amt <= 0:
                        return {"account": acct.name, "status": "skip"}
                    acct.executor.close_long_market(symbol)
                else:
                    amt = acct.executor.get_short_position_amount(symbol)
                    if not isinstance(amt, (int, float)) or amt <= 0:
                        return {"account": acct.name, "status": "skip"}
                    acct.executor.close_short_market(symbol)
            return {"account": acct.name, "status": "ok"}
        except Exception as e:
            return {"account": acct.name, "status": "fail", "error": str(e)[:80]}
    tasks = [(_close_task, {"account": c.name}) for c in contexts]
    results_raw = _run_parallel_tasks(tasks)
    ok = sum(1 for r in results_raw if r.get("status") == "ok")
    fail = sum(1 for r in results_raw if r.get("status") == "fail")
    skipped = sum(1 for r in results_raw if r.get("status") == "skip")
    msg = f"{symbol} {side} 청산: ok={ok} fail={fail} skip={skipped}"
    return redirect(url_for("follower_positions", notice=msg))


@app.post("/positions/close_single")
def close_single_position():
    symbol = _normalize_symbol(request.form.get("symbol") or "")
    side = (request.form.get("side") or "").strip().upper()
    account_name = (request.form.get("account") or "").strip()
    if not symbol or side not in ("LONG", "SHORT") or not account_name:
        return redirect(url_for("follower_positions", notice="잘못된 요청"))
    try:
        contexts = _build_account_contexts_web(include_inactive=True)
    except Exception:
        contexts = []
    target = next((c for c in contexts if str(c.name) == account_name), None)
    if not target:
        return redirect(url_for("follower_positions", notice="계정을 찾을 수 없습니다."))
    try:
        with target.executor.activate():
            if side == "LONG":
                amt = target.executor.get_long_position_amount(symbol)
                if not isinstance(amt, (int, float)) or amt <= 0:
                    return redirect(url_for("follower_positions", notice="해당 포지션 없음"))
                target.executor.close_long_market(symbol)
            else:
                amt = target.executor.get_short_position_amount(symbol)
                if not isinstance(amt, (int, float)) or amt <= 0:
                    return redirect(url_for("follower_positions", notice="해당 포지션 없음"))
                target.executor.close_short_market(symbol)
    except Exception as e:
        return redirect(url_for("follower_positions", notice=f"청산 실패: {str(e)[:80]}"))
    return redirect(url_for("follower_positions", notice=f"{account_name} {symbol} {side} 청산 완료"))


def _coerce_bool(val: object) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(int(val))
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return False


def _state_path_for_account(account: dict) -> str:
    name = str(account.get("name") or "")
    account_id = int(account.get("id"))
    if name == "admin":
        return "state.json"
    return f"state_{account_id}.json"


def _load_account_settings(account_id: int) -> dict:
    settings = accounts_db.get_account_settings(account_id)
    defaults = accounts_db.DEFAULT_SETTINGS
    for key, value in defaults.items():
        if key not in settings or settings.get(key) is None:
            settings[key] = value
    return settings


def _build_follower_positions() -> tuple[list[dict], dict]:
    now = time.time()
    cached = _FOLLOWER_POS_CACHE
    if _FOLLOWER_POS_TTL_SEC > 0 and (now - float(cached.get("ts") or 0.0)) <= _FOLLOWER_POS_TTL_SEC:
        return cached.get("items") or [], cached.get("groups") or {}
    items: list[dict] = []
    groups: dict = {"admin": [], "followers": []}
    try:
        contexts = _build_account_contexts_web(include_inactive=True)
    except Exception:
        contexts = []
    def _rows_from_state(acct_ctx: AccountContext, state: dict) -> list[dict]:
        is_active = bool((acct_ctx.meta.get("db") or {}).get("is_active", 1))
        rows: list[dict] = []
        for tr in _trade_log_entries(state):
            if tr.get("status") != "open":
                continue
            side = tr.get("side")
            if side not in ("LONG", "SHORT"):
                continue
            sym = tr.get("symbol")
            if not sym:
                continue
            qty = tr.get("qty")
            entry = tr.get("entry_price")
            notional = None
            if isinstance(qty, (int, float)) and isinstance(entry, (int, float)):
                notional = float(qty) * float(entry)
            row = {
                "account_id": acct_ctx.account_id,
                "account": acct_ctx.name,
                "is_active": is_active,
                "side": side,
                "symbol": sym,
                "qty": qty,
                "entry": entry,
                "mark": None,
                "notional": notional,
                "pnl": None,
                "roi": None,
                "leverage": tr.get("leverage"),
                "group": "admin" if str(acct_ctx.name) == "admin" else "followers",
            }
            rows.append(row)
        return rows

    for acct in contexts:
        state = None
        try:
            state_path = _state_path_for_account(acct.meta.get("db") or {"id": acct.account_id, "name": acct.name})
            state = load_state_from(state_path)
        except Exception:
            state = {}
        is_active = bool((acct.meta.get("db") or {}).get("is_active", 1))
        try:
            with acct.executor.activate():
                syms = acct.executor.list_open_position_symbols(force=True)
                if not (syms.get("long") or syms.get("short")) and state:
                    for row in _rows_from_state(acct, state):
                        items.append(row)
                        groups[row["group"]].append(row)
                    continue
                for sym in sorted(syms.get("long") or []):
                    detail = acct.executor.get_long_position_detail(sym) or {}
                    row = {
                        "account_id": acct.account_id,
                        "account": acct.name,
                        "is_active": is_active,
                        "side": "LONG",
                        "symbol": sym,
                        "qty": detail.get("qty"),
                        "entry": detail.get("entry"),
                        "mark": detail.get("mark"),
                        "notional": detail.get("notional"),
                        "pnl": detail.get("pnl"),
                        "roi": detail.get("roi"),
                        "leverage": detail.get("leverage"),
                        "group": "admin" if str(acct.name) == "admin" else "followers",
                    }
                    items.append(row)
                    groups[row["group"]].append(row)
                for sym in sorted(syms.get("short") or []):
                    detail = acct.executor.get_short_position_detail(sym) or {}
                    row = {
                        "account_id": acct.account_id,
                        "account": acct.name,
                        "is_active": is_active,
                        "side": "SHORT",
                        "symbol": sym,
                        "qty": detail.get("qty"),
                        "entry": detail.get("entry"),
                        "mark": detail.get("mark"),
                        "notional": detail.get("notional"),
                        "pnl": detail.get("pnl"),
                        "roi": detail.get("roi"),
                        "leverage": detail.get("leverage"),
                        "group": "admin" if str(acct.name) == "admin" else "followers",
                    }
                    items.append(row)
                    groups[row["group"]].append(row)
        except Exception:
            if state:
                for row in _rows_from_state(acct, state):
                    items.append(row)
                    groups[row["group"]].append(row)
            continue
    items.sort(key=lambda x: (x.get("account") or "", x.get("side") or "", x.get("symbol") or ""))
    _FOLLOWER_POS_CACHE["ts"] = now
    _FOLLOWER_POS_CACHE["items"] = items
    _FOLLOWER_POS_CACHE["groups"] = groups
    return items, groups


def _build_executor(account: dict, settings: dict, force_hedge: bool = False) -> AccountExecutor:
    position_mode = "hedge" if force_hedge else os.getenv("POSITION_MODE", "hedge").lower().strip()
    return AccountExecutor(
        api_key=str(account.get("api_key") or ""),
        api_secret=str(account.get("api_secret") or ""),
        dry_run=_coerce_bool(settings.get("dry_run")),
        position_mode=position_mode,
        default_leverage=int(settings.get("leverage") or 10),
        base_entry_usdt=40.0,
        dca_enabled=_coerce_bool(settings.get("dca_enabled")),
        dca_pct=float(settings.get("dca_pct") or 2.0),
        dca_first_pct=float(settings.get("dca1_pct") or 30.0),
        dca_second_pct=float(settings.get("dca2_pct") or 30.0),
        dca_third_pct=float(settings.get("dca3_pct") or 30.0),
    )


def _normalize_symbol(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    if not raw:
        return ""
    if "/" in raw:
        if ":USDT" in raw:
            return raw
        if raw.endswith("/USDT"):
            return f"{raw}:USDT"
        return raw
    if raw.endswith("USDT"):
        base = raw[:-4]
    else:
        base = raw
    return f"{base}/USDT:USDT"


def _entry_usdt_available(executor: AccountExecutor, entry_pct: float) -> float:
    pct = float(entry_pct or 0.0)
    if pct <= 0:
        return 0.0
    if pct > 100.0:
        pct = 100.0
    avail = None
    try:
        avail = executor.get_available_usdt()
    except Exception:
        avail = None
    if not isinstance(avail, (int, float)) or avail <= 0:
        if executor.ctx.dry_run:
            try:
                avail = float(os.getenv("SIM_USDT_BALANCE", "1000"))
            except Exception:
                avail = 1000.0
        else:
            return 0.0
    return max(0.0, float(avail) * (pct / 100.0))


def _open_positions_for_account(executor: AccountExecutor, state: dict) -> list[dict]:
    positions = []
    with executor.activate():
        sym_map = executor.list_open_position_symbols(force=True)
        # Fallback: if positions API returns empty, probe symbols from state.
        if not (sym_map.get("long") or sym_map.get("short")):
            cand_long = set()
            cand_short = set()
            for tr in _trade_log_entries(state):
                if tr.get("status") != "open":
                    continue
                sym = tr.get("symbol")
                side = tr.get("side")
                if not sym or side not in ("LONG", "SHORT"):
                    continue
                if side == "LONG":
                    cand_long.add(sym)
                else:
                    cand_short.add(sym)
            for sym, st in (state or {}).items():
                if not isinstance(st, dict):
                    continue
                if st.get("in_pos_long"):
                    cand_long.add(sym)
                if st.get("in_pos_short"):
                    cand_short.add(sym)
            sym_map = {"long": set(), "short": set()}
            for sym in sorted(cand_long):
                try:
                    if executor.get_long_position_amount(sym) > 0:
                        sym_map["long"].add(sym)
                except Exception:
                    continue
            for sym in sorted(cand_short):
                try:
                    if executor.get_short_position_amount(sym) > 0:
                        sym_map["short"].add(sym)
                except Exception:
                    continue
        for sym in sorted(sym_map.get("long") or []):
            detail = executor.get_long_position_detail(sym) or {}
            engine_label = "UNKNOWN"
            tr = _get_open_trade(state, "LONG", sym)
            if tr:
                engine_label = tr.get("engine_label") or _engine_label_from_reason((tr.get("meta") or {}).get("reason"))
            positions.append(
                {
                    "symbol": sym,
                    "side": "LONG",
                    "entry": detail.get("entry"),
                    "qty": detail.get("qty"),
                    "roi": detail.get("roi"),
                    "engine": engine_label,
                }
            )
        for sym in sorted(sym_map.get("short") or []):
            detail = executor.get_short_position_detail(sym) or {}
            engine_label = "UNKNOWN"
            tr = _get_open_trade(state, "SHORT", sym)
            if tr:
                engine_label = tr.get("engine_label") or _engine_label_from_reason((tr.get("meta") or {}).get("reason"))
            positions.append(
                {
                    "symbol": sym,
                    "side": "SHORT",
                    "entry": detail.get("entry"),
                    "qty": detail.get("qty"),
                    "roi": detail.get("roi"),
                    "engine": engine_label,
                }
            )
    return positions


def _list_accounts() -> list[dict]:
    accounts_db.ensure_default_account("admin")
    return accounts_db.list_active_accounts()

def parse_toggle(cmd: str, arg: str | None) -> tuple[str | None, bool | None]:
    item = COMMANDS_BY_CMD.get(cmd)
    if not item or item.get("type") != "toggle":
        return None, None
    key = item["key"]
    if arg and arg.lower() in ("on", "true", "1"):
        return key, True
    if arg and arg.lower() in ("off", "false", "0"):
        return key, False
    return key, True


def parse_numeric(cmd: str, value: str | None) -> tuple[str | None, float | None]:
    item = COMMANDS_BY_CMD.get(cmd)
    if not item or item.get("type") not in ("number", "int"):
        return None, None
    if value is None or str(value).strip() == "":
        return item["key"], None
    try:
        num = float(value)
    except Exception:
        return item["key"], None
    if item.get("type") == "int":
        num = int(num)
    return item["key"], num


def _format_value(item: dict, value: object) -> str:
    if item.get("type") == "toggle":
        return "ON" if bool(value) else "OFF"
    if item.get("type") == "int":
        return str(int(value))
    if item.get("type") == "json":
        try:
            return json.dumps(value, ensure_ascii=True)
        except Exception:
            return str(value)
    if item.get("type") == "text":
        return str(value or "")
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


def _notify_change(item: dict, value: object) -> None:
    try:
        label = item.get("label") or item.get("cmd")
        msg = f"🌐 <b>WEB 설정 변경</b>\n{label}: <b>{_format_value(item, value)}</b>"
        send_telegram(msg, allow_early=True)
    except Exception:
        pass


def _trade_log_entries(state: dict) -> list[dict]:
    raw = state.get("_trade_log", [])
    if isinstance(raw, dict):
        items = raw.values()
    elif isinstance(raw, list):
        items = raw
    else:
        return []
    return [tr for tr in items if isinstance(tr, dict)]


def _kst_today_start_ts(now_ts: float | None = None) -> float:
    base = float(now_ts if now_ts is not None else time.time())
    kst = base + 9 * 3600
    day_start_kst = int(kst // 86400) * 86400
    # Binance 기준: KST 09:00 ~ 다음날 08:59:59
    return (day_start_kst - 9 * 3600) + 9 * 3600


def _fetch_realized_pnl_since(ex, since_ts: float) -> tuple[float | None, int, str | None]:
    start_ms = int(float(since_ts) * 1000)
    now_ms = int(time.time() * 1000)
    total = 0.0
    count = 0
    income_types = ("REALIZED_PNL", "FUNDING_FEE", "COMMISSION")
    try:
        for income_type in income_types:
            since = start_ms
            while True:
                params = {
                    "incomeType": income_type,
                    "startTime": since,
                    "endTime": now_ms,
                    "limit": 1000,
                }
                if hasattr(ex, "fapiPrivateGetIncome"):
                    batch = ex.fapiPrivateGetIncome(params)
                else:
                    batch = ex.fapiPrivate_get_income(params)
                if not batch:
                    break
                last_ts = None
                for row in batch:
                    if not isinstance(row, dict):
                        continue
                    if row.get("incomeType") != income_type:
                        continue
                    asset = row.get("asset")
                    if asset and str(asset).upper() not in ("USDT",):
                        continue
                    try:
                        val = float(row.get("income"))
                    except Exception:
                        val = None
                    if val is not None:
                        total += val
                        count += 1
                    ts = row.get("time")
                    try:
                        ts_ms = int(float(ts))
                        if last_ts is None or ts_ms > last_ts:
                            last_ts = ts_ms
                    except Exception:
                        pass
                if last_ts is None:
                    break
                since = last_ts + 1
                if last_ts >= now_ms:
                    break
    except Exception as e:
        return None, 0, str(e)[:80]
    return total, count, None


@app.route("/")
def index():
    state = load_state()
    totals = {"LONG": 0, "SHORT": 0}
    for tr in _trade_log_entries(state):
        if tr.get("status") == "open":
            side = tr.get("side")
            if side in totals:
                totals[side] += 1
    return render_template(
        "index.html",
        state=state,
        totals=totals,
        last_report=state.get("_daily_report_date"),
        commands=COMMAND_DEFS,
    )


@app.route("/status")
def status():
    state = load_state()
    positions = sum(1 for tr in _trade_log_entries(state) if tr.get("status") == "open")
    payload = {
        "timestamp": datetime.now().isoformat(),
        "positions": positions,
    }
    for item in COMMAND_DEFS:
        key = item["key"]
        value = state.get(key)
        if item.get("type") == "toggle" and not isinstance(value, bool):
            value = None
        if item.get("type") in ("number", "int") and not isinstance(value, (int, float)):
            value = None
        if item.get("type") == "text" and not isinstance(value, str):
            value = None
        if value is None and key in DEFAULTS:
            value = DEFAULTS[key]
        payload[key] = value
    accounts_payload = []
    pnl_today_payload = []
    pnl_start_ts = _kst_today_start_ts()
    pnl_date_kst = time.strftime("%Y-%m-%d", time.gmtime(pnl_start_ts + 9 * 3600))
    for acct in _list_accounts():
        account_id = int(acct["id"])
        settings = _load_account_settings(account_id)
        state_path = _state_path_for_account(acct)
        acct_state = load_state_from(state_path)
        executor = _build_executor(acct, settings, force_hedge=True)
        futures_usdt = None
        entry_usdt_available = 0.0
        pnl_val = None
        pnl_trades = 0
        pnl_err = None
        try:
            with executor.activate():
                futures_usdt = executor.get_available_usdt()
                entry_usdt_available = _entry_usdt_available(executor, float(settings.get("entry_pct") or 0.0))
                pnl_val, pnl_trades, pnl_err = _fetch_realized_pnl_since(executor.ctx.exchange, pnl_start_ts)
        except Exception:
            futures_usdt = None
        admin_follow_enabled = acct_state.get("_admin_follow_enabled")
        if admin_follow_enabled is None:
            admin_follow_enabled = True
        manual_entry_enabled = acct_state.get("_admin_manual_entry_enabled")
        if manual_entry_enabled is None:
            manual_entry_enabled = True
        accounts_payload.append(
            {
                "account_id": account_id,
                "name": str(acct.get("name") or account_id),
                "open_positions": _open_positions_for_account(executor, acct_state),
                "futures_usdt": futures_usdt,
                "entry_usdt_available": entry_usdt_available,
                "admin_follow_enabled": bool(admin_follow_enabled),
                "manual_entry_enabled": bool(manual_entry_enabled),
            }
        )
        pnl_today_payload.append(
            {
                "account_id": account_id,
                "name": str(acct.get("name") or account_id),
                "pnl": pnl_val,
                "trades": pnl_trades,
                "error": pnl_err,
                "date": pnl_date_kst,
            }
        )
    payload["accounts"] = accounts_payload
    payload["pnl_today"] = pnl_today_payload
    return jsonify(payload)


@app.route("/report/<date>")
def report(date):
    state = load_state()
    try:
        if date in ("today", "yesterday"):
            base = datetime.now()
            if date == "yesterday":
                base = base - timedelta(days=1)
            date = base.strftime("%Y-%m-%d")
        msg = _build_daily_report(state, date, compact=False)
        msg = re.sub(r"<[^>]+>", "", msg)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return app.response_class(msg, mimetype="text/plain")


@app.route("/command", methods=["POST"])
def command():
    payload = request.json or {}
    cmd = payload.get("cmd", "").strip()
    key = payload.get("key")
    value = payload.get("value")
    state = load_state()
    if key:
        item = COMMANDS_BY_KEY.get(key)
        if not item:
            return jsonify({"status": "unknown key", "key": key}), 400
        if item["type"] == "toggle":
            state[key] = bool(value)
        elif item["type"] in ("number", "int"):
            if value is None or str(value).strip() == "":
                return jsonify({"status": "missing value", "key": key}), 400
            try:
                num = float(value)
            except Exception:
                return jsonify({"status": "invalid number", "key": key}), 400
            if item["type"] == "int":
                num = int(num)
            state[key] = num
        elif item["type"] == "json":
            if value is None or (isinstance(value, str) and not value.strip()):
                return jsonify({"status": "missing value", "key": key}), 400
            if isinstance(value, dict):
                state[key] = value
            else:
                try:
                    parsed = json.loads(value)
                except Exception:
                    return jsonify({"status": "invalid json", "key": key}), 400
                if not isinstance(parsed, dict):
                    return jsonify({"status": "json must be object", "key": key}), 400
                state[key] = parsed
        elif item["type"] == "text":
            if value is None:
                state[key] = ""
            else:
                state[key] = str(value).strip()
        state["_runtime_cfg_ts"] = time.time()
        save_state(state)
        _sync_admin_settings_from_state(state)
        if key == "_max_open_positions":
            try:
                admin_id = accounts_db.get_account_id_by_name("admin")
                if admin_id:
                    accounts_db.update_account_setting(admin_id, "max_positions", int(state.get(key)))
            except Exception:
                pass
        if key == "_entry_usdt":
            try:
                admin_id = accounts_db.get_account_id_by_name("admin")
                if admin_id:
                    accounts_db.update_account_setting(admin_id, "entry_pct", float(state.get(key)))
            except Exception:
                pass
        if key == "_live_trading":
            try:
                admin_id = accounts_db.get_account_id_by_name("admin")
                if admin_id:
                    accounts_db.update_account_setting(admin_id, "dry_run", 0 if state.get(key) else 1)
            except Exception:
                pass
        _notify_change(item, state.get(key))
        return jsonify({"status": "ok", "key": key, "value": state.get(key)})
    if not cmd:
        return jsonify({"status": "missing cmd"}), 400
    parts = cmd.split()
    verb = parts[0].lower() if parts else ""
    arg = parts[1] if len(parts) > 1 else None
    key, value = parse_toggle(verb, arg)
    if key:
        state[key] = bool(value)
        state["_runtime_cfg_ts"] = time.time()
        save_state(state)
        _sync_admin_settings_from_state(state)
        item = COMMANDS_BY_CMD.get(verb) or {"label": verb, "type": "toggle"}
        _notify_change(item, state.get(key))
        return jsonify({"status": "ok", "cmd": cmd})
    num_key, num_val = parse_numeric(verb, arg)
    if num_key and num_val is not None:
        state[num_key] = num_val
        state["_runtime_cfg_ts"] = time.time()
        save_state(state)
        _sync_admin_settings_from_state(state)
        if num_key == "_max_open_positions":
            try:
                admin_id = accounts_db.get_account_id_by_name("admin")
                if admin_id:
                    accounts_db.update_account_setting(admin_id, "max_positions", int(state.get(num_key)))
            except Exception:
                pass
        if num_key == "_entry_usdt":
            try:
                admin_id = accounts_db.get_account_id_by_name("admin")
                if admin_id:
                    accounts_db.update_account_setting(admin_id, "entry_pct", float(state.get(num_key)))
            except Exception:
                pass
        item = COMMANDS_BY_CMD.get(verb) or {"label": verb, "type": "number"}
        _notify_change(item, state.get(num_key))
        return jsonify({"status": "ok", "cmd": cmd})
    return jsonify({"status": "unknown command", "cmd": cmd}), 400


@app.route("/admin/accounts", methods=["GET", "POST"])
def admin_accounts():
    if request.method == "GET":
        items = []
        for acct in _list_accounts():
            account_id = int(acct["id"])
            state_path = _state_path_for_account(acct)
            acct_state = load_state_from(state_path)
            admin_follow_enabled = acct_state.get("_admin_follow_enabled")
            if admin_follow_enabled is None:
                admin_follow_enabled = True
            manual_entry_enabled = acct_state.get("_admin_manual_entry_enabled")
            if manual_entry_enabled is None:
                manual_entry_enabled = True
            items.append(
                {
                    "account_id": account_id,
                    "name": str(acct.get("name") or account_id),
                    "is_active": bool(acct.get("is_active", 1)),
                    "admin_follow_enabled": bool(admin_follow_enabled),
                    "manual_entry_enabled": bool(manual_entry_enabled),
                }
            )
        return jsonify({"accounts": items})
    payload = request.json or {}
    account_id = payload.get("account_id")
    if account_id is None:
        return jsonify({"status": "missing account_id"}), 400
    account_id = int(account_id)
    accounts = {int(a["id"]): a for a in _list_accounts()}
    acct = accounts.get(account_id)
    if not acct:
        return jsonify({"status": "unknown account", "account_id": account_id}), 404
    state_path = _state_path_for_account(acct)
    acct_state = load_state_from(state_path)
    if "admin_follow_enabled" in payload:
        acct_state["_admin_follow_enabled"] = bool(payload.get("admin_follow_enabled"))
    if "manual_entry_enabled" in payload:
        acct_state["_admin_manual_entry_enabled"] = bool(payload.get("manual_entry_enabled"))
    acct_state["_runtime_cfg_ts"] = time.time()
    save_state_to(acct_state, state_path)
    return jsonify({"status": "ok", "account_id": account_id})


@app.route("/admin/entry", methods=["POST"])
def admin_entry():
    payload = request.json or {}
    symbol = _normalize_symbol(payload.get("symbol"))
    side = str(payload.get("side") or "").strip().upper()
    account_ids = payload.get("account_ids") or []
    if not symbol:
        return jsonify({"status": "missing symbol"}), 400
    if side not in ("LONG", "SHORT"):
        return jsonify({"status": "invalid side"}), 400
    if not isinstance(account_ids, list) or not account_ids:
        return jsonify({"status": "missing account_ids"}), 400
    accounts = {int(a["id"]): a for a in _list_accounts()}
    now_ts = time.time()
    def _entry_task(account_id: int) -> dict:
        acct = accounts.get(account_id)
        if not acct:
            return {"account_id": account_id, "status": "unknown_account"}
        state_path = _state_path_for_account(acct)
        acct_state = load_state_from(state_path)
        admin_follow_enabled = acct_state.get("_admin_follow_enabled")
        if admin_follow_enabled is None:
            admin_follow_enabled = True
        manual_entry_enabled = acct_state.get("_admin_manual_entry_enabled")
        if manual_entry_enabled is None:
            manual_entry_enabled = True
        if not admin_follow_enabled:
            return {"account_id": account_id, "status": "skip", "reason": "admin_follow_disabled"}
        if not manual_entry_enabled:
            return {"account_id": account_id, "status": "skip", "reason": "manual_entry_disabled"}
        settings = _load_account_settings(account_id)
        executor = _build_executor(acct, settings, force_hedge=True)
        usdt_amount = _entry_usdt_available(executor, float(settings.get("entry_pct") or 0.0))
        if usdt_amount <= 0:
            return {"account_id": account_id, "status": "skip", "reason": "entry_usdt_unavailable"}
        leverage = int(settings.get("leverage") or 10)
        margin_mode = str(settings.get("margin_mode") or "cross")
        with executor.activate():
            if side == "LONG":
                res = executor.long_market(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)
            else:
                res = executor.short_market(symbol, usdt_amount=usdt_amount, leverage=leverage, margin_mode=margin_mode)
        status = res.get("status")
        if status in ("ok", "dry_run"):
            entry_price = res.get("last") or res.get("price")
            qty = res.get("amount")
            order_id = res.get("order_id")
            meta = {"reason": "manual_admin", "source": "admin_web"}
            _log_trade_entry(
                acct_state,
                side,
                symbol,
                entry_ts=now_ts,
                entry_price=entry_price,
                qty=qty,
                usdt=usdt_amount,
                entry_order_id=order_id,
                meta=meta,
            )
            save_state_to(acct_state, state_path)
        return {"account_id": account_id, "status": status, "result": res}

    tasks = []
    for acct_id in account_ids:
        try:
            account_id = int(acct_id)
        except Exception:
            tasks.append((lambda acct_id=acct_id: {"account_id": acct_id, "status": "invalid_account_id"}, {"account_id": acct_id}))
            continue
        tasks.append((lambda account_id=account_id: _entry_task(account_id), {"account_id": account_id}))
    results = _run_parallel_tasks(tasks)
    return jsonify({"status": "ok", "symbol": symbol, "side": side, "results": results})


@app.route("/commands")
def commands():
    return jsonify({"commands": COMMAND_DEFS})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("WEB_PORT", 5000)), debug=False)
