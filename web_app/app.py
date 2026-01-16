from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
import time
import re

from flask import Flask, jsonify, redirect, render_template, request, url_for
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engine_runner import (
    _build_daily_report,
    _engine_label_from_reason,
    _get_open_trade,
    _log_trade_entry,
    load_state,
    load_state_from,
    save_state,
    save_state_to,
    LIVE_TRADING,
    LONG_LIVE_TRADING,
    AUTO_EXIT_ENABLED,
    MAX_OPEN_POSITIONS,
    USDT_PER_TRADE,
    ATLAS_FABIO_ENABLED,
    SWAGGY_ATLAS_LAB_ENABLED,
    DTFX_ENABLED,
    PUMPFADE_ENABLED,
    DIV15M_LONG_ENABLED,
    DIV15M_SHORT_ENABLED,
    ATLAS_RS_FAIL_SHORT_ENABLED,
    RSI_ENABLED,
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
)
import accounts_db
from executor import AccountExecutor

COMMAND_DEFS = [
    {"cmd": "/live", "key": "_live_trading", "label": "Live Shorts", "type": "toggle"},
    {"cmd": "/long_live", "key": "_long_live", "label": "Live Longs", "type": "toggle"},
    {"cmd": "/auto_exit", "key": "_auto_exit", "label": "Auto Exit", "type": "toggle"},
    {"cmd": "/atlasfabio", "key": "_atlas_fabio_enabled", "label": "AtlasFabio", "type": "toggle"},
    {"cmd": "/swaggy_atlas_lab", "key": "_swaggy_atlas_lab_enabled", "label": "Swaggy Atlas Lab", "type": "toggle"},
    {"cmd": "/dtfx", "key": "_dtfx_enabled", "label": "DTFX", "type": "toggle"},
    {"cmd": "/pumpfade", "key": "_pumpfade_enabled", "label": "PumpFade", "type": "toggle"},
    {"cmd": "/atlas_rs_fail_short", "key": "_atlas_rs_fail_short_enabled", "label": "Atlas RS Fail Short", "type": "toggle"},
    {"cmd": "/div15m_long", "key": "_div15m_long_enabled", "label": "Div15m Long", "type": "toggle"},
    {"cmd": "/div15m_short", "key": "_div15m_short_enabled", "label": "Div15m Short", "type": "toggle"},
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
    {"cmd": "/max_pos", "key": "_max_open_positions", "label": "최대 포지션 수", "type": "int", "step": 1},
]
COMMANDS_BY_CMD = {d["cmd"]: d for d in COMMAND_DEFS}
COMMANDS_BY_KEY = {d["key"]: d for d in COMMAND_DEFS}

app = Flask(__name__, template_folder="templates")
DEFAULTS = {
    "_live_trading": LIVE_TRADING,
    "_long_live": LONG_LIVE_TRADING,
    "_auto_exit": AUTO_EXIT_ENABLED,
    "_max_open_positions": MAX_OPEN_POSITIONS,
    "_entry_usdt": USDT_PER_TRADE,
    "_atlas_fabio_enabled": ATLAS_FABIO_ENABLED,
    "_swaggy_atlas_lab_enabled": SWAGGY_ATLAS_LAB_ENABLED,
    "_dtfx_enabled": DTFX_ENABLED,
    "_pumpfade_enabled": PUMPFADE_ENABLED,
    "_div15m_long_enabled": DIV15M_LONG_ENABLED,
    "_div15m_short_enabled": DIV15M_SHORT_ENABLED,
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
}


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


@app.route("/")
def index():
    state = load_state()
    totals = {"LONG": 0, "SHORT": 0}
    for tr in state.get("_trade_log", []):
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
    positions = sum(1 for tr in state.get("_trade_log", []) if tr.get("status") == "open")
    payload = {
        "timestamp": datetime.now().isoformat(),
        "positions": positions,
    }
    for item in COMMAND_DEFS:
        key = item["key"]
        value = state.get(key)
        if value is None and key in DEFAULTS:
            value = DEFAULTS[key]
        payload[key] = value
    accounts_payload = []
    for acct in _list_accounts():
        account_id = int(acct["id"])
        settings = _load_account_settings(account_id)
        state_path = _state_path_for_account(acct)
        acct_state = load_state_from(state_path)
        executor = _build_executor(acct, settings, force_hedge=True)
        futures_usdt = None
        entry_usdt_available = 0.0
        try:
            with executor.activate():
                futures_usdt = executor.get_available_usdt()
                entry_usdt_available = _entry_usdt_available(executor, float(settings.get("entry_pct") or 0.0))
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
    payload["accounts"] = accounts_payload
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
        state["_runtime_cfg_ts"] = time.time()
        save_state(state)
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
        item = COMMANDS_BY_CMD.get(verb) or {"label": verb, "type": "toggle"}
        _notify_change(item, state.get(key))
        return jsonify({"status": "ok", "cmd": cmd})
    num_key, num_val = parse_numeric(verb, arg)
    if num_key and num_val is not None:
        state[num_key] = num_val
        state["_runtime_cfg_ts"] = time.time()
        save_state(state)
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
    results = []
    now_ts = time.time()
    for acct_id in account_ids:
        try:
            account_id = int(acct_id)
        except Exception:
            results.append({"account_id": acct_id, "status": "invalid_account_id"})
            continue
        acct = accounts.get(account_id)
        if not acct:
            results.append({"account_id": account_id, "status": "unknown_account"})
            continue
        state_path = _state_path_for_account(acct)
        acct_state = load_state_from(state_path)
        admin_follow_enabled = acct_state.get("_admin_follow_enabled")
        if admin_follow_enabled is None:
            admin_follow_enabled = True
        manual_entry_enabled = acct_state.get("_admin_manual_entry_enabled")
        if manual_entry_enabled is None:
            manual_entry_enabled = True
        if not admin_follow_enabled:
            results.append({"account_id": account_id, "status": "skip", "reason": "admin_follow_disabled"})
            continue
        if not manual_entry_enabled:
            results.append({"account_id": account_id, "status": "skip", "reason": "manual_entry_disabled"})
            continue
        settings = _load_account_settings(account_id)
        executor = _build_executor(acct, settings, force_hedge=True)
        usdt_amount = _entry_usdt_available(executor, float(settings.get("entry_pct") or 0.0))
        if usdt_amount <= 0:
            results.append({"account_id": account_id, "status": "skip", "reason": "entry_usdt_unavailable"})
            continue
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
        results.append({"account_id": account_id, "status": status, "result": res})
    return jsonify({"status": "ok", "symbol": symbol, "side": side, "results": results})


@app.route("/commands")
def commands():
    return jsonify({"commands": COMMAND_DEFS})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("WEB_PORT", 5000)), debug=False)
