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
    load_state,
    save_state,
    LIVE_TRADING,
    LONG_LIVE_TRADING,
    AUTO_EXIT_ENABLED,
    MAX_OPEN_POSITIONS,
    USDT_PER_TRADE,
    ATLAS_FABIO_ENABLED,
    SWAGGY_ATLAS_LAB_ENABLED,
    SWAGGY_NO_ATLAS_ENABLED,
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
    ENGINE_EXIT_OVERRIDES,
)

COMMAND_DEFS = [
    {"cmd": "/live", "key": "_live_trading", "label": "Live Shorts", "type": "toggle"},
    {"cmd": "/long_live", "key": "_long_live", "label": "Live Longs", "type": "toggle"},
    {"cmd": "/auto_exit", "key": "_auto_exit", "label": "Auto Exit", "type": "toggle"},
    {"cmd": "/atlasfabio", "key": "_atlas_fabio_enabled", "label": "AtlasFabio", "type": "toggle"},
    {"cmd": "/swaggy_atlas_lab", "key": "_swaggy_atlas_lab_enabled", "label": "Swaggy Atlas Lab", "type": "toggle"},
    {"cmd": "/swaggy_no_atlas", "key": "_swaggy_no_atlas_enabled", "label": "Swaggy No Atlas", "type": "toggle"},
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
    {"cmd": "/engine_exit", "key": "_engine_exit_overrides", "label": "엔진 TP/SL JSON", "type": "json"},
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
    "_swaggy_no_atlas_enabled": SWAGGY_NO_ATLAS_ENABLED,
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
    "_engine_exit_overrides": ENGINE_EXIT_OVERRIDES,
}


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
        if item.get("type") == "toggle" and not isinstance(value, bool):
            value = None
        if item.get("type") in ("number", "int") and not isinstance(value, (int, float)):
            value = None
        if value is None and key in DEFAULTS:
            value = DEFAULTS[key]
        payload[key] = value
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


@app.route("/commands")
def commands():
    return jsonify({"commands": COMMAND_DEFS})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("WEB_PORT", 5000)), debug=False)
