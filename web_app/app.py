from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, redirect, render_template, request, url_for
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engine_runner import _build_daily_report, load_state

WEB_COMMANDS = {
    "/fabio": "_fabio_enabled",
    "/atlasfabio": "_atlas_fabio_enabled",
    "/swaggy": "_swaggy_enabled",
    "/auto_exit": "_auto_exit",
}

app = Flask(__name__, template_folder="templates")


def save_state(state: dict) -> None:
    with open("state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def parse_toggle(cmd: str, arg: str | None) -> tuple[str, bool]:
    key = WEB_COMMANDS.get(cmd)
    if arg and arg.lower() in ("on", "true", "1"):
        return key, True
    if arg and arg.lower() in ("off", "false", "0"):
        return key, False
    return key, True


@app.route("/")
def index():
    state = load_state()
    totals = {"LONG": 0, "SHORT": 0}
    for tr in state.get("_trade_log", []):
        if tr.get("status") == "open":
            side = tr.get("side")
            if side in totals:
                totals[side] += 1
    cmd_help = list(WEB_COMMANDS.keys())
    return render_template(
        "index.html",
        state=state,
        totals=totals,
        last_report=state.get("_daily_report_date"),
        cmd_help=cmd_help,
    )


@app.route("/status")
def status():
    state = load_state()
    positions = sum(1 for tr in state.get("_trade_log", []) if tr.get("status") == "open")
    return jsonify(
        {
            "timestamp": datetime.now().isoformat(),
            "positions": positions,
            "fabio_enabled": bool(state.get("_fabio_enabled")),
            "atlasfabio_enabled": bool(state.get("_atlas_fabio_enabled")),
            "swaggy_enabled": bool(state.get("_swaggy_enabled")),
            "auto_exit": bool(state.get("_auto_exit")),
        }
    )


@app.route("/report/<date>")
def report(date):
    state = load_state()
    try:
        msg = _build_daily_report(state, date)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return app.response_class(msg, mimetype="text/plain")


@app.route("/command", methods=["POST"])
def command():
    payload = request.json or {}
    cmd = payload.get("cmd", "").strip()
    parts = cmd.split()
    verb = parts[0].lower() if parts else ""
    arg = parts[1] if len(parts) > 1 else None
    state = load_state()
    key, value = parse_toggle(verb, arg)
    if key:
        state[key] = bool(value)
        save_state(state)
        return jsonify({"status": "ok", "cmd": cmd})
    return jsonify({"status": "unknown command", "cmd": cmd}), 400


@app.route("/commands")
def commands():
    return jsonify({"commands": list(WEB_COMMANDS.keys())})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("WEB_PORT", 5000)), debug=False)
