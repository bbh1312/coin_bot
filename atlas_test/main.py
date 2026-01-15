import os
import time
from datetime import datetime, timedelta, timezone
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from env_loader import load_env

from atlas_test.atlas_config import Config
from atlas_test.atlas_engine import evaluate_atlas, set_cache, compute_global_gate, get_ref_symbol
from engines.atlas.atlas_engine import AtlasSwaggyConfig
from atlas_test.data_feed import init_exchange, wait_with_backoff, fetch_ohlcv
from atlas_test.notifier_telegram import send_message
from atlas_test.state_store import load_state, save_state
from engines.universe import build_universe_from_tickers


def _fmt_kst(ts_ms: int) -> str:
    kst = timezone(timedelta(hours=9))
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=kst)
    return dt.strftime("%Y-%m-%d %H:%M KST")


def _fmt_now_kst() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S KST")


def _fmt_val(v: object, fmt: str = "{:.4f}") -> str:
    if isinstance(v, (int, float)):
        return fmt.format(v)
    return "n/a"


def _build_instant_message(symbol: str, result: dict, ts_ms: int, instant_min: int) -> str:
    state = result["state"]
    score = result["score"]
    atlas_gate = result.get("atlas_gate") or {}
    atlas_local = result.get("atlas_local") or {}
    regime = atlas_gate.get("regime")
    rs = atlas_local.get("rs")
    rs_z = atlas_local.get("rs_z")
    corr = atlas_local.get("corr")
    beta = atlas_local.get("beta")
    vol_ratio = atlas_local.get("vol_ratio")
    time_str = _fmt_kst(ts_ms) if ts_ms else "N/A"
    action = "LONG ONLY" if state == "STRONG_BULL" else "SHORT ONLY"
    return (
        "[ATLAS-TEST] 신규 STRONG ✅ (15m 마감 기준)\n"
        f"심볼: {symbol}\n"
        f"상태: {state} → {action}\n"
        f"점수: {score} (기준: {instant_min}+)\n"
        f"근거: regime={regime}, rs={_fmt_val(rs)}, rs_z={_fmt_val(rs_z)}, corr={_fmt_val(corr)}, beta={_fmt_val(beta)}, vol={_fmt_val(vol_ratio, '{:.2f}')}\n"
        f"시간: {time_str}"
    )


def _build_summary_message(cfg: Config, summary_ts: int, results: dict, universe_label: str) -> str:
    time_str = _fmt_kst(summary_ts)
    bulls = results.get("bulls", [])
    bears = results.get("bears", [])
    exits = results.get("exits", [])
    counts = results.get("counts", {"STRONG_BULL": 0, "STRONG_BEAR": 0, "NO_TRADE": 0})
    total = sum(int(v or 0) for v in counts.values())
    lines = [
        f"[ATLAS-TEST] 15m 요약 (KST {time_str})",
        f"Universe: {universe_label}",
        f"Universe size: {total}",
        f"STRONG_BULL: {counts.get('STRONG_BULL', 0)} | STRONG_BEAR: {counts.get('STRONG_BEAR', 0)} | NO_TRADE: {counts.get('NO_TRADE', 0)}",
        "",
        "TOP BULL",
    ]
    if bulls:
        for idx, item in enumerate(bulls, start=1):
            lines.append(
                f"{idx}) {item['symbol']} score={item['score']} rs={_fmt_val(item['rs'])} corr={_fmt_val(item['corr'])} beta={_fmt_val(item['beta'])} vol={_fmt_val(item['vol_ratio'], '{:.2f}')}"
            )
    else:
        lines.append("none")
    lines.append("")
    lines.append("TOP BEAR")
    if bears:
        for idx, item in enumerate(bears, start=1):
            lines.append(
                f"{idx}) {item['symbol']} score={item['score']} rs={_fmt_val(item['rs'])} corr={_fmt_val(item['corr'])} beta={_fmt_val(item['beta'])} vol={_fmt_val(item['vol_ratio'], '{:.2f}')}"
            )
    else:
        lines.append("none")
    lines.append("")
    lines.append("EXIT (STRONG→NO)")
    if exits:
        for idx, item in enumerate(exits, start=1):
            lines.append(f"{idx}) {item['symbol']} from={item['from_state']}")
    else:
        lines.append("none")
    return "\n".join(lines)


def _build_common_universe(cfg: Config, tickers: dict) -> tuple:
    anchors = ("BTC/USDT:USDT", "ETH/USDT:USDT")
    universe = build_universe_from_tickers(
        tickers,
        min_quote_volume_usdt=float(cfg.min_quote_volume or 0.0),
        top_n=int(cfg.fabio_universe_top_n or 0) or None,
        anchors=anchors,
    )
    label = (
        f"qVol>={int(cfg.min_quote_volume):,} abs(pct) top {cfg.fabio_universe_top_n} "
        "anchors(BTC/ETH)"
    )
    return universe, label


def _score_breakdown(result: dict) -> dict:
    atlas_gate = result.get("atlas_gate") or {}
    atlas_local = result.get("atlas_local") or {}
    regime = atlas_gate.get("regime")
    score_regime = 25 if regime in ("bull", "bear") else (15 if regime == "chaos_vol" else 5)
    cfg = AtlasSwaggyConfig()

    rs = atlas_local.get("rs")
    rs_z = atlas_local.get("rs_z")
    rs_slow = atlas_local.get("rs_slow")
    rs_z_slow = atlas_local.get("rs_z_slow")
    rs_pass = (
        (isinstance(rs, (int, float)) and rs >= cfg.rs_pass)
        or (isinstance(rs_z, (int, float)) and rs_z >= cfg.rs_z_pass)
        or (isinstance(rs_slow, (int, float)) and rs_slow >= cfg.rs_pass)
        or (isinstance(rs_z_slow, (int, float)) and rs_z_slow >= cfg.rs_z_pass)
    )

    corr = atlas_local.get("corr")
    beta = atlas_local.get("beta")
    corr_slow = atlas_local.get("corr_slow")
    beta_slow = atlas_local.get("beta_slow")
    indep_pass = (
        (isinstance(corr, (int, float)) and corr <= cfg.indep_corr)
        or (isinstance(beta, (int, float)) and beta <= cfg.indep_beta)
        or (isinstance(corr_slow, (int, float)) and corr_slow <= cfg.indep_corr)
        or (isinstance(beta_slow, (int, float)) and beta_slow <= cfg.indep_beta)
    )

    vol_ratio = atlas_local.get("vol_ratio")
    vol_pass = isinstance(vol_ratio, (int, float)) and vol_ratio >= cfg.vol_pass

    return {
        "regime": regime,
        "score_regime": score_regime,
        "score_rs": 25 if rs_pass else 0,
        "score_indep": 25 if indep_pass else 0,
        "score_vol": 25 if vol_pass else 0,
        "rs": rs,
        "rs_z": rs_z,
        "rs_slow": rs_slow,
        "rs_z_slow": rs_z_slow,
        "corr": corr,
        "beta": beta,
        "corr_slow": corr_slow,
        "beta_slow": beta_slow,
        "vol_ratio": vol_ratio,
    }


def _append_detail_log(line: str) -> None:
    try:
        log_dir = os.path.join(ROOT_DIR, "logs", "atlas", "atlastest")
        os.makedirs(log_dir, exist_ok=True)
        kst = timezone(timedelta(hours=9))
        date_tag = datetime.now(tz=kst).strftime("%Y-%m-%d")
        filename = f"atlas_test_detail-{date_tag}.log"
        with open(os.path.join(log_dir, filename), "a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
    except Exception:
        pass


def main() -> None:
    load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN_ATLAS", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_ATLAS", "").strip()
    cfg = Config()
    if not token or not chat_id:
        print("[atlas-test] missing TELEGRAM_BOT_TOKEN_ATLAS or TELEGRAM_CHAT_ID_ATLAS")
        return
    exchange = init_exchange()
    exchange.load_markets()
    symbols_all = [s for s in exchange.symbols if s.endswith("USDT:USDT")]
    state = load_state(cfg.state_file)
    print("[atlas-test] start (fabio universe)")

    while True:
        try:
            tickers = exchange.fetch_tickers(symbols_all)
        except Exception as e:
            print(f"[atlas-test] tickers error: {e}")
            wait_with_backoff(cfg.poll_sec)
            continue

        universe, universe_label = _build_common_universe(cfg, tickers)
        if not universe:
            wait_with_backoff(cfg.poll_sec)
            continue

        ref_symbol = get_ref_symbol()
        if ref_symbol not in symbols_all:
            ref_symbol = "BTC/USDT:USDT"
        ref_ohlcv = fetch_ohlcv(exchange, ref_symbol, cfg.ltf_tf, cfg.ltf_limit, drop_last=True)
        if len(ref_ohlcv) < 120:
            print("[atlas-test] warmup (insufficient candles)")
            wait_with_backoff(cfg.poll_sec)
            continue
        set_cache(ref_symbol, cfg.ltf_tf, ref_ohlcv)
        atlas_gate = compute_global_gate()
        summary_ts = int(ref_ohlcv[-1]["ts"] or 0)
        if summary_ts <= 0:
            wait_with_backoff(cfg.poll_sec)
            continue

        last_summary = int(state.get("global", {}).get("last_summary_ts") or 0)
        if summary_ts <= last_summary:
            wait_with_backoff(cfg.poll_sec)
            continue

        counts = {"STRONG_BULL": 0, "STRONG_BEAR": 0, "NO_TRADE": 0}
        bulls = []
        bears = []
        exits = []
        symbols_state = state.setdefault("symbols", {})
        now_ts = int(summary_ts)

        for idx, sym in enumerate(universe, start=1):
            sym_cfg = Config()
            sym_cfg.symbol = sym
            sym_cfg.ltf_tf = cfg.ltf_tf
            sym_cfg.ltf_limit = cfg.ltf_limit
            result = evaluate_atlas(exchange, sym_cfg, atlas_gate)
            if result.get("status") != "ok":
                counts["NO_TRADE"] += 1
                continue
            state_now = result.get("state") or "NO_TRADE"
            counts[state_now] = counts.get(state_now, 0) + 1
            atlas_local = result.get("atlas_local") or {}
            breakdown = _score_breakdown(result)
            sym_state = symbols_state.get(sym, {"last_state": "NO_TRADE", "last_alert_state": "NO_TRADE"})
            last_detail_score = sym_state.get("last_detail_score")
            last_detail_ts = sym_state.get("last_detail_ts")
            allow_detail = True
            if last_detail_score == (result.get("score") or 0) and isinstance(last_detail_ts, (int, float)):
                if now_ts - int(last_detail_ts) < 3600:
                    allow_detail = False
            detail_line = (
                "[{ts}] [atlas-test] idx={idx} sym={sym} state={state} score={score} "
                "regime={regime} dir={direction} part=R{sr}/RS{srsi}/I{sind}/V{svol} "
                "rs={rs} rs_z={rsz} rs_slow={rsl} rs_z_slow={rszl} "
                "corr={corr} beta={beta} corr_slow={corrs} beta_slow={betas} "
                "vol={vol}"
            ).format(
                ts=_fmt_now_kst(),
                idx=idx,
                sym=sym.replace("/USDT:USDT", ""),
                state=state_now,
                score=int(result.get("score") or 0),
                regime=breakdown.get("regime"),
                direction=atlas_local.get("symbol_direction"),
                sr=breakdown.get("score_regime"),
                srsi=breakdown.get("score_rs"),
                sind=breakdown.get("score_indep"),
                svol=breakdown.get("score_vol"),
                rs=_fmt_val(breakdown.get("rs")),
                rsz=_fmt_val(breakdown.get("rs_z")),
                rsl=_fmt_val(breakdown.get("rs_slow")),
                rszl=_fmt_val(breakdown.get("rs_z_slow")),
                corr=_fmt_val(breakdown.get("corr")),
                beta=_fmt_val(breakdown.get("beta")),
                corrs=_fmt_val(breakdown.get("corr_slow")),
                betas=_fmt_val(breakdown.get("beta_slow")),
                vol=_fmt_val(breakdown.get("vol_ratio"), "{:.2f}"),
            )
            if allow_detail:
                print(detail_line)
                _append_detail_log(detail_line)
                sym_state["last_detail_score"] = result.get("score") or 0
                sym_state["last_detail_ts"] = now_ts
            entry = {
                "symbol": sym.replace("/USDT:USDT", ""),
                "score": result.get("score") or 0,
                "rs": atlas_local.get("rs"),
                "corr": atlas_local.get("corr"),
                "beta": atlas_local.get("beta"),
                "vol_ratio": atlas_local.get("vol_ratio"),
            }
            if state_now == "STRONG_BULL":
                bulls.append(entry)
            elif state_now == "STRONG_BEAR":
                bears.append(entry)

            last_state = sym_state.get("last_state", "NO_TRADE")
            last_alert = sym_state.get("last_alert_state", "NO_TRADE")
            if state_now != last_state:
                sym_state["last_state"] = state_now
            if last_state in ("STRONG_BULL", "STRONG_BEAR") and state_now == "NO_TRADE":
                exits.append({"symbol": sym.replace("/USDT:USDT", ""), "from_state": last_state})
            if last_state == "NO_TRADE" and state_now in ("STRONG_BULL", "STRONG_BEAR"):
                if result.get("score", 0) >= cfg.instant_score_min and state_now != last_alert:
                    msg = _build_instant_message(
                        sym.replace("/USDT:USDT", ""),
                        result,
                        summary_ts,
                        cfg.instant_score_min,
                    )
                    ok = send_message(token, chat_id, msg)
                    print(f"[atlas-test] instant {sym} {state_now} sent={ok}")
                    sym_state["last_alert_state"] = state_now
            symbols_state[sym] = sym_state

        bulls.sort(key=lambda x: x["score"], reverse=True)
        bears.sort(key=lambda x: x["score"], reverse=True)
        summary = {
            "counts": counts,
            "bulls": bulls[: cfg.summary_top_n],
            "bears": bears[: cfg.summary_top_n],
            "exits": exits[: cfg.summary_top_n],
        }
        msg = _build_summary_message(cfg, summary_ts, summary, universe_label)
        print(msg)
        ok = False
        for attempt in range(1, 4):
            ok = send_message(token, chat_id, msg)
            if ok:
                break
            print(f"[atlas-test] summary send failed attempt={attempt}")
            time.sleep(0.5)
        print(f"[atlas-test] summary sent={ok} symbols={len(universe)}")
        state["global"]["last_summary_ts"] = summary_ts
        state["symbols"] = symbols_state
        save_state(cfg.state_file, state)
        wait_with_backoff(cfg.poll_sec)


if __name__ == "__main__":
    main()
