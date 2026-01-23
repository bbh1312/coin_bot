from __future__ import annotations

from typing import Dict, List


def compute_metrics(trades: List[Dict], tail_loss_pct: float = -0.03) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not trades:
        return {
            "trades": 0,
            "winrate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "avg_duration": 0.0,
            "tail_loss_count": 0,
        }
    pnl_pcts = [float(t.get("pnl_pct") or 0.0) for t in trades]
    wins = [p for p in pnl_pcts if p > 0]
    losses = [p for p in pnl_pcts if p <= 0]
    out["trades"] = len(pnl_pcts)
    out["winrate"] = len(wins) / len(pnl_pcts) if pnl_pcts else 0.0
    out["avg_win"] = sum(wins) / len(wins) if wins else 0.0
    out["avg_loss"] = sum(losses) / len(losses) if losses else 0.0
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    out["profit_factor"] = (gross_win / gross_loss) if gross_loss > 0 else 0.0
    out["expectancy"] = sum(pnl_pcts) / len(pnl_pcts) if pnl_pcts else 0.0
    durations = [int(t.get("duration_bars") or 0) for t in trades]
    out["avg_duration"] = sum(durations) / len(durations) if durations else 0.0
    out["tail_loss_count"] = len([p for p in pnl_pcts if p <= tail_loss_pct])
    # drawdown
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        equity += float(t.get("pnl_usdt") or 0.0)
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
    out["max_drawdown"] = max_dd
    return out
