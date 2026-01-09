import pandas as pd

from engines.swaggy.levels import Level
from engines.swaggy.triggers import (
    detect_breakout_retest,
    detect_reclaim,
    detect_rejection_wick,
    detect_sweep_and_return,
)


def _df(rows):
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def test_reclaim_long():
    df = _df([
        [1, 10, 10.5, 9.5, 9.8, 10],
        [2, 9.7, 10.0, 9.6, 9.9, 12],
        [3, 10.0, 10.4, 9.9, 10.15, 11],
        [4, 10.1, 10.5, 10.0, 10.2, 11],
    ])
    level = Level(price=10.0, kind="TEST")
    trig = detect_reclaim(df, level, touch_eps=0.0001)
    assert trig is not None
    assert trig.side == "long"


def test_rejection_wick_short():
    df = _df([
        [1, 10, 10.5, 9.5, 9.9, 10],
        [2, 9.9, 10.4, 9.8, 9.95, 50],
        [3, 10.0, 10.8, 9.9, 10.2, 60],
    ])
    level = Level(price=10.6, kind="TEST")
    trig = detect_rejection_wick(df, level, wick_ratio=0.4, vol_mult=1.0)
    assert trig is not None
    assert trig.side == "short"


def test_sweep_and_return_long():
    df = _df([
        [1, 10, 10.1, 9.7, 9.9, 10],
        [2, 9.9, 10.0, 9.4, 10.05, 12],
        [3, 10.0, 10.2, 9.6, 10.1, 11],
    ])
    level = Level(price=9.8, kind="TEST")
    trig = detect_sweep_and_return(df, level, sweep_eps=0.01)
    assert trig is not None
    assert trig.side == "long"


def test_breakout_retest_long():
    df = _df([
        [1, 10, 10.1, 9.9, 10.0, 10],
        [2, 10.0, 10.4, 10.0, 10.3, 12],
        [3, 10.3, 10.35, 9.95, 10.1, 10],
        [4, 10.1, 10.5, 10.1, 10.4, 13],
    ])
    level = Level(price=10.0, kind="TEST")
    trig = detect_breakout_retest(df, level, hold_eps=0.0001)
    assert trig is not None
    assert trig.side == "long"
