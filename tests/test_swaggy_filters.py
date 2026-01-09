import pandas as pd

from engines.swaggy.filters import cooldown_ok, dist_to_level_ok, expansion_bar, in_lvn_gap, regime_ok
from engines.swaggy.levels import Level
from engines.swaggy.vp_profile import VPLevels
from engines.volume_profile import VolumeProfile, ZoneBand


def _df(rows):
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def test_dist_to_level_filter():
    level = Level(price=100.0, kind="TEST")
    assert dist_to_level_ok(100.2, level, max_dist=0.01).ok
    assert not dist_to_level_ok(105.0, level, max_dist=0.01).ok


def test_lvn_gap_filter():
    profile = VolumeProfile(
        poc=ZoneBand("POC", 100.0, 100.1, 100.0),
        vah=110.0,
        val=90.0,
        hvn=[],
        lvn=[ZoneBand("LVN", 99.0, 101.0, 10.0)],
        bins={},
        bin_size=0.1,
    )
    vp = VPLevels(profile=profile, poc=100.05, vah=110.0, val=90.0, lvn_gaps=[(99.0, 101.0)])
    assert not in_lvn_gap(100.0, vp).ok
    assert in_lvn_gap(105.0, vp).ok


def test_expansion_filter():
    df = _df([
        [1, 10, 10.2, 9.9, 10.1, 10],
        [2, 10.1, 10.3, 10.0, 10.2, 11],
        [3, 10.2, 10.4, 10.1, 10.3, 12],
        [4, 10.3, 11.2, 9.5, 10.8, 13],
    ])
    assert not expansion_bar(df, atr_len=2, expansion_mult=1.2).ok


def test_cooldown_and_regime():
    assert not cooldown_ok(1000, 990, cooldown_min=1).ok
    assert cooldown_ok(1000, 900, cooldown_min=1).ok
    assert not regime_ok("bull", "SHORT", allow_countertrend=False).ok
    assert regime_ok("bull", "SHORT", allow_countertrend=True).ok
