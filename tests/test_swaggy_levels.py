import pandas as pd

from engines.swaggy.levels import build_levels
from engines.swaggy.vp_profile import VPLevels
from engines.volume_profile import VolumeProfile, ZoneBand


def _df(rows):
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def test_build_levels_with_swings_and_vp():
    df = _df([
        [1, 10, 11, 9, 10, 10],
        [2, 10, 12, 9.5, 11, 12],
        [3, 11, 11.5, 10, 10.5, 11],
        [4, 10.5, 12.2, 10.2, 12.0, 15],
        [5, 12.0, 12.1, 11.0, 11.2, 9],
        [6, 11.2, 11.4, 10.5, 10.8, 8],
    ])
    profile = VolumeProfile(
        poc=ZoneBand("POC", 10.0, 10.2, 100.0),
        vah=12.0,
        val=9.5,
        hvn=[ZoneBand("HVN", 10.8, 11.2, 50.0)],
        lvn=[ZoneBand("LVN", 9.8, 10.0, 10.0)],
        bins={},
        bin_size=0.1,
    )
    vp = VPLevels(profile=profile, poc=10.1, vah=12.0, val=9.5, lvn_gaps=[(9.8, 10.0)])
    levels = build_levels(df, vp, last_price=11.0, level_cluster_pct=0.01, tick_size=0.1)
    assert len(levels) > 0
    kinds = {lv.kind for lv in levels}
    assert "VP_POC" in kinds
