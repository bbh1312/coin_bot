from dataclasses import dataclass


@dataclass
class CurrPositionSwaggyConfig:
    interval_sec: int = 15 * 60
