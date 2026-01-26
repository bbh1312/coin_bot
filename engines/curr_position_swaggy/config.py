from dataclasses import dataclass


@dataclass
class CurrPositionSwaggyConfig:
    interval_sec: int = 60 * 60
