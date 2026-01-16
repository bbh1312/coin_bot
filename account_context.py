from dataclasses import dataclass
from typing import Any, Dict, Optional

from executor import AccountExecutor
from telegram_client import TelegramClient


@dataclass
class AccountSettings:
    entry_pct: float
    dry_run: bool
    auto_exit: bool
    max_positions: int
    leverage: int
    margin_mode: str
    exit_cooldown_h: float
    long_tp_pct: float
    long_sl_pct: float
    short_tp_pct: float
    short_sl_pct: float
    dca_enabled: bool
    dca_pct: float
    dca1_pct: float
    dca2_pct: float
    dca3_pct: float


@dataclass
class AccountContext:
    account_id: int
    name: str
    api_key: str
    api_secret: str
    settings: AccountSettings
    executor: AccountExecutor
    telegram: Optional[TelegramClient]
    state_path: str
    meta: Dict[str, Any]
