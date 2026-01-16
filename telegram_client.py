import time
from typing import Optional

import requests


class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = (bot_token or "").strip()
        self.chat_id = str(chat_id or "").strip()

    def send(self, text: str, allow_early: bool = False, chat_id: Optional[str] = None) -> bool:
        if not self.bot_token:
            return False
        target_chat = chat_id or self.chat_id
        if not target_chat:
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": target_chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        for attempt in range(2):
            try:
                r = requests.post(url, json=payload, timeout=20)
                return bool(r.ok)
            except Exception:
                if attempt == 1:
                    return False
                time.sleep(0.8)
        return False
