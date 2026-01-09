import json
import time
import urllib.parse
import urllib.request


def send_message(token: str, chat_id: str, text: str, retries: int = 2) -> bool:
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    data = urllib.parse.urlencode(payload).encode("utf-8")
    for _ in range(max(1, retries + 1)):
        try:
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                if "ok" in body:
                    return True
        except Exception:
            time.sleep(0.5)
            continue
    return False
