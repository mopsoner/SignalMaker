from __future__ import annotations

import requests


class NotifierService:
    def test(self, telegram_chat_id: str = '', telegram_secret: str = '', discord_url: str = '') -> dict:
        checks: dict[str, str] = {}
        if telegram_chat_id and telegram_secret:
            checks['telegram'] = 'configured'
        else:
            checks['telegram'] = 'missing_config'
        if discord_url:
            try:
                response = requests.post(discord_url, json={'content': 'SignalMaker notification test'}, timeout=10)
                checks['discord'] = 'ok' if response.ok else f'http_{response.status_code}'
            except Exception as exc:
                checks['discord'] = f'error:{exc.__class__.__name__}'
        else:
            checks['discord'] = 'missing_config'
        return checks
