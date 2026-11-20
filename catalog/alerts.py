"""Opt-in Telegram notifications for the SATAuto supervisor."""
import json
import logging
import os
import re
from http.client import HTTPException
from time import monotonic
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg

from .satauto import BASE


LOGGER = logging.getLogger(__name__)
TELEGRAM_API = "https://api.telegram.org"


class TelegramAlerts:
    def __init__(self, token, chat_id, stall_seconds):
        self._token = token
        self._chat_id = chat_id
        self.stall_seconds = stall_seconds

    @classmethod
    def from_environment(cls):
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token and not chat_id:
            return None
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token) or not chat_id:
            LOGGER.warning("Telegram отключён. Проверьте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID.")
            return None
        try:
            stall_seconds = int(os.getenv("CATALOG_ALERT_STALL_SECONDS", "1800"))
        except ValueError:
            stall_seconds = 0
        if stall_seconds < 1:
            LOGGER.warning("Telegram отключён. CATALOG_ALERT_STALL_SECONDS должен быть положительным целым числом.")
            return None
        return cls(token, chat_id, stall_seconds)

    def send(self, text):
        request = Request(
            f"{TELEGRAM_API}/bot{self._token}/sendMessage",
            data=json.dumps({"chat_id": self._chat_id, "text": text}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=4) as response:
                payload = json.load(response)
            if isinstance(payload, dict) and payload.get("ok") is True:
                return True
        except HTTPError as error:
            LOGGER.warning("Telegram не принял уведомление (HTTP %s).", error.code)
            error.close()
            return False
        except (OSError, HTTPException, ValueError):
            LOGGER.warning("Telegram недоступен или вернул некорректный ответ.")
            return False
        LOGGER.warning("Telegram не подтвердил доставку уведомления.")
        return False


class SyncAlerts:
    def __init__(self, db, telegram, profile, selection):
        self.db = db
        self.telegram = telegram
        self.profile = profile
        self.scope = selection.get("country", "все страны")
        if "year" in selection:
            self.scope += f", {selection['year']} г."
        if "year_min" in selection:
            self.scope += f", от {selection['year_min']} г."
        if "year_max" in selection:
            self.scope += f", до {selection['year_max']} г."
        self.last_pages = None
        self.active_seconds = 0
        self.retry_after = {}

    def observe(self, lane, active_seconds=0, *, reason=None):
        try:
            self._observe(lane, active_seconds, reason)
        except psycopg.Error:
            LOGGER.warning("Не удалось обновить состояние уведомлений SATAuto в PostgreSQL.")

    def _observe(self, lane, elapsed, reason):
        pages, incident, incident_pages, paused = self.db.execute("""
            SELECT pages_saved, alert_kind, alert_pages, EXISTS (
                SELECT 1 FROM crawl_run WHERE origin=%s AND retry_at>clock_timestamp()
            )
            FROM catalog_sync_state WHERE profile=%s
            """, (BASE, self.profile)).fetchone()
        if pages != self.last_pages:
            self.last_pages = pages
            self.active_seconds = 0
        elif not paused:
            self.active_seconds += elapsed

        if reason is not None:
            if incident == "stopped":
                return
            event = "stopped"
            message = f"Сбор остановлен из-за ошибки. Причина — {reason}."
        elif incident and pages > incident_pages:
            event = "recovered"
            message = "Сбор восстановился. Новая страница сохранена в PostgreSQL."
        elif not incident and not paused and self.active_seconds >= self.telegram.stall_seconds:
            event = "stalled"
            message = f"Страницы не сохраняются уже {int(self.active_seconds)} с активного обхода."
        else:
            return

        if monotonic() < self.retry_after.get(event, 0):
            return
        label = "новые объявления" if lane == "recent" else "архив"
        if not self.telegram.send(f"SATAuto · {label} · {self.scope}\n{message}"):
            self.retry_after[event] = monotonic() + 60
            return
        self.retry_after.clear()
        self.db.execute("""
            UPDATE catalog_sync_state SET alert_kind=%s, alert_pages=%s WHERE profile=%s
            """, (None, None, self.profile) if event == "recovered" else (event, pages, self.profile))


def main():
    telegram = TelegramAlerts.from_environment()
    if telegram is None:
        LOGGER.error("Укажите TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .telegram.env.")
        return 1
    if not telegram.send("SATAuto · проверка доставки уведомлений"):
        return 1
    print("Тестовое уведомление отправлено.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
