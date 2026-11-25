import json
import io
import threading
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

from catalog.alerts import TelegramAlerts, main


class TelegramDeliveryTests(unittest.TestCase):
    def test_configuration_is_optional(self):
        with patch.dict("os.environ", clear=True):
            self.assertIsNone(TelegramAlerts.from_environment())
        with patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "123:test", "TELEGRAM_CHAT_ID": "-123"}, clear=True):
            self.assertEqual(TelegramAlerts.from_environment().stall_seconds, 1800)

    def test_smoke_command_attempts_all_three_notifications(self):
        telegram = Mock(stall_seconds=5)
        telegram.send.side_effect = [True, False, True]
        with patch("sys.argv", ["alerts", "--test-events"]), patch("catalog.alerts.sleep"), \
                patch.object(TelegramAlerts, "from_environment", return_value=telegram), redirect_stdout(io.StringIO()):
            self.assertEqual(main(), 1)
        messages = [call.args[0] for call in telegram.send.call_args_list]
        self.assertEqual(len(messages), 3)
        for message, label in zip(messages, ("Сбор остановлен", "Страницы не сохраняются", "Сбор восстановился")):
            self.assertTrue(message.startswith("ТЕСТ"))
            self.assertIn(label, message)

    def test_send_message_and_sanitize_failures(self):
        requests = []
        replies = iter([(200, b'{"ok":true}'), (200, b'{"ok":false}'), (429, b'{}')])

        class TelegramFixture(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
                status, payload = next(replies)
                self.send_response(status)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), TelegramFixture) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                with patch("catalog.alerts.TELEGRAM_API", f"http://127.0.0.1:{server.server_port}"):
                    telegram = TelegramAlerts("123:test", "-123", 1800)
                    self.assertTrue(telegram.send("Сбор восстановился"))
                    with self.assertLogs("catalog.alerts", level="WARNING") as logs:
                        self.assertFalse(telegram.send("Сбор восстановился"))
                        self.assertFalse(telegram.send("Сбор восстановился"))
                    self.assertNotIn("123:test", "\n".join(logs.output))
            finally:
                server.shutdown()
                thread.join()
        self.assertEqual(requests, [
            ("/bot123:test/sendMessage", {"chat_id": "-123", "text": "Сбор восстановился"})
        ] * 3)
