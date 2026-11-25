import unittest
from unittest.mock import Mock

from scrapy.exceptions import CloseSpider

from catalog.monitor import evaluate
from catalog.satauto_api import ApiScan, SatautoApiSpider


class ApiSpiderTests(unittest.TestCase):
    def setUp(self):
        self.scan = ApiScan("JP", None, {})
        self.spider = SatautoApiSpider(Mock(), 1, [self.scan], None)
        self.spider.crawler = Mock()
        self.spider.settings = Mock()
        self.spider.settings.getint.return_value = 16_384
        self.request = next(self.spider.next_requests())

    def test_internal_error_releases_slot_and_keeps_traceback(self):
        self.spider.scans.append(ApiScan("KR", None, {}))
        self.spider.listing_page = Mock(return_value=([{"id": 1, "source": {"country": "JP"}}], None))
        self.spider.add_detail_links = Mock(side_effect=TypeError("регрессия в обработчике"))
        response = Mock(status=200)
        self.spider.db.transaction.return_value = unittest.mock.MagicMock()
        with self.assertRaisesRegex(TypeError, "регрессия в обработчике"):
            list(self.spider.parse_page(response, self.scan))
        self.assertEqual(self.spider.in_flight, 0)
        self.spider.crawler.stats.set_value.assert_called_once_with("catalog/diagnostic", "api_error")
        self.assertEqual(list(self.spider.next_requests()), [])

    def test_invalid_source_data_releases_slot_and_has_its_own_diagnostic(self):
        self.spider.listing_page = Mock(return_value=([{"id": 1, "source": []}], None))
        with self.assertRaises(CloseSpider) as raised:
            list(self.spider.parse_page(Mock(status=200), self.scan))
        self.assertEqual(raised.exception.reason, "invalid_vehicle")
        self.assertEqual(self.spider.in_flight, 0)

    def test_download_failure_releases_slot(self):
        failure = Mock()
        failure.check.return_value = False
        failure.type = TimeoutError
        with self.assertRaises(CloseSpider) as raised:
            self.spider.request_failed(failure)
        self.assertEqual(raised.exception.reason, "TimeoutError")
        self.assertEqual(self.spider.in_flight, 0)


class MonitorTests(unittest.TestCase):
    def test_monitor_distinguishes_lost_heartbeat_and_stale_sample(self):
        snapshot = {"checked_at": "2026-11-23T12:00:00+00:00", "heartbeat_at": "2026-11-23T11:59:55+00:00",
                    "rows": [{"country": "JP", "selection": {"_lane": "recent"}, "partition": None,
                              "last_success_at": "2026-11-23T11:55:00+00:00"}]}
        args = ("JP", {"_lane": "recent"}, None, 75)
        self.assertEqual(evaluate(snapshot, *args, 600)["state"], "ok")
        self.assertEqual(evaluate(snapshot, *args, 100)["state"], "selection_stale")
        snapshot["heartbeat_at"] = None
        self.assertEqual(evaluate(snapshot, *args, 600)["state"], "heartbeat_missing")
