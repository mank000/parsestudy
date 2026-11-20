import signal
import unittest
from unittest.mock import Mock, patch

from catalog import worker


class WorkerRecovery(unittest.TestCase):
    def test_restarts_transient_failure_and_removes_restart_option(self):
        failed = Mock()
        failed.poll.return_value = failed.wait.return_value = 1
        complete = Mock()
        complete.poll.return_value = complete.wait.return_value = 0
        with patch.object(worker.subprocess, "Popen", side_effect=[failed, complete]) as popen, \
                patch.object(worker.signal, "signal"), \
                patch.object(worker, "source_retry_at", return_value=None), \
                patch.object(worker, "retry_delay", return_value=0) as delay, \
                patch.object(worker.sys, "argv", ["worker", "--restart", "--country", "CN"]):
            self.assertEqual(worker.main(), 0)
        self.assertEqual(popen.call_count, 2)
        self.assertIn("--restart", popen.call_args_list[0].args[0])
        self.assertNotIn("--restart", popen.call_args_list[1].args[0])
        delay.assert_called_once_with(1, None)

    def test_permanent_failure_stops_restarts(self):
        child = Mock()
        child.poll.return_value = child.wait.return_value = 2
        with patch.object(worker.subprocess, "Popen", return_value=child) as popen, \
                patch.object(worker.signal, "signal"), \
                patch.object(worker.sys, "argv", ["worker"]):
            self.assertEqual(worker.main(), 0)
        popen.assert_called_once()

    def test_stop_signal_is_forwarded_to_child(self):
        handlers = {}
        child = Mock()
        child.poll.return_value = None

        class StopEvent:
            stopped = False

            def is_set(self):
                return self.stopped

            def set(self):
                self.stopped = True

            def wait(self, timeout):
                handlers[signal.SIGTERM](signal.SIGTERM, None)
                return True

        def register(number, handler):
            handlers[number] = handler

        with patch.object(worker.subprocess, "Popen", return_value=child), \
                patch.object(worker.signal, "signal", side_effect=register), \
                patch.object(worker.threading, "Event", StopEvent), \
                patch.object(worker.sys, "argv", ["worker"]):
            self.assertEqual(worker.main(), 0)
        child.send_signal.assert_called_once_with(signal.SIGINT)
        child.wait.assert_called_once_with(timeout=25)
