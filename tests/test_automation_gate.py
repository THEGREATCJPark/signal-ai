import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))


def load_gate():
    spec = importlib.util.spec_from_file_location(
        "automation_gate",
        ROOT / "scripts" / "automation_gate.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_handoff_gate():
    spec = importlib.util.spec_from_file_location(
        "local_crawl_handoff_gate",
        ROOT / "scripts" / "local_crawl_handoff_gate.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AutomationGateTest(unittest.TestCase):
    def setUp(self):
        self.gate = load_gate()

    def test_skips_before_daily_publish_hour(self):
        now = datetime(2026, 4, 23, 7, 59, tzinfo=KST)

        decision = self.gate.should_run(
            now,
            last_run_at=datetime(2026, 4, 22, 8, 0, tzinfo=KST),
            publish_hour=8,
        )

        self.assertFalse(decision.run)
        self.assertIn("not due", decision.reason)

    def test_runs_after_publish_hour_when_last_success_is_past_day(self):
        now = datetime(2026, 4, 23, 9, 4, tzinfo=KST)

        decision = self.gate.should_run(
            now,
            last_run_at=datetime(2026, 4, 22, 8, 0, tzinfo=KST),
            publish_hour=8,
        )

        self.assertTrue(decision.run)
        self.assertIn("catch-up", decision.reason)

    def test_skips_when_already_ran_today(self):
        now = datetime(2026, 4, 23, 21, 0, tzinfo=KST)

        decision = self.gate.should_run(
            now,
            last_run_at=datetime(2026, 4, 23, 9, 4, tzinfo=KST),
            publish_hour=8,
        )

        self.assertFalse(decision.run)
        self.assertIn("already ran today", decision.reason)

    def test_run_command_if_due_uses_lock_and_records_skip(self):
        calls = []
        now = datetime(2026, 4, 23, 9, 4, tzinfo=KST)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "articles.json"
            state.write_text(
                '{"last_run_at":"2026-04-22T08:00:00+09:00","articles":[]}',
                encoding="utf-8",
            )
            log = root / "logs" / "signal_daily.log"
            lock = root / "data" / "automation.lock"

            rc = self.gate.run_command_if_due(
                root=root,
                state_path=state,
                log_path=log,
                lock_path=lock,
                now=now,
                command=["echo", "ran"],
                runner=lambda cmd, **kwargs: calls.append(cmd) or 0,
            )

        self.assertEqual(rc, 0)
        self.assertEqual(calls, [["echo", "ran"]])

    def test_run_command_if_due_skips_second_run_same_day(self):
        calls = []
        now = datetime(2026, 4, 23, 21, 0, tzinfo=KST)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "articles.json"
            state.write_text(
                '{"last_run_at":"2026-04-23T09:04:00+09:00","articles":[]}',
                encoding="utf-8",
            )
            log = root / "logs" / "signal_daily.log"
            lock = root / "data" / "automation.lock"

            rc = self.gate.run_command_if_due(
                root=root,
                state_path=state,
                log_path=log,
                lock_path=lock,
                now=now,
                command=["echo", "ran"],
                runner=lambda cmd, **kwargs: calls.append(cmd) or 0,
            )

        self.assertEqual(rc, 0)
        self.assertEqual(calls, [])


class LocalCrawlHandoffGateTest(unittest.TestCase):
    def setUp(self):
        self.gate = load_handoff_gate()

    def test_successful_handoff_updates_own_state_and_prevents_double_run(self):
        calls = []
        now = datetime(2026, 4, 25, 7, 30, tzinfo=KST)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "data" / "local_crawl_handoff_state.json"
            log = root / "logs" / "local_crawl_handoff.log"
            lock = root / "data" / "local_crawl_handoff.lock"

            rc = self.gate.run_command_if_due(
                root=root,
                state_path=state,
                log_path=log,
                lock_path=lock,
                now=now,
                command=["echo", "handoff"],
                runner=lambda cmd, **kwargs: calls.append(cmd) or 0,
            )
            rc2 = self.gate.run_command_if_due(
                root=root,
                state_path=state,
                log_path=log,
                lock_path=lock,
                now=now.replace(hour=9),
                command=["echo", "handoff"],
                runner=lambda cmd, **kwargs: calls.append(cmd) or 0,
            )
            text = state.read_text(encoding="utf-8")

        self.assertEqual(0, rc)
        self.assertEqual(0, rc2)
        self.assertEqual([["echo", "handoff"]], calls)
        self.assertIn("last_run_at", text)
        self.assertIn("2026-04-25T07:30:00+09:00", text)

    def test_handoff_task_wrapper_uses_gate_and_dispatcher(self):
        wrapper = ROOT / "run_local_crawl_handoff_task.sh"
        text = wrapper.read_text(encoding="utf-8")

        self.assertIn("scripts/local_crawl_handoff_gate.py", text)
        self.assertIn("scripts/dispatch_local_crawl_handoff.py", text)


if __name__ == "__main__":
    unittest.main()
