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


if __name__ == "__main__":
    unittest.main()
