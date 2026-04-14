import json
import tempfile
import unittest
from pathlib import Path

from src.source_grounded_method_audit import load_profile, run_audit


ROOT = Path(__file__).resolve().parents[1]


class SourceGroundedMethodAuditTests(unittest.TestCase):
    def test_demo_profile_loads_two_aspects(self):
        profile = load_profile(ROOT / "examples/profile.json")
        self.assertEqual(sorted(profile), ["method_alpha", "method_beta"])

    def test_grounded_answer_requires_event_and_direct_conclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_audit(
                ROOT / "examples/profile.json",
                ROOT / "examples/demo_records.jsonl",
                Path(tmp),
            )
            by_aspect = {item["aspect"]: item for item in summary["aspects"]}
            self.assertEqual(by_aspect["method_alpha"]["status"], "grounded_answer")
            self.assertTrue(by_aspect["method_alpha"]["answer_pass"])
            self.assertGreaterEqual(by_aspect["method_alpha"]["event_count"], 1)
            self.assertGreaterEqual(by_aspect["method_alpha"]["direct_conclusion_count"], 1)

    def test_allegation_and_procedural_spans_do_not_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            summary = run_audit(ROOT / "examples/profile.json", ROOT / "examples/demo_records.jsonl", out)
            by_aspect = {item["aspect"]: item for item in summary["aspects"]}
            self.assertEqual(by_aspect["method_beta"]["status"], "event_found_direct_conclusion_missing")
            self.assertFalse(by_aspect["method_beta"]["answer_pass"])
            self.assertGreaterEqual(by_aspect["method_beta"]["allegation_count"], 1)
            self.assertGreaterEqual(by_aspect["method_beta"]["procedural_count"], 1)
            findings = json.loads((out / "findings.json").read_text(encoding="utf-8"))
            self.assertEqual(findings["method_beta"]["direct_method_conclusion_spans"], [])

    def test_outputs_are_source_addressed(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            run_audit(ROOT / "examples/profile.json", ROOT / "examples/demo_records.jsonl", out)
            findings = json.loads((out / "findings.json").read_text(encoding="utf-8"))
            first_alpha = findings["method_alpha"]["event_spans"][0]
            self.assertEqual(first_alpha["source_id"], "demo::1")
            self.assertIn("record_hash", first_alpha)
            self.assertIsInstance(first_alpha["span_start"], int)
            self.assertIsInstance(first_alpha["span_end"], int)


if __name__ == "__main__":
    unittest.main()
