#!/usr/bin/env python3
"""Tests for agui-stream-doctor.

Run:  python3 -m unittest discover -s tests -v
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "agui_stream_doctor.py")
EX_IN = os.path.join(ROOT, "examples", "in")
sys.path.insert(0, ROOT)
import agui_stream_doctor as doc  # noqa: E402


def run(*args):
    proc = subprocess.run([sys.executable, TOOL] + list(args), capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def codes(report):
    return sorted({f["code"] for f in report["findings"]})


class TestBehavior(unittest.TestCase):
    def test_clean_stream_exit_zero(self):
        rc, out, err = run(os.path.join(EX_IN, "clean.jsonl"))
        self.assertEqual(rc, 0, err)
        self.assertIn("OK", out)

    def test_clean_json_report_parses_and_ok(self):
        rc, out, _ = run(os.path.join(EX_IN, "clean.jsonl"), "--json")
        report = json.loads(out)
        self.assertEqual(rc, 0)
        self.assertTrue(report["ok"])
        self.assertEqual(report["events"], 14)
        self.assertEqual(report["findings"], [])

    def test_clean_replay_final_state(self):
        rc, out, _ = run(os.path.join(EX_IN, "clean.jsonl"), "--json", "--replay")
        report = json.loads(out)
        self.assertEqual(rc, 0)
        self.assertEqual(report["final_state"], {"count": 2, "items": ["a", "b"]})

    def test_broken_stream_exit_one(self):
        rc, out, err = run(os.path.join(EX_IN, "broken.jsonl"), "--json")
        self.assertEqual(rc, 1)
        report = json.loads(out)
        self.assertFalse(report["ok"])
        self.assertGreaterEqual(report["counts"]["error"], 5)

    def test_broken_stream_finding_codes(self):
        _, out, _ = run(os.path.join(EX_IN, "broken.jsonl"), "--json")
        report = json.loads(out)
        for code in ("E-RUN-ORDER", "E-TOOL-UNOPENED", "E-TOOL-ARGS-NOT-JSON",
                     "E-STATE-DELTA-NO-BASE", "E-STATE-PATCH-FAILED",
                     "E-RUN-OUTCOME", "E-TEXT-UNCLOSED"):
            self.assertIn(code, codes(report), code)

    def test_truncated_stream_reports_unclosed_run(self):
        rc, out, _ = run(os.path.join(EX_IN, "truncated.jsonl"), "--json")
        report = json.loads(out)
        found = codes(report)
        self.assertEqual(rc, 1)
        self.assertIn("E-RUN-UNCLOSED", found)
        self.assertIn("E-TEXT-UNCLOSED", found)

    def test_sse_input_is_parsed(self):
        rc, out, _ = run(os.path.join(EX_IN, "clean.sse"), "--json")
        report = json.loads(out)
        self.assertEqual(rc, 0)
        self.assertEqual(report["events"], 5)
        self.assertTrue(report["ok"])

    def test_missing_file_exit_two(self):
        rc, _, err = run("/nonexistent/stream.jsonl")
        self.assertEqual(rc, 2)
        self.assertIn("cannot read", err)

    def test_empty_file_exit_two(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write("\n\n")
            path = fh.name
        try:
            rc, _, err = run(path)
        finally:
            os.unlink(path)
        self.assertEqual(rc, 2)
        self.assertIn("input error", err)

    def test_all_lines_unparseable_exit_two(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write("{oops\n{also bad\n")
            path = fh.name
        try:
            rc, _, err = run(path)
        finally:
            os.unlink(path)
        self.assertEqual(rc, 2)

    def test_fail_on_never_forces_zero(self):
        rc, _, _ = run(os.path.join(EX_IN, "broken.jsonl"), "--fail-on", "never")
        self.assertEqual(rc, 0)

    def test_fail_on_warning_trips_on_warning_only_stream(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write(json.dumps({"type": "RUN_STARTED", "threadId": "t", "runId": "r"}) + "\n")
            fh.write(json.dumps({"type": "STEP_STARTED", "stepName": "s"}) + "\n")
            fh.write(json.dumps({"type": "RUN_FINISHED", "threadId": "t", "runId": "r"}) + "\n")
            path = fh.name
        try:
            rc, out, _ = run(path, "--json", "--fail-on", "warning")
            report = json.loads(out)
        finally:
            os.unlink(path)
        self.assertEqual(rc, 1)
        self.assertEqual(report["counts"]["error"], 0)
        self.assertEqual(report["counts"]["warning"], 1)
        self.assertEqual(codes(report), ["E-STEP-UNCLOSED"])


class TestUnits(unittest.TestCase):
    def test_unknown_event_type_flagged(self):
        findings, _ = doc.check_events([{"type": "TEXT_MESSAGE_EXPLODE", "messageId": "m"}])
        self.assertIn("E-UNKNOWN-TYPE", [f.code for f in findings])

    def test_missing_required_field_flagged(self):
        findings, _ = doc.check_events([{"type": "RUN_STARTED", "threadId": "t"}])
        self.assertIn("E-MISSING-REQUIRED", [f.code for f in findings])

    def test_pointer_set_add_array_append(self):
        state = {"items": ["a"]}
        doc.pointer_set(state, "/items/-", "b")
        self.assertEqual(state, {"items": ["a", "b"]})

    def test_pointer_escape(self):
        state = {"a/b": {"~x": 1}}
        self.assertEqual(doc.pointer_get(state, "/a~1b/~0x"), 1)

    def test_patch_test_op_mismatch_returns_error(self):
        _, problem = doc.apply_patch({"a": 1}, [{"op": "test", "path": "/a", "value": 2}])
        self.assertIsNotNone(problem)

    def test_patch_replace_requires_existing_path(self):
        _, problem = doc.apply_patch({}, [{"op": "replace", "path": "/nope", "value": 1}])
        self.assertIn("cannot apply", problem)

    def test_outcome_without_string_type_flagged(self):
        findings, _ = doc.check_events([
            {"type": "RUN_STARTED", "threadId": "t", "runId": "r"},
            {"type": "RUN_FINISHED", "threadId": "t", "runId": "r", "outcome": {"foo": 1}},
        ])
        self.assertIn("E-RUN-OUTCOME", [f.code for f in findings])

    def test_empty_input_raises(self):
        with self.assertRaises(ValueError):
            doc.parse_stream("   \n")


if __name__ == "__main__":
    unittest.main()
