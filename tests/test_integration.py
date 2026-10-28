import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAIN_FILE = PROJECT_ROOT / "main.py"


class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.work_dir = Path(self.temp_dir.name)

        self.input_file = self.work_dir / "events.jsonl"
        self.db_file = self.work_dir / "test_stream.db"

        events = [
            {
                "event_id": "evt-001",
                "event_type": "purchase",
                "entity_id": "user-001",
                "event_timestamp": "2026-01-03T10:00:00Z",
                "amount": 100.0,
            },
            {
                "event_id": "evt-002",
                "event_type": "purchase",
                "entity_id": "user-002",
                "event_timestamp": "2026-01-05T10:00:00Z",
                "amount": 250.0,
                "schema_version": 2,
                "source": "integration_test",
            },
            {
                "event_id": "evt-001",
                "event_type": "purchase",
                "entity_id": "user-001",
                "event_timestamp": "2026-01-03T10:00:00Z",
                "amount": 100.0,
            },
            {
                "event_id": "evt-003",
                "event_type": "refund",
                "entity_id": "user-003",
                "event_timestamp": "2026-01-01T10:00:00Z",
                "amount": 25.0,
            },
        ]

        lines = [json.dumps(event) for event in events]
        lines.append("{invalid json")

        self.input_file.write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_pipeline(self):
        env = os.environ.copy()
        env["PIPELINE_INPUT"] = str(self.input_file)
        env["PIPELINE_DB"] = str(self.db_file)
        env["PIPELINE_BATCH_SIZE"] = "2"

        result = subprocess.run(
            [sys.executable, str(MAIN_FILE)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

        self.assertEqual(
            result.returncode,
            0,
            msg=f"Pipeline failed:\n{result.stdout}\n{result.stderr}",
        )

        return result.stdout

    def test_processing_and_recovery(self):
        # First run: process all five input lines.
        output = self.run_pipeline()

        self.assertIn("New events inserted: 3", output)
        self.assertIn("Duplicates skipped: 1", output)
        self.assertIn("Rejected records: 1", output)
        self.assertIn("Late events detected this run: 1", output)
        self.assertIn("Reconciliation passed", output)

        with sqlite3.connect(self.db_file) as conn:
            event_count = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

            rejected_count = conn.execute(
                "SELECT COUNT(*) FROM rejected_events"
            ).fetchone()[0]

            late_count = conn.execute(
                "SELECT COUNT(*) FROM late_events"
            ).fetchone()[0]

            checkpoint = conn.execute(
                "SELECT last_line FROM checkpoints"
            ).fetchone()[0]

            schema_event = conn.execute(
                "SELECT event_id FROM events WHERE event_id = ?",
                ("evt-002",),
            ).fetchone()

        self.assertEqual(event_count, 3)
        self.assertEqual(rejected_count, 1)
        self.assertEqual(late_count, 1)
        self.assertEqual(checkpoint, 5)
        self.assertIsNotNone(schema_event)

        # Second run: checkpoint should prevent reprocessing.
        second_output = self.run_pipeline()

        self.assertIn("Lines processed this run: 0", second_output)
        self.assertIn("New events inserted: 0", second_output)

        with sqlite3.connect(self.db_file) as conn:
            event_count_after_rerun = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

            rejected_after_rerun = conn.execute(
                "SELECT COUNT(*) FROM rejected_events"
            ).fetchone()[0]

        self.assertEqual(event_count_after_rerun, 3)
        self.assertEqual(rejected_after_rerun, 1)

    def test_retry_after_failed_run(self):
        # Save the input and temporarily remove it.
        original_content = self.input_file.read_text(encoding="utf-8")
        self.input_file.unlink()

        # The pipeline should fail because the input file is missing.
        with self.assertRaises(AssertionError):
            self.run_pipeline()

        # Verify failure monitoring and checkpoint state.
        with sqlite3.connect(self.db_file) as conn:
            failed_run = conn.execute("""
                SELECT status, failure_reason
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
            """).fetchone()

            checkpoint = conn.execute(
                "SELECT last_line FROM checkpoints"
            ).fetchone()[0]

        self.assertEqual(failed_run[0], "failed")
        self.assertIsNotNone(failed_run[1])
        self.assertEqual(checkpoint, 0)

        # Restore the input and retry.
        self.input_file.write_text(original_content, encoding="utf-8")
        output = self.run_pipeline()

        self.assertIn("New events inserted: 3", output)
        self.assertIn("Reconciliation passed", output)

        with sqlite3.connect(self.db_file) as conn:
            checkpoint = conn.execute(
                "SELECT last_line FROM checkpoints"
            ).fetchone()[0]

            event_count = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

        self.assertEqual(checkpoint, 5)
        self.assertEqual(event_count, 3)


if __name__ == "__main__":
    unittest.main()
