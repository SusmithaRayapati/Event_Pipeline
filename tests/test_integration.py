
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


MAIN_FILE = Path(__file__).resolve().parents[1] / "main.py"


class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.root = Path(self.temp_dir.name)
        self.input_file = self.root / "events.jsonl"
        self.db_file = self.root / "test_stream.db"

        records = [
            {
                "event_id": "evt-001",
                "event_type": "order_placed",
                "entity_id": "order-001",
                "event_timestamp": "2026-10-28T10:00:00+00:00",
                "amount": 100,
            },
            {
                "event_id": "evt-002",
                "event_type": "order_placed",
                "entity_id": "order-002",
                "event_timestamp": "2026-10-28T10:01:00+00:00",
                "amount": 200,
            },
            {
                "event_id": "evt-001",
                "event_type": "order_placed",
                "entity_id": "order-001",
                "event_timestamp": "2026-10-28T10:00:00+00:00",
                "amount": 100,
            },
            {
                "event_id": "evt-003",
                "event_type": "payment_completed",
                "entity_id": "order-003",
                "event_timestamp": "2026-10-25T09:00:00+00:00",
                "amount": 300,
            },
        ]

        self.write_records(records, malformed_line=True)

    def write_records(self, records, malformed_line=False):
        """Write JSONL test input records."""
        with self.input_file.open("w", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record) + "\n")

            if malformed_line:
                file.write('{"event_id": \n')

    def run_pipeline(self, *args, batch_size="2", input_file=None):
        env = os.environ.copy()
        env["PIPELINE_INPUT"] = str(input_file or self.input_file)
        env["PIPELINE_DB"] = str(self.db_file)
        env["PIPELINE_BATCH_SIZE"] = batch_size

        return subprocess.run(
            [sys.executable, str(MAIN_FILE), *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

    def run_pipeline_success(self, *args, batch_size="2"):
        result = self.run_pipeline(*args, batch_size=batch_size)

        self.assertEqual(
            result.returncode,
            0,
            msg=(
                f"Pipeline failed.\nSTDOUT:\n{result.stdout}"
                f"\nSTDERR:\n{result.stderr}"
            ),
        )
        return result.stdout

    def query(self, sql, parameters=()):
        conn = sqlite3.connect(self.db_file, timeout=10)
        try:
            return conn.execute(sql, parameters).fetchone()
        finally:
            conn.close()

    def test_processing_and_recovery(self):
        output = self.run_pipeline_success()

        self.assertIn("Run status: success", output)
        self.assertIn("New events inserted: 3", output)
        self.assertIn("Duplicates skipped: 1", output)
        self.assertIn("Rejected records: 1", output)

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 3
        )
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0], 1
        )
        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            5,
        )

        # Re-running should resume from the saved checkpoint.
        second_output = self.run_pipeline_success()
        self.assertIn("Lines processed this run: 0", second_output)

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 3
        )

    def test_retry_after_failed_run(self):
        failed = self.run_pipeline(batch_size="0")

        self.assertNotEqual(failed.returncode, 0)
        self.assertIn(
            "BATCH_SIZE must be at least 1",
            failed.stdout + failed.stderr,
        )

        failed_run = self.query("""
            SELECT status, failure_reason
            FROM pipeline_runs
            ORDER BY started_at
            LIMIT 1
        """)

        self.assertEqual(failed_run[0], "failed")
        self.assertIn("BATCH_SIZE", failed_run[1])

        # Retry with a valid configuration.
        output = self.run_pipeline_success(batch_size="2")
        self.assertIn("Run status: success", output)

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 3
        )

    def test_backfill_uses_separate_checkpoint(self):
        self.run_pipeline_success()

        normal_checkpoint = self.query(
            "SELECT last_line FROM checkpoints WHERE source = ?",
            (self.input_file.name,),
        )[0]
        self.assertEqual(normal_checkpoint, 5)

        output = self.run_pipeline_success("--backfill")

        self.assertIn("Mode: backfill", output)
        self.assertIn("New events inserted: 0", output)
        self.assertIn("Duplicates skipped: 4", output)
        self.assertIn("Rejected records: 1", output)

        backfill_source = f"{self.input_file.name}::backfill"

        backfill_checkpoint = self.query(
            "SELECT last_line FROM checkpoints WHERE source = ?",
            (backfill_source,),
        )[0]
        self.assertEqual(backfill_checkpoint, 5)

        # Backfill must not modify the normal checkpoint.
        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            5,
        )

        # Rejections are tracked separately for normal and backfill sources.
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0], 2
        )

        second_output = self.run_pipeline_success("--backfill")
        self.assertIn("Lines processed this run: 0", second_output)
        self.assertIn("New events inserted: 0", second_output)

    def test_missing_input_file_records_failed_run(self):
        missing_file = self.root / "missing.jsonl"

        result = self.run_pipeline(input_file=missing_file)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Input file not found",
            result.stdout + result.stderr,
        )

        run = self.query("""
            SELECT status, failure_reason, ended_at
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)

        self.assertEqual(run[0], "failed")
        self.assertIn("FileNotFoundError", run[1])
        self.assertIsNotNone(run[2])

    def test_unexpected_schema_is_rejected_end_to_end(self):
        valid_event = {
            "event_id": "evt-valid",
            "event_type": "order_placed",
            "entity_id": "order-101",
            "event_timestamp": "2026-10-28T10:00:00+00:00",
            "amount": 150,
        }

        unexpected_event = {
            "event_id": "evt-extra",
            "event_type": "order_placed",
            "entity_id": "order-102",
            "event_timestamp": "2026-10-28T10:01:00+00:00",
            "amount": 200,
            "unexpected_field": "not_allowed",
        }

        self.write_records([valid_event, unexpected_event])

        output = self.run_pipeline_success()

        self.assertIn("New events inserted: 1", output)
        self.assertIn("Rejected records: 1", output)

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 1
        )
        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM events WHERE event_id = ?",
                ("evt-extra",),
            )[0],
            0,
        )

        rejected = self.query("""
            SELECT errors
            FROM rejected_events
            WHERE line_number = 2
        """)

        self.assertIsNotNone(rejected)
        self.assertIn("unexpected_field", rejected[0])

        checkpoint = self.query(
            "SELECT last_line FROM checkpoints WHERE source = ?",
            (self.input_file.name,),
        )[0]
        self.assertEqual(checkpoint, 2)

    def test_successful_run_metrics_are_recorded(self):
        self.run_pipeline_success()

        run = self.query("""
            SELECT status, input_lines, inserted_count,
                   duplicate_count, rejected_count, late_count,
                   failure_reason, ended_at
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)

        self.assertEqual(run[0], "success")
        self.assertEqual(run[1], 5)
        self.assertEqual(run[2], 3)
        self.assertEqual(run[3], 1)
        self.assertEqual(run[4], 1)
        self.assertIsNone(run[6])
        self.assertIsNotNone(run[7])


if __name__ == "__main__":
    unittest.main()
