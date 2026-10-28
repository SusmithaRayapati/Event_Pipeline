
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main as pipeline


class TestStreamingBehavior(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.root = Path(self.temp_dir.name)
        self.input_file = self.root / "events.jsonl"
        self.db_file = self.root / "events.db"

        self.settings = patch.multiple(
            pipeline,
            INPUT_FILE=self.input_file,
            DB_FILE=self.db_file,
            SOURCE_NAME=self.input_file.name,
            BATCH_SIZE=2,
        )
        self.settings.start()
        self.addCleanup(self.settings.stop)

        pipeline.setup_database()

    def make_event(self, event_id, timestamp, amount=100):
        return {
            "event_id": event_id,
            "event_type": "order_placed",
            "entity_id": "order_001",
            "event_timestamp": timestamp,
            "amount": amount,
        }

    def make_batch(self, events):
        return [
            (line_number, json.dumps(event))
            for line_number, event in enumerate(events, start=1)
        ]

    def query(self, sql, parameters=()):
        conn = sqlite3.connect(self.db_file)
        try:
            return conn.execute(sql, parameters).fetchone()
        finally:
            conn.close()

    def test_empty_input(self):
        result = pipeline.process_and_checkpoint([])
        self.assertEqual(result, (0, 0, 0, 0))

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 0
        )
        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            0,
        )

    def test_duplicate_event_id(self):
        event = self.make_event(
            "evt_001", "2026-10-28T10:00:00+00:00"
        )

        result = pipeline.process_and_checkpoint(
            self.make_batch([event, event])
        )

        self.assertEqual(result, (1, 1, 0, 0))
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 1
        )
        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            2,
        )

    def test_duplicate_id_with_different_payload_keeps_first_event(self):
        original = self.make_event(
            "evt_shared", "2026-10-28T10:00:00+00:00", 100
        )
        changed = self.make_event(
            "evt_shared", "2026-10-28T11:00:00+00:00", 999
        )

        first = pipeline.process_and_checkpoint(
            [(1, json.dumps(original))]
        )
        second = pipeline.process_and_checkpoint(
            [(2, json.dumps(changed))]
        )

        self.assertEqual(first, (1, 0, 0, 0))
        self.assertEqual(second, (0, 1, 0, 0))

        stored = self.query(
            "SELECT amount, event_timestamp FROM events "
            "WHERE event_id = ?",
            ("evt_shared",),
        )

        self.assertEqual(stored[0], 100)
        self.assertEqual(stored[1], "2026-10-28T10:00:00+00:00")

    def test_late_event_detection(self):
        recent = self.make_event(
            "evt_recent", "2026-10-28T10:00:00+00:00"
        )
        late = self.make_event(
            "evt_late", "2026-10-26T09:00:00+00:00"
        )

        result = pipeline.process_and_checkpoint(
            self.make_batch([recent, late])
        )

        self.assertEqual(result, (2, 0, 0, 1))
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM late_events")[0], 1
        )
        self.assertEqual(
            self.query("SELECT event_id FROM late_events")[0],
            "evt_late",
        )

    def test_aggregate_reconciliation(self):
        events = [
            self.make_event(
                "evt_001", "2026-10-28T10:00:00+00:00", 100
            ),
            self.make_event(
                "evt_002", "2026-10-28T10:01:00+00:00", 200
            ),
        ]

        result = pipeline.process_and_checkpoint(
            self.make_batch(events)
        )
        self.assertEqual(result, (2, 0, 0, 0))

        count, total_amount = self.query("""
            SELECT COUNT(*), SUM(amount) FROM events
        """)
        distinct_ids = self.query(
            "SELECT COUNT(DISTINCT event_id) FROM events"
        )[0]

        self.assertEqual(count, 2)
        self.assertEqual(total_amount, 300)
        self.assertEqual(count, distinct_ids)

    def test_batch_failure_rolls_back_events_and_checkpoint(self):
        event = self.make_event(
            "evt_rollback", "2026-10-28T10:00:00+00:00"
        )
        batch = self.make_batch([event])

        with patch.object(
            pipeline,
            "validate_event",
            side_effect=RuntimeError("Simulated batch failure"),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "Simulated batch failure"
            ):
                pipeline.process_and_checkpoint(batch)

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 0
        )
        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            0,
        )

    def test_unexpected_fields_are_rejected(self):
        event = self.make_event(
            "evt_extra", "2026-10-28T10:00:00+00:00"
        )
        event["unexpected_field"] = "extra"

        result = pipeline.process_and_checkpoint(
            self.make_batch([event])
        )

        self.assertEqual(result, (0, 0, 1, 0))
        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM events WHERE event_id = ?",
                ("evt_extra",),
            )[0],
            0,
        )
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0],
            1,
        )

    def test_same_event_id_from_another_source_is_deduplicated(self):
        event = self.make_event(
            "evt_shared", "2026-10-28T10:00:00+00:00"
        )
        raw = json.dumps(event)

        pipeline.process_and_checkpoint(
            [(1, raw)], checkpoint_source="source_a"
        )
        result = pipeline.process_and_checkpoint(
            [(1, raw)], checkpoint_source="source_b"
        )

        self.assertEqual(result, (0, 1, 0, 0))
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 1
        )

    def test_rejected_record_is_saved(self):
        malformed = '{"event_id": '

        result = pipeline.process_and_checkpoint([(1, malformed)])

        self.assertEqual(result, (0, 0, 1, 0))
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0],
            1,
        )

    def test_retry_does_not_duplicate_rejected_record(self):
        malformed = '{"event_id": '

        first = pipeline.process_and_checkpoint([(1, malformed)])
        second = pipeline.process_and_checkpoint([(1, malformed)])

        self.assertEqual(first, (0, 0, 1, 0))
        self.assertEqual(second, (0, 0, 1, 0))

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0],
            1,
        )

    def test_non_object_json_is_rejected(self):
        for raw in ("[]", '"hello"', "123", "null"):
            with self.subTest(raw=raw):
                result = pipeline.process_and_checkpoint([(1, raw)])
                self.assertEqual(result, (0, 0, 1, 0))

        self.assertEqual(
            self.query("SELECT COUNT(*) FROM events")[0], 0
        )
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM rejected_events")[0], 1
        )

    def test_timestamp_is_normalized_to_utc(self):
        event = self.make_event(
            "evt_timezone", "2026-10-28T15:30:00+05:30"
        )

        result = pipeline.process_and_checkpoint(
            [(1, json.dumps(event))]
        )

        self.assertEqual(result, (1, 0, 0, 0))
        stored_timestamp = self.query(
            "SELECT event_timestamp FROM events WHERE event_id = ?",
            ("evt_timezone",),
        )[0]

        self.assertEqual(
            stored_timestamp, "2026-10-28T10:00:00+00:00"
        )

    def test_checkpoint_value(self):
        event = self.make_event(
            "evt_checkpoint", "2026-10-28T10:00:00+00:00"
        )

        pipeline.process_and_checkpoint(self.make_batch([event]))

        self.assertEqual(
            self.query(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (self.input_file.name,),
            )[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
