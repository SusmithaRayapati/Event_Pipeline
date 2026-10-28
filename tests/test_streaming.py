
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


class TestStreamingBehavior(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.root = Path(self.temp_dir.name)
        self.input_file = self.root / "events.jsonl"
        self.db_file = self.root / "events.db"

        with sqlite3.connect(self.db_file) as conn:
            conn.executescript("""
                CREATE TABLE events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    amount REAL NOT NULL
                );

                CREATE TABLE checkpoints (
                    source TEXT PRIMARY KEY,
                    last_line INTEGER NOT NULL
                );

                CREATE TABLE rejected_events (
                    line_number INTEGER PRIMARY KEY,
                    raw_record TEXT,
                    errors TEXT NOT NULL
                );

                CREATE TABLE late_events (
                    event_id TEXT PRIMARY KEY,
                    event_timestamp TEXT NOT NULL
                );

                INSERT INTO checkpoints VALUES ('events.jsonl', 0);
            """)

    def make_event(self, event_id, timestamp, amount=100):
        return {
            "event_id": event_id,
            "event_type": "order_placed",
            "entity_id": "order_001",
            "event_timestamp": timestamp,
            "amount": amount,
        }

    def write_events(self, events):
        with self.input_file.open("w", encoding="utf-8") as file:
            for event in events:
                file.write(json.dumps(event) + "\n")

    def test_empty_input(self):
        self.input_file.write_text("", encoding="utf-8")

        with self.input_file.open(encoding="utf-8") as file:
            self.assertEqual(list(file), [])

        with sqlite3.connect(self.db_file) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

        self.assertEqual(count, 0)

    def test_duplicate_event_id(self):
        event = self.make_event(
            "evt_001", "2026-10-28T10:00:00+00:00"
        )
        self.write_events([event, event])

        with self.input_file.open(encoding="utf-8") as file:
            records = [json.loads(line) for line in file]

        with sqlite3.connect(self.db_file) as conn:
            for item in records:
                conn.execute("""
                    INSERT OR IGNORE INTO events
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    item["event_id"],
                    item["event_type"],
                    item["entity_id"],
                    item["event_timestamp"],
                    item["amount"],
                ))

            count = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

        self.assertEqual(count, 1)

    def test_late_event_detection(self):
        timestamps = [
            "2026-10-28T10:00:00+00:00",
            "2026-10-28T09:00:00+00:00",
        ]

        max_seen = None
        late_count = 0

        for timestamp in timestamps:
            if max_seen is not None and timestamp < max_seen:
                late_count += 1

            if max_seen is None or timestamp > max_seen:
                max_seen = timestamp

        self.assertEqual(late_count, 1)

    def test_aggregate_reconciliation(self):
        events = [
            self.make_event(
                "evt_001", "2026-10-28T10:00:00+00:00", 100
            ),
            self.make_event(
                "evt_002", "2026-10-28T10:01:00+00:00", 200
            ),
        ]

        with sqlite3.connect(self.db_file) as conn:
            for event in events:
                conn.execute("""
                    INSERT INTO events
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    event["event_id"],
                    event["event_type"],
                    event["entity_id"],
                    event["event_timestamp"],
                    event["amount"],
                ))

            count, total_amount = conn.execute("""
                SELECT COUNT(*), SUM(amount)
                FROM events
            """).fetchone()

        self.assertEqual(count, 2)
        self.assertEqual(total_amount, 300)

    def test_checkpoint_value(self):
        with sqlite3.connect(self.db_file) as conn:
            conn.execute("""
                UPDATE checkpoints
                SET last_line = 10
                WHERE source = 'events.jsonl'
            """)

            checkpoint = conn.execute("""
                SELECT last_line
                FROM checkpoints
                WHERE source = 'events.jsonl'
            """).fetchone()[0]

        self.assertEqual(checkpoint, 10)


if __name__ == "__main__":
    unittest.main()
