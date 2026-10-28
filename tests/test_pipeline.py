
import json
import sqlite3
import unittest

from main import validate_event


def valid_event():
    return {
        "event_id": "evt_test_001",
        "event_type": "order_placed",
        "entity_id": "order_001",
        "event_timestamp": "2026-10-28T10:00:00+00:00",
        "amount": 250.0,
    }


class TestEventValidation(unittest.TestCase):

    def test_valid_event(self):
        self.assertEqual(validate_event(valid_event()), [])

    def test_missing_required_field(self):
        event = valid_event()
        del event["event_id"]

        errors = validate_event(event)
        self.assertTrue(any("event_id" in error for error in errors))

    def test_null_amount(self):
        event = valid_event()
        event["amount"] = None
        self.assertTrue(validate_event(event))

    def test_negative_amount(self):
        event = valid_event()
        event["amount"] = -100
        self.assertTrue(
            any("amount" in error for error in validate_event(event))
        )

    def test_invalid_timestamp(self):
        event = valid_event()
        event["event_timestamp"] = "not-a-timestamp"
        self.assertTrue(
            any(
                "event_timestamp" in error
                for error in validate_event(event)
            )
        )

    def test_malformed_json(self):
        with self.assertRaises(json.JSONDecodeError):
            json.loads('{"event_id": ')

    def test_empty_string_fields(self):
        for field in ("event_id", "event_type", "entity_id"):
            with self.subTest(field=field):
                event = valid_event()
                event[field] = " "
                self.assertTrue(validate_event(event))

    def test_timestamp_without_timezone(self):
        event = valid_event()
        event["event_timestamp"] = "2026-10-28T10:00:00"

        self.assertTrue(
            any(
                "event_timestamp" in error
                for error in validate_event(event)
            )
        )

    def test_non_finite_amounts(self):
        for amount in (
            float("nan"),
            float("inf"),
            float("-inf"),
        ):
            with self.subTest(amount=amount):
                event = valid_event()
                event["amount"] = amount

                self.assertTrue(
                    any(
                        "amount" in error
                        for error in validate_event(event)
                    )
                )

    def test_boolean_amount_is_invalid(self):
        event = valid_event()
        event["amount"] = True

        self.assertTrue(
            any(
                "amount" in error
                for error in validate_event(event)
            )
        )

    def test_record_must_be_a_json_object(self):
        self.assertTrue(validate_event([]))
        self.assertTrue(validate_event("not an object"))

    def test_duplicate_primary_key_is_ignored(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("""
                CREATE TABLE events (
                    event_id TEXT PRIMARY KEY
                )
            """)

            conn.execute(
                "INSERT OR IGNORE INTO events VALUES (?)",
                ("evt_test_001",),
            )
            conn.execute(
                "INSERT OR IGNORE INTO events VALUES (?)",
                ("evt_test_001",),
            )

            count = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

            self.assertEqual(count, 1)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
