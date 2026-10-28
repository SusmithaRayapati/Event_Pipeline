
import os
import json
import math
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone

INPUT_FILE = Path(
    os.getenv("PIPELINE_INPUT", "data/events.jsonl")
)
DB_FILE = Path(
    os.getenv("PIPELINE_DB", "data/stream.db")
)

BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "1000"))
SOURCE_NAME = INPUT_FILE.name
ALLOWED_LATENESS_HOURS = 24


def parse_timestamp(value):
    """Parse a timestamp and normalize it to UTC."""
    if not isinstance(value, str):
        raise ValueError("Timestamp must be a string")

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return parsed.astimezone(timezone.utc)


def validate_event(event):
    if not isinstance(event, dict):
        return ["Record must be a JSON object"]

    required = [
        "event_id",
        "event_type",
        "entity_id",
        "event_timestamp",
        "amount",
    ]

    errors = [
        f"Missing required field: {key}"
        for key in required
        if key not in event or event[key] is None
    ]

    if errors:
        return errors

    for key in ("event_id", "event_type", "entity_id"):
        if not isinstance(event[key], str) or not event[key].strip():
            errors.append(f"{key} must be a non-empty string")

    try:
        parse_timestamp(event["event_timestamp"])
    except (ValueError, TypeError, OverflowError):
        errors.append("Invalid event_timestamp")

    amount = event["amount"]

    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or amount < 0
    ):
        errors.append("amount must be a finite non-negative number")

    return errors


def setup_database():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_FILE) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                amount REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                source TEXT PRIMARY KEY,
                last_line INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rejected_events (
                line_number INTEGER PRIMARY KEY,
                raw_record TEXT,
                errors TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS late_events (
                event_id TEXT PRIMARY KEY,
                event_timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(event_timestamp);

            CREATE INDEX IF NOT EXISTS idx_events_type
            ON events(event_type);
        """)

        conn.execute("""
            INSERT OR IGNORE INTO checkpoints
                (source, last_line)
            VALUES (?, 0)
        """, (SOURCE_NAME,))


def process_and_checkpoint(batch):
    inserted = 0
    duplicates = 0
    rejected = 0
    late_count = 0

    with sqlite3.connect(DB_FILE) as conn:
        try:
            conn.execute("BEGIN")

            max_timestamp = conn.execute(
                "SELECT MAX(event_timestamp) FROM events"
            ).fetchone()[0]

            max_seen = (
                parse_timestamp(max_timestamp)
                if max_timestamp
                else None
            )

            watermark_delay = timedelta(
                hours=ALLOWED_LATENESS_HOURS
            )

            for line_number, raw_line in batch:
                try:
                    event = json.loads(raw_line)
                    errors = validate_event(event)
                except json.JSONDecodeError as error:
                    event = None
                    errors = [f"Invalid JSON: {error.msg}"]

                if errors:
                    conn.execute("""
                        INSERT OR IGNORE INTO rejected_events
                        (line_number, raw_record, errors)
                        VALUES (?, ?, ?)
                    """, (
                        line_number,
                        raw_line.rstrip(),
                        json.dumps(errors),
                    ))
                    rejected += 1
                    continue

                event_time = parse_timestamp(
                    event["event_timestamp"]
                )
                timestamp = event_time.isoformat()

                cursor = conn.execute("""
                    INSERT OR IGNORE INTO events
                    (event_id, event_type, entity_id,
                     event_timestamp, amount)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    event["event_id"],
                    event["event_type"],
                    event["entity_id"],
                    timestamp,
                    event["amount"],
                ))

                if cursor.rowcount == 0:
                    duplicates += 1
                    continue

                inserted += 1

                # Late means older than the allowed-lateness watermark.
                if (
                    max_seen is not None
                    and event_time < max_seen - watermark_delay
                ):
                    conn.execute("""
                        INSERT OR IGNORE INTO late_events
                        (event_id, event_timestamp)
                        VALUES (?, ?)
                    """, (
                        event["event_id"],
                        timestamp,
                    ))
                    late_count += 1

                if max_seen is None or event_time > max_seen:
                    max_seen = event_time

            if batch:
                conn.execute("""
                    UPDATE checkpoints
                    SET last_line = ?
                    WHERE source = ?
                """, (batch[-1][0], SOURCE_NAME))

            conn.commit()

        except Exception:
            conn.rollback()
            raise

    return inserted, duplicates, rejected, late_count


def generate_aggregates():
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT
                event_type,
                COUNT(*) AS event_count,
                ROUND(SUM(amount), 2) AS total_amount
            FROM events
            GROUP BY event_type
            ORDER BY event_type
        """).fetchall()

        total_events = conn.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]

        distinct_ids = conn.execute(
            "SELECT COUNT(DISTINCT event_id) FROM events"
        ).fetchone()[0]

        rejected_count = conn.execute(
            "SELECT COUNT(*) FROM rejected_events"
        ).fetchone()[0]

        late_count = conn.execute(
            "SELECT COUNT(*) FROM late_events"
        ).fetchone()[0]

    print("\nAGGREGATE REPORT")
    print("----------------")

    for event_type, count, amount in rows:
        print(
            f"{event_type}: count={count}, total_amount={amount}"
        )

    print("\nRECONCILIATION REPORT")
    print("---------------------")
    print(f"Total events: {total_events}")
    print(f"Distinct event IDs: {distinct_ids}")
    print(f"Rejected records: {rejected_count}")
    print(f"Late events recorded: {late_count}")

    assert total_events == distinct_ids, (
        "Reconciliation failed: duplicate event IDs found"
    )

    print("Reconciliation passed: all stored event IDs are unique.")


def main():
    if BATCH_SIZE < 1:
        raise ValueError("PIPELINE_BATCH_SIZE must be at least 1")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input: {INPUT_FILE}")

    setup_database()

    with sqlite3.connect(DB_FILE) as conn:
        checkpoint = conn.execute(
            "SELECT last_line FROM checkpoints WHERE source = ?",
            (SOURCE_NAME,),
        ).fetchone()[0]

    totals = [0, 0, 0, 0]
    processed_lines = 0
    batch = []

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if line_number <= checkpoint:
                continue

            batch.append((line_number, line))

            if len(batch) >= BATCH_SIZE:
                result = process_and_checkpoint(batch)

                for index in range(4):
                    totals[index] += result[index]

                processed_lines += len(batch)
                batch = []

        if batch:
            result = process_and_checkpoint(batch)

            for index in range(4):
                totals[index] += result[index]

            processed_lines += len(batch)

    with sqlite3.connect(DB_FILE) as conn:
        total_events = conn.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]

        current_checkpoint = conn.execute(
            "SELECT last_line FROM checkpoints WHERE source = ?",
            (SOURCE_NAME,),
        ).fetchone()[0]

    print("\nSTREAMING PIPELINE REPORT")
    print("-------------------------")
    print(f"Input file: {INPUT_FILE}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Lines processed this run: {processed_lines}")
    print(f"New events inserted: {totals[0]}")
    print(f"Duplicates skipped: {totals[1]}")
    print(f"Rejected records: {totals[2]}")
    print(f"Late events detected this run: {totals[3]}")
    print(f"Checkpoint line: {current_checkpoint}")
    print(f"Total database events: {total_events}")

    generate_aggregates()


if __name__ == "__main__":
    main()
