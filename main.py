
import json
import math
import os
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


INPUT_FILE = Path(os.getenv("PIPELINE_INPUT", "data/events.jsonl"))
DB_FILE = Path(os.getenv("PIPELINE_DB", "data/stream.db"))
BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "1000"))
SOURCE_NAME = INPUT_FILE.name
BACKFILL_MODE = "--backfill" in sys.argv
CHECKPOINT_SOURCE = (
    f"{SOURCE_NAME}::backfill" if BACKFILL_MODE else SOURCE_NAME
)
ALLOWED_LATENESS_HOURS = 24

REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "entity_id",
    "event_timestamp",
    "amount",
}
ALLOWED_FIELDS = REQUIRED_FIELDS.copy()


@contextmanager
def db_connection():
    """Open a SQLite connection and always close it."""
    conn = sqlite3.connect(DB_FILE)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_timestamp(value):
    """Parse a timezone-aware timestamp and normalize it to UTC."""
    if not isinstance(value, str):
        raise ValueError("Timestamp must be a string")

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")

    return parsed.astimezone(timezone.utc)


def validate_event(event):
    """Validate event structure, schema and business rules."""
    if not isinstance(event, dict):
        return ["Record must be a JSON object"]

    errors = []

    missing_fields = [
        key for key in REQUIRED_FIELDS
        if key not in event or event[key] is None
    ]
    if missing_fields:
        errors.extend(
            f"Missing required field: {key}"
            for key in sorted(missing_fields)
        )

    unexpected_fields = set(event.keys()) - ALLOWED_FIELDS
    if unexpected_fields:
        errors.append(
            "Unexpected fields: "
            + ", ".join(sorted(unexpected_fields))
        )

    # Validate fields that are present and non-null.
    for key in ("event_id", "event_type", "entity_id"):
        if key in event and event[key] is not None:
            if not isinstance(event[key], str) or not event[key].strip():
                errors.append(f"{key} must be a non-empty string")

    if "event_timestamp" in event and event["event_timestamp"] is not None:
        try:
            parse_timestamp(event["event_timestamp"])
        except (ValueError, TypeError, OverflowError):
            errors.append("Invalid event_timestamp")

    if "amount" in event and event["amount"] is not None:
        amount = event["amount"]
        valid_amount = (
            isinstance(amount, (int, float))
            and not isinstance(amount, bool)
        )

        if valid_amount:
            try:
                valid_amount = math.isfinite(amount) and amount >= 0
            except (OverflowError, TypeError, ValueError):
                valid_amount = False

        if not valid_amount:
            errors.append("amount must be a finite non-negative number")

    return errors


def create_rejected_table(conn):
    conn.execute("""
        CREATE TABLE rejected_events (
            source TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            raw_record TEXT,
            errors TEXT NOT NULL,
            PRIMARY KEY (source, line_number)
        )
    """)


def migrate_rejected_events(conn):
    """Migrate older rejected_events schemas to a source-aware key."""
    exists = conn.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'rejected_events'
    """).fetchone()

    if not exists:
        create_rejected_table(conn)
        return

    info = conn.execute("PRAGMA table_info(rejected_events)").fetchall()
    columns = {row[1] for row in info}
    primary_keys = {
        row[1]: row[5]
        for row in info
        if row[5] > 0
    }

    correct_schema = (
        columns >= {"source", "line_number", "raw_record", "errors"}
        and primary_keys == {"source": 1, "line_number": 2}
    )

    if correct_schema:
        return

    conn.execute("""
        ALTER TABLE rejected_events RENAME TO rejected_events_legacy
    """)
    create_rejected_table(conn)

    legacy_info = conn.execute(
        "PRAGMA table_info(rejected_events_legacy)"
    ).fetchall()
    legacy_columns = {row[1] for row in legacy_info}

    source_expr = "source" if "source" in legacy_columns else "?"
    line_expr = (
        "line_number" if "line_number" in legacy_columns else "NULL"
    )
    raw_expr = (
        "raw_record" if "raw_record" in legacy_columns else "NULL"
    )
    errors_expr = "errors" if "errors" in legacy_columns else "'[]'"

    query = f"""
        INSERT OR IGNORE INTO rejected_events
            (source, line_number, raw_record, errors)
        SELECT {source_expr}, {line_expr}, {raw_expr}, {errors_expr}
        FROM rejected_events_legacy
        WHERE {line_expr} IS NOT NULL
    """

    if source_expr == "?":
        conn.execute(query, (SOURCE_NAME,))
    else:
        conn.execute(query)

    conn.execute("DROP TABLE rejected_events_legacy")



def setup_database():
    """Create pipeline tables and initialize the active checkpoint."""
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Calculate dynamically so tests that patch SOURCE_NAME work correctly.
    checkpoint_source = (
        f"{SOURCE_NAME}::backfill"
        if BACKFILL_MODE
        else SOURCE_NAME
    )

    with db_connection() as conn:
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

            CREATE TABLE IF NOT EXISTS late_events (
                event_id TEXT PRIMARY KEY,
                event_timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(event_timestamp);

            CREATE INDEX IF NOT EXISTS idx_events_type
            ON events(event_type);

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                input_lines INTEGER DEFAULT 0,
                inserted_count INTEGER DEFAULT 0,
                duplicate_count INTEGER DEFAULT 0,
                rejected_count INTEGER DEFAULT 0,
                late_count INTEGER DEFAULT 0,
                failure_reason TEXT
            );
        """)

        migrate_rejected_events(conn)

        conn.execute("""
            INSERT OR IGNORE INTO checkpoints (source, last_line)
            VALUES (?, 0)
        """, (checkpoint_source,))


def process_and_checkpoint(batch, checkpoint_source=None):
    """Process a batch atomically with its checkpoint update."""
    source = checkpoint_source or SOURCE_NAME

    inserted = duplicates = rejected = late_count = 0

    with db_connection() as conn:
        conn.execute("BEGIN")

        max_timestamp = conn.execute(
            "SELECT MAX(event_timestamp) FROM events"
        ).fetchone()[0]

        max_seen = parse_timestamp(max_timestamp) if max_timestamp else None
        watermark_delay = timedelta(hours=ALLOWED_LATENESS_HOURS)

        for line_number, raw_line in batch:
            try:
                event = json.loads(raw_line)
                errors = validate_event(event)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                event = None
                errors = [f"Invalid JSON: {error}"]

            if errors:
                conn.execute("""
                    INSERT OR IGNORE INTO rejected_events
                        (source, line_number, raw_record, errors)
                    VALUES (?, ?, ?, ?)
                """, (
                    source,
                    line_number,
                    raw_line.rstrip("\r\n"),
                    json.dumps(errors),
                ))
                rejected += 1
                continue

            # The validator has already checked the timestamp.
            event_time = parse_timestamp(event["event_timestamp"])
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

            if (
                max_seen is not None
                and event_time < max_seen - watermark_delay
            ):
                late_cursor = conn.execute("""
                    INSERT OR IGNORE INTO late_events
                        (event_id, event_timestamp)
                    VALUES (?, ?)
                """, (event["event_id"], timestamp))

                if late_cursor.rowcount > 0:
                    late_count += 1

            if max_seen is None or event_time > max_seen:
                max_seen = event_time

        if batch:
            conn.execute("""
                UPDATE checkpoints
                SET last_line = MAX(last_line, ?)
                WHERE source = ?
            """, (batch[-1][0], source))

    return inserted, duplicates, rejected, late_count


def generate_aggregates():
    """Print aggregate metrics and reconcile stored event IDs."""
    with db_connection() as conn:
        rows = conn.execute("""
            SELECT event_type, COUNT(*), ROUND(SUM(amount), 2)
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
        print(f"{event_type}: count={count}, total_amount={amount}")

    print("\nRECONCILIATION REPORT")
    print("---------------------")
    print(f"Total events: {total_events}")
    print(f"Distinct event IDs: {distinct_ids}")
    print(f"Rejected records: {rejected_count}")
    print(f"Late events recorded: {late_count}")

    if total_events != distinct_ids:
        raise RuntimeError("Reconciliation failed: duplicate event IDs found")

    print("Reconciliation passed: all stored event IDs are unique.")


def main():
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    totals = [0, 0, 0, 0]
    processed_lines = 0
    status = "failed"
    failure_reason = None

    try:
        setup_database()

        with db_connection() as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                    (run_id, source_file, started_at, status)
                VALUES (?, ?, ?, 'running')
            """, (run_id, str(INPUT_FILE), started_at))

        if BATCH_SIZE < 1:
            raise ValueError("BATCH_SIZE must be at least 1.")

        if not INPUT_FILE.is_file():
            raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

        with db_connection() as conn:
            row = conn.execute(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (CHECKPOINT_SOURCE,),
            ).fetchone()
            checkpoint = row[0] if row else 0

        processed_lines = 0
        batch = []

        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, raw_line in enumerate(file, start=1):
                if line_number <= checkpoint:
                    continue

                processed_lines += 1
                batch.append((line_number, raw_line))

                if len(batch) >= BATCH_SIZE:
                    result = process_and_checkpoint(
                        batch, checkpoint_source=CHECKPOINT_SOURCE
                    )
                    totals = [a + b for a, b in zip(totals, result)]
                    batch = []

        if batch:
            result = process_and_checkpoint(
                batch, checkpoint_source=CHECKPOINT_SOURCE
            )
            totals = [a + b for a, b in zip(totals, result)]

        with db_connection() as conn:
            row = conn.execute(
                "SELECT last_line FROM checkpoints WHERE source = ?",
                (CHECKPOINT_SOURCE,),
            ).fetchone()
            last_checkpoint = row[0] if row else 0
            total_events = conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0]

        generate_aggregates()
        status = "success"

        print(f"\nRun ID: {run_id}")
        print(f"Source file: {INPUT_FILE}")
        print(f"Mode: {'backfill' if BACKFILL_MODE else 'normal'}")
        print(f"Started at: {started_at}")
        print(f"Batch size: {BATCH_SIZE}")
        print(f"Lines processed this run: {processed_lines}")
        print(f"New events inserted: {totals[0]}")
        print(f"Duplicates skipped: {totals[1]}")
        print(f"Rejected records: {totals[2]}")
        print(f"Late events detected this run: {totals[3]}")
        print(f"Last checkpoint: {last_checkpoint}")
        print(f"Total events in database: {total_events}")

    except Exception as error:
        failure_reason = f"{type(error).__name__}: {error}"
        print(f"Pipeline failed: {failure_reason}")
        raise

    finally:
        if DB_FILE.exists():
            try:
                with db_connection() as conn:
                    exists = conn.execute("""
                        SELECT 1 FROM sqlite_master
                        WHERE type = 'table' AND name = 'pipeline_runs'
                    """).fetchone()

                    if exists:
                        conn.execute("""
                            UPDATE pipeline_runs
                            SET ended_at = ?,
                                status = ?,
                                input_lines = ?,
                                inserted_count = ?,
                                duplicate_count = ?,
                                rejected_count = ?,
                                late_count = ?,
                                failure_reason = ?
                            WHERE run_id = ?
                        """, (
                            datetime.now(timezone.utc).isoformat(),
                            status,
                            processed_lines,
                            totals[0],
                            totals[1],
                            totals[2],
                            totals[3],
                            failure_reason,
                            run_id,
                        ))
            except sqlite3.Error as db_error:
                print(f"Could not update run status: {db_error}")

        print(f"Run status: {status}")


if __name__ == "__main__":
    main()
