# Event Streaming Pipeline with Python and SQLite

A lightweight event-processing pipeline built to simulate near-real-time data ingestion using micro-batches. The project focuses on data quality, duplicate handling, checkpointing, late-arriving events, aggregate reporting, and safe reruns.


## Architecture Flow

```text
JSONL Event Source
       |
       v
Read Events in Micro-Batches
       |
       v
Validate Schema and Business Rules
       |
       +---- Invalid Events ----> Rejected Records
       |
       v
Deduplicate Using event_id
       |
       v
Detect Late-Arriving Events
       |
       v
Store Events in SQLite
       |
       v
Update Checkpoint Transactionally
       |
       v
Publish Aggregates and Reconciliation Results
```

The pipeline reads JSONL records in configurable batches, validates each record, stores valid events in SQLite, tracks rejected and late-arriving records, and reports aggregates. Checkpoints support safe reruns of completed input.



## Data Dictionary

| Field / Entity | Type | Purpose |
|---|---|---|
| `event_id` | String | Unique event identifier and deduplication key |
| `event_type` | String | Event category used for aggregation |
| `entity_id` | String | Identifier of the entity associated with an event |
| `event_timestamp` | ISO-8601 timestamp | Time when the event occurred |
| `amount` | Numeric | Non-negative event amount used in aggregates |
| `checkpoints` | SQLite table | Tracks the last processed input line for a source |
| `rejected_events` | SQLite table | Stores invalid records and validation errors |
| `late_events` | SQLite table | Tracks events classified as late |

### Assumptions and Constraints

- `event_id` must be unique for accepted events.
- Required event fields must be present and valid.
- Timestamps must be parseable and include timezone information.
- `amount` must be numeric, finite, and non-negative.
- Events arriving more than 24 hours behind the maximum observed event timestamp are classified as late.
- The current implementation does not define foreign-key relationships between event entities.



## Key Features

- **Micro-batch processing:** Processes events in configurable batches.
- **Data validation:** Checks required fields, timestamps, string fields, and non-negative numeric amounts.
- **Duplicate handling:** Uses event IDs to prevent duplicate records from being stored.
- **Checkpointing:** Persists the last processed input line.
- **Late-event detection:** Uses a 24-hour event-time threshold.
- **Rejected-record tracking:** Records invalid input and validation errors.
- **SQLite storage:** Persists events, checkpoints, rejected records, and late-event information.
- **Aggregate reporting:** Reports event counts and total amounts by event type.
- **Reconciliation:** Compares total stored events with distinct event IDs.
- **Synthetic data generation:** Creates reproducible datasets using a configurable random seed.
- **Automated tests:** Covers validation, duplicates, empty input, checkpoints, late events, and reconciliation.

## Tech Stack

- Python
- SQLite
- JSON / JSONL
- Python `unittest`
- PowerShell for local execution on Windows


## Project Structure

```text
Event_Pipeline/
├── data/
│   ├── events.jsonl
│   └── test_events.jsonl
├── tests/
│   ├── test_integration.py
│   ├── test_pipeline.py
│   └── test_streaming.py
├── .gitignore
├── generate_events.py
├── main.py
├── requirements.txt
└── README.md
```

Generated databases, benchmark datasets, and pipeline output files are kept locally and excluded from Git where appropriate.

## Setup

Ensure Python 3.9 or later is installed.

No third-party dependencies are required for the initial pipeline.

Run the pipeline:

```bash
python main.py
```

Run automated tests:

```bash
python -m unittest discover -s tests -v
```

## Configurable Processing

The pipeline supports environment variables for its input file, SQLite database, and batch size.

PowerShell example:

```powershell
$env:PIPELINE_INPUT = "data/events.jsonl"
$env:PIPELINE_DB = "data/stream.db"
$env:PIPELINE_BATCH_SIZE = "1000"

python main.py
```

Use a separate database when testing a new dataset to keep earlier results intact.

## Large-Dataset Test

Generate 100,000 regular events plus additional records for duplicate, invalid, and late-event scenarios:

```powershell
python generate_events.py --count 100000 --output data/performance_events.jsonl --seed 42
```

Run the performance test:

```powershell
$env:PIPELINE_INPUT = "data/performance_events.jsonl"
$env:PIPELINE_DB = "data/performance_v2.db"
$env:PIPELINE_BATCH_SIZE = "1000"

Measure-Command { python main.py }
```

### Observed Local Results

| Metric | Result |
|---|---:|
| Input records | 100,004 |
| Processing time | Approximately 8.61 seconds |
| Stored unique events | 100,002 |
| Rejected records | 1 |
| Late events recorded | 1 |
| Final checkpoint | 100,004 |

These are results from one local run. Actual performance depends on the machine, database state, and execution environment.

## Run Automated Tests

```powershell
python -m unittest discover -s tests -v
```

Latest recorded test result: **14 tests passed**.

The tests cover event validation, malformed JSON, null and negative amounts, duplicate event IDs, empty input, checkpoint values, late-event detection, and aggregate reconciliation.

## Recovery and Idempotency

The pipeline stores its checkpoint in SQLite and updates it within the batch-processing transaction. This helps keep data writes and recorded progress consistent.

A rerun against an already processed input file was verified to leave the event count and checkpoint unchanged.

For a fresh experiment, use a new database path rather than deleting an existing database containing test results.

## Design Decisions

- **SQLite:** Keeps the project easy to run locally without requiring a separate database server.
- **JSONL:** Supports line-by-line event ingestion and synthetic dataset generation.
- **Batch processing:** Balances processing overhead and memory usage.
- **Event IDs:** Provide a stable key for deduplication.
- **Event-time threshold:** Separates significantly delayed events from ordinary out-of-order events.
- **Reconciliation:** Helps detect inconsistencies in the stored event data.

## Future Improvements

- Add dedicated end-to-end integration tests.
- Test interruption and recovery during a batch.
- Add explicit schema-version handling and schema evolution tests.
- Improve structured logging and pipeline run-status tracking.
- Measure memory usage and benchmark multiple batch sizes.
- Add automated CI testing.

## Learning Outcomes

This project demonstrates practical work with event ingestion, data validation, incremental processing, SQLite transactions, idempotency, checkpointing, late-arriving data, testing, and performance measurement.
