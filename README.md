# Event Pipeline — Reliable Micro-Batch Processing

A Python and SQLite event-processing pipeline designed to simulate continuous event ingestion in micro-batches, validate incoming records, handle duplicates and late-arriving events, maintain checkpoints, and produce reconciled aggregate reports.

## Project Overview

Real-time systems need to process incoming events reliably, even when records are invalid, duplicated, delayed, or a previous run fails.

This project demonstrates a micro-batch processing workflow with:

- JSONL-based event ingestion
- Schema and business-rule validation
- Idempotent event insertion and deduplication
- Checkpoint-based restart and recovery
- Late-arriving event detection using a 24-hour threshold
- Rejected-record tracking
- Backfill processing with a separate checkpoint
- Transactional batch processing
- Aggregate reporting and reconciliation
- Automated unit, streaming, and integration tests

## Architecture

```text
JSONL Event Source
       |
       v
Read Events in Micro-Batches
       |
       v
Parse JSON and Validate Schema
       |
       +------ Invalid ------> Rejected Events
       |
       v
Deduplicate by Event ID
       |
       v
Detect Late-Arriving Events
       |
       v
SQLite Transaction
       |
       +------ Events Table
       +------ Late Events Table
       +------ Checkpoint Table
       |
       v
Aggregate Report
       |
       v
Reconciliation and Run Metrics
```

## Technology Stack

- **Python** — event processing and validation
- **SQLite** — persistent storage, transactions, checkpoints, and aggregates
- **JSONL** — line-delimited event input
- **unittest** — automated testing
- **Git and GitHub** — version control and project delivery

## Repository Structure

```text
Event_Pipeline/
├── data/
│   ├── events.jsonl
│   └── test_events.jsonl
├── tests/
│   ├── test_integration.py
│   ├── test_pipeline.py
│   └── test_streaming.py
├── generate_events.py
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
```

Generated benchmark datasets and SQLite database files are excluded from version control.

## Event Schema

Each input line must contain a JSON object with the following fields:

| Field | Type | Description |
|---|---|---|
| `event_id` | String | Unique event identifier |
| `event_type` | String | Type of event |
| `entity_id` | String | Associated business entity |
| `event_timestamp` | ISO 8601 string | Time of the event, including timezone |
| `amount` | Number | Finite, non-negative event amount |

Records with missing or unexpected fields, invalid timestamps, invalid amounts, or malformed JSON are rejected.

## Setup

Python 3.9 or a compatible newer version is recommended.

Clone the repository:

```bash
git clone https://github.com/SusmithaRayapati/Event_Pipeline.git
cd Event_Pipeline
```

Create and activate a virtual environment:

**Windows PowerShell**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies, if any:

```powershell
pip install -r requirements.txt
```

## Run the Pipeline

Run with the default input and database settings:

```powershell
python main.py
```

The default input is `data/events.jsonl`, and the default database is `data/stream.db`.

Configure a separate input, database, or batch size with environment variables:

```powershell
$env:PIPELINE_INPUT="data/test_events.jsonl"
$env:PIPELINE_DB="data/test_run.db"
$env:PIPELINE_BATCH_SIZE="100"
python main.py
```

The pipeline reports inserted events, duplicates, rejected records, late events, checkpoint position, aggregate totals, and reconciliation status.

## Generate Synthetic Events

Generate a repeatable dataset for testing:

```powershell
python generate_events.py --count 1000 --output data/generated_test_events.jsonl
```

The generator supports deterministic output through a random seed and can include edge cases such as duplicates, invalid records, and late-arriving events.

Run it using a separate database:

```powershell
$env:PIPELINE_INPUT="data/generated_test_events.jsonl"
$env:PIPELINE_DB="data/generated_test.db"
$env:PIPELINE_BATCH_SIZE="100"
python main.py
```

## Recovery and Backfill

The pipeline stores a checkpoint for the input source so completed lines can be skipped on a subsequent run.

```powershell
python main.py
```

To run in backfill mode:

```powershell
python main.py --backfill
```

Backfill mode uses a separate checkpoint namespace. Event IDs remain globally deduplicated in the events table.

## Run Automated Tests

Run the full test suite:

```powershell
python -m unittest discover -s tests -v
```

**Latest verified result: 31 tests passed.**

Coverage includes event validation, duplicate handling, timestamp normalization, late-event detection, checkpoint behavior, transaction rollback, rejected-record retry behavior, backfill, recovery, and integration scenarios.

## Performance Benchmark

A local benchmark was run using a generated JSONL dataset and a SQLite database.

| Metric | Observed result |
|---|---:|
| Input records | 100,004 |
| Batch size | 1,000 |
| Measured runtime | Approximately 7.60 seconds |
| Approximate throughput | 13,160 records/second |

These figures are from one local run and are not a universal performance guarantee. Actual performance depends on the machine, storage, database state, and input data.

## Reliability and Data Quality

- **Deduplication:** a primary key on `event_id` prevents duplicate stored events.
- **Checkpointing:** completed input lines are tracked for restart recovery.
- **Transactional batches:** event writes and checkpoint updates are committed together.
- **Schema validation:** malformed and invalid records are recorded separately.
- **Late-event handling:** events arriving more than 24 hours behind the latest observed event timestamp are identified.
- **Reconciliation:** stored event counts and distinct event IDs are compared before reporting success.

## Limitations and Future Improvements

- SQLite is suitable for this demonstration but is not a distributed streaming platform.
- Input ingestion uses a JSONL file rather than a live message broker.
- The benchmark is a local measurement, not a controlled performance study.
- Future improvements could include Kafka integration, a streaming framework, configurable event-time watermarks, CI automation, and monitoring dashboards.

## AI Assistance Disclosure

AI tools were used to assist with development tasks such as debugging, test design, and documentation. The implementation was reviewed and the automated test suite was executed locally. The project author remains responsible for understanding the code, verifying the results, and explaining the design decisions.

## Author

**Susmitha Rayapati**

GitHub: https://github.com/SusmithaRayapati

Repository: https://github.com/SusmithaRayapati/Event_Pipeline
