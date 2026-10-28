# Design Decisions and Trade-offs

## 1. Project Overview

The Event Pipeline is a Python-based data processing project that reads event records from JSONL files, validates incoming data, processes records in micro-batches, stores accepted events in SQLite, and generates aggregate results.

The main goals are data quality, duplicate prevention, recoverability, and reproducible testing.

## 2. Technology Choices

### Python

**Decision:** Use Python for the pipeline implementation.

**Reason:** Python provides readable code and useful libraries for JSON processing, validation, database access, and automated testing.

**Trade-off:** Performance may become a limitation for very large workloads without further optimization.

### SQLite

**Decision:** Use SQLite as the storage layer.

**Reason:** It is lightweight, requires no separate database server, and makes the project easy to run locally.

**Trade-off:** SQLite is less suitable for distributed ingestion and workloads requiring extensive concurrent writes.

### JSON Lines (JSONL)

**Decision:** Use one JSON event per line.

**Reason:** JSONL is easy to generate, inspect, and process incrementally.

**Trade-off:** Malformed lines need explicit error handling, and input-file changes can complicate checkpoint recovery.

## 3. Processing Design

### Micro-batch Processing

The pipeline processes input incrementally in batches instead of loading the complete file into memory.

**Benefit:** Reduces memory pressure and supports larger input files.

**Trade-off:** Results become available batch by batch rather than after every individual event.

### Data Validation

Incoming records are checked before they are accepted into the pipeline.

**Benefit:** Helps prevent invalid records from affecting downstream analytics.

**Trade-off:** Validation rules must be maintained as the event schema evolves.

### Deduplication

Event identifiers are used to identify duplicate records.

**Benefit:** Reprocessing the same event should not create another stored copy.

**Trade-off:** This approach depends on stable, unique event identifiers.

### Checkpointing and Recovery

The pipeline records processing progress to support resuming after interruption.

**Benefit:** Previously processed input does not always need to be processed again.

**Trade-off:** Checkpoint updates must remain consistent with database writes. Recovery behaviour needs to be tested under interrupted and partially failed runs.

### Late-arriving Events

The pipeline identifies late events using a configured 24-hour threshold.

**Benefit:** Makes delayed records visible for monitoring and analysis.

**Trade-off:** A fixed threshold may not fit every data source or business requirement.

## 4. Error Handling

Invalid records should be tracked separately with enough information to investigate the failure.

**Benefit:** Data-quality issues remain visible instead of being silently ignored.

**Trade-off:** Rejected-record storage requires additional disk space and monitoring.

Retries must also be designed carefully so that recovering from a failure does not introduce duplicate events.

## 5. Testing Strategy

The project includes unit, streaming, and integration tests.

Testing focuses on validation, duplicate handling, incremental processing, checkpoint recovery, and integration behaviour.

Additional reliability checks should include empty files, malformed JSON, null values, late events, and interrupted processing.

## 6. Performance Considerations

Batch size can affect throughput, memory use, and database-write overhead.

Performance measurements should use reproducible input data and report the record count, batch size, elapsed time, and throughput.

Local benchmark results are specific to the test environment and should not be treated as guaranteed production performance.

## 7. Current Limitations

- The pipeline uses a fixed event schema.
- SQLite limits the suitability of the design for distributed, high-concurrency workloads.
- The late-event threshold may need configuration for different use cases.
- Production deployment would require stronger monitoring, alerting, and operational controls.

## 8. Future Improvements

Possible improvements include schema versioning, configurable validation rules, configurable lateness thresholds, more detailed metrics, additional fault-injection tests, and a scalable database or distributed processing framework.

## 9. Conclusion

The project prioritizes simplicity, reproducibility, data quality, and recoverability. The selected technologies make it straightforward to run and test locally, while the documented trade-offs identify areas for future improvement.
