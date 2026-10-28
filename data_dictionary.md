# Data Dictionary

## Overview

This document describes the event records accepted by the Event Streaming Pipeline and the fields stored in SQLite.

## Event Input Fields

| Field | Data Type | Required | Description | Validation |
|---|---|---|---|---|
| `event_id` | String | Yes | Unique identifier for an event | Must be a non-empty string; duplicate IDs are not inserted again |
| `event_type` | String | Yes | Category or type of event | Must be a non-empty string |
| `entity_id` | String | Yes | Identifier of the entity associated with the event | Must be a non-empty string |
| `event_timestamp` | ISO 8601 timestamp string | Yes | Time when the event occurred | Must contain a timezone; normalized to UTC |
| `amount` | Number | Yes | Numeric value associated with the event | Must be finite and non-negative |

## SQLite Tables

- **`events`** — stores accepted events; `event_id` is the primary key.
- **`checkpoints`** — stores the last processed line for each input source.
- **`rejected_events`** — stores invalid raw records and their validation errors.
- **`late_events`** — records accepted events that fall outside the configured 24-hour lateness threshold.

## Processing Rules

- Input is read as JSON Lines (one JSON record per line).
- Records missing required fields or failing validation are rejected.
- Duplicate event IDs are skipped.
- Timestamps are normalized to UTC.
- Aggregate reports group accepted events by `event_type`, showing event counts and total amounts.
- Reconciliation checks that the total stored event count equals the number of distinct event IDs.

## Notes

The pipeline currently uses a fixed event schema. Schema versioning and schema evolution are future improvements.
