
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


EVENT_TYPES = [
    "order_placed",
    "payment_completed",
    "product_viewed",
]


def make_event(event_id, event_type, entity_id, event_timestamp, amount):
    """Build one event using the pipeline's expected schema."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "entity_id": entity_id,
        "event_timestamp": event_timestamp,
        "amount": amount,
    }


def generate_events(count, output, seed=42, include_edge_cases=True):
    """
    Generate reproducible synthetic JSONL events.

    Edge-case records include:
    - One duplicate event ID written twice
    - One invalid event with a null amount
    - One event older than the allowed lateness window
    """
    if count < 1:
        raise ValueError("Event count must be at least 1.")

    rng = random.Random(seed)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    base_time = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
    total_written = 0

    with output.open("w", encoding="utf-8") as file:

        # 1. Generate regular events.
        # Keep timestamps within a one-hour window.
        for i in range(1, count + 1):
            event_time = base_time - timedelta(
                minutes=rng.randint(0, 60)
            )

            event = make_event(
                event_id=f"evt_{i:07d}",
                event_type=rng.choice(EVENT_TYPES),
                entity_id=f"order_{rng.randint(1, 3000):05d}",
                event_timestamp=event_time.isoformat(),
                amount=round(rng.uniform(100, 5000), 2),
            )

            file.write(json.dumps(event) + "\n")
            total_written += 1

        extra_records = 0

        if include_edge_cases:
            # 2. Write the same event twice.
            # The first copy should be inserted; the second should deduplicate.
            duplicate = make_event(
                event_id="evt_duplicate_001",
                event_type="order_placed",
                entity_id="order_00001",
                event_timestamp=base_time.isoformat(),
                amount=250.00,
            )

            file.write(json.dumps(duplicate) + "\n")
            file.write(json.dumps(duplicate) + "\n")
            total_written += 2
            extra_records += 2

            # 3. Invalid record: amount cannot be null.
            invalid = make_event(
                event_id="evt_invalid_001",
                event_type="order_placed",
                entity_id="order_00002",
                event_timestamp=base_time.isoformat(),
                amount=None,
            )

            file.write(json.dumps(invalid) + "\n")
            total_written += 1
            extra_records += 1

            # 4. Late-arriving event.
            late = make_event(
                event_id="evt_late_001",
                event_type="payment_completed",
                entity_id="order_00003",
                event_timestamp=(
                    base_time - timedelta(days=2)
                ).isoformat(),
                amount=499.99,
            )

            file.write(json.dumps(late) + "\n")
            total_written += 1
            extra_records += 1

    print("\nSynthetic event generation completed")
    print("-------------------------------------")
    print(f"Regular events generated : {count}")
    print(f"Extra edge-case records  : {extra_records}")
    print(f"Total records written    : {total_written}")
    print(f"Output file              : {output.resolve()}")
    print(f"Random seed              : {seed}")
    print(
        "Edge cases enabled      : "
        f"{'Yes' if include_edge_cases else 'No'}"
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate reproducible synthetic streaming events "
            "for the Event Pipeline project."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of regular events to generate (default: 100).",
    )

    parser.add_argument(
        "--output",
        default="data/events.jsonl",
        help="Destination JSONL file (default: data/events.jsonl).",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible output (default: 42).",
    )

    parser.add_argument(
        "--no-edge-cases",
        action="store_true",
        help="Generate regular events only.",
    )

    args = parser.parse_args()

    try:
        generate_events(
            count=args.count,
            output=args.output,
            seed=args.seed,
            include_edge_cases=not args.no_edge_cases,
        )
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
