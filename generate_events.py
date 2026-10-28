
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_events(count, output, seed=42):
    if count < 1:
        raise ValueError("Event count must be at least 1.")

    rng = random.Random(seed)

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    event_types = [
        "order_placed",
        "payment_completed",
        "product_viewed",
    ]

    base_time = datetime(
        2026, 10, 28, 10, 0, tzinfo=timezone.utc
    )

    generated = 0

    with output.open("w", encoding="utf-8") as file:
        for i in range(1, count + 1):
            event_time = base_time - timedelta(
                minutes=rng.randint(0, 60)
            )

            event = {
                "event_id": f"evt_{i:07d}",
                "event_type": rng.choice(event_types),
                "entity_id": f"order_{rng.randint(1, 3000):05d}",
                "event_timestamp": event_time.isoformat(),
                "amount": round(rng.uniform(100, 5000), 2),
            }

            file.write(json.dumps(event) + "\n")
            generated += 1

        # Add a duplicate record for deduplication testing.
        duplicate = {
            "event_id": "evt_duplicate_001",
            "event_type": "order_placed",
            "entity_id": "order_00001",
            "event_timestamp": base_time.isoformat(),
            "amount": 250.00,
        }
        file.write(json.dumps(duplicate) + "\n")
        file.write(json.dumps(duplicate) + "\n")

        # Add an invalid record.
        invalid = {
            "event_id": "evt_invalid_001",
            "event_type": "order_placed",
            "entity_id": "order_00002",
            "event_timestamp": base_time.isoformat(),
            "amount": None,
        }
        file.write(json.dumps(invalid) + "\n")

        # Add a late-arriving event.
        late = {
            "event_id": "evt_late_001",
            "event_type": "payment_completed",
            "entity_id": "order_00003",
            "event_timestamp": (
                base_time - timedelta(days=2)
            ).isoformat(),
            "amount": 499.99,
        }
        file.write(json.dumps(late) + "\n")

    total = generated + 4

    print(f"Regular events generated: {generated}")
    print("Extra test records: 4")
    print(f"Total records written: {total}")
    print(f"Saved to: {output}")
    print(f"Random seed: {seed}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic streaming events."
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
        help="Output JSONL file path.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data.",
    )

    args = parser.parse_args()

    generate_events(
        count=args.count,
        output=args.output,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
