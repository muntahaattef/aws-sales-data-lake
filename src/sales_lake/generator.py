"""Generate deterministic daily sales CSV files with a few known bad rows."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

PRODUCTS = (("P-100", "12.50"), ("P-200", "25.00"), ("P-300", "7.75"), ("P-400", "49.99"))


def generate_daily_files(output_dir: Path, start_date: date, days: int = 3, rows_per_day: int = 250, seed: int = 2025) -> list[Path]:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    sequence = 0
    for day_offset in range(days):
        sale_date = start_date + timedelta(days=day_offset)
        path = output_dir / f"sales_{sale_date:%Y%m%d}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            fields = ("transaction_id", "sale_date", "customer_id", "product_id", "quantity", "unit_price", "currency")
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for _ in range(rows_per_day):
                sequence += 1
                product_id, price = rng.choice(PRODUCTS)
                record: dict[str, object] = {
                    "transaction_id": f"TX-{sequence:07d}",
                    "sale_date": sale_date.isoformat(),
                    "customer_id": f"C-{rng.randint(1, 200):04d}",
                    "product_id": product_id,
                    "quantity": rng.randint(1, 5),
                    "unit_price": price,
                    "currency": "USD",
                }
                if sequence % 97 == 0:
                    record["quantity"] = 0
                if sequence % 101 == 0:
                    record["customer_id"] = ""
                if sequence % 103 == 0:
                    record["unit_price"] = "bad-price"
                if sequence % 107 == 0:
                    record["sale_date"] = "invalid-date"
                writer.writerow(record)
        paths.append(path)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic daily sales CSV files")
    parser.add_argument("--output-dir", type=Path, default=Path("lake/raw"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--rows-per-day", type=int, default=250)
    parser.add_argument("--seed", type=int, default=2025)
    args = parser.parse_args()
    paths = generate_daily_files(args.output_dir, args.start_date, args.days, args.rows_per_day, args.seed)
    print(f"generated {len(paths)} files and {args.days * args.rows_per_day} rows under {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

