"""Validate raw sales CSVs and write date-partitioned Parquet plus rejects."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

FIELDS = ("transaction_id", "sale_date", "customer_id", "product_id", "quantity", "unit_price", "currency")


def transform_lake(raw_dir: Path, curated_dir: Path, rejected_dir: Path) -> dict[str, int]:
    input_count = accepted_count = rejected_count = 0
    seen_ids: set[str] = set()
    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"no CSV files found under {raw_dir}")

    for source_path in files:
        source = pd.read_csv(source_path, dtype=str, keep_default_na=False)
        missing = [column for column in FIELDS if column not in source.columns]
        if missing:
            raise ValueError(f"{source_path.name} missing columns: {', '.join(missing)}")
        input_count += len(source)
        dates = pd.to_datetime(source["sale_date"], format="%Y-%m-%d", errors="coerce")
        quantities = pd.to_numeric(source["quantity"], errors="coerce")
        prices = pd.to_numeric(source["unit_price"], errors="coerce")
        blank_ids = source[["transaction_id", "customer_id", "product_id"]].eq("").any(axis=1)
        duplicate_ids = source["transaction_id"].isin(seen_ids) | source["transaction_id"].duplicated(keep="first")
        bad_date = dates.isna()
        bad_quantity = quantities.isna() | quantities.le(0) | quantities.mod(1).ne(0)
        bad_price = prices.isna() | prices.le(0)
        bad_currency = ~source["currency"].str.fullmatch(r"[A-Z]{3}")
        invalid = blank_ids | duplicate_ids | bad_date | bad_quantity | bad_price | bad_currency

        reasons = pd.Series("", index=source.index, dtype="string")
        for mask, reason in (
            (blank_ids, "missing required identifier"), (duplicate_ids, "duplicate transaction_id"),
            (bad_date, "invalid sale_date"), (bad_quantity, "quantity must be a positive integer"),
            (bad_price, "unit_price must be positive"), (bad_currency, "invalid currency"),
        ):
            reasons.loc[mask] = reasons.loc[mask].apply(lambda value: f"{value}; {reason}".strip("; "))

        rejected = source.loc[invalid].copy()
        rejected["rejection_reason"] = reasons.loc[invalid]
        if not rejected.empty:
            rejected_dir.mkdir(parents=True, exist_ok=True)
            rejected.to_csv(rejected_dir / f"{source_path.stem}.rejected.csv", index=False)

        clean = source.loc[~invalid].copy()
        clean["sale_date"] = dates.loc[~invalid].dt.date
        clean["quantity"] = quantities.loc[~invalid].astype("int32")
        clean["unit_price"] = prices.loc[~invalid].round(2)
        clean["revenue"] = (clean["quantity"] * clean["unit_price"]).round(2)
        for sale_date, partition in clean.groupby("sale_date"):
            partition_dir = curated_dir / f"sale_date={sale_date.isoformat()}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            partition.drop(columns="sale_date").to_parquet(partition_dir / f"part-{source_path.stem}.parquet", index=False)

        seen_ids.update(source["transaction_id"].loc[~invalid])
        accepted_count += len(clean)
        rejected_count += len(rejected)

    metrics = {"file_count": len(files), "input_count": input_count, "accepted_count": accepted_count, "rejected_count": rejected_count}
    curated_dir.mkdir(parents=True, exist_ok=True)
    (curated_dir / "_run_metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Transform raw sales CSVs to curated partitioned Parquet")
    parser.add_argument("--raw-dir", type=Path, default=Path("lake/raw"))
    parser.add_argument("--curated-dir", type=Path, default=Path("lake/curated"))
    parser.add_argument("--rejected-dir", type=Path, default=Path("lake/rejected"))
    args = parser.parse_args()
    print(json.dumps(transform_lake(args.raw_dir, args.curated_dir, args.rejected_dir), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

