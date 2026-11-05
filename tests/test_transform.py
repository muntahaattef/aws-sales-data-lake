from datetime import date

import pandas as pd

from sales_lake.generator import generate_daily_files
from sales_lake.transform import transform_lake


def test_generator_and_partitioned_transform_are_reproducible(tmp_path):
    raw = tmp_path / "lake" / "raw"
    curated = tmp_path / "lake" / "curated"
    rejected = tmp_path / "lake" / "rejected"
    paths = generate_daily_files(raw, date(2025, 1, 1), days=2, rows_per_day=120, seed=7)
    first_bytes = [path.read_bytes() for path in paths]

    metrics = transform_lake(raw, curated, rejected)
    assert metrics == {"file_count": 2, "input_count": 240, "accepted_count": 232, "rejected_count": 8}
    partitions = sorted(curated.glob("sale_date=*/part-*.parquet"))
    assert len(partitions) == 2
    assert sum(len(pd.read_parquet(path)) for path in partitions) == 232
    assert len(list(rejected.glob("*.csv"))) == 2

    generate_daily_files(raw, date(2025, 1, 1), days=2, rows_per_day=120, seed=7)
    assert [path.read_bytes() for path in paths] == first_bytes


def test_transform_rejects_missing_schema(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "bad.csv").write_text("transaction_id,sale_date\nT1,2025-01-01\n", encoding="utf-8")
    try:
        transform_lake(raw, tmp_path / "curated", tmp_path / "rejected")
    except ValueError as exc:
        assert "missing columns" in str(exc)
    else:
        raise AssertionError("missing schema should fail")

