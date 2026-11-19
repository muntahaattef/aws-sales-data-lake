"""Ingest one NYC TLC yellow taxi release month and optionally verify it in Athena."""
import argparse
import json
import shutil
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SOURCE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
COLUMNS = {
    "tpep_pickup_datetime": pa.timestamp("us"),
    "tpep_dropoff_datetime": pa.timestamp("us"),
    "PULocationID": pa.int32(),
    "DOLocationID": pa.int32(),
    "trip_distance": pa.float64(),
}


def month_range(first, last):
    while first <= last:
        yield first
        first = date(first.year + (first.month == 12), first.month % 12 + 1, 1)


def transform(raw, curated, rejected, first):
    source = pq.ParquetFile(raw)
    following_month = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    start, end = (pa.scalar(datetime.combine(day, datetime.min.time()), pa.timestamp("us"))
                  for day in (first, following_month))
    schema = pa.schema(COLUMNS)
    total = accepted = 0
    with pq.ParquetWriter(curated, schema) as good, pq.ParquetWriter(
        rejected, schema.append(pa.field("reason", pa.string()))
    ) as bad:
        for batch in source.iter_batches(columns=list(COLUMNS), batch_size=100_000):
            rows = pa.Table.from_batches([batch]).select(list(COLUMNS)).cast(schema)
            pickup, dropoff = rows["tpep_pickup_datetime"], rows["tpep_dropoff_datetime"]
            distance = rows["trip_distance"]
            checks = [
                (pc.and_kleene(pc.greater_equal(pickup, start), pc.less(pickup, end)), "pickup_outside_release_month"),
                (pc.greater(dropoff, pickup), "invalid_dropoff_time"),
                (pc.and_kleene(pc.is_finite(distance), pc.greater_equal(distance, 0)), "invalid_distance"),
                (pc.and_kleene(pc.greater(rows["PULocationID"], 0), pc.greater(rows["DOLocationID"], 0)), "invalid_zone"),
            ]
            reasons = pa.nulls(len(rows), type=pa.string())
            for check, reason in reversed(checks):
                reasons = pc.if_else(pc.fill_null(check, False), reasons, pa.scalar(reason))
            valid = pc.is_null(reasons)
            good_rows = rows.filter(valid)
            good.write_table(good_rows)
            bad.write_table(rows.append_column("reason", reasons).filter(pc.invert(valid)))
            total += len(rows)
            accepted += len(good_rows)
    if total != source.metadata.num_rows or accepted == 0:
        raise RuntimeError("Source row count did not reconcile or the batch had no accepted trips")
    return {"source_rows": total, "accepted_rows": accepted, "rejected_rows": total - accepted}


def completed(s3, bucket, key):
    result = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return any(item["Key"] == key for item in result.get("Contents", []))


def verify_athena(args, month, accepted):
    athena = boto3.client("athena")
    query = f"SELECT count(*) FROM yellow_trips WHERE release_month = '{month}'"
    execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": args.database},
        WorkGroup=args.workgroup,
        ResultConfiguration={"OutputLocation": args.results_s3},
    )["QueryExecutionId"]
    for _ in range(60):
        execution = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        status = execution["Status"]
        if status["State"] == "SUCCEEDED":
            result = athena.get_query_results(QueryExecutionId=execution_id)
            value = int(result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
            if value != accepted:
                raise RuntimeError(f"Athena counted {value} trips, expected {accepted}")
            return {
                "athena_query_id": execution_id,
                "athena_rows": value,
                "athena_bytes_scanned": execution["Statistics"]["DataScannedInBytes"],
            }
        if status["State"] in {"FAILED", "CANCELLED"}:
            raise RuntimeError(status.get("StateChangeReason", status["State"]))
        time.sleep(2)
    athena.stop_query_execution(QueryExecutionId=execution_id)
    raise TimeoutError(f"Athena verification timed out for {execution_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", type=date.fromisoformat, help="Release month as YYYY-MM-01")
    parser.add_argument("--through", type=date.fromisoformat, help="Inclusive backfill end")
    parser.add_argument("--output", type=Path, default=Path("tlc-output"))
    parser.add_argument("--source-file", type=Path, help="Use a downloaded file for a single month")
    parser.add_argument("--bucket")
    parser.add_argument("--prefix", default="tlc-yellow")
    parser.add_argument("--database", default="tlc_lake")
    parser.add_argument("--workgroup", default="primary")
    parser.add_argument("--results-s3")
    args = parser.parse_args()
    # Scheduled runs revisit three likely published months and skip verified releases.
    today = datetime.now(timezone.utc).date()
    month_number = today.year * 12 + today.month - 1
    first_year, first_month = divmod(month_number - 5, 12)
    last_year, last_month = divmod(month_number - 3, 12)
    first = args.month or date(first_year, first_month + 1, 1)
    last = args.through or (first if args.month else date(last_year, last_month + 1, 1))
    if first.day != 1 or last.day != 1 or last < first:
        parser.error("Use YYYY-MM-01 dates in ascending order")
    if args.source_file and (first != last or args.bucket):
        parser.error("--source-file is only for a single local release")
    if bool(args.bucket) != bool(args.results_s3):
        parser.error("--bucket and --results-s3 must be set together")
    s3 = boto3.client("s3") if args.bucket else None
    for release in month_range(first, last):
        month = release.strftime("%Y-%m")
        partition = f"release_month={month}"
        prefix = args.prefix.strip("/")
        marker = f"{prefix}/runs/{month}.json"
        if s3 and completed(s3, args.bucket, marker):
            print(f"{month}: already verified")
            continue
        target = args.output / month
        target.mkdir(parents=True, exist_ok=True)
        raw, good, bad = (target / name for name in ("raw.parquet", "curated.parquet", "rejected.parquet"))
        url = SOURCE + month + ".parquet"
        if args.source_file:
            raw = args.source_file
        else:
            request = Request(url, headers={"User-Agent": "tlc-portfolio-ingest/1.0"})
            with urlopen(request, timeout=90) as response, raw.open("wb") as output:
                shutil.copyfileobj(response, output)
        metrics = transform(raw, good, bad, release)
        metrics.update({"release_month": month, "source": url})
        if s3:
            for area, path in (("raw", raw), ("curated", good), ("rejected", bad)):
                s3.upload_file(str(path), args.bucket, f"{prefix}/{area}/{partition}/trips.parquet",
                               ExtraArgs={"ServerSideEncryption": "AES256"})
            metrics.update(verify_athena(args, month, metrics["accepted_rows"]))
        report = json.dumps(metrics, indent=2) + "\n"
        (target / "metrics.json").write_text(report, encoding="utf-8")
        print(report, end="")
        if s3:
            s3.put_object(Bucket=args.bucket, Key=marker, Body=report.encode(), ContentType="application/json",
                          ServerSideEncryption="AES256")


if __name__ == "__main__":
    main()
