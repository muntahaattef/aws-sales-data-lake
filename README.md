# AWS Sales Data Lake

A small local-first data lake project that preserves deterministic daily CSV files under `raw/`, validates them into Parquet under Hive-style `curated/sale_date=YYYY-MM-DD/` partitions, and keeps rejected rows under `rejected/`. Upload to a user-owned private Amazon S3 bucket and querying with Amazon Athena are explicit opt-in steps.

## Architecture

```text
seeded daily CSVs ──> lake/raw/
                         |
                 validate + type clean
                    /              \
lake/rejected/*.csv                lake/curated/sale_date=.../*.parquet
                    \              /
                     optional S3 upload -> Glue catalog -> Athena
```

Required fields are `transaction_id`, `sale_date`, `customer_id`, `product_id`, `quantity`, `unit_price`, and `currency`. Curated data adds `revenue`, uses integer quantity and numeric price, and moves `sale_date` into the partition key. Transaction IDs are deduplicated across all files in a run.

S3 storage alone is not a database: it stores objects but does not provide schemas, relational constraints, indexes, transactions, or a SQL execution engine. The Glue Data Catalog supplies table metadata and Athena supplies serverless SQL execution over the S3 objects.

## Local execution

```bash
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
python -m pip install -e '.[dev]'

generate-lake-data --start-date 2025-01-01 --days 3 --rows-per-day 250 --seed 2025
transform-sales-lake --raw-dir lake/raw --curated-dir lake/curated --rejected-dir lake/rejected
upload-sales-lake --local-root lake --bucket example-private-bucket --prefix sales-lake --dry-run
pytest -q --cov=sales_lake --cov-report=term-missing
```

## Actual local and mocked results

Verified on November 5, 2026 using Python 3.12.16:

| Metric | Result |
| --- | ---: |
| Raw files | 3 |
| Input records | 750 |
| Curated records | 722 |
| Rejected records | 28 |
| Dry-run objects discovered | 10 |

The run produced three partitioned Parquet files, three rejected CSV files, metrics, and preserved all three source files. Test result: **5 passed** with **76% total statement coverage**. Tests use local files and mock S3/Athena clients; they do not require credentials and do not contact AWS.

No S3 upload, Glue catalog operation, Athena query, or AWS deployment was performed. Therefore this repository reports no actual AWS object counts, query results, timings, or cloud costs.

## Optional real AWS setup

Use standard boto3 credential discovery: an IAM role, AWS IAM Identity Center, `AWS_PROFILE`, or the normal AWS environment variables. Never put access keys in `.env` or source control.

1. Create or select a private bucket yourself, enable Block Public Access, and choose the region. This project never creates a bucket.
2. Replace `BUCKET_NAME`, `YOUR_PREFIX`, `REGION`, and `ACCOUNT_ID` in [`iam/least-privilege-policy.json`](iam/least-privilege-policy.json), then attach it to the role or user running this project.
3. Confirm bucket access and preview deterministic keys:

```bash
aws sts get-caller-identity
aws s3api get-public-access-block --bucket "$SALES_LAKE_BUCKET"
upload-sales-lake --local-root lake --bucket "$SALES_LAKE_BUCKET" --prefix "$SALES_LAKE_PREFIX" --dry-run
```

4. Upload and verify. The uploader requests S3-managed encryption (`AES256`) and checks every uploaded object's `ContentLength` against its local file size:

```bash
upload-sales-lake --local-root lake --bucket "$SALES_LAKE_BUCKET" --prefix "$SALES_LAKE_PREFIX"
aws s3 ls "s3://$SALES_LAKE_BUCKET/$SALES_LAKE_PREFIX/" --recursive --summarize
```

Size verification detects incomplete transfers; it is not an end-to-end content hash. For stronger production verification, store and compare explicit SHA-256 object metadata or checksums.

## Athena configuration

Create a dedicated private query-results prefix such as `s3://BUCKET_NAME/athena-results/`. Replace the bucket and prefix placeholders in [`athena/ddl.sql`](athena/ddl.sql), then execute its statements individually in the Athena console or CLI:

1. Create the `sales_lake` database.
2. Create the partitioned external table.
3. Run `MSCK REPAIR TABLE sales_lake.curated_sales` after new partition prefixes are uploaded.

For production ingestion, explicit `ALTER TABLE ADD PARTITION` statements or Glue-managed partition registration avoid repeatedly scanning all prefixes. The included [`athena/queries.sql`](athena/queries.sql) contains five analyses: daily revenue, product revenue, repeat customers, daily average transaction revenue, and running revenue.

The optional helper starts one query and requires an explicit results location:

```bash
run-athena-query --database sales_lake \
  --results-s3 "s3://$SALES_LAKE_BUCKET/athena-results/" \
  --sql "SELECT sale_date, SUM(revenue) FROM curated_sales GROUP BY sale_date"
```

## Permissions, cost, and cleanup

The policy template limits S3 access to the selected project and Athena-results prefixes, Athena calls to one workgroup, and Glue access to the `sales_lake` database and tables. If your bucket uses SSE-KMS, add only the necessary KMS key actions and key ARN.

Potential charges include S3 storage and requests, Glue catalog storage/requests, Athena bytes scanned, and retained query-result files. Parquet column pruning, date partitions, compression, selective queries, and lifecycle rules reduce cost. Check current AWS pricing and use a budget before real use.

Preview cleanup first, then remove only the project prefixes when you are certain:

```bash
aws s3 rm "s3://$SALES_LAKE_BUCKET/$SALES_LAKE_PREFIX/" --recursive --dryrun
aws s3 rm "s3://$SALES_LAKE_BUCKET/$SALES_LAKE_PREFIX/" --recursive
aws s3 rm "s3://$SALES_LAKE_BUCKET/athena-results/" --recursive
```

Drop the Athena table and database separately if no longer needed. These commands do not delete the bucket or any local source file.

## Limitations

- The transformer loads each daily file into memory and does not compact small Parquet files.
- Cross-run deduplication requires an external manifest or table; the in-memory set covers one run only.
- Prices use floating-point Parquet values in this portfolio example rather than fixed-precision decimal metadata.
- `MSCK REPAIR TABLE` becomes inefficient with very large partition trees.
- Object-size verification does not prove byte-for-byte equality.

## Interview questions and answers

1. **Why keep raw files?** Immutable source objects support replay, auditing, and correcting transformation logic without asking the producer to resend data.
2. **Why partition by sale date?** Date-filtered Athena queries can skip unrelated prefixes, reducing bytes scanned and cost.
3. **Why use Parquet instead of CSV for curated data?** Parquet preserves types, compresses columns, and lets Athena read only referenced columns.
4. **Why is S3 not a database?** It is object storage; schema, SQL execution, transactions, and integrity guarantees must come from other services or application logic.
5. **How would production deduplication improve?** Record processed-file checksums and transaction keys in a durable catalog or transactional table and make partition publication atomic.
