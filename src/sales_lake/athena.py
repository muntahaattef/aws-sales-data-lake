"""Start one user-selected Athena query; this module does not run automatically."""

from __future__ import annotations

import argparse

import boto3


def start_query(sql: str, database: str, results_s3: str, workgroup: str = "primary") -> str:
    client = boto3.client("athena")
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": results_s3},
        WorkGroup=workgroup,
    )
    return response["QueryExecutionId"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Start an Athena query using standard AWS credential discovery")
    parser.add_argument("--sql", required=True, help="SQL text to execute")
    parser.add_argument("--database", required=True)
    parser.add_argument("--results-s3", required=True, help="s3:// location for Athena results")
    parser.add_argument("--workgroup", default="primary")
    args = parser.parse_args()
    print(start_query(args.sql, args.database, args.results_s3, args.workgroup))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
