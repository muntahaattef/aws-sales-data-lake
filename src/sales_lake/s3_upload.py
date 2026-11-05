"""Opt-in upload and size verification for an existing private S3 bucket."""

from __future__ import annotations

import argparse
import mimetypes
from pathlib import Path
from typing import Any

import boto3


def upload_tree(local_root: Path, bucket: str, prefix: str, s3_client: Any | None = None, dry_run: bool = False) -> dict[str, int]:
    client = s3_client or boto3.client("s3")  # Standard AWS profile/environment/role discovery.
    normalized_prefix = prefix.strip("/")
    files = sorted(path for path in local_root.rglob("*") if path.is_file() and path.name != ".gitkeep")
    uploaded = verified = 0
    for path in files:
        relative = path.relative_to(local_root).as_posix()
        key = f"{normalized_prefix}/{relative}" if normalized_prefix else relative
        if dry_run:
            print(f"DRY RUN s3://{bucket}/{key}")
            continue
        body = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, ServerSideEncryption="AES256")
        uploaded += 1
        metadata = client.head_object(Bucket=bucket, Key=key)
        if metadata["ContentLength"] != len(body):
            raise RuntimeError(f"size verification failed for s3://{bucket}/{key}")
        verified += 1
    return {"discovered": len(files), "uploaded": uploaded, "verified": verified}


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload local lake prefixes to an existing private S3 bucket")
    parser.add_argument("--local-root", type=Path, default=Path("lake"))
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="sales-lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = upload_tree(args.local_root, args.bucket, args.prefix, dry_run=args.dry_run)
    print(f"discovered={result['discovered']} uploaded={result['uploaded']} verified={result['verified']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

