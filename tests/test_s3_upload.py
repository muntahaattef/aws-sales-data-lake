from sales_lake.s3_upload import upload_tree


class MockS3Client:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]

    def head_object(self, *, Bucket, Key):
        return {"ContentLength": len(self.objects[(Bucket, Key)])}


def test_mocked_s3_upload_uses_deterministic_keys_and_verifies_sizes(tmp_path):
    (tmp_path / "raw").mkdir()
    (tmp_path / "curated" / "sale_date=2025-01-01").mkdir(parents=True)
    (tmp_path / "raw" / "sales.csv").write_text("source", encoding="utf-8")
    (tmp_path / "curated" / "sale_date=2025-01-01" / "part.parquet").write_bytes(b"parquet")
    client = MockS3Client()

    result = upload_tree(tmp_path, "private-bucket", "/sales-lake/", client)

    assert result == {"discovered": 2, "uploaded": 2, "verified": 2}
    assert set(client.objects) == {
        ("private-bucket", "sales-lake/raw/sales.csv"),
        ("private-bucket", "sales-lake/curated/sale_date=2025-01-01/part.parquet"),
    }


def test_dry_run_makes_no_aws_calls(tmp_path):
    (tmp_path / "file.txt").write_text("data", encoding="utf-8")
    client = MockS3Client()
    result = upload_tree(tmp_path, "bucket", "prefix", client, dry_run=True)
    assert result == {"discovered": 1, "uploaded": 0, "verified": 0}
    assert client.objects == {}

