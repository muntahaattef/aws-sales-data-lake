from sales_lake import athena


class MockAthenaClient:
    def start_query_execution(self, **kwargs):
        self.arguments = kwargs
        return {"QueryExecutionId": "query-123"}


def test_athena_uses_configured_database_results_and_workgroup(monkeypatch):
    client = MockAthenaClient()
    monkeypatch.setattr(athena.boto3, "client", lambda service: client)
    query_id = athena.start_query("SELECT 1", "sales_lake", "s3://bucket/results/", "analytics")
    assert query_id == "query-123"
    assert client.arguments["QueryExecutionContext"] == {"Database": "sales_lake"}
    assert client.arguments["ResultConfiguration"] == {"OutputLocation": "s3://bucket/results/"}
    assert client.arguments["WorkGroup"] == "analytics"
