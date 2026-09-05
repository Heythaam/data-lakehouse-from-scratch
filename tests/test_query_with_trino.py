import query_with_trino as mod


class FakeCursor:
    def __init__(self, rows, description):
        self._rows = rows
        self.description = description
        self.executed_sql = None

    def execute(self, sql):
        self.executed_sql = sql

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_run_query_executes_sql_and_reports_elapsed_time(capsys):
    cursor = FakeCursor(
        rows=[[1, 18.37], [2, 22.02]],
        description=[("month",), ("avg_fare",)],
    )
    conn = FakeConnection(cursor)

    elapsed = mod.run_query(conn, "Test title", "SELECT 1")

    assert isinstance(elapsed, float)
    assert elapsed >= 0
    assert cursor.executed_sql == "SELECT 1"

    out = capsys.readouterr().out
    assert "Test title" in out
    assert "Execution time:" in out


def test_queries_dict_is_well_formed():
    assert len(mod.QUERIES) == 3
    for title, sql in mod.QUERIES.items():
        assert isinstance(title, str) and title
        assert "yellow_trips" in sql
