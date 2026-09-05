import benchmark as mod


class FakeCursor:
    def __init__(self, rows, description, stats):
        self._rows = rows
        self.description = description
        self.stats = stats
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


def test_run_benchmark_reports_rows_scanned_and_returned(capsys):
    cursor = FakeCursor(
        rows=[[38310226]],
        description=[("total_rows",)],
        stats={"processedRows": 38310226},
    )
    conn = FakeConnection(cursor)

    result = mod.run_benchmark(conn, "Full scan", "SELECT count(*) FROM t")

    assert result["benchmark"] == "Full scan"
    assert result["rows_scanned"] == 38310226
    assert result["rows_returned"] == 1
    assert result["execution_time_s"] >= 0

    out = capsys.readouterr().out
    assert "Rows scanned:   38310226" in out
    assert "Rows returned:  1" in out


def test_run_benchmark_handles_multi_row_result(capsys):
    cursor = FakeCursor(
        rows=[[1, "A"], [2, "B"], [3, "C"]],
        description=[("id",), ("label",)],
        stats={"processedRows": 12345},
    )
    conn = FakeConnection(cursor)

    result = mod.run_benchmark(conn, "Multi-row", "SELECT id, label FROM t")

    assert result["rows_returned"] == 3
    assert result["rows_scanned"] == 12345


def test_benchmarks_dict_is_well_formed():
    assert len(mod.BENCHMARKS) == 4
    for title, sql in mod.BENCHMARKS.items():
        assert isinstance(title, str) and title
        assert "yellow_trips" in sql
