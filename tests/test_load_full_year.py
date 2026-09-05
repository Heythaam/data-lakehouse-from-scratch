import pyarrow as pa
import pytest
import requests

import load_full_year as mod
import register_iceberg_table as reg


class FakeResponse:
    def __init__(self, chunks, status_ok=True):
        self._chunks = chunks
        self._status_ok = status_ok

    def raise_for_status(self):
        if not self._status_ok:
            raise requests.exceptions.HTTPError("boom")

    def iter_content(self, chunk_size):
        return iter(self._chunks)


def test_download_month_skips_when_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "RAW_DIR", str(tmp_path))
    dest = tmp_path / "yellow_tripdata_2023-03.parquet"
    dest.write_bytes(b"existing")

    calls = []
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: calls.append(1))

    result = mod.download_month(3)

    assert result == str(dest)
    assert calls == []
    assert dest.read_bytes() == b"existing"


def test_download_month_downloads_and_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "RAW_DIR", str(tmp_path))

    def fake_get(url, stream, timeout):
        assert url == mod.BASE_URL.format(month=4)
        return FakeResponse([b"hello ", b"world"])

    monkeypatch.setattr(mod.requests, "get", fake_get)

    result = mod.download_month(4)
    dest = tmp_path / "yellow_tripdata_2023-04.parquet"

    assert result == str(dest)
    assert dest.read_bytes() == b"hello world"
    assert not (tmp_path / "yellow_tripdata_2023-04.parquet.part").exists()


def test_download_month_raises_on_http_error(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "RAW_DIR", str(tmp_path))
    monkeypatch.setattr(
        mod.requests, "get", lambda *a, **k: FakeResponse([], status_ok=False)
    )

    with pytest.raises(requests.exceptions.HTTPError):
        mod.download_month(5)


def test_align_to_table_schema_fixes_casing_and_type_drift():
    # Mirrors the real January table schema: VendorID/passenger_count/
    # airport_fee, plus the "month" column added at table-creation time.
    target_arrow_schema = pa.schema(
        [
            pa.field("VendorID", pa.int64()),
            pa.field("passenger_count", pa.float64()),
            pa.field("airport_fee", pa.float64()),
        ]
    )
    iceberg_schema, _ = reg.build_schema_and_partition_spec(target_arrow_schema)

    # Mirrors the real February file: "Airport_fee" casing, passenger_count
    # as int64 instead of double, columns in a different order.
    incoming = pa.table(
        {
            "Airport_fee": pa.array([1.5, 2.5], type=pa.float64()),
            "passenger_count": pa.array([1, 2], type=pa.int64()),
            "VendorID": pa.array([1, 2], type=pa.int32()),
            "month": pa.array([2, 2], type=pa.int32()),
        }
    )

    result = mod.align_to_table_schema(incoming, iceberg_schema)

    assert result.schema.names == iceberg_schema.as_arrow().names
    assert result.schema.field("airport_fee").type == pa.float64()
    assert result.column("airport_fee").to_pylist() == [1.5, 2.5]
    assert result.schema.field("passenger_count").type == pa.float64()
    assert result.schema.field("VendorID").type == pa.int64()
    assert result.column("VendorID").to_pylist() == [1, 2]


class FakePartitions:
    def __init__(self, rows):
        self._rows = rows

    def to_pylist(self):
        return self._rows


class FakeInspect:
    def __init__(self, rows):
        self._rows = rows

    def partitions(self):
        return FakePartitions(self._rows)


class FakeTable:
    def __init__(self, rows):
        self.inspect = FakeInspect(rows)


def test_partition_row_counts_aggregates_by_month():
    rows = [
        {"partition": {"month": 1}, "record_count": 100},
        {"partition": {"month": 1}, "record_count": 50},
        {"partition": {"month": 2}, "record_count": 5},
    ]

    counts = mod.partition_row_counts(FakeTable(rows))

    assert counts == {1: 150, 2: 5}
