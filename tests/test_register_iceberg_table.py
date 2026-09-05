import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType

import register_iceberg_table as mod


def test_require_env_returns_value(monkeypatch):
    monkeypatch.setenv("X", "y")
    assert mod._require_env("X") == "y"


def test_require_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv("X", raising=False)
    with pytest.raises(RuntimeError, match="X"):
        mod._require_env("X")


def test_build_schema_and_partition_spec_adds_month_partition():
    arrow_schema = pa.schema(
        [
            pa.field("tpep_pickup_datetime", pa.timestamp("us")),
            pa.field("fare_amount", pa.float64()),
        ]
    )

    iceberg_schema, partition_spec = mod.build_schema_and_partition_spec(arrow_schema)

    month_field = iceberg_schema.find_field("month")
    assert isinstance(month_field.field_type, IntegerType)

    assert len(partition_spec.fields) == 1
    partition_field = partition_spec.fields[0]
    assert partition_field.name == "month"
    assert partition_field.source_id == month_field.field_id
    assert isinstance(partition_field.transform, IdentityTransform)


def test_load_data_with_month_derives_calendar_month(tmp_path):
    pickup_times = [
        dt.datetime(2023, 1, 15, 10, 0, 0),
        dt.datetime(2023, 6, 3, 8, 30, 0),
        dt.datetime(2023, 12, 31, 23, 59, 59),
    ]
    table = pa.table(
        {
            "tpep_pickup_datetime": pa.array(pickup_times, type=pa.timestamp("us")),
            "fare_amount": pa.array([10.0, 20.0, 30.0]),
        }
    )
    path = tmp_path / "sample.parquet"
    pq.write_table(table, path)

    result = mod.load_data_with_month(str(path))

    assert result.column("month").to_pylist() == [1, 6, 12]
    assert result.num_rows == 3


def test_ensure_namespace_creates_when_missing(capsys):
    class FakeCatalog:
        def create_namespace(self, namespace):
            self.created = namespace

    catalog = FakeCatalog()
    mod.ensure_namespace(catalog, "nyc_taxi")

    assert catalog.created == "nyc_taxi"
    assert "Created namespace: nyc_taxi" in capsys.readouterr().out


def test_ensure_namespace_handles_already_exists(capsys):
    class FakeCatalog:
        def create_namespace(self, namespace):
            raise NamespaceAlreadyExistsError("already there")

    mod.ensure_namespace(FakeCatalog(), "nyc_taxi")

    assert "Namespace already exists: nyc_taxi" in capsys.readouterr().out
