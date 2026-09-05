import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

load_dotenv()


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill in your MinIO credentials."
        )
    return value


NESSIE_ICEBERG_REST_URI = os.environ.get(
    "NESSIE_ICEBERG_REST_URI", "http://localhost:19120/iceberg/main"
)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ROOT_USER = _require_env("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = _require_env("MINIO_ROOT_PASSWORD")

LOCAL_FILE = os.path.join("data", "raw", "yellow_tripdata_2023-01.parquet")
NAMESPACE = "nyc_taxi"
TABLE_NAME = "yellow_trips"
IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"


def get_catalog():
    return load_catalog(
        "nessie",
        **{
            "uri": NESSIE_ICEBERG_REST_URI,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ROOT_USER,
            "s3.secret-access-key": MINIO_ROOT_PASSWORD,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def ensure_namespace(catalog, namespace):
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace already exists: {namespace}")


def build_schema_and_partition_spec(arrow_schema):
    month_field = pa.field("month", pa.int32(), nullable=True)
    augmented_schema = arrow_schema.append(month_field)

    # Same conversion Catalog.create_table applies internally to a raw pa.Schema,
    # done explicitly here so we can look up the fresh field-id assigned to "month".
    iceberg_schema = Catalog._convert_schema_if_needed(augmented_schema)
    month_field_id = iceberg_schema.find_field("month").field_id

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=month_field_id,
            field_id=1000,
            transform=IdentityTransform(),
            name="month",
        )
    )
    return iceberg_schema, partition_spec


def load_data_with_month(local_file):
    data = pq.read_table(local_file)
    month = pc.month(data["tpep_pickup_datetime"]).cast(pa.int32())
    return data.append_column("month", month)


def main():
    if not os.path.isfile(LOCAL_FILE):
        raise FileNotFoundError(LOCAL_FILE)

    catalog = get_catalog()
    ensure_namespace(catalog, NAMESPACE)

    if catalog.table_exists(IDENTIFIER):
        print(f"Table already exists: {IDENTIFIER}")
        table = catalog.load_table(IDENTIFIER)
    else:
        arrow_schema = pq.read_schema(LOCAL_FILE)
        iceberg_schema, partition_spec = build_schema_and_partition_spec(arrow_schema)
        table = catalog.create_table(
            IDENTIFIER,
            schema=iceberg_schema,
            partition_spec=partition_spec,
        )
        print(f"Created table: {IDENTIFIER} (partitioned by month)")

    existing_row_count = table.scan().to_arrow().num_rows
    if existing_row_count > 0:
        print("Data already loaded, skipping append.")
    else:
        data = load_data_with_month(LOCAL_FILE)
        table.append(data)
        print(f"Appended {data.num_rows} rows into {IDENTIFIER}")

    table = catalog.load_table(IDENTIFIER)
    print("\nTable schema:")
    print(table.schema())

    row_count = table.scan().to_arrow().num_rows
    print(f"\nTotal row count: {row_count}")


if __name__ == "__main__":
    main()
