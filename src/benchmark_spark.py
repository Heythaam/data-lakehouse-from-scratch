import json
import os
import time

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

# Unlike every other script here, this one does not run on the host: PySpark's
# driver needs a local JVM even in client mode against a remote master, and
# this host has no Java installed. Instead it runs as `spark-submit` inside the
# spark-master container (see `make benchmark-spark` / docker-compose.yml's
# ./src:/app mount), so both the driver and the executors resolve MinIO through
# the "minio" service name on the shared Docker network.
SPARK_MINIO_ENDPOINT = os.environ.get("SPARK_MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")

HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_BUNDLE_VERSION = "1.12.262"

NAMESPACE = "nyc_taxi"
TABLE_NAME = "yellow_trips"

BENCHMARKS = {
    "Benchmark 1 - Full scan: COUNT(*) with no filter": """
        SELECT count(*) AS total_rows
        FROM yellow_trips
    """,
    "Benchmark 2 - Partition pruning: COUNT(*) WHERE month = 6": """
        SELECT count(*) AS total_rows
        FROM yellow_trips
        WHERE month = 6
    """,
    "Benchmark 3 - Aggregation: avg fare and distance by month": """
        SELECT
            month,
            avg(fare_amount) AS avg_fare,
            avg(trip_distance) AS avg_distance
        FROM yellow_trips
        GROUP BY month
        ORDER BY month
    """,
    "Benchmark 4 - Heavy aggregation: top 10 pickup locations by trip count": """
        SELECT
            pulocationid,
            count(*) AS trip_count
        FROM yellow_trips
        GROUP BY pulocationid
        ORDER BY trip_count DESC
        LIMIT 10
    """,
}


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill in your MinIO credentials."
        )
    return value


def get_spark_session():
    return (
        SparkSession.builder.appName("nyc-taxi-benchmark-spark")
        .master(SPARK_MASTER_URL)
        .config(
            "spark.jars.packages",
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},"
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VERSION}",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", _require_env("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", _require_env("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def _resolve_table_data_path(spark, bucket, namespace, table_name):
    """PyIceberg/Nessie's REST catalog appends a random UUID to a table's
    storage location (e.g. yellow_trips_f0a50da6-...-8a4407caaa9d) to avoid
    collisions across create/drop cycles — it is not "{namespace}/{table}" and
    is not guessable or safe to hardcode. Trino resolves it through the
    catalog on every query; this benchmark reads Parquet directly with no
    catalog involved, so it has to list the namespace directory itself and
    find it. Uses the Hadoop FileSystem API already wired up by the s3a jars
    (spark.jars.packages), so no extra Python dependency is needed for this.
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(f"s3a://{bucket}/")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    namespace_dir = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{namespace}/")

    candidates = []
    for status in fs.listStatus(namespace_dir):
        if not status.isDirectory():
            continue
        name = status.getPath().getName()
        if name == table_name or name.startswith(f"{table_name}_"):
            candidates.append((status.getModificationTime(), name))

    if not candidates:
        raise RuntimeError(
            f"No table directory found for '{table_name}' under s3a://{bucket}/{namespace}/"
        )
    candidates.sort()
    _, latest_name = candidates[-1]
    return f"s3a://{bucket}/{namespace}/{latest_name}/data"


def load_table(spark):
    data_path = _resolve_table_data_path(spark, MINIO_BUCKET, NAMESPACE, TABLE_NAME)
    print(f"Resolved table data path: {data_path}")

    # recursiveFileLookup disables Spark's Hive-style partition discovery.
    # Without it, Spark reads the "month=6" directory names PyIceberg's
    # identity-partition writer produces and tries to add "month" a second
    # time as an inferred partition column — but "month" already exists as a
    # real column inside the Parquet schema itself, and Spark raises
    # "Found duplicate column(s) in the data schema and the partition schema".
    # The cost: Spark loses directory-level pruning and must list every file
    # under data_path for every query, unlike Trino/Iceberg which skips whole
    # files using manifest statistics before touching storage.
    df = spark.read.option("recursiveFileLookup", "true").parquet(data_path)
    df.createOrReplaceTempView("yellow_trips")
    return df


_AQE_STAGE_NODES = ("ShuffleQueryStageExec", "BroadcastQueryStageExec")


def _walk_scan_metrics(node, metrics):
    class_name = node.getClass().getSimpleName()
    # Adaptive Query Execution (on by default since Spark 3.2) replans at
    # runtime and wraps completed stages in QueryStageExec nodes whose
    # .children() is empty from a plain tree-walk's perspective — the real
    # sub-plan (and, eventually, the FileSourceScanExec leaf with our metrics)
    # is reached only through .plan(), not through the TreeNode API.
    if class_name in _AQE_STAGE_NODES:
        _walk_scan_metrics(node.plan(), metrics)
        return

    children = node.children()
    child_it = children.iterator()
    if not child_it.hasNext():
        leaf_metrics = node.metrics()
        for key in ("numOutputRows", "numFiles"):
            try:
                value = leaf_metrics.apply(key).value()
                metrics[key] = metrics.get(key, 0) + value
            except Exception:
                pass
        return
    while child_it.hasNext():
        _walk_scan_metrics(child_it.next(), metrics)


def _leaf_scan_metrics(result_df):
    """Best-effort read of the physical plan's leaf scan metrics (numOutputRows,
    numFiles) — the closest Spark analog to Trino's cursor.stats["processedRows"].
    Only populated after an action (collect/count) has run."""
    metrics = {}
    try:
        executed_plan = result_df._jdf.queryExecution().executedPlan()
        if executed_plan.getClass().getSimpleName() == "AdaptiveSparkPlanExec":
            executed_plan = executed_plan.executedPlan()
        _walk_scan_metrics(executed_plan, metrics)
    except Exception:
        pass
    return metrics


def run_benchmark(spark, title, sql):
    print(f"\n{'=' * 80}")
    print(title)
    print("=" * 80)
    print(f"Query:{sql}")

    start = time.perf_counter()
    result_df = spark.sql(sql)
    rows = result_df.collect()
    elapsed = time.perf_counter() - start

    metrics = _leaf_scan_metrics(result_df)
    rows_scanned = metrics.get("numOutputRows")
    files_scanned = metrics.get("numFiles")
    rows_returned = len(rows)

    df = pd.DataFrame(rows, columns=result_df.columns)
    print(df.to_string(index=False))
    print(f"\nExecution time: {elapsed:.3f} seconds")
    print(f"Rows scanned:   {rows_scanned}")
    print(f"Files scanned:  {files_scanned}")
    print(f"Rows returned:  {rows_returned}")

    return {
        "benchmark": title,
        "execution_time_s": round(elapsed, 3),
        "rows_scanned": rows_scanned,
        "rows_returned": rows_returned,
    }


def run_all_benchmarks(spark):
    load_table(spark)
    return [run_benchmark(spark, title, sql) for title, sql in BENCHMARKS.items()]


def main():
    spark = get_spark_session()
    try:
        results = run_all_benchmarks(spark)

        print(f"\n{'=' * 80}")
        print("Benchmark summary")
        print("=" * 80)
        summary = pd.DataFrame(results)
        print(summary.to_string(index=False))

        # Machine-readable line for benchmark_comparison.py, which runs on the
        # host and captures this script's stdout via `docker compose exec`
        # rather than importing it (PySpark's driver needs a JVM this host
        # doesn't have — see the SPARK_MASTER_URL comment above).
        print("SPARK_BENCHMARK_RESULTS_JSON=" + json.dumps(results))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
