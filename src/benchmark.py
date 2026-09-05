import os
import time

import pandas as pd
import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "nyc_taxi"

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


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def run_benchmark(conn, title, sql):
    print(f"\n{'=' * 80}")
    print(title)
    print("=" * 80)
    print(f"Query:{sql}")

    cursor = conn.cursor()
    start = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - start

    rows_scanned = cursor.stats.get("processedRows")
    rows_returned = len(rows)

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    print(df.to_string(index=False))
    print(f"\nExecution time: {elapsed:.3f} seconds")
    print(f"Rows scanned:   {rows_scanned}")
    print(f"Rows returned:  {rows_returned}")

    return {
        "benchmark": title,
        "execution_time_s": round(elapsed, 3),
        "rows_scanned": rows_scanned,
        "rows_returned": rows_returned,
    }


def run_all_benchmarks(conn):
    return [run_benchmark(conn, title, sql) for title, sql in BENCHMARKS.items()]


def main():
    conn = get_connection()
    results = run_all_benchmarks(conn)

    print(f"\n{'=' * 80}")
    print("Benchmark summary")
    print("=" * 80)
    summary = pd.DataFrame(results)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
