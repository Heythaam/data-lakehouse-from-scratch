import os
import time

import pandas as pd
import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "nyc_taxi"

QUERIES = {
    "Query 1: Total trips and average fare per month": """
        SELECT
            month,
            count(*) AS total_trips,
            avg(fare_amount) AS avg_fare
        FROM yellow_trips
        GROUP BY month
        ORDER BY month
    """,
    "Query 2: Top 5 pickup locations by trip count": """
        SELECT
            pulocationid,
            count(*) AS trip_count
        FROM yellow_trips
        GROUP BY pulocationid
        ORDER BY trip_count DESC
        LIMIT 5
    """,
    "Query 3: Average trip distance and duration (minutes) by passenger_count": """
        SELECT
            passenger_count,
            avg(trip_distance) AS avg_trip_distance,
            avg(date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0) AS avg_duration_minutes
        FROM yellow_trips
        WHERE passenger_count BETWEEN 1 AND 6
        GROUP BY passenger_count
        ORDER BY passenger_count
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


def run_query(conn, title, sql):
    print(f"\n{'=' * 80}")
    print(title)
    print("=" * 80)

    cursor = conn.cursor()
    start = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - start

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    print(df.to_string(index=False))
    print(f"\nExecution time: {elapsed:.3f} seconds")
    return elapsed


def main():
    conn = get_connection()
    timings = {}
    for title, sql in QUERIES.items():
        timings[title] = run_query(conn, title, sql)

    print(f"\n{'=' * 80}")
    print("Benchmark summary")
    print("=" * 80)
    summary = pd.DataFrame(
        [{"query": title, "execution_time_seconds": round(t, 3)} for title, t in timings.items()]
    )
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
