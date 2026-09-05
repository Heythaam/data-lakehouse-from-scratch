import json
import subprocess
import sys

import pandas as pd

import benchmark

SPARK_EXEC_COMMAND = [
    "docker",
    "compose",
    "exec",
    "-T",
    "-u",
    "0",
    "-e",
    "HOME=/tmp",
    "spark-master",
    "bash",
    "-c",
    "pip install --quiet pandas python-dotenv && "
    "spark-submit --master spark://spark-master:7077 "
    "--conf spark.jars.ivy=/tmp/.ivy2 /app/benchmark_spark.py",
]

RESULTS_MARKER = "SPARK_BENCHMARK_RESULTS_JSON="


def run_trino_benchmarks():
    print(f"\n{'#' * 80}")
    print("Running Trino benchmarks")
    print("#" * 80)
    conn = benchmark.get_connection()
    return benchmark.run_all_benchmarks(conn)


def run_spark_benchmarks():
    # benchmark_spark.py can't be imported here: PySpark's driver needs a JVM,
    # and this host has none. It runs as `spark-submit` inside the spark-master
    # container instead; its stdout (echoed below) is the same output you'd see
    # running `make benchmark-spark` directly, plus a trailing JSON line this
    # function parses out to build the comparison table.
    print(f"\n{'#' * 80}")
    print("Running Spark benchmarks (inside spark-master container)")
    print("#" * 80)
    proc = subprocess.run(SPARK_EXEC_COMMAND, capture_output=True, text=True)
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError(
            f"Spark benchmark subprocess failed with exit code {proc.returncode}"
        )

    for line in proc.stdout.splitlines():
        if line.startswith(RESULTS_MARKER):
            return json.loads(line[len(RESULTS_MARKER):])

    print(proc.stderr, file=sys.stderr)
    raise RuntimeError("Spark benchmark output did not contain a results marker line")


def _winner(trino_time, spark_time):
    if trino_time is None or spark_time is None:
        return "n/a"
    return "Trino" if trino_time < spark_time else "Spark"


def main():
    trino_results = run_trino_benchmarks()
    spark_results = run_spark_benchmarks()

    rows = []
    for trino_row, spark_row in zip(trino_results, spark_results):
        trino_time = trino_row["execution_time_s"]
        spark_time = spark_row["execution_time_s"]
        rows.append(
            {
                "Query": trino_row["benchmark"].split(" - ")[0],
                "Trino time": f"{trino_time:.3f}s",
                "Spark time": f"{spark_time:.3f}s",
                "Winner": _winner(trino_time, spark_time),
            }
        )

    print(f"\n{'=' * 80}")
    print("Trino vs Spark - side-by-side comparison")
    print("=" * 80)
    comparison = pd.DataFrame(rows)
    print(comparison.to_string(index=False))


if __name__ == "__main__":
    main()
