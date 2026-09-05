import os
import sys
import time

import requests

from register_iceberg_table import IDENTIFIER, get_catalog, load_data_with_month

# Windows consoles default to a non-UTF-8 codepage; PyIceberg's schema-mismatch
# errors render a Unicode table (checkmarks etc.) that would otherwise crash
# the print() in the except block below and mask the real error.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month:02d}.parquet"
RAW_DIR = os.path.join("data", "raw")
MONTHS = range(2, 13)  # February - December

# A handful of rows in every monthly file have a pickup timestamp that falls
# in a different month (bad source data), which lands a few dozen stray rows
# in the "wrong" partition. A real month's file contributes low millions of
# rows, so this threshold safely distinguishes "already loaded" from "a few
# stray rows leaked in from another file".
LOADED_ROW_THRESHOLD = 500_000


def download_month(month):
    dest = os.path.join(RAW_DIR, f"yellow_tripdata_2023-{month:02d}.parquet")
    if os.path.isfile(dest):
        print(f"Month {month:02d}: file already exists, skipping download")
        return dest

    url = BASE_URL.format(month=month)
    print(f"Month {month:02d}: downloading {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    tmp_dest = dest + ".part"
    with open(tmp_dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    os.replace(tmp_dest, dest)
    return dest


def align_to_table_schema(data, table_schema):
    # NYC TLC's monthly files drift in both column-name casing (e.g.
    # "airport_fee" vs "Airport_fee" from Feb 2023 onward) and column types
    # (e.g. passenger_count/RatecodeID as int64 in some months, double in
    # others). PyIceberg's append requires an exact name+type match, so
    # realign the incoming batch to the table's existing schema first.
    name_map = {field.name.lower(): field.name for field in table_schema.fields}
    renamed = [name_map.get(col.lower(), col) for col in data.column_names]
    data = data.rename_columns(renamed)

    target_arrow_schema = table_schema.as_arrow()
    data = data.select(target_arrow_schema.names)
    return data.cast(target_arrow_schema)


def partition_row_counts(table):
    counts = {}
    for row in table.inspect.partitions().to_pylist():
        month = row["partition"]["month"]
        counts[month] = counts.get(month, 0) + row["record_count"]
    return counts


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    start_time = time.perf_counter()

    catalog = get_catalog()
    table = catalog.load_table(IDENTIFIER)

    counts = partition_row_counts(table)
    already_loaded = {m for m, c in counts.items() if c >= LOADED_ROW_THRESHOLD}
    cumulative_total = sum(counts.values())

    files_loaded = 0
    for month in MONTHS:
        try:
            local_file = download_month(month)
        except requests.exceptions.RequestException as e:
            print(f"Month {month:02d}: download failed ({e}), skipping")
            continue

        if month in already_loaded:
            print(f"Month {month:02d}: already loaded, skipping append")
            continue

        try:
            data = load_data_with_month(local_file)
            data = align_to_table_schema(data, table.schema())
            table.append(data)
            cumulative_total += data.num_rows
            files_loaded += 1
            print(
                f"Month {month:02d} loaded: {data.num_rows} rows, "
                f"cumulative total: {cumulative_total} rows"
            )
        except Exception as e:
            print(f"Month {month:02d}: append failed ({e}), skipping")
            continue

    elapsed = time.perf_counter() - start_time
    final_total = sum(partition_row_counts(catalog.load_table(IDENTIFIER)).values())

    print(f"\n{'=' * 80}")
    print("Load summary")
    print("=" * 80)
    print(f"Total rows in table: {final_total}")
    print(f"Total files loaded this run: {files_loaded} / {len(list(MONTHS))}")
    print(f"Total time taken: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
