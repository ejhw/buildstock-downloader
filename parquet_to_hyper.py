#!/usr/bin/env python3
"""
Convert all Parquet files under a directory tree into a single Tableau .hyper file.

The script:
  1. Recursively discovers every *.parquet file under the input directory.
  2. Reads them as a unified PyArrow dataset (handles nested Hive-style
     partitioning like state=XX/puma=YYY/).
  3. Streams record batches into a single .hyper file via the Hyper Inserter.

Usage examples:
    # Convert all parquet files under a download tree
    python parquet_to_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3/metadata_and_annual_results_aggregates

    # Specify a custom output file name
    python parquet_to_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3/metadata_and_annual_results_aggregates \\
        --output comstock_annual.hyper

    # Custom table name
    python parquet_to_hyper.py --input-dir ./my_data --table-name annual_data
"""

import argparse
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    Telemetry,
    HyperException,
    TableName,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def discover_parquet_files(input_dir: Path) -> list[Path]:
    """Return a sorted list of .parquet files under *input_dir* (recursive)."""
    return sorted(input_dir.rglob("*.parquet"))


def infer_output_folder(input_dir: Path, parquet_files: list[Path]) -> Path:
    """
    Infer output folder, including aggregation/version subfolders when present.

    Handles two cases:
    - input_dir is the aggregation root (e.g. .../metadata_and_annual_results_aggregates):
      appends <aggregation>/<version>/ from file paths.
    - input_dir already points inside a version folder (e.g. .../by_state_and_puma/full):
      uses input_dir as-is.
    """
    aggregations = {
        "by_state_and_county",
        "by_state_and_puma",
        "by_state",
        "national",
    }
    versions = {"basic", "full"}

    # If input_dir itself already ends with a known version, use it directly.
    if input_dir.name in versions:
        return input_dir
    # Also check if a version already appears anywhere in the path.
    if any(p in versions for p in input_dir.parts):
        return input_dir

    if not parquet_files:
        return input_dir / "full"

    rel_parts = parquet_files[0].relative_to(input_dir).parts
    agg = next((p for p in rel_parts if p in aggregations), None)
    version = next((p for p in rel_parts if p in versions), None)

    out_dir = input_dir
    if agg and agg not in input_dir.parts:
        out_dir = out_dir / agg
    if version:
        out_dir = out_dir / version
    else:
        out_dir = out_dir / "full"
    return out_dir


_WRITE_BATCH_SIZE = 500_000


def _pa_type_to_sql(pa_type: pa.DataType) -> SqlType:
    """Map a PyArrow type to a Hyper SqlType."""
    if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        return SqlType.int()
    elif pa.types.is_int64(pa_type):
        return SqlType.big_int()
    elif pa.types.is_float32(pa_type) or pa.types.is_float64(pa_type):
        return SqlType.double()
    elif pa.types.is_boolean(pa_type):
        return SqlType.bool()
    elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return SqlType.text()
    elif pa.types.is_date(pa_type):
        return SqlType.date()
    elif pa.types.is_timestamp(pa_type):
        return SqlType.timestamp()
    else:
        return SqlType.text()


def _build_table_def(schema: pa.Schema, table_name: str) -> TableDefinition:
    """Create a Hyper TableDefinition from a PyArrow schema."""
    columns = [
        TableDefinition.Column(field.name, _pa_type_to_sql(field.type))
        for field in schema
    ]
    return TableDefinition(TableName("public", table_name), columns)


def _cast_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Cast float32 columns to float64 (Hyper doesn't support float32)."""
    new_arrays = []
    new_fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if pa.types.is_float32(field.type):
            col = col.cast(pa.float64())
            field = field.with_type(pa.float64())
        new_arrays.append(col)
        new_fields.append(field)
    return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))


def parquet_dir_to_hyper(
    input_dir: Path,
    hyper_path: Path,
    table_name: str,
    batch_size: int = _WRITE_BATCH_SIZE,
    state_filter: str | None = None,
) -> int:
    """
    Read all Parquet files under *input_dir* as a PyArrow dataset and
    stream-insert them into a single .hyper file.

    Uses Hive-style partitioning discovery so partition columns
    (state, puma, county, etc.) become regular columns in the output.

    If *state_filter* is given (e.g. "AL"), only rows for that state
    are included.

    Returns the total number of rows inserted.
    """
    hyper_path.parent.mkdir(parents=True, exist_ok=True)

    # Open the dataset – PyArrow will discover the Hive partitioning.
    # exclude_invalid_files=True prevents PyArrow from trying to parse
    # any .hyper (or other non-parquet) files that may sit in the same tree.
    dataset = ds.dataset(
        input_dir,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )

    # Build optional row filter
    row_filter = None
    if state_filter:
        row_filter = ds.field("state") == state_filter

    # Read one batch to discover the unified schema
    scanner = dataset.scanner(batch_size=batch_size, filter=row_filter)
    schema = scanner.projected_schema

    # Cast float32 → float64 in schema for table definition
    cast_fields = []
    for field in schema:
        if pa.types.is_float32(field.type):
            cast_fields.append(field.with_type(pa.float64()))
        else:
            cast_fields.append(field)
    cast_schema = pa.schema(cast_fields)

    table_def = _build_table_def(cast_schema, table_name)
    total_rows = 0
    batches_written = 0

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=str(hyper_path),
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_table(table_def)
            col_names = cast_schema.names

            with Inserter(connection, table_def) as inserter:
                for batch in scanner.to_batches():
                    batch = _cast_batch(batch)
                    num_rows = batch.num_rows
                    # Convert columnar batch to rows for inserter
                    columns = {name: batch.column(name).to_pylist() for name in col_names}
                    rows = [
                        tuple(columns[c][i] for c in col_names)
                        for i in range(num_rows)
                    ]
                    inserter.add_rows(rows)
                    total_rows += num_rows
                    batches_written += 1
                    if batches_written % 10 == 0:
                        print(f"    … {total_rows:,} rows ingested", end="\r", flush=True)
                inserter.execute()

    print(f"    … {total_rows:,} rows ingested")
    return total_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Root folder containing .parquet files (searched recursively)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output .hyper file path "
             "(default: inferred aggregation/version folder under input-dir)",
    )
    parser.add_argument(
        "--table-name",
        default="Extract",
        help='Table name inside the Hyper file (default: "Extract")',
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_WRITE_BATCH_SIZE,
        help=f"Number of rows per insert batch (default: {_WRITE_BATCH_SIZE:,})",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        metavar="XX",
        help="Only include data for this two-letter state code (e.g. --state AL).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        print(f"Error: input directory does not exist: {input_dir}", file=sys.stderr)
        return 1

    table_name = args.table_name
    state_filter = args.state.upper() if args.state else None

    # Discover parquet files
    parquet_files = discover_parquet_files(input_dir)
    if not parquet_files:
        print(f"No .parquet files found under {input_dir}")
        return 1

    # Default output: place in inferred aggregation/version folder, append state if filtered
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_dir = infer_output_folder(input_dir, parquet_files)
        name = input_dir.name
        if state_filter:
            name += f"_{state_filter}"
        output_path = output_dir / f"{name}.hyper"

    print(f"Found {len(parquet_files):,} parquet file(s) under {input_dir}")
    if state_filter:
        print(f"Filtering to state: {state_filter}")
    print(f"Output: {output_path}")
    print()

    t0 = time.monotonic()
    total_rows = parquet_dir_to_hyper(
        input_dir, output_path, table_name, args.batch_size,
        state_filter=state_filter,
    )
    elapsed = time.monotonic() - t0

    print(f"\nDone: {total_rows:,} rows in {_fmt_time(elapsed)}  →  {output_path}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except HyperException as ex:
        print(ex, file=sys.stderr)
        sys.exit(1)