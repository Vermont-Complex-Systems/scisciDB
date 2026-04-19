"""
Payload for s2_papers asset.

Loads Semantic Scholar papers parquet files into DuckLake, partitioned by year.
"""
import os

import duckdb
from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import ducklake_load, estimate_memory
except ImportError:
    from scisciDB.payload_utils import ducklake_load, estimate_memory


def main(context):
    s2_data_root = context.get_extra("s2_data_root")
    dry_run = context.get_extra("dry_run") or False
    source_dir = os.path.join(s2_data_root, "papers")

    if not os.path.isdir(source_dir):
        raise RuntimeError(f"Source directory not found: {source_dir}")

    if dry_run:
        conn = duckdb.connect()
        try:
            context.report_asset_materialization(
                metadata=estimate_memory(context, source_dir, conn)
            )
        finally:
            conn.close()
        return

    duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "40GB"
    duckdb_threads = context.get_extra("duckdb_threads") or 8

    ducklake_load(
        context,
        table_name="s2_papers",
        source_glob=f"{source_dir}/**/*.parquet",
        partition_by="year",
        duckdb_settings={
            "memory_limit": duckdb_memory_limit,
            "threads": duckdb_threads,
        },
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
