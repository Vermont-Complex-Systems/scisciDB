"""
Payload for oa_sources asset.

Loads OpenAlex sources parquet files into DuckLake.
Excludes is_indexed_in_scopus which has schema conflicts across file vintages.
"""
import os

from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import ducklake_load
except ImportError:
    from scisciDB.payload_utils import ducklake_load


def main(context):
    oa_data_root = context.get_extra("oa_data_root")
    source_dir = os.path.join(oa_data_root, "sources")

    if not os.path.isdir(source_dir):
        raise RuntimeError(f"Source directory not found: {source_dir}")

    duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "7GB"
    duckdb_threads = context.get_extra("duckdb_threads") or 4

    ducklake_load(
        context,
        table_name="oa_sources",
        source_glob=f"{source_dir}/**/*.parquet",
        select_expr="* EXCLUDE(is_indexed_in_scopus)",
        read_parquet_opts=", union_by_name=true",
        duckdb_settings={
            "memory_limit": duckdb_memory_limit,
            "threads": duckdb_threads,
        },
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
