"""
Payload for oa_works asset.

Loads OpenAlex works parquet files into DuckLake, partitioned by publication_year.
"""
import os

from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import ducklake_load
except ImportError:
    from scisciDB.payload_utils import ducklake_load


def main(context):
    oa_data_root = context.get_extra("oa_data_root")
    source_dir = os.path.join(oa_data_root, "works")

    if not os.path.isdir(source_dir):
        raise RuntimeError(
            f"Source directory not found: {source_dir}\n"
            "Raw OpenAlex parquet data lives only on VACC. "
            "Re-run this asset with use_slurm=True in the launchpad."
        )

    duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "110GB"
    duckdb_threads = context.get_extra("duckdb_threads") or 32

    ducklake_load(
        context,
        table_name="oa_works",
        source_glob=f"{source_dir}/**/*.parquet",
        partition_by="publication_year",
        duckdb_settings={
            "memory_limit": duckdb_memory_limit,
            "threads": duckdb_threads,
        },
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
