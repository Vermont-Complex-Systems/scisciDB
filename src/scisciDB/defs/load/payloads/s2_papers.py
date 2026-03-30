"""
Payload for s2_papers asset.

Loads Semantic Scholar papers parquet files into DuckLake.
"""
import os

import duckdb
from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import collect_resource_usage, estimate_memory
except ImportError:
    from scisciDB.payload_utils import collect_resource_usage, estimate_memory


def main(context):
    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    s2_data_root = context.get_extra("s2_data_root")
    dry_run = context.get_extra("dry_run") or False

    source_dir = os.path.join(s2_data_root, "papers")

    if not os.path.isdir(source_dir):
        raise RuntimeError(f"Source directory not found: {source_dir}")

    conn = duckdb.connect()
    try:
        if dry_run:
            context.report_asset_materialization(
                metadata=estimate_memory(context, source_dir, conn)
            )
            return

        duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "40GB"
        duckdb_threads = context.get_extra("duckdb_threads") or 8
        conn.execute(f"SET memory_limit = '{duckdb_memory_limit}'")
        conn.execute(f"SET threads = {duckdb_threads}")
        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET enable_progress_bar = true")

        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.execute("DROP TABLE IF EXISTS s2_papers")

        import pathlib
        n_files = len(list(pathlib.Path(source_dir).rglob("*.parquet")))
        context.log.info(f"Creating s2_papers table from {n_files} parquet files...")

        conn.execute(f"""
            CREATE TABLE s2_papers AS
            SELECT * FROM read_parquet('{source_dir}/**/*.parquet')
        """)
        context.log.info("CREATE TABLE completed, running stats...")

        total_rows = conn.execute("SELECT COUNT(*) FROM s2_papers").fetchone()[0]
        context.log.info(f"Loaded {total_rows:,} rows")

        context.report_asset_materialization(metadata={
            "total_rows": total_rows,
            "source_dir": source_dir,
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
