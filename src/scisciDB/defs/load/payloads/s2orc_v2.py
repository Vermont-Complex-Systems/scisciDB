"""
Payload for s2orc_v2 asset.

Creates a view over S2ORC v2 full-text parsed documents in DuckLake.
Parquet files already have proper struct types from the import/parse step.
Just DDL — no data is scanned at creation time.
"""
import os
import time

import duckdb
from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import collect_resource_usage
except ImportError:
    from scisciDB.payload_utils import collect_resource_usage


def main(context):
    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    s2_data_root = context.get_extra("s2_data_root")

    source_dir = os.path.join(s2_data_root, "s2orc_v2")
    if not os.path.isdir(source_dir):
        raise RuntimeError(f"Source directory not found: {source_dir}")

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")

        context.log.info(f"Creating s2orc_v2 view over {source_dir}...")
        t0 = time.time()

        conn.execute("DROP VIEW IF EXISTS s2orc_v2")
        conn.execute(f"""
            CREATE VIEW s2orc_v2 AS
            SELECT * FROM read_parquet('{source_dir}/**/*.parquet')
        """)

        elapsed = time.time() - t0
        context.log.info(f"View created in {elapsed:.1f}s")

        context.report_asset_materialization(metadata={
            "elapsed_seconds": round(elapsed, 1),
            "source_dir": source_dir,
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
