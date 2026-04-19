"""
Payload for s2_has_fulltext asset.

Adds has_fulltext boolean column to s2_papers by checking whether
corpusid exists in s2orc_v2. Incremental by default.
"""
import os
import shutil
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

    duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "110GB"
    duckdb_threads = context.get_extra("duckdb_threads") or 16
    force_update = context.get_extra("force_update") or False

    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

    mode = "force_update" if force_update else "incremental"

    conn = duckdb.connect()
    try:
        conn.execute(f"SET temp_directory = '{duckdb_tmp}/'")
        conn.execute(f"SET memory_limit = '{duckdb_memory_limit}'")
        conn.execute(f"SET threads = {duckdb_threads}")
        conn.execute("SET max_temp_directory_size = '500GB'")
        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET enable_progress_bar = true")

        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")

        conn.execute("ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS has_fulltext BOOLEAN")

        where_clause = "" if force_update else "AND p.has_fulltext IS NULL"
        if force_update:
            context.log.warning("force_update=True: re-checking ALL s2_papers (expensive)")
        else:
            context.log.info("Incremental mode: only updating papers where has_fulltext IS NULL")

        context.log.info("Updating has_fulltext...")
        t0 = time.time()

        result = conn.execute(f"""
            UPDATE s2_papers AS p
            SET has_fulltext = TRUE
            FROM s2orc_v2 o
            WHERE p.corpusid = o.corpusid
              {where_clause}
            RETURNING p.corpusid
        """)
        updated = len(result.fetchall())

        elapsed = time.time() - t0
        context.log.info(f"has_fulltext: {updated:,} rows set to TRUE in {elapsed:.1f}s")

        # Set remaining NULLs to FALSE
        conn.execute("UPDATE s2_papers SET has_fulltext = FALSE WHERE has_fulltext IS NULL")

        total = conn.execute("SELECT COUNT(*) FROM s2_papers").fetchone()[0]
        n_fulltext = conn.execute(
            "SELECT COUNT(*) FROM s2_papers WHERE has_fulltext = TRUE"
        ).fetchone()[0]

        context.log.info(
            f"has_fulltext: {n_fulltext:,}/{total:,} ({n_fulltext/total*100:.1f}%) — mode={mode}"
        )

        context.report_asset_materialization(metadata={
            "rows_updated": updated,
            "total_with_fulltext": n_fulltext,
            "total_s2_papers": total,
            "mode": mode,
            "elapsed_seconds": round(elapsed, 1),
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()
        shutil.rmtree(duckdb_tmp, ignore_errors=True)


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
