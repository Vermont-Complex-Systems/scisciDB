"""
Payload for papers_lookup asset.

Creates a DOI-based cross-reference table between S2 papers and OpenAlex works.
Joins s2_papers against oa_works_deduped (view) on DOI.
Rebuilt from scratch on each materialization.
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

    # SLURM scratch dir for DuckDB temp/spill files
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS papersLookup (
                corpusid INT32,
                oa_id VARCHAR,
                publication_year INT16,
                match_method VARCHAR,
                match_confidence FLOAT,
                created_at TIMESTAMP
            )
        """)
        conn.execute("DELETE FROM papersLookup")

        context.log.info("Joining s2_papers with oa_works_deduped on DOI...")
        t0 = time.time()

        conn.execute("""
            INSERT INTO papersLookup
            SELECT
                s2.corpusid::INT32,
                oa.id,
                oa.publication_year::INT16,
                'doi'               AS match_method,
                1.0                 AS match_confidence,
                CURRENT_TIMESTAMP   AS created_at
            FROM s2_papers s2
            JOIN oa_works_deduped oa
                ON s2.externalids.DOI = REPLACE(oa.doi, 'https://doi.org/', '')
            WHERE s2.externalids.DOI IS NOT NULL
              AND oa.doi IS NOT NULL
              AND oa.publication_year BETWEEN 1900 AND 2025
        """)

        elapsed = time.time() - t0
        count = conn.execute("SELECT COUNT(*) FROM papersLookup").fetchone()[0]

        context.log.info(
            f"papersLookup: {count:,} S2 <-> OA matches via DOI ({elapsed:.1f}s)"
        )

        context.report_asset_materialization(metadata={
            "matched_pairs": count,
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
