"""
Payload for s2_oa_topics asset.

Enriches s2_papers with OpenAlex topics, concepts, and primary_topic
by joining through papersLookup. Incremental by default — only updates
papers that don't yet have oa_topics.
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

    # SLURM scratch dir for DuckDB temp/spill files
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

    where_clause = "" if force_update else "AND s2.oa_topics IS NULL"
    mode = "force_update" if force_update else "incremental"

    if force_update:
        context.log.warning("force_update=True: re-enriching ALL s2_papers (expensive)")
    else:
        context.log.info("Incremental mode: only updating papers without oa_topics")

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

        # Add columns idempotently
        conn.execute("""
            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_topics
                STRUCT(id VARCHAR, display_name VARCHAR,
                       subfield STRUCT(id VARCHAR, display_name VARCHAR),
                       field   STRUCT(id VARCHAR, display_name VARCHAR),
                       "domain" STRUCT(id VARCHAR, display_name VARCHAR),
                       score DOUBLE)[]
        """)
        conn.execute("""
            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_concepts
                STRUCT(id VARCHAR, wikidata VARCHAR, display_name VARCHAR,
                       "level" BIGINT, score DOUBLE)[]
        """)
        conn.execute("""
            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_primary_topic
                STRUCT(id VARCHAR, display_name VARCHAR,
                       subfield STRUCT(id VARCHAR, display_name VARCHAR),
                       field   STRUCT(id VARCHAR, display_name VARCHAR),
                       "domain" STRUCT(id VARCHAR, display_name VARCHAR),
                       score DOUBLE)
        """)

        context.log.info("Updating s2_papers with OA topics...")
        t0 = time.time()

        result = conn.execute(f"""
            UPDATE s2_papers AS s2
            SET
                oa_topics        = oa.topics,
                oa_concepts      = oa.concepts,
                oa_primary_topic = oa.topics[1]
            FROM papersLookup lookup
            JOIN oa_works oa ON lookup.oa_id = oa.id
            WHERE s2.corpusid = lookup.corpusid
              {where_clause}
            RETURNING s2.corpusid
        """)
        updated = len(result.fetchall())

        elapsed = time.time() - t0
        context.log.info(f"UPDATE completed in {elapsed:.1f}s, {updated:,} papers updated")

        total = conn.execute("SELECT COUNT(*) FROM s2_papers").fetchone()[0]
        enriched = conn.execute(
            "SELECT COUNT(*) FROM s2_papers WHERE oa_topics IS NOT NULL"
        ).fetchone()[0]

        context.log.info(
            f"Enrichment: {enriched:,}/{total:,} ({enriched/total*100:.1f}%) — mode={mode}"
        )

        context.report_asset_materialization(metadata={
            "papers_updated": updated,
            "total_enriched": enriched,
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
