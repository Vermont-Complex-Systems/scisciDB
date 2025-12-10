#!/usr/bin/env python3
"""
ENRICH step: Add derived fields and computed columns
Replaces MongoDB aggregation pipelines with DuckDB transformations
"""
import argparse
import duckdb
from pathlib import Path
import logging
import os
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_duckdb_connection():
    """Get DuckDB connection with DuckLake attached and optimized settings."""
    # Create temp directory first
    temp_dir = os.getenv('DUCKDB_TEMP')

    # Connect with temp directory set from the start
    conn = duckdb.connect()

    # Set temp directory FIRST before any operations
    conn.execute(f"PRAGMA temp_directory='{temp_dir}';")

    # Then optimize memory settings for large joins
    conn.execute("PRAGMA memory_limit='40GB';")  # Use most of your 46GB
    conn.execute("PRAGMA max_temp_directory_size='100GB';")  # Increase temp space
    conn.execute("SET threads=8;")  # Reduce threads to save memory
    conn.execute("SET preserve_insertion_order=false;")  # Save memory

    # Attach DuckLake (adjust password as needed)
    conn.execute("""
        ATTACH 'ducklake:postgres:dbname=complex_stories host=localhost user=jstonge1'
        AS scisciDB (DATA_PATH '/netfiles/compethicslab/scisciDB/');
    """)

    conn.execute("USE scisciDB;")
    return conn

def s2_add_oa_topics(conn, force_update=False):
    """Add OpenAlex topics and concepts columns to s2_papers table.

    Args:
        conn: DuckDB connection
        force_update: If True, re-enriches ALL papers (expensive, creates snapshot).
                     If False, only enriches papers without OA topics (incremental).
    """
    if force_update:
        logger.warning("FORCE UPDATE MODE: Re-enriching ALL papers (this is expensive!)")
        logger.warning("This will create a new snapshot for auditability.")
        where_clause = ""
    else:
        logger.info("Incremental enrichment: Only updating papers without OA topics")
        where_clause = "AND s2.oa_topics IS NULL"

    # Begin transaction for atomic update + commit message
    conn.execute("BEGIN;")

    try:
        # Add the new columns with full struct definitions (idempotent)
        conn.execute("""
            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_topics
                STRUCT(id VARCHAR, display_name VARCHAR, subfield STRUCT(id VARCHAR, display_name VARCHAR), field STRUCT(id VARCHAR, display_name VARCHAR), "domain" STRUCT(id VARCHAR, display_name VARCHAR), score DOUBLE)[];

            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_concepts
                STRUCT(id VARCHAR, wikidata VARCHAR, display_name VARCHAR, "level" BIGINT, score DOUBLE)[];

            ALTER TABLE s2_papers ADD COLUMN IF NOT EXISTS oa_primary_topic
                STRUCT(id VARCHAR, display_name VARCHAR, subfield STRUCT(id VARCHAR, display_name VARCHAR), field STRUCT(id VARCHAR, display_name VARCHAR), "domain" STRUCT(id VARCHAR, display_name VARCHAR), score DOUBLE);
        """)

        # Update papers with OA topics
        result = conn.execute(f"""
            UPDATE s2_papers AS s2
            SET
                oa_topics = oa.topics,
                oa_concepts = oa.concepts,
                oa_primary_topic = oa.primary_topic
            FROM (
                SELECT corpusid, oa_id
                FROM papersLookup
                ORDER BY corpusid, oa_id
            ) lookup
            JOIN oa_works oa ON lookup.oa_id = oa.id
            WHERE s2.corpusid = lookup.corpusid
                {where_clause}
            RETURNING s2.corpusid;
        """)

        updated_count = len(result.fetchall())

        # Get enrichment stats
        total_s2 = conn.execute("SELECT COUNT(*) FROM s2_papers").fetchone()[0]
        enriched = conn.execute("SELECT COUNT(*) FROM s2_papers WHERE oa_topics IS NOT NULL").fetchone()[0]

        # Tag this snapshot with commit message
        timestamp = datetime.now().isoformat()
        if force_update:
            commit_msg = "FULL RE-ENRICHMENT: OpenAlex topics for all papers"
            extra_info = f'{{"papers_updated": {updated_count}, "total_enriched": {enriched}, "mode": "force_update", "date": "{timestamp}"}}'
        else:
            commit_msg = "Incremental enrichment: OpenAlex topics"
            extra_info = f'{{"papers_updated": {updated_count}, "total_enriched": {enriched}, "mode": "incremental", "date": "{timestamp}"}}'

        conn.execute(f"""
            CALL scisciDB.set_commit_message(
                'enrichment_bot',
                '{commit_msg}',
                extra_info => '{extra_info}'
            );
        """)

        conn.execute("COMMIT;")

        logger.info(f"Updated {updated_count:,} papers in this run")
        logger.info(f"Total enriched: {enriched:,} out of {total_s2:,} S2 papers ({enriched/total_s2*100:.1f}%)")

        return enriched, total_s2

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Enrichment failed, transaction rolled back: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="ENRICH step: Add derived fields"
    )
    parser.add_argument("--dataset", default="papers", help="Dataset name")
    parser.add_argument("--input", type=Path, default=Path("import"))
    parser.add_argument("--output", type=Path, default=Path("enrich"))
    parser.add_argument("--operation", help="Which enrichment operation to run")
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force re-enrichment of ALL papers (expensive, creates snapshot for auditability)"
    )

    args = parser.parse_args()

    try:
        conn = get_duckdb_connection()

        if args.operation == 'oa_topics':
            logger.info("Adding OpenAlex topics and concepts...")
            s2_add_oa_topics(conn, force_update=args.force_update)

        logger.info("Enrichment completed successfully!")

    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()