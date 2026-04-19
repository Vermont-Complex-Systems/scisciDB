"""
Payload for oa_works_deduped asset.

Creates a deduplicated view of oa_works in DuckLake, keeping the best record
per DOI.  Just DDL — the view is lazy so no data is scanned at creation time.
Must run on VACC because the catalog lives on gpfs.
"""
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

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")

        context.log.info("Creating oa_works_deduped view...")
        t0 = time.time()

        conn.execute("DROP VIEW IF EXISTS oa_works_deduped")
        conn.execute("""
            CREATE VIEW oa_works_deduped AS
            WITH ranked AS (
                SELECT id,
                    ROW_NUMBER() OVER (
                        PARTITION BY doi
                        ORDER BY
                            cited_by_count DESC NULLS LAST,
                            len(authorships) DESC NULLS LAST,
                            (abstract_inverted_index IS NOT NULL) DESC,
                            len(topics) DESC NULLS LAST,
                            id
                    ) AS rn
                FROM oa_works
                WHERE doi IS NOT NULL
            )
            SELECT w.*
            FROM oa_works w
            JOIN ranked r ON w.id = r.id
            WHERE r.rn = 1
        """)

        elapsed = time.time() - t0
        context.log.info(f"View created in {elapsed:.1f}s")

        # Skip counting through the view — it expands the full window
        # function over 200M+ rows and can OOM on small allocations.
        # The dedup stats were validated interactively (~0.3% of DOIs
        # are duplicates, ~98% of those are pairs).

        context.report_asset_materialization(metadata={
            "elapsed_seconds": round(elapsed, 1),
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
