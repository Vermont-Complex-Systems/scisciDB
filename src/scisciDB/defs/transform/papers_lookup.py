"""
Asset: papers_lookup

DOI-based cross-reference table between S2 papers (corpusid) and
OpenAlex works (oa_id). Used by downstream enrich assets.
"""
import os
from pathlib import Path

import dagster as dg
import duckdb
from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
_DEFAULT_METADATA_PATH = str(
    Path(os.environ.get("SCISCIDB_METADATA_PATH", str(_PROJECT_ROOT / "metadata.ducklake")))
)


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="transform",
    deps=["s2_papers", "oa_works_deduped"],
    description=(
        "DOI-based lookup table joining S2 papers (corpusid) with OpenAlex works (oa_id). "
        "Uses oa_works_deduped for clean 1-to-1 DOI matches. "
        "Rebuilt from scratch on each materialization."
    ),
)
def papers_lookup(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    metadata_path = _DEFAULT_METADATA_PATH
    sciscidb_data_root = os.environ.get("SCISCIDB_DATA_ROOT", "")

    conn = duckdb.connect()
    try:
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

        count = conn.execute("SELECT COUNT(*) FROM papersLookup").fetchone()[0]
        context.log.info(f"papersLookup: {count:,} S2 ↔ OA matches via DOI")

        return dg.MaterializeResult(metadata={"matched_pairs": count})
    finally:
        conn.close()
