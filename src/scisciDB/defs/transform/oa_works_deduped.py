"""
Asset: oa_works_deduped

Creates a deduplicated view of oa_works, keeping the best record per DOI.
Runs locally (just a view definition in DuckLake — no data is moved).
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
    deps=["oa_works"],
    description=(
        "Deduplicated view of oa_works: one record per DOI, ranked by "
        "Crossref > most concepts > most topics > ID."
    ),
)
def oa_works_deduped(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    metadata_path = _DEFAULT_METADATA_PATH
    sciscidb_data_root = os.environ.get("SCISCIDB_DATA_ROOT", "")

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")

        conn.execute("DROP VIEW IF EXISTS oa_works_deduped")
        conn.execute("""
            CREATE VIEW oa_works_deduped AS
            WITH ranked_ids AS (
                SELECT id,
                    ROW_NUMBER() OVER (
                        PARTITION BY doi
                        ORDER BY
                            CASE WHEN doi_registration_agency = 'crossref' THEN 1 ELSE 2 END,
                            concepts_count DESC NULLS LAST,
                            topics_count DESC NULLS LAST,
                            id
                    ) AS rn
                FROM oa_works
                WHERE doi IS NOT NULL
            )
            SELECT w.*
            FROM oa_works w
            JOIN ranked_ids r ON w.id = r.id
            WHERE r.rn = 1
        """)

        original = conn.execute(
            "SELECT COUNT(*) FROM oa_works WHERE doi IS NOT NULL"
        ).fetchone()[0]
        deduped = conn.execute("SELECT COUNT(*) FROM oa_works_deduped").fetchone()[0]
        removed = original - deduped

        context.log.info(
            f"Deduplicated: {original:,} → {deduped:,} (removed {removed:,} duplicates)"
        )

        return dg.MaterializeResult(metadata={
            "original_rows": original,
            "deduped_rows": deduped,
            "duplicates_removed": removed,
        })
    finally:
        conn.close()
