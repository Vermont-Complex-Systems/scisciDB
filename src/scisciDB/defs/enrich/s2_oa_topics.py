"""
Asset: s2_oa_topics

Enriches s2_papers with OpenAlex topics, concepts, and primary_topic
by joining through papersLookup. Incremental by default — only updates
papers that don't yet have oa_topics. Set force_update=True to re-enrich all.
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


class S2OaTopicsConfig(dg.Config):
    force_update: bool = False


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="enrich",
    deps=["papers_lookup"],
    description=(
        "Add oa_topics, oa_concepts, and oa_primary_topic columns to s2_papers "
        "via papersLookup ↔ oa_works join. Incremental by default; "
        "set force_update=True in the launchpad to re-enrich all papers."
    ),
)
def s2_oa_topics(
    context: dg.AssetExecutionContext,
    config: S2OaTopicsConfig,
) -> dg.MaterializeResult:
    metadata_path = _DEFAULT_METADATA_PATH
    sciscidb_data_root = os.environ.get("SCISCIDB_DATA_ROOT", "")

    where_clause = "" if config.force_update else "AND s2.oa_topics IS NULL"
    mode = "force_update" if config.force_update else "incremental"

    if config.force_update:
        context.log.warning("force_update=True: re-enriching ALL s2_papers (expensive)")
    else:
        context.log.info("Incremental mode: only updating papers without oa_topics")

    conn = duckdb.connect()
    try:
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

        result = conn.execute(f"""
            UPDATE s2_papers AS s2
            SET
                oa_topics        = oa.topics,
                oa_concepts      = oa.concepts,
                oa_primary_topic = oa.primary_topic
            FROM papersLookup lookup
            JOIN oa_works oa ON lookup.oa_id = oa.id
            WHERE s2.corpusid = lookup.corpusid
              {where_clause}
            RETURNING s2.corpusid
        """)
        updated = len(result.fetchall())

        total = conn.execute("SELECT COUNT(*) FROM s2_papers").fetchone()[0]
        enriched = conn.execute(
            "SELECT COUNT(*) FROM s2_papers WHERE oa_topics IS NOT NULL"
        ).fetchone()[0]

        context.log.info(
            f"Updated {updated:,} papers — "
            f"{enriched:,}/{total:,} enriched ({enriched/total*100:.1f}%)"
        )

        return dg.MaterializeResult(metadata={
            "papers_updated": updated,
            "total_enriched": enriched,
            "total_s2_papers": total,
            "mode": mode,
        })
    finally:
        conn.close()
