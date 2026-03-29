#!/usr/bin/env python3
"""
TRANSFORM: Create deduplicated views for datasets

Deduplicates tables by creating views that keep only the best record per duplicate key.
"""
import argparse
import sys
import duckdb
import os
from dotenv import load_dotenv
from pyprojroot import here

PROJECT_ROOT = here()

load_dotenv()


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection with DuckLake attached"""
    conn = duckdb.connect()
    conn.execute(f"""
        ATTACH 'ducklake:{PROJECT_ROOT / "metadata.ducklake"}' AS scisciDB
            (DATA_PATH '{os.getenv("SCISCIDB_DATA_ROOT")}');
        """)
    conn.execute("USE scisciDB;")
    return conn


def create_oa_works_deduped(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Create deduplicated OpenAlex works view with best record per DOI.

    Deduplication strategy:
    1. Partition by DOI (groups duplicates)
    2. Rank by: Crossref > most concepts > most topics > ID
    3. Keep only rank 1 (best record)

    Returns:
        Number of duplicate records removed
    """
    print("[TRANSFORM] Creating deduplicated oa_works view...")

    # Drop existing view
    conn.execute("DROP VIEW IF EXISTS oa_works_deduped;")

    # Create deduplicated view
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
                ) as rn
            FROM oa_works
            WHERE doi IS NOT NULL
        )
        SELECT w.*
        FROM oa_works w
        JOIN ranked_ids r ON w.id = r.id
        WHERE r.rn = 1;
    """)

    # Get stats
    original = conn.execute("SELECT COUNT(*) FROM oa_works WHERE doi IS NOT NULL").fetchone()[0]
    deduped = conn.execute("SELECT COUNT(*) FROM oa_works_deduped").fetchone()[0]
    removed = original - deduped

    print(f"[TRANSFORM] ✓ Deduplicated: {original:,} → {deduped:,} (removed {removed:,} duplicates)")

    return removed


def main():
    parser = argparse.ArgumentParser(
        description="TRANSFORM: Create deduplicated views for datasets"
    )
    parser.add_argument(
        'table',
        choices=['oa_works'],
        help='Table to deduplicate'
    )

    args = parser.parse_args()

    try:
        conn = get_ducklake_connection()

        try:
            if args.table == 'oa_works':
                removed = create_oa_works_deduped(conn)

                print(f"\n🎉 SUCCESS!")
                print(f"\n📋 Query the deduplicated view:")
                print(f"   SELECT * FROM oa_works_deduped LIMIT 10;")
                print(f"\n📊 Duplicates removed: {removed:,}")

        finally:
            conn.close()

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()