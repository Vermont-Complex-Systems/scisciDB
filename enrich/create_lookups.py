#!/usr/bin/env python3
"""
ENRICH step: Create lookup/mapping tables for cross-database operations
"""
import argparse
import duckdb
from pathlib import Path
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

def create_papers_lookup(conn):
    """Create DOI-based lookup table between S2 papers and OpenAlex works."""
    print("[LOOKUP] Creating papers lookup table (S2 ↔ OpenAlex via DOI)...")

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS papersLookup (
                corpusid INT32,
                oa_id VARCHAR,
                publication_year INT16,
                match_method VARCHAR,
                match_confidence FLOAT,
                created_at TIMESTAMP
            );
        """)

        # Clear existing data
        conn.execute("DELETE FROM papersLookup;")

        conn.execute("""
            INSERT INTO papersLookup
            SELECT
                s2.corpusid::INT32,
                oa.id,
                oa.publication_year::INT16,
                'doi' as match_method,
                1.0 as match_confidence,
                CURRENT_TIMESTAMP as created_at
            FROM s2_papers s2
            JOIN oa_works oa
                ON s2.externalids.DOI = REPLACE(oa.doi, 'https://doi.org/', '')
                AND s2.externalids.DOI IS NOT NULL
                AND oa.doi IS NOT NULL
                AND oa.publication_year BETWEEN 1900 AND 2025;
        """)

        # Get stats
        count = conn.execute("SELECT COUNT(*) FROM papersLookup").fetchone()[0]
        print(f"[LOOKUP] ✓ Created lookup table with {count:,} matches")

    except Exception as e:
        print(f"[LOOKUP] ❌ Error creating papers lookup: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="ENRICH step: Create lookup tables")
    parser.add_argument("--operation",
                       choices=["papers_lookup", "all"],
                       default="all",
                       help="Which lookup operation to run")

    args = parser.parse_args()

    try:
        conn = get_ducklake_connection()

        if args.operation == "papers_lookup":
            create_papers_lookup(conn)
        elif args.operation == "all":
            print("[LOOKUP] Creating all lookup tables...")
            create_papers_lookup(conn)

        print("[LOOKUP] ✓ Lookup creation completed successfully!")

    except Exception as e:
        print(f"[LOOKUP] ❌ Lookup creation failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()