#!/usr/bin/env python3
"""
ENRICH step: Add derived fields and computed columns
Replaces MongoDB aggregation pipelines with DuckDB transformations
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


# def s2_has_abstract(conn):
#     conn.execute("""
#         ALTER TABLE s2_papers ADD COLUMN has_abstract BOOLEAN;
#         UPDATE s2_papers AS p
#         SET has_abstract = EXISTS (
#             SELECT 1 FROM abstracts AS o
#             WHERE o.corpusid = p.corpusid
#         );
#     """)

# def s2_has_fulltext(conn):
#     conn.execute("""
#         ALTER TABLE s2_papers ADD COLUMN has_fulltext BOOLEAN;
#         UPDATE s2_papers AS p
#         SET has_fulltext = EXISTS (
#             SELECT 1 FROM s2orc_v2 AS o
#             WHERE o.corpusid = p.corpusid
#         );
#     """)


def main():
    parser = argparse.ArgumentParser(
        description="ENRICH step: Add derived fields"
    )
    parser.add_argument("--operation",
                       choices=["has_abstract", "has_fulltext"],
                       default="all",
                       help="Which enrichment operation to run")

    args = parser.parse_args()

    try:
        conn = get_ducklake_connection()

        # if args.operation == 'has_abstract':
        #     s2_has_abstract(conn)
        # elif args.operation == 'has_fulltext':
        #     s2_has_fulltext(conn)
        
    except Exception as e:
        print(f"Enrichment failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()