#!/usr/bin/env python3
"""
LOAD: Create tables and views in DuckLake from parquet data

Loads parquet datasets into DuckLake as queryable tables and views.
Supports both automatic view creation and manual table creation with schema handling.
"""
import argparse
import sys
from pathlib import Path
import duckdb
import os
import re
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

def create_table(conn: duckdb.DuckDBPyConnection, db_name: str, entity: str, is_view: bool = False, cleanup_parquet: bool = True) -> None:
    """Create table or view for dataset in DuckLake"""

    data_root = Path(os.getenv("OA_DATA_ROOT" if db_name == 'openalex' else "S2_DATA_ROOT"))
    dataset_dir = data_root / entity

    # Generate table/view name with database prefix
    if db_name == 'openalex':
        table_name = f"oa_{entity}"
    elif db_name == 's2' and not entity.startswith('s2orc_'):
        table_name = f"s2_{entity}"
    else:
        table_name = entity

    table_name = re.sub("-", "_", table_name)

    object_type = "view" if is_view else "table"
    
    # Drop existing object
    conn.execute(f"DROP VIEW IF EXISTS {table_name};")
    conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Create view or table
    create_stmt = "CREATE VIEW" if is_view else "CREATE TABLE"
    conn.execute(f"""
        {create_stmt} {table_name} AS
        SELECT * FROM read_parquet('{dataset_dir}/**/*.parquet', union_by_name=true);
    """)

    # Verify
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"[LOAD] ✓ {object_type.title()} '{table_name}' created with {count:,} records")

    # Cleanup parquet files for tables only
    if not is_view and cleanup_parquet:
        print(f"[LOAD] Cleaning up parquet files (data preserved in DuckLake)")
        parquet_files = list(dataset_dir.glob("**/*.parquet"))
        if parquet_files:
            for pf in parquet_files:
                pf.unlink()
            print(f"[LOAD] ✓ Removed {len(parquet_files)} parquet files")
        else:
            print(f"[LOAD] No parquet files to clean up")

def create_oa_sources_table(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create oa_sources table from OpenAlex sources parquet files
    
    Uses reverse ordering so most recent schema is used first.
    Excludes is_indexed_in_scopus column which has schema conflicts.
    Always drops and recreates the table.
    """
    print("[LOAD] Creating oa_sources table...")
    
    # Always drop existing table
    conn.execute("DROP TABLE IF EXISTS oa_sources;")
    print("[LOAD] Dropped existing table (if any)")
    
    # Get files in reverse order (newest schema first)
    data_root = os.getenv('OA_DATA_ROOT')
    files = conn.execute(f"SELECT * FROM glob('{data_root}/sources/**/*.parquet')").fetchall()
    files = sorted([x[0] for x in files], reverse=True)
    
    print(f"[LOAD] Loading {len(files)} parquet files...")
    
    conn.execute(f"""
        CREATE TABLE oa_sources AS 
        SELECT * EXCLUDE(is_indexed_in_scopus)
        FROM read_parquet({files});
    """)
    
    # Verify
    count = conn.execute("SELECT COUNT(*) FROM oa_sources;").fetchone()[0]
    print(f"[LOAD] ✓ oa_sources table created with {count:,} records")

def create_oa_works_table(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create partitioned oa_works table from OpenAlex works parquet files
    Partitioned by pub_year and primary_topic for optimized filtering
    """
    print("[LOAD] Creating partitioned oa_works table...")

    # Always drop existing table
    conn.execute("DROP TABLE IF EXISTS oa_works;")

    # Get files in reverse order (newest schema first)
    data_root = os.getenv('OA_DATA_ROOT')
    files = conn.execute(f"SELECT * FROM glob('{data_root}/works/**/*.parquet')").fetchall()
    files = sorted([x[0] for x in files], reverse=True)

    print(f"[LOAD] Loading {len(files)} parquet files...")

    # Step 1: Create empty table with base schema from first file
    print("[LOAD] Creating base table schema...")
    conn.execute(f"""
        CREATE TABLE oa_works AS
        SELECT *
        FROM read_parquet({files}, union_by_name=true)
        WHERE 1=0;
    """)

    # Step 2: Add partition columns
    print("[LOAD] Adding partition columns...")
    conn.execute("""
        ALTER TABLE oa_works ADD COLUMN primary_topic_id VARCHAR;
    """)
    conn.execute("""
        ALTER TABLE oa_works ADD COLUMN primary_topic INTEGER;
    """)

    # Step 3: Set partitioning
    print("[LOAD] Setting partition scheme (publication_year, primary_topic)...")
    conn.execute("""
        ALTER TABLE oa_works SET PARTITIONED BY (publication_year, primary_topic);
    """)

    # Insert data with partition columns
    print("[LOAD] Inserting data into partitioned table...")
    conn.execute(f"""
        INSERT INTO oa_works
        SELECT
            *,
            topics[1].id AS primary_topic_id,
            CAST(regexp_extract(
                topics[1].id,
                'T(\\d{{3}})',
                1
            ) AS INTEGER) AS primary_topic
        FROM read_parquet({files}, union_by_name=true)
        WHERE len(topics) > 0;
    """)

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM oa_works;").fetchone()[0]
    print(f"[LOAD] ✓ oa_works table created with {count:,} records")

def main():
    parser = argparse.ArgumentParser(description="LOAD: Create tables and views in DuckLake from parquet data")

    parser.add_argument('db_name', choices=['openalex', 's2'], help='Database name')
    parser.add_argument('entity', help='Entity name (papers, works, authors, etc.)')
    parser.add_argument('--view', action='store_true', help='Create view instead of table')
    parser.add_argument('--no-cleanup', action='store_true', help='Keep parquet files after table creation (ignored for views)')

    args = parser.parse_args()

    try:
        conn = get_ducklake_connection()

        try:
            cleanup_parquet = not args.no_cleanup
            create_table(conn, args.db_name, args.entity, is_view=args.view, cleanup_parquet=cleanup_parquet)

            # Generate table name for output message
            if args.db_name == 'openalex':
                table_name = f"oa_{args.entity}"
            elif args.db_name == 's2' and not args.entity.startswith('s2orc_'):
                table_name = f"s2_{args.entity}"
            else:
                table_name = args.entity

            object_type = "view" if args.view else "table"
            print(f"\n🎉 SUCCESS!")
            print(f"\n📋 Query the {object_type}:")
            print(f"   SELECT * FROM {table_name} LIMIT 10;")

        finally:
            conn.close()

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()