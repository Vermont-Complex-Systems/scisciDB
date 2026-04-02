"""
Payload for s2_papers asset.

Loads Semantic Scholar papers parquet files into DuckLake, partitioned by year.
"""
import os
import shutil
import time

import duckdb
from dagster_pipes import open_dagster_pipes

try:
    from payload_utils import collect_resource_usage, estimate_memory
except ImportError:
    from scisciDB.payload_utils import collect_resource_usage, estimate_memory


def main(context):
    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    s2_data_root = context.get_extra("s2_data_root")
    dry_run = context.get_extra("dry_run") or False

    source_dir = os.path.join(s2_data_root, "papers")

    if not os.path.isdir(source_dir):
        raise RuntimeError(f"Source directory not found: {source_dir}")

    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

    conn = duckdb.connect()
    try:
        if dry_run:
            context.report_asset_materialization(
                metadata=estimate_memory(context, source_dir, conn)
            )
            return

        duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "40GB"
        duckdb_threads = context.get_extra("duckdb_threads") or 8
        conn.execute(f"SET temp_directory = '{duckdb_tmp}/'")
        conn.execute(f"SET memory_limit = '{duckdb_memory_limit}'")
        conn.execute(f"SET threads = {duckdb_threads}")
        conn.execute("SET max_temp_directory_size = '500GB'")
        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET enable_progress_bar = true")
        conn.execute("SET partitioned_write_max_open_files = 16")

        test_year = context.get_extra("test_year") or 0

        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.execute("DROP TABLE IF EXISTS s2_papers")

        conn.execute(f"""
            CREATE TABLE s2_papers AS
            SELECT * FROM read_parquet('{source_dir}/**/*.parquet')
            WHERE 1=0
        """)
        conn.execute("ALTER TABLE s2_papers SET PARTITIONED BY (year)")

        year_filter = (
            f"year = {test_year}"
            if test_year
            else "year BETWEEN 1900 AND 2026"
        )
        if test_year:
            context.log.info(f"TEST MODE: inserting only year {test_year}")

        import pathlib
        n_files = len(list(pathlib.Path(source_dir).rglob("*.parquet")))
        context.log.info(f"Inserting s2_papers from {n_files} parquet files...")

        t0 = time.time()
        conn.execute(f"""
            INSERT INTO s2_papers
            SELECT *
            FROM read_parquet('{source_dir}/**/*.parquet')
            WHERE {year_filter}
        """)
        elapsed = time.time() - t0
        context.log.info(f"INSERT completed in {elapsed:.1f}s")
        context.log.info("Running stats...")

        total_rows, years, min_year, max_year = conn.execute("""
            SELECT
                COUNT(*)                AS total_rows,
                COUNT(DISTINCT year)    AS years,
                MIN(year)               AS min_year,
                MAX(year)               AS max_year
            FROM s2_papers
        """).fetchone()

        context.log.info(
            f"Loaded {total_rows:,} rows across {years} years ({min_year}–{max_year})"
        )

        context.report_asset_materialization(metadata={
            "total_rows": total_rows,
            "distinct_years": years,
            "min_year": min_year,
            "max_year": max_year,
            "elapsed_seconds": round(elapsed, 1),
            "test_year": test_year or "all",
            "source_dir": source_dir,
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()
        shutil.rmtree(duckdb_tmp, ignore_errors=True)


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
