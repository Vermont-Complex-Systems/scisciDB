"""
Payload for oa_works asset.

Loads OpenAlex works parquet files into DuckLake, partitioned by publication_year.
Replicates oa_works_ducklake.sh via the Python duckdb API so results are reported
back to the Dagster orchestrator via Dagster Pipes.

NOTE: source parquet data lives only on VACC gpfs. This payload must be run with
use_slurm=True — it will raise immediately if the source directory is not found.
"""
import os
import shutil

import duckdb
from dagster_pipes import open_dagster_pipes

# payload_utils.py is uploaded alongside this script by ScisciDBComputeResource.
# Fall back to the package import when running locally without the upload.
try:
    from payload_utils import collect_resource_usage
except ImportError:
    from scisciDB.payload_utils import collect_resource_usage


def main(context):
    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    oa_data_root = context.get_extra("oa_data_root")

    source_dir = os.path.join(oa_data_root, "works")

    if not os.path.isdir(source_dir):
        raise RuntimeError(
            f"Source directory not found: {source_dir}\n"
            "Raw OpenAlex parquet data lives only on VACC. "
            "Re-run this asset with use_slurm=True in the launchpad."
        )

    # SLURM scratch dir for DuckDB temp files; fall back to /tmp locally
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

    context.log.info(f"Source parquet: {source_dir}")
    context.log.info(f"DuckLake catalog: {metadata_path}")
    context.log.info(f"Data root: {sciscidb_data_root}")

    duckdb_memory_limit = context.get_extra("duckdb_memory_limit") or "110GB"
    duckdb_threads = context.get_extra("duckdb_threads") or 32

    conn = duckdb.connect()
    try:
        conn.execute(f"SET temp_directory = '{duckdb_tmp}/'")
        conn.execute(f"SET memory_limit = '{duckdb_memory_limit}'")
        conn.execute(f"SET threads = {duckdb_threads}")
        conn.execute("SET max_temp_directory_size = '500GB'")
        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET enable_progress_bar = true")
        # Keep all year-partitions open at once so each thread writes at most
        # one file per partition (avoids many small fragment files).
        conn.execute("SET partitioned_write_max_open_files = 16")

        test_year = context.get_extra("test_year") or 0

        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.execute("DROP TABLE IF EXISTS oa_works")

        # Create empty table with schema inferred from parquet (no rows read).
        # Partitioning must be set BEFORE inserting data — DuckLake only applies
        # the partition key to new writes, so ALTER TABLE after a CTAS is too late.
        conn.execute(f"""
            CREATE TABLE oa_works AS
            SELECT * FROM read_parquet('{source_dir}/**/*.parquet')
            WHERE 1=0
        """)
        conn.execute("ALTER TABLE oa_works SET PARTITIONED BY (publication_year)")

        year_filter = (
            f"publication_year = {test_year}"
            if test_year
            else "publication_year BETWEEN 1900 AND 2026"
        )
        if test_year:
            context.log.info(f"TEST MODE: inserting only year {test_year}")

        import time
        n_files = len(list(__import__("pathlib").Path(source_dir).rglob("*.parquet")))
        context.log.info(f"Inserting oa_works from {n_files} parquet files...")
        t0 = time.time()
        conn.execute(f"""
            INSERT INTO oa_works
            SELECT *
            FROM read_parquet('{source_dir}/**/*.parquet')
            WHERE {year_filter}
        """)
        elapsed = time.time() - t0
        context.log.info(f"INSERT completed in {elapsed:.1f}s")
        context.log.info("Running stats...")

        total_rows, years, min_year, max_year = conn.execute("""
            SELECT
                COUNT(*)                    AS total_rows,
                COUNT(DISTINCT publication_year) AS years,
                MIN(publication_year)       AS min_year,
                MAX(publication_year)       AS max_year
            FROM oa_works
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
