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

    conn = duckdb.connect()
    try:
        conn.execute(f"SET temp_directory = '{duckdb_tmp}/'")
        conn.execute("SET memory_limit = '110GB'")
        conn.execute("SET threads = 32")
        conn.execute("SET max_temp_directory_size = '500GB'")
        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET enable_progress_bar = true")
        # Keep all year-partitions open at once so each thread writes at most
        # one file per partition (avoids many small fragment files).
        conn.execute("SET partitioned_write_max_open_files = 600")

        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.execute("DROP TABLE IF EXISTS oa_works")

        n_files = len(list(__import__("pathlib").Path(source_dir).rglob("*.parquet")))
        context.log.info(f"Creating oa_works table from {n_files} parquet files — this may take ~2h...")
        conn.execute(f"""
            CREATE TABLE oa_works AS
            SELECT *
            FROM read_parquet('{source_dir}/**/*.parquet')
            WHERE publication_year BETWEEN 1500 AND 2026
        """)
        context.log.info("CREATE TABLE completed, running stats...")

        conn.execute("ALTER TABLE oa_works SET PARTITIONED BY (publication_year)")

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
