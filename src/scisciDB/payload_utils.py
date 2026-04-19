"""
Shared utilities for Dagster Pipes payload scripts.

This module is automatically uploaded alongside every payload by ScisciDBComputeResource,
so payloads can import it directly:

    from payload_utils import ducklake_load, collect_resource_usage, estimate_memory
"""
import os
import pathlib
import resource
import shutil
import subprocess
import time

import duckdb


def ducklake_load(
    context,
    table_name: str,
    source_glob: str,
    *,
    partition_by: str | None = None,
    year_column: str | None = None,
    select_expr: str = "*",
    read_parquet_opts: str = "",
    duckdb_settings: dict | None = None,
):
    """Load parquet files into a DuckLake table.

    Handles: connect -> configure -> attach -> drop -> create schema ->
    partition -> insert (with year filter / test_year) -> stats -> report.

    Args:
        context: Dagster Pipes context.
        table_name: DuckLake table name (e.g. "oa_works").
        source_glob: Parquet glob (e.g. "/path/to/works/**/*.parquet").
        partition_by: Column to partition by (e.g. "publication_year").
        year_column: Column used for year filtering / test_year support.
            Defaults to partition_by if set.
        select_expr: SELECT expression (default "*").
        read_parquet_opts: Extra options for read_parquet (e.g. ", union_by_name=true").
        duckdb_settings: Dict of DuckDB SET key=value pairs.
    """
    rp = f"read_parquet('{source_glob}'{read_parquet_opts})"

    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    test_year = context.get_extra("test_year") or 0

    if year_column is None:
        year_column = partition_by

    # Scratch temp dir on NVMe
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    duckdb_tmp = f"/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_{job_id}"
    os.makedirs(duckdb_tmp, exist_ok=True)

    context.log.info(f"Source: {source_glob}")
    context.log.info(f"DuckLake catalog: {metadata_path}")
    context.log.info(f"Data root: {sciscidb_data_root}")

    conn = duckdb.connect()
    try:
        # DuckDB settings — sensible defaults, overridable per-asset
        settings = {
            "temp_directory": f"'{duckdb_tmp}/'",
            "memory_limit": "'40GB'",
            "threads": 8,
            "max_temp_directory_size": "'500GB'",
            "preserve_insertion_order": "false",
            "enable_progress_bar": "true",
            "partitioned_write_max_open_files": 16,
        }
        if duckdb_settings:
            for k, v in duckdb_settings.items():
                if isinstance(v, str):
                    settings[k] = f"'{v}'"
                elif isinstance(v, bool):
                    settings[k] = "true" if v else "false"
                else:
                    settings[k] = v
        for k, v in settings.items():
            conn.execute(f"SET {k} = {v}")

        # Attach DuckLake
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create empty table (schema only) then set partitioning BEFORE insert
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT {select_expr} FROM {rp}
            WHERE 1=0
        """)
        if partition_by:
            conn.execute(f"ALTER TABLE {table_name} SET PARTITIONED BY ({partition_by})")

        # Build WHERE clause
        if year_column:
            if test_year:
                where = f"{year_column} = {test_year}"
                context.log.info(f"TEST MODE: inserting only {year_column} = {test_year}")
            else:
                where = f"{year_column} BETWEEN 1900 AND 2026"
        else:
            where = "1=1"

        # Extract base dir from glob (everything before the first wildcard)
        base_dir = source_glob.split("*")[0].rstrip("/")
        n_files = len(list(pathlib.Path(base_dir).rglob("*.parquet")))
        context.log.info(f"Inserting {table_name} from {n_files} parquet files...")

        t0 = time.time()
        conn.execute(f"""
            INSERT INTO {table_name}
            SELECT {select_expr}
            FROM {rp}
            WHERE {where}
        """)
        elapsed = time.time() - t0
        context.log.info(f"INSERT completed in {elapsed:.1f}s")

        # Stats
        if year_column:
            total_rows, years, min_year, max_year = conn.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT {year_column}),
                       MIN({year_column}), MAX({year_column})
                FROM {table_name}
            """).fetchone()
            context.log.info(
                f"Loaded {total_rows:,} rows across {years} years ({min_year}-{max_year})"
            )
            stats = {
                "total_rows": total_rows,
                "distinct_years": years,
                "min_year": min_year,
                "max_year": max_year,
            }
        else:
            total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            context.log.info(f"Loaded {total_rows:,} rows")
            stats = {"total_rows": total_rows}

        context.report_asset_materialization(metadata={
            **stats,
            "elapsed_seconds": round(elapsed, 1),
            "test_year": test_year or "all",
            "source_glob": source_glob,
            "metadata_path": metadata_path,
            **collect_resource_usage(),
        })
    finally:
        conn.close()
        shutil.rmtree(duckdb_tmp, ignore_errors=True)


def estimate_memory(context, source_dir: str, conn) -> dict:
    """Scan parquet metadata to estimate memory without loading data.

    Returns a metadata dict suitable for context.report_asset_materialization.
    """
    files = sorted(pathlib.Path(source_dir).rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"No parquet files found in {source_dir}")

    total_compressed_bytes = sum(f.stat().st_size for f in files)

    total_rows = conn.execute(
        f"SELECT SUM(num_rows) FROM parquet_file_metadata('{source_dir}/**/*.parquet')"
    ).fetchone()[0]

    sample_file = str(files[0])
    sample_rows = conn.execute(
        f"SELECT num_rows FROM parquet_file_metadata('{sample_file}')"
    ).fetchone()[0]
    sample_uncompressed = conn.execute(
        f"SELECT SUM(total_uncompressed_size) FROM parquet_metadata('{sample_file}')"
    ).fetchone()[0]
    bytes_per_row = sample_uncompressed / sample_rows if sample_rows else 0

    estimated_memory_gb = (total_rows * bytes_per_row) / (1024 ** 3)
    compressed_gb = total_compressed_bytes / (1024 ** 3)

    context.log.info(
        f"DRY RUN — {len(files)} files, {total_rows:,} rows\n"
        f"  Compressed on disk : {compressed_gb:.1f} GB\n"
        f"  Estimated in-memory: {estimated_memory_gb:.1f} GB\n"
        f"  (sampled from {files[0].name}, {bytes_per_row:.0f} bytes/row uncompressed)"
    )

    return {
        "n_files": len(files),
        "total_rows_estimate": total_rows,
        "compressed_size_gb": round(compressed_gb, 2),
        "estimated_memory_gb": round(estimated_memory_gb, 1),
        "dry_run": True,
    }


def collect_resource_usage() -> dict:
    """Collect peak memory and (on SLURM) job accounting via sacct."""
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    stats: dict = {"peak_memory_mb": round(peak_kb / 1024, 1)}

    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        return stats

    stats["slurm_job_id"] = job_id
    stats["slurm_node"] = os.environ.get("SLURM_NODELIST", "")
    stats["slurm_partition"] = os.environ.get("SLURM_JOB_PARTITION", "")

    try:
        result = subprocess.run(
            [
                "sacct", "-j", f"{job_id}.batch",
                "--format=Elapsed,MaxRSS,TotalCPU,AllocCPUS",
                "--noheader", "--parsable2",
            ],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            elapsed, max_rss, total_cpu, alloc_cpus = result.stdout.strip().split("|")
            stats["slurm_elapsed"] = elapsed
            stats["slurm_max_rss"] = max_rss
            stats["slurm_total_cpu"] = total_cpu
            stats["slurm_alloc_cpus"] = alloc_cpus
    except Exception:
        pass

    return stats
