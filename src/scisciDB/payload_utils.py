"""
Shared utilities for Dagster Pipes payload scripts.

This module is automatically uploaded alongside every payload by ScisciDBComputeResource,
so payloads can import it directly:

    from payload_utils import collect_resource_usage, estimate_memory
"""
import os
import pathlib
import resource
import subprocess


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
