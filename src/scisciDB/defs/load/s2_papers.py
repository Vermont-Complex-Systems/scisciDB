"""
Asset: s2_papers

Loads Semantic Scholar papers parquet files into DuckLake.
Set dry_run=True in the launchpad to estimate memory without loading.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class S2PapersConfig(dg.Config):
    dry_run: bool = False
    partition: str = "short"
    mem: str = "64G"
    cpus_per_task: int = 16
    time_limit: str = "02:00:00"
    test_year: int = 0  # If non-zero, only insert this year (for testing)


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "Load Semantic Scholar papers parquet files into DuckLake. "
        "Source data is VACC-only — requires use_slurm=True. "
        "Set dry_run=True to estimate memory requirements without loading. "
        "Tune partition/mem/cpus_per_task/time_limit based on the estimate."
    ),
)
def s2_papers(
    context: dg.AssetExecutionContext,
    config: S2PapersConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    # Parse mem string (e.g. "64G") to set DuckDB limit at ~90% to leave headroom
    # for the OS and dagster-pipes overhead.
    mem_value = int(config.mem.rstrip("GgMm"))
    mem_unit = config.mem[-1].upper()
    duckdb_mem = f"{int(mem_value * 0.9)}{mem_unit}B"  # e.g. "64G" -> "57GB"

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "s2_papers.py"),
        extras={
            "dry_run": config.dry_run,
            "duckdb_memory_limit": duckdb_mem,
            "duckdb_threads": config.cpus_per_task,
            "test_year": config.test_year or None,
        },
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
