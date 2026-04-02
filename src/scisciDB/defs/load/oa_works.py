"""
Asset: oa_works

Loads OpenAlex works parquet files into DuckLake, partitioned by publication_year.
Memory-intensive (110GB+); set use_slurm=True in the launchpad to run on VACC.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class OaWorksConfig(dg.Config):
    partition: str = "short"
    mem: str = "128G"
    cpus_per_task: int = 16
    time_limit: str = "03:00:00"
    test_year: int = 0  # If non-zero, only insert this year (for testing)


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "Load OpenAlex works parquet files into DuckLake, partitioned by "
        "publication_year. Source data lives only on VACC gpfs — must be run "
        "with use_slurm=True. Tune partition/mem/cpus_per_task/time_limit in "
        "the launchpad."
    ),
)
def oa_works(
    context: dg.AssetExecutionContext,
    config: OaWorksConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "oa_works must be run with use_slurm=True — source data only exists on VACC gpfs. "
            "Set use_slurm=True in the Dagster launchpad before materializing."
        )
    mem_value = int(config.mem.rstrip("GgMm"))
    mem_unit = config.mem[-1].upper()
    duckdb_mem = f"{int(mem_value * 0.9)}{mem_unit}B"

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "oa_works.py"),
        extras={
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
