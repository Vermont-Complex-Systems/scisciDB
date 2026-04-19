"""
Asset: s2_abstracts

Creates a view over Semantic Scholar abstracts parquet files in DuckLake.
Runs on VACC via SLURM (source data lives only on VACC gpfs).
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class S2AbstractsConfig(dg.Config):
    partition: str = "short"
    mem: str = "4G"
    cpus_per_task: int = 1
    time_limit: str = "00:10:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "View over Semantic Scholar abstracts parquet files. "
        "Source data is VACC-only — requires use_slurm=True."
    ),
)
def s2_abstracts(
    context: dg.AssetExecutionContext,
    config: S2AbstractsConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "s2_abstracts must be run with use_slurm=True — source data only exists on VACC gpfs."
        )

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "s2_abstracts.py"),
        extras={},
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
