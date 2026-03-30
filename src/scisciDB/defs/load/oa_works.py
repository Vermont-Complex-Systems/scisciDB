"""
Asset: oa_works

Loads OpenAlex works parquet files into DuckLake, partitioned by publication_year.
Memory-intensive (110GB+); set use_slurm=True in the launchpad to run on VACC.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "Load OpenAlex works parquet files into DuckLake, partitioned by "
        "publication_year. Source data lives only on VACC gpfs — must be run "
        "with use_slurm=True. Requires ~110GB RAM / 32 CPUs."
    ),
)
def oa_works(
    context: dg.AssetExecutionContext,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "oa_works must be run with use_slurm=True — source data only exists on VACC gpfs. "
            "Set use_slurm=True in the Dagster launchpad before materializing."
        )
    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "oa_works.py"),
        # Per-asset SLURM overrides (mirrors the old oa_works_slurm.sh #SBATCH headers)
        extra_slurm_opts={
            "partition": "short",
            "cpus_per_task": 32,
            "mem": "128G",
            "time_limit": "03:00:00",
        },
    ).get_results()
