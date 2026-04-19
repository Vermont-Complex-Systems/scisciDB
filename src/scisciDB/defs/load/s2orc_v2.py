"""
Asset: s2orc_v2

Creates a view over S2ORC v2 (full-text parsed documents) parquet files in DuckLake.
Contains body text with annotations (section headers, sentences, paragraphs,
bibliography references) for papers that have been parsed by Semantic Scholar.
Runs on VACC via SLURM (source data lives only on VACC gpfs).
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class S2orcV2Config(dg.Config):
    partition: str = "short"
    mem: str = "4G"
    cpus_per_task: int = 1
    time_limit: str = "00:10:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "View over S2ORC v2 full-text parsed documents. "
        "Contains body text with annotations (sections, sentences, bibliography). "
        "Source data is VACC-only — requires use_slurm=True."
    ),
)
def s2orc_v2(
    context: dg.AssetExecutionContext,
    config: S2orcV2Config,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "s2orc_v2 must be run with use_slurm=True — source data only exists on VACC gpfs."
        )

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "s2orc_v2.py"),
        extras={},
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
