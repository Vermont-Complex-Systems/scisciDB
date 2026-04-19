"""
Asset: s2_has_fulltext

Adds has_fulltext boolean column to s2_papers by checking existence
in s2orc_v2. Incremental by default — only updates papers where
the flag is NULL. Runs on VACC via SLURM.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class S2HasFulltextConfig(dg.Config):
    force_update: bool = False
    partition: str = "short"
    mem: str = "128G"
    cpus_per_task: int = 16
    time_limit: str = "03:00:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="derived_fields",
    deps=["s2orc_v2"],
    description=(
        "Add has_fulltext boolean column to s2_papers via semi-join "
        "against s2orc_v2. Incremental by default."
    ),
)
def s2_has_fulltext(
    context: dg.AssetExecutionContext,
    config: S2HasFulltextConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "s2_has_fulltext must be run with use_slurm=True — data only exists on VACC gpfs."
        )

    mem_value = int(config.mem.rstrip("GgMm"))
    duckdb_mem = f"{int(mem_value * 0.9)}GB"

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "s2_has_fulltext.py"),
        extras={
            "duckdb_memory_limit": duckdb_mem,
            "duckdb_threads": config.cpus_per_task,
            "force_update": config.force_update,
        },
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
