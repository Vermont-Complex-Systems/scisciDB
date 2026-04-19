"""
Asset: papers_lookup

DOI-based cross-reference table between S2 papers (corpusid) and
OpenAlex works (oa_id). Used by downstream enrich assets.
Runs on VACC via SLURM (data lives only on VACC gpfs).
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class PapersLookupConfig(dg.Config):
    partition: str = "short"
    mem: str = "128G"
    cpus_per_task: int = 16
    time_limit: str = "03:00:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="transform",
    deps=["s2_papers", "oa_works_deduped"],
    description=(
        "DOI-based lookup table joining S2 papers (corpusid) with OpenAlex works (oa_id). "
        "Uses oa_works_deduped for clean 1-to-1 DOI matches. "
        "Rebuilt from scratch on each materialization."
    ),
)
def papers_lookup(
    context: dg.AssetExecutionContext,
    config: PapersLookupConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "papers_lookup must be run with use_slurm=True — data only exists on VACC gpfs."
        )

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "papers_lookup.py"),
        extras={
            "duckdb_memory_limit": f"{int(int(config.mem.rstrip('GgMm')) * 0.9)}GB",
            "duckdb_threads": config.cpus_per_task,
        },
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
