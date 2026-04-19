"""
Asset: oa_works_deduped

Creates a deduplicated view of oa_works, keeping the best record per DOI.
Runs on VACC via SLURM (data lives only on VACC gpfs).

Deduplication strategy:
  - One record per DOI, ranked by richest metadata.
  - Ranking: cited_by_count > number of authors > has abstract > number of
    topics > OpenAlex ID (stable tiebreaker).
  - Well-validated for DOI pairs (~98% of duplicates). For DOIs with 3+
    records (often issue-level DOIs assigned to multiple distinct papers)
    the ranking is best-effort — we still keep one record to ensure DOI
    uniqueness for downstream S2↔OA matching.
  - Rows without a DOI are dropped (no use for downstream S2↔OA matching).
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class OaWorksDedupedConfig(dg.Config):
    partition: str = "short"
    mem: str = "4G"
    cpus_per_task: int = 1
    time_limit: str = "00:10:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="transform",
    deps=["oa_works"],
    description=(
        "Deduplicated view of oa_works: one record per DOI, ranked by "
        "cited_by_count > authors > has_abstract > topics > ID. "
        "Rows without a DOI are dropped."
    ),
)
def oa_works_deduped(
    context: dg.AssetExecutionContext,
    config: OaWorksDedupedConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    if not compute.use_slurm:
        raise RuntimeError(
            "oa_works_deduped must be run with use_slurm=True — data only exists on VACC gpfs."
        )

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "oa_works_deduped.py"),
        extras={},
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
