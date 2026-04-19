"""
Asset: oa_sources

Loads OpenAlex sources parquet files into DuckLake.
Small dataset — lightweight SLURM settings.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


class OaSourcesConfig(dg.Config):
    partition: str = "short"
    mem: str = "8G"
    cpus_per_task: int = 4
    time_limit: str = "00:30:00"


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="load",
    description=(
        "Load OpenAlex sources parquet files into DuckLake. "
        "Excludes is_indexed_in_scopus (schema conflicts across vintages). "
        "Source data is VACC-only. Use use_slurm=False when running directly on VACC."
    ),
)
def oa_sources(
    context: dg.AssetExecutionContext,
    config: OaSourcesConfig,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    mem_value = int(config.mem.rstrip("GgMm"))
    mem_unit = config.mem[-1].upper()
    duckdb_mem = f"{int(mem_value * 0.9)}{mem_unit}B"

    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "oa_sources.py"),
        extras={
            "duckdb_memory_limit": duckdb_mem,
            "duckdb_threads": config.cpus_per_task,
        },
        extra_slurm_opts={
            "partition": config.partition,
            "cpus_per_task": config.cpus_per_task,
            "mem": config.mem,
            "time_limit": config.time_limit,
        },
    ).get_results()
