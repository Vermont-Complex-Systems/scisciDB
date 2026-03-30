"""
Asset: vacc_env_check

Lightweight diagnostic that verifies the SLURM connection, deployed Python
environment, and filesystem path visibility from a VACC compute node.
Run this with use_slurm=True before triggering any heavyweight load jobs.
"""
from pathlib import Path

import dagster as dg

from scisciDB.defs.resources import ScisciDBComputeResource


@dg.asset(
    kinds={"slurm"},
    group_name="checks",
    description=(
        "Verify SLURM connection, deployed env packages, and path accessibility "
        "from a VACC node. Run with use_slurm=True before any load job."
    ),
)
def vacc_env_check(
    context: dg.AssetExecutionContext,
    compute: ScisciDBComputeResource,
) -> dg.MaterializeResult:
    return compute.run(
        context=context,
        payload_path=str(Path(__file__).parent / "payloads" / "vacc_env.py"),
        extra_slurm_opts={
            "partition": "short",
            "cpus_per_task": 4,
            "mem": "8G",
            "time_limit": "03:00:00",
        }
    ).get_results()
