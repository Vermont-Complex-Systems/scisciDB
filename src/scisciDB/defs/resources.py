import os

import dagster as dg
from dagster_slurm import BashLauncher, ComputeResource, SlurmQueueConfig, SlurmResource, SSHConnectionResource
from dotenv import load_dotenv
from pyprojroot import here

_PROJECT_ROOT = here()

load_dotenv(_PROJECT_ROOT / ".env", override=True)


_DEFAULT_METADATA_PATH = str(_PROJECT_ROOT / "metadata.ducklake")


class ScisciDBComputeResource(dg.ConfigurableResource):
    """Compute resource for scisciDB.

    Wraps dagster-slurm's ComputeResource with a use_slurm toggle so
    any asset can switch between local and SLURM execution from the
    Dagster launchpad without editing .env.

    Defaults to use_slurm=True — set to False in the launchpad for local runs.
    Requires SLURM_SSH_HOST / SLURM_SSH_USER / SLURM_SSH_KEY in .env.
    """

    use_slurm: bool = True

    def _local(self) -> ComputeResource:
        return ComputeResource(
            mode="local",
            pre_deployed_env_path=os.environ.get("SCISCIDB_DEPLOYED_ENV_PATH"),
            default_launcher=BashLauncher(),
        )

    def _slurm(self) -> ComputeResource:
        # Queue defaults are minimal — each asset sets its own partition/mem/cpus
        # via extra_slurm_opts so nothing is allocated unnecessarily.
        return ComputeResource(
            mode="slurm",
            pre_deployed_env_path=os.environ.get("SCISCIDB_DEPLOYED_ENV_PATH"),
            slurm=SlurmResource(
                ssh=SSHConnectionResource.from_env(),
                queue=SlurmQueueConfig(),
            ),
            default_launcher=BashLauncher(),
        )

    def run(self, context, payload_path: str, **kwargs):
        compute = self._slurm() if self.use_slurm else self._local()

        # Inject shared paths so payloads don't need to load .env themselves.
        # SLURM_ prefixed vars point to faster VACC-local gpfs paths.
        prefix = "SLURM_" if self.use_slurm else ""
        extras = kwargs.pop("extras", None) or {}
        extras.setdefault("metadata_path", _DEFAULT_METADATA_PATH)
        extras.setdefault("sciscidb_data_root", os.environ.get("SCISCIDB_DATA_ROOT", ""))
        extras.setdefault("oa_data_root", os.environ.get(f"{prefix}OA_DATA_ROOT", os.environ.get("OA_DATA_ROOT", "")))
        extras.setdefault("s2_data_root", os.environ.get(f"{prefix}S2_DATA_ROOT", os.environ.get("S2_DATA_ROOT", "")))

        # Always upload payload_utils.py so every payload can: from payload_utils import ...
        extra_files = list(kwargs.pop("extra_files", None) or [])
        utils_path = str(_PROJECT_ROOT / "src" / "scisciDB" / "payload_utils.py")
        if utils_path not in extra_files:
            extra_files.append(utils_path)

        return compute.run(
            context=context,
            payload_path=payload_path,
            extras=extras,
            extra_files=extra_files,
            **kwargs,
        )


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "compute": ScisciDBComputeResource(),
        }
    )
