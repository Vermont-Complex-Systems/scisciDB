# VACC / dagster-slurm setup

This project uses [dagster-slurm](https://github.com/...) to submit heavy jobs
(e.g. `oa_works`) to the UVM VACC cluster from the local Dagster orchestrator.
dagster-slurm normally packages the Python environment with **pixi pack** before
uploading it to VACC. Because this project uses **uv** instead of pixi, we skip
auto-packaging and point to a **pre-deployed environment** that lives on the
shared NFS filesystem (`/users/j/s/jstonge1/`), which is mounted on all
compute nodes.

---

## Required directory structure

dagster-slurm's `pre_deployed_env_path` option expects this layout:

```
~/scisciDB-env/
├── activate.sh          ← sources the venv; dagster-slurm runs this before the payload
└── env/                 ← actual uv venv (NOT the root itself)
    └── bin/
        └── python
```

The same convention used by `open-academic-analytics-env/`:

```bash
cat ~/open-academic-analytics-env/activate.sh
# source ~/open-academic-analytics-env/env/bin/activate
```

---

## One-time VACC setup

SSH into the login node and create the env in the required layout:

```bash
ssh jstonge1@login.vacc.uvm.edu

# Create the wrapper directory and the venv inside it
mkdir -p ~/scisciDB-env
uv venv ~/scisciDB-env/env

# Create activate.sh (dagster-slurm sources this before running the payload)
echo 'source ~/scisciDB-env/env/bin/activate' > ~/scisciDB-env/activate.sh

# Install payload dependencies via uv (avoids PIP_USER issues on VACC)
uv pip install --python ~/scisciDB-env/env/bin/python duckdb dagster-pipes

# Pre-cache the DuckLake extension so compute nodes don't need outbound internet
~/scisciDB-env/env/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake')"
```

Point `.env` at the wrapper directory (not the `env/` subdirectory):

```
SCISCIDB_DEPLOYED_ENV_PATH=/users/j/s/jstonge1/scisciDB-env
```

---

## Verify the setup

Run `vacc_env_check` from the Dagster UI (or CLI) with `use_slurm=True`.
It checks:

| check | what it validates |
|---|---|
| `pkg_duckdb` | duckdb is installed in the deployed env |
| `path_oa_works_source` | gpfs source parquet dir is reachable |
| `ducklake_attach` | DuckLake extension loads and catalog opens |
| `parquet_read` | duckdb can read at least one parquet file from gpfs |
| `scratch_writable` | `/gpfs3tmp/pi/alwoodwa/scratch/` is writable |
| `ready_for_oa_works` | all of the above passed |

```bash
# from the project root
uv run dg dev          # open UI, then materialize vacc_env_check with use_slurm=True
```

---

## Why not pixi?

dagster-slurm's default workflow calls `pixi run pack-only` to create a
self-contained `.conda` archive that is uploaded and extracted on the compute
node at job submission time. This requires a `pixi.toml` in the project.

Because scisciDB uses `uv` + `pyproject.toml`, we use the
`pre_deployed_env_path` option instead, which tells dagster-slurm to skip
packaging entirely and activate the given path as the Python environment on
the remote node. The trade-off:

| | pixi auto-pack | uv pre-deployed |
|---|---|---|
| setup | zero (pixi does it) | one-time `uv venv` on VACC |
| env updates | automatic on each run | manual `pip install --upgrade` after dep changes |
| isolation | per-run archive | shared venv (all jobs share it) |

---

## Updating the deployed env after dependency changes

If `duckdb` or `dagster-pipes` change version:

```bash
ssh jstonge1@login.vacc.uvm.edu
uv pip install --python ~/scisciDB-env/env/bin/python --upgrade duckdb dagster-pipes
# Re-cache DuckLake extension if duckdb major version changed
~/scisciDB-env/env/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake')"
```

---

## Key `.env` variables

| variable | purpose |
|---|---|
| `SCISCIDB_DEPLOYED_ENV_PATH` | path to the wrapper dir on VACC (must contain `activate.sh` and `env/`) |
| `SLURM_SSH_HOST` | VACC login node |
| `SLURM_SSH_USER` | your VACC netid |
| `SLURM_SSH_KEY` | path to SSH key (ed25519 recommended) |
| `SLURM_OA_DATA_ROOT` | gpfs path to raw OA parquet files (VACC-only) |
| `SLURM_S2_DATA_ROOT` | gpfs path to raw S2 parquet files (VACC-only) |
| `SCISCIDB_DATA_ROOT` | netfiles path where DuckLake writes output data |
