"""
Payload for vacc_env_check asset.

Diagnostic script that validates everything oa_works needs from a SLURM node:
  1. Python environment + packages (duckdb, dagster-pipes)
  2. Filesystem paths (metadata catalog, data root, source parquet dir)
  3. DuckLake extension loadable by duckdb (no outbound internet needed)
  4. duckdb can read at least one parquet file from gpfs
  5. Scratch temp dir is writable
"""
import importlib
import os
import platform
import sys
import tempfile

from dagster_pipes import open_dagster_pipes


def _check_ducklake(metadata_path: str, sciscidb_data_root: str, log) -> tuple[bool, str]:
    """Attempt to ATTACH the DuckLake catalog — catches missing extension or bad path."""
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")
        conn.close()
        return True, "ok"
    except Exception as e:
        return False, str(e)


def _check_parquet_read(source_dir: str, log) -> tuple[bool, str]:
    """Try reading a single row from any parquet file in the source dir."""
    try:
        import duckdb
        conn = duckdb.connect()
        result = conn.execute(
            f"SELECT 1 FROM read_parquet('{source_dir}/**/*.parquet') LIMIT 1"
        ).fetchone()
        conn.close()
        return result is not None, "ok" if result else "no rows returned"
    except Exception as e:
        return False, str(e)


def _check_scratch_writable(scratch_base: str, log) -> tuple[bool, str]:
    """Check that a temp file can be created and deleted in the scratch dir."""
    try:
        os.makedirs(scratch_base, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=scratch_base, delete=True):
            pass
        return True, "ok"
    except Exception as e:
        return False, str(e)


def main(context):
    metadata_path = context.get_extra("metadata_path")
    sciscidb_data_root = context.get_extra("sciscidb_data_root")
    oa_data_root = context.get_extra("oa_data_root")
    source_dir = os.path.join(oa_data_root, "works")
    scratch_base = "/gpfs3tmp/pi/alwoodwa/scratch"

    node = platform.node()
    slurm_job_id = os.environ.get("SLURM_JOB_ID", "N/A (local)")
    slurm_partition = os.environ.get("SLURM_JOB_PARTITION", "N/A")

    context.log.info(f"Python:    {sys.version}")
    context.log.info(f"Node:      {node}  |  Job: {slurm_job_id} ({slurm_partition})")

    results = {}

    # 1. Package presence
    context.log.info("── packages ──")
    for pkg in ("duckdb", "dagster_pipes"):
        try:
            mod = importlib.import_module(pkg)
            ver = getattr(mod, "__version__", "installed")
            results[f"pkg_{pkg}"] = ver
            context.log.info(f"  ✓ {pkg} {ver}")
        except ImportError:
            results[f"pkg_{pkg}"] = "MISSING"
            context.log.warning(f"  ✗ {pkg} MISSING")

    # 2. Path existence
    context.log.info("── paths ──")
    for label, path in {
        "metadata_path": metadata_path,
        "sciscidb_data_root": sciscidb_data_root,
        "oa_works_source": source_dir,
    }.items():
        ok = os.path.exists(path)
        results[f"path_{label}"] = ok
        icon, log = ("✓", context.log.info) if ok else ("✗", context.log.warning)
        log(f"  {icon} {label}: {path}")

    # 3. DuckLake attach
    context.log.info("── ducklake ──")
    ducklake_ok, ducklake_msg = _check_ducklake(metadata_path, sciscidb_data_root, context.log)
    results["ducklake_attach"] = ducklake_ok
    if ducklake_ok:
        context.log.info(f"  ✓ ATTACH ducklake succeeded")
    else:
        context.log.warning(f"  ✗ ATTACH ducklake failed: {ducklake_msg}")

    # 4. Parquet read (only if source dir exists)
    context.log.info("── parquet read ──")
    if results.get("path_oa_works_source"):
        parquet_ok, parquet_msg = _check_parquet_read(source_dir, context.log)
        results["parquet_read"] = parquet_ok
        if parquet_ok:
            context.log.info("  ✓ read_parquet succeeded")
        else:
            context.log.warning(f"  ✗ read_parquet failed: {parquet_msg}")
    else:
        results["parquet_read"] = False
        context.log.warning("  – skipped (source dir not found)")

    # 5. Scratch dir writable
    context.log.info("── scratch ──")
    scratch_ok, scratch_msg = _check_scratch_writable(scratch_base, context.log)
    results["scratch_writable"] = scratch_ok
    if scratch_ok:
        context.log.info(f"  ✓ writable: {scratch_base}")
    else:
        context.log.warning(f"  ✗ not writable: {scratch_msg}")

    ready = all([
        results.get("pkg_duckdb", "MISSING") != "MISSING",
        results.get("path_oa_works_source", False),
        results.get("ducklake_attach", False),
        results.get("parquet_read", False),
        results.get("scratch_writable", False),
    ])
    results["ready_for_oa_works"] = ready

    if ready:
        context.log.info("── ✓ all checks passed — oa_works is safe to run ──")
    else:
        context.log.warning("── ✗ some checks failed — fix before running oa_works ──")

    context.report_asset_materialization(metadata={
        "node": node,
        "slurm_job_id": slurm_job_id,
        "slurm_partition": slurm_partition,
        **results,
    })


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main(context)
