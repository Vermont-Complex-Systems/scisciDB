"""
Asset: ducklake_cleanup

Cleans up DuckLake files scheduled for deletion (e.g. after DROP TABLE + reload).
Always runs a dry-run preview first so you can see what will be purged before
committing. Set dry_run=False in the launchpad to actually delete files.
"""
import os
from pathlib import Path

import dagster as dg
import duckdb
from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
_DEFAULT_METADATA_PATH = str(
    Path(os.environ.get("SCISCIDB_METADATA_PATH", str(_PROJECT_ROOT / "metadata.ducklake")))
)


class DucklakeCleanupConfig(dg.Config):
    dry_run: bool = True
    cleanup_all: bool = True


@dg.asset(
    kinds={"duckdb", "ducklake"},
    group_name="maintenance",
    description=(
        "Clean up DuckLake files scheduled for deletion. "
        "Always previews (dry_run=True) before deleting — set dry_run=False to commit. "
        "Run after re-loading a table to reclaim disk space from old files."
    ),
)
def ducklake_cleanup(context: dg.AssetExecutionContext, config: DucklakeCleanupConfig) -> dg.MaterializeResult:
    metadata_path = _DEFAULT_METADATA_PATH
    sciscidb_data_root = os.environ.get("SCISCIDB_DATA_ROOT", "")

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{metadata_path}' AS scisciDB
                (DATA_PATH '{sciscidb_data_root}')
        """)
        conn.execute("USE scisciDB")

        # Always preview first
        preview = conn.execute(f"""
            SELECT * FROM ducklake_cleanup_old_files(
                cleanup_all := {str(config.cleanup_all).lower()},
                dry_run := true
            )
        """).fetchall()

        context.log.info(f"Files scheduled for deletion: {len(preview)}")
        for row in preview:
            context.log.info(f"  would delete: {row}")

        if config.dry_run:
            context.log.info("dry_run=True — no files deleted. Set dry_run=False to commit.")
            return dg.MaterializeResult(metadata={
                "files_scheduled": len(preview),
                "dry_run": True,
            })

        # Actually delete
        result = conn.execute(f"""
            SELECT * FROM ducklake_cleanup_old_files(
                cleanup_all := {str(config.cleanup_all).lower()},
                dry_run := false
            )
        """).fetchall()

        context.log.info(f"Deleted {len(result)} files.")
        return dg.MaterializeResult(metadata={
            "files_deleted": len(result),
            "dry_run": False,
        })
    finally:
        conn.close()
