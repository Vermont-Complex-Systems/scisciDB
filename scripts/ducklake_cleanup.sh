#!/usr/bin/env bash
# ducklake_cleanup.sh — run on VACC to purge orphaned DuckLake files
#
# Usage (from the scisciDB repo root on VACC):
#   bash load/ducklake_cleanup.sh            # dry-run (default, safe)
#   bash load/ducklake_cleanup.sh --commit   # actually delete files
#
# Requires: uv (reads uv.lock from the repo)

set -euo pipefail

METADATA_PATH="/netfiles/compethicslab/duckdb/metadata.ducklake"
DATA_ROOT="/gpfs1/home/j/s/jstonge1/scisciDB"
DRY_RUN=true

if [[ "${1:-}" == "--commit" ]]; then
    DRY_RUN=false
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo "==> DRY RUN — no files will be deleted. Pass --commit to actually delete."
else
    echo "==> COMMIT mode — files will be permanently deleted."
    read -r -p "Are you sure? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

uv run python - <<PYEOF
import duckdb

metadata_path = "$METADATA_PATH"
data_root = "$DATA_ROOT"
dry_run = $( [[ "$DRY_RUN" == "true" ]] && echo "True" || echo "False" )

conn = duckdb.connect()
conn.execute(f"""
    ATTACH 'ducklake:{metadata_path}' AS scisciDB
        (DATA_PATH '{data_root}')
""")
conn.execute("USE scisciDB")

preview = conn.execute("""
    SELECT * FROM ducklake_cleanup_old_files(
        cleanup_all := true,
        dry_run := true
    )
""").fetchall()

print(f"Files scheduled for deletion: {len(preview)}")
for row in preview:
    print(f"  {row}")

if dry_run:
    print("\\nDry run complete. Re-run with --commit to delete.")
else:
    result = conn.execute("""
        SELECT * FROM ducklake_cleanup_old_files(
            cleanup_all := true,
            dry_run := false
        )
    """).fetchall()
    print(f"\\nDeleted {len(result)} files.")

conn.close()
PYEOF
