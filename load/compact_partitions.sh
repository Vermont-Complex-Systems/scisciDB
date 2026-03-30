#!/bin/bash
# Compact partitioned parquet: merge many small files per year into 1 file each.
# Run after oa_works.sh to consolidate the 327K+ fragment files.
# Submit: sbatch --partition=short --cpus-per-task=8 --mem=32G --time=03:00:00 \
#                --output=logs/compact_%j.out \
#                --wrap="bash load/compact_partitions.sh"
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARTITION_DIR="${SCRIPT_DIR}/output/openalex/works"

if [ ! -d "${PARTITION_DIR}" ]; then
  echo "Error: ${PARTITION_DIR} does not exist"
  exit 1
fi

BEFORE_FILES=$(find "${PARTITION_DIR}" -name "*.parquet" | wc -l)
BEFORE_SIZE=$(du -sh "${PARTITION_DIR}" | cut -f1)
echo "============================================"
echo "  COMPACT PARTITIONS"
echo "============================================"
echo "  Directory: ${PARTITION_DIR}"
echo "  Before: ${BEFORE_FILES} files (${BEFORE_SIZE})"
echo "============================================"

COMPACT_START=$(date +%s)
COMPACTED=0
SKIPPED=0

for YEAR_DIR in "${PARTITION_DIR}"/publication_year=*/; do
  FILE_COUNT=$(find "${YEAR_DIR}" -name "*.parquet" | wc -l)

  if [ "${FILE_COUNT}" -le 1 ]; then
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  YEAR=$(basename "${YEAR_DIR}" | sed 's/publication_year=//')
  COMPACT_FILE="${YEAR_DIR}/compacted.parquet"

  duckdb -c "
    COPY (SELECT * FROM read_parquet('${YEAR_DIR}*.parquet'))
    TO '${COMPACT_FILE}' (FORMAT PARQUET, ROW_GROUP_SIZE 122880, COMPRESSION 'zstd');
  "

  # Remove old fragment files, keep compacted
  find "${YEAR_DIR}" -name "*.parquet" ! -name "compacted.parquet" -delete
  COMPACTED=$((COMPACTED + 1))

  # Progress every 50 years
  if [ $((COMPACTED % 50)) -eq 0 ]; then
    echo "  Compacted ${COMPACTED} partitions..."
  fi
done

COMPACT_SECS=$(( $(date +%s) - COMPACT_START ))
AFTER_FILES=$(find "${PARTITION_DIR}" -name "*.parquet" | wc -l)
AFTER_SIZE=$(du -sh "${PARTITION_DIR}" | cut -f1)

echo ""
echo "============================================"
echo "  RESULTS"
echo "============================================"
echo "  Compacted: ${COMPACTED} partitions (${SKIPPED} already single-file)"
echo "  Time: ${COMPACT_SECS}s"
echo "  Before: ${BEFORE_FILES} files (${BEFORE_SIZE})"
echo "  After:  ${AFTER_FILES} files (${AFTER_SIZE})"
echo "============================================"
