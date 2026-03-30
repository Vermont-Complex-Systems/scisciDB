#!/bin/bash
# Repartition OpenAlex works by publication_year.
# Prerequisites: source Parquet files must be sorted by publication_year (see import/parse.py)
# Output goes directly to GPFS (no staging/rsync needed).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_DIR="/users/j/s/jstonge1/data/scisciDB/openalex/data/works"
OUTPUT_DIR="${SCRIPT_DIR}/output/openalex/works"
DUCKDB_TMP=/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_${SLURM_JOB_ID:-$$}

mkdir -p "${OUTPUT_DIR}" "${DUCKDB_TMP}"

echo "Starting oa_works repartition..."
echo "  Source: ${SOURCE_DIR}"
echo "  Output: ${OUTPUT_DIR}"

duckdb << EOF
SET temp_directory = '${DUCKDB_TMP}/';
SET memory_limit = '110GB';
SET threads = 32;
SET max_temp_directory_size = '500GB';
SET preserve_insertion_order = false;
-- Default is 100. PARTITION_BY writes 1 file per thread per partition; when
-- open file count exceeds this limit, DuckDB flushes and reopens partitions,
-- creating extra small files. Set >= distinct partition count (~527 years)
-- so all partitions stay open and each thread writes at most 1 file per year.
SET partitioned_write_max_open_files = 600;
SET enable_progress_bar = true;

COPY (
  SELECT *
  FROM read_parquet('${SOURCE_DIR}/**/*.parquet')
  WHERE publication_year BETWEEN 1500 AND 2026
)
TO '${OUTPUT_DIR}'
(FORMAT PARQUET, PARTITION_BY publication_year, ROW_GROUP_SIZE 122880, COMPRESSION 'zstd', OVERWRITE);

EOF

echo "DuckDB export complete."

OUTPUT_FILES=$(find "${OUTPUT_DIR}" -name "*.parquet" | wc -l)
OUTPUT_SIZE=$(du -sh "${OUTPUT_DIR}" | cut -f1)
echo "Output: ${OUTPUT_FILES} files (${OUTPUT_SIZE})"

rm -rf "${DUCKDB_TMP}"
echo "Done!"
