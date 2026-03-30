#!/bin/bash
# Load OpenAlex works into DuckLake, partitioned by publication_year.
# Prerequisites: source Parquet files must be sorted by publication_year (see import/parse.py)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load environment variables
source "${PROJECT_ROOT}/.env"

SOURCE_DIR="/users/j/s/jstonge1/data/scisciDB/openalex/data/works"
METADATA_PATH="${PROJECT_ROOT}/metadata.ducklake"
DUCKDB_TMP=/gpfs3tmp/pi/alwoodwa/scratch/duckdb_tmp_${SLURM_JOB_ID:-$$}

mkdir -p "${DUCKDB_TMP}"

echo "Starting oa_works load into DuckLake..."
echo "  Source: ${SOURCE_DIR}"
echo "  Catalog: ${METADATA_PATH}"
echo "  Data root: ${SCISCIDB_DATA_ROOT}"

duckdb << EOF
SET temp_directory = '${DUCKDB_TMP}/';
SET memory_limit = '110GB';
SET threads = 32;
SET max_temp_directory_size = '500GB';
SET preserve_insertion_order = false;
SET enable_progress_bar = true;
-- Default is 100. PARTITION_BY writes 1 file per thread per partition; when
-- open file count exceeds this limit, DuckDB flushes and reopens partitions,
-- creating extra small files. Set >= distinct partition count (~527 years)
-- so all partitions stay open and each thread writes at most 1 file per year.
SET partitioned_write_max_open_files = 600;

-- Attach DuckLake catalog
ATTACH 'ducklake:${METADATA_PATH}' AS scisciDB
    (DATA_PATH '${SCISCIDB_DATA_ROOT}');

USE scisciDB;

-- Drop existing table if it exists
DROP TABLE IF EXISTS oa_works;

-- Create table from source parquet files
CREATE TABLE oa_works AS
SELECT *
FROM read_parquet('${SOURCE_DIR}/**/*.parquet')
WHERE publication_year BETWEEN 1500 AND 2026;

-- Set partitioning for future writes/compaction
ALTER TABLE oa_works SET PARTITIONED BY (publication_year);

-- Report stats
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT publication_year) as years,
    MIN(publication_year) as min_year,
    MAX(publication_year) as max_year
FROM oa_works;

EOF

rm -rf "${DUCKDB_TMP}"

echo ""
echo "✓ oa_works loaded into DuckLake!"
echo "  Query with: SELECT * FROM scisciDB.oa_works LIMIT 10;"
