---
name: duckdb-slurm-hpc
description: >
  Interactive tuning guide for DuckDB workloads on the Vermont Advanced Computing Core (VACC)
  BlueMoon cluster, and SLURM HPC clusters generally, with large datasets.
  Use this skill whenever the user shares a DuckDB query they want to run or tune on VACC or
  any cluster, mentions performance problems with DuckDB on HPC, wants to convert/partition large
  Parquet or CSV datasets, is hitting OOM or slowness on a SLURM job, asks about thread/memory
  settings, storage tiers (/gpfs1, /gpfs2, /gpfs3, /netfiles), partition selection on BlueMoon,
  or file quota issues. Trigger even if the user just pastes a DuckDB query and mentions VACC,
  BlueMoon, gpfs, SLURM, or large files — don't wait for an explicit "tuning help" request.
  The skill leads an evidence-based diagnosis: ask targeted questions and request specific
  profiling queries before recommending anything.
---

# DuckDB on VACC BlueMoon — Applied Tuning Protocol

**Core philosophy**: Never prescribe before diagnosing. DuckDB performance on HPC depends on
factors the user may not think to mention (filesystem tier, partition skew, row group sizes,
file count quotas). Always do detective work first.

**Always read `references/vacc.md`** for VACC-specific storage paths, partition specs,
and file quota details before generating any SLURM script or storage recommendation.

---

## Phase 0 — Read and Reflect Before Responding

When a user shares a DuckDB query or describes a pipeline:

1. **Parse what the query is actually doing**: scan type, filters, joins, aggregations, output format.
2. **Identify the unknowns** that control performance but aren't visible in the query alone.
3. **Do not give recommendations yet.** Go to Phase 1.

---

## Phase 1 — Intake Interview

Ask the user these questions up front — group them naturally, adapt to what's already visible.

### 1a. SLURM allocation
If no job script is given, ask:
- Which **partition** (`bluemoon`, `bigmem`, `datamountain`, `highclockspeed`)?
- `--cpus-per-task` and `--mem`?
- `--time` estimate?

See `references/vacc.md` → "BlueMoon Partition Guide" to recommend the right partition.

### 1b. Storage path classification (CRITICAL — check every path in the query)

Classify every path mentioned using the VACC storage tier table in `references/vacc.md`:

| Path prefix | Tier | Risk for DuckDB |
|---|---|---|
| `/gpfs3/` | NVMe SSD scratch | ✅ Best — use for temp_directory and staging |
| `/gpfs2/scratch/` | HDD scratch | ✅ OK for temp/staging |
| `/gpfs1/` | GPFS backed-up | ✅ OK for reads; OK for final output |
| `/netfiles/` | NFS-mounted | ⚠️ OK for reads; **avoid writing many files** |
| `/home/` | NFS | ❌ Never use for data, temp, or output |

If the output path is `/netfiles/...`, flag it immediately:
> "Your output path appears to be on NFS (`/netfiles/`). Writing a partitioned Parquet dataset
> there will be slow. I'll recommend staging to `/gpfs3` first — but let's profile the data first."

### 1c. Dataset profile
- Approximate total size (GB/TB)?
- Number of files in the glob pattern (rough estimate ok)?
- Was this data written by a consistent pipeline (same schema, compression)?

### 1d. File quota awareness
For any `PARTITION_BY` query, always ask (or calculate):
- How many distinct partition values are expected?
- Estimate: `threads × partitions` ≈ minimum output file count
- Run `groupquota` on the cluster to check current usage vs. limit

### 1e. Prior attempts
- Has this job been run before? Did it complete, OOM, stall, or timeout?
- Any existing job script to review?

---

## Phase 2 — Diagnostic Queries

Once you have enough context (or the user wants to skip straight to diagnostics), issue a
**targeted set of queries** for the user to run and paste back. Explain why each one matters.

Always tailor these to the user's actual paths. Below are templates — fill in real paths.

### D0. File count pre-check (run before any PARTITION_BY job — VACC-specific)
Estimate output file count vs. VACC quota *before* running the full job.
```bash
# Check current quota usage on VACC
groupquota

# Count source files
find /path/to/source -name "*.parquet" | wc -l
```
Then estimate: `threads × distinct_partition_values ≈ minimum output files`.
If this approaches your soft limit (check `references/vacc.md` → "File Count Quotas"),
reduce `threads` or process partitions in a job array.

### D1. File inventory (always run this first)
Reveals: file count, sizes, row group counts, compression codec.
```sql
SELECT
    file_name,
    num_rows,
    round(total_compressed_size / 1e9, 3)   AS size_gb,
    num_row_groups,
    round(num_rows / num_row_groups)         AS rows_per_group
FROM parquet_metadata('YOUR_GLOB_PATH/**/*.parquet')
ORDER BY total_compressed_size DESC
LIMIT 20;
```
What to look for:
- `rows_per_group` < 10,000 → parallelism will be poor; flag this
- Very large files (>20 GB) with few row groups → single-threaded bottleneck
- Mix of very large and very small files → uneven parallelism

### D2. Partition key distribution (run when PARTITION_BY is involved)
Reveals: skew across the partition key — critical before any `PARTITION_BY` operation.
```sql
SELECT
    publication_year,
    count(*)                                          AS n_rows,
    round(count(*) * 100.0 / sum(count(*)) OVER(), 2) AS pct_of_total
FROM read_parquet('YOUR_GLOB_PATH/**/*.parquet')
GROUP BY publication_year
ORDER BY publication_year;
```
What to look for:
- A handful of years dominating (e.g., 2020–2024 = 60% of rows) → those partitions will be
  huge; consider sub-partitioning or processing them separately
- Years with very few rows (< ~100K) → tiny output files with poor compression and useless
  zonemaps; consider bucketing small years together
- NULL values in `publication_year` → creates `__HIVE_DEFAULT_PARTITION__` directories;
  decide whether to filter or coalesce them

### D3. Schema inspection (run when schema provenance is uncertain)
```sql
DESCRIBE SELECT * FROM read_parquet('YOUR_GLOB_PATH/**/*.parquet') LIMIT 0;
```
What to look for:
- `STRUCT`, `MAP`, or `LIST` columns that aren't needed → avoid `SELECT *` on wide nested tables
- `VARCHAR` used for timestamps, IDs, or `publication_year` → suggest type casting before writing
- Confirm partition column type — if `VARCHAR`, cast to `INTEGER` before partitioning

### D4. Total row count and data volume
```sql
SELECT
    count(*)                                      AS total_rows,
    round(sum(total_compressed_size) / 1e9, 2)   AS total_compressed_gb
FROM parquet_metadata('YOUR_GLOB_PATH/**/*.parquet');
```

### D5. EXPLAIN ANALYZE on the actual query (for complex queries or slowness reports)
```sql
EXPLAIN ANALYZE
COPY (
    SELECT * FROM read_parquet('YOUR_PATH/**/*.parquet')
) TO 'YOUR_OUTPUT' (FORMAT PARQUET, PARTITION_BY col, OVERWRITE);
```
See `references/interpreting_explain.md` for how to read the output.

---

## Phase 3 — Interpret Findings and Prescribe

Only after the user returns results from Phase 2 should you make specific recommendations.
Each recommendation must cite which diagnostic result drove it.

### Configuration baseline for VACC

```sql
-- Always set these at the top of every DuckDB session on VACC
SET threads         = <SLURM_CPUS_PER_TASK>;
SET temp_directory  = '/gpfs3/scratch/<USER>/duckdb_tmp_<JOBID>/';  -- NVMe SSD
SET memory_limit    = '<N>GB';               -- 90% of --mem; 1-2 GB/thread agg, 3-4 GB/thread join
SET preserve_insertion_order = false;        -- reduces memory for large writes without ORDER BY
```

**Never point `temp_directory` at `/home/` or `/netfiles/`** — both are NFS.

### VACC storage-driven recommendations

**Output to `/netfiles/`** (NFS — e.g., `/netfiles/compethicslab/...`):
- Each file create in a `PARTITION_BY` output is a network round-trip. For 100+ partitions
  this turns minutes into hours. Always stage first:
  ```bash
  STAGING=/gpfs3/scratch/${USER}/staging_${SLURM_JOB_ID}
  mkdir -p ${STAGING}

  duckdb << EOF
    COPY (...) TO '${STAGING}/output/'
    (FORMAT PARQUET, PARTITION_BY publication_year, OVERWRITE);
  EOF

  rsync -av --progress ${STAGING}/output/ /netfiles/compethicslab/scisciDB/oa_works/
  rm -rf ${STAGING}
  ```

**Output to `/gpfs1/`** (GPFS backed-up — fine for final output):
- Write directly. No staging needed. Still use `/gpfs3` for `temp_directory`.

**Output to `/gpfs2/` or `/gpfs3/`** (scratch — fine for intermediate):
- Write directly. Use `/gpfs3` for `temp_directory`.

**Reads from any GPFS path** (`/gpfs1`, `/gpfs2`, `/gpfs3`):
- GPFS is a parallel filesystem; reads are efficient. No special handling needed.
- If reads feel slow, it may be contention from other cluster users — reschedule or use a
  local staging copy.

### File quota recommendations (VACC-specific — from D0)

If estimated `threads × partitions` is within 30% of your soft limit:

```sql
-- Reduce threads for the COPY step only
SET threads = 4;   -- 4 threads × 125 years = 500 files instead of 2,000
COPY (...) TO '...' (FORMAT PARQUET, PARTITION_BY publication_year, OVERWRITE);
```

Or use a SLURM job array (one job per year) — each job creates O(1) files per partition:
```bash
#SBATCH --array=1950-2024
YEAR=${SLURM_ARRAY_TASK_ID}
duckdb << EOF
COPY (SELECT * FROM read_parquet(...) WHERE publication_year = ${YEAR})
TO '/gpfs3/scratch/${USER}/output/publication_year=${YEAR}/'
(FORMAT PARQUET, ROW_GROUP_SIZE 122880, OVERWRITE);
EOF
```

### Partition skew recommendations (from D2)

When `D2` reveals heavy skew (e.g., OpenAlex works: pre-2000 tiny, post-2015 massive):

**Option A — Process heavy years separately** (cleanest for SLURM arrays):
```bash
#SBATCH --array=1950-2024
duckdb << EOF
COPY (SELECT * FROM read_parquet(...) WHERE publication_year = ${SLURM_ARRAY_TASK_ID})
TO 'output/publication_year=${SLURM_ARRAY_TASK_ID}/'
(FORMAT PARQUET, ROW_GROUP_SIZE 122880, OVERWRITE);
EOF
```

**Option B — Two-pass: old years together, recent years separate**:
```sql
-- Small years bucketed together
COPY (SELECT * FROM read_parquet(...) WHERE publication_year < 1990)
TO 'output/' (FORMAT PARQUET, PARTITION_BY publication_year, OVERWRITE);

-- Large years get their own pass with row group control
COPY (SELECT * FROM read_parquet(...) WHERE publication_year >= 1990)
TO 'output/' (FORMAT PARQUET, PARTITION_BY publication_year,
              ROW_GROUP_SIZE 122880, OVERWRITE);
```

**Option C — Control output row group size** (always recommended for PARTITION_BY):
```sql
COPY (...) TO 'output/' (
    FORMAT PARQUET,
    PARTITION_BY publication_year,
    ROW_GROUP_SIZE 122880,
    COMPRESSION 'zstd',
    OVERWRITE
);
```

### Row group size recommendations (from D1)

| `rows_per_group` (D1) | Diagnosis | Action |
|---|---|---|
| < 5,000 | Source files will serialize; parallelism broken | Rewrite source with larger row groups first |
| 5,000–50,000 | Sub-optimal throughput | Consolidate source files if possible |
| 50,000–200,000 | ✅ Good | No action needed |
| > 500,000 in output | Output may not be downstream-parallelizable | Add `ROW_GROUP_SIZE 122880` to COPY |

Consolidation pattern (when source row groups are too small):
```sql
-- Rewrite source into well-structured staging Parquet first, then run the pipeline
COPY (SELECT * FROM read_parquet('source/**/*.parquet'))
TO '${SLURM_TMPDIR}/staging/'
(FORMAT PARQUET, ROW_GROUP_SIZE 122880, COMPRESSION 'zstd');
-- Then run the actual PARTITION_BY query on staging/
```

### Schema-driven recommendations (from D3)

- **Unnecessary nested columns**: `SELECT *` on wide tables with `STRUCT`/`LIST` columns is
  expensive. Select only what's needed.
- **VARCHAR partition key**: Cast before partitioning to avoid silent type issues:
  ```sql
  COPY (SELECT *, TRY_CAST(publication_year AS INTEGER) AS publication_year FROM ...)
  TO '...' (FORMAT PARQUET, PARTITION_BY publication_year, OVERWRITE);
  ```
- **NULL partition keys**: Filter or coalesce to avoid `__HIVE_DEFAULT_PARTITION__` directories:
  ```sql
  WHERE publication_year IS NOT NULL
  -- or:
  SELECT *, COALESCE(publication_year, 0) AS publication_year FROM ...
  ```

---

## Phase 4 — Generate Tailored VACC SLURM Script

After diagnostics, produce a tailored job script. Always read `references/vacc.md` to confirm
the right partition, node class, and storage paths. Template:

```bash
#!/bin/bash
#SBATCH --job-name=duckdb_partition
#SBATCH --partition=bluemoon          # or bigmem for join-heavy / huge intermediates
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32            # start with physical cores; node300s=64, node400=96
#SBATCH --mem=128G                    # 4 GB/thread; adjust from diagnostics
#SBATCH --time=08:00:00
#SBATCH --output=%x_%j.out

# Fast NVMe scratch for temp spill and staging
DUCKDB_TMP=/gpfs3/scratch/${USER}/duckdb_tmp_${SLURM_JOB_ID}
STAGING=/gpfs3/scratch/${USER}/staging_${SLURM_JOB_ID}
mkdir -p ${DUCKDB_TMP} ${STAGING}

MEM_LIMIT=$(( SLURM_MEM_PER_NODE * 9 / 10 / 1024 ))  # 90% of --mem in GB

duckdb << EOF
SET threads         = ${SLURM_CPUS_PER_TASK};
SET temp_directory  = '${DUCKDB_TMP}/';
SET memory_limit    = '${MEM_LIMIT}GB';
SET preserve_insertion_order = false;

COPY (
    SELECT * FROM read_parquet('/path/to/source/**/*.parquet')
    WHERE publication_year IS NOT NULL
)
TO '${STAGING}/output/'
(FORMAT PARQUET, PARTITION_BY publication_year,
 ROW_GROUP_SIZE 122880, COMPRESSION 'zstd', OVERWRITE);
EOF

# Sync from fast /gpfs3 staging to final destination
# (use rsync if destination is /netfiles/; cp -r if destination is /gpfs1/)
echo "Syncing to final destination..."
rsync -av --progress ${STAGING}/output/ /netfiles/compethicslab/scisciDB/oa_works/

# Cleanup
rm -rf ${DUCKDB_TMP} ${STAGING}
```

---

## Reference Files

Load these when the conversation goes deeper into a specific area:

| File | Load when... |
|---|---|
| `references/vacc.md` | **Always load first** — storage tiers, partition guide, file quotas, SLURM template |
| `references/file_formats.md` | Row group sizing benchmarks, codec comparison, CSV tips |
| `references/schema_and_indexing.md` | Type benchmarks, bulk load + constraint timing, ART indexes |
| `references/join_and_memory.md` | Join order, out-of-core limits, blocking operators |
| `references/interpreting_explain.md` | Reading EXPLAIN ANALYZE output, thread utilization, red flags |