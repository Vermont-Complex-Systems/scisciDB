# Vermont Advanced Computing Core (VACC) — System Reference

## Storage Tiers (Read Before Every Job)

| Mount | Type | Backed up? | Best use for DuckDB |
|---|---|---|---|
| `/gpfs1` | GPFS parallel FS | ✅ Daily | Final outputs, permanent datasets |
| `/gpfs2/scratch/` | GPFS scratch | ❌ No backups | Large intermediate files |
| `/gpfs3/` | GPFS NVMe SSD scratch | ❌ No backups | **temp_directory, staging rewrites** — fastest IO |
| `/netfiles/` | NFS-mounted (likely) | Varies | ⚠️ Read-only queries OK; avoid writing many small files |
| `/home/` | NFS | ✅ | Code only — never data or temp |

### Key rule: always use `/gpfs3` for DuckDB temp and staging

```sql
SET temp_directory = '/gpfs3/scratch/<username>/duckdb_tmp/';
```

`/gpfs3` is 96 TB of NVMe SSD — it is the fastest storage on VACC and the right place for
spill-to-disk and staging rewrites. `/gpfs2` is acceptable as a fallback but slower.

**Never use `/home/` or `/netfiles/` for `temp_directory`.**

### The `/netfiles/` warning

Paths like `/netfiles/compethicslab/...` are NFS-mounted shared storage. Writing many small
files to NFS (as `PARTITION_BY` does) triggers one network round-trip per file create. For a
PARTITION_BY with 100 years × 16 threads = 1,600 file operations, this can turn a 10-minute
job into a multi-hour one.

**Pattern for NFS output destinations:**
```bash
# Write to fast /gpfs3 staging area first
duckdb << EOF
  COPY (...) TO '/gpfs3/scratch/$USER/staging/oa_works/'
  (FORMAT PARQUET, PARTITION_BY publication_year, ...);
EOF

# Then move to /netfiles destination
rsync -av --progress /gpfs3/scratch/$USER/staging/oa_works/ \
    /netfiles/compethicslab/scisciDB/oa_works/

# Clean up staging
rm -rf /gpfs3/scratch/$USER/staging/
```

---

## ⚠️ File Count Quotas — Critical for PARTITION_BY

VACC enforces file count limits per PI group. **DuckDB's PARTITION_BY creates
`threads × partitions` output files**, which can silently hit quota.

| Tier | File soft limit | File hard limit |
|---|---|---|
| 1 | 1,000,000 | 1,500,000 |
| 2 | 3,000,000 | 6,000,000 |
| 3 | 6,000,000 | 12,000,000 |

### Estimating your output file count

For a `PARTITION_BY publication_year` job with 16 threads and years 1900–2024 (125 years):
```
Max files = threads × distinct_partition_values × files_per_partition
           = 16 × 125 × 1 = ~2,000 files (minimum)
```
In practice DuckDB may create more than 1 file per partition per thread for large partitions.

Check current quota usage:
```bash
groupquota
```

### Reducing output file count

```sql
-- Option 1: Reduce threads specifically for the COPY step
SET threads = 4;   -- fewer threads = fewer output files per partition
COPY (...) TO '...' (FORMAT PARQUET, PARTITION_BY publication_year, OVERWRITE);

-- Option 2: Merge output files after writing (glue small partitions together)
-- Run a second COPY per partition that reads and rewrites each year's directory
```

---

## BlueMoon Partition Guide

| SLURM partition | Node class | Cores/node | RAM/node | Best for |
|---|---|---|---|---|
| `bluemoon` (default) | node300s | 128 | 1 TB | Standard large jobs |
| `bluemoon` | node400 | 192 | 1.5 TB | Largest core-count jobs |
| `bigmem` | Large memory | 64 | 4 TB | Joins producing huge intermediates |
| `datamountain` | Data Mountain | 56 | 8 TB | Extreme memory (rare; large in-memory datasets) |
| `highclockspeed` | high clock | 32 | 256 GB | Clock-sensitive sequential workloads |
| `gpu` | GPU nodes | varies | varies | Not relevant for standard DuckDB |

### Selecting the right partition

For DuckDB Parquet conversion/partitioning jobs:
- **Default choice**: `bluemoon` on node300s or node400 — 128+ cores, 1+ TB RAM
- **Join-heavy with huge intermediates**: `bigmem` (4 TB RAM, 64 cores)
- **Memory-constrained sequential scans**: `highclockspeed` (fewer but faster cores)

### Memory per thread guidelines on VACC

| Workload | Recommended | SLURM request (16 threads) |
|---|---|---|
| Scan + PARTITION_BY write | 2–3 GB/thread | `--mem=48G` |
| Aggregation-heavy | 1–2 GB/thread | `--mem=32G` |
| Join-heavy (many-to-many) | 3–4 GB/thread | `--mem=64G` |
| Extreme joins | Use `bigmem` partition | `--mem=500G` |

### Physical vs. logical cores on AMD EPYC

VACC's node300s and node400 use AMD EPYC processors with SMT (simultaneous multi-threading).
DuckDB often performs better with physical cores only:
- node300s: 128 logical cores → start with `--cpus-per-task=64` (physical cores)
- node400: 192 logical cores → start with `--cpus-per-task=96`

Test with `EXPLAIN ANALYZE` to confirm thread utilization before scaling up.

---

## SLURM Submission on VACC

### Basic job script template for DuckDB

```bash
#!/bin/bash
#SBATCH --job-name=duckdb_job
#SBATCH --partition=bluemoon
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=08:00:00
#SBATCH --output=%x_%j.out

# Always create temp dir on fast NVMe scratch
DUCKDB_TMP=/gpfs3/scratch/${USER}/duckdb_tmp_${SLURM_JOB_ID}
mkdir -p ${DUCKDB_TMP}

MEM_LIMIT=$(( SLURM_MEM_PER_NODE * 9 / 10 / 1024 ))  # GB, 90% headroom

duckdb << EOF
SET threads        = ${SLURM_CPUS_PER_TASK};
SET temp_directory = '${DUCKDB_TMP}/';
SET memory_limit   = '${MEM_LIMIT}GB';
SET preserve_insertion_order = false;

-- your query here

EOF

# Cleanup temp
rm -rf ${DUCKDB_TMP}
```

### Interactive session for diagnostics

Run diagnostic queries interactively before committing to a long batch job:
```bash
srun --partition=bluemoon --nodes=1 --ntasks-per-node=1 \
     --cpus-per-task=8 --mem=32G --time=01:00:00 --pty bash
```
Then launch `duckdb` directly in the interactive session.

---

## Common VACC-Specific Pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| Job killed with no OOM message | Hit memory hard limit | Add more `--mem` or switch to `bigmem` partition |
| `Disk quota exceeded` error | File count limit hit | Reduce threads; check `groupquota` |
| Very slow write despite fast query | Output going to `/netfiles/` (NFS) | Stage to `/gpfs3`, then rsync |
| Slow reads from `/gpfs1` | GPFS contention from other users | Schedule off-peak or use `/gpfs3` staging copy |
| Job starts but stalls | `temp_directory` on `/home/` (NFS) | Move `temp_directory` to `/gpfs3` |