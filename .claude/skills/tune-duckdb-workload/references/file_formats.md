# File Formats Reference — DuckDB on HPC

## Parquet

### When to query directly vs. load into DuckDB

**Query directly** when:
- Disk space is tight (loading costs ~same space as source)
- Workload is projection + filter + aggregation (Parquet zonemaps handle this well)
- Data is accessed infrequently

**Load into DuckDB first** when:
- Running many queries on the same dataset (amortizes load cost)
- Queries have many joins (DuckDB's HyperLogLog stats give better cardinality estimates)
- Parquet uses gzip compression (expensive to decompress repeatedly)
- Query plans show bad join orders on Parquet → loading usually fixes this

### Row group sizes (critical for HPC parallelism)

DuckDB parallelizes at the row group level. One row group = one thread unit.

| Row group size | Relative execution time |
|---|---|
| < 5,000 | 5–10× slower than optimal |
| 5,000–20,000 | 1.5–2.5× slower |
| 100,000–1,000,000 | ✅ Optimal (~0.87–0.97s range) |
| > 1,000,000 | Minimal further gain |

Check your files:
```sql
SELECT row_group_id, num_rows, total_compressed_size
FROM parquet_metadata('/scratch/data/file.parquet');
```

Write with optimal row groups:
```sql
COPY my_table TO '/scratch/output/' (FORMAT PARQUET, ROW_GROUP_SIZE 122880);
```

### File sizes
- Target **100 MB – 10 GB** per file.
- Ensure total row groups across all files ≥ number of threads for full parallelism.

### Compression
| Algorithm | Decompression speed | Recommended |
|---|---|---|
| Snappy | Fast | ✅ Yes |
| LZ4 | Fast | ✅ Yes |
| zstd | Medium-fast | ✅ Yes |
| gzip | Slow | ❌ Avoid for repeated queries |

### Hive partitioning for filter pushdown
Partitioned folder structure lets DuckDB skip entire directories:
```
/scratch/data/year=2023/month=01/part-0001.parquet
/scratch/data/year=2023/month=02/part-0001.parquet
```
```sql
SELECT * FROM read_parquet('/scratch/data/**/*.parquet', hive_partitioning=true)
WHERE year = 2023 AND month = 01;
```

---

## CSV

### Compressed CSV
Load `.csv.gz` directly — DuckDB decompresses on-the-fly, which is **faster** than pre-decompressing:

| Approach | Load time |
|---|---|
| Load from `.csv.gz` directly | ~107 s |
| Decompress first, then load | ~121 s |

### Many small CSV files — disable the sniffer
For job arrays processing hundreds of small CSV files, the sniffer overhead adds up.
Detect schema once, then apply across all files:

```sql
-- Step 1 (run once):
.mode line
SELECT Prompt FROM sniff_csv('/scratch/data/part-0001.csv');

-- Step 2: use the output options with glob
FROM read_csv('/scratch/data/part-*.csv',
    auto_detect=false, delim=',', quote='"',
    escape='"', new_line='\n', skip=0, header=true,
    columns={'id': 'BIGINT', 'value': 'DOUBLE'});
```