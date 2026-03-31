# Interpreting EXPLAIN ANALYZE — DuckDB on HPC

## What EXPLAIN ANALYZE Shows

`EXPLAIN ANALYZE` runs the query and annotates each operator with:
- **Rows** returned (actual vs. estimated)
- **Execution time** per operator (wall-clock, summed across threads — will exceed total time)
- **Thread count** active at each stage

## Key Red Flags

### 1. Nested Loop Join instead of Hash Join
```
NESTED_LOOP_JOIN
```
→ Rewrite the join condition so both sides are directly comparable without an expression.
   Hash joins handle large datasets; nested loops are O(n×m).

### 2. Scan without filter pushdown
```
SEQ_SCAN ... Filters: []
```
→ The filter is applied *after* reading the full table. Restructure the WHERE clause to
   reference the column directly (not wrapped in a function) so DuckDB can push it into the scan.

### 3. Cardinality explosion
```
── HASH_JOIN
     estimated rows: 1,000,000
     actual rows:    8,400,000,000
```
→ Bad join order or missing statistics. On Parquet: load data into DuckDB for better stats.
   Force join order manually if needed (see `references/join_and_memory.md`).

### 4. Underutilized threads
```
Operator: PARQUET_SCAN
Threads: 1 / 16
```
→ Too few row groups in source files. One thread per row group is the ceiling.
   Check D1 diagnostic: `rows_per_group` needs to be ≥ ~10,000 for meaningful parallelism.
   Source files may need to be rewritten with larger row groups.

### 5. Huge temp spill
```
Spilled: 48.2 GB
```
→ DuckDB is spilling to `temp_directory`. Confirm temp_directory is on fast local storage
   (not NFS). If spill is expected (dataset > RAM), this is OK on local SSD. On NFS, it
   will be very slow.

### 6. PARTITION_BY writing many tiny files
After a PARTITION_BY COPY, check output:
```bash
find output/ -name "*.parquet" | wc -l
du -sh output/publication_year=*/
```
If you see: `threads × partitions` files (e.g., 16 threads × 200 years = 3,200 files),
consider reducing threads or processing large partitions separately.

## Normal / Expected Output

A healthy `EXPLAIN ANALYZE` for a large scan+partition operation looks like:
- `PARQUET_SCAN` using N threads (where N = `SET threads`)
- `COPY_TO` operator at the root
- Estimated vs. actual rows within ~2-5× of each other
- No spill, or spill on local scratch storage

## Checking Remote IO (S3 / object storage)

For remote Parquet reads, `EXPLAIN ANALYZE` also reports:
```
HTTP Requests: 1,423
Data Transferred: 4.2 GB
```
High request count with small data → missing filter pushdown or poor row group alignment.
Apply filters that match the Parquet column order to reduce IO.