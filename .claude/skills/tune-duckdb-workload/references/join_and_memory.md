# Join Operations & Memory Reference — DuckDB on HPC

## Out-of-Core Processing

DuckDB spills to disk for: `GROUP BY`, `JOIN`, `ORDER BY`, `WINDOW (PARTITION BY ... ORDER BY ...)`.

### What does NOT spill (avoid on large HPC datasets)
- `list()` aggregate
- `string_agg()` aggregate
- `PIVOT` (uses `list()` internally)
- Aggregate functions that use internal sorting

These will OOM on large data. Alternatives:
- Replace `PIVOT` with manual `CASE WHEN` aggregation.
- Replace `list()` with streaming aggregates where possible.

### Multiple blocking operators
When multiple blocking operators appear in one query, OOM risk increases significantly.
Break the query into stages using temp tables:

```sql
-- Stage 1: handle the GROUP BY separately
CREATE TEMP TABLE agg_result AS
    SELECT region, sum(amount) AS total
    FROM orders
    GROUP BY region;

-- Stage 2: join on the smaller result
SELECT c.name, a.total
FROM agg_result a
JOIN customers c ON a.region = c.region;
```

---

## Memory Configuration

```sql
-- 1 GB per thread for aggregation workloads
-- 3-4 GB per thread for join-heavy workloads
SET memory_limit = '48GB';   -- set below SLURM --mem allocation

-- Enable temp spill (must be fast local storage)
SET temp_directory = '/scratch/local/duckdb.tmp/';
```

Minimum memory: 125 MB per thread. Below this, DuckDB may behave unpredictably.

---

## Join Order Tuning

DuckDB's cost-based optimizer uses table statistics. Statistics are better for native `.db` files
than for Parquet (Parquet lacks HyperLogLog stats).

**If join order is bad on Parquet → load data into DuckDB first.**

### Forcing join order manually
```sql
-- Disable optimizer (joins execute in written order)
SET disabled_optimizers = 'join_order,build_side_probe_side';

SELECT *
FROM a
JOIN b ON a.id = b.a_id     -- this join runs first
JOIN c ON b.id = c.b_id;    -- this join runs second

SET disabled_optimizers = '';   -- re-enable after
```

### Breaking into temp tables
```sql
CREATE OR REPLACE TEMP TABLE t1 AS
    SELECT * FROM large_table WHERE filter_col = 'value';

CREATE OR REPLACE TEMP TABLE t2 AS
    SELECT t1.*, other.info
    FROM t1 JOIN other_table other ON t1.id = other.id;

SELECT * FROM t2 JOIN final_table ...;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
```

---

## EXPLAIN ANALYZE on HPC

```sql
EXPLAIN ANALYZE SELECT ...;
```

Metrics to check:
- **Rows scanned vs. rows estimated** — large divergence → bad statistics → consider loading to .db
- **Join type** — `NESTED_LOOP` is bad for large tables; should be `HASH_JOIN`
- **Threads active** — if <<`threads` setting, row groups are too large or too few
- **Bytes read (remote)** — for S3/object storage queries, shows IO cost