# Schema & Indexing Reference — DuckDB on HPC

## Type Selection

Using correct types reduces storage, improves compression, and speeds up filtering/joins.

| Use case | Recommended type | Avoid |
|---|---|---|
| Timestamps | `DATETIME` | `VARCHAR` |
| Integer IDs | `BIGINT` | `VARCHAR` |
| Small integers | `TINYINT` / `SMALLINT` | `BIGINT` (wastes memory during processing) |
| Flags / booleans | `BOOLEAN` | `VARCHAR` ('true'/'false') |

### Performance impact example (554M rows)
| Type | Storage | Query time |
|---|---|---|
| `DATETIME` | 3.3 GB | 0.9 s |
| `VARCHAR` | 5.2 GB | 3.9 s |

Join on correct types is ~1.8× faster than VARCHAR:
| Join type | Query time |
|---|---|
| `BIGINT JOIN BIGINT` | 1.2 s |
| `VARCHAR JOIN VARCHAR` | 2.1 s |

---

## Constraints on HPC

Constraints (`PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`) build ART indexes and slow bulk loads significantly.

| Load strategy | Time (554M rows) |
|---|---|
| Load **with** primary key | 461.6 s |
| Load **without** primary key | 121.0 s |
| Load without, then add key | 242.0 s |

**HPC best practice**: Load data first, add constraints after if needed.

---

## Zonemaps (automatic min-max indexes)

DuckDB auto-creates zonemaps on all columns. They allow skipping entire row groups when filtering.

- Works best on **ordered** data (e.g., timestamps in ascending order).
- Ordered `DATETIME` column: 2.5× smaller storage, 1.5× faster queries vs. unordered.
- Use ordered `INTEGER` IDs instead of `UUID` for point-lookup queries.

---

## ART Indexes

Only add ART indexes when:
1. You have **highly selective** point-lookup queries (equality or `IN(...)` conditions).
2. You have sufficient memory (indexes stay in memory and aren't evicted).

Do **not** add ART indexes for join, aggregation, or sort queries — they don't help.

Eligible for index scan (single column, no expression):
```sql
CREATE INDEX idx ON tbl (col1);          -- ✅ eligible
CREATE INDEX idx ON tbl (col1, col2);    -- ❌ not eligible
CREATE INDEX idx ON tbl (col1 + 1);      -- ❌ not eligible
```

### Memory note for HPC
Index buffers are not evicted under memory pressure. On memory-constrained nodes,
large indexes can crowd out working memory. Mitigate with:
```sql
DETACH my_db;
ATTACH 'my_db.db' AS my_db;   -- lazy deserialization frees unused index memory
```