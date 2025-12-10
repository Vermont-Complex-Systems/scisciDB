# Summary: Building a Research Data Pipeline with DuckLake

## Context
We designed a data pipeline for large-scale academic datasets (S2ORC from Semantic Scholar, ~50GB+ of papers with full text). The goal was to enable fast analytical queries while maintaining update compatibility with upstream data sources.

**Principled Data Processing Pipeline**
Following Patrick Ball's methodology:
```
scripts/s2orc/
├── input/          # Raw downloads (JSON.GZ from Semantic Scholar API)
│   └── download.py
├── import/         # Parse & validate (JSON → Parquet with structs)
│   ├── parse.py
│   └── validate.py
├── enrich/         # Add derived fields (like primary_s2field)
│   └── add_fields.py
│   └── add_crossover_fields.py  # Cross-dataset enrichment (S2 + OpenAlex)
├── export/         # Load to DuckLake
│   └── update_ducklake.py
└── Makefile        # Orchestrates: make all DATASET=papers
```

Each step is:

Auditable: Clear input → output
Reproducible: Makefile documents process
Testable: Validate before export
Versioned: Frozen snapshots for reproducibility

## Enrichment Best Practices

### Cross-Dataset Enrichment (e.g., S2 + OpenAlex)

When enriching one dataset with another (e.g., adding OpenAlex topics to S2 papers):

**Pattern: Incremental by default, force update for auditability**
```python
# Normal monthly sync (fast, only new papers)
python enrich/add_crossover_fields.py --operation oa_topics

# When papersLookup is updated/fixed (slow, all papers, creates auditable snapshot)
python enrich/add_crossover_fields.py --operation oa_topics --force-update
```

**Why this works:**
- `ALTER TABLE ADD COLUMN` is cheap (metadata only, no file rewrites)
- Incremental `UPDATE WHERE oa_topics IS NULL` only touches new papers (fast, ~200MB snapshots)
- Force update creates full snapshot for auditability when lookup data changes
- DuckLake snapshots enable time travel and comparison between enrichment versions

**Snapshot audit trail:**
```sql
-- View enrichment history
SELECT snapshot_id, commit_message, extra_info
FROM scisciDB.snapshots()
WHERE commit_message LIKE '%OpenAlex%';

-- Compare before/after re-enrichment
SELECT * FROM s2_papers AT (VERSION => 5);  -- Before
SELECT * FROM s2_papers AT (VERSION => 6);  -- After

-- Rollback if needed
CREATE OR REPLACE TABLE s2_papers AS
SELECT * FROM s2_papers AT (VERSION => 5);
```

## Performance Considerations

### Memory Management

46GB RAM available
Set memory_limit='35GB' (leave headroom)
Set threads=8-16 (based on CPU cores)
Set preserve_insertion_order=false for better performance

### When Queries are Slow

Filter early: Add WHERE clauses before joins
**Don't SELECT ***: Especially avoid body.text (huge field) unless needed
Use VIEWs: Don't materialize unless necessary
Export subsets: For repeated analysis, export filtered results to smaller Parquet

### Key Lessons

VIEWs > TABLEs for large read-only datasets (no duplication)
Convert nested JSON to STRUCT during parse (one-time cost, forever benefit)
Partition strategically (by year for temporal data)
DuckLake when modifying, vanilla DuckDB when read-only
No premature caching - DuckDB is fast, add caching only if needed
Separate concerns: Data pipeline (scripts/) vs API (app/)

### Tools & Technologies

DuckDB: Analytical queries on Parquet
DuckLake: Version control for datasets
PostgreSQL: Metadata catalog + pg_duckdb for queries
FastAPI: REST API layer
Parquet: Columnar storage format
Makefile: Pipeline orchestration

This architecture handles 100GB+ datasets efficiently on a single VM with 46GB RAM and 4.3TB network storage.

## DuckLake Usage Guide

### Overview

DuckLake is an ACID-compliant data lakehouse format providing time travel, schema evolution, and transactional guarantees for Parquet data lakes. Built on DuckDB with local file storage.

### Quick Start

```sql
INSTALL ducklake;
ATTACH 'ducklake:my_lake.ducklake' AS my_lake;
USE my_lake;
```

### Core Constraints

**No Primary Keys/Indexes**: DuckLake doesn't support primary keys, indexes, or unique constraints. Use:
- **Partitioning** on frequently-filtered columns: `ALTER TABLE sales SET PARTITIONED BY (region, year(order_date));`
- **File-level statistics** for automatic pruning
- **Query pattern-driven schema design**

### Essential Best Practices

#### Connection Management
- Always use `USE database_name` or qualify table names to avoid accidental operations on in-memory database
- Use relative paths for local storage: `ATTACH 'ducklake:./data/my_lake.ducklake' AS my_lake;`

#### Schema Design
- Plan schema carefully upfront - avoid frequent structural changes
- Use snake_case naming for consistency
- Specify meaningful defaults for new columns
- Design with type promotions in mind (int32 → int64)

#### Maintenance Strategy
- Use `CHECKPOINT` for automated maintenance
- Run `merge_adjacent_files()` for tables with frequent small inserts
- Configure retention: `expire_older_than` and periodic `cleanup_files()`

#### Performance
- Partition strategically on query filter columns
- Configure `target_file_size` (default 512MB) based on usage patterns
- Use `DATA_INLINING_ROW_LIMIT` for small, frequently-updated tables
- Set `parquet_compression = 'zstd'` for balanced speed/size

### Configuration Hierarchy

Settings: Table → Schema → Global → Default

```sql
CALL my_lake.set_option('parquet_compression', 'zstd');                    -- Global
CALL my_lake.set_option('parquet_compression', 'snappy', schema => 'logs'); -- Schema
```

### Common Pitfalls

1. **Schema Evolution**: Frequent structural changes hurt maintainability
2. **File Proliferation**: Monitor small files; use data inlining or batch inserts
3. **Snapshot Accumulation**: Implement retention policies
4. **Connection Leaks**: Always detach properly: `DETACH database_name`

### Cleaning Up Duplicate Imports

If you accidentally import data twice, DuckLake's time travel lets you fix it without restarting:

1. **Delete duplicate data**: `DELETE FROM babynames WHERE geo = 'location_name'`
2. **Re-import correctly**: Run the import again (duplicate prevention now built into loaders)
3. **List snapshots**: `SELECT * FROM babylake.snapshots()` to identify problematic snapshot IDs
4. **Expire bad snapshots**: `CALL ducklake_expire_snapshots('babylake', versions => [6, 7])`
5. **Clean up orphaned files**: `CALL ducklake_delete_orphaned_files('babylake', cleanup_all => true)`
6. **Verify cleanup**: Check `metadata.ducklake.files/` directory - old parquet files should be removed

Note: In some cases, parquet files may persist even after cleanup. This is safe - DuckDB only reads files referenced by active snapshots. If needed, you can manually remove unreferenced parquet files after verifying the correct data is intact.

### Additional DuckLake Usage References

For detailed usage patterns and advanced features, refer to:
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/usage/` - Core usage patterns (connecting, time travel, schema evolution, etc.)
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/maintenance/` - Maintenance operations and best practices
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/advanced_features/` - Advanced features (partitioning, encryption, etc.)