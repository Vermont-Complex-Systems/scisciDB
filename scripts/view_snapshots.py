#!/usr/bin/env python3
"""
View DuckLake snapshot history for audit trail
"""
import duckdb
import sys

def main():
    # Connect to DuckLake
    conn = duckdb.connect()
    conn.execute("""
        ATTACH 'ducklake:postgres:dbname=complex_stories host=localhost user=jstonge1'
        AS scisciDB (DATA_PATH '/netfiles/compethicslab/scisciDB/');
    """)

    # Query snapshots
    result = conn.execute("""
        SELECT
            snapshot_id,
            snapshot_time,
            changes,
            author,
            commit_message,
            commit_extra_info
        FROM scisciDB.snapshots()
        ORDER BY snapshot_id DESC
        LIMIT 20;
    """).fetchdf()

    print("\n=== Recent DuckLake Snapshots ===\n")
    for _, row in result.iterrows():
        print(f"Snapshot {row['snapshot_id']} | {row['snapshot_time']}")
        if row['author']:
            print(f"  Author: {row['author']}")
        if row['commit_message']:
            print(f"  Message: {row['commit_message']}")
        if row['commit_extra_info']:
            print(f"  Extra: {row['commit_extra_info']}")
        print(f"  Changes: {row['changes']}")
        print()

    conn.close()

if __name__ == "__main__":
    main()
