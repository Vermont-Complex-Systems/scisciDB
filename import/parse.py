#!/usr/bin/env python3
"""
IMPORT step: Parse and convert raw JSON to Parquet
Follows principled data processing - converts input/ to import/
"""
import argparse
import sys
from pathlib import Path
import duckdb
import json
import os
from dotenv import load_dotenv
from typing import Callable, List, Tuple, Optional

from pyprojroot import here
PROJECT_ROOT = here()

sys.path.append(str(PROJECT_ROOT))
from schemas import sources_columns, works_columns, s2_papers_columns

load_dotenv()

class ParseError(Exception):
    """Exception raised when parsing fails"""
    pass

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_json_files(dataset_dir: Path) -> List[Path]:
    """Find all valid JSON files in dataset directory"""
    # Find all JSON files (use set to avoid duplicates)
    json_files = set()
    json_files.update(dataset_dir.glob("*.json"))
    json_files.update(dataset_dir.glob("*.json.gz"))
    json_files.update(dataset_dir.glob("**/*.gz"))

    # Convert back to list and filter out metadata files
    json_files = [
        f for f in json_files
        if not f.name.startswith(('release_', 'download_', 'parse_manifest'))
    ]
    
    # Filter out empty files
    valid_files = []
    for json_file in json_files:
        file_size = json_file.stat().st_size
        if file_size < 50:
            print(f"[IMPORT] ⚠️  Skipping empty file: {json_file.name} ({file_size} bytes)")
        else:
            valid_files.append(json_file)
    
    return valid_files

def cleanup_parquet_files(dataset_dir: Path) -> None:
    """Remove empty or broken parquet files"""
    parquet_files = (
        list(dataset_dir.glob("*.parquet")) +
        list(dataset_dir.glob("**/*.parquet"))
    )
    
    removed_count = 0
    for parquet_file in parquet_files:
        if parquet_file.stat().st_size < 130:
            print(f"[IMPORT]   Removing: {parquet_file.name} ({parquet_file.stat().st_size} bytes)")
            parquet_file.unlink()
            removed_count += 1
    
    if removed_count > 0:
        print(f"[IMPORT] Removed {removed_count} empty parquet files")

def should_skip_conversion(json_file: Path, force: bool = False) -> Tuple[bool, Path]:
    """Check if conversion should be skipped (parquet already exists)"""
    output_file = json_file.parent / (json_file.stem.replace('.json', '') + '.parquet')
    
    if force:
        return False, output_file
    
    if output_file.exists() and output_file.stat().st_size > 100:
        return True, output_file
    
    return False, output_file

def process_file(
    conn: duckdb.DuckDBPyConnection,
    json_file: Path,
    converter: Callable,
    force: bool = False
) -> Tuple[bool, str]:
    """
    Process a single JSON file
    
    Returns:
        Tuple of (success, output_filename)
    """
    should_skip, output_file = should_skip_conversion(json_file, force)
    
    if should_skip:
        size_mb = output_file.stat().st_size / 1024 / 1024
        print(f"[IMPORT]   ✓ Already exists ({size_mb:.1f} MB), skipping")
        return True, output_file.name
    
    try:
        # Convert the file
        converter(conn, json_file, output_file)
        
        # Log results
        input_size_mb = json_file.stat().st_size / 1024 / 1024
        output_size_mb = output_file.stat().st_size / 1024 / 1024
        compression_ratio = (1 - output_size_mb / input_size_mb) * 100
        
        print(f"[IMPORT]   ✓ {output_file.name}")
        print(f"[IMPORT]     {input_size_mb:.1f} MB → {output_size_mb:.1f} MB "
              f"({compression_ratio:.0f}% smaller)")
        
        return True, output_file.name
        
    except Exception as e:
        print(f"[IMPORT]   ✗ Failed: {e}")
        return False, ""

def create_manifest(
    dataset_dir: Path,
    db_name: str,
    dataset_name: str,
    converter_name: str,
    total_files: int,
    parsed_files: List[str]
) -> None:
    """Create parsing manifest file"""
    manifest = {
        "database": db_name,
        "dataset_name": dataset_name,
        "converter": converter_name,
        "input_files": total_files,
        "parsed_files": len(parsed_files),
        "files": parsed_files,
        "output_dir": str(dataset_dir)
    }
    
    # Add special metadata for s2orc_v2
    if dataset_name == 's2orc_v2':
        manifest["annotation_types"] = {
            "section_header": "STRUCT(start BIGINT, end BIGINT, attributes JSON)[]",
            "sentence": "STRUCT(start BIGINT, end BIGINT)[]",
            "paragraph": "STRUCT(start BIGINT, end BIGINT)[]",
            "bib_ref": "STRUCT(start BIGINT, end BIGINT, ref_id VARCHAR)[]"
        }
    
    manifest_file = dataset_dir / "parse_manifest.json"
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"[IMPORT] Manifest: {manifest_file.name}")


# ============================================================================
# CONVERTERS
# ============================================================================

def convert_s2_papers(conn: duckdb.DuckDBPyConnection, json_file: Path, output_file: Path) -> None:
    """
    S2 Papers converter with explicit schema to prevent type inference issues
    Ensures all external IDs are properly typed as strings
    """
    conn.execute(f"""
        COPY (
            SELECT * FROM read_json('{json_file}', columns={s2_papers_columns})
        )
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

def convert_generic(conn: duckdb.DuckDBPyConnection, json_file: Path, output_file: Path) -> None:
    """Generic JSON to Parquet converter using DuckDB auto-inference"""
    conn.execute(f"""
        COPY (SELECT * FROM read_json_auto('{json_file}'))
        TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'zstd')
    """)

def convert_s2orc_v2(conn: duckdb.DuckDBPyConnection, json_file: Path, output_file: Path) -> None:
    """
    S2ORC v2 converter: Parse nested annotations to proper STRUCT arrays
    Converts VARCHAR JSON strings → STRUCT types for fast queries
    """
    conn.execute(f"""
        COPY (
            SELECT 
                corpusid,
                openaccessinfo,
                title,
                authors,
                STRUCT_PACK(
                    text := body.text,
                    annotations := STRUCT_PACK(
                        section_header := from_json(
                            body.annotations.section_header,
                            '[{{"start": "BIGINT", "end": "BIGINT", "attributes": "JSON"}}]'
                        ),
                        sentence := from_json(
                            body.annotations.sentence,
                            '[{{"start": "BIGINT", "end": "BIGINT"}}]'
                        ),
                        paragraph := from_json(
                            body.annotations.paragraph,
                            '[{{"start": "BIGINT", "end": "BIGINT"}}]'
                        ),
                        bib_ref := from_json(
                            body.annotations.bib_ref,
                            '[{{"start": "BIGINT", "end": "BIGINT", "ref_id": "VARCHAR"}}]'
                        )
                    )
                ) as body,
                STRUCT_PACK(
                    text := bibliography.text,
                    annotations := from_json(
                        bibliography.annotations,
                        '{{"bib_entry": "VARCHAR", "bib_id": "JSON", "bib_title": "VARCHAR", "bib_venue": "VARCHAR", "bib_author_first_name": "JSON", "bib_author_last_name": "JSON"}}'
                    )
                ) as bibliography
            FROM read_json_auto('{json_file}')
        )
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION 'zstd')
    """)

def convert_openalex_works(conn: duckdb.DuckDBPyConnection, json_file: Path, output_file: Path) -> None:
    """
    OpenAlex Works converter with explicit schema
    Defines precise types to prevent auto-inference conflicts
    """
    conn.execute(f"""
        COPY (
            SELECT * FROM read_json(
                '{json_file}',
                columns={works_columns},
                ignore_errors=true
            )
        )
        TO '{output_file}' 
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

def convert_openalex_sources(conn: duckdb.DuckDBPyConnection, json_file: Path, output_file: Path) -> None:
    """
    OpenAlex Sources converter with explicit schema
    Defines precise types to prevent auto-inference conflicts
    """
    conn.execute(f"""
        COPY (
            SELECT * FROM read_json(
                '{json_file}',
                columns={sources_columns},
                ignore_errors=true
            )
        )
        TO '{output_file}' 
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

def get_converter(db_name: str, dataset_name: str) -> Tuple[Callable, str]:
    """
    Select appropriate converter based on database and dataset

    Returns:
        Tuple of (converter_function, converter_name)
    """
    if dataset_name == 's2orc_v2':
        return convert_s2orc_v2, "s2orc_v2"

    if db_name == 's2':
        if dataset_name == 'papers':
            return convert_s2_papers, "s2_papers"

    if db_name == 'openalex':
        if dataset_name == 'works':
            return convert_openalex_works, "openalex_works"
        elif dataset_name == 'sources':
            return convert_openalex_sources, "openalex_sources"

    return convert_generic, "generic"

def parse_json_to_parquet(db_name: str, entity: Optional[str] = None, force: bool = False) -> Path:
    """
    Convert JSON/JSON.GZ files to Parquet format

    Args:
        db_name: Database name (s2, openalex)
        entity: Name of entity (papers, works, authors, etc.)
        force: Force reparse even if parquet exists

    Returns:
        Path to output directory with Parquet files
    """
    # Setup paths
    if db_name == 's2':
        if not entity:
            raise ParseError("S2 parsing requires an entity (papers, authors, etc.)")
        data_root = Path(os.getenv("S2_DATA_ROOT"))
        dataset_dir = data_root / entity
        return _parse_single_dataset(dataset_dir, db_name, entity, force)
    else:  # openalex
        data_root = Path(os.getenv("OA_DATA_ROOT"))
        if entity:
            # Parse specific entity
            dataset_dir = data_root / "data" / entity
            return _parse_single_dataset(dataset_dir, db_name, entity, force)
        else:
            # Parse all entities in OpenAlex snapshot
            return _parse_openalex_all(data_root, force)


def _parse_single_dataset(dataset_dir: Path, db_name: str, entity: str, force: bool = False) -> Path:
    """Parse a single dataset directory"""
    # Select converter
    converter, converter_name = get_converter(db_name, entity)

    # Log start
    print(f"[IMPORT] Parsing {entity} entity")
    print(f"[IMPORT] Database: {db_name}")
    print(f"[IMPORT] Converter: {converter_name}")
    print(f"[IMPORT] Location: {dataset_dir}")

    # Validate dataset directory
    if not dataset_dir.exists():
        raise ParseError(f"Dataset directory not found: {dataset_dir}")

    # Find JSON files
    json_files = find_json_files(dataset_dir)
    if not json_files:
        raise ParseError(f"No JSON files found in {dataset_dir}")

    print(f"[IMPORT] Found {len(json_files)} files to parse")

    # Cleanup if forcing reparse
    if force:
        print("[IMPORT] Force mode: cleaning up existing parquet files")
        cleanup_parquet_files(dataset_dir)

    # Setup DuckDB connection
    conn = duckdb.connect()

    # Process all files
    parsed_files = []
    for i, json_file in enumerate(sorted(json_files), 1):
        print(f"[IMPORT] Processing {i}/{len(json_files)}: {json_file.name}")

        success, output_filename = process_file(conn, json_file, converter, force)
        if success:
            parsed_files.append(output_filename)

    # Verify we parsed something
    if not parsed_files:
        raise ParseError("No files were successfully parsed")

    # Create manifest
    create_manifest(
        dataset_dir,
        db_name,
        entity,
        converter_name,
        len(json_files),
        parsed_files
    )

    # Log completion
    print(f"\n[IMPORT] ✓ Parse complete!")
    print(f"[IMPORT] Parsed: {len(parsed_files)}/{len(json_files)} files")
    print(f"[IMPORT] Location: {dataset_dir}/")

    return dataset_dir


def _parse_openalex_all(data_root: Path, force: bool = False) -> Path:
    """Parse all entities in OpenAlex snapshot"""
    data_dir = data_root / "data"

    if not data_dir.exists():
        raise ParseError(f"OpenAlex data directory not found: {data_dir}")

    # Find all entity directories
    entity_dirs = [d for d in data_dir.iterdir() if d.is_dir()]

    if not entity_dirs:
        raise ParseError(f"No entity directories found in {data_dir}")

    print(f"[IMPORT] Parsing OpenAlex full snapshot")
    print(f"[IMPORT] Found {len(entity_dirs)} entities to parse")

    for entity_dir in entity_dirs:
        entity_name = entity_dir.name
        print(f"\n[IMPORT] === Parsing entity: {entity_name} ===")
        try:
            _parse_single_dataset(entity_dir, "openalex", entity_name, force)
        except ParseError as e:
            print(f"[IMPORT] ⚠️  Skipping {entity_name}: {e}")
            continue

    print(f"\n[IMPORT] ✓ OpenAlex full snapshot parsing complete!")
    return data_dir


def main():
    parser = argparse.ArgumentParser(
        description="IMPORT step: Parse JSON to Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "db_name",
        help="Database name (s2, openalex)"
    )

    parser.add_argument(
        "entity",
        nargs="?",
        help="Entity name (required for s2: papers, s2orc_v2; optional for openalex: works, sources, etc.)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reparse all files, even if parquet already exists"
    )
    
    args = parser.parse_args()
    
    try:
        if args.force:
            print("🔄 Force mode enabled - will reparse all files\n")
        
        parse_json_to_parquet(args.db_name, args.entity, args.force)
        
        print(f"\n🎉 SUCCESS!")
        print(f"\n📋 Next steps:")
        print(f"   1. Run 'make validate' to check data quality")
        print(f"   2. Run 'make export' to load into database")
        
    except ParseError as e:
        print(f"❌ {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()