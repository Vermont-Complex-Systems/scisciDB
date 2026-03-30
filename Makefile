.PHONY: all download parse reparse load load-view load-table create-lookups enrich export help clean

DB ?= s2
ENTITY ?=
TABLE ?= all
GROUP_BY ?= field
FIELD ?= all
RELEASE ?=

# Simple timing wrapper
TIME := time -p

download:
	@echo "=== DOWNLOAD: $(DB) $(ENTITY) ==="
	@$(TIME) uv run python input/download.py $(DB) $(ENTITY)

# Format standardization (JSON → Parquet)
parse:
	@echo "=== PARSE: $(DB) $(ENTITY) ==="
	@$(TIME) uv run python import/parse.py $(DB) $(ENTITY) $(if $(filter 1,$(FORCE)),--force)

# Database ingestion (Parquet → DuckLake)
load-view:
	@echo "=== LOAD VIEW: $(DB) $(ENTITY) ==="
	@$(TIME) uv run python load/load.py $(DB) $(ENTITY) --view

load-table:
	@echo "=== LOAD TABLE: $(DB) $(ENTITY) ==="
	@$(TIME) uv run python load/load.py $(DB) $(ENTITY) --no-cleanup

# Create lookup/mapping tables (dependencies for enrichment)
create-lookups:
	@echo "=== CREATE LOOKUPS: Cross-database mapping tables ==="
	@$(TIME) uv run python enrich/create_lookups.py

# Feature engineering and derived fields (depends on lookups)
enrich: create-lookups
	@echo "=== ENRICH: Add derived fields ($(FIELD)) ==="
	@$(TIME) uv run python enrich/add_derived_fields.py --operation $(FIELD)


# export-arxiv-fulltext:
# 	@echo "=== EXPORT: Create arXiv fulltext materialized view ==="
# 	python export/arxiv_fulltext.py

# export-arxiv-fulltext-force:
# 	@echo "=== EXPORT: Force recreate arXiv fulltext materialized view ==="
# 	python export/arxiv_fulltext.py --force


clean:
	@echo "=== CLEAN: Removing temporary files ==="
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type f -name ".DS_Store" -delete


# ============================================================================
# HELP
# ============================================================================
help:
	@echo "ScisciDB Data Pipeline"
	@echo ""
	@echo "Usage: make [target] [options]"
	@echo ""
	@echo "Main Targets:"
	@echo "  download        Download raw data"
	@echo "  parse           Convert JSON to Parquet"
	@echo "  load-view       Create database views (recommended)"
	@echo "  load-table      Create database tables"
	@echo "  create-lookups  Create cross-database mapping tables"
	@echo "  enrich          Add derived fields (depends on lookups)"
	@echo "  export          Export data/counts"
	@echo ""
	@echo "Examples:"
	@echo "  make download DB=s2 ENTITY=papers     # S2 requires entity"
	@echo "  make download DB=openalex             # OpenAlex downloads full snapshot"
	@echo "  make parse DB=s2 ENTITY=papers"
	@echo "  make load-view DB=s2 ENTITY=papers"
	@echo ""
	@echo "Options:"
	@echo "  FORCE=1      Force overwrite existing data"
	@echo "  RELEASE=xxx  Use specific S2 release"
