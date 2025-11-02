# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BemiDB is an open-source Postgres-compatible analytical database that combines a data syncing layer (similar to Fivetran) with a query engine (similar to Snowflake). It syncs data from various sources, stores it in compressed columnar Parquet format in S3 using the Iceberg table format, and serves it via a Postgres-compatible query interface powered by DuckDB.

## Development Environment

### Prerequisites
- **Devbox**: The project uses [Devbox](https://www.jetify.com/devbox) for dependency management
- **Required packages**: Go 1.24.4, PostgreSQL (managed by devbox.json)
- **Environment variables**: Copy `.env.sample` to `.env` and configure:
  - AWS credentials (`AWS_REGION`, `AWS_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - Catalog database URL (`CATALOG_DATABASE_URL`)
  - Source-specific credentials for syncers

### Setup Commands
```bash
# Enter devbox shell with environment variables
make sh

# Install dependencies (runs go mod tidy across all modules)
make install

# Build Docker image
make build

# Build local Docker image for testing
make local-build
```

## Code Architecture

### Module Structure
The codebase is organized into multiple Go modules:

- **`src/common/`**: Shared utilities and core infrastructure
  - Iceberg catalog operations and table writing (`iceberg_*.go`)
  - DuckDB client wrapper (`duckdb_client.go`)
  - S3 storage layer (`storage_*.go`, `s3_client.go`)
  - Data type conversions between Postgres/Iceberg/Parquet
  - Error handling and logging utilities

- **`src/server/`**: BemiDB query server
  - Implements Postgres wire protocol (`postgres_server.go`)
  - Query handling and DuckDB integration (`query_handler.go`)
  - SQL parsing and query remapping (`parser_*.go`, `query_remapper*.go`)
  - Converts Postgres queries to DuckDB-compatible SQL
  - Creates pg_catalog and information_schema compatibility layer (`pg_constants.go`)

- **`src/syncer-postgres/`**: Postgres data syncer
  - `main.go`: Entry point that delegates to lib
  - `lib/`: Core syncing logic
    - Full-refresh sync implementation
    - Schema introspection and column mapping
    - Data extraction and transformation

- **`src/syncer-amplitude/`**: Amplitude analytics syncer
  - Incremental syncing from Amplitude Export API
  - `lib/`: API client and data parsing

- **`src/syncer-attio/`**: Attio CRM syncer
  - Full-refresh syncing from Attio API
  - `lib/`: Record parsers for companies, people, deals

### Key Architectural Patterns

1. **Query Flow** (server):
   - Client connects via Postgres protocol → `postgres_server.go`
   - Query parsed using pg_query_go → `parser_*.go`
   - Query rewritten for DuckDB compatibility → `query_remapper*.go`
   - Executed via DuckDB → `query_handler.go`
   - Results formatted as Postgres wire protocol → `response_handler.go`

2. **Data Sync Flow** (syncers):
   - Extract data from source (API or Postgres connection)
   - Convert to Iceberg schema via `common/iceberg_schema_column.go`
   - Write Parquet files and Iceberg metadata via `common/iceberg_table_writer.go`
   - Store in S3 with catalog updates in Postgres

3. **Storage Layer**:
   - Uses Apache Iceberg open table format
   - Parquet files stored in S3 (or S3-compatible storage like MinIO)
   - Metadata catalog stored in separate Postgres database
   - DuckDB reads directly from Iceberg tables in S3

## Testing & Development

### Running Tests
```bash
# Run all tests (builds test Docker image)
make test

# Run specific test function
make test-function FUNC=TestFunctionName

# Debug tests with Delve
make debug

# Interactive Go REPL
make console
```

### Local Development Workflow

**Option 1: Docker-based (recommended for full integration testing)**
```bash
# Build local image
make local-build

# Run server locally
make local-server

# Run syncers locally
make local-syncer-postgres
make local-syncer-amplitude
make local-syncer-attio

# Get shell access
make local-sh
```

**Option 2: Direct Go execution (faster iteration)**
```bash
# Enter devbox shell
make sh

# Run server directly (requires proper env vars)
cd src/server && go run .

# Run tests with live reload
cd src/server && go test ./... -v
```

### Linting & Code Quality
```bash
# Run linters (go fmt, staticcheck, deadcode)
make lint
```

The lint command runs across all modules and checks:
- Code formatting (`go fmt`)
- Static analysis (`staticcheck`)
- Dead code detection (`deadcode`)

### Benchmarking
```bash
# Benchmark BemiDB performance
make benchmark

# TPC-H benchmark setup
make tpch-install      # Clone and build TPC-H tools
make tpch-generate     # Generate test data
make pg-create         # Load data into Postgres for comparison
make pg-benchmark      # Benchmark Postgres
```

### Debugging & Profiling
```bash
# Network traffic inspection
make sniff        # BemiDB traffic on port 54321
make pg-sniff     # Postgres traffic on port 5432

# Memory profiling (when BEMIDB_LOG_LEVEL=TRACE)
make profile-mem
make measure-mem
```

## Common Development Patterns

### Adding a New Data Syncer
1. Create `src/syncer-[source]/` directory with `main.go` and `lib/` subdirectory
2. Implement config loading, API client, and sync logic in `lib/`
3. Register flags in `init()` function in `main.go`
4. Add data type mappings to Iceberg schema
5. Update Dockerfile to compile new binary
6. Add Make target and docker/bin/run.sh case
7. Update README with usage documentation

### Modifying Query Compatibility
- Add new function mappings in `src/server/query_remapper_function.go`
- Extend pg_catalog views in `src/server/query_remapper_table.go`
- Add parser support in `src/server/parser_*.go` if needed
- Test against actual Postgres clients (psql, Metabase, DBeaver, etc.)

### Working with Iceberg/Parquet
- Schema definitions: `src/common/iceberg_schema_column.go`
- Type conversions: `src/common/storage_utils.go`
- Table operations: `src/common/iceberg_table_writer.go`
- DuckDB integration: `src/common/duckdb_client.go`

## Docker Packaging

The project builds a single multi-purpose Docker image:
- Base: Debian 12 slim with PostgreSQL client
- Compiled binaries: server, syncer-postgres, syncer-amplitude, syncer-attio
- Entrypoint: `/app/bin/run.sh` routes to appropriate binary based on first argument
- Supports both linux/amd64 and linux/arm64 platforms

## Production Deployment

### Quick Start
See **`DEPLOYMENT_PLAN.md`** for comprehensive production deployment guide covering:
- Fly.io server deployment
- Neon PostgreSQL catalog setup
- Cloudflare R2 storage configuration
- GitHub Actions cron scheduling
- Complete step-by-step instructions

### Recommended Stack
- **Server**: Fly.io (Postgres wire protocol, ~$30/month)
- **Catalog**: Neon PostgreSQL (managed, ~$5/month)
- **Storage**: Cloudflare R2 (S3-compatible, ~$1.50/month for 100GB)
- **Cron**: GitHub Actions (free for public repos)
- **Total**: ~$36/month (vs $300-1000 for Snowflake + Fivetran)

### Code Modification: Independent Table Syncing

**IMPORTANT**: The `DeleteOldTables()` function has been disabled in `src/syncer-postgres/lib/syncer_full_refresh.go:39` to support independent table syncing.

**What this means:**
- ✅ You can sync different tables on different schedules
- ✅ Tables not in current sync are preserved (not deleted)
- ⚠️ Manual cleanup required for renamed/removed tables: `DROP TABLE schema.old_table`

**Example:**
```bash
# Hourly: Sync fast-changing tables
docker run -e SOURCE_POSTGRES_INCLUDE_TABLES=public.users,public.sessions ...

# Daily: Sync slow-changing tables
docker run -e SOURCE_POSTGRES_INCLUDE_TABLES=public.analytics,public.reports ...

# Result: Both sets of tables coexist in BemiDB
```

**To re-enable old behavior** (delete tables not in sync):
Uncomment line 39 in `src/syncer-postgres/lib/syncer_full_refresh.go`

## Important Notes

- **Go version**: Project uses Go 1.24.4 (specified in devbox.json and Dockerfile)
- **CGO requirement**: DuckDB Go bindings require CGO_ENABLED=1
- **Module dependencies**: Use `replace` directives for local common module
- **Version**: Defined in `src/common/common_config.go` as a constant
- **Postgres compatibility**: Server implements Postgres wire protocol but DuckDB limitations apply
- **JSON columns**: Stored as strings with JSON logical type; use `->>` operator for querying
- **Catalog requirement**: Separate Postgres database required for Iceberg metadata (cannot use external Iceberg catalogs like R2's catalog service)
- **S3 compatibility**: Works with any S3-compatible storage (AWS S3, Cloudflare R2, MinIO, etc.) via `AWS_S3_ENDPOINT`
