#!/bin/bash
#
# DuckLake-specific entrypoint for BemiDB Docker container
# This script validates DuckLake environment variables and starts the server
# Syncers are not supported in DuckLake mode

set -euo pipefail

case "${1:-server}" in
  bash)
    exec /bin/bash
    ;;
  server)
    # Validate required DuckLake catalog configuration
    : "${DUCKLAKE_CATALOG_URL:?Environment variable DUCKLAKE_CATALOG_URL must be set}"
    : "${DUCKLAKE_CATALOG_NAME:?Environment variable DUCKLAKE_CATALOG_NAME must be set}"
    : "${DUCKLAKE_DATA_PATH:?Environment variable DUCKLAKE_DATA_PATH must be set}"

    # Validate required R2 storage credentials
    : "${R2_ACCOUNT_ID:?Environment variable R2_ACCOUNT_ID must be set}"
    : "${R2_ACCESS_KEY_ID:?Environment variable R2_ACCESS_KEY_ID must be set}"
    : "${R2_SECRET_ACCESS_KEY:?Environment variable R2_SECRET_ACCESS_KEY must be set}"

    # Optional: Validate R2 path format
    if [[ ! "$DUCKLAKE_DATA_PATH" =~ ^r2:// ]]; then
      echo "WARNING: DUCKLAKE_DATA_PATH should start with 'r2://' (got: $DUCKLAKE_DATA_PATH)"
    fi

    echo "Starting BemiDB server with DuckLake integration..."
    echo "  Catalog: $DUCKLAKE_CATALOG_NAME"
    echo "  Data path: $DUCKLAKE_DATA_PATH"
    echo "  Port: ${BEMIDB_PORT:-54321}"
    echo "  Database: ${BEMIDB_DATABASE:-bemidb}"
    echo ""

    ./bin/server
    ;;
  *)
    echo "Unknown argument: ${1:-}"
    echo "Available options: server, bash"
    echo ""
    echo "Note: Syncers are not supported in DuckLake mode."
    echo "The DuckLake catalog is managed externally."
    exit 1
    ;;
esac
