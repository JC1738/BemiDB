package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"slices"
	"sync/atomic"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	COMMAND_START   = "start"
	COMMAND_VERSION = "version"

	DUCKDB_SCHEMA_MAIN = "main"
)

func main() {
	config := LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	// Debug: Verify configuration loading
	common.LogDebug(config.CommonConfig, "=== Configuration Debug ===")
	common.LogDebug(config.CommonConfig, fmt.Sprintf("DuckLake.CatalogUrl: '%s'", config.CommonConfig.Ducklake.CatalogUrl))
	common.LogDebug(config.CommonConfig, fmt.Sprintf("DuckLake.CatalogName: '%s'", config.CommonConfig.Ducklake.CatalogName))
	common.LogDebug(config.CommonConfig, fmt.Sprintf("DuckLake.DataPath: '%s'", config.CommonConfig.Ducklake.DataPath))
	common.LogDebug(config.CommonConfig, fmt.Sprintf("R2.AccountId: '%s'", config.CommonConfig.R2.AccountId))
	common.LogDebug(config.CommonConfig, "R2.AccessKeyId: [REDACTED]")
	common.LogDebug(config.CommonConfig, "R2.SecretAccessKey: [REDACTED]")
	common.LogDebug(config.CommonConfig, fmt.Sprintf("CatalogDatabaseUrl: '%s'", config.CommonConfig.CatalogDatabaseUrl))
	common.LogDebug(config.CommonConfig, "=== End Configuration ===")

	if config.CommonConfig.LogLevel == common.LOG_LEVEL_TRACE {
		go enableProfiling()
	}

	tcpListener := NewTcpListener(config)
	common.LogInfo(config.CommonConfig, "BemiDB: Listening on", tcpListener.Addr())

	// Phase 1: Create DuckDB client with basic initialization (no pg_catalog yet)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, duckdbBootQueriesPhase1(config))
	common.LogInfo(config.CommonConfig, "DuckDB: Connected")
	defer duckdbClient.Close()

	// Phase 2: Initialize DuckLake (for DuckLake mode)
	if config.CommonConfig.Ducklake.CatalogUrl != "" {
		ctx := context.Background()
		err := duckdbClient.InitializeDucklake(ctx)
		common.PanicIfError(config.CommonConfig, err)
		common.LogInfo(config.CommonConfig, "DuckLake: Initialized")
	}

	// Phase 3: Create pg_catalog tables/views
	ctx := context.Background()
	for _, query := range duckdbBootQueriesPhase2(config) {
		_, err := duckdbClient.ExecContext(ctx, query)
		common.PanicIfError(config.CommonConfig, err)
	}
	common.LogInfo(config.CommonConfig, "pg_catalog: Initialized")

	queryHandler := NewQueryHandler(config, duckdbClient)

	// Connection limiting to prevent resource exhaustion
	connectionSemaphore := make(chan struct{}, config.MaxConnections)
	common.LogInfo(config.CommonConfig, "BemiDB: Max concurrent connections:", common.IntToString(config.MaxConnections))

	var connectionCount int64 = 0
	for {
		// Block if at max connections (semaphore pattern)
		connectionSemaphore <- struct{}{}

		conn := AcceptConnection(config, tcpListener)
		atomic.AddInt64(&connectionCount, 1)
		common.LogInfo(config.CommonConfig, "BemiDB: Accepted", common.Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
		server := NewPostgresServer(config, &conn)

		go func() {
			defer func() { <-connectionSemaphore }() // Release semaphore slot
			server.Run(queryHandler)
			defer server.Close()
			common.LogInfo(config.CommonConfig, "BemiDB: Closed", common.Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
			atomic.AddInt64(&connectionCount, -1)
		}()
	}
}

// Phase 1: Basic DuckDB setup (before cache is built)
func duckdbBootQueriesPhase1(config *Config) []string {
	var bootQueries []string

	// Use DuckLake boot queries if DuckLake is configured, otherwise use Iceberg
	if config.CommonConfig.Ducklake.CatalogUrl != "" {
		bootQueries = common.GetDucklakeServerBootQueries(config.CommonConfig)
	} else {
		bootQueries = []string{
			// Set up Iceberg (legacy)
			"INSTALL iceberg",
			"LOAD iceberg",
			"SET memory_limit='3GB'",
			"SET threads=2",
			"SET scalar_subquery_error_on_multiple_rows=false",
		}
	}

	return slices.Concat(
		bootQueries,
		[]string{
			// Set up schemas
			"SELECT oid FROM pg_catalog.pg_namespace",
			"CREATE SCHEMA " + PG_SCHEMA_PUBLIC,
		},
	)
}

// Phase 2: pg_catalog setup
func duckdbBootQueriesPhase2(config *Config) []string {
	return slices.Concat(
		// Create pg-compatible functions
		CreatePgCatalogMacroQueries(config),
		CreateInformationSchemaMacroQueries(config),

		// Create pg-compatible tables and views
		CreatePgCatalogTableQueries(config, nil),
		CreateInformationSchemaTableQueries(config),

		// Use the public schema
		[]string{"USE " + PG_SCHEMA_PUBLIC},
	)
}

func enableProfiling() {
	func() { log.Println(http.ListenAndServe(":6060", nil)) }()
}
