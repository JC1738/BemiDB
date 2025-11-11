package common

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

var SYNCER_DUCKDB_BOOT_QUERIES = []string{
	"SET memory_limit='2GB'",
	"SET threads=2",
	// DuckLake syncers are disabled - this array kept for compatibility
	// If syncers are re-enabled, add DuckLake extension here
}

// GetDucklakeServerBootQueries returns DuckLake boot queries with dynamic configuration
func GetDucklakeServerBootQueries(config *CommonConfig) []string {
	// Determine thread count: use config value if set, otherwise auto-detect
	numCPU := config.Threads
	if numCPU == 0 {
		numCPU = runtime.NumCPU()
	}

	// Determine memory limit: use config value if set, otherwise use default
	memoryLimit := config.MemoryLimit
	if memoryLimit == "" {
		memoryLimit = DEFAULT_MEMORY_LIMIT
	}

	queries := []string{
		fmt.Sprintf("SET memory_limit='%s'", memoryLimit),
		fmt.Sprintf("SET threads TO %d", numCPU), // Use all available CPU cores for better JOIN performance
		"SET scalar_subquery_error_on_multiple_rows=false",
		// HTTP/Cache optimizations for R2/S3 performance
		"SET enable_http_metadata_cache=true", // Cache Parquet metadata (saves 200-500ms per query)
		"SET http_retries=3",                   // Retry failed R2 requests
		"SET http_retry_wait_ms=100",           // Wait between retries
		"SET http_keep_alive=true",             // Reuse HTTP connections
		"SET http_timeout=30000",               // 30 second timeout for HTTP requests (prevents indefinite hangs)
		"SET enable_object_cache=true",         // Cache remote objects
		// Resource optimizations
		fmt.Sprintf("SET s3_uploader_thread_limit=%d", numCPU*2), // Optimize S3 uploader threads
		"SET checkpoint_threshold='256MB'",                       // Higher threshold for read-only workloads
		"SET preserve_insertion_order=false",                     // Allow reordering for better memory efficiency
		"INSTALL ducklake",
		"LOAD ducklake",
	}

	// Add temp directory if configured
	if config.TempDirectory != "" {
		queries = append([]string{fmt.Sprintf("SET temp_directory='%s'", config.TempDirectory)}, queries...)
	}

	return queries
}

type DuckdbClient struct {
	Config    *CommonConfig
	Db        *sql.DB
	Connector *duckdb.Connector
}

func NewDuckdbClient(config *CommonConfig, bootQueries ...[]string) *DuckdbClient {
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	PanicIfError(config, err)
	db := sql.OpenDB(connector)
	PanicIfError(config, err)

	// Configure connection pool for optimal DuckDB concurrency
	numCPU := runtime.NumCPU()
	db.SetMaxOpenConns(numCPU * 4)           // Allow concurrent queries (4x CPU cores)
	db.SetMaxIdleConns(numCPU)               // Keep warm connections (1x CPU cores)
	db.SetConnMaxLifetime(30 * time.Minute)  // Prevent stale connections
	db.SetConnMaxIdleTime(5 * time.Minute)   // Close idle connections after 5 minutes

	client := &DuckdbClient{
		Config:    config,
		Db:        db,
		Connector: connector,
	}

	queries := []string{
		"SET timezone='UTC'",
	}
	if bootQueries != nil {
		queries = append(queries, bootQueries[0]...)
	}
	for _, query := range queries {
		_, err := client.ExecContext(ctx, query)
		PanicIfError(config, err)
	}

	client.setExplicitAwsCredentials(ctx)

	if IsLocalHost(config.Aws.S3Endpoint) {
		_, err = client.ExecContext(ctx, "SET s3_use_ssl=false")
		PanicIfError(config, err)
	}

	if config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
		// Use endpoint/bucket/key (path, deprecated on AWS) instead of bucket.endpoint/key (vhost)
		_, err = client.ExecContext(ctx, "SET s3_url_style='path'")
		PanicIfError(config, err)
	}

	if config.LogLevel == LOG_LEVEL_TRACE {
		_, err = client.ExecContext(ctx, "PRAGMA enable_logging('HTTP')")
		PanicIfError(config, err)
		_, err = client.ExecContext(ctx, "SET logging_storage = 'stdout'")
		PanicIfError(config, err)
	}

	return client
}

func (client *DuckdbClient) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	start := time.Now()
	LogDebug(client.Config, "Querying DuckDB:", query)
	rows, err := client.Db.QueryContext(ctx, query)
	duration := time.Since(start)

	// Log slow queries (> 1 second) at INFO level for production observability
	if duration > 1*time.Second {
		LogInfo(client.Config, fmt.Sprintf("Slow query (%.2fs): %s", duration.Seconds(), query))
	}

	return rows, err
}

func (client *DuckdbClient) QueryRowContext(ctx context.Context, query string, args ...map[string]string) *sql.Row {
	LogDebug(client.Config, "Querying DuckDB row:", query)
	if len(args) == 0 {
		return client.Db.QueryRowContext(ctx, query)
	}
	return client.Db.QueryRowContext(ctx, replaceNamedStringArgs(query, args[0]))
}

func (client *DuckdbClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	LogDebug(client.Config, "Preparing DuckDB statement:", query)
	return client.Db.PrepareContext(ctx, query)
}

func (client *DuckdbClient) ExecContext(ctx context.Context, query string, args ...map[string]string) (sql.Result, error) {
	LogDebug(client.Config, "Executing DuckDB:", query)
	if len(args) == 0 {
		return client.Db.ExecContext(ctx, query)
	}

	return client.Db.ExecContext(ctx, replaceNamedStringArgs(query, args[0]))
}

func (client *DuckdbClient) ExecTransactionContext(ctx context.Context, queries []string, args ...[]map[string]string) error {
	tx, err := client.Db.Begin()
	LogDebug(client.Config, "Executing DuckDB: BEGIN")
	if err != nil {
		return err
	}

	for i, query := range queries {
		LogDebug(client.Config, "Executing DuckDB in transaction:", query)
		var err error
		if len(args) == 0 {
			_, err = tx.ExecContext(ctx, query)
		} else {
			_, err = tx.ExecContext(ctx, replaceNamedStringArgs(query, args[0][i]))
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	LogDebug(client.Config, "Executing DuckDB: COMMIT")
	return tx.Commit()
}

func (client *DuckdbClient) Appender(schema string, table string) (*duckdb.Appender, error) {
	conn, err := client.Connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return duckdb.NewAppenderFromConn(conn, schema, table)
}

func (client *DuckdbClient) Close() {
	client.Db.Close()
}

// InitializeDucklake sets up R2 secret and attaches DuckLake catalog
func (client *DuckdbClient) InitializeDucklake(ctx context.Context) error {
	config := client.Config

	// Validate DuckLake configuration
	if config.Ducklake.CatalogUrl == "" {
		return fmt.Errorf("DUCKLAKE_CATALOG_URL not set")
	}
	if config.Ducklake.DataPath == "" {
		return fmt.Errorf("DUCKLAKE_DATA_PATH not set")
	}
	if config.R2.AccountId == "" || config.R2.AccessKeyId == "" || config.R2.SecretAccessKey == "" {
		return fmt.Errorf("R2 credentials not set (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)")
	}

	// Create R2 secret for data access
	createSecretQuery := fmt.Sprintf(`
		CREATE OR REPLACE SECRET r2_data_secret (
			TYPE R2,
			KEY_ID '%s',
			SECRET '%s',
			ACCOUNT_ID '%s'
		)`,
		config.R2.AccessKeyId,
		config.R2.SecretAccessKey,
		config.R2.AccountId,
	)

	_, err := client.ExecContext(ctx, createSecretQuery)
	if err != nil {
		return fmt.Errorf("failed to create R2 secret: %w", err)
	}

	LogInfo(config, "DuckLake: Created R2 secret")

	// Attach DuckLake catalog
	attachQuery := fmt.Sprintf(`
		ATTACH 'ducklake:postgres:%s' AS %s (
			DATA_PATH '%s'
		)`,
		config.Ducklake.CatalogUrl,
		config.Ducklake.CatalogName,
		config.Ducklake.DataPath,
	)

	_, err = client.ExecContext(ctx, attachQuery)
	if err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	LogInfo(config, fmt.Sprintf("DuckLake: Attached catalog '%s'", config.Ducklake.CatalogName))

	return nil
}

// setExplicitAwsCredentials configures AWS S3 access for Iceberg tables
// DEPRECATED: Not used with DuckLake (uses R2 secret instead)
func (client *DuckdbClient) setExplicitAwsCredentials(ctx context.Context) {
	config := client.Config

	// Skip if DuckLake is configured
	if config.Ducklake.CatalogUrl != "" {
		return
	}

	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := client.ExecContext(ctx, query, map[string]string{
		"accessKeyId":     config.Aws.AccessKeyId,
		"secretAccessKey": config.Aws.SecretAccessKey,
		"region":          config.Aws.Region,
		"endpoint":        config.Aws.S3Endpoint,
		"s3Bucket":        "s3://" + config.Aws.S3Bucket,
	})
	PanicIfError(config, err)
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	for key, value := range args {
		query = strings.ReplaceAll(
			query,
			"$"+key,
			strings.ReplaceAll(value, "'", "''"),
		)
	}
	return query
}
