package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/BemiHQ/BemiDB/src/common"
)

// CatalogCacheSQLite wraps CatalogCache with in-memory SQLite for fast queries
type CatalogCacheSQLite struct {
	db     *sql.DB
	cache  *CatalogCache
	config *common.CommonConfig
}

// NewCatalogCacheSQLite creates in-memory SQLite with catalog data
func NewCatalogCacheSQLite(cache *CatalogCache, config *common.CommonConfig) (*CatalogCacheSQLite, error) {
	if cache == nil {
		return nil, nil
	}

	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLite cache: %w", err)
	}

	cacheSQL := &CatalogCacheSQLite{
		db:     db,
		cache:  cache,
		config: config,
	}

	// Populate SQLite with cached data
	if err := cacheSQL.populate(); err != nil {
		db.Close()
		return nil, err
	}

	common.LogInfo(config, "CatalogCache: SQLite initialized for fast catalog queries")
	return cacheSQL, nil
}

// populate loads cache data into SQLite tables
func (c *CatalogCacheSQLite) populate() error {
	ctx := context.Background()

	// Create pg_class table
	_, err := c.db.ExecContext(ctx, `
		CREATE TABLE pg_class (
			oid INTEGER PRIMARY KEY,
			relname TEXT,
			relnamespace INTEGER,
			relkind TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create pg_class: %w", err)
	}

	// Insert tables from cache
	publicNS := int64(2035)
	for _, table := range c.cache.GetAllTables() {
		_, err := c.db.ExecContext(ctx,
			"INSERT INTO pg_class (oid, relname, relnamespace, relkind) VALUES (?, ?, ?, ?)",
			table.OID, table.Name, publicNS, "r")
		if err != nil {
			return fmt.Errorf("failed to insert table: %w", err)
		}
	}

	// Insert indexes from cache
	for _, pk := range c.cache.GetAllPrimaryKeys() {
		_, err := c.db.ExecContext(ctx,
			"INSERT INTO pg_class (oid, relname, relnamespace, relkind) VALUES (?, ?, ?, ?)",
			pk.OID, pk.ConstraintName, publicNS, "i")
		if err != nil {
			return fmt.Errorf("failed to insert index: %w", err)
		}
	}

	// Create pg_attribute table
	_, err = c.db.ExecContext(ctx, `
		CREATE TABLE pg_attribute (
			attrelid INTEGER,
			attname TEXT,
			atttypid INTEGER,
			attnum INTEGER
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create pg_attribute: %w", err)
	}

	// Insert columns from cache
	for tableName, columns := range c.cache.GetAllColumns() {
		tableOID, ok := c.cache.GetTableOID(tableName)
		if !ok {
			continue
		}
		for _, col := range columns {
			_, err := c.db.ExecContext(ctx,
				"INSERT INTO pg_attribute (attrelid, attname, atttypid, attnum) VALUES (?, ?, ?, ?)",
				tableOID, col.Name, 25, col.Index) // Type 25 = text
			if err != nil {
				return fmt.Errorf("failed to insert column: %w", err)
			}
		}
	}

	common.LogInfo(c.config, fmt.Sprintf("CatalogCache: SQLite populated with %d tables, %d columns",
		len(c.cache.Tables), len(c.cache.Columns)))
	return nil
}

// QueryContext executes query against SQLite cache
func (c *CatalogCacheSQLite) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query)
}

// Close closes the SQLite database
func (c *CatalogCacheSQLite) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ShouldIntercept checks if query should use SQLite cache
func ShouldIntercept(query string) bool {
	normalized := strings.ToLower(strings.TrimSpace(query))

	// Don't intercept queries with complex functions that SQLite doesn't support
	complexPatterns := []string{
		"_pg_expandarray",     // Array expansion function
		"pg_get_expr",         // Expression getter
		"unnest(",            // Array unnesting
		"array_agg(",         // Array aggregation
		"string_agg(",        // String aggregation
		"generate_series(",   // Series generation
	}

	for _, pattern := range complexPatterns {
		if strings.Contains(normalized, pattern) {
			return false
		}
	}

	// Intercept simple queries to cached tables (no complex functions)
	catalogTables := []string{
		"from main.pg_class",
		"from pg_class",
		"from main.pg_attribute",
		"from pg_attribute",
		"join main.pg_class",
		"join pg_class",
		"join main.pg_attribute",
		"join pg_attribute",
	}

	for _, pattern := range catalogTables {
		if strings.Contains(normalized, pattern) {
			return true
		}
	}

	return false
}
