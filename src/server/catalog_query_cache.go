package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

// Simple in-memory catalog query cache for fast single-table lookups
// Complex JOINs still go to DuckDB for correctness

type CatalogQueryCache struct {
	cache  *CatalogCache
	config *common.CommonConfig
}

func NewCatalogQueryCache(cache *CatalogCache, config *common.CommonConfig) *CatalogQueryCache {
	if cache == nil {
		return nil
	}
	return &CatalogQueryCache{
		cache:  cache,
		config: config,
	}
}

// TryIntercept checks if we can serve this query from cache
// Returns (result, true) if intercepted, (nil, false) to pass through to DuckDB
func (c *CatalogQueryCache) TryIntercept(ctx context.Context, query string) (*CachedQueryResult, bool) {
	if c == nil || c.cache == nil {
		return nil, false
	}

	normalized := strings.ToLower(strings.TrimSpace(query))

	// Pattern 1: SELECT COUNT(*) FROM main.pg_class WHERE relkind = 'r'
	if strings.Contains(normalized, "count(*)") &&
		strings.Contains(normalized, "from main.pg_class") &&
		strings.Contains(normalized, "relkind = 'r'") {
		count := len(c.cache.Tables)
		result := &CachedQueryResult{
			Columns: []string{"count"},
			Rows:    [][]interface{}{{count}},
		}
		common.LogDebug(c.config, fmt.Sprintf("CatalogCache: Served COUNT query from cache (%d rows)", count))
		return result, true
	}

	// Pattern 2: SELECT relname FROM pg_class WHERE relkind = 'r' (simple table list)
	if !strings.Contains(normalized, "join") &&
		strings.Contains(normalized, "from main.pg_class") &&
		strings.Contains(normalized, "relkind = 'r'") {
		
		var rows [][]interface{}
		for _, table := range c.cache.GetAllTables() {
			rows = append(rows, []interface{}{table.Name})
		}
		
		result := &CachedQueryResult{
			Columns: []string{"relname"},
			Rows:    rows,
		}
		common.LogDebug(c.config, fmt.Sprintf("CatalogCache: Served table list from cache (%d rows)", len(rows)))
		return result, true
	}

	// For complex queries (JOINs), let DuckDB handle them
	if strings.Contains(normalized, "join") {
		common.LogDebug(c.config, "CatalogCache: Complex JOIN query, using DuckDB")
		return nil, false
	}

	// Default: pass through to DuckDB
	return nil, false
}

// CachedQueryResult represents a query result from cache
type CachedQueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

// ToSQLRows converts cached result to sql.Rows format
// Note: This is a simplified version that works for our use case
func (r *CachedQueryResult) ToMessages(handler *ResponseHandler, originalQuery string) ([]interface{}, error) {
	// For now, return nil to indicate we should use DuckDB
	// Full implementation would require converting to pgproto3.Message format
	return nil, fmt.Errorf("not yet implemented")
}
