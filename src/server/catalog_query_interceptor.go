package main

import (
	"context"
	"database/sql"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

// CatalogQueryInterceptor serves catalog queries directly from Go cache
type CatalogQueryInterceptor struct {
	cache  *CatalogCache
	config *Config
}

func NewCatalogQueryInterceptor(cache *CatalogCache, config *Config) *CatalogQueryInterceptor {
	return &CatalogQueryInterceptor{
		cache:  cache,
		config: config,
	}
}

// TryIntercept checks if query can be served from cache, returns (rows, true) if intercepted
func (interceptor *CatalogQueryInterceptor) TryIntercept(ctx context.Context, query string) (*sql.Rows, bool) {
	if interceptor.cache == nil {
		return nil, false
	}

	// Normalize query
	normalized := strings.ToLower(strings.TrimSpace(query))

	// Pattern: SELECT ... FROM main.pg_class WHERE ...
	if strings.Contains(normalized, "from main.pg_class") ||
		strings.Contains(normalized, "from pg_class") {
		// Let simple table scans through to DuckDB for now
		// Complex JOINs will still go to DuckDB
		// This is a starting point - we can optimize specific patterns
		common.LogDebug(interceptor.config.CommonConfig, "CatalogInterceptor: pg_class query (passthrough)")
		return nil, false
	}

	// Pattern: SELECT ... FROM main.pg_attribute WHERE ...
	if strings.Contains(normalized, "from main.pg_attribute") ||
		strings.Contains(normalized, "from pg_attribute") {
		common.LogDebug(interceptor.config.CommonConfig, "CatalogInterceptor: pg_attribute query (passthrough)")
		return nil, false
	}

	// Not a catalog query or too complex - let DuckDB handle it
	return nil, false
}
