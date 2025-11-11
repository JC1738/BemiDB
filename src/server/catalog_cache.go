package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/BemiHQ/BemiDB/src/common"
)

// CatalogCache stores pre-computed metadata about tables, columns, and primary keys
// This eliminates the need for expensive pg_catalog queries on every Metabase sync
type CatalogCache struct {
	mu sync.RWMutex

	// Table OID -> Table info
	Tables map[int64]*TableInfo

	// Table name -> Table OID
	TableNameToOID map[string]int64

	// Primary key constraint info by table name
	PrimaryKeys map[string]*PrimaryKeyInfo

	// Column info by table name
	Columns map[string][]*ColumnInfo

	// Type OID -> Type info (pg_type)
	Types map[int64]*TypeInfo

	// Table OID -> Column defaults (pg_attrdef)
	Attrdefs map[int64][]*AttrdefInfo

	// Object descriptions (pg_description)
	Descriptions []*DescriptionInfo
}

type TableInfo struct {
	OID       int64
	Name      string
	Namespace string // "public"
}

type PrimaryKeyInfo struct {
	OID           int64
	ConstraintName string
	TableOID      int64
	TableName     string
	ColumnName    string // For DuckLake: either "id" or "*_id" column
	ColumnIndex   int    // Always 1 for DuckLake synthetic PKs
}

type ColumnInfo struct {
	Name     string
	DataType string
	Index    int
}

type TypeInfo struct {
	OID          int64
	TypeName     string
	TypeType     string // 'b' = base type, 'd' = domain, etc.
	TypeBasetype int64  // Base type OID (for domains)
	TypeNotnull  bool
	TypeMod      int
}

type AttrdefInfo struct {
	OID     int64
	AdRelid int64  // Table OID
	AdNum   int    // Column number
	AdBin   string // Default expression (binary)
}

type DescriptionInfo struct {
	ObjOID      int64
	ClassOID    int64
	ObjSubID    int
	Description string
}

func NewCatalogCache() *CatalogCache {
	return &CatalogCache{
		Tables:         make(map[int64]*TableInfo),
		TableNameToOID: make(map[string]int64),
		PrimaryKeys:    make(map[string]*PrimaryKeyInfo),
		Columns:        make(map[string][]*ColumnInfo),
		Types:          make(map[int64]*TypeInfo),
		Attrdefs:       make(map[int64][]*AttrdefInfo),
		Descriptions:   []*DescriptionInfo{},
	}
}

// BuildCache queries DuckLake once and builds the entire cache
func (c *CatalogCache) BuildCache(ctx context.Context, client *common.DuckdbClient, catalogName string, config *common.CommonConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	common.LogDebug(config, "CatalogCache: Building catalog metadata cache...")

	// Query all tables with their primary key columns in a single efficient query
	// This replicates the logic from pg_constraint VIEW but runs ONCE instead of per-query
	tablesWithPKQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			(hash(t.table_name) %% 2147483647)::BIGINT AS table_oid,
			-- Priority 1: 'id' column, Priority 2: first column ending with '_id'
			COALESCE(
				(SELECT column_name FROM duckdb_columns() c2
				 WHERE c2.table_oid = t.table_oid
				 AND c2.column_name = 'id'
				 AND NOT c2.internal
				 LIMIT 1),
				(SELECT column_name FROM duckdb_columns() c2
				 WHERE c2.table_oid = t.table_oid
				 AND c2.column_name ~ '.*_id$'
				 AND NOT c2.internal
				 ORDER BY c2.column_name
				 LIMIT 1)
			) AS pk_column_name
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '%s'
			AND t.table_name NOT LIKE 'ducklake_%%'
		ORDER BY t.table_name
	`, catalogName)

	tableRows, err := client.QueryContext(ctx, tablesWithPKQuery)
	if err != nil {
		return fmt.Errorf("failed to query tables with PK columns: %w", err)
	}
	defer tableRows.Close()

	tableCount := 0
	pkCount := 0
	for tableRows.Next() {
		var tableName string
		var tableOID int64
		var pkColumnName *string // nullable
		if err := tableRows.Scan(&tableName, &tableOID, &pkColumnName); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}

		// Store table info
		c.Tables[tableOID] = &TableInfo{
			OID:       tableOID,
			Name:      tableName,
			Namespace: "public",
		}
		c.TableNameToOID[tableName] = tableOID

		// Create primary key constraint if table has qualifying column
		if pkColumnName != nil && *pkColumnName != "" {
			pkOID := hashString(tableName + "_pkey")
			c.PrimaryKeys[tableName] = &PrimaryKeyInfo{
				OID:            pkOID,
				ConstraintName: tableName + "_pkey",
				TableOID:       tableOID,
				TableName:      tableName,
				ColumnName:     *pkColumnName,
				ColumnIndex:    1,
			}
			pkCount++
		}

		tableCount++
	}

	common.LogDebug(config, fmt.Sprintf("CatalogCache: Cached %d tables (%d with primary keys)", tableCount, pkCount))

	// Query columns for all tables
	columnsQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			c.column_name,
			c.data_type,
			row_number() OVER (PARTITION BY t.table_name ORDER BY c.column_index) AS column_index
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		JOIN duckdb_columns() c ON t.table_oid = c.table_oid
		WHERE d.database_name = '%s'
			AND t.table_name NOT LIKE 'ducklake_%%'
			AND NOT c.internal
		ORDER BY t.table_name, column_index
	`, catalogName)

	columnRows, err := client.QueryContext(ctx, columnsQuery)
	if err != nil {
		return fmt.Errorf("failed to query columns: %w", err)
	}
	defer columnRows.Close()

	columnCount := 0
	for columnRows.Next() {
		var tableName, columnName, dataType string
		var columnIndex int
		if err := columnRows.Scan(&tableName, &columnName, &dataType, &columnIndex); err != nil {
			return fmt.Errorf("failed to scan column row: %w", err)
		}

		if c.Columns[tableName] == nil {
			c.Columns[tableName] = []*ColumnInfo{}
		}

		c.Columns[tableName] = append(c.Columns[tableName], &ColumnInfo{
			Name:     columnName,
			DataType: dataType,
			Index:    columnIndex,
		})

		columnCount++
	}

	common.LogDebug(config, fmt.Sprintf("CatalogCache: Cached %d columns across %d tables", columnCount, tableCount))

	// Query pg_type from DuckDB's pg_catalog
	typesQuery := `
		SELECT
			oid,
			typname,
			typtype,
			COALESCE(typbasetype, 0) AS typbasetype,
			COALESCE(typnotnull, FALSE) AS typnotnull,
			COALESCE(typtypmod, -1) AS typtypmod
		FROM pg_catalog.pg_type
		WHERE typtype IN ('b', 'd')
			AND oid IS NOT NULL
		ORDER BY oid
	`

	typeRows, err := client.QueryContext(ctx, typesQuery)
	if err != nil {
		common.LogDebug(config, fmt.Sprintf("CatalogCache: pg_type query failed (non-fatal): %v", err))
	} else {
		defer typeRows.Close()

		typeCount := 0
		for typeRows.Next() {
			var oid, typbasetype int64
			var typname, typtype string
			var typnotnull bool
			var typtypmod int32

			if err := typeRows.Scan(&oid, &typname, &typtype, &typbasetype, &typnotnull, &typtypmod); err != nil {
				return fmt.Errorf("failed to scan type row: %w", err)
			}

			c.Types[oid] = &TypeInfo{
				OID:          oid,
				TypeName:     typname,
				TypeType:     typtype,
				TypeBasetype: typbasetype,
				TypeNotnull:  typnotnull,
				TypeMod:      int(typtypmod),
			}
			typeCount++
		}

		common.LogDebug(config, fmt.Sprintf("CatalogCache: Cached %d types", typeCount))
	}

	// Query pg_attrdef (column defaults) if it exists
	attrdefQuery := `
		SELECT
			oid,
			adrelid,
			adnum,
			COALESCE(adbin, '') AS adbin
		FROM pg_catalog.pg_attrdef
		ORDER BY adrelid, adnum
	`

	attrdefRows, err := client.QueryContext(ctx, attrdefQuery)
	if err != nil {
		common.LogDebug(config, "CatalogCache: pg_attrdef not available, skipping")
	} else {
		defer attrdefRows.Close()

		attrdefCount := 0
		for attrdefRows.Next() {
			var oid, adrelid int64
			var adnum int
			var adbin string

			if err := attrdefRows.Scan(&oid, &adrelid, &adnum, &adbin); err != nil {
				return fmt.Errorf("failed to scan attrdef row: %w", err)
			}

			if c.Attrdefs[adrelid] == nil {
				c.Attrdefs[adrelid] = []*AttrdefInfo{}
			}

			c.Attrdefs[adrelid] = append(c.Attrdefs[adrelid], &AttrdefInfo{
				OID:     oid,
				AdRelid: adrelid,
				AdNum:   adnum,
				AdBin:   adbin,
			})
			attrdefCount++
		}

		common.LogDebug(config, fmt.Sprintf("CatalogCache: Cached %d column defaults", attrdefCount))
	}

	// Query pg_description (object comments) if it exists
	descQuery := `
		SELECT
			objoid,
			classoid,
			objsubid,
			COALESCE(description, '') AS description
		FROM pg_catalog.pg_description
		WHERE description IS NOT NULL AND description != ''
		ORDER BY objoid, objsubid
	`

	descRows, err := client.QueryContext(ctx, descQuery)
	if err != nil {
		common.LogDebug(config, "CatalogCache: pg_description not available, skipping")
	} else {
		defer descRows.Close()

		descCount := 0
		for descRows.Next() {
			var objoid, classoid int64
			var objsubid int
			var description string

			if err := descRows.Scan(&objoid, &classoid, &objsubid, &description); err != nil {
				return fmt.Errorf("failed to scan description row: %w", err)
			}

			c.Descriptions = append(c.Descriptions, &DescriptionInfo{
				ObjOID:      objoid,
				ClassOID:    classoid,
				ObjSubID:    objsubid,
				Description: description,
			})
			descCount++
		}

		common.LogDebug(config, fmt.Sprintf("CatalogCache: Cached %d descriptions", descCount))
	}

	return nil
}

// RebuildCache clears and rebuilds the entire cache
// Called when tables may have changed (e.g., during Metabase sync)
func (c *CatalogCache) RebuildCache(ctx context.Context, client *common.DuckdbClient, catalogName string, config *common.CommonConfig) error {
	common.LogDebug(config, "CatalogCache: Rebuilding cache...")

	c.mu.Lock()
	// Clear existing cache
	c.Tables = make(map[int64]*TableInfo)
	c.TableNameToOID = make(map[string]int64)
	c.PrimaryKeys = make(map[string]*PrimaryKeyInfo)
	c.Columns = make(map[string][]*ColumnInfo)
	c.Types = make(map[int64]*TypeInfo)
	c.Attrdefs = make(map[int64][]*AttrdefInfo)
	c.Descriptions = []*DescriptionInfo{}
	c.mu.Unlock()

	// Rebuild from scratch
	return c.BuildCache(ctx, client, catalogName, config)
}

// GetPrimaryKey returns the primary key info for a table
func (c *CatalogCache) GetPrimaryKey(tableName string) *PrimaryKeyInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.PrimaryKeys[tableName]
}

// GetColumns returns all columns for a table
func (c *CatalogCache) GetColumns(tableName string) []*ColumnInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Columns[tableName]
}

// GetTableOID returns the OID for a table name
func (c *CatalogCache) GetTableOID(tableName string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	oid, ok := c.TableNameToOID[tableName]
	return oid, ok
}

// GetAllTables returns all cached tables
func (c *CatalogCache) GetAllTables() []*TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]*TableInfo, 0, len(c.Tables))
	for _, table := range c.Tables {
		tables = append(tables, table)
	}
	return tables
}

// GetAllPrimaryKeys returns all cached primary keys
func (c *CatalogCache) GetAllPrimaryKeys() []*PrimaryKeyInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pks := make([]*PrimaryKeyInfo, 0, len(c.PrimaryKeys))
	for _, pk := range c.PrimaryKeys {
		pks = append(pks, pk)
	}
	return pks
}

// GetAllColumns returns all cached columns for all tables
func (c *CatalogCache) GetAllColumns() map[string][]*ColumnInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to avoid concurrent modification issues
	result := make(map[string][]*ColumnInfo)
	for tableName, cols := range c.Columns {
		result[tableName] = cols
	}
	return result
}

// GetAllTypes returns all cached types
func (c *CatalogCache) GetAllTypes() []*TypeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	types := make([]*TypeInfo, 0, len(c.Types))
	for _, t := range c.Types {
		types = append(types, t)
	}
	return types
}

// GetAllAttrdefs returns all cached column defaults for all tables
func (c *CatalogCache) GetAllAttrdefs() map[int64][]*AttrdefInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to avoid concurrent modification issues
	result := make(map[int64][]*AttrdefInfo)
	for tableOID, attrdefs := range c.Attrdefs {
		result[tableOID] = attrdefs
	}
	return result
}

// GetAllDescriptions returns all cached descriptions
func (c *CatalogCache) GetAllDescriptions() []*DescriptionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to avoid concurrent modification issues
	result := make([]*DescriptionInfo, len(c.Descriptions))
	copy(result, c.Descriptions)
	return result
}

// hashString returns a 32-bit hash of a string (matches DuckDB's hash() function behavior)
func hashString(s string) int64 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int64(h.Sum32() % 2147483647)
}
