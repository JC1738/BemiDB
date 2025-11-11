package main

import (
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryToIcebergTable struct {
	QuerySchemaTable QuerySchemaTable
	IcebergTablePath string
}

type ParserTable struct {
	config *Config
	utils  *ParserUtils
}

func NewParserTable(config *Config) *ParserTable {
	return &ParserTable{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserTable) NodeToQuerySchemaTable(node *pgQuery.Node) QuerySchemaTable {
	rangeVar := node.GetRangeVar()
	var alias string

	if rangeVar.Alias != nil {
		alias = rangeVar.Alias.Aliasname
	}

	return QuerySchemaTable{
		Schema: rangeVar.Schemaname,
		Table:  rangeVar.Relname,
		Alias:  alias,
	}
}

func (parser *ParserTable) RemapSchemaToMain(node *pgQuery.Node) {
	node.GetRangeVar().Schemaname = DUCKDB_SCHEMA_MAIN
}

// Other information_schema.* tables
func (parser *ParserTable) IsTableFromInformationSchema(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_INFORMATION_SCHEMA
}

// public.table -> (SELECT * FROM iceberg_scan('path')) table
// schema.table -> (SELECT * FROM iceberg_scan('path')) schema_table
// MakeIcebergTableNode creates a table reference node for Iceberg tables
// DEPRECATED: Used by legacy Iceberg catalog. Use MakeDucklakeTableNode() for DuckLake.
// public.table -> (SELECT permitted, columns FROM iceberg_scan('path')) table
// public.table -> (SELECT NULL WHERE FALSE) table
// public.table t -> (SELECT * FROM iceberg_scan('path')) t
func (parser *ParserTable) MakeIcebergTableNode(queryToIcebergTable QueryToIcebergTable, permissions *map[string][]string) *pgQuery.Node {
	var query string
	if permissions == nil {
		query = "SELECT * FROM iceberg_scan('" + queryToIcebergTable.IcebergTablePath + "')"
	} else if columnNames, allowed := (*permissions)[queryToIcebergTable.QuerySchemaTable.ToIcebergSchemaTable().ToArg()]; allowed {
		quotedColumnNames := make([]string, len(columnNames))
		for i, columnName := range columnNames {
			quotedColumnNames[i] = "\"" + columnName + "\""
		}
		query = "SELECT " + strings.Join(quotedColumnNames, ", ") + " FROM iceberg_scan('" + queryToIcebergTable.IcebergTablePath + "')"
	} else {
		query = "SELECT NULL WHERE FALSE"
	}

	return parser.makeSubselectNode(query, queryToIcebergTable.QuerySchemaTable)
}

// MakeDucklakeTableNode creates a table reference node for DuckLake
// Unlike Iceberg which uses iceberg_scan(), DuckLake uses direct table references
// PERFORMANCE: Uses direct RangeVar nodes to allow DuckDB query optimizer to push down LIMIT/WHERE/column filters
func (parser *ParserTable) MakeDucklakeTableNode(queryToIcebergTable QueryToIcebergTable, permissions *map[string][]string) *pgQuery.Node {
	ducklakeTablePath := queryToIcebergTable.IcebergTablePath // Format: catalog.schema.table

	// Quote each component to preserve case sensitivity
	// catalog.schema.table -> "catalog"."schema"."table"
	parts := strings.Split(ducklakeTablePath, ".")
	quotedParts := make([]string, len(parts))
	for i, part := range parts {
		quotedParts[i] = "\"" + part + "\""
	}
	quotedTablePath := strings.Join(quotedParts, ".")

	if permissions == nil {
		// NO PERMISSIONS: Use direct table reference for optimal performance
		// This allows DuckDB to push down LIMIT, WHERE, and column projections
		return parser.makeDirectTableNode(quotedTablePath, queryToIcebergTable.QuerySchemaTable)
	} else if columnNames, allowed := (*permissions)[queryToIcebergTable.QuerySchemaTable.ToIcebergSchemaTable().ToArg()]; allowed {
		// WITH PERMISSIONS: Use subquery to filter columns
		// TODO: Optimize this case to avoid subquery if possible
		quotedColumnNames := make([]string, len(columnNames))
		for i, columnName := range columnNames {
			quotedColumnNames[i] = "\"" + columnName + "\""
		}
		query := "SELECT " + strings.Join(quotedColumnNames, ", ") + " FROM " + quotedTablePath
		return parser.makeSubselectNode(query, queryToIcebergTable.QuerySchemaTable)
	} else {
		// NO ACCESS: Return empty result set
		query := "SELECT NULL WHERE FALSE"
		return parser.makeSubselectNode(query, queryToIcebergTable.QuerySchemaTable)
	}
}

// information_schema.tables -> (SELECT * FROM main.tables) information_schema_tables
// information_schema.tables -> (SELECT * FROM main.tables WHERE table_schema || '.' || table_name IN ('permitted.table')) information_schema_tables
// information_schema.tables t -> (SELECT * FROM main.tables) t
func (parser *ParserTable) MakeInformationSchemaTablesNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string, isDucklake bool) *pgQuery.Node {
	// Filter out internal DuckLake catalog tables (ducklake_*)
	var query string
	if isDucklake {
		// For DuckLake, map 'main' schema to 'public' for Postgres compatibility
		query = "SELECT table_catalog, CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END AS table_schema, table_name, table_type FROM main.tables WHERE table_name NOT LIKE 'ducklake_%'"
	} else {
		query = "SELECT * FROM main.tables WHERE table_name NOT LIKE 'ducklake_%'"
	}

	if permissions != nil {
		quotedSchemaTableNames := []string{}
		for schemaTable := range *permissions {
			quotedSchemaTableNames = append(quotedSchemaTableNames, "'"+schemaTable+"'")
		}
		if isDucklake {
			query += " AND (CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END) || '.' || table_name IN (" + strings.Join(quotedSchemaTableNames, ", ") + ")"
		} else {
			query += " AND table_schema || '.' || table_name IN (" + strings.Join(quotedSchemaTableNames, ", ") + ")"
		}
	}

	return parser.makeSubselectNode(query, qSchemaTable)
}

// information_schema.columns -> (SELECT * FROM main.columns) information_schema_columns
// information_schema.columns -> (SELECT * FROM main.columns WHERE (table_schema || '.' || table_name IN ('permitted.table') AND column_name IN ('permitted', 'columns')) OR ...) information_schema_columns
// information_schema.columns c -> (SELECT * FROM main.columns) c
func (parser *ParserTable) MakeInformationSchemaColumnsNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string, isDucklake bool) *pgQuery.Node {
	// Filter out internal DuckLake catalog tables (ducklake_*)
	var query string
	if isDucklake {
		// For DuckLake, map 'main' schema to 'public' for Postgres compatibility
		query = "SELECT table_catalog, CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END AS table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision, character_set_catalog, character_set_schema, character_set_name, collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name, udt_catalog, udt_schema, udt_name, scope_catalog, scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_self_referencing, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle, is_generated, generation_expression, is_updatable FROM main.columns WHERE table_name NOT LIKE 'ducklake_%'"
	} else {
		query = "SELECT * FROM main.columns WHERE table_name NOT LIKE 'ducklake_%'"
	}

	if permissions != nil {
		conditions := []string{}
		for schemaTable, columnNames := range *permissions {
			quotedColumnNames := []string{}
			for _, columnName := range columnNames {
				quotedColumnNames = append(quotedColumnNames, "'"+columnName+"'")
			}
			if isDucklake {
				conditions = append(conditions, "((CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END) || '.' || table_name = '"+schemaTable+"' AND column_name IN ("+strings.Join(quotedColumnNames, ", ")+"))")
			} else {
				conditions = append(conditions, "(table_schema || '.' || table_name = '"+schemaTable+"' AND column_name IN ("+strings.Join(quotedColumnNames, ", ")+"))")
			}
		}
		query += " AND (" + strings.Join(conditions, " OR ") + ")"
	}

	return parser.makeSubselectNode(query, qSchemaTable)
}

// information_schema.table_constraints -> (SELECT * FROM main.table_constraints) information_schema_table_constraints
func (parser *ParserTable) MakeInformationSchemaTableConstraintsNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string, isDucklake bool) *pgQuery.Node {
	query := "SELECT * FROM main." + PG_TABLE_TABLE_CONSTRAINTS

	if permissions != nil && len(*permissions) > 0 {
		conditions := []string{}
		for schemaTable := range *permissions {
			if isDucklake {
				conditions = append(conditions, "((CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END) || '.' || table_name = '"+schemaTable+"')")
			} else {
				conditions = append(conditions, "(table_schema || '.' || table_name = '"+schemaTable+"')")
			}
		}
		query += " WHERE " + strings.Join(conditions, " OR ")
	}

	return parser.makeSubselectNode(query, qSchemaTable)
}

// information_schema.key_column_usage -> (SELECT * FROM main.key_column_usage) information_schema_key_column_usage
func (parser *ParserTable) MakeInformationSchemaKeyColumnUsageNode(qSchemaTable QuerySchemaTable, permissions *map[string][]string, isDucklake bool) *pgQuery.Node {
	query := "SELECT * FROM main." + PG_TABLE_KEY_COLUMN_USAGE

	if permissions != nil && len(*permissions) > 0 {
		conditions := []string{}
		for schemaTable, columnNames := range *permissions {
			var condition string
			if len(columnNames) == 0 {
				if isDucklake {
					condition = "((CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END) || '.' || table_name = '" + schemaTable + "')"
				} else {
					condition = "(table_schema || '.' || table_name = '" + schemaTable + "')"
				}
			} else {
				quotedColumnNames := make([]string, len(columnNames))
				for i, columnName := range columnNames {
					quotedColumnNames[i] = "'" + columnName + "'"
				}
				if isDucklake {
					condition = "((CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END) || '.' || table_name = '" + schemaTable + "' AND column_name IN (" + strings.Join(quotedColumnNames, ", ") + "))"
				} else {
					condition = "(table_schema || '.' || table_name = '" + schemaTable + "' AND column_name IN (" + strings.Join(quotedColumnNames, ", ") + "))"
				}
			}
			conditions = append(conditions, condition)
		}
		query += " WHERE " + strings.Join(conditions, " OR ")
	}

	return parser.makeSubselectNode(query, qSchemaTable)
}

func (parser *ParserTable) TopLevelSchemaFunction(rangeFunction *pgQuery.RangeFunction) *QuerySchemaFunction {
	if len(rangeFunction.Functions) == 0 || len(rangeFunction.Functions[0].GetList().Items) == 0 {
		return nil
	}

	functionNode := rangeFunction.Functions[0].GetList().Items[0]
	if functionNode.GetFuncCall() == nil {
		return nil // E.g., system PG calls like "... FROM user" => sqlvalue_function:{op:SVFOP_USER}
	}

	return parser.utils.SchemaFunction(functionNode.GetFuncCall())
}

func (parser *ParserTable) TableFunctionCalls(rangeFunction *pgQuery.RangeFunction) []*pgQuery.FuncCall {
	functionCalls := []*pgQuery.FuncCall{}

	for _, funcNode := range rangeFunction.Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			functionCall := funcItemNode.GetFuncCall()
			if functionCall != nil {
				functionCalls = append(functionCalls, functionCall)
			}
		}
	}

	return functionCalls
}

func (parser *ParserTable) Alias(rangeFunction *pgQuery.RangeFunction) string {
	if rangeFunction.GetAlias() != nil {
		return rangeFunction.GetAlias().Aliasname
	}

	return ""
}

func (parser *ParserTable) SetAlias(rangeFunction *pgQuery.RangeFunction, alias string, columnName string) {
	rangeFunction.Alias = &pgQuery.Alias{
		Aliasname: alias,
		Colnames:  []*pgQuery.Node{pgQuery.MakeStrNode(columnName)},
	}
}

func (parser *ParserTable) SetAliasIfNotExists(rangeFunction *pgQuery.RangeFunction, alias string) {
	if rangeFunction.GetAlias() != nil {
		return
	}

	rangeFunction.Alias = &pgQuery.Alias{Aliasname: alias}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// (query) AS qSchemaTable
func (parser *ParserTable) makeSubselectNode(query string, qSchemaTable QuerySchemaTable) *pgQuery.Node {
	queryTree, err := pgQuery.Parse(query)
	common.PanicIfError(parser.config.CommonConfig, err)

	alias := qSchemaTable.Alias
	if alias == "" {
		if qSchemaTable.Schema == PG_SCHEMA_PUBLIC || qSchemaTable.Schema == "" {
			alias = qSchemaTable.Table
		} else {
			alias = qSchemaTable.Schema + "_" + qSchemaTable.Table
		}
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: queryTree.Stmts[0].Stmt.GetSelectStmt(),
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
				},
			},
		},
	}
}

// makeDirectTableNode creates a direct table reference without subquery wrapping
// This allows DuckDB's query optimizer to push down LIMIT, WHERE, and column projections
// quotedTablePath should be in format: "catalog"."schema"."table"
func (parser *ParserTable) makeDirectTableNode(quotedTablePath string, qSchemaTable QuerySchemaTable) *pgQuery.Node {
	// Parse a simple SELECT to extract the table reference
	query := "SELECT * FROM " + quotedTablePath
	queryTree, err := pgQuery.Parse(query)
	common.PanicIfError(parser.config.CommonConfig, err)

	// Extract the FROM clause RangeVar node
	selectStmt := queryTree.Stmts[0].Stmt.GetSelectStmt()
	tableNode := selectStmt.FromClause[0]

	// Set the alias to match the original table name
	rangeVar := tableNode.GetRangeVar()
	alias := qSchemaTable.Alias
	if alias == "" {
		if qSchemaTable.Schema == PG_SCHEMA_PUBLIC || qSchemaTable.Schema == "" {
			alias = qSchemaTable.Table
		} else {
			alias = qSchemaTable.Schema + "_" + qSchemaTable.Table
		}
	}
	rangeVar.Alias = &pgQuery.Alias{Aliasname: alias}

	return tableNode
}
