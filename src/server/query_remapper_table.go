package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"

	"github.com/BemiHQ/BemiDB/src/common"
)

var PG_CATALOG_TABLE_NAMES = common.Set[string]{}

type QueryRemapperTable struct {
	parserTable                   *ParserTable
	parserFunction                *ParserFunction
	remapperFunction              *QueryRemapperFunction
	IcebergPersistentSchemaTables common.Set[common.IcebergSchemaTable]
	IcebergMaterlizedSchemaTables common.Set[common.IcebergSchemaTable]
	IcebergMaterializedViews      []common.IcebergMaterializedView
	icebergReader                 *IcebergReader
	ServerDuckdbClient            *common.DuckdbClient // nilable
	config                        *Config
}

func NewQueryRemapperTable(config *Config, icebergReader *IcebergReader, serverDuckdbClient *common.DuckdbClient) *QueryRemapperTable {
	remapper := &QueryRemapperTable{
		parserTable:        NewParserTable(config),
		parserFunction:     NewParserFunction(config),
		remapperFunction:   NewQueryRemapperFunction(config, icebergReader),
		icebergReader:      icebergReader,
		ServerDuckdbClient: serverDuckdbClient,
		config:             config,
	}
	remapper.reloadIcebergTables()
	return remapper
}

// FROM / JOIN [TABLE]
func (remapper *QueryRemapperTable) RemapTable(node *pgQuery.Node, permissions *map[string][]string) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)

	// pg_catalog.pg_* system tables
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// pg_class -> reload Iceberg/DuckLake tables
		case PG_TABLE_PG_CLASS:
			if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" {
				remapper.reloadDucklakeTables()
			} else {
				remapper.reloadIcebergTables()
			}

		// pg_stat_user_tables -> return Iceberg/DuckLake tables
		case PG_TABLE_PG_STAT_USER_TABLES:
			if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" {
				remapper.reloadDucklakeTables()
			} else {
				remapper.reloadIcebergTables()
			}
			remapper.upsertPgStatUserTables()

		// pg_matviews -> reload Iceberg materialized views
		case PG_TABLE_PG_MATVIEWS:
			remapper.reloadIcebergMaterializedViews()
			remapper.upsertPgMatviews()
		}

		// pg_catalog.[table] -> main.[table] for tables defined in CreatePgCatalogTableQueries
		if PG_CATALOG_TABLE_NAMES.Contains(qSchemaTable.Table) {
			parser.RemapSchemaToMain(node)
			return node
		}

		// pg_catalog.pg_* other system tables -> return as is
		return node
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {
		// information_schema.tables -> (SELECT * FROM main.tables) information_schema_tables
		// information_schema.tables -> (SELECT * FROM main.tables WHERE table_schema || '.' || table_name IN ('permitted.table')) information_schema_tables
		case PG_TABLE_TABLES:
			if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" {
				remapper.reloadDucklakeTables()
			} else {
				remapper.reloadIcebergTables()
			}
			return parser.MakeInformationSchemaTablesNode(qSchemaTable, permissions, remapper.config.CommonConfig.Ducklake.CatalogUrl != "")

		// information_schema.columns -> (SELECT * FROM main.columns) information_schema_columns
		// information_schema.columns -> (SELECT * FROM main.columns WHERE (table_schema || '.' || table_name IN ('permitted.table') AND column_name IN ('permitted', 'columns')) OR ...) information_schema_columns
		case PG_TABLE_COLUMNS:
			return parser.MakeInformationSchemaColumnsNode(qSchemaTable, permissions, remapper.config.CommonConfig.Ducklake.CatalogUrl != "")

		// information_schema.table_constraints -> (SELECT * FROM main.table_constraints) information_schema_table_constraints
		case PG_TABLE_TABLE_CONSTRAINTS:
			return parser.MakeInformationSchemaTableConstraintsNode(qSchemaTable, permissions, remapper.config.CommonConfig.Ducklake.CatalogUrl != "")

		// information_schema.key_column_usage -> (SELECT * FROM main.key_column_usage) information_schema_key_column_usage
		case PG_TABLE_KEY_COLUMN_USAGE:
			return parser.MakeInformationSchemaKeyColumnUsageNode(qSchemaTable, permissions, remapper.config.CommonConfig.Ducklake.CatalogUrl != "")
		}

		// information_schema.* other system tables -> return as is
		return node
	}

	// public.table -> catalog_name.schema.table
	// schema.table -> catalog_name.schema.table
	schemaTable := qSchemaTable.ToIcebergSchemaTable()

	// For DuckLake: map "public" schema to "main" schema
	if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" && schemaTable.Schema == PG_SCHEMA_PUBLIC {
		schemaTable.Schema = DUCKDB_SCHEMA_MAIN
	}

	// Check if table exists in DuckLake catalog
	if !remapper.IcebergPersistentSchemaTables.Contains(schemaTable) && !remapper.IcebergMaterlizedSchemaTables.Contains(schemaTable) {
		remapper.reloadIcebergTables()
		if !remapper.IcebergPersistentSchemaTables.Contains(schemaTable) && !remapper.IcebergMaterlizedSchemaTables.Contains(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}

	// Use DuckLake if configured, otherwise use legacy Iceberg
	if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" {
		// Return DuckLake table reference
		// Format: catalog_name.schema.table
		ducklakeCatalogName := remapper.config.CommonConfig.Ducklake.CatalogName
		ducklakeTablePath := fmt.Sprintf("%s.%s.%s", ducklakeCatalogName, schemaTable.Schema, schemaTable.Table)

		common.LogDebug(remapper.config.CommonConfig, fmt.Sprintf("RemapTable: qSchemaTable=%v, schemaTable=%v, ducklakeTablePath=%s", qSchemaTable, schemaTable, ducklakeTablePath))

		return parser.MakeDucklakeTableNode(QueryToIcebergTable{
			QuerySchemaTable:     qSchemaTable,
			IcebergTablePath:     ducklakeTablePath, // Reusing field name for compatibility
		}, permissions)
	} else {
		// Legacy Iceberg path
		icebergPath := remapper.icebergReader.MetadataFileS3Path(schemaTable)
		return parser.MakeIcebergTableNode(QueryToIcebergTable{
			QuerySchemaTable: qSchemaTable,
			IcebergTablePath: icebergPath,
		}, permissions)
	}
}

// FROM FUNCTION()
func (remapper *QueryRemapperTable) RemapTableFunctionCall(rangeFunction *pgQuery.RangeFunction) {
	schemaFunction := remapper.parserTable.TopLevelSchemaFunction(rangeFunction)
	if schemaFunction != nil {
		// SELECT value FROM jsonb_array_elements(...) value -> SELECT value FROM unnest(json_extract(..., '$[*]')) unnest(value)
		if (schemaFunction.Schema == PG_SCHEMA_PG_CATALOG || schemaFunction.Schema == "") &&
			(schemaFunction.Function == PG_FUNCTION_JSON_ARRAY_ELEMENTS || schemaFunction.Function == PG_FUNCTION_JSONB_ARRAY_ELEMENTS) {
			alias := remapper.parserTable.Alias(rangeFunction)
			if alias == "" {
				remapper.parserTable.SetAlias(rangeFunction, "unnest", "value")
			} else {
				remapper.parserTable.SetAlias(rangeFunction, "unnest", alias)
			}
		}

		remapper.parserTable.SetAliasIfNotExists(rangeFunction, schemaFunction.Function)
	}

	for _, functionCall := range remapper.parserTable.TableFunctionCalls(rangeFunction) {
		remapper.remapperFunction.RemapFunctionCall(functionCall)
		remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
	}
}

func (remapper *QueryRemapperTable) reloadIcebergTables() {
	remapper.reloadIcebergMaterializedViews()
	remapper.reloadIcebergPersistentTables()
}

func (remapper *QueryRemapperTable) reloadIcebergPersistentTables() {
	// Use DuckLake if configured
	if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" {
		remapper.reloadDucklakeTables()
		return
	}

	// Legacy Iceberg implementation
	newIcebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	common.PanicIfError(remapper.config.CommonConfig, err)

	// Exclude materialized views
	for _, icebergSchemaTable := range remapper.IcebergMaterlizedSchemaTables.Values() {
		newIcebergSchemaTables.Remove(icebergSchemaTable)
	}

	previousIcebergSchemaTables := remapper.IcebergPersistentSchemaTables
	remapper.IcebergPersistentSchemaTables = newIcebergSchemaTables

	ctx := context.Background()
	// CREATE TABLE IF NOT EXISTS
	for _, icebergSchemaTable := range newIcebergSchemaTables.Values() {
		if !previousIcebergSchemaTables.Contains(icebergSchemaTable) {
			catalogTableColumns, err := remapper.icebergReader.TableColumns(icebergSchemaTable)
			common.PanicIfError(remapper.config.CommonConfig, err)

			var sqlColumns []string
			for _, catalogTableColumn := range catalogTableColumns {
				sqlColumns = append(sqlColumns, catalogTableColumn.ToSql())
			}

			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+icebergSchemaTable.Schema)
			common.PanicIfError(remapper.config.CommonConfig, err)
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+icebergSchemaTable.String()+" ("+strings.Join(sqlColumns, ", ")+")")
			common.PanicIfError(remapper.config.CommonConfig, err)
		}
	}
	// DROP TABLE IF EXISTS
	for _, icebergSchemaTable := range previousIcebergSchemaTables.Values() {
		if !newIcebergSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "DROP TABLE IF EXISTS "+icebergSchemaTable.String())
			common.PanicIfError(remapper.config.CommonConfig, err)
		}
	}
}

func (remapper *QueryRemapperTable) reloadDucklakeTables() {
	ctx := context.Background()

	// Query DuckLake catalog for available tables using duckdb_tables() function
	catalogName := remapper.config.CommonConfig.Ducklake.CatalogName

	query := fmt.Sprintf(`
		SELECT 'main' AS table_schema, t.table_name
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '%s'
		  AND t.table_name NOT LIKE 'ducklake_%%'
	`, catalogName)

	rows, err := remapper.ServerDuckdbClient.QueryContext(ctx, query)
	common.PanicIfError(remapper.config.CommonConfig, err)
	defer rows.Close()

	newIcebergSchemaTables := common.NewSet[common.IcebergSchemaTable]()

	for rows.Next() {
		var schemaName, tableName string
		err := rows.Scan(&schemaName, &tableName)
		common.PanicIfError(remapper.config.CommonConfig, err)

		schemaTable := common.IcebergSchemaTable{
			Schema: schemaName,
			Table:  tableName,
		}
		newIcebergSchemaTables.Add(schemaTable)

		// Log new tables discovered
		if !remapper.IcebergPersistentSchemaTables.Contains(schemaTable) {
			common.LogInfo(remapper.config.CommonConfig, fmt.Sprintf("DuckLake: Discovered table %s.%s", schemaName, tableName))
		}
	}

	remapper.IcebergPersistentSchemaTables = newIcebergSchemaTables

	common.LogInfo(remapper.config.CommonConfig, fmt.Sprintf("DuckLake: Loaded %d tables from catalog", len(newIcebergSchemaTables)))
}

func (remapper *QueryRemapperTable) reloadIcebergMaterializedViews() {
	newIcebergMaterializedViews, err := remapper.icebergReader.MaterializedViews()
	common.PanicIfError(remapper.config.CommonConfig, err)

	remapper.IcebergMaterializedViews = newIcebergMaterializedViews

	newMaterializedSchemaTables := common.NewSet[common.IcebergSchemaTable]()
	for _, icebergMaterializedView := range newIcebergMaterializedViews {
		newMaterializedSchemaTables.Add(icebergMaterializedView.ToIcebergSchemaTable())
	}
	previousIcebergSchemaTables := remapper.IcebergMaterlizedSchemaTables
	remapper.IcebergMaterlizedSchemaTables = newMaterializedSchemaTables

	ctx := context.Background()
	// CREATE VIEW IF NOT EXISTS
	for _, icebergSchemaTable := range remapper.IcebergMaterlizedSchemaTables.Values() {
		if !previousIcebergSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+icebergSchemaTable.Schema)
			common.PanicIfError(remapper.config.CommonConfig, err)
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE VIEW IF NOT EXISTS "+icebergSchemaTable.String()+" AS SELECT 1")
			common.PanicIfError(remapper.config.CommonConfig, err)
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "INSERT INTO pg_matviews VALUES ('"+icebergSchemaTable.Schema+"', '"+icebergSchemaTable.Table+"', '"+remapper.config.User+"', NULL, FALSE, TRUE, '')")
			common.PanicIfError(remapper.config.CommonConfig, err)
		}
	}
	// DROP VIEW IF EXISTS
	for _, icebergSchemaTable := range previousIcebergSchemaTables.Values() {
		if !newMaterializedSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "DROP VIEW IF EXISTS "+icebergSchemaTable.String())
			common.PanicIfError(remapper.config.CommonConfig, err)
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "DELETE FROM pg_matviews WHERE schemaname = '"+icebergSchemaTable.Schema+"' AND matviewname = '"+icebergSchemaTable.Table+"'")
			common.PanicIfError(remapper.config.CommonConfig, err)
		}
	}
}

func (remapper *QueryRemapperTable) upsertPgStatUserTables() {
	icebergSchemaTables := append(remapper.IcebergPersistentSchemaTables.Values(), remapper.IcebergMaterlizedSchemaTables.Values()...)

	sqls := []string{"DELETE FROM pg_stat_user_tables"}
	if len(icebergSchemaTables) > 0 {
		values := make([]string, len(icebergSchemaTables))
		for i, icebergSchemaTable := range icebergSchemaTables {
			// For DuckLake, map 'main' schema to 'public' for Postgres compatibility
			schema := icebergSchemaTable.Schema
			if remapper.config.CommonConfig.Ducklake.CatalogUrl != "" && schema == DUCKDB_SCHEMA_MAIN {
				schema = PG_SCHEMA_PUBLIC
			}
			values[i] = "('123456', '" + schema + "', '" + icebergSchemaTable.Table + "', 0, NULL, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, NULL, NULL, NULL, NULL, 0, 0, 0, 0)"
		}
		sqls = append(sqls, "INSERT INTO pg_stat_user_tables VALUES "+strings.Join(values, ", "))
	}
	err := remapper.ServerDuckdbClient.ExecTransactionContext(context.Background(), sqls)
	common.PanicIfError(remapper.config.CommonConfig, err)
}

func (remapper *QueryRemapperTable) upsertPgMatviews() {
	args := []map[string]string{map[string]string{}}
	sqls := []string{"DELETE FROM pg_matviews"}
	if len(remapper.IcebergMaterializedViews) > 0 {
		values := make([]string, len(remapper.IcebergMaterializedViews))
		arg := map[string]string{}
		for i, icebergMaterializedView := range remapper.IcebergMaterializedViews {
			iStr := common.IntToString(i)
			values[i] = "('$schema" + iStr + "', '$table" + iStr + "', '$owner" + iStr + "', NULL, FALSE, TRUE, '$definition" + iStr + "')"
			arg["schema"+iStr] = icebergMaterializedView.Schema
			arg["table"+iStr] = icebergMaterializedView.Table
			arg["owner"+iStr] = remapper.config.User
			arg["definition"+iStr] = icebergMaterializedView.Definition
		}
		sqls = append(sqls, "INSERT INTO pg_matviews VALUES "+strings.Join(values, ", "))
		args = append(args, arg)
	}
	err := remapper.ServerDuckdbClient.ExecTransactionContext(context.Background(), sqls, args)
	common.PanicIfError(remapper.config.CommonConfig, err)
}

// System pg_* tables
func (remapper *QueryRemapperTable) isTableFromPgCatalog(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_PG_CATALOG ||
		(qSchemaTable.Schema == "" &&
			(PG_SYSTEM_TABLES.Contains(qSchemaTable.Table) || PG_SYSTEM_VIEWS.Contains(qSchemaTable.Table)) &&
			!remapper.IcebergPersistentSchemaTables.Contains(qSchemaTable.ToIcebergSchemaTable()) &&
			!remapper.IcebergMaterlizedSchemaTables.Contains(qSchemaTable.ToIcebergSchemaTable()))
}

func extractTableNames(tables []string) common.Set[string] {
	names := make(common.Set[string])
	re := regexp.MustCompile(`CREATE (TABLE|VIEW) (\w+)`)

	for _, table := range tables {
		matches := re.FindStringSubmatch(table)
		if len(matches) > 1 {
			names.Add(matches[2])
		}
	}

	return names
}

// getDuckDBTypeToPostgresOidCaseExpression returns a SQL CASE expression that maps
// DuckDB data_type strings to Postgres type OIDs
func getDuckDBTypeToPostgresOidCaseExpression(dataTypeColumn string) string {
	return `CASE
		WHEN ` + dataTypeColumn + ` = 'BOOLEAN' THEN 16
		WHEN ` + dataTypeColumn + ` = 'TINYINT' THEN 21
		WHEN ` + dataTypeColumn + ` = 'SMALLINT' THEN 21
		WHEN ` + dataTypeColumn + ` = 'INTEGER' THEN 23
		WHEN ` + dataTypeColumn + ` = 'BIGINT' THEN 20
		WHEN ` + dataTypeColumn + ` = 'UTINYINT' THEN 21
		WHEN ` + dataTypeColumn + ` = 'USMALLINT' THEN 23
		WHEN ` + dataTypeColumn + ` = 'UINTEGER' THEN 20
		WHEN ` + dataTypeColumn + ` = 'UBIGINT' THEN 20
		WHEN ` + dataTypeColumn + ` = 'FLOAT' THEN 700
		WHEN ` + dataTypeColumn + ` = 'DOUBLE' THEN 701
		WHEN ` + dataTypeColumn + ` = 'DECIMAL' THEN 1700
		WHEN ` + dataTypeColumn + ` = 'VARCHAR' THEN 1043
		WHEN ` + dataTypeColumn + ` LIKE 'VARCHAR%' THEN 1043
		WHEN ` + dataTypeColumn + ` = 'TEXT' THEN 25
		WHEN ` + dataTypeColumn + ` = 'DATE' THEN 1082
		WHEN ` + dataTypeColumn + ` = 'TIMESTAMP' THEN 1114
		WHEN ` + dataTypeColumn + ` = 'TIMESTAMP WITH TIME ZONE' THEN 1184
		WHEN ` + dataTypeColumn + ` = 'TIME' THEN 1083
		WHEN ` + dataTypeColumn + ` = 'TIME WITH TIME ZONE' THEN 1266
		WHEN ` + dataTypeColumn + ` = 'INTERVAL' THEN 1186
		WHEN ` + dataTypeColumn + ` = 'JSON' THEN 114
		WHEN ` + dataTypeColumn + ` = 'JSONB' THEN 3802
		WHEN ` + dataTypeColumn + ` = 'UUID' THEN 2950
		WHEN ` + dataTypeColumn + ` = 'BLOB' THEN 17
		WHEN ` + dataTypeColumn + ` = 'BYTEA' THEN 17
		WHEN ` + dataTypeColumn + ` LIKE '%[]' THEN 2277
		WHEN ` + dataTypeColumn + ` LIKE 'MAP(%' THEN 114
		WHEN ` + dataTypeColumn + ` LIKE 'STRUCT(%' THEN 2249
		WHEN ` + dataTypeColumn + ` = '"NULL"' THEN 25
		ELSE 25
	END`
}

func CreatePgCatalogTableQueries(config *Config) []string {
	result := []string{
		// Static empty tables
		"CREATE TABLE pg_inherits(inhrelid oid, inhparent oid, inhseqno int4, inhdetachpending bool)",
		"CREATE TABLE pg_shdescription(objoid oid, classoid oid, description text)",
		"CREATE TABLE pg_statio_user_tables(relid oid, schemaname text, relname text, heap_blks_read int8, heap_blks_hit int8, idx_blks_read int8, idx_blks_hit int8, toast_blks_read int8, toast_blks_hit int8, tidx_blks_read int8, tidx_blks_hit int8)",
		"CREATE TABLE pg_replication_slots(slot_name text, plugin text, slot_type text, datoid oid, database text, temporary bool, active bool, active_pid int4, xmin int8, catalog_xmin int8, restart_lsn text, confirmed_flush_lsn text, wal_status text, safe_wal_size int8, two_phase bool, conflicting bool)",
		"CREATE TABLE pg_stat_gssapi(pid int4, gss_authenticated bool, principal text, encrypted bool, credentials_delegated bool)",
		"CREATE TABLE pg_auth_members(oid text, roleid oid, member oid, grantor oid, admin_option bool, inherit_option bool, set_option bool)",
		"CREATE TABLE pg_stat_activity(datid oid, datname text, pid int4, usesysid oid, usename text, application_name text, client_addr inet, client_hostname text, client_port int4, backend_start timestamp, xact_start timestamp, query_start timestamp, state_change timestamp, wait_event_type text, wait_event text, state text, backend_xid int8, backend_xmin int8, query text, backend_type text)",
		"CREATE TABLE pg_views(schemaname text, viewname text, viewowner text, definition text)",
		"CREATE TABLE pg_matviews(schemaname text, matviewname text, matviewowner text, tablespace text, hasindexes bool, ispopulated bool, definition text)",
		"CREATE TABLE pg_opclass(oid oid, opcmethod oid, opcname text, opcnamespace oid, opcowner oid, opcfamily oid, opcintype oid, opcdefault bool, opckeytype oid)",
		"CREATE TABLE pg_policy(oid oid, polname text, polrelid oid, polcmd text, polpermissive bool, polroles oid, polqual text, polwithcheck text)",
		"CREATE TABLE pg_statistic_ext(oid oid, stxrelid oid, stxname text, stxnamespace oid, stxowner oid, stxstattarget int4, stxkeys oid, stxkind text, stxexprs text)",
		"CREATE TABLE pg_publication(oid oid, pubname text, pubowner oid, puballtables bool, pubinsert bool, pubupdate bool, pubdelete bool, pubtruncate bool, pubviaroot bool)",
		"CREATE TABLE pg_publication_rel(oid oid, prpubid oid, prrelid oid, prqual text, prattrs text)",
		"CREATE TABLE pg_publication_namespace(oid oid, pnpubid oid, pnnspid oid)",
		"CREATE TABLE pg_rewrite(oid oid, rulename text, ev_class oid, ev_type char, ev_enabled char, is_instead bool, ev_qual text, ev_action text)",

		// Dynamic tables
		// DuckDB doesn't handle dynamic view replacement properly
		"CREATE TABLE pg_stat_user_tables(relid oid, schemaname text, relname text, seq_scan int8, last_seq_scan timestamp, seq_tup_read int8, idx_scan int8, last_idx_scan timestamp, idx_tup_fetch int8, n_tup_ins int8, n_tup_upd int8, n_tup_del int8, n_tup_hot_upd int8, n_tup_newpage_upd int8, n_live_tup int8, n_dead_tup int8, n_mod_since_analyze int8, n_ins_since_vacuum int8, last_vacuum timestamp, last_autovacuum timestamp, last_analyze timestamp, last_autoanalyze timestamp, vacuum_count int8, autovacuum_count int8, analyze_count int8, autoanalyze_count int8)",

		// Static views
		"CREATE VIEW pg_shadow AS SELECT '" + config.User + "' AS usename, '10'::oid AS usesysid, FALSE AS usecreatedb, FALSE AS usesuper, TRUE AS userepl, FALSE AS usebypassrls, '" + config.EncryptedPassword + "' AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig",
		"CREATE VIEW pg_roles AS SELECT '10'::oid AS oid, '" + config.User + "' AS rolname, TRUE AS rolsuper, TRUE AS rolinherit, TRUE AS rolcreaterole, TRUE AS rolcreatedb, TRUE AS rolcanlogin, FALSE AS rolreplication, -1 AS rolconnlimit, NULL::text AS rolpassword, NULL::timestamp AS rolvaliduntil, FALSE AS rolbypassrls, NULL::text[] AS rolconfig",
		"CREATE VIEW pg_extension AS SELECT '13823'::oid AS oid, 'plpgsql' AS extname, '10'::oid AS extowner, '11'::oid AS extnamespace, FALSE AS extrelocatable, '1.0'::text AS extversion, NULL::text[] AS extconfig, NULL::text[] AS extcondition",
		"CREATE VIEW pg_database AS SELECT '16388'::oid AS oid, '" + config.Database + "' AS datname, '10'::oid AS datdba, '6'::int4 AS encoding, 'c' AS datlocprovider, FALSE AS datistemplate, TRUE AS datallowconn, '-1'::int4 AS datconnlimit, '722'::int8 AS datfrozenxid, '1'::int4 AS datminmxid, '1663'::oid AS dattablespace, 'en_US.UTF-8' AS datcollate, 'en_US.UTF-8' AS datctype, 'en_US.UTF-8' AS datlocale, NULL::text AS daticurules, NULL::text AS datcollversion, NULL::text[] AS datacl",
		"CREATE VIEW pg_user AS SELECT '" + config.User + "' AS usename, '10'::oid AS usesysid, TRUE AS usecreatedb, TRUE AS usesuper, TRUE AS userepl, TRUE AS usebypassrls, '' AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig",
		"CREATE VIEW pg_collation AS SELECT '100'::oid AS oid, 'default' AS collname, '11'::oid AS collnamespace, '10'::oid AS collowner, 'd' AS collprovider, TRUE AS collisdeterministic, '-1'::int4 AS collencoding, NULL::text AS collcollate, NULL::text AS collctype, NULL::text AS colliculocale, NULL::text AS collicurules, NULL::text AS collversion",
		"CREATE VIEW user AS SELECT '" + config.User + "' AS user",

		// Dynamic views
		// Note: pg_index view is created later for DuckLake integration with primary key support
		// Hide DuckDB's system and duplicate schemas
		"CREATE VIEW pg_namespace AS SELECT * FROM pg_catalog.pg_namespace WHERE oid >= (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '" + PG_SCHEMA_PUBLIC + "')",
		// DuckDB does not support relforcerowsecurity column
		`CREATE VIEW pg_class AS
		SELECT
			c.oid,
			c.relname,
			c.relnamespace,
			c.reltype,
			c.reloftype,
			c.relowner,
			c.relam,
			c.relfilenode,
			c.reltablespace,
			c.relpages,
			c.reltuples,
			c.relallvisible,
			c.reltoastrelid,
			c.relhasindex,
			c.relisshared,
			c.relpersistence,
			CASE c.relkind
				WHEN 'v' THEN
					CASE c.relnamespace >= (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '` + PG_SCHEMA_PUBLIC + `')
					WHEN TRUE THEN
						'm'
					ELSE
						'v'
					END
			ELSE
				c.relkind
			END AS relkind,
			FALSE AS relforcerowsecurity
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname IN ('pg_catalog', 'information_schema')
		UNION ALL
		SELECT
			(hash(t.table_name) % 2147483647)::BIGINT AS oid,
			t.table_name AS relname,
			(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public') AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			0 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			0 AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			FALSE AS relhasindex,
			FALSE AS relisshared,
			'p' AS relpersistence,
			'r' AS relkind,
			FALSE AS relforcerowsecurity
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + config.CommonConfig.Ducklake.CatalogName + `'
		  AND t.table_name NOT LIKE 'ducklake_%%'
		UNION ALL
		SELECT
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS oid,
			t.table_name || '_pkey' AS relname,
			(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public') AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			403 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			0 AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			FALSE AS relhasindex,
			FALSE AS relisshared,
			'p' AS relpersistence,
			'i' AS relkind,
			FALSE AS relforcerowsecurity
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + config.CommonConfig.Ducklake.CatalogName + `'
		  AND t.table_name NOT LIKE 'ducklake_%%'
		  AND (
		      -- Priority 1: Has an 'id' column
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name = 'id'
		          AND NOT c.internal
		      )
		      OR
		      -- Priority 2: Has a column ending with '_id' (e.g., airtable_id, user_id)
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name ~ '.*_id$'
		          AND NOT c.internal
		      )
		  )`,
		`CREATE VIEW pg_type AS
			SELECT * FROM pg_catalog.pg_type
			UNION ALL
			SELECT 18, 'char', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 1, true, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 19, 'name', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 64, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 22, 'int2vector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 24, 'regproc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 25, 'text', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'S', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 26, 'oid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 27, 'tid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 6, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 28, 'xid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 29, 'cid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 30, 'oidvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 32, 'pg_ddl_command', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 71, 'pg_type', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 75, 'pg_attribute', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 81, 'pg_proc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 83, 'pg_class', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 114, 'json', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 142, 'xml', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 143, '_xml', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 194, 'pg_node_tree', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 199, '_json', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 210, '_pg_type', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 269, 'table_am_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 270, '_pg_attribute', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 271, '_xid8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 272, '_pg_proc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 273, '_pg_class', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 325, 'index_am_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 600, 'point', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 16, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 601, 'lseg', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 32, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 602, 'path', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 603, 'box', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 32, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 604, 'polygon', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 628, 'line', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 24, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 629, '_line', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 650, 'cidr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'I', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 651, '_cidr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 705, 'unknown', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -2, false, 'p', 'X', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 718, 'circle', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 24, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 719, '_circle', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 774, 'macaddr8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 775, '_macaddr8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 790, 'money', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 791, '_money', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 829, 'macaddr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 6, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 869, 'inet', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'I', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1000, '_bool', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1001, '_bytea', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1002, '_char', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1003, '_name', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1005, '_int2', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1006, '_int2vector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1007, '_int4', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1008, '_regproc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1009, '_text', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1010, '_tid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1011, '_xid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1012, '_cid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1013, '_oidvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1014, '_bpchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1015, '_varchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1016, '_int8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1017, '_point', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1018, '_lseg', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1019, '_path', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1020, '_box', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1021, '_float4', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1022, '_float8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1027, '_polygon', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1028, '_oid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1033, 'aclitem', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 16, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1034, '_aclitem', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1040, '_macaddr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1041, '_inet', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1042, 'bpchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1115, '_timestamp', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1182, '_date', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1183, '_time', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1185, '_timestamptz', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1187, '_interval', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1231, '_numeric', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1248, 'pg_database', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1263, '_cstring', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1270, '_timetz', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1561, '_bit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1562, 'varbit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'V', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1563, '_varbit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1790, 'refcursor', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2201, '_refcursor', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2202, 'regprocedure', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2203, 'regoper', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2204, 'regoperator', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2205, 'regclass', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2206, 'regtype', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2207, '_regprocedure', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2208, '_regoper', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2209, '_regoperator', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2210, '_regclass', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2211, '_regtype', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2249, 'record', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2275, 'cstring', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -2, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2276, 'any', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2277, 'anyarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2278, 'void', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2279, 'trigger', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2280, 'language_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2281, 'internal', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2283, 'anyelement', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2287, '_record', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2776, 'anynonarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2842, 'pg_authid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2843, 'pg_auth_members', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2949, '_txid_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2951, '_uuid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2970, 'txid_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3115, 'fdw_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3220, 'pg_lsn', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3221, '_pg_lsn', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3310, 'tsm_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3361, 'pg_ndistinct', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3402, 'pg_dependencies', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3500, 'anyenum', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3614, 'tsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3615, 'tsquery', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3642, 'gtsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3643, '_tsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3644, '_gtsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3645, '_tsquery', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3734, 'regconfig', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3735, '_regconfig', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3769, 'regdictionary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3770, '_regdictionary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3802, 'jsonb', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3807, '_jsonb', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3831, 'anyrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3838, 'event_trigger', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3904, 'int4range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3905, '_int4range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3906, 'numrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3907, '_numrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3908, 'tsrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3909, '_tsrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3910, 'tstzrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3911, '_tstzrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3912, 'daterange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3913, '_daterange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3926, 'int8range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3927, '_int8range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4066, 'pg_shseclabel', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4072, 'jsonpath', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4073, '_jsonpath', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4089, 'regnamespace', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4090, '_regnamespace', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4096, 'regrole', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4097, '_regrole', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4191, 'regcollation', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4192, '_regcollation', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4451, 'int4multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4532, 'nummultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4533, 'tsmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4534, 'tstzmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4535, 'datemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4536, 'int8multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4537, 'anymultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4538, 'anycompatiblemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4600, 'pg_brin_bloom_summary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4601, 'pg_brin_minmax_multi_summary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5017, 'pg_mcv_list', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5038, 'pg_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5039, '_pg_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5069, 'xid8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5077, 'anycompatible', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5078, 'anycompatiblearray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5079, 'anycompatiblenonarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5080, 'anycompatiblerange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6101, 'pg_subscription', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6150, '_int4multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6151, '_nummultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6152, '_tsmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6153, '_tstzmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6155, '_datemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6157, '_int8multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
		`,
	}

	// Add pg_attribute view for DuckLake tables if DuckLake is configured
	if config.CommonConfig.Ducklake.CatalogUrl != "" {
		catalogName := config.CommonConfig.Ducklake.CatalogName
		pgAttributeView := `CREATE VIEW pg_attribute AS
		SELECT a.* FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname IN ('pg_catalog', 'information_schema')
		UNION ALL
		SELECT
			(hash(c.table_name) % 2147483647)::BIGINT AS attrelid,
			c.column_name AS attname,
			` + getDuckDBTypeToPostgresOidCaseExpression("c.data_type") + `::BIGINT AS atttypid,
			-1 AS attstattarget,
			CASE
				WHEN c.data_type = 'BOOLEAN' THEN 1
				WHEN c.data_type IN ('TINYINT', 'UTINYINT') THEN 1
				WHEN c.data_type IN ('SMALLINT', 'USMALLINT') THEN 2
				WHEN c.data_type IN ('INTEGER', 'UINTEGER') THEN 4
				WHEN c.data_type IN ('BIGINT', 'UBIGINT') THEN 8
				WHEN c.data_type = 'FLOAT' THEN 4
				WHEN c.data_type = 'DOUBLE' THEN 8
				WHEN c.data_type = 'DATE' THEN 4
				WHEN c.data_type LIKE 'TIMESTAMP%' THEN 8
				ELSE -1
			END AS attlen,
			c.column_index AS attnum,
			0 AS attndims,
			-1 AS attcacheoff,
			-1 AS atttypmod,
			FALSE AS attbyval,
			'p' AS attstorage,
			'i' AS attalign,
			NOT c.is_nullable AS attnotnull,
			FALSE AS atthasdef,
			FALSE AS atthasmissing,
			'' AS attidentity,
			'' AS attgenerated,
			FALSE AS attisdropped,
			TRUE AS attislocal,
			0 AS attinhcount,
			0 AS attcollation,
			NULL AS attcompression,
			NULL AS attacl,
			NULL AS attoptions,
			NULL AS attfdwoptions,
			NULL AS attmissingval
		FROM duckdb_databases() d
		JOIN duckdb_columns() c ON d.database_oid = c.database_oid
		WHERE d.database_name = '` + catalogName + `'
		  AND c.table_name NOT LIKE 'ducklake_%%'
		  AND NOT c.internal`

		result = append(result, pgAttributeView)

		// Add pg_tables view to include DuckLake tables
		pgTablesView := `CREATE VIEW pg_tables AS
		SELECT * FROM pg_catalog.pg_tables
		UNION ALL
		SELECT
			'public' AS schemaname,
			t.table_name AS tablename,
			current_user AS tableowner,
			NULL AS tablespace,
			FALSE AS hasindexes,
			FALSE AS hasrules,
			FALSE AS hastriggers
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + catalogName + `'
		  AND t.table_name NOT LIKE 'ducklake_%%'`

		result = append(result, pgTablesView)

		// Add pg_index entries for primary keys (tables with 'id' column)
		// This is needed for Metabase and other BI tools to discover primary keys
		pgIndexView := `CREATE VIEW pg_index AS
		SELECT *, FALSE AS indnullsnotdistinct FROM (
		SELECT * FROM pg_catalog.pg_index
		UNION ALL
		SELECT
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS indexrelid,
			(hash(t.table_name) % 2147483647)::BIGINT AS indrelid,
			1 AS indnatts,
			1 AS indnkeyatts,
			TRUE AS indisunique,
			TRUE AS indisprimary,
			FALSE AS indisexclusion,
			TRUE AS indimmediate,
			FALSE AS indisclustered,
			TRUE AS indisvalid,
			FALSE AS indcheckxmin,
			TRUE AS indisready,
			TRUE AS indislive,
			FALSE AS indisreplident,
			ARRAY[1::INT2] AS indkey,
			ARRAY[]::BIGINT[] AS indcollation,
			ARRAY[]::BIGINT[] AS indclass,
			ARRAY[]::INTEGER[] AS indoption,
			NULL AS indexprs,
			NULL::INTEGER AS indpred
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + catalogName + `'
		  AND t.table_name NOT LIKE 'ducklake_%%'
		  AND (
		      -- Priority 1: Has an 'id' column
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name = 'id'
		          AND NOT c.internal
		      )
		      OR
		      -- Priority 2: Has a column ending with '_id' (e.g., airtable_id, user_id)
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name ~ '.*_id$'
		          AND NOT c.internal
		      )
		  )
		)`

		result = append(result, pgIndexView)

		// Add pg_indexes view for Metabase compatibility
		pgIndexesView := `CREATE VIEW pg_indexes AS
		SELECT
			n.nspname AS schemaname,
			c.relname AS tablename,
			i.relname AS indexname,
			NULL AS tablespace,
			pg_get_indexdef(idx.indexrelid) AS indexdef
		FROM pg_index idx
		JOIN pg_class c ON idx.indrelid = c.oid
		JOIN pg_class i ON idx.indexrelid = i.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')`

		result = append(result, pgIndexesView)

		// Add pg_constraint view for Metabase compatibility
		// Metabase queries pg_constraint to discover primary keys
		pgConstraintView := `CREATE VIEW pg_constraint AS
		SELECT
			oid, conname, connamespace, contype, condeferrable, condeferred, convalidated,
			conrelid, contypid, conindid, conparentid, confrelid, confupdtype, confdeltype, confmatchtype,
			conislocal, coninhcount, connoinherit, conkey, confkey, conpfeqop, conppeqop, conffeqop,
			ARRAY[]::INTEGER[] AS confdelsetcols, conexclop, conbin
		FROM pg_catalog.pg_constraint
		UNION ALL
		SELECT
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS oid,
			t.table_name || '_pkey' AS conname,
			(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public') AS connamespace,
			'p' AS contype,
			FALSE AS condeferrable,
			FALSE AS condeferred,
			TRUE AS convalidated,
			(hash(t.table_name) % 2147483647)::BIGINT AS conrelid,
			0::INTEGER AS contypid,
			(hash(t.table_name || '_pkey') % 2147483647)::INTEGER AS conindid,
			0::INTEGER AS conparentid,
			0::INTEGER AS confrelid,
			0::INTEGER AS confupdtype,
			0::INTEGER AS confdeltype,
			0::INTEGER AS confmatchtype,
			TRUE AS conislocal,
			0 AS coninhcount,
			FALSE AS connoinherit,
			ARRAY[1::BIGINT] AS conkey,
			ARRAY[]::INTEGER[] AS confkey,
			ARRAY[]::INTEGER[] AS conpfeqop,
			ARRAY[]::INTEGER[] AS conppeqop,
			ARRAY[]::INTEGER[] AS conffeqop,
			ARRAY[]::INTEGER[] AS confdelsetcols,
			ARRAY[]::INTEGER[] AS conexclop,
			NULL AS conbin
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + catalogName + `'
		  AND t.table_name NOT LIKE 'ducklake_%%'
		  AND (
		      -- Priority 1: Has an 'id' column
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name = 'id'
		          AND NOT c.internal
		      )
		      OR
		      -- Priority 2: Has a column ending with '_id' (e.g., airtable_id, user_id)
		      EXISTS (
		          SELECT 1 FROM duckdb_columns() c
		          WHERE c.table_oid = t.table_oid
		          AND c.column_name ~ '.*_id$'
		          AND NOT c.internal
		      )
		  )`

		result = append(result, pgConstraintView)
	} else {
		// For non-DuckLake setups, use standard pg_index view
		// DuckDB does not support indnullsnotdistinct column
		result = append(result, "CREATE VIEW pg_index AS SELECT *, FALSE AS indnullsnotdistinct FROM pg_catalog.pg_index")
	}

	PG_CATALOG_TABLE_NAMES = extractTableNames(result)
	return result
}

func CreateInformationSchemaTableQueries(config *Config) []string {
	var tablesViewQuery string

	// For DuckLake: Create view from duckdb_tables() to show attached catalog tables
	if config.CommonConfig.Ducklake.CatalogUrl != "" {
		catalogName := config.CommonConfig.Ducklake.CatalogName
		tablesViewQuery = `CREATE VIEW ` + PG_TABLE_TABLES + ` AS
		SELECT
			'` + config.Database + `' AS table_catalog,
			'public' AS table_schema,
			t.table_name,
			'BASE TABLE' AS table_type,
			NULL AS self_referencing_column_name,
			NULL AS reference_generation,
			NULL AS user_defined_type_catalog,
			NULL AS user_defined_type_schema,
			NULL AS user_defined_type_name,
			'YES' AS is_insertable_into,
			'NO' AS is_typed,
			NULL AS commit_action
		FROM duckdb_databases() d
		JOIN duckdb_tables() t ON d.database_oid = t.database_oid
		WHERE d.database_name = '` + catalogName + `'
		UNION ALL
		SELECT
			table_catalog,
			table_schema,
			table_name,
			table_type,
			self_referencing_column_name,
			reference_generation,
			user_defined_type_catalog,
			user_defined_type_schema,
			user_defined_type_name,
			is_insertable_into,
			is_typed,
			commit_action
		FROM information_schema.tables
		WHERE table_type != 'VIEW' AND table_schema NOT IN ('main', 'pg_catalog', 'information_schema')`
	} else {
		// Legacy Iceberg mode
		tablesViewQuery = `CREATE VIEW ` + PG_TABLE_TABLES + ` AS SELECT
			table_catalog,
			table_schema,
			table_name,
			table_type,
			self_referencing_column_name,
			reference_generation,
			user_defined_type_catalog,
			user_defined_type_schema,
			user_defined_type_name,
			is_insertable_into,
			is_typed,
			commit_action
		FROM information_schema.tables
		WHERE table_type != 'VIEW' AND table_schema != 'main'`
	}

	result := []string{
		// Dynamic views
		// DuckDB does not support udt_catalog, udt_schema, udt_name
		`CREATE VIEW ` + PG_TABLE_COLUMNS + ` AS
		SELECT
			table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision, character_set_catalog, character_set_schema, character_set_name, collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name,
			'` + config.Database + `' AS udt_catalog,
			'pg_catalog' AS udt_schema,
			CASE data_type
				WHEN 'BIGINT' THEN 'int8'
				WHEN 'BIGINT[]' THEN '_int8'
				WHEN 'BLOB' THEN 'bytea'
				WHEN 'BLOB[]' THEN '_bytea'
				WHEN 'BOOLEAN' THEN 'bool'
				WHEN 'BOOLEAN[]' THEN '_bool'
				WHEN 'DATE' THEN 'date'
				WHEN 'DATE[]' THEN '_date'
				WHEN 'FLOAT' THEN 'float4'
				WHEN 'FLOAT[]' THEN '_float4'
				WHEN 'DOUBLE' THEN 'float8'
				WHEN 'DOUBLE[]' THEN '_float8'
				WHEN 'DECIMAL' THEN 'numeric'
				WHEN 'DECIMAL[]' THEN '_numeric'
				WHEN 'INTEGER' THEN 'int4'
				WHEN 'INTEGER[]' THEN '_int4'
				WHEN 'VARCHAR' THEN 'text'
				WHEN 'VARCHAR[]' THEN '_text'
				WHEN 'TIME' THEN 'time'
				WHEN 'TIME[]' THEN '_time'
				WHEN 'TIMESTAMP' THEN 'timestamp'
				WHEN 'TIMESTAMP[]' THEN '_timestamp'
				WHEN 'UUID' THEN 'uuid'
				WHEN 'UUID[]' THEN '_uuid'
			WHEN 'JSON' THEN 'json'
			WHEN 'JSON[]' THEN '_json'
		ELSE
			CASE
				WHEN starts_with(data_type, 'DECIMAL') THEN 'numeric'
		        ELSE 'unknown'
			END
		END AS udt_name,
		scope_catalog, scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_self_referencing, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle, is_generated, generation_expression, is_updatable
	FROM information_schema.columns`,
	`CREATE VIEW ` + PG_TABLE_TABLE_CONSTRAINTS + ` AS
	SELECT
		'` + config.Database + `' AS constraint_catalog,
		n.nspname AS constraint_schema,
		con.conname AS constraint_name,
		'` + config.Database + `' AS table_catalog,
		n.nspname AS table_schema,
		cl.relname AS table_name,
		CASE con.contype
			WHEN 'p' THEN 'PRIMARY KEY'
			WHEN 'u' THEN 'UNIQUE'
			WHEN 'f' THEN 'FOREIGN KEY'
			WHEN 'c' THEN 'CHECK'
			ELSE 'UNKNOWN'
		END AS constraint_type,
		'NO' AS is_deferrable,
		'NO' AS initially_deferred,
		'YES' AS enforced
	FROM pg_constraint con
	JOIN pg_class cl ON con.conrelid = cl.oid
	JOIN pg_namespace n ON cl.relnamespace = n.oid
	WHERE con.contype = 'p'
	  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	  AND cl.relname NOT LIKE 'ducklake_%'`,
	`CREATE VIEW ` + PG_TABLE_KEY_COLUMN_USAGE + ` AS
	SELECT
		'` + config.Database + `' AS constraint_catalog,
		n.nspname AS constraint_schema,
		con.conname AS constraint_name,
		'` + config.Database + `' AS table_catalog,
		n.nspname AS table_schema,
		cl.relname AS table_name,
		attr.attname AS column_name,
		key_cols.ordinal_position,
		NULL::INTEGER AS position_in_unique_constraint
	FROM pg_constraint con
	JOIN pg_class cl ON con.conrelid = cl.oid
	JOIN pg_namespace n ON cl.relnamespace = n.oid
	JOIN LATERAL (
		SELECT
			unnested.attnum,
			ROW_NUMBER() OVER (ORDER BY ord)::INTEGER AS ordinal_position
		FROM UNNEST(con.conkey) WITH ORDINALITY AS unnested(attnum, ord)
	) key_cols ON TRUE
	JOIN pg_attribute attr ON attr.attrelid = cl.oid AND attr.attnum = key_cols.attnum
	WHERE con.contype = 'p'
	  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	  AND cl.relname NOT LIKE 'ducklake_%'`,
	tablesViewQuery,
}
return result
}
