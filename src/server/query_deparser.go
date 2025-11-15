package main

import (
	"strings"
	"unicode/utf8"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

const POSTGRES_MAX_IDENTIFIER_LENGTH = 63

// ExtractLongIdentifiersFromQuery extracts all quoted identifiers > 63 bytes from a query string.
//
// This function scans the original query string BEFORE parsing to capture identifiers that
// will be truncated by PostgreSQL's parser. It builds a map of truncated -> full identifier
// names that can be used to restore the full names after deparsing.
//
// LIMITATIONS (documented as low-priority theoretical concerns):
// 1. Only handles double-quoted identifiers (single-quoted are string literals, not identifiers)
// 2. Assumes no two identifiers share the same 63-byte truncated prefix (astronomically unlikely)
// 3. Does not extract from E-strings or dollar-quoted strings (identifiers don't appear in literals)
//
// These limitations do not affect real-world usage with Metabase or typical PostgreSQL queries.
// All long identifiers in valid SQL must be double-quoted, as PostgreSQL truncates unquoted
// identifiers at DDL time.
func ExtractLongIdentifiersFromQuery(query string) map[string]string {
	identifierMap := make(map[string]string)
	i := 0
	for i < len(query) {
		// Skip single-quoted strings
		if query[i] == '\'' {
			i++
			for i < len(query) {
				if query[i] == '\'' {
					if i+1 < len(query) && query[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}

		// Handle double-quoted identifiers
		if query[i] == '"' {
			i++
			var identifier strings.Builder
			for i < len(query) {
				if query[i] == '"' {
					if i+1 < len(query) && query[i+1] == '"' {
						identifier.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				identifier.WriteByte(query[i])
				i++
			}

			fullName := identifier.String()
			if len(fullName) > POSTGRES_MAX_IDENTIFIER_LENGTH {
				truncated := truncateIdentifier(fullName)
				identifierMap[truncated] = fullName
			}
			continue
		}

		i++
	}
	return identifierMap
}

// DeparseWithLongIdentifiers deparses a query and fixes truncated identifiers
func DeparseWithLongIdentifiers(tree *pgQuery.ParseResult, identifierMap map[string]string) (string, error) {
	// Collect additional identifiers from AST (though they will be truncated)
	for _, stmt := range tree.Stmts {
		collectLongIdentifiers(stmt.Stmt, identifierMap)
	}

	// Deparse normally (this will truncate long identifiers)
	deparsed, err := pgQuery.Deparse(tree)
	if err != nil {
		return "", err
	}

	// Replace truncated identifiers with full names
	return replaceTruncatedIdentifiers(deparsed, identifierMap), nil
}

// collectLongIdentifiers recursively traverses the AST to find identifiers > 63 bytes
func collectLongIdentifiers(node *pgQuery.Node, identifierMap map[string]string) {
	if node == nil {
		return
	}

	// Check String nodes (column names, aliases, etc.)
	if strNode := node.GetString_(); strNode != nil {
		fullName := strNode.Sval
		if len(fullName) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(fullName)
			identifierMap[truncated] = fullName
		}
	}

	// Traverse SelectStmt
	if selectStmt := node.GetSelectStmt(); selectStmt != nil {
		// Target list (SELECT columns)
		for _, target := range selectStmt.TargetList {
			collectLongIdentifiers(target, identifierMap)
		}
		// FROM clause
		for _, fromNode := range selectStmt.FromClause {
			collectLongIdentifiers(fromNode, identifierMap)
		}
		// WHERE clause
		collectLongIdentifiers(selectStmt.WhereClause, identifierMap)
		// ORDER BY
		for _, sortNode := range selectStmt.SortClause {
			collectLongIdentifiers(sortNode, identifierMap)
		}
		// GROUP BY
		for _, groupNode := range selectStmt.GroupClause {
			collectLongIdentifiers(groupNode, identifierMap)
		}
		// WITH clause
		if selectStmt.WithClause != nil {
			for _, cte := range selectStmt.WithClause.Ctes {
				collectLongIdentifiers(cte, identifierMap)
			}
		}
		// UNION left/right
		if selectStmt.Larg != nil {
			collectLongIdentifiers(&pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: selectStmt.Larg}}, identifierMap)
		}
		if selectStmt.Rarg != nil {
			collectLongIdentifiers(&pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: selectStmt.Rarg}}, identifierMap)
		}
	}

	// Traverse ResTarget (SELECT target with alias)
	if resTarget := node.GetResTarget(); resTarget != nil {
		// Check alias name
		if resTarget.Name != "" && len(resTarget.Name) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(resTarget.Name)
			identifierMap[truncated] = resTarget.Name
		}
		collectLongIdentifiers(resTarget.Val, identifierMap)
	}

	// Traverse ColumnRef
	if columnRef := node.GetColumnRef(); columnRef != nil {
		for _, field := range columnRef.Fields {
			collectLongIdentifiers(field, identifierMap)
		}
	}

	// Traverse TypeCast
	if typeCast := node.GetTypeCast(); typeCast != nil {
		collectLongIdentifiers(typeCast.Arg, identifierMap)
	}

	// Traverse FuncCall
	if funcCall := node.GetFuncCall(); funcCall != nil {
		for _, funcName := range funcCall.Funcname {
			collectLongIdentifiers(funcName, identifierMap)
		}
		for _, arg := range funcCall.Args {
			collectLongIdentifiers(arg, identifierMap)
		}
		collectLongIdentifiers(funcCall.AggFilter, identifierMap)
	}

	// Traverse A_Expr (operators)
	if aExpr := node.GetAExpr(); aExpr != nil {
		collectLongIdentifiers(aExpr.Lexpr, identifierMap)
		collectLongIdentifiers(aExpr.Rexpr, identifierMap)
	}

	// Traverse BoolExpr (AND/OR/NOT)
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			collectLongIdentifiers(arg, identifierMap)
		}
	}

	// Traverse CaseExpr
	if caseExpr := node.GetCaseExpr(); caseExpr != nil {
		for _, whenNode := range caseExpr.Args {
			collectLongIdentifiers(whenNode, identifierMap)
		}
		collectLongIdentifiers(caseExpr.Defresult, identifierMap)
	}

	// Traverse CaseWhen
	if caseWhen := node.GetCaseWhen(); caseWhen != nil {
		collectLongIdentifiers(caseWhen.Expr, identifierMap)
		collectLongIdentifiers(caseWhen.Result, identifierMap)
	}

	// Traverse JoinExpr
	if joinExpr := node.GetJoinExpr(); joinExpr != nil {
		collectLongIdentifiers(joinExpr.Larg, identifierMap)
		collectLongIdentifiers(joinExpr.Rarg, identifierMap)
		collectLongIdentifiers(joinExpr.Quals, identifierMap)
	}

	// Traverse SubLink (subquery)
	if subLink := node.GetSubLink(); subLink != nil {
		collectLongIdentifiers(subLink.Subselect, identifierMap)
	}

	// Traverse NullTest
	if nullTest := node.GetNullTest(); nullTest != nil {
		collectLongIdentifiers(nullTest.Arg, identifierMap)
	}

	// Traverse List
	if list := node.GetList(); list != nil {
		for _, item := range list.Items {
			collectLongIdentifiers(item, identifierMap)
		}
	}

	// Traverse CoalesceExpr
	if coalesceExpr := node.GetCoalesceExpr(); coalesceExpr != nil {
		for _, arg := range coalesceExpr.Args {
			collectLongIdentifiers(arg, identifierMap)
		}
	}

	// Traverse SortBy
	if sortBy := node.GetSortBy(); sortBy != nil {
		collectLongIdentifiers(sortBy.Node, identifierMap)
	}

	// Traverse A_Indirection
	if aIndirection := node.GetAIndirection(); aIndirection != nil {
		collectLongIdentifiers(aIndirection.Arg, identifierMap)
	}

	// Traverse RangeVar (table reference)
	if rangeVar := node.GetRangeVar(); rangeVar != nil {
		if rangeVar.Alias != nil && len(rangeVar.Alias.Aliasname) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(rangeVar.Alias.Aliasname)
			identifierMap[truncated] = rangeVar.Alias.Aliasname
		}
		if rangeVar.Relname != "" && len(rangeVar.Relname) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(rangeVar.Relname)
			identifierMap[truncated] = rangeVar.Relname
		}
		if rangeVar.Schemaname != "" && len(rangeVar.Schemaname) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(rangeVar.Schemaname)
			identifierMap[truncated] = rangeVar.Schemaname
		}
	}

	// Traverse RangeSubselect
	if rangeSubselect := node.GetRangeSubselect(); rangeSubselect != nil {
		// Check subquery alias
		if rangeSubselect.Alias != nil && len(rangeSubselect.Alias.Aliasname) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(rangeSubselect.Alias.Aliasname)
			identifierMap[truncated] = rangeSubselect.Alias.Aliasname
		}
		collectLongIdentifiers(rangeSubselect.Subquery, identifierMap)
	}

	// Traverse CommonTableExpr (CTE)
	if cte := node.GetCommonTableExpr(); cte != nil {
		// Check CTE name
		if cte.Ctename != "" && len(cte.Ctename) > POSTGRES_MAX_IDENTIFIER_LENGTH {
			truncated := truncateIdentifier(cte.Ctename)
			identifierMap[truncated] = cte.Ctename
		}
		collectLongIdentifiers(cte.Ctequery, identifierMap)
	}
}

// truncateIdentifier mimics PostgreSQL's identifier truncation (first 63 bytes)
// Ensures the result is valid UTF-8 by not splitting multibyte characters
func truncateIdentifier(name string) string {
	if len(name) <= POSTGRES_MAX_IDENTIFIER_LENGTH {
		return name
	}

	// Truncate at byte boundary
	truncated := name[:POSTGRES_MAX_IDENTIFIER_LENGTH]

	// If we split a multibyte character, back up to the last valid boundary
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}

	return truncated
}

// replaceTruncatedIdentifiers replaces truncated identifiers in SQL with full names
func replaceTruncatedIdentifiers(sql string, identifierMap map[string]string) string {
	if len(identifierMap) == 0 {
		return sql
	}

	literalRegions := findLiteralRegions(sql)
	regionIdx := 0

	var builder strings.Builder
	builder.Grow(len(sql))

	for i := 0; i < len(sql); {
		if isInsideLiteral(literalRegions, &regionIdx, i) {
			builder.WriteByte(sql[i])
			i++
			continue
		}

		if sql[i] == '"' {
			start := i
			i++

			var identifierBuilder strings.Builder
			for i < len(sql) {
				if sql[i] == '"' {
					if i+1 < len(sql) && sql[i+1] == '"' {
						identifierBuilder.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				identifierBuilder.WriteByte(sql[i])
				i++
			}

			identifier := identifierBuilder.String()
			if fullName, ok := identifierMap[identifier]; ok {
				builder.WriteByte('"')
				builder.WriteString(escapeDoubleQuotes(fullName))
				builder.WriteByte('"')
			} else {
				builder.WriteString(sql[start:i])
			}
			continue
		}

		if isIdentifierStart(sql[i]) {
			start := i
			i++
			for i < len(sql) && isIdentifierPart(sql[i]) {
				i++
			}

			token := sql[start:i]
			if fullName, ok := identifierMap[token]; ok {
				builder.WriteByte('"')
				builder.WriteString(escapeDoubleQuotes(fullName))
				builder.WriteByte('"')
			} else {
				builder.WriteString(token)
			}
			continue
		}

		builder.WriteByte(sql[i])
		i++
	}

	return builder.String()
}

// literalRegion represents a string literal region in SQL
type literalRegion struct {
	start int
	end   int
}

// findLiteralRegions returns the byte ranges of quoted string literals, dollar-quoted
// strings, and SQL comments. Replacements must avoid these regions so we don't mutate
// data values or comments.
func findLiteralRegions(sql string) []literalRegion {
	var regions []literalRegion
	i := 0

	for i < len(sql) {
		// Single-line comment --
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			start := i
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			regions = append(regions, literalRegion{start: start, end: i})
			continue
		}

		// Block comment /* ... */
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			start := i
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < len(sql) {
				i += 2
			}
			regions = append(regions, literalRegion{start: start, end: i})
			continue
		}

		// E'...' escape string
		if (sql[i] == 'E' || sql[i] == 'e') && i+1 < len(sql) && sql[i+1] == '\'' {
			start := i
			i = scanSingleQuotedLiteral(sql, i+2, true)
			regions = append(regions, literalRegion{start: start, end: i})
			continue
		}

		// Regular single-quoted string
		if sql[i] == '\'' {
			start := i
			i = scanSingleQuotedLiteral(sql, i+1, false)
			regions = append(regions, literalRegion{start: start, end: i})
			continue
		}

		// Dollar-quoted string
		if sql[i] == '$' {
			if end := scanDollarQuotedLiteral(sql, i); end > i {
				regions = append(regions, literalRegion{start: i, end: end})
				i = end
				continue
			}
		}

		i++
	}

	return regions
}

func scanSingleQuotedLiteral(sql string, idx int, allowBackslash bool) int {
	for idx < len(sql) {
		if allowBackslash && sql[idx] == '\\' {
			// Skip escaped character (e.g. \' or \n)
			if idx+1 < len(sql) {
				idx += 2
				continue
			}
			idx++
			break
		}

		if sql[idx] == '\'' {
			if idx+1 < len(sql) && sql[idx+1] == '\'' {
				idx += 2
				continue
			}
			idx++
			break
		}

		idx++
	}
	return idx
}

func scanDollarQuotedLiteral(sql string, start int) int {
	// Find the dollar quote tag (e.g., $tag$ or just $$)
	tagEnd := start + 1
	for tagEnd < len(sql) && isDollarTagChar(sql[tagEnd]) {
		tagEnd++
	}
	if tagEnd >= len(sql) || sql[tagEnd] != '$' {
		return start // Not a valid dollar quote
	}

	// Search for the closing tag
	tag := sql[start : tagEnd+1]
	searchFrom := tagEnd + 1
	pos := strings.Index(sql[searchFrom:], tag)
	if pos == -1 {
		return len(sql) // Unclosed dollar quote - treat rest as literal
	}
	return searchFrom + pos + len(tag)
}

func isDollarTagChar(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_'
}

func isInsideLiteral(regions []literalRegion, idx *int, pos int) bool {
	for *idx < len(regions) && pos >= regions[*idx].end {
		*idx++
	}
	if *idx >= len(regions) {
		return false
	}
	return pos >= regions[*idx].start && pos < regions[*idx].end
}

func isIdentifierStart(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		b == '_' ||
		b == '$' ||
		b >= 0x80
}

func isIdentifierPart(b byte) bool {
	return isIdentifierStart(b) ||
		(b >= '0' && b <= '9')
}

func escapeDoubleQuotes(value string) string {
	return strings.ReplaceAll(value, `"`, `""`)
}
