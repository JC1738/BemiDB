package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserTypeCast struct {
	utils  *ParserUtils
	config *Config
}

func NewParserTypeCast(config *Config) *ParserTypeCast {
	return &ParserTypeCast{utils: NewParserUtils(config), config: config}
}

func (parser *ParserTypeCast) TypeCast(node *pgQuery.Node) *pgQuery.TypeCast {
	if node.GetTypeCast() == nil {
		return nil
	}

	typeCast := node.GetTypeCast()
	if len(typeCast.TypeName.Names) == 0 {
		return nil
	}

	return typeCast
}

func (parser *ParserTypeCast) TypeName(typeCast *pgQuery.TypeCast) string {
	if typeCast == nil {
		return ""
	}

	typeNameNode := typeCast.TypeName
	var typeNames []string

	for _, name := range typeNameNode.Names {
		typeNames = append(typeNames, name.GetString_().Sval)
	}

	typeName := strings.Join(typeNames, ".")

	if typeNameNode.ArrayBounds != nil {
		typeName += "[]"
	}

	return typeName
}

func (parser *ParserTypeCast) SetTypeName(typeCast *pgQuery.TypeCast, typeName string) {
	if typeCast == nil || len(typeCast.TypeName.Names) != 1 {
		return
	}

	typeCast.TypeName.Names[0] = pgQuery.MakeStrNode(typeName)
}

func (parser *ParserTypeCast) NestedTypeCast(typeCast *pgQuery.TypeCast) *pgQuery.TypeCast {
	return parser.TypeCast(typeCast.Arg)
}

// "value" COLLATE pg_catalog.default -> "value"
func (parser *ParserTypeCast) RemovedDefaultCollateClause(node *pgQuery.Node) *pgQuery.Node {
	collname := node.GetCollateClause().Collname

	if len(collname) == 2 && collname[0].GetString_().Sval == "pg_catalog" && collname[1].GetString_().Sval == "default" {
		return node.GetCollateClause().Arg
	}

	return node
}

func (parser *ParserTypeCast) ArgStringValue(typeCast *pgQuery.TypeCast) string {
	return typeCast.Arg.GetAConst().GetSval().Sval
}

// pg_catalog.[type] -> [type]
func (parser *ParserTypeCast) RemovePgCatalog(typeCast *pgQuery.TypeCast) {
	if typeCast != nil && len(typeCast.TypeName.Names) == 2 && typeCast.TypeName.Names[0].GetString_().Sval == PG_SCHEMA_PG_CATALOG {
		typeCast.TypeName.Names = typeCast.TypeName.Names[1:]
	}
}

func (parser *ParserTypeCast) SetTypeCastArg(typeCast *pgQuery.TypeCast, arg *pgQuery.Node) {
	typeCast.Arg = arg
}

func (parser *ParserTypeCast) MakeListValueFromArray(node *pgQuery.Node) *pgQuery.Node {
	// Handle non-constant arrays (e.g., array[$1, $2]) - return as-is
	aConst := node.GetAConst()
	if aConst == nil {
		return node
	}
	sval := aConst.GetSval()
	if sval == nil {
		return node
	}

	arrayStr := sval.Sval
	arrayStr = strings.Trim(arrayStr, "{}")
	elements := strings.Split(arrayStr, ",")

	funcCall := &pgQuery.FuncCall{
		Funcname: []*pgQuery.Node{
			pgQuery.MakeStrNode("list_value"),
		},
	}

	for _, elem := range elements {
		funcCall.Args = append(funcCall.Args,
			pgQuery.MakeAConstStrNode(elem, 0))
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_FuncCall{
			FuncCall: funcCall,
		},
	}
}

// SELECT p.oid
// FROM pg_proc p
// JOIN pg_namespace n ON n.oid = p.pronamespace
// WHERE n.nspname = 'schema' AND p.proname = 'function'
// (or WHERE n.nspname = ANY(current_schemas(TRUE)) when schema isn't provided)
func (parser *ParserTypeCast) MakeSubselectOidBySchemaFunctionArg(argumentNode *pgQuery.Node) *pgQuery.Node {
	targetNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{
			pgQuery.MakeStrNode("p"),
			pgQuery.MakeStrNode("oid"),
		}, 0),
		0,
	)

	joinNode := pgQuery.MakeJoinExprNode(
		pgQuery.JoinType_JOIN_INNER,
		pgQuery.MakeFullRangeVarNode("", "pg_proc", "p", 0),
		pgQuery.MakeFullRangeVarNode("", "pg_namespace", "n", 0),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{
				pgQuery.MakeStrNode("="),
			},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("n"),
				pgQuery.MakeStrNode("oid"),
			}, 0),
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("p"),
				pgQuery.MakeStrNode("pronamespace"),
			}, 0),
			0,
		),
	)

	if argumentNode.GetAConst() == nil {
		// NOTE: ::regproc::oid on non-constants is not fully supported yet
		return parser.utils.MakeNullNode()
	}

	value := argumentNode.GetAConst().GetSval().Sval
	parsedLiteral := parseRegprocLiteral(value)
	if parsedLiteral.function == "" {
		return parser.utils.MakeNullNode()
	}

	conditions := []*pgQuery.Node{
		makeNamespaceCondition(parsedLiteral.schema),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{
				pgQuery.MakeStrNode("="),
			},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("p"),
				pgQuery.MakeStrNode("proname"),
			}, 0),
			pgQuery.MakeAConstStrNode(parsedLiteral.function, 0),
			0,
		),
	}

	if argCount := len(parsedLiteral.argTypes); argCount > 0 {
		conditions = append(conditions, pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("p"),
				pgQuery.MakeStrNode("pronargs"),
			}, 0),
			pgQuery.MakeAConstIntNode(int64(argCount), 0),
			0,
		))

		for idx, argType := range parsedLiteral.argTypes {
			typeOidNode := makeTypeOidLookupNode(argType.schema, argType.typeName)
			if typeOidNode == nil {
				return parser.utils.MakeNullNode()
			}
			conditions = append(conditions, pgQuery.MakeAExprNode(
				pgQuery.A_Expr_Kind_AEXPR_OP,
				[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
				makeProArgTypeElementNode(idx),
				typeOidNode,
				0,
			))
		}
	}

	whereNode := pgQuery.MakeBoolExprNode(
		pgQuery.BoolExprType_AND_EXPR,
		conditions,
		0,
	)

	return &pgQuery.Node{
		Node: &pgQuery.Node_SubLink{
			SubLink: &pgQuery.SubLink{
				SubLinkType: pgQuery.SubLinkType_EXPR_SUBLINK,
				Subselect: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  []*pgQuery.Node{targetNode},
							FromClause:  []*pgQuery.Node{joinNode},
							WhereClause: whereNode,
						},
					},
				},
			},
		},
	}
}

// SELECT c.oid
// FROM pg_class c
// JOIN pg_namespace n ON n.oid = c.relnamespace
// WHERE n.nspname = 'schema' AND c.relname = 'table'
func (parser *ParserTypeCast) MakeSubselectOidBySchemaTableArg(argumentNode *pgQuery.Node) *pgQuery.Node {
	targetNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{
			pgQuery.MakeStrNode("c"),
			pgQuery.MakeStrNode("oid"),
		}, 0),
		0,
	)

	joinNode := pgQuery.MakeJoinExprNode(
		pgQuery.JoinType_JOIN_INNER,
		pgQuery.MakeFullRangeVarNode("", "pg_class", "c", 0),
		pgQuery.MakeFullRangeVarNode("", "pg_namespace", "n", 0),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{
				pgQuery.MakeStrNode("="),
			},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("n"),
				pgQuery.MakeStrNode("oid"),
			}, 0),
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("c"),
				pgQuery.MakeStrNode("relnamespace"),
			}, 0),
			0,
		),
	)

	if argumentNode.GetAConst() == nil {
		// NOTE: ::regclass::oid on non-constants is not fully supported yet
		return parser.utils.MakeNullNode()
	}

	value := argumentNode.GetAConst().GetSval().Sval
	qSchemaTable := NewQuerySchemaTableFromString(value)
	if qSchemaTable.Schema == "" {
		qSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}

	whereNode := pgQuery.MakeBoolExprNode(
		pgQuery.BoolExprType_AND_EXPR,
		[]*pgQuery.Node{
			pgQuery.MakeAExprNode(
				pgQuery.A_Expr_Kind_AEXPR_OP,
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("="),
				},
				pgQuery.MakeColumnRefNode([]*pgQuery.Node{
					pgQuery.MakeStrNode("n"),
					pgQuery.MakeStrNode("nspname"),
				}, 0),
				pgQuery.MakeAConstStrNode(qSchemaTable.Schema, 0),
				0,
			),
			pgQuery.MakeAExprNode(
				pgQuery.A_Expr_Kind_AEXPR_OP,
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("="),
				},
				pgQuery.MakeColumnRefNode([]*pgQuery.Node{
					pgQuery.MakeStrNode("c"),
					pgQuery.MakeStrNode("relname"),
				}, 0),
				pgQuery.MakeAConstStrNode(qSchemaTable.Table, 0),
				0,
			),
		},
		0,
	)

	return &pgQuery.Node{
		Node: &pgQuery.Node_SubLink{
			SubLink: &pgQuery.SubLink{
				SubLinkType: pgQuery.SubLinkType_EXPR_SUBLINK,
				Subselect: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  []*pgQuery.Node{targetNode},
							FromClause:  []*pgQuery.Node{joinNode},
							WhereClause: whereNode,
						},
					},
				},
			},
		},
	}

}

type regprocLiteralParts struct {
	schema   string
	function string
	argTypes []typeReference
}

type typeReference struct {
	schema   string
	typeName string
}

func parseRegprocLiteral(value string) regprocLiteralParts {
	value = strings.TrimSpace(value)
	if value == "" {
		return regprocLiteralParts{}
	}

	functionPart := value
	argList := ""

	// Find matching parentheses for function signature
	if parenIdx := strings.Index(value, "("); parenIdx >= 0 {
		closeIdx := findMatchingParen(value, parenIdx)
		if closeIdx > parenIdx {
			functionPart = strings.TrimSpace(value[:parenIdx])
			argList = strings.TrimSpace(value[parenIdx+1 : closeIdx])
		}
	}

	schema, functionName, _, _ := splitQualifiedIdentifier(functionPart)
	parts := splitArgumentList(argList)
	argRefs := make([]typeReference, 0, len(parts))
	for _, part := range parts {
		ref, ok := parseTypeReference(part)
		if !ok {
			return regprocLiteralParts{}
		}
		argRefs = append(argRefs, ref)
	}

	return regprocLiteralParts{
		schema:   schema,
		function: functionName,
		argTypes: argRefs,
	}
}

// findMatchingParen finds the closing parenthesis that matches the opening one at openIdx
func findMatchingParen(s string, openIdx int) int {
	depth := 1
	inQuotes := false

	for i := openIdx + 1; i < len(s); i++ {
		if s[i] == '"' {
			inQuotes = !inQuotes
			continue
		}

		if !inQuotes {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}

	return -1 // No matching paren found
}

func splitQualifiedIdentifier(input string) (string, string, bool, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", false, false
	}

	inQuotes := false
	lastDot := -1

	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '"':
			inQuotes = !inQuotes
		case '.':
			if !inQuotes {
				lastDot = i
			}
		}
	}

	var rawSchema, rawName string
	if lastDot == -1 {
		rawName = strings.TrimSpace(input)
	} else {
		rawSchema = strings.TrimSpace(input[:lastDot])
		rawName = strings.TrimSpace(input[lastDot+1:])
	}

	schemaQuoted := len(rawSchema) >= 2 && rawSchema[0] == '"' && rawSchema[len(rawSchema)-1] == '"'
	nameQuoted := len(rawName) >= 2 && rawName[0] == '"' && rawName[len(rawName)-1] == '"'

	if lastDot == -1 {
		return "", normalizeIdentifierToken(rawName), false, nameQuoted
	}

	return normalizeIdentifierToken(rawSchema), normalizeIdentifierToken(rawName), schemaQuoted, nameQuoted
}

func normalizeIdentifierToken(token string) string {
	token = strings.TrimSpace(token)
	length := len(token)
	if length == 0 {
		return ""
	}

	if token[0] == '"' && token[length-1] == '"' && length >= 2 {
		unescaped := strings.ReplaceAll(token[1:length-1], `""`, `"`)
		return unescaped
	}

	return strings.ToLower(token)
}

func splitArgumentList(argumentList string) []string {
	if argumentList == "" {
		return nil
	}

	var args []string
	var current strings.Builder
	depth := 0
	inQuotes := false

	for i := 0; i < len(argumentList); i++ {
		ch := argumentList[i]

		if ch == '"' {
			inQuotes = !inQuotes
			current.WriteByte(ch)
			continue
		}

		if !inQuotes {
			switch ch {
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			case ',':
				if depth == 0 {
					arg := strings.TrimSpace(current.String())
					if arg != "" {
						args = append(args, arg)
					}
					current.Reset()
					continue
				}
			}
		}

		current.WriteByte(ch)
	}

	if arg := strings.TrimSpace(current.String()); arg != "" {
		args = append(args, arg)
	}

	return args
}

func makeNamespaceCondition(schemaName string) *pgQuery.Node {
	return makeNamespaceConditionForAlias("n", schemaName)
}

func makeBoolConstNode(value bool) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AConst{
			AConst: &pgQuery.A_Const{
				Val: &pgQuery.A_Const_Boolval{
					Boolval: &pgQuery.Boolean{
						Boolval: value,
					},
				},
			},
		},
	}
}

func makeNamespaceConditionForAlias(alias string, schemaName string) *pgQuery.Node {
	columnRef := pgQuery.MakeColumnRefNode([]*pgQuery.Node{
		pgQuery.MakeStrNode(alias),
		pgQuery.MakeStrNode("nspname"),
	}, 0)

	if schemaName == "" {
		return pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP_ANY,
			[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
			columnRef,
			pgQuery.MakeFuncCallNode(
				[]*pgQuery.Node{pgQuery.MakeStrNode("current_schemas")},
				[]*pgQuery.Node{makeBoolConstNode(true)},
				0,
			),
			0,
		)
	}

	return pgQuery.MakeAExprNode(
		pgQuery.A_Expr_Kind_AEXPR_OP,
		[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
		columnRef,
		pgQuery.MakeAConstStrNode(schemaName, 0),
		0,
	)
}

func makeProArgTypeElementNode(index int) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AIndirection{
			AIndirection: &pgQuery.A_Indirection{
				Arg: pgQuery.MakeColumnRefNode([]*pgQuery.Node{
					pgQuery.MakeStrNode("p"),
					pgQuery.MakeStrNode("proargtypes"),
				}, 0),
				Indirection: []*pgQuery.Node{
					{
						Node: &pgQuery.Node_AIndices{
							AIndices: &pgQuery.A_Indices{
								IsSlice: false,
								Uidx:    pgQuery.MakeAConstIntNode(int64(index), 0),
							},
						},
					},
				},
			},
		},
	}
}

func makeTypeOidLookupNode(schemaName string, typeName string) *pgQuery.Node {
	if typeName == "" {
		return nil
	}

	targetNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{
			pgQuery.MakeStrNode("ty"),
			pgQuery.MakeStrNode("oid"),
		}, 0),
		0,
	)

	joinNode := pgQuery.MakeJoinExprNode(
		pgQuery.JoinType_JOIN_INNER,
		pgQuery.MakeFullRangeVarNode("", "pg_type", "ty", 0),
		pgQuery.MakeFullRangeVarNode("", "pg_namespace", "tn", 0),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("tn"),
				pgQuery.MakeStrNode("oid"),
			}, 0),
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("ty"),
				pgQuery.MakeStrNode("typnamespace"),
			}, 0),
			0,
		),
	)

	conditions := []*pgQuery.Node{
		makeNamespaceConditionForAlias("tn", schemaName),
		pgQuery.MakeAExprNode(
			pgQuery.A_Expr_Kind_AEXPR_OP,
			[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{
				pgQuery.MakeStrNode("ty"),
				pgQuery.MakeStrNode("typname"),
			}, 0),
			pgQuery.MakeAConstStrNode(typeName, 0),
			0,
		),
	}

	whereNode := pgQuery.MakeBoolExprNode(
		pgQuery.BoolExprType_AND_EXPR,
		conditions,
		0,
	)

	return &pgQuery.Node{
		Node: &pgQuery.Node_SubLink{
			SubLink: &pgQuery.SubLink{
				SubLinkType: pgQuery.SubLinkType_EXPR_SUBLINK,
				Subselect: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  []*pgQuery.Node{targetNode},
							FromClause:  []*pgQuery.Node{joinNode},
							WhereClause: whereNode,
						},
					},
				},
			},
		},
	}
}

func parseTypeReference(input string) (typeReference, bool) {
	arg := strings.TrimSpace(input)
	if arg == "" {
		return typeReference{}, false
	}

	lowerArg := strings.ToLower(arg)
	if strings.HasPrefix(lowerArg, "variadic ") {
		arg = strings.TrimSpace(arg[len("variadic "):])
	}

	arrayDims := 0
	for strings.HasSuffix(arg, "[]") {
		arrayDims++
		arg = strings.TrimSpace(arg[:len(arg)-2])
	}

	schema, typeName, _, nameQuoted := splitQualifiedIdentifier(arg)
	if typeName == "" {
		typeName = schema
		schema = ""
	}
	if typeName == "" {
		return typeReference{}, false
	}

	canonical := canonicalTypeName(typeName, nameQuoted)
	if canonical == "" {
		canonical = typeName
	}
	if arrayDims > 0 {
		canonical = "_" + canonical
	}

	return typeReference{
		schema:   schema,
		typeName: canonical,
	}, true
}

func canonicalTypeName(name string, wasQuoted bool) string {
	if wasQuoted {
		return name
	}

	lower := strings.ToLower(name)
	if mapped, ok := typeAliasMap[lower]; ok {
		return mapped
	}

	return name
}

var typeAliasMap = map[string]string{
	"int":                         "int4",
	"integer":                     "int4",
	"int4":                        "int4",
	"smallint":                    "int2",
	"int2":                        "int2",
	"bigint":                      "int8",
	"int8":                        "int8",
	"serial":                      "int4",
	"bigserial":                   "int8",
	"real":                        "float4",
	"float4":                      "float4",
	"double precision":            "float8",
	"float8":                      "float8",
	"boolean":                     "bool",
	"bool":                        "bool",
	"character varying":           "varchar",
	"varchar":                     "varchar",
	"character":                   "bpchar",
	"char":                        "bpchar",
	"bpchar":                      "bpchar",
	"numeric":                     "numeric",
	"decimal":                     "numeric",
	"timestamp":                   "timestamp",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
	"timestamptz":                 "timestamptz",
	"time":                        "time",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
	"timetz":                      "timetz",
	"interval":                    "interval",
	"uuid":                        "uuid",
	"text":                        "text",
	"bytea":                       "bytea",
	"name":                        "name",
	"json":                        "json",
	"jsonb":                       "jsonb",
	"xml":                         "xml",
	"regclass":                    "regclass",
	"regproc":                     "regproc",
	"regprocedure":                "regprocedure",
	"regtype":                     "regtype",
	"anyelement":                  "anyelement",
	"anyarray":                    "anyarray",
}
