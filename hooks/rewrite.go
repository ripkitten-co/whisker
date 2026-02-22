package hooks

import (
	"fmt"
	"strings"
)

// rewriteInsert transforms an ORM INSERT targeting a plain table into a
// Whisker JSONB insert. Column values that aren't id/version are packed
// into a jsonb_build_object call.
func rewriteInsert(info *modelInfo, sql string, args []any) (string, []any, error) {
	cols := extractInsertColumns(sql)
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("hooks: cannot parse INSERT columns from: %s", sql)
	}

	colArgs := make(map[string]any, len(cols))
	for i, col := range cols {
		if i < len(args) {
			colArgs[col] = args[i]
		}
	}

	var jsonPairs []string
	var newArgs []any
	argIdx := 1

	idVal, ok := colArgs[info.idColumn]
	if !ok {
		return "", nil, fmt.Errorf("hooks: INSERT missing id column %q", info.idColumn)
	}
	newArgs = append(newArgs, idVal)
	argIdx++

	for _, dc := range info.dataCols {
		val, exists := colArgs[dc.name]
		if !exists {
			continue
		}
		jsonPairs = append(jsonPairs, fmt.Sprintf("'%s', $%d", dc.jsonKey, argIdx))
		newArgs = append(newArgs, val)
		argIdx++
	}

	jsonExpr := "'{}'::jsonb"
	if len(jsonPairs) > 0 {
		jsonExpr = fmt.Sprintf("jsonb_build_object(%s)", strings.Join(jsonPairs, ", "))
	}

	rewritten := fmt.Sprintf(
		"INSERT INTO %s (id, data, version, created_at, updated_at) VALUES ($1, %s, 1, now(), now())",
		info.table, jsonExpr,
	)

	return rewritten, newArgs, nil
}

// rewriteSelect transforms an ORM SELECT into a Whisker JSONB query.
// Column references in WHERE are translated to JSONB paths.
// The result includes (id, data, version) — caller unpacks via rows wrapper.
func rewriteSelect(info *modelInfo, sql string, args []any) (string, []any, error) {
	upper := strings.ToUpper(sql)

	rewritten := replaceTableName(sql, info.name, info.table)

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx >= 0 {
		before := rewritten[:whereIdx+7]
		after := rewritten[whereIdx+7:]
		after = rewriteColumnRefs(after, info)
		rewritten = before + after
	}

	rewritten = rewriteSelectColumns(rewritten, info)

	return rewritten, args, nil
}

func replaceTableName(sql, oldTable, newTable string) string {
	result := strings.ReplaceAll(sql, "\""+oldTable+"\"", newTable)
	result = replaceWord(result, oldTable, newTable)
	return result
}

func replaceWord(s, old, replacement string) string {
	idx := 0
	for {
		pos := strings.Index(strings.ToLower(s[idx:]), strings.ToLower(old))
		if pos == -1 {
			break
		}
		absPos := idx + pos
		before := absPos == 0 || !isIdentChar(s[absPos-1])
		after := absPos+len(old) >= len(s) || !isIdentChar(s[absPos+len(old)])
		if before && after {
			s = s[:absPos] + replacement + s[absPos+len(old):]
			idx = absPos + len(replacement)
		} else {
			idx = absPos + len(old)
		}
	}
	return s
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func rewriteColumnRefs(whereClause string, info *modelInfo) string {
	for _, dc := range info.dataCols {
		whereClause = replaceWord(whereClause, dc.name, fmt.Sprintf("data->>'%s'", dc.jsonKey))
	}
	return whereClause
}

func rewriteSelectColumns(sql string, info *modelInfo) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT ")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx == -1 || fromIdx == -1 {
		return sql
	}
	return sql[:selectIdx+7] + "id, data, version" + sql[fromIdx:]
}

func extractInsertColumns(sql string) []string {
	upper := strings.ToUpper(sql)
	start := strings.IndexByte(upper, '(')
	if start == -1 {
		return nil
	}
	end := strings.IndexByte(upper[start:], ')')
	if end == -1 {
		return nil
	}
	colStr := sql[start+1 : start+end]
	parts := strings.Split(colStr, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		col = strings.Trim(col, "\"")
		if col != "" {
			cols = append(cols, strings.ToLower(col))
		}
	}
	return cols
}
