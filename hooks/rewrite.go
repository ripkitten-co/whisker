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
