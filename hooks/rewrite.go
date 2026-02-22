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
		jsonPairs = append(jsonPairs, fmt.Sprintf("'%s', $%d::text", dc.jsonKey, argIdx))
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

// rewriteUpdate transforms an ORM UPDATE SET into a Whisker JSONB update.
// SET columns are packed into jsonb_build_object, version is auto-incremented.
func rewriteUpdate(info *modelInfo, sql string, args []any) (string, []any, error) {
	setCols, setArgs, whereClause, whereArgs := parseUpdate(sql, args, info)

	var jsonPairs []string
	var newArgs []any
	argIdx := 1

	for i, col := range setCols {
		for _, dc := range info.dataCols {
			if strings.EqualFold(col, dc.name) {
				jsonPairs = append(jsonPairs, fmt.Sprintf("'%s', $%d::text", dc.jsonKey, argIdx))
				newArgs = append(newArgs, setArgs[i])
				argIdx++
				break
			}
		}
	}

	jsonExpr := "'{}'::jsonb"
	if len(jsonPairs) > 0 {
		jsonExpr = fmt.Sprintf("jsonb_build_object(%s)", strings.Join(jsonPairs, ", "))
	}

	where := rewriteColumnRefs(whereClause, info)
	for _, wa := range whereArgs {
		newArgs = append(newArgs, wa)
		argIdx++
	}

	rewritten := fmt.Sprintf(
		"UPDATE %s SET data = %s, version = version + 1, updated_at = now() WHERE %s",
		info.table, jsonExpr, renumberArgs(where, len(setCols)+1, len(newArgs)-len(whereArgs)+1),
	)

	return rewritten, newArgs, nil
}

func rewriteDelete(info *modelInfo, sql string, args []any) (string, []any, error) {
	rewritten := replaceTableName(sql, info.name, info.table)
	upper := strings.ToUpper(rewritten)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx >= 0 {
		before := rewritten[:whereIdx+7]
		after := rewritten[whereIdx+7:]
		after = rewriteColumnRefs(after, info)
		rewritten = before + after
	}
	return rewritten, args, nil
}

func parseUpdate(sql string, args []any, info *modelInfo) ([]string, []any, string, []any) {
	upper := strings.ToUpper(sql)
	setIdx := strings.Index(upper, " SET ") + 5
	whereIdx := strings.Index(upper, " WHERE ")

	var setClause, whereClause string
	if whereIdx > 0 {
		setClause = sql[setIdx:whereIdx]
		whereClause = sql[whereIdx+7:]
	} else {
		setClause = sql[setIdx:]
	}

	parts := strings.Split(setClause, ",")
	var cols []string
	var setArgIdxs []int
	for _, p := range parts {
		eqIdx := strings.IndexByte(p, '=')
		if eqIdx == -1 {
			continue
		}
		col := strings.TrimSpace(p[:eqIdx])
		col = strings.Trim(col, "\"")
		cols = append(cols, strings.ToLower(col))

		val := strings.TrimSpace(p[eqIdx+1:])
		if len(val) > 1 && val[0] == '$' {
			idx := 0
			for _, c := range val[1:] {
				if c >= '0' && c <= '9' {
					idx = idx*10 + int(c-'0')
				} else {
					break
				}
			}
			setArgIdxs = append(setArgIdxs, idx-1)
		}
	}

	var setArgs []any
	for _, idx := range setArgIdxs {
		if idx < len(args) {
			setArgs = append(setArgs, args[idx])
		}
	}

	var whereArgs []any
	for i := len(setArgIdxs); i < len(args); i++ {
		whereArgs = append(whereArgs, args[i])
	}

	return cols, setArgs, whereClause, whereArgs
}

func renumberArgs(sql string, oldStart, newStart int) string {
	result := sql
	offset := newStart - oldStart
	if offset == 0 {
		return result
	}
	for i := 20; i >= oldStart; i-- {
		old := fmt.Sprintf("$%d", i)
		replacement := fmt.Sprintf("$%d", i+offset)
		result = strings.ReplaceAll(result, old, replacement)
	}
	return result
}

// rewriteCreateTable replaces an ORM-generated CREATE TABLE with Whisker's
// standard document table DDL.
func rewriteCreateTable(info *modelInfo, _ string) (string, error) {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id TEXT PRIMARY KEY,
	data JSONB NOT NULL,
	version INTEGER NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, info.table), nil
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
