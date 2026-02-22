package hooks

import (
	"strings"
)

type sqlOp int

const (
	opUnknown sqlOp = iota
	opInsert
	opSelect
	opSelectJoin
	opUpdate
	opDelete
	opCreateTable
)

// parseSQL extracts the primary table name and operation from an SQL statement.
// Returns (table, op, true) if recognized, or ("", 0, false) for passthrough.
func parseSQL(sql string) (string, sqlOp, bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	switch {
	case strings.HasPrefix(upper, "INSERT INTO "):
		return extractInsertTable(trimmed[12:]), opInsert, true

	case strings.HasPrefix(upper, "SELECT "):
		table, hasJoin := extractSelectTable(trimmed, upper)
		if table == "" {
			return "", opUnknown, false
		}
		if hasJoin {
			return table, opSelectJoin, true
		}
		return table, opSelect, true

	case strings.HasPrefix(upper, "UPDATE "):
		return extractUpdateTable(trimmed[7:]), opUpdate, true

	case strings.HasPrefix(upper, "DELETE FROM "):
		return extractDeleteTable(trimmed[12:]), opDelete, true

	case strings.HasPrefix(upper, "CREATE TABLE "):
		return extractCreateTable(trimmed, upper), opCreateTable, true

	default:
		return "", opUnknown, false
	}
}

func extractInsertTable(after string) string {
	return extractFirstWord(after)
}

func extractSelectTable(sql, upper string) (string, bool) {
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		return "", false
	}
	after := sql[fromIdx+6:]
	table := extractFirstWord(after)
	hasJoin := strings.Contains(upper[fromIdx:], " JOIN ")
	return table, hasJoin
}

func extractUpdateTable(after string) string {
	return extractFirstWord(after)
}

func extractDeleteTable(after string) string {
	return extractFirstWord(after)
}

func extractCreateTable(sql, upper string) string {
	rest := upper
	idx := strings.Index(rest, "CREATE TABLE ")
	rest = rest[idx+13:]
	if strings.HasPrefix(rest, "IF NOT EXISTS ") {
		rest = rest[14:]
		sql = sql[idx+13+14:]
	} else {
		sql = sql[idx+13:]
	}
	return extractFirstWord(sql)
}

func extractFirstWord(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 0 && s[0] == '"' {
		end := strings.IndexByte(s[1:], '"')
		if end >= 0 {
			return s[1 : end+1]
		}
	}
	end := 0
	for end < len(s) {
		c := s[end]
		if c == ' ' || c == '(' || c == '\t' || c == '\n' || c == ',' || c == ';' {
			break
		}
		end++
	}
	return strings.Trim(s[:end], "\"")
}
