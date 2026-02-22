package documents

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/meta"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

type Direction string

const (
	Asc  Direction = "ASC"
	Desc Direction = "DESC"
)

type orderByClause struct {
	field     string
	direction Direction
}

var knownColumns = map[string]bool{
	"id": true, "version": true, "created_at": true, "updated_at": true,
}

func resolveField(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("query: empty field name")
	}
	if knownColumns[field] {
		return field, nil
	}
	if strings.Contains(field, "->") {
		return field, nil
	}
	for _, c := range field {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return "", fmt.Errorf("query: invalid field name %q", field)
		}
	}
	return fmt.Sprintf("data->>'%s'", field), nil
}

var allowedOps = map[string]bool{
	"=": true, "!=": true,
	">": true, "<": true,
	">=": true, "<=": true,
}

type condition struct {
	field string
	op    string
	value any
}

type Query[T any] struct {
	name       string
	table      string
	exec       pg.Executor
	codec      codecs.Codec
	schema     *schema.Bootstrap
	indexes    []meta.IndexMeta
	conditions []condition
	orderBys   []orderByClause
	limit      *uint64
	offset     *uint64
	afterVal   any
}

func (q *Query[T]) clone() *Query[T] {
	c := &Query[T]{
		name:     q.name,
		table:    q.table,
		exec:     q.exec,
		codec:    q.codec,
		schema:   q.schema,
		indexes:  q.indexes,
		limit:    q.limit,
		offset:   q.offset,
		afterVal: q.afterVal,
	}
	if len(q.conditions) > 0 {
		c.conditions = make([]condition, len(q.conditions))
		copy(c.conditions, q.conditions)
	}
	if len(q.orderBys) > 0 {
		c.orderBys = make([]orderByClause, len(q.orderBys))
		copy(c.orderBys, q.orderBys)
	}
	return c
}

func (c *CollectionOf[T]) Query() *Query[T] {
	return &Query[T]{
		name:    c.name,
		table:   c.table,
		exec:    c.exec,
		codec:   c.codec,
		schema:  c.schema,
		indexes: c.indexes,
	}
}

func (c *CollectionOf[T]) Where(field, op string, value any) *Query[T] {
	return c.Query().Where(field, op, value)
}

func (q *Query[T]) Where(field, op string, value any) *Query[T] {
	c := q.clone()
	c.conditions = append(c.conditions, condition{field, op, value})
	return c
}

func (q *Query[T]) toSQL() (string, []any, error) {
	builder := psql.Select("id", "data", "version").From(q.table)

	for _, c := range q.conditions {
		if !allowedOps[c.op] {
			return "", nil, fmt.Errorf("query: unsupported operator %q", c.op)
		}
		field, err := resolveField(c.field)
		if err != nil {
			return "", nil, err
		}
		expr := fmt.Sprintf("%s %s ?", field, c.op)
		builder = builder.Where(sq.Expr(expr, c.value))
	}

	return builder.ToSql()
}

func (q *Query[T]) Execute(ctx context.Context) ([]*T, error) {
	col := &CollectionOf[T]{
		name:    q.name,
		table:   q.table,
		exec:    q.exec,
		codec:   q.codec,
		schema:  q.schema,
		indexes: q.indexes,
	}
	if err := col.ensure(ctx); err != nil {
		return nil, err
	}

	sql, args, err := q.toSQL()
	if err != nil {
		return nil, err
	}

	rows, err := q.exec.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query: execute: %w", err)
	}
	defer rows.Close()

	var results []*T
	for rows.Next() {
		var id string
		var data []byte
		var version int
		if err := rows.Scan(&id, &data, &version); err != nil {
			return nil, fmt.Errorf("query: scan: %w", err)
		}

		var doc T
		if err := q.codec.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("query: unmarshal: %w", err)
		}
		meta.SetID(&doc, id)
		meta.SetVersion(&doc, version)
		results = append(results, &doc)
	}

	return results, rows.Err()
}
