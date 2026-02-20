package documents

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/meta"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

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
	conditions []condition
}

func (c *CollectionOf[T]) Where(field, op string, value any) *Query[T] {
	return &Query[T]{
		name:       c.name,
		table:      c.table,
		exec:       c.exec,
		codec:      c.codec,
		schema:     c.schema,
		conditions: []condition{{field, op, value}},
	}
}

func (q *Query[T]) Where(field, op string, value any) *Query[T] {
	q.conditions = append(q.conditions, condition{field, op, value})
	return q
}

func (q *Query[T]) toSQL() (string, []any, error) {
	builder := psql.Select("id", "data", "version").From(q.table)

	for _, c := range q.conditions {
		if !allowedOps[c.op] {
			return "", nil, fmt.Errorf("query: unsupported operator %q", c.op)
		}
		expr := fmt.Sprintf("data->>? %s ?", c.op)
		builder = builder.Where(sq.Expr(expr, c.field, c.value))
	}

	return builder.ToSql()
}

func (q *Query[T]) Execute(ctx context.Context) ([]*T, error) {
	if err := q.schema.EnsureCollection(ctx, q.exec, q.name); err != nil {
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
