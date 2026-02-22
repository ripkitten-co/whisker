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

// Direction specifies sort order for query results.
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
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
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

// Query builds and executes filtered, sorted, paginated queries against a
// document collection. All methods return a new Query (immutable chaining).
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

// Query starts a fluent query builder for this collection.
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

// Where starts a query with an initial filter condition.
func (c *CollectionOf[T]) Where(field, op string, value any) *Query[T] {
	return c.Query().Where(field, op, value)
}

// Where adds a filter condition. Field names are resolved to JSONB paths
// automatically. Supported operators: =, !=, >, <, >=, <=.
func (q *Query[T]) Where(field, op string, value any) *Query[T] {
	c := q.clone()
	c.conditions = append(c.conditions, condition{field, op, value})
	return c
}

// OrderBy adds a sort clause. Multiple calls add secondary sort keys.
func (q *Query[T]) OrderBy(field string, dir Direction) *Query[T] {
	c := q.clone()
	c.orderBys = append(c.orderBys, orderByClause{field, dir})
	return c
}

// Limit caps the number of results returned.
func (q *Query[T]) Limit(n uint64) *Query[T] {
	c := q.clone()
	if n > 0 {
		c.limit = &n
	}
	return c
}

// Offset skips the first n results.
func (q *Query[T]) Offset(n uint64) *Query[T] {
	c := q.clone()
	c.offset = &n
	return c
}

// After enables cursor-based pagination. Requires at least one OrderBy clause.
// Returns documents after the given value in the sort order.
func (q *Query[T]) After(value any) *Query[T] {
	c := q.clone()
	c.afterVal = value
	return c
}

func (q *Query[T]) applyConditions(builder sq.SelectBuilder) (sq.SelectBuilder, error) {
	for _, c := range q.conditions {
		if !allowedOps[c.op] {
			return builder, fmt.Errorf("query: unsupported operator %q", c.op)
		}
		field, err := resolveField(c.field)
		if err != nil {
			return builder, err
		}
		expr := fmt.Sprintf("%s %s ?", field, c.op)
		builder = builder.Where(sq.Expr(expr, c.value))
	}
	return builder, nil
}

func (q *Query[T]) ensureTable(ctx context.Context) error {
	col := &CollectionOf[T]{
		name:    q.name,
		table:   q.table,
		exec:    q.exec,
		codec:   q.codec,
		schema:  q.schema,
		indexes: q.indexes,
	}
	return col.ensure(ctx)
}

func (q *Query[T]) toCountSQL() (string, []any, error) {
	builder := psql.Select("COUNT(*)").From(q.table)
	builder, err := q.applyConditions(builder)
	if err != nil {
		return "", nil, err
	}
	return builder.ToSql()
}

func (q *Query[T]) toExistsSQL() (string, []any, error) {
	builder := psql.Select("1").From(q.table)
	builder, err := q.applyConditions(builder)
	if err != nil {
		return "", nil, err
	}
	innerSQL, args, err := builder.ToSql()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("SELECT EXISTS(%s)", innerSQL), args, nil
}

// Count returns the number of documents matching the query conditions.
func (q *Query[T]) Count(ctx context.Context) (int64, error) {
	if err := q.ensureTable(ctx); err != nil {
		return 0, err
	}
	sql, args, err := q.toCountSQL()
	if err != nil {
		return 0, err
	}
	var count int64
	err = q.exec.QueryRow(ctx, sql, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query: count: %w", err)
	}
	return count, nil
}

// Exists returns true if at least one document matches the query conditions.
func (q *Query[T]) Exists(ctx context.Context) (bool, error) {
	if err := q.ensureTable(ctx); err != nil {
		return false, err
	}
	sql, args, err := q.toExistsSQL()
	if err != nil {
		return false, err
	}
	var exists bool
	err = q.exec.QueryRow(ctx, sql, args...).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query: exists: %w", err)
	}
	return exists, nil
}

func (q *Query[T]) toSQL() (string, []any, error) {
	builder := psql.Select("id", "data", "version").From(q.table)

	var err error
	builder, err = q.applyConditions(builder)
	if err != nil {
		return "", nil, err
	}

	if q.afterVal != nil {
		if len(q.orderBys) == 0 {
			return "", nil, fmt.Errorf("query: After requires at least one OrderBy clause")
		}
		ob := q.orderBys[0]
		field, err := resolveField(ob.field)
		if err != nil {
			return "", nil, err
		}
		op := ">"
		if ob.direction == Desc {
			op = "<"
		}
		builder = builder.Where(sq.Expr(fmt.Sprintf("%s %s ?", field, op), q.afterVal))
	}

	if len(q.orderBys) > 0 {
		clauses := make([]string, len(q.orderBys))
		for i, ob := range q.orderBys {
			field, err := resolveField(ob.field)
			if err != nil {
				return "", nil, err
			}
			clauses[i] = fmt.Sprintf("%s %s", field, ob.direction)
		}
		builder = builder.OrderBy(clauses...)
	}

	if q.limit != nil {
		builder = builder.Limit(*q.limit)
	}
	if q.offset != nil {
		builder = builder.Offset(*q.offset)
	}

	return builder.ToSql()
}

// Execute runs the query and returns matching documents.
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
