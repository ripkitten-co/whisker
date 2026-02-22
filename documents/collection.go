package documents

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/indexes"
	"github.com/ripkitten-co/whisker/internal/meta"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type CollectionOf[T any] struct {
	name    string
	table   string
	exec    pg.Executor
	codec   codecs.Codec
	schema  *schema.Bootstrap
	indexes []meta.IndexMeta
}

func Collection[T any](b whisker.Backend, name string) *CollectionOf[T] {
	m := meta.Analyze[T]()
	return &CollectionOf[T]{
		name:    name,
		table:   "whisker_" + name,
		exec:    b.DBExecutor(),
		codec:   b.JSONCodec(),
		schema:  b.SchemaBootstrap(),
		indexes: m.Indexes,
	}
}

func (c *CollectionOf[T]) ensure(ctx context.Context) error {
	if err := c.schema.EnsureCollection(ctx, c.exec, c.name); err != nil {
		return err
	}
	return c.ensureIndexes(ctx)
}

func (c *CollectionOf[T]) ensureIndexes(ctx context.Context) error {
	if len(c.indexes) == 0 {
		return nil
	}
	if tx, ok := c.exec.(pg.Transactional); ok && tx.InTransaction() {
		return nil
	}
	ddls := indexes.IndexDDLs(c.name, c.indexes)
	for i, ddl := range ddls {
		name := indexes.IndexName(c.name, c.indexes[i])
		if c.schema.IsIndexCreated(name) {
			continue
		}
		if _, err := c.exec.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("collection %s: create index %s: %w", c.name, name, err)
		}
		c.schema.MarkIndexCreated(name)
	}
	return nil
}

func (c *CollectionOf[T]) Insert(ctx context.Context, doc *T) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	id, err := meta.ExtractID(doc)
	if err != nil {
		return fmt.Errorf("collection %s: %w", c.name, err)
	}
	if id == "" {
		return fmt.Errorf("collection %s: insert: ID must not be empty", c.name)
	}

	data, err := c.codec.Marshal(doc)
	if err != nil {
		return fmt.Errorf("collection %s: insert %s: marshal: %w", c.name, id, err)
	}

	sql, args, err := psql.Insert(c.table).Columns("id", "data").Values(id, data).ToSql()
	if err != nil {
		return fmt.Errorf("collection %s: insert %s: build sql: %w", c.name, id, err)
	}

	_, err = c.exec.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("collection %s: insert %s: %w", c.name, id, err)
	}

	meta.SetVersion(doc, 1)
	return nil
}

func (c *CollectionOf[T]) Update(ctx context.Context, doc *T) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	id, err := meta.ExtractID(doc)
	if err != nil {
		return fmt.Errorf("collection %s: update: %w", c.name, err)
	}

	currentVersion, hasVersion := meta.ExtractVersion(doc)
	data, err := c.codec.Marshal(doc)
	if err != nil {
		return fmt.Errorf("collection %s: update %s: marshal: %w", c.name, id, err)
	}

	newVersion := currentVersion + 1
	builder := psql.Update(c.table).
		Set("data", data).
		Set("version", newVersion).
		Set("updated_at", sq.Expr("now()")).
		Where(sq.Eq{"id": id})

	if hasVersion {
		builder = builder.Where(sq.Eq{"version": currentVersion})
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return fmt.Errorf("collection %s: update %s: build sql: %w", c.name, id, err)
	}

	tag, err := c.exec.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("collection %s: update %s: %w", c.name, id, err)
	}

	if tag.RowsAffected() == 0 {
		if hasVersion {
			return fmt.Errorf("collection %s: update %s: %w", c.name, id, whisker.ErrConcurrencyConflict)
		}
		return fmt.Errorf("collection %s: update %s: %w", c.name, id, whisker.ErrNotFound)
	}

	meta.SetVersion(doc, newVersion)
	return nil
}

func (c *CollectionOf[T]) Delete(ctx context.Context, id string) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	query, args, err := psql.Delete(c.table).Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return fmt.Errorf("collection %s: delete %s: build sql: %w", c.name, id, err)
	}

	tag, err := c.exec.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("collection %s: delete %s: %w", c.name, id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("collection %s: delete %s: %w", c.name, id, whisker.ErrNotFound)
	}
	return nil
}

func (c *CollectionOf[T]) Count(ctx context.Context) (int64, error) {
	return c.Query().Count(ctx)
}

func (c *CollectionOf[T]) Exists(ctx context.Context, id string) (bool, error) {
	if err := c.ensure(ctx); err != nil {
		return false, err
	}
	builder := psql.Select("1").From(c.table).Where(sq.Eq{"id": id})
	innerSQL, args, err := builder.ToSql()
	if err != nil {
		return false, fmt.Errorf("collection %s: exists: build sql: %w", c.name, err)
	}
	sql := fmt.Sprintf("SELECT EXISTS(%s)", innerSQL)
	var exists bool
	err = c.exec.QueryRow(ctx, sql, args...).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("collection %s: exists %s: %w", c.name, id, err)
	}
	return exists, nil
}

func (c *CollectionOf[T]) Load(ctx context.Context, id string) (*T, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}

	sql, args, err := psql.Select("data", "version").From(c.table).Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("collection %s: load %s: build sql: %w", c.name, id, err)
	}

	var data []byte
	var version int
	err = c.exec.QueryRow(ctx, sql, args...).Scan(&data, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("collection %s: load %s: %w", c.name, id, whisker.ErrNotFound)
		}
		return nil, fmt.Errorf("collection %s: load %s: %w", c.name, id, err)
	}

	var doc T
	if err := c.codec.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("collection %s: load %s: unmarshal: %w", c.name, id, err)
	}

	meta.SetID(&doc, id)
	meta.SetVersion(&doc, version)
	return &doc, nil
}
