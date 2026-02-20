package documents

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/internal/tags"
	"github.com/ripkitten-co/whisker/schema"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type CollectionOf[T any] struct {
	name   string
	table  string
	exec   pg.Executor
	codec  codecs.Codec
	schema *schema.Bootstrap
}

func Collection[T any](b whisker.Backend, name string) *CollectionOf[T] {
	return &CollectionOf[T]{
		name:   name,
		table:  "whisker_" + name,
		exec:   b.DBExecutor(),
		codec:  b.JSONCodec(),
		schema: b.SchemaBootstrap(),
	}
}

func (c *CollectionOf[T]) ensure(ctx context.Context) error {
	return c.schema.EnsureCollection(ctx, c.exec, c.name)
}

func (c *CollectionOf[T]) Insert(ctx context.Context, doc *T) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	id, err := tags.ExtractID(doc)
	if err != nil {
		return fmt.Errorf("collection %s: %w", c.name, err)
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

	tags.SetVersion(doc, 1)
	return nil
}

func (c *CollectionOf[T]) Update(ctx context.Context, doc *T) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	id, err := tags.ExtractID(doc)
	if err != nil {
		return fmt.Errorf("collection %s: update: %w", c.name, err)
	}

	currentVersion, hasVersion := tags.ExtractVersion(doc)
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

	tags.SetVersion(doc, newVersion)
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

	tags.SetVersion(&doc, version)
	return &doc, nil
}
