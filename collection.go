package whisker

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/pg"
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

func Collection[T any](b Backend, name string) *CollectionOf[T] {
	be := b.whiskerBackend()
	return &CollectionOf[T]{
		name:   name,
		table:  "whisker_" + name,
		exec:   be.exec,
		codec:  be.codec,
		schema: be.schema,
	}
}

func (c *CollectionOf[T]) ensure(ctx context.Context) error {
	return c.schema.EnsureCollection(ctx, c.exec, c.name)
}

func (c *CollectionOf[T]) Insert(ctx context.Context, doc *T) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}

	id, err := extractID(doc)
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

	setVersion(doc, 1)
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
			return nil, fmt.Errorf("collection %s: load %s: %w", c.name, id, ErrNotFound)
		}
		return nil, fmt.Errorf("collection %s: load %s: %w", c.name, id, err)
	}

	var doc T
	if err := c.codec.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("collection %s: load %s: unmarshal: %w", c.name, id, err)
	}

	setVersion(&doc, version)
	return &doc, nil
}
