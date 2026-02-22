package documents

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/indexes"
	"github.com/ripkitten-co/whisker/internal/meta"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// CollectionOf provides typed CRUD operations for a named document collection.
// Documents are stored as JSONB in a whisker_{name} table with automatic schema
// creation and optional index management.
type CollectionOf[T any] struct {
	name         string
	table        string
	exec         pg.Executor
	codec        codecs.Codec
	schema       *schema.Bootstrap
	indexes      []meta.IndexMeta
	maxBatchSize int
}

// Collection creates a new typed collection backed by the given store.
func Collection[T any](b whisker.Backend, name string) *CollectionOf[T] {
	m := meta.Analyze[T]()
	return &CollectionOf[T]{
		name:         name,
		table:        "whisker_" + name,
		exec:         b.DBExecutor(),
		codec:        b.JSONCodec(),
		schema:       b.SchemaBootstrap(),
		indexes:      m.Indexes,
		maxBatchSize: b.MaxBatchSize(),
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

// Insert stores a new document. The document must have a non-empty ID field.
// On success, the document's Version is set to 1.
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

// Update replaces an existing document's data. If the document has a Version
// field, optimistic concurrency is enforced — a concurrent modification returns
// ErrConcurrencyConflict. On success, Version is incremented.
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

// Delete removes a document by ID. Returns ErrNotFound if absent.
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

// Count returns the total number of documents in the collection.
func (c *CollectionOf[T]) Count(ctx context.Context) (int64, error) {
	return c.Query().Count(ctx)
}

// Exists checks whether a document with the given ID exists.
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

// Load retrieves a single document by ID. Returns ErrNotFound if absent.
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

// InsertMany stores multiple documents in a single INSERT statement.
// All documents must have non-empty ID fields. On success, each document's
// Version is set to 1. Returns a BatchError on unique constraint violations.
func (c *CollectionOf[T]) InsertMany(ctx context.Context, docs []*T) error {
	if len(docs) == 0 {
		return nil
	}
	if err := c.checkBatchSize(len(docs)); err != nil {
		return err
	}
	if err := c.ensure(ctx); err != nil {
		return err
	}

	builder := psql.Insert(c.table).Columns("id", "data")
	ids := make([]string, len(docs))

	for i, doc := range docs {
		id, err := meta.ExtractID(doc)
		if err != nil {
			return fmt.Errorf("collection %s: %w", c.name, err)
		}
		if id == "" {
			return fmt.Errorf("collection %s: insert many: document %d: ID must not be empty", c.name, i)
		}
		ids[i] = id

		data, err := c.codec.Marshal(doc)
		if err != nil {
			return fmt.Errorf("collection %s: insert many %s: marshal: %w", c.name, id, err)
		}
		builder = builder.Values(id, data)
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return fmt.Errorf("collection %s: insert many: build sql: %w", c.name, err)
	}

	_, err = c.exec.Exec(ctx, sql, args...)
	if err != nil {
		if isPgUniqueViolation(err) {
			errs := map[string]error{}
			for _, id := range ids {
				errs[id] = whisker.ErrDuplicateID
			}
			return &BatchError{Op: "insert", Total: len(ids), Errors: errs}
		}
		return fmt.Errorf("collection %s: insert many: %w", c.name, err)
	}

	for _, doc := range docs {
		meta.SetVersion(doc, 1)
	}
	return nil
}

// LoadMany retrieves multiple documents by ID in a single SELECT with WHERE IN.
// Documents are returned in no guaranteed order. If some IDs are missing, the found
// documents are returned alongside a BatchError listing the missing IDs.
func (c *CollectionOf[T]) LoadMany(ctx context.Context, ids []string) ([]*T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := c.checkBatchSize(len(ids)); err != nil {
		return nil, err
	}
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}

	query, args, err := psql.Select("id", "data", "version").
		From(c.table).
		Where(sq.Eq{"id": ids}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("collection %s: load many: build sql: %w", c.name, err)
	}

	rows, err := c.exec.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("collection %s: load many: %w", c.name, err)
	}
	defer rows.Close()

	foundIDs := make(map[string]bool, len(ids))
	docs := make([]*T, 0, len(ids))

	for rows.Next() {
		var id string
		var data []byte
		var version int
		if err := rows.Scan(&id, &data, &version); err != nil {
			return nil, fmt.Errorf("collection %s: load many: scan: %w", c.name, err)
		}

		var doc T
		if err := c.codec.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("collection %s: load many %s: unmarshal: %w", c.name, id, err)
		}

		meta.SetID(&doc, id)
		meta.SetVersion(&doc, version)
		docs = append(docs, &doc)
		foundIDs[id] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collection %s: load many: %w", c.name, err)
	}

	if len(foundIDs) < len(ids) {
		errs := map[string]error{}
		for _, id := range ids {
			if !foundIDs[id] {
				errs[id] = whisker.ErrNotFound
			}
		}
		return docs, &BatchError{Op: "load", Total: len(ids), Errors: errs}
	}

	return docs, nil
}

func (c *CollectionOf[T]) checkBatchSize(n int) error {
	if c.maxBatchSize > 0 && n > c.maxBatchSize {
		return fmt.Errorf("collection %s: %w: %d exceeds max %d", c.name, whisker.ErrBatchTooLarge, n, c.maxBatchSize)
	}
	return nil
}

func isPgUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
