package schema

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/ripkitten-co/whisker/internal/pg"
)

var validName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,54}$`)

// ValidateCollectionName checks that name is a valid collection identifier
// (alphanumeric + underscores, max 55 characters, starts with a letter).
func ValidateCollectionName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("schema: invalid collection name %q: must be alphanumeric with underscores, max 55 chars", name)
	}
	return nil
}

func collectionDDL(name string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS whisker_%s (
	id TEXT PRIMARY KEY,
	data JSONB NOT NULL,
	version INTEGER NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, name)
}

func eventsDDL() string {
	return `CREATE TABLE IF NOT EXISTS whisker_events (
	stream_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	type TEXT NOT NULL,
	data JSONB NOT NULL,
	metadata JSONB,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	global_position BIGINT GENERATED ALWAYS AS IDENTITY,
	PRIMARY KEY (stream_id, version)
)`
}

// Bootstrap manages idempotent creation of Whisker tables and indexes.
// It caches which tables and indexes have been created to avoid repeated DDL.
type Bootstrap struct {
	tables  sync.Map
	indexes sync.Map
}

// New returns a Bootstrap with empty caches.
func New() *Bootstrap {
	return &Bootstrap{}
}

// IsCreated reports whether the named table has been created in this session.
func (b *Bootstrap) IsCreated(table string) bool {
	_, ok := b.tables.Load(table)
	return ok
}

// MarkCreated records that the named table has been created.
func (b *Bootstrap) MarkCreated(table string) {
	b.tables.Store(table, true)
}

// IsIndexCreated reports whether the named index has been created in this session.
func (b *Bootstrap) IsIndexCreated(name string) bool {
	_, ok := b.indexes.Load(name)
	return ok
}

// MarkIndexCreated records that the named index has been created.
func (b *Bootstrap) MarkIndexCreated(name string) {
	b.indexes.Store(name, true)
}

// EnsureCollection creates the whisker_{name} table if it doesn't exist.
func (b *Bootstrap) EnsureCollection(ctx context.Context, exec pg.Executor, name string) error {
	if err := ValidateCollectionName(name); err != nil {
		return err
	}
	table := "whisker_" + name
	if _, ok := b.tables.Load(table); ok {
		return nil
	}
	_, err := exec.Exec(ctx, collectionDDL(name))
	if err != nil {
		return fmt.Errorf("schema: create table %s: %w", table, err)
	}
	b.tables.Store(table, true)
	return nil
}

// EnsureEvents creates the whisker_events table if it doesn't exist.
func (b *Bootstrap) EnsureEvents(ctx context.Context, exec pg.Executor) error {
	if _, ok := b.tables.Load("whisker_events"); ok {
		return nil
	}
	_, err := exec.Exec(ctx, eventsDDL())
	if err != nil {
		return fmt.Errorf("schema: create events table: %w", err)
	}
	b.tables.Store("whisker_events", true)
	return nil
}

// EnsureEventsGlobalPositionIndex creates an index on global_position for
// ordered reads across all streams.
func (b *Bootstrap) EnsureEventsGlobalPositionIndex(ctx context.Context, exec pg.Executor) error {
	const name = "idx_whisker_events_global_position"
	if _, ok := b.indexes.Load(name); ok {
		return nil
	}
	_, err := exec.Exec(ctx,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_events_global_position ON whisker_events (global_position)`,
	)
	if err != nil {
		return fmt.Errorf("schema: create events global_position index: %w", err)
	}
	b.indexes.Store(name, true)
	return nil
}
