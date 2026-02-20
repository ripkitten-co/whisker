package schema

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/ripkitten-co/whisker/internal/pg"
)

var validName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,54}$`)

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
	PRIMARY KEY (stream_id, version)
)`
}

type Bootstrap struct {
	tables sync.Map
}

func New() *Bootstrap {
	return &Bootstrap{}
}

func (b *Bootstrap) IsCreated(table string) bool {
	_, ok := b.tables.Load(table)
	return ok
}

func (b *Bootstrap) MarkCreated(table string) {
	b.tables.Store(table, true)
}

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
