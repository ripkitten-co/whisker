package schema

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/ripkitten-co/whisker/internal/pg"
)

var validName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

func ValidateCollectionName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("schema: invalid collection name %q: must be alphanumeric with underscores", name)
	}
	return nil
}

func CollectionDDL(name string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS whisker_%s (
	id TEXT PRIMARY KEY,
	data JSONB NOT NULL,
	version INTEGER NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, name)
}

func EventsDDL() string {
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
	mu      sync.Mutex
	created map[string]bool
}

func New() *Bootstrap {
	return &Bootstrap{created: make(map[string]bool)}
}

func (b *Bootstrap) IsCreated(table string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.created[table]
}

func (b *Bootstrap) MarkCreated(table string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.created[table] = true
}

func (b *Bootstrap) EnsureCollection(ctx context.Context, exec pg.Executor, name string) error {
	if err := ValidateCollectionName(name); err != nil {
		return err
	}
	table := "whisker_" + name
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.created[table] {
		return nil
	}
	_, err := exec.Exec(ctx, CollectionDDL(name))
	if err != nil {
		return fmt.Errorf("schema: create table %s: %w", table, err)
	}
	b.created[table] = true
	return nil
}

func (b *Bootstrap) EnsureEvents(ctx context.Context, exec pg.Executor) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.created["whisker_events"] {
		return nil
	}
	_, err := exec.Exec(ctx, EventsDDL())
	if err != nil {
		return fmt.Errorf("schema: create events table: %w", err)
	}
	b.created["whisker_events"] = true
	return nil
}
