package projections

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

type pgProcessingStore struct {
	exec   pg.Executor
	schema *schema.Bootstrap
	name   string
}

func NewProcessingStoreFromBackend(b whisker.Backend, name string) ProcessingStore {
	return &pgProcessingStore{
		exec:   b.DBExecutor(),
		schema: b.SchemaBootstrap(),
		name:   name,
	}
}

func (ps *pgProcessingStore) table() string {
	return "whisker_" + ps.name
}

func (ps *pgProcessingStore) ensure(ctx context.Context) error {
	return ps.schema.EnsureCollection(ctx, ps.exec, ps.name)
}

// LoadState reads the projected document and its version from the collection.
// Returns (nil, 0, nil) when the document does not exist.
func (ps *pgProcessingStore) LoadState(ctx context.Context, _ string, id string) ([]byte, int, error) {
	if err := ps.ensure(ctx); err != nil {
		return nil, 0, fmt.Errorf("processing store %s: ensure table: %w", ps.name, err)
	}

	var data []byte
	var version int
	err := ps.exec.QueryRow(ctx,
		fmt.Sprintf(`SELECT data, version FROM %s WHERE id = $1`, ps.table()),
		id,
	).Scan(&data, &version)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("processing store %s: load %s: %w", ps.name, id, err)
	}
	return data, version, nil
}

// UpsertState inserts or updates a projected document. The stored version is
// incremented by one on each upsert.
func (ps *pgProcessingStore) UpsertState(ctx context.Context, _ string, id string, data []byte, version int) error {
	if err := ps.ensure(ctx); err != nil {
		return fmt.Errorf("processing store %s: ensure table: %w", ps.name, err)
	}

	_, err := ps.exec.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (id, data, version, created_at, updated_at)
		 VALUES ($1, $2, $3, now(), now())
		 ON CONFLICT (id) DO UPDATE SET data = $2, version = $3, updated_at = now()`, ps.table()),
		id, data, version+1,
	)
	if err != nil {
		return fmt.Errorf("processing store %s: upsert %s: %w", ps.name, id, err)
	}
	return nil
}

// DeleteState removes a projected document from the collection.
func (ps *pgProcessingStore) DeleteState(ctx context.Context, _ string, id string) error {
	if err := ps.ensure(ctx); err != nil {
		return fmt.Errorf("processing store %s: ensure table: %w", ps.name, err)
	}

	_, err := ps.exec.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, ps.table()),
		id,
	)
	if err != nil {
		return fmt.Errorf("processing store %s: delete %s: %w", ps.name, id, err)
	}
	return nil
}
