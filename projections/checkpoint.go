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

// CheckpointStore tracks the last processed global_position for each
// projection, enabling resume-from-where-you-left-off semantics.
type CheckpointStore struct {
	exec   pg.Executor
	schema *schema.Bootstrap
}

// NewCheckpointStore creates a checkpoint store backed by the given whisker backend.
func NewCheckpointStore(b whisker.Backend) *CheckpointStore {
	return &CheckpointStore{
		exec:   b.DBExecutor(),
		schema: b.SchemaBootstrap(),
	}
}

func (cs *CheckpointStore) ensure(ctx context.Context) error {
	return cs.schema.EnsureProjectionCheckpoints(ctx, cs.exec)
}

// Load returns the last processed position and status for the named projection.
// If no checkpoint exists, it returns (0, "running", nil).
func (cs *CheckpointStore) Load(ctx context.Context, name string) (int64, string, error) {
	if err := cs.ensure(ctx); err != nil {
		return 0, "", fmt.Errorf("checkpoint %s: ensure table: %w", name, err)
	}

	var position int64
	var status string
	err := cs.exec.QueryRow(ctx,
		`SELECT last_position, status FROM whisker_projection_checkpoints WHERE projection_name = $1`,
		name,
	).Scan(&position, &status)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "running", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("checkpoint %s: load: %w", name, err)
	}
	return position, status, nil
}

// Save upserts the checkpoint position for the named projection.
func (cs *CheckpointStore) Save(ctx context.Context, name string, position int64) error {
	if err := cs.ensure(ctx); err != nil {
		return fmt.Errorf("checkpoint %s: ensure table: %w", name, err)
	}

	_, err := cs.exec.Exec(ctx,
		`INSERT INTO whisker_projection_checkpoints (projection_name, last_position, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (projection_name) DO UPDATE SET last_position = $2, updated_at = now()`,
		name, position,
	)
	if err != nil {
		return fmt.Errorf("checkpoint %s: save: %w", name, err)
	}
	return nil
}

// SetStatus updates the status column for the named projection.
func (cs *CheckpointStore) SetStatus(ctx context.Context, name string, status string) error {
	if err := cs.ensure(ctx); err != nil {
		return fmt.Errorf("checkpoint %s: ensure table: %w", name, err)
	}

	_, err := cs.exec.Exec(ctx,
		`INSERT INTO whisker_projection_checkpoints (projection_name, last_position, status, updated_at)
		 VALUES ($1, 0, $2, now())
		 ON CONFLICT (projection_name) DO UPDATE SET status = $2, updated_at = now()`,
		name, status,
	)
	if err != nil {
		return fmt.Errorf("checkpoint %s: set status: %w", name, err)
	}
	return nil
}

// Reset sets the projection position back to 0 with status 'rebuilding'.
func (cs *CheckpointStore) Reset(ctx context.Context, name string) error {
	if err := cs.ensure(ctx); err != nil {
		return fmt.Errorf("checkpoint %s: ensure table: %w", name, err)
	}

	_, err := cs.exec.Exec(ctx,
		`INSERT INTO whisker_projection_checkpoints (projection_name, last_position, status, updated_at)
		 VALUES ($1, 0, 'rebuilding', now())
		 ON CONFLICT (projection_name) DO UPDATE SET last_position = 0, status = 'rebuilding', updated_at = now()`,
		name,
	)
	if err != nil {
		return fmt.Errorf("checkpoint %s: reset: %w", name, err)
	}
	return nil
}
