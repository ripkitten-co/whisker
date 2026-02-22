package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// Event represents a single event in a stream.
type Event struct {
	StreamID       string
	Version        int
	Type           string
	Data           []byte
	Metadata       []byte
	CreatedAt      time.Time
	GlobalPosition int64
}

// Store provides append-only event stream operations backed by a single
// whisker_events table.
type Store struct {
	exec   pg.Executor
	schema *schema.Bootstrap
}

// New creates an event store using the given backend's executor and schema.
func New(b whisker.Backend) *Store {
	return &Store{
		exec:   b.DBExecutor(),
		schema: b.SchemaBootstrap(),
	}
}

// Append writes events to a stream with optimistic concurrency control.
// Pass expectedVersion 0 to create a new stream. Returns ErrStreamExists
// if the stream already exists with version 0, or ErrConcurrencyConflict
// if the expected version doesn't match.
func (es *Store) Append(ctx context.Context, streamID string, expectedVersion int, evts []Event) error {
	if len(evts) == 0 {
		return fmt.Errorf("events: append %s: at least one event required", streamID)
	}

	if err := es.schema.EnsureEvents(ctx, es.exec); err != nil {
		return err
	}

	if expectedVersion > 0 {
		var currentVersion int
		err := es.exec.QueryRow(ctx,
			"SELECT COALESCE(MAX(version), 0) FROM whisker_events WHERE stream_id = $1",
			streamID,
		).Scan(&currentVersion)
		if err != nil {
			return fmt.Errorf("events: append %s: check version: %w", streamID, err)
		}
		if currentVersion != expectedVersion {
			return fmt.Errorf("events: append %s: expected version %d but got %d: %w",
				streamID, expectedVersion, currentVersion, whisker.ErrConcurrencyConflict)
		}
	}

	builder := psql.Insert("whisker_events").
		Columns("stream_id", "version", "type", "data", "metadata")

	for i, evt := range evts {
		version := expectedVersion + i + 1
		builder = builder.Values(streamID, version, evt.Type, evt.Data, evt.Metadata)
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return fmt.Errorf("events: append %s: build sql: %w", streamID, err)
	}

	_, err = es.exec.Exec(ctx, sql, args...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			if expectedVersion == 0 {
				return fmt.Errorf("events: append %s: %w", streamID, whisker.ErrStreamExists)
			}
			return fmt.Errorf("events: append %s: %w", streamID, whisker.ErrConcurrencyConflict)
		}
		return fmt.Errorf("events: append %s: %w", streamID, err)
	}

	// best-effort notification for projection pollers
	_, _ = es.exec.Exec(ctx, "SELECT pg_notify('whisker_events', '')")

	return nil
}

// ReadStream returns all events for a stream starting from fromVersion.
// Pass 0 to read from the beginning. Returns an empty slice if the stream
// doesn't exist.
func (es *Store) ReadStream(ctx context.Context, streamID string, fromVersion int) ([]Event, error) {
	if err := es.schema.EnsureEvents(ctx, es.exec); err != nil {
		return nil, err
	}

	builder := psql.
		Select("stream_id", "version", "type", "data", "metadata", "created_at", "global_position").
		From("whisker_events").
		Where(sq.Eq{"stream_id": streamID}).
		OrderBy("version ASC")

	if fromVersion > 0 {
		builder = builder.Where(sq.GtOrEq{"version": fromVersion})
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("events: read %s: build sql: %w", streamID, err)
	}

	rows, err := es.exec.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("events: read %s: %w", streamID, err)
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.StreamID, &e.Version, &e.Type, &e.Data, &e.Metadata, &e.CreatedAt, &e.GlobalPosition); err != nil {
			return nil, fmt.Errorf("events: read %s: scan: %w", streamID, err)
		}
		result = append(result, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("events: read %s: %w", streamID, err)
	}

	return result, nil
}

// ReadAll returns events across all streams ordered by global_position.
// Pass afterPosition 0 to start from the beginning. Returns up to limit events.
func (es *Store) ReadAll(ctx context.Context, afterPosition int64, limit int) ([]Event, error) {
	if err := es.schema.EnsureEvents(ctx, es.exec); err != nil {
		return nil, err
	}
	if err := es.schema.EnsureEventsGlobalPositionIndex(ctx, es.exec); err != nil {
		return nil, err
	}

	builder := psql.
		Select("stream_id", "version", "type", "data", "metadata", "created_at", "global_position").
		From("whisker_events").
		Where(sq.Gt{"global_position": afterPosition}).
		OrderBy("global_position ASC").
		Limit(uint64(limit))

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("events: read all: build sql: %w", err)
	}

	rows, err := es.exec.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("events: read all: %w", err)
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.StreamID, &e.Version, &e.Type, &e.Data, &e.Metadata, &e.CreatedAt, &e.GlobalPosition); err != nil {
			return nil, fmt.Errorf("events: read all: scan: %w", err)
		}
		result = append(result, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("events: read all: %w", err)
	}

	return result, nil
}
