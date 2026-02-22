package whisker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

// Session wraps a PostgreSQL transaction spanning document operations and event
// appends. Call Commit to persist all changes atomically, or Close/Rollback to
// discard them.
type Session struct {
	tx     pgx.Tx
	be     backend
	closed bool
}

// Session begins a new transaction and returns a Session.
func (s *Store) Session(ctx context.Context) (*Session, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("whisker: begin session: %w", err)
	}

	return &Session{
		tx: tx,
		be: backend{
			exec:         txExecutor{tx},
			codec:        s.be.codec,
			schema:       schema.New(),
			maxBatchSize: s.be.maxBatchSize,
		},
	}, nil
}

func (s *Session) DBExecutor() pg.Executor            { return s.be.exec }
func (s *Session) JSONCodec() codecs.Codec            { return s.be.codec }
func (s *Session) SchemaBootstrap() *schema.Bootstrap { return s.be.schema }
func (s *Session) MaxBatchSize() int                  { return s.be.maxBatchSize }

// Commit persists all operations in this session atomically.
func (s *Session) Commit(ctx context.Context) error {
	if s.closed {
		return fmt.Errorf("whisker: session already closed")
	}
	s.closed = true
	if err := s.tx.Commit(ctx); err != nil {
		return fmt.Errorf("whisker: commit session: %w", err)
	}
	return nil
}

// Rollback discards all operations. Safe to call multiple times.
func (s *Session) Rollback(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.tx.Rollback(ctx); err != nil {
		return fmt.Errorf("whisker: rollback session: %w", err)
	}
	return nil
}

// Close rolls back if not already committed. Safe to defer.
func (s *Session) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	return s.Rollback(ctx)
}

type txExecutor struct {
	tx pgx.Tx
}

func (t txExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, args...)
}

func (t txExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (t txExecutor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t txExecutor) InTransaction() bool { return true }
