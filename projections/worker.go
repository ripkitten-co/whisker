package projections

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
)

// Worker drives a single subscriber: poll events, filter, process, checkpoint.
// Each worker runs in its own goroutine, coordinated by the Daemon.
type Worker struct {
	store               *whisker.Store
	pool                *pgxpool.Pool
	subscriber          Subscriber
	checkpoint          *CheckpointStore
	poller              *Poller
	batchSize           int
	maxRetries          int
	consecutiveFailures int
	lockConn            *pgxpool.Conn
}

// NewWorker creates a worker for the given subscriber with sensible defaults
// (batch size 100, max retries 5).
func NewWorker(store *whisker.Store, sub Subscriber) *Worker {
	return &Worker{
		store:      store,
		pool:       store.PgxPool(),
		subscriber: sub,
		checkpoint: NewCheckpointStore(store),
		poller:     NewPoller(store, 100),
		batchSize:  100,
		maxRetries: 5,
	}
}

// SetMaxRetries configures the number of consecutive failures before the
// worker transitions the projection to dead_letter status.
func (w *Worker) SetMaxRetries(n int) {
	w.maxRetries = n
}

// ProcessBatch polls for events after the last checkpoint position and processes
// them through the subscriber. Returns the number of events polled (before
// filtering) so callers can decide whether to keep draining.
func (w *Worker) ProcessBatch(ctx context.Context) (int, error) {
	name := w.subscriber.Name()

	pos, status, err := w.checkpoint.Load(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("worker %s: load checkpoint: %w", name, err)
	}

	if status == "dead_letter" || status == "stopped" {
		return 0, nil
	}

	evts, err := w.poller.Poll(ctx, pos)
	if err != nil {
		return 0, fmt.Errorf("worker %s: poll: %w", name, err)
	}
	if len(evts) == 0 {
		return 0, nil
	}

	filtered := w.filterEvents(evts)

	if len(filtered) == 0 {
		return len(evts), w.checkpoint.Save(ctx, name, evts[len(evts)-1].GlobalPosition)
	}

	ps := NewProcessingStoreFromBackend(w.store, name)
	if err := w.subscriber.Process(ctx, filtered, ps); err != nil {
		w.consecutiveFailures++
		if w.consecutiveFailures >= w.maxRetries {
			_ = w.checkpoint.SetStatus(ctx, name, "dead_letter")
		}
		return 0, fmt.Errorf("worker %s: process: %w", name, err)
	}

	w.consecutiveFailures = 0
	return len(evts), w.checkpoint.Save(ctx, name, evts[len(evts)-1].GlobalPosition)
}

// TryAcquireLock acquires a dedicated connection from the pool and attempts a
// PostgreSQL session-level advisory lock keyed by the subscriber name. The
// connection is held until ReleaseLock is called, ensuring the lock protects
// the entire processing cycle. Returns false if another instance holds the lock.
func (w *Worker) TryAcquireLock(ctx context.Context) (bool, error) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("worker %s: acquire conn: %w", w.subscriber.Name(), err)
	}

	lockID := lockHash(w.subscriber.Name())
	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	if err != nil {
		conn.Release()
		return false, fmt.Errorf("worker %s: acquire lock: %w", w.subscriber.Name(), err)
	}
	if !acquired {
		conn.Release()
		return false, nil
	}

	w.lockConn = conn
	return true, nil
}

// ReleaseLock releases the advisory lock and returns the dedicated connection
// to the pool.
func (w *Worker) ReleaseLock(ctx context.Context) error {
	if w.lockConn == nil {
		return nil
	}
	defer func() {
		w.lockConn.Release()
		w.lockConn = nil
	}()

	lockID := lockHash(w.subscriber.Name())
	var released bool
	err := w.lockConn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
	if err != nil {
		return fmt.Errorf("worker %s: release lock: %w", w.subscriber.Name(), err)
	}
	return nil
}

func (w *Worker) filterEvents(evts []events.Event) []events.Event {
	types := make(map[string]struct{}, len(w.subscriber.EventTypes()))
	for _, t := range w.subscriber.EventTypes() {
		types[t] = struct{}{}
	}

	var filtered []events.Event
	for _, evt := range evts {
		if _, ok := types[evt.Type]; ok {
			filtered = append(filtered, evt)
		}
	}
	return filtered
}

func lockHash(name string) int64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return int64(h.Sum64())
}
