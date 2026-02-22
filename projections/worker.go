package projections

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/internal/pg"
)

type Worker struct {
	store               *whisker.Store
	subscriber          Subscriber
	checkpoint          *CheckpointStore
	poller              *Poller
	exec                pg.Executor
	batchSize           int
	maxRetries          int
	consecutiveFailures int
}

func NewWorker(store *whisker.Store, sub Subscriber) *Worker {
	return &Worker{
		store:      store,
		subscriber: sub,
		checkpoint: NewCheckpointStore(store),
		poller:     NewPoller(store, 100),
		exec:       store.DBExecutor(),
		batchSize:  100,
		maxRetries: 5,
	}
}

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

func (w *Worker) TryAcquireLock(ctx context.Context) (bool, error) {
	lockID := lockHash(w.subscriber.Name())
	var acquired bool
	err := w.exec.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("worker %s: acquire lock: %w", w.subscriber.Name(), err)
	}
	return acquired, nil
}

func (w *Worker) ReleaseLock(ctx context.Context) error {
	lockID := lockHash(w.subscriber.Name())
	var released bool
	err := w.exec.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
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
