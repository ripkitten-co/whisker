package projections

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ripkitten-co/whisker"
)

// DaemonOption configures the projection daemon.
type DaemonOption func(*daemonConfig)

type daemonConfig struct {
	pollingInterval time.Duration
	batchSize       int
}

// WithPollingInterval sets how often each worker polls for new events.
// Defaults to 5 seconds.
func WithPollingInterval(d time.Duration) DaemonOption {
	return func(c *daemonConfig) { c.pollingInterval = d }
}

// WithBatchSize sets the maximum number of events fetched per poll cycle.
// Defaults to 100.
func WithBatchSize(n int) DaemonOption {
	return func(c *daemonConfig) { c.batchSize = n }
}

// Daemon runs registered subscribers in independent goroutines, each with its
// own checkpoint and advisory lock. It is the main entry point for running
// projections and side-effect handlers.
type Daemon struct {
	store       *whisker.Store
	config      daemonConfig
	subscribers []Subscriber
}

// NewDaemon creates a daemon bound to the given store.
func NewDaemon(store *whisker.Store, opts ...DaemonOption) *Daemon {
	cfg := daemonConfig{
		pollingInterval: 5 * time.Second,
		batchSize:       100,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &Daemon{store: store, config: cfg}
}

// Add registers a subscriber (projection or handler) to be run by the daemon.
func (d *Daemon) Add(sub Subscriber) {
	d.subscribers = append(d.subscribers, sub)
}

// Run starts all subscribers in separate goroutines and blocks until the
// context is cancelled.
func (d *Daemon) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, sub := range d.subscribers {
		w := NewWorker(d.store, sub)
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.runWorker(ctx, w)
		}()
	}

	wg.Wait()
}

func (d *Daemon) runWorker(ctx context.Context, w *Worker) {
	drainBatches(ctx, w)

	ticker := time.NewTicker(d.config.pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drainBatches(ctx, w)
		}
	}
}

func drainBatches(ctx context.Context, w *Worker) {
	acquired, err := w.TryAcquireLock(ctx)
	if err != nil {
		slog.Error("acquire lock", "worker", w.subscriber.Name(), "error", err)
		return
	}
	if !acquired {
		return
	}
	defer func() {
		if err := w.ReleaseLock(ctx); err != nil {
			slog.Error("release lock", "worker", w.subscriber.Name(), "error", err)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		n, err := w.ProcessBatch(ctx)
		if err != nil {
			slog.Error("process batch", "worker", w.subscriber.Name(), "error", err)
			return
		}
		if n == 0 {
			return
		}
	}
}

// Rebuild drops the read model table for the named projection, resets its
// checkpoint to zero, and replays all events from the beginning.
func (d *Daemon) Rebuild(ctx context.Context, name string) error {
	var sub Subscriber
	for _, s := range d.subscribers {
		if s.Name() == name {
			sub = s
			break
		}
	}
	if sub == nil {
		return fmt.Errorf("daemon: subscriber %q not found", name)
	}

	exec := d.store.DBExecutor()

	_, err := exec.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS whisker_%s", name))
	if err != nil {
		return fmt.Errorf("daemon: drop table whisker_%s: %w", name, err)
	}

	cs := NewCheckpointStore(d.store)
	if err := cs.Reset(ctx, name); err != nil {
		return fmt.Errorf("daemon: reset checkpoint %s: %w", name, err)
	}

	// bootstrap cache still thinks the table exists, so recreate manually
	_, err = exec.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS whisker_%s (
		id TEXT PRIMARY KEY,
		data JSONB NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`, name))
	if err != nil {
		return fmt.Errorf("daemon: recreate table whisker_%s: %w", name, err)
	}

	w := NewWorker(d.store, sub)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := w.ProcessBatch(ctx)
		if err != nil {
			return fmt.Errorf("daemon: rebuild %s: %w", name, err)
		}
		if n == 0 {
			break
		}
	}

	if err := cs.SetStatus(ctx, name, "running"); err != nil {
		return fmt.Errorf("daemon: rebuild %s set status: %w", name, err)
	}

	return nil
}
