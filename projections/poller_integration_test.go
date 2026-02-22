//go:build integration

package projections_test

import (
	"context"
	"testing"
	"time"

	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/projections"
)

func TestPoller_PollReturnsEvents(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "poll-stream-1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"1"}`)},
		{Type: "OrderPaid", Data: []byte(`{"amount":50}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	poller := projections.NewPoller(store, 100)
	got, err := poller.Poll(ctx, 0)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].Type != "OrderCreated" {
		t.Errorf("event[0].Type: got %q, want %q", got[0].Type, "OrderCreated")
	}
	if got[1].Type != "OrderPaid" {
		t.Errorf("event[1].Type: got %q, want %q", got[1].Type, "OrderPaid")
	}
}

func TestPoller_PollFromPosition(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "poll-stream-2", 0, []events.Event{
		{Type: "First", Data: []byte(`{}`)},
		{Type: "Second", Data: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	all, err := es.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) < 2 {
		t.Fatalf("got %d events, want at least 2", len(all))
	}

	poller := projections.NewPoller(store, 100)
	got, err := poller.Poll(ctx, all[0].GlobalPosition)
	if err != nil {
		t.Fatalf("poll from position: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].Type != "Second" {
		t.Errorf("event[0].Type: got %q, want %q", got[0].Type, "Second")
	}
}

func TestPoller_WaitForNotification(t *testing.T) {
	store := setupStore(t)
	es := events.New(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	poller := projections.NewPoller(store, 100)

	go func() {
		errCh <- poller.WaitForNotification(ctx)
	}()

	// give the listener time to set up
	time.Sleep(200 * time.Millisecond)

	err := es.Append(ctx, "notify-stream", 0, []events.Event{
		{Type: "Triggered", Data: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("wait for notification: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for notification")
	}
}
