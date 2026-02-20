# Whisker

[![Go Reference](https://pkg.go.dev/badge/github.com/ripkitten-co/whisker.svg)](https://pkg.go.dev/github.com/ripkitten-co/whisker)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A PostgreSQL-powered document store and event sourcing library for Go. Zero migrations, JSONB storage, append-only event streams, and transactional sessions — all backed by PostgreSQL.

## Features

- **Document Collections** — Type-safe CRUD with Go generics, JSONB storage, and optimistic concurrency
- **Event Streams** — Append-only event sourcing with expected version checks
- **Sessions** — Unit of Work pattern wrapping a PostgreSQL transaction for atomic operations across documents and events
- **Zero Migrations** — Tables are created automatically via `CREATE TABLE IF NOT EXISTS`
- **Swappable Codecs** — JSON serialization is pluggable (jsoniter by default)

## Install

```bash
go get github.com/ripkitten-co/whisker
```

Requires Go 1.23+ and PostgreSQL 15+.

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/events"
)

type User struct {
	ID      string `whisker:"id" json:"id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Version int    `whisker:"version" json:"-"`
}

func main() {
	ctx := context.Background()

	store, err := whisker.New(ctx, "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Documents
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@example.com"})

	user, _ := users.Load(ctx, "u1")
	fmt.Println(user.Name, user.Version) // Alice 1

	user.Name = "Bob"
	users.Update(ctx, user)
	fmt.Println(user.Version) // 2

	results, _ := users.Where("name", "=", "Bob").Execute(ctx)
	fmt.Println(len(results)) // 1

	// Events
	es := events.New(store)

	es.Append(ctx, "user-u1", 0, []events.Event{
		{Type: "UserCreated", Data: []byte(`{"name":"Alice"}`)},
		{Type: "UserRenamed", Data: []byte(`{"name":"Bob"}`)},
	})

	stream, _ := es.ReadStream(ctx, "user-u1", 0)
	fmt.Println(len(stream)) // 2
}
```

## Documents

Collections are typed with Go generics. Mark your ID and version fields with struct tags:

```go
type Order struct {
	ID      string `whisker:"id" json:"id"`
	Item    string `json:"item"`
	Total   int    `json:"total"`
	Version int    `whisker:"version" json:"-"`
}

orders := documents.Collection[Order](store, "orders")
```

The `whisker:"version"` tag enables optimistic concurrency — updates check `WHERE version = $current` and increment automatically. If another writer changed the document, you get `whisker.ErrConcurrencyConflict`. Omit the version tag to skip concurrency checking.

### CRUD

```go
orders.Insert(ctx, &Order{ID: "o1", Item: "widget", Total: 100})
order, err := orders.Load(ctx, "o1")
order.Total = 200
orders.Update(ctx, order)
orders.Delete(ctx, "o1")
```

### Queries

```go
results, err := orders.Where("item", "=", "widget").Execute(ctx)
results, err  = orders.Where("total", ">", 50).Where("item", "!=", "gizmo").Execute(ctx)
```

Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.

## Events

Append-only event streams with optimistic concurrency on the stream level.

```go
es := events.New(store)

// expectedVersion 0 means "new stream"
err := es.Append(ctx, "order-123", 0, []events.Event{
	{Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
	{Type: "OrderPaid", Data: []byte(`{"amount":100}`)},
})

// read all events
stream, _ := es.ReadStream(ctx, "order-123", 0)

// read from a specific version
stream, _ = es.ReadStream(ctx, "order-123", 2)
```

Appending to an existing stream with `expectedVersion: 0` returns `whisker.ErrStreamExists`. Appending with a wrong expected version returns `whisker.ErrConcurrencyConflict`.

## Sessions

Sessions wrap a PostgreSQL transaction. Documents and events within a session are committed or rolled back atomically.

```go
sess, err := store.Session(ctx)

orders := documents.Collection[Order](sess, "orders")
orders.Insert(ctx, &Order{ID: "o1", Item: "widget"})

events.New(sess).Append(ctx, "order-o1", 0, []events.Event{
	{Type: "OrderCreated", Data: []byte(`{}`)},
})

err = sess.Commit(ctx)   // all-or-nothing
// or
err = sess.Rollback(ctx) // discard everything
```

## Schema

Whisker manages its own tables. All tables are prefixed with `whisker_`:

- `whisker_{collection}` — one table per document collection
- `whisker_events` — single table for all event streams

Tables are created lazily on first use. No migration files, no setup steps.

## Configuration

```go
store, err := whisker.New(ctx, connString,
	whisker.WithCodec(myCustomCodec), // swap JSON serialization
)
```

## Development

```bash
# run unit tests
go test ./...

# run integration tests (requires Docker)
go test -tags=integration ./...

# start a local PostgreSQL
docker compose up -d
```

## License

[MIT](LICENSE)
