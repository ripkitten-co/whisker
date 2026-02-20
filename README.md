# Whisker

[![Go Reference](https://pkg.go.dev/badge/github.com/ripkitten-co/whisker.svg)](https://pkg.go.dev/github.com/ripkitten-co/whisker)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

PostgreSQL-backed document store and event sourcing for Go. Think MongoDB semantics on top of PostgreSQL: JSONB collections, append-only event streams, transactional sessions, zero migration files.

## Features

- **Document Collections** - type-safe CRUD with Go generics, JSONB storage, optimistic concurrency
- **Event Streams** - append-only event sourcing with expected version checks
- **Sessions** - Unit of Work wrapping a single PostgreSQL transaction
- **Zero Migrations** - tables created automatically on first use
- **Convention Over Configuration** - plain Go structs, no tags needed
- **Swappable Codecs** - pluggable JSON serialization (jsoniter by default)

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
	ID      string
	Name    string
	Email   string
	Version int
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

Collections are typed with Go generics. Just use plain Go structs:

```go
type Order struct {
	ID      string
	Item    string
	Total   int
	Version int
}

orders := documents.Collection[Order](store, "orders")
```

Whisker detects fields by convention:

| Field | Role | Storage |
|-------|------|---------|
| `ID` | document identity | own column, excluded from JSONB |
| `Version` | optimistic concurrency | own column, excluded from JSONB |
| everything else | document data | camelCase JSONB keys (`FirstName` -> `"firstName"`) |

If a struct has a `Version` field (type `int`), updates check `WHERE version = $current` and increment automatically. Concurrent writes return `whisker.ErrConcurrencyConflict`. No `Version` field = no concurrency checking.

Override conventions with tags when you need to: `whisker:"id"` / `whisker:"version"` to pick a different field, `json` tags to control JSONB key names.

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

Append-only event streams with optimistic concurrency per stream.

```go
es := events.New(store)

// expectedVersion 0 means "new stream"
err := es.Append(ctx, "order-123", 0, []events.Event{
	{Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
	{Type: "OrderPaid", Data: []byte(`{"amount":100}`)},
})

stream, _ := es.ReadStream(ctx, "order-123", 0) // all events
stream, _ = es.ReadStream(ctx, "order-123", 2)  // from version 2
```

Appending to an existing stream with `expectedVersion: 0` returns `whisker.ErrStreamExists`. Wrong expected version returns `whisker.ErrConcurrencyConflict`.

## Sessions

Sessions wrap a PostgreSQL transaction. Everything inside commits or rolls back atomically.

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

Whisker manages its own tables, all prefixed with `whisker_`:

- `whisker_{collection}` - one table per document collection
- `whisker_events` - single table for all event streams

Tables are created lazily on first use. No migration files, no setup steps.

## Configuration

```go
store, err := whisker.New(ctx, connString,
	whisker.WithCodec(myCustomCodec), // swap JSON serialization
)
```

## Development

```bash
go test ./...                        # unit tests
go test -tags=integration ./...      # integration tests (requires Docker)
docker compose up -d                 # local PostgreSQL
```

## License

[MIT](LICENSE)
