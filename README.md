# Whisker

[![Go Reference](https://pkg.go.dev/badge/github.com/ripkitten-co/whisker.svg)](https://pkg.go.dev/github.com/ripkitten-co/whisker)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

MongoDB-style document store on top of PostgreSQL. JSONB collections, event streams, transactions. No migration files.

## Why?

We wanted [Marten](https://martendb.io/)'s developer experience in Go. Store documents as JSONB, append events, wrap it all in a transaction. Postgres does the heavy lifting, Whisker just makes it nice to use.

## Install

```bash
go get github.com/ripkitten-co/whisker
```

Go 1.23+, PostgreSQL 15+.

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

	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@example.com"})

	user, _ := users.Load(ctx, "u1")
	fmt.Println(user.Name, user.Version) // Alice 1

	user.Name = "Bob"
	users.Update(ctx, user)
	fmt.Println(user.Version) // 2

	results, _ := users.Where("name", "=", "Bob").Execute(ctx)
	fmt.Println(len(results)) // 1

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

Plain Go structs, no tags needed. Whisker figures it out:

| Field | What it does | Where it lives |
|-------|-------------|----------------|
| `ID` | document key | its own column (not in the JSONB) |
| `Version` | optimistic locking | its own column (not in the JSONB) |
| anything else | your data | JSONB, keys are camelCased (`FirstName` -> `"firstName"`) |

```go
type Order struct {
	ID      string
	Item    string
	Total   int
	Version int
}

orders := documents.Collection[Order](store, "orders")
```

The `Version` field opts you into optimistic concurrency. Updates do `WHERE version = $current` and bump it. If someone else wrote first, you get `whisker.ErrConcurrencyConflict`. Leave out `Version` if you don't care about that.

You can override the defaults with `whisker:"id"` / `whisker:"version"` tags, or use `json` tags for custom JSONB key names.

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

Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.

## Events

Append-only streams. Each stream has its own version counter for concurrency.

```go
es := events.New(store)

err := es.Append(ctx, "order-123", 0, []events.Event{
	{Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
	{Type: "OrderPaid", Data: []byte(`{"amount":100}`)},
})

stream, _ := es.ReadStream(ctx, "order-123", 0) // from the beginning
stream, _ = es.ReadStream(ctx, "order-123", 2)  // from version 2
```

`expectedVersion: 0` means "this stream shouldn't exist yet" - if it does, `whisker.ErrStreamExists`. Wrong version on an existing stream gives `whisker.ErrConcurrencyConflict`.

## Sessions

Everything in a session runs in one Postgres transaction.

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

All tables are prefixed `whisker_` and created on first use (`CREATE TABLE IF NOT EXISTS`). No migration files, nothing to set up.

- `whisker_{name}` per document collection
- `whisker_events` for all event streams

## Configuration

```go
store, err := whisker.New(ctx, connString,
	whisker.WithCodec(myCustomCodec),
)
```

## Development

```bash
go test ./...                        # unit tests
go test -tags=integration ./...      # needs Docker
docker compose up -d                 # local pg
```

## License

[MIT](LICENSE)
