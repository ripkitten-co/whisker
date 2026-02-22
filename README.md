# Whisker

[![Go Reference](https://pkg.go.dev/badge/github.com/ripkitten-co/whisker.svg)](https://pkg.go.dev/github.com/ripkitten-co/whisker)
[![Go Report Card](https://goreportcard.com/badge/github.com/ripkitten-co/whisker)](https://goreportcard.com/report/github.com/ripkitten-co/whisker)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go 1.25+](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![PostgreSQL 16+](https://img.shields.io/badge/PostgreSQL-16+-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

**Postgres document & event store for Go. Zero migrations.**

## Why?

Go has great Postgres drivers, but if you want document-style storage you're stuck writing `CREATE TABLE`, managing migrations, and hand-rolling JSONB queries. Or you reach for MongoDB and lose transactions.

Whisker gives you [Marten](https://martendb.io/)-style DX in Go: throw structs into collections, append events to streams, wrap it in a transaction. Tables create themselves. Postgres does the rest.

## Quickstart

```bash
go get github.com/ripkitten-co/whisker
```

```go
store, _ := whisker.New(ctx, "postgres://localhost:5432/mydb")
defer store.Close()

type User struct {
    ID      string
    Name    string
    Email   string
    Version int
}

users := documents.Collection[User](store, "users")
users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@example.com"})

user, _ := users.Load(ctx, "u1")
// user.Name == "Alice", user.Version == 1
```

That's it. No `CREATE TABLE`, no schema files, no code generation. The `users` table shows up in Postgres the first time you insert.

## Features

### Document Collections

Generic collections backed by JSONB. Whisker looks at your struct and figures out what goes where:

| Field | What it does | Where it lives |
|-------|-------------|----------------|
| `ID` | document key | its own column (not in the JSONB) |
| `Version` | optimistic locking | its own column (not in the JSONB) |
| anything else | your data | JSONB, keys are camelCased (`FirstName` -> `"firstName"`) |

No struct tags needed. If you have a field called `ID`, that's your key. Got a `Version int` field? You get optimistic concurrency for free: updates check `WHERE version = $current` and bump it. Concurrent writes return `whisker.ErrConcurrencyConflict`.

Override when you need to: `whisker:"id"` / `whisker:"version"` to pick different fields, `json` tags for custom JSONB keys.

```go
// CRUD
orders.Insert(ctx, &Order{ID: "o1", Item: "widget", Total: 100})
order, _ := orders.Load(ctx, "o1")
order.Total = 200
orders.Update(ctx, order)
orders.Delete(ctx, "o1")

// Queries
results, _ := orders.Where("item", "=", "widget").Execute(ctx)
results, _  = orders.Where("total", ">", 50).Where("item", "!=", "gizmo").Execute(ctx)

// Sorting and pagination
results, _ = orders.Query().
    OrderBy("total", documents.Desc).
    Limit(20).
    Offset(40).
    Execute(ctx)

// Cursor-based pagination
nextPage, _ := orders.Query().
    OrderBy("created_at", documents.Asc).
    Limit(20).
    After("2024-01-15T10:00:00Z").
    Execute(ctx)

// Aggregates
count, _ := orders.Count(ctx)
count, _  = orders.Where("item", "=", "widget").Count(ctx)

exists, _ := orders.Exists(ctx, "o1")
exists, _  = orders.Where("item", "=", "widget").Exists(ctx)
```

### Event Streams

Append-only event sourcing. Each stream has its own version counter.

```go
es := events.New(store)

es.Append(ctx, "order-123", 0, []events.Event{
    {Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
    {Type: "OrderPaid", Data: []byte(`{"amount":100}`)},
})

stream, _ := es.ReadStream(ctx, "order-123", 0) // from the start
stream, _ = es.ReadStream(ctx, "order-123", 2)  // from version 2
```

`expectedVersion: 0` means "new stream." Wrong version? `whisker.ErrConcurrencyConflict`.

### Projections

Async read-model projections and side-effect handlers. Each projection runs in its own goroutine with independent checkpoints and PostgreSQL advisory locks for single-writer coordination.

```go
// Read-model projection — transforms events into a queryable collection
proj := projections.New[OrderSummary](store, "order_summaries").
    On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
        var p OrderCreatedPayload
        json.Unmarshal(evt.Data, &p)
        return &OrderSummary{ID: evt.StreamID, Total: p.Total, Status: "pending"}, nil
    }).
    On("OrderPaid", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
        state.Status = "paid"
        return state, nil
    })

// Side-effect handler — react to events without maintaining state
notifier := projections.NewHandler("email_notifier").
    On("OrderPaid", func(ctx context.Context, evt events.Event) error {
        return sendReceipt(ctx, evt)
    })

// Daemon runs everything
daemon := projections.NewDaemon(store,
    projections.WithPollingInterval(5 * time.Second),
    projections.WithBatchSize(100),
)
daemon.Add(proj)
daemon.Add(notifier)
daemon.Run(ctx) // blocks until ctx cancelled

// Full rebuild from event history
daemon.Rebuild(ctx, "order_summaries")
```

Returning `nil` from a projection handler deletes the read model for that stream. Dead-letter handling stops a projection after consecutive failures.

### Sessions (Transactions)

Documents + events in one atomic Postgres transaction:

```go
sess, _ := store.Session(ctx)

documents.Collection[Order](sess, "orders").Insert(ctx, &Order{ID: "o1", Item: "widget"})
events.New(sess).Append(ctx, "order-o1", 0, []events.Event{{Type: "OrderCreated", Data: []byte(`{}`)}})

sess.Commit(ctx) // all or nothing
```

### ORM Hooks (GORM, Ent, Bun)

Already using an ORM? Whisker can sit underneath it. The hooks middleware intercepts SQL at the pgx driver level and rewrites it to target JSONB document storage. Your ORM thinks it's talking to normal tables.

```go
pool := hooks.NewPool(store)
hooks.Register[User](pool, "users")

// GORM
db, _ := hooks.OpenGORM(pool)
db.AutoMigrate(&User{})           // creates whisker_users table
db.Create(&User{ID: "u1", Name: "Alice"})
db.First(&user, "id = ?", "u1")   // reads from JSONB

// Ent
driver := hooks.EntDriver(pool)

// Bun
adapter := hooks.BunAdapter(pool)
```

INSERT, SELECT, UPDATE, DELETE, CREATE TABLE, and JOIN queries are all rewritten transparently. The ORM sees normal columns; Whisker stores JSONB.

### Swappable Codecs

jsoniter ships as the default. Swap it:

```go
store, _ := whisker.New(ctx, connString,
    whisker.WithCodec(myCodec),
)
```

## Roadmap

Whisker is in early development. Here's what's coming:

- [x] `Count()`, `Exists()`, `OrderBy`, `Limit`/`Offset`, cursor pagination
- [x] JSONB indexes (btree + GIN) via struct tags
- [x] ORM hooks (GORM, Ent, Bun adapters)
- [x] Projections (rebuild read models from event streams)
- [x] Subscriptions (react to new events in real-time)
- [x] Batch operations
- [ ] Soft deletes

Want something else? [Open an issue.](https://github.com/ripkitten-co/whisker/issues)

## Development

```bash
go test ./...                        # unit tests
go test -tags=integration ./...      # needs Docker
docker compose up -d                 # local pg
```

## License

[MIT](LICENSE)
