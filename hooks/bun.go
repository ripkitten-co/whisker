package hooks

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/stdlib"
)

// bunAdapter wraps a *sql.DB with SQL interception for registered Whisker models.
// It satisfies the minimal interface Bun needs: ExecContext and QueryContext.
type bunAdapter struct {
	db   *sql.DB
	reg  *registry
	pool *Pool
}

// BunAdapter returns an adapter that intercepts SQL for registered Whisker models.
// The returned value provides ExecContext and QueryContext compatible with
// database/sql types, which Bun's SQL driver expects.
func BunAdapter(p *Pool) *bunAdapter {
	sqlDB := stdlib.OpenDBFromPool(p.store.PgxPool())
	return &bunAdapter{
		db:   sqlDB,
		reg:  p.reg,
		pool: p,
	}
}

func (a *bunAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	rewritten, newArgs := a.rewriteExec(ctx, query, args)
	return a.db.ExecContext(ctx, rewritten, newArgs...)
}

func (a *bunAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rewritten, newArgs := a.rewriteQuery(query, args)
	return a.db.QueryContext(ctx, rewritten, newArgs...)
}

func (a *bunAdapter) rewriteExec(ctx context.Context, query string, args []any) (string, []any) {
	table, op, ok := parseSQL(query)
	if !ok {
		return query, args
	}

	info, found := a.reg.lookupByTable(table)
	if !found {
		return query, args
	}

	switch op {
	case opInsert:
		_ = a.pool.ensureTable(ctx, info)
		rewritten, newArgs, err := rewriteInsert(info, query, args)
		if err != nil {
			return query, args
		}
		return rewritten, newArgs

	case opUpdate:
		rewritten, newArgs, err := rewriteUpdate(info, query, args)
		if err != nil {
			return query, args
		}
		return rewritten, newArgs

	case opDelete:
		rewritten, newArgs, err := rewriteDelete(info, query, args)
		if err != nil {
			return query, args
		}
		return rewritten, newArgs

	case opCreateTable:
		rewritten, err := rewriteCreateTable(info, query)
		if err != nil {
			return query, args
		}
		return rewritten, nil

	default:
		return query, args
	}
}

func (a *bunAdapter) rewriteQuery(query string, args []any) (string, []any) {
	table, op, ok := parseSQL(query)
	if !ok {
		return query, args
	}

	info, found := a.reg.lookupByTable(table)
	if !found {
		return query, args
	}

	if op != opSelect && op != opSelectJoin {
		return query, args
	}

	rewritten, newArgs := rewriteGORMSelect(info, query, args)
	return rewritten, newArgs
}

// Close releases the underlying *sql.DB connection.
func (a *bunAdapter) Close() error {
	return a.db.Close()
}
