package hooks

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/stdlib"
)

// entDriver wraps a *sql.DB with SQL interception for registered Whisker models.
// It satisfies the minimal interface Ent needs: ExecContext and QueryContext.
type entDriver struct {
	db   *sql.DB
	reg  *registry
	pool *Pool
}

// EntDriver returns an adapter that intercepts SQL for registered Whisker models.
// The returned value provides ExecContext and QueryContext compatible with
// database/sql types, which Ent's dialect drivers expect.
func EntDriver(p *Pool) *entDriver {
	sqlDB := stdlib.OpenDBFromPool(p.store.PgxPool())
	return &entDriver{
		db:   sqlDB,
		reg:  p.reg,
		pool: p,
	}
}

func (d *entDriver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	rewritten, newArgs := d.rewriteExec(ctx, query, args)
	return d.db.ExecContext(ctx, rewritten, newArgs...)
}

func (d *entDriver) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rewritten, newArgs := d.rewriteQuery(query, args)
	return d.db.QueryContext(ctx, rewritten, newArgs...)
}

func (d *entDriver) rewriteExec(ctx context.Context, query string, args []any) (string, []any) {
	table, op, ok := parseSQL(query)
	if !ok {
		return query, args
	}

	info, found := d.reg.lookupByTable(table)
	if !found {
		return query, args
	}

	switch op {
	case opInsert:
		_ = d.pool.ensureTable(ctx, info)
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

func (d *entDriver) rewriteQuery(query string, args []any) (string, []any) {
	table, op, ok := parseSQL(query)
	if !ok {
		return query, args
	}

	info, found := d.reg.lookupByTable(table)
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
func (d *entDriver) Close() error {
	return d.db.Close()
}
