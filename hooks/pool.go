package hooks

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Exec intercepts INSERT/UPDATE/DELETE, rewrites for registered models,
// and passes through everything else.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	table, op, ok := parseSQL(sql)
	if !ok {
		return p.store.DBExecutor().Exec(ctx, sql, args...)
	}

	info, found := p.reg.lookupByTable(table)
	if !found {
		return p.store.DBExecutor().Exec(ctx, sql, args...)
	}

	switch op {
	case opInsert:
		if err := p.ensureTable(ctx, info); err != nil {
			return pgconn.CommandTag{}, fmt.Errorf("hooks: ensure table %s: %w", info.table, err)
		}
		rewritten, newArgs, err := rewriteInsert(info, sql, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return p.store.DBExecutor().Exec(ctx, rewritten, newArgs...)

	case opUpdate:
		rewritten, newArgs, err := rewriteUpdate(info, sql, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return p.store.DBExecutor().Exec(ctx, rewritten, newArgs...)

	case opDelete:
		rewritten, newArgs, err := rewriteDelete(info, sql, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return p.store.DBExecutor().Exec(ctx, rewritten, newArgs...)

	case opCreateTable:
		rewritten, err := rewriteCreateTable(info, sql)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return p.store.DBExecutor().Exec(ctx, rewritten)

	default:
		return p.store.DBExecutor().Exec(ctx, sql, args...)
	}
}

// Query intercepts SELECT, rewrites for registered models, and wraps
// results to unpack JSONB into individual columns.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	table, op, ok := parseSQL(sql)
	if !ok {
		return p.store.DBExecutor().Query(ctx, sql, args...)
	}

	info, found := p.reg.lookupByTable(table)
	if !found {
		return p.store.DBExecutor().Query(ctx, sql, args...)
	}

	switch op {
	case opSelectJoin:
		rewritten, newArgs, err := rewriteJoin(p.reg, sql, args)
		if err != nil {
			return nil, err
		}
		return p.store.DBExecutor().Query(ctx, rewritten, newArgs...)

	case opSelect:
		rewritten, newArgs, err := rewriteSelect(info, sql, args)
		if err != nil {
			return nil, err
		}
		rows, err := p.store.DBExecutor().Query(ctx, rewritten, newArgs...)
		if err != nil {
			return nil, err
		}
		return &translatedRows{inner: rows, info: info}, nil

	default:
		return p.store.DBExecutor().Query(ctx, sql, args...)
	}
}

// QueryRow wraps Query for single-row results.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := p.Query(ctx, sql, args...)
	if err != nil {
		return &errRow{err: err}
	}
	return &singleRow{rows: rows}
}

// translatedRows wraps pgx.Rows to unpack JSONB into ORM-expected columns.
type translatedRows struct {
	inner      pgx.Rows
	info       *modelInfo
	currentRow map[string]any
	scanned    bool
}

func (r *translatedRows) Close()                        { r.inner.Close() }
func (r *translatedRows) Err() error                    { return r.inner.Err() }
func (r *translatedRows) CommandTag() pgconn.CommandTag { return r.inner.CommandTag() }
func (r *translatedRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.inner.FieldDescriptions()
}
func (r *translatedRows) RawValues() [][]byte    { return r.inner.RawValues() }
func (r *translatedRows) Conn() *pgx.Conn        { return r.inner.Conn() }
func (r *translatedRows) Values() ([]any, error) { return r.inner.Values() }

func (r *translatedRows) Next() bool {
	if !r.inner.Next() {
		return false
	}
	var id string
	var data []byte
	var version int
	if err := r.inner.Scan(&id, &data, &version); err != nil {
		return false
	}
	r.currentRow = unpackRow(r.info, id, data, version)
	r.scanned = true
	return true
}

func (r *translatedRows) Scan(dest ...any) error {
	if !r.scanned {
		return fmt.Errorf("hooks: Scan called before Next")
	}
	cols := r.orderedColumns()
	for i, d := range dest {
		if i >= len(cols) {
			break
		}
		val := r.currentRow[cols[i]]
		if err := scanValue(d, val); err != nil {
			return err
		}
	}
	return nil
}

func (r *translatedRows) orderedColumns() []string {
	cols := []string{r.info.idColumn}
	for _, dc := range r.info.dataCols {
		cols = append(cols, dc.name)
	}
	cols = append(cols, r.info.versionCol)
	return cols
}

func scanValue(dest any, val any) error {
	switch d := dest.(type) {
	case *string:
		switch v := val.(type) {
		case string:
			*d = v
		default:
			*d = fmt.Sprint(v)
		}
	case *int:
		switch v := val.(type) {
		case int:
			*d = v
		case float64:
			*d = int(v)
		case int64:
			*d = int(v)
		}
	case *int64:
		switch v := val.(type) {
		case int:
			*d = int64(v)
		case float64:
			*d = int64(v)
		case int64:
			*d = v
		}
	case *any:
		*d = val
	}
	return nil
}

type errRow struct {
	err error
}

func (r *errRow) Scan(_ ...any) error { return r.err }

type singleRow struct {
	rows    pgx.Rows
	scanned bool
}

func (r *singleRow) Scan(dest ...any) error {
	if !r.scanned {
		if !r.rows.Next() {
			r.rows.Close()
			if err := r.rows.Err(); err != nil {
				return err
			}
			return pgx.ErrNoRows
		}
		r.scanned = true
	}
	err := r.rows.Scan(dest...)
	r.rows.Close()
	return err
}
