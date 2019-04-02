package sql

import (
	"context"
	stdSql "database/sql"
)

// Row is the result of calling QueryRow to select a single row.
type Row struct {
	ctx          context.Context
	row          *stdSql.Row
	killerPool   StdSQLDB
	connectionID string
}

// Scan copies the columns from the matched row into the values
// pointed at by dest. See the documentation on Rows.Scan for details.
// If more than one row matches the query,
// Scan uses the first row and discards the rest. If no row matches
// the query, Scan returns ErrNoRows.
func (r *Row) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if r.ctx.Err() != nil {
		kill(r.killerPool, r.connectionID)
	}
	return err
}
