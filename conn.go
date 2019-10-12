// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
	"time"
)

// Conn represents a single database connection rather than a pool of database
// connections. Prefer running queries from DB unless there is a specific
// need for a continuous single database connection.
//
// A Conn must call Close to return the connection to the database pool
// and may do so concurrently with a running query.
//
// After a call to Close, all operations on the
// connection fail with ErrConnDone.
type Conn struct {
	conn         *stdSql.Conn
	killerPool   StdSQLDB
	connectionID string
	kto          time.Duration
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (c *Conn) Unleak() {
	c.killerPool = nil
	c.connectionID = ""
}

// Begin starts a transaction. The default isolation level is dependent on the driver.
func (c *Conn) Begin(opts *stdSql.TxOptions) (*Tx, error) {
	return c.BeginTx(context.Background(), opts)
}

// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (c *Conn) BeginTx(ctx context.Context, opts *stdSql.TxOptions) (*Tx, error) {

	tx, err := c.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{tx: tx, killerPool: c.killerPool, connectionID: c.connectionID}, nil
}

// Close returns the connection to the connection pool.
// All operations after a Close will return with ErrConnDone.
// Close is safe to call concurrently with other operations and will
// block until all other operations finish. It may be useful to first
// cancel any used context and then call close directly after.
func (c *Conn) Close() error {
	err := c.conn.Close()
	if err != nil {
		return err
	}
	c.Unleak() // Should this be called in a defer to guarantee it gets called?
	return nil
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (c *Conn) Exec(query string, args ...interface{}) (stdSql.Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...interface{}) (stdSql.Result, error) {

	// Create a context that is used to cancel ExecContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan stdSql.Result)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(c.killerPool, c.connectionID, c.kto)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := c.conn.ExecContext(cancelCtx, query, args...)
		if err != nil {
			errChan <- err
			return
		}
		outChan <- res
	}()

	select {
	case err := <-errChan:
		return nil, err
	case out := <-outChan:
		return out, nil
	}
}

// Ping verifies a connection to the database is still alive.
func (c *Conn) Ping() error {
	return c.PingContext(context.Background())
}

// PingContext verifies the connection to the database is still alive.
func (c *Conn) PingContext(ctx context.Context) error {
	// You can not cancel a Ping.
	// See: https://github.com/rocketlaunchr/mysql-go/issues/3
	return c.conn.PingContext(ctx)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (c *Conn) Prepare(query string) (*Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
func (c *Conn) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	// You can not cancel a Prepare.
	// See: https://github.com/rocketlaunchr/mysql-go/issues/3
	stmt, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt, c.killerPool, c.connectionID, c.kto}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *Conn) Query(query string, args ...interface{}) (*Rows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {

	// We can't use the same approach used in ExecContext because defer cancelFunc()
	// cancels rows.Scan.
	defer func() {
		if ctx.Err() != nil {
			kill(c.killerPool, c.connectionID, c.kto)
		}
	}()

	rows, err := c.conn.QueryContext(ctx, query, args...)
	return &Rows{ctx: ctx, rows: rows, killerPool: c.killerPool, connectionID: c.connectionID}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (c *Conn) QueryRow(query string, args ...interface{}) *Row {
	return c.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(c.killerPool, c.connectionID, c.kto)
		}
	}()

	row := c.conn.QueryRowContext(ctx, query, args...)
	return &Row{ctx: ctx, row: row, killerPool: c.killerPool, connectionID: c.connectionID}
}
