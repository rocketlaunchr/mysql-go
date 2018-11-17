// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
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
	killerPool   *stdSql.DB
	connectionID string
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (c *Conn) Unleak() {
	c.killerPool = nil
	c.connectionID = ""
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
			kill(c.killerPool, c.connectionID)
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

// PingContext verifies the connection to the database is still alive.
func (c *Conn) PingContext(ctx context.Context) error {

	// Create a context that is used to cancel PingContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(c.killerPool, c.connectionID)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		err := c.conn.PingContext(cancelCtx)
		errChan <- err
	}()

	select {
	case err := <-errChan:
		return err
	}
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

	// Create a context that is used to cancel PrepareContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan *Stmt)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(c.killerPool, c.connectionID)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		stmt, err := c.conn.PrepareContext(cancelCtx, query)
		if err != nil {
			errChan <- err
			return
		}
		outChan <- &Stmt{stmt, c.killerPool, c.connectionID}
	}()

	select {
	case err := <-errChan:
		return nil, err
	case out := <-outChan:
		return out, nil
	}

}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*stdSql.Rows, error) {

	// We can't use the same approach used in ExecContext because defer cancelFunc()
	// cancels rows.Scan.
	defer func() {
		if ctx.Err() != nil {
			kill(c.killerPool, c.connectionID)
		}
	}()

	return c.conn.QueryContext(ctx, query, args...)
}

}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *stdSql.Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(c.killerPool, c.connectionID)
		}
	}()

	row := c.conn.QueryRowContext(ctx, query, args...)
	return row
}
