// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
)

// Stmt is a prepared statement.
// A Stmt is safe for concurrent use by multiple goroutines.
type Stmt struct {
	*stdSql.Stmt
	killerPool   *stdSql.DB
	connectionID string
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (s *Stmt) Unleak() {
	s.killerPool = nil
	s.connectionID = ""
}

// Close closes the statement.
func (s *Stmt) Close() error {
	err := s.Close()
	if err != nil {
		return err
	}
	s.Unleak() // Should this be called in a defer to guarantee it gets called?
	return nil
}

// Exec executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (s *Stmt) Exec(args ...interface{}) (stdSql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// ExecContext executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (stdSql.Result, error) {

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
			kill(s.killerPool, s.connectionID)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := s.ExecContext(cancelCtx, args...)
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

// Query executes a prepared query statement with the given arguments
// and returns the query results as a *Rows.
func (s *Stmt) Query(args ...interface{}) (*stdSql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with the given arguments
// and returns the query results as a *Rows.
func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*stdSql.Rows, error) {

	// Create a context that is used to cancel QueryContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan *stdSql.Rows)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(s.killerPool, s.connectionID)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := s.QueryContext(cancelCtx, args...)
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

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
//
// Example usage:
//
//  var name string
//  err := nameByUseridStmt.QueryRow(id).Scan(&name)
func (s *Stmt) QueryRow(args ...interface{}) *stdSql.Row {
	return s.QueryRowContext(context.Background(), args...)
}

// QueryRowContext executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *stdSql.Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(s.killerPool, s.connectionID)
		}
	}()

	row := s.QueryRowContext(ctx, args...)
	return row
}
