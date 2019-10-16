// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
	"sync"
	"time"
)

// Tx is an in-progress database transaction.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the
// transaction fail with ErrTxDone.
//
// The statements prepared for a transaction by calling
// the transaction's Prepare or Stmt methods are closed
// by the call to Commit or Rollback.
type Tx struct {
	tx           *stdSql.Tx
	killerPool   StdSQLDB
	connectionID string
	kto          time.Duration

	// Lock and store stmts
	lock  sync.Mutex
	stmts []*Stmt
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (tx *Tx) Unleak() {
	tx.killerPool = nil
	tx.connectionID = ""
	tx.kto = 0
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	defer func() {
		// Perhaps only do this if err == nil
		tx.lock.Lock()
		for i := range tx.stmts {
			tx.stmts[i].Unleak()
		}
		tx.lock.Unlock()
	}()

	err := tx.tx.Commit()
	// if err == nil { See: https://github.com/golang/go/issues/28474
	tx.Unleak()
	// }
	return err
}

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (tx *Tx) Exec(query string, args ...interface{}) (stdSql.Result, error) {
	return tx.tx.Exec(query, args...)
}

// ExecContext executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (stdSql.Result, error) {

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
			kill(tx.killerPool, tx.connectionID, tx.kto)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := tx.tx.ExecContext(cancelCtx, query, args...)
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

// Prepare creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and can no longer
// be used once the transaction has been committed or rolled back.
//
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed
// when the transaction has been committed or rolled back.
//
// To use an existing prepared statement on this transaction, see Tx.Stmt.
//
// The provided context will be used for the preparation of the context, not
// for the execution of the returned statement. The returned statement
// will run in the transaction context.
func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	// You can not cancel a Prepare.
	// See: https://github.com/rocketlaunchr/mysql-go/issues/3
	stmt, err := tx.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	st := &Stmt{stmt, tx.killerPool, tx.connectionID, tx.kto}
	tx.lock.Lock()
	tx.stmts = append(tx.stmts, st)
	tx.lock.Unlock()
	return st, nil
}

// Query executes a query that returns rows, typically a SELECT.
func (tx *Tx) Query(query string, args ...interface{}) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {

	// We can't use the same approach used in ExecContext because defer cancelFunc()
	// cancels rows.Scan.
	defer func() {
		if ctx.Err() != nil {
			kill(tx.killerPool, tx.connectionID, tx.kto)
		}
	}()

	rows, err := tx.tx.QueryContext(ctx, query, args...)
	return &Rows{ctx: ctx, rows: rows, killerPool: tx.killerPool, connectionID: tx.connectionID}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(tx.killerPool, tx.connectionID, tx.kto)
		}
	}()

	row := tx.tx.QueryRowContext(ctx, query, args...)
	return &Row{ctx: ctx, row: row, killerPool: tx.killerPool, connectionID: tx.connectionID}
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	defer func() {
		// Perhaps only do this if err == nil
		tx.lock.Lock()
		for i := range tx.stmts {
			tx.stmts[i].Unleak()
		}
		tx.lock.Unlock()
	}()

	err := tx.tx.Rollback()
	// if err == nil { // See: https://github.com/golang/go/issues/28474
	tx.Unleak()
	// }
	return err
}

// Stmt returns a transaction-specific prepared statement from
// an existing statement.
//
// Example:
//  updateMoney, err := db.Prepare("UPDATE balance SET money=money+? WHERE id=?")
//  ...
//  tx, err := db.Begin()
//  ...
//  res, err := tx.Stmt(updateMoney).Exec(123.45, 98293203)
//
// The returned statement operates within the transaction and will be closed
// when the transaction has been committed or rolled back.
func (tx *Tx) Stmt(stmt *stdSql.Stmt) *Stmt {
	return tx.StmtContext(context.Background(), stmt)
}

// StmtContext returns a transaction-specific prepared statement from
// an existing statement.
//
// Example:
//  updateMoney, err := db.Prepare("UPDATE balance SET money=money+? WHERE id=?")
//  ...
//  tx, err := db.Begin()
//  ...
//  res, err := tx.StmtContext(ctx, updateMoney).Exec(123.45, 98293203)
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
//
// The returned statement operates within the transaction and will be closed
// when the transaction has been committed or rolled back.
func (tx *Tx) StmtContext(ctx context.Context, stmt *stdSql.Stmt) *Stmt {

	st := &Stmt{tx.tx.StmtContext(ctx, stmt), tx.killerPool, tx.connectionID, tx.kto}
	tx.lock.Lock()
	tx.stmts = append(tx.stmts, st)
	tx.lock.Unlock()
	return st
}
