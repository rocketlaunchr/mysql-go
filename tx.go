package mysql

import (
	"context"
	"database/sql"
)

type Tx struct {
	*sql.Tx
	db           *sql.DB
	connectionID string
}

// If context cancels, then call the kill function. Or should we rollback?

// type Tx
//     func (tx *Tx) Commit() error
//     func (tx *Tx) Exec(query string, args ...interface{}) (Result, error)
//     func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
//     func (tx *Tx) Prepare(query string) (*Stmt, error)
//     func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error)
//     func (tx *Tx) Query(query string, args ...interface{}) (*Rows, error)
//     func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
//     func (tx *Tx) QueryRow(query string, args ...interface{}) *Row
//     func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row
//     func (tx *Tx) Rollback() error
//     func (tx *Tx) Stmt(stmt *Stmt) *Stmt
//     func (tx *Tx) StmtContext(ctx context.Context, stmt *Stmt) *Stmt
