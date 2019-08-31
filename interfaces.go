// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
	"database/sql/driver"
	"time"
)

// StdSQLLegacy will potentially be removed in Go 2.
type StdSQLLegacy interface {
	Exec(query string, args ...interface{}) (stdSql.Result, error)
	Prepare(query string) (*stdSql.Stmt, error)
	Query(query string, args ...interface{}) (*stdSql.Rows, error)
	QueryRow(query string, args ...interface{}) *stdSql.Row
}

// StdSQLCommon is the interface that allows query and exec interactions with a database.
type StdSQLCommon interface {
	StdSQLLegacy
	ExecContext(ctx context.Context, query string, args ...interface{}) (stdSql.Result, error)
	PrepareContext(ctx context.Context, query string) (*stdSql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*stdSql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *stdSql.Row
}

// StdSQLDB is the interface that allows a transaction to be created.
type StdSQLDB interface {
	Ping() error
	PingContext(ctx context.Context) error
	StdSQLCommon
	Conn(ctx context.Context) (*stdSql.Conn, error)
	Begin() (*stdSql.Tx, error)
	BeginTx(ctx context.Context, opts *stdSql.TxOptions) (*stdSql.Tx, error)
	Close() error
}

// StdSQLDBExtra is the interface that directly maps to a *stdSql.DB.
type StdSQLDBExtra interface {
	StdSQLDB
	Driver() driver.Driver
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() stdSql.DBStats
}

// StdSQLTx is the interface that allows a transaction to be committed or rolledback.
type StdSQLTx interface {
	StdSQLCommon
	Stmt(stmt *stdSql.Stmt) *stdSql.Stmt
	StmtContext(ctx context.Context, stmt *stdSql.Stmt) *stdSql.Stmt
	Commit() error
	Rollback() error
}

// SQLBasic is the interface that allows Conn and Stmt to be used.
type SQLBasic interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (stdSql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row
}

// SQLConn is the interface that allows Conn and Stmt to be used.
type SQLConn interface {
	SQLBasic
	BeginTx(ctx context.Context, opts *stdSql.TxOptions) (*Tx, error)
	Close() error
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*Stmt, error)
}

// SQLTx is the interface that allows Tx to be used.
type SQLTx interface {
	SQLBasic
	Stmt(stmt *stdSql.Stmt) *Stmt
	StmtContext(ctx context.Context, stmt *stdSql.Stmt) *Stmt
	Commit() error
	Rollback() error
}
