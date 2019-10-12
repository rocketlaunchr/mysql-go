// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
	"database/sql/driver"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Open opens a database specified by its connection information.
//
// You will need to import "github.com/go-sql-driver/mysql".
//
// Open will just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// Ping.
//
// The returned DB contains 2 pools. The KillerPool is configured to have only
// 1 max open connection. It is for internal use only.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Open
// function should be called just once. It is rarely necessary to
// close a DB. For the cancelation feature, a Conn needs to be created.
func Open(driverName string, dataSourceName ...string) (*DB, error) {
	var (
		dn  string
		dsn string
	)

	if len(dataSourceName) == 0 {
		dn = "mysql"
		dsn = driverName
	} else {
		dn = driverName
		dsn = dataSourceName[0]
	}

	var (
		mp *stdSql.DB
		kp *stdSql.DB
	)

	var g errgroup.Group

	g.Go(func() error {
		pool, err := stdSql.Open(dn, dsn)
		if err != nil {
			return err
		}

		mp = pool
		return nil
	})

	g.Go(func() error {
		pool, err := stdSql.Open(dn, dsn)
		if err != nil {
			return err
		}

		kp = pool
		kp.SetMaxOpenConns(1)
		return nil
	})

	err := g.Wait()
	if err != nil {
		if mp != nil {
			mp.Close()
		}

		if kp != nil {
			kp.Close()
		}

		return nil, err
	}

	return &DB{
		DB:         mp,
		KillerPool: kp,
	}, nil
}

// OpenDB opens a database using a Connector, allowing drivers to
// bypass a string based data source name.
//
// You will need to import "github.com/go-sql-driver/mysql".
//
// OpenDB will just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// Ping.
//
// The returned DB contains 2 pools. The KillerPool is configured to have only
// 1 max open connection. It is for internal use only.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the OpenDB
// function should be called just once. It is rarely necessary to
// close a DB. For the cancelation feature, a Conn needs to be created.
func OpenDB(c driver.Connector) *DB {

	var (
		mp *stdSql.DB
		kp *stdSql.DB
	)

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		defer wg.Done()
		mp = stdSql.OpenDB(c)
	}()

	go func() {
		wg.Add(1)
		defer wg.Done()
		kp = stdSql.OpenDB(c)
		kp.SetMaxOpenConns(1)
	}()

	wg.Wait()

	return &DB{
		DB:         mp,
		KillerPool: kp,
	}
}

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
//
// The sql package creates and frees connections automatically; it
// also maintains a free pool of idle connections. If the database has
// a concept of per-connection state, such state can be reliably observed
// within a transaction (Tx) or connection (Conn). Once DB.Begin is called, the
// returned Tx is bound to a single connection. Once Commit or
// Rollback is called on the transaction, that transaction's
// connection is returned to DB's idle connection pool. The pool size
// can be controlled with SetMaxIdleConns.
type DB struct {

	// DB is the primary connection pool (i.e. *stdSql.DB).
	DB StdSQLDBExtra

	// KillerPool is an optional (but recommended) secondary connection pool (i.e. *stdSql.DB).
	// If provided, it is used to fire KILL signals.
	KillerPool StdSQLDBExtra

	// KillTimeout sets how long to attempt sending the KILL signal.
	// A value of zero is equivalent to no time limit (not recommended).
	KillTimeout time.Duration
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
func (db *DB) Begin() (*stdSql.Tx, error) {
	return db.DB.Begin()
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
func (db *DB) BeginTx(ctx context.Context, opts *stdSql.TxOptions) (*stdSql.Tx, error) {
	return db.DB.BeginTx(ctx, opts)
}

// Close closes the database and prevents new queries from starting.
// Close then waits for all queries that have started processing on the server
// to finish.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (db *DB) Close() error {

	if db.KillerPool == db.DB {
		return db.DB.Close()
	}

	if db.KillerPool != nil {
		db.KillerPool.Close()
	}

	return db.DB.Close()
}

// Conn returns a single connection by either opening a new connection
// or returning an existing connection from the connection pool. Conn will
// block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
//
// Every Conn must be returned to the database pool after use by
// calling Conn.Close.
func (db *DB) Conn(ctx context.Context) (*Conn, error) {

	// Obtain an exclusive connection
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// Determine the connection's connection_id
	var connectionID string

	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID)
	if err != nil {
		// Return the connection back to the pool
		conn.Close()
		return nil, err
	}

	if db.KillerPool == nil {
		return &Conn{conn, db.DB, connectionID, db.KillTimeout}, nil
	}
	return &Conn{conn, db.KillerPool, connectionID, db.KillTimeout}, nil
}

// Driver returns the database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.DB.Driver()
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(query string, args ...interface{}) (stdSql.Result, error) {
	return db.DB.Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (stdSql.Result, error) {
	return db.DB.ExecContext(ctx, query, args...)
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return db.DB.Ping()
}

// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	return db.DB.PingContext(ctx)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (db *DB) Prepare(query string) (*stdSql.Stmt, error) {
	return db.DB.Prepare(query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
func (db *DB) PrepareContext(ctx context.Context, query string) (*stdSql.Stmt, error) {
	return db.DB.PrepareContext(ctx, query)
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (db *DB) Query(query string, args ...interface{}) (*stdSql.Rows, error) {
	return db.DB.Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*stdSql.Rows, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (db *DB) QueryRow(query string, args ...interface{}) *stdSql.Row {
	return db.DB.QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *stdSql.Row {
	return db.DB.QueryRowContext(ctx, query, args...)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.DB.SetConnMaxLifetime(d)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func (db *DB) SetMaxIdleConns(n int) {
	db.DB.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	db.DB.SetMaxOpenConns(n)
}

// Stats returns database statistics.
func (db *DB) Stats() stdSql.DBStats {
	return db.DB.Stats()
}
