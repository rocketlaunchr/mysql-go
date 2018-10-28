package mysql

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// https://www.sohamkamani.com/blog/golang/2018-06-17-golang-using-context-cancellation/

type DB struct {
	*sql.DB
}

type Conn struct {
	*sql.Conn
	db           *sql.DB
	connectionID string
}

// kill is used to kill a running query.
func kill(db *sql.DB, connectionID string) error {

	_, err := db.Exec("KILL QUERY ?", connectionID)
	if err != nil {
		return err
	}

	return nil
}

// Conn returns a Conn object.
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
		return nil, err
	}

	return &Conn{conn, db.DB, connectionID}, nil
}

func (c *Conn) Close() error {
	err := c.Close()
	if err != nil {
		return err
	}
	c.db = nil
	c.connectionID = ""
	return nil
}

func (c *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {

	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{tx, c.db, c.connectionID}
}

func (c *Conn) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {

	// Create a context that is used to cancel ExecContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan sql.Result)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(c.db, c.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
		}
	}()

	go func() {
		res, err := c.ExecContext(cancelCtx, query, args...)
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
			kill(c.db, c.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
		}
	}()

	go func() {
		err := c.PingContext(cancelCtx)
		errChan <- err
	}()

	select {
	case err := <-errChan:
		return nil, err
	}
}

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
			kill(c.db, c.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
		}
	}()

	go func() {
		stmt, err := c.PrepareContext(cancelCtx, query)
		if err != nil {
			errChan <- err
			return
		}
		outChan <- &Stmt{stmt, c.db, c.connectionID}
	}()

	select {
	case err := <-errChan:
		return nil, err
	case out := <-outChan:
		return out, nil
	}

}

func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {

	// Create a context that is used to cancel QueryContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan *sql.Rows)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(c.db, c.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
		}
	}()

	go func() {
		res, err := c.QueryContext(cancelCtx, query, args...)
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
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(c.db, c.connectionID)
		}
	}()

	row := c.QueryRowContext(ctx, query, args...)
	return row
}

// type Conn
//     func (c *Conn) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error)
//     func (c *Conn) Close() error
//     func (c *Conn) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
//     func (c *Conn) PingContext(ctx context.Context) error
//     func (c *Conn) PrepareContext(ctx context.Context, query string) (*Stmt, error)
//     func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
//     func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row
// type DB
//     func Open(driverName, dataSourceName string) (*DB, error)
//     func OpenDB(c driver.Connector) *DB
//     func (db *DB) Begin() (*Tx, error)
//     func (db *DB) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error)
//     func (db *DB) Close() error
//     func (db *DB) Conn(ctx context.Context) (*Conn, error)
//     func (db *DB) Driver() driver.Driver
//     func (db *DB) Exec(query string, args ...interface{}) (Result, error)
//     func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
//     func (db *DB) Ping() error
//     func (db *DB) PingContext(ctx context.Context) error
//     func (db *DB) Prepare(query string) (*Stmt, error)
//     func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error)
//     func (db *DB) Query(query string, args ...interface{}) (*Rows, error)
//     func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
//     func (db *DB) QueryRow(query string, args ...interface{}) *Row
//     func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row
//     func (db *DB) SetConnMaxLifetime(d time.Duration)
//     func (db *DB) SetMaxIdleConns(n int)
//     func (db *DB) SetMaxOpenConns(n int)
//     func (db *DB) Stats() DBStats
