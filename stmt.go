package mysql

import (
	"context"
	"database/sql"
)

type Stmt struct {
	*sql.Stmt
	db           *sql.DB
	connectionID string
}

func (s *Stmt) Close() error {
	err := s.Close()
	if err != nil {
		return err
	}
	s.db = nil
	s.connectionID = ""
	return nil
}

func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {

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
			kill(s.db, s.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
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

func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {

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
			kill(s.db, s.connectionID)
			errChan <- ctx.Err()
			cancelFunc()
			return
		case <-returnedChan:
			return
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

func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {

	// Since sql.Row does not export err field, this is the best we can do:
	defer func() {
		if ctx.Err() != nil {
			kill(s.db, s.connectionID)
		}
	}()

	row := s.QueryRowContext(ctx, args...)
	return row
}

// type Stmt
//     func (s *Stmt) Close() error
//     func (s *Stmt) Exec(args ...interface{}) (Result, error)
//     func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (Result, error)
//     func (s *Stmt) Query(args ...interface{}) (*Rows, error)
//     func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*Rows, error)
//     func (s *Stmt) QueryRow(args ...interface{}) *Row
//     func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *Row
