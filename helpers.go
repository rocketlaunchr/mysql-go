// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	stdSql "database/sql"
	"errors"
	"time"
)

var errRetry = errors.New("retry")

// kill is used to kill a running query.
// It is advised that db be another pool that the
// connection was NOT derived from.
func kill(db *stdSql.DB, connectionID, serverID, flavor string, killTimeout *time.Duration) error {

	if connectionID == "" {
		return nil
	}

	if killTimeout == nil {
		// Database not behind a reverse proxy
		_, err := db.Exec("KILL QUERY ?", connectionID)
		return err
	}

	// Database is behind a reverse proxy

	ctx, cancel := context.WithTimeout(context.Background(), *killTimeout)
	defer cancel()

	if flavor == MariaDB {
		// Not supported at the moment
		return nil
	}

	for {

		if ctx.Err() != nil {
			// Timed out
			return ctx.Err()
		}

		err := func() error {

			// Get an exclusive connection
			conn, err := db.Conn(ctx)
			if err != nil {
				return errRetry
			}
			defer conn.Close()

			curServerID, err := determineServerID(ctx, conn, flavor)
			if err != nil || curServerID == "" {
				return errRetry
			}

			if curServerID != serverID {
				// Wrong server so retry
				return errRetry
			}

			// This is the correct server so send kill signal
			_, err = db.Exec("KILL QUERY ?", connectionID)
			if err != nil {
				// Don't retry but send back error
				return err
			}
			return nil

		}()

		if err == nil {
			return nil
		} else if err != errRetry {
			return err
		}

	}

	return nil
}

// determineServerID is used to determine the server's unique identifier
func determineServerID(ctx context.Context, conn *stdSql.Conn, flavor string) (string, error) {

	if flavor == MariaDB {
		return "", nil
	}

	var serverID string

	err := conn.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&serverID)
	if err != nil {
		return "", err
	}

	return serverID, nil
}
