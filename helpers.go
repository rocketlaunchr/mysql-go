// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	"time"
)

// kill is used to kill a running query.
// It is advised that db be another pool that the
// connection was NOT derived from.
func kill(db StdSQLDB, connectionID string, kto time.Duration) error {

	if connectionID == "" {
		return nil
	}

	stmt := `KILL QUERY ?`

	if kto == 0 {
		_, err := db.Exec(stmt, connectionID)
		if err != nil {
			return err
		}
	} else {
		ctx, cancelFunc := context.WithTimeout(context.Background(), kto)
		defer cancelFunc()
		_, err := db.ExecContext(ctx, stmt, connectionID)
		if err != nil {
			return err
		}
	}

	return nil
}
