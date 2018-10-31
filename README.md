Canceling MySQL in Go [![GoDoc](http://godoc.org/github.com/rocketlaunchr/mysql-go?status.svg)](http://godoc.org/github.com/rocketlaunchr/mysql-go) [![Go Report Card](https://goreportcard.com/badge/github.com/rocketlaunchr/mysql-go)](https://goreportcard.com/report/github.com/rocketlaunchr/mysql-go)
===============

This is an experimental package to help you properly cancel MySQL queries. Without this package, context cancelation does not actually cancel a MySQL query.
It may or may not be suitable for your needs. Field reports are greatly appreciated.

See [Article](https://medium.com/@rocketlaunchr.cloud/canceling-mysql-in-go-827ed8f83b30) for details of the behind-the-scenes magic.

The API is designed to resemble the standard library.

## Dependencies

* [Go MySQL Driver](https://github.com/go-sql-driver/mysql)

## Installation

```
go get -u github.com/rocketlaunchr/mysql-go
```

## QuickStart

```go

import (
   stdSql "database/sql"
   sql "github.com/rocketlaunchr/mysql-go"
)

p, _ := stdSql.Open("mysql", "user:password@/dbname")
kP, _ := stdSql.Open("mysql", "user:password@/dbname") // KillerPool
kP.SetMaxOpenConns(1)

pool := sql.DB{p, kP}

```

## Read Query

```go

// Obtain an exclusive connection
conn, err := pool.Conn(ctx)
defer conn.Close() // Return the connection back to the pool

// Perform your read operation.
rows, err := conn.QueryContext(ctx, stmt)
if err != nil {
   return err
}

```

## Write Query

```go

// Obtain an exclusive connection
conn, err := pool.Conn(ctx)
defer conn.Close() // Return the connection back to the pool

// Perform the write operation
tx, err := conn.BeginTx(ctx, nil)

_, err = tx.ExecContext(ctx, stmt)
if err != nil {
   return tx.Rollback()
}

tx.Commit()
```

## Cancel Query

Cancel the context. This will send a `KILL` signal to MySQL automatically.

It is highly recommended you set a KillerPool when you instantiate the `DB` object.

The KillerPool is used to call the `KILL` signal.


#

### Legal Information

The license is a modified MIT license. Refer to `LICENSE` file for more details.

**© 2018 PJ Engineering and Business Solutions Pty. Ltd.**

### Final Notes

Feel free to enhance features by issuing pull-requests.

**Star** the project to show your appreciation.
