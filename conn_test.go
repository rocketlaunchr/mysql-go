package sql

// Special thanks to @soniah for tests below.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"
)

// nolint:gochecknoglobals
var dockerPool *dockertest.Pool // the connection to docker
// nolint:gochecknoglobals
var systemdb *sql.DB // the connection to the mysql 'system' database
// nolint:gochecknoglobals
var sqlConfig *mysql.Config // the mysql container and config for connecting to other databases
// nolint:gochecknoglobals
var testMu *sync.Mutex // controls access to sqlConfig

func TestMain(m *testing.M) {
	_ = mysql.SetLogger(log.New(ioutil.Discard, "", 0)) // silence mysql logger
	testMu = &sync.Mutex{}

	var err error
	dockerPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}
	dockerPool.MaxWait = time.Minute * 2

	runOptions := dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5.6",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
	}
	mysqlContainer, err := dockerPool.RunWithOptions(&runOptions)
	if err != nil {
		log.Fatalf("could not start mysqlContainer: %s", err)
	}

	sqlConfig = &mysql.Config{
		User:                 "root",
		Passwd:               "secret",
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("localhost:%s", mysqlContainer.GetPort("3306/tcp")),
		DBName:               "mysql",
		AllowNativePasswords: true,
	}

	if err = dockerPool.Retry(func() error {
		systemdb, err = sql.Open("mysql", sqlConfig.FormatDSN())
		if err != nil {
			return err
		}
		return systemdb.Ping()
	}); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// You can't defer this because os.Exit ignores defer
	if err := dockerPool.Purge(mysqlContainer); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestCancel(t *testing.T) {
	var err error
	_, err = systemdb.Exec("create database TestCancel")
	assert.NoError(t, err)

	testMu.Lock()
	testCancelConfig := sqlConfig
	testMu.Unlock()
	testCancelConfig.DBName = "TestCancel"
	var dbStd *sql.DB
	if err := dockerPool.Retry(func() error {
		dbStd, err = sql.Open("mysql", testCancelConfig.FormatDSN())
		if err != nil {
			return err
		}
		return dbStd.Ping()
	}); err != nil {
		log.Fatal(err)
	}

	dbKiller, err := sql.Open("mysql", testCancelConfig.FormatDSN())
	dbKiller.SetMaxOpenConns(1)
	pool := &DB{DB: dbStd, KillerPool: dbKiller}

	procs, err := helperFullProcessList(dbStd)
	assert.NoError(t, err)

	filterDB := func(m mySQLProcInfo) bool { return m.DB == "TestCancel" }
	filterState := func(m mySQLProcInfo) bool { return m.State == "executing" }
	procs = procs.Filter(filterDB, filterState)
	assert.Len(t, procs, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	conn, err := pool.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	go func() {
		_, err = conn.ExecContext(ctx, "select benchmark(9999999999, md5('I like traffic lights'))")
		assert.Equal(t, context.DeadlineExceeded, err)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
Loop:
	for {
		select {
		case <-ticker.C:
			procs, err := helperFullProcessList(dbStd)
			assert.NoError(t, err)
			procs = procs.Filter(filterDB, filterState)
			assert.Len(t, procs, 1)
		case <-ctx.Done():
			time.Sleep(3000 * time.Millisecond)
			procs, err := helperFullProcessList(dbStd)
			assert.NoError(t, err)
			procs = procs.Filter(filterDB, filterState)
			assert.Len(t, procs, 0)
			break Loop
		}
	}
}

type mySQLProcInfo struct {
	ID      int64   `db:"Id"`
	User    string  `db:"User"`
	Host    string  `db:"Host"`
	DB      string  `db:"db"`
	Command string  `db:"Command"`
	Time    int     `db:"Time"`
	State   string  `db:"State"`
	Info    *string `db:"Info"`
}
type mySQLProcsInfo []mySQLProcInfo

func helperFullProcessList(db *sql.DB) (mySQLProcsInfo, error) {
	dbx := sqlx.NewDb(db, "mysql")
	var procs []mySQLProcInfo
	if err := dbx.Select(&procs, "show full processlist"); err != nil {
		return nil, err
	}
	return procs, nil
}

func (ms mySQLProcsInfo) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "ID\tUser\tHost\tDB\tCommand\tTime\tState\tInfo")
	for _, m := range ms {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", m.ID, m.User, m.Host, m.DB, m.Command, m.Time,
			m.State, m.Info)
	}
	w.Flush()
	return buf.String()
}

func (ms mySQLProcsInfo) Filter(fns ...func(m mySQLProcInfo) bool) (result mySQLProcsInfo) {
	for _, m := range ms {
		ok := true
		for _, fn := range fns {
			if !fn(m) {
				ok = false
				break
			}
		}
		if ok {
			result = append(result, m)
		}
	}
	return result
}
