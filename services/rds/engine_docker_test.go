package rds_test

import (
	"context"
	"database/sql"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const (
	dockerEngineWait = 4 * time.Minute
	createItems      = "CREATE TABLE items (id int primary key, name text)"
	insertItem       = "INSERT INTO items VALUES (1, 'alpha')"
)

type recordingEngineRuntime struct {
	rds.EngineRuntime
	ids []string
	mu  sync.Mutex
}

func (r *recordingEngineRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.EngineRuntime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingEngineRuntime) Close() error {
	if c, ok := r.EngineRuntime.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (r *recordingEngineRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func dockerContainerExists(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--no-trunc", "--filter", "id="+id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) != ""
}

func newDockerEngineBackend(t *testing.T) (*rds.InMemoryBackend, *recordingEngineRuntime) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(rds.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	b.EnableEngine(rds.EngineConfig{Runtime: rec})
	t.Cleanup(b.Close)

	return b, rec
}

func openEngineDB(t *testing.T, kind string, inst rds.DBInstance, user, password string) *sql.DB {
	t.Helper()

	addr := net.JoinHostPort(inst.Endpoint, strconv.Itoa(inst.Port))

	var (
		db  *sql.DB
		err error
	)

	if kind == "postgres" {
		db, err = sql.Open("pgx", "postgres://"+user+":"+password+"@"+addr+"/appdb?sslmode=disable")
	} else {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, password, "tcp", addr, "appdb"

		connector, cerr := mysql.NewConnector(cfg)
		require.NoError(t, cerr)

		db = sql.OpenDB(connector)
	}

	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestEngineDockerRealDatabases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		kind       string
		createStmt string
		insertStmt string
	}{
		{name: "postgres", engine: "postgres", kind: "postgres", createStmt: createItems, insertStmt: insertItem},
		{name: "mysql", engine: "mysql", kind: "mysql", createStmt: createItems, insertStmt: insertItem},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rec := newDockerEngineBackend(t)

			_, err := b.CreateDBInstance("it1", tt.engine, "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "firstpass1"})
			require.NoError(t, err)

			var inst rds.DBInstance
			require.Eventually(t, func() bool {
				got, derr := b.DescribeDBInstances("it1")
				require.NoError(t, derr)
				inst = got[0]
				require.NotEqual(t, "failed", inst.DBInstanceStatus)

				return inst.DBInstanceStatus == "available"
			}, dockerEngineWait, 500*time.Millisecond)

			assert.Equal(t, "127.0.0.1", inst.Endpoint)

			db := openEngineDB(t, tt.kind, inst, "master", "firstpass1")
			require.NoError(t, db.PingContext(t.Context()))
			_, err = db.ExecContext(t.Context(), tt.createStmt)
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), tt.insertStmt)
			require.NoError(t, err)

			var name string
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT name FROM items WHERE id = 1").Scan(&name))
			assert.Equal(t, "alpha", name)

			_, err = b.ModifyDBInstance("it1", "", 0,
				rds.DBInstanceOptions{MasterUserPassword: "secondpass2", ApplyImmediately: true})
			require.NoError(t, err)

			fresh := openEngineDB(t, tt.kind, inst, "master", "secondpass2")
			require.NoError(t, fresh.PingContext(t.Context()))
			require.NoError(t, fresh.QueryRowContext(t.Context(), "SELECT name FROM items WHERE id = 1").Scan(&name))

			old := openEngineDB(t, tt.kind, inst, "master", "firstpass1")
			require.Error(t, old.PingContext(t.Context()))

			ids := rec.containerIDs()
			require.Len(t, ids, 1)
			require.True(t, dockerContainerExists(t, ids[0]))

			_, err = b.DeleteDBInstanceWithOptions("it1", true, "", true)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return !dockerContainerExists(t, ids[0]) },
				time.Minute, 250*time.Millisecond)
		})
	}
}
