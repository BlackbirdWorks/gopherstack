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
	ids    []string
	netIDs []string
	mu     sync.Mutex
}

func (r *recordingEngineRuntime) network() (container.NetworkRuntime, bool) {
	n, ok := r.EngineRuntime.(container.NetworkRuntime)

	return n, ok
}

func (r *recordingEngineRuntime) CreateNetwork(ctx context.Context, name string) (string, error) {
	n, ok := r.network()
	if !ok {
		return "", container.ErrNetworkUnsupported
	}

	id, err := n.CreateNetwork(ctx, name)
	if err == nil {
		r.mu.Lock()
		r.netIDs = append(r.netIDs, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingEngineRuntime) ConnectNetwork(ctx context.Context, networkID, containerID string) error {
	n, ok := r.network()
	if !ok {
		return container.ErrNetworkUnsupported
	}

	return n.ConnectNetwork(ctx, networkID, containerID)
}

func (r *recordingEngineRuntime) RemoveNetwork(ctx context.Context, networkID string) error {
	n, ok := r.network()
	if !ok {
		return container.ErrNetworkUnsupported
	}

	return n.RemoveNetwork(ctx, networkID)
}

func (r *recordingEngineRuntime) networkIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.netIDs...)
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

func (r *recordingEngineRuntime) Exec(
	ctx context.Context, id string, cmd []string,
) (container.ExecResult, error) {
	x, ok := r.EngineRuntime.(rds.EngineExecRuntime)
	if !ok {
		return container.ExecResult{}, container.ErrExecUnsupported
	}

	return x.Exec(ctx, id, cmd)
}

func (r *recordingEngineRuntime) ExecEnv(
	ctx context.Context, id string, cmd, env []string,
) (container.ExecResult, error) {
	x, ok := r.EngineRuntime.(rds.EngineExecRuntime)
	if !ok {
		return container.ExecResult{}, container.ErrExecUnsupported
	}

	return container.ExecWithEnv(ctx, x, id, cmd, env)
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

func dockerPublishedHostIP(t *testing.T, id string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "inspect", "-f",
		"{{range $p, $b := .HostConfig.PortBindings}}{{(index $b 0).HostIp}}{{end}}", id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
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

func openEngineDB(t *testing.T, kind string, inst rds.DBInstance, password string) *sql.DB {
	t.Helper()

	addr := net.JoinHostPort(inst.Endpoint, strconv.Itoa(inst.Port))
	const user = "master"

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
				requireNotFailed(t, b, "it1", inst.DBInstanceStatus)

				return inst.DBInstanceStatus == "available"
			}, dockerEngineWait, 500*time.Millisecond)

			assert.Equal(t, "127.0.0.1", inst.Endpoint)

			db := openEngineDB(t, tt.kind, inst, "firstpass1")
			require.NoError(t, db.PingContext(dbCtx(t)))
			_, err = db.ExecContext(dbCtx(t), tt.createStmt)
			require.NoError(t, err)
			_, err = db.ExecContext(dbCtx(t), tt.insertStmt)
			require.NoError(t, err)

			var name string
			require.NoError(t, db.QueryRowContext(dbCtx(t), "SELECT name FROM items WHERE id = 1").Scan(&name))
			assert.Equal(t, "alpha", name)

			_, err = b.ModifyDBInstance("it1", "", 0,
				rds.DBInstanceOptions{MasterUserPassword: "secondpass2", ApplyImmediately: true})
			require.NoError(t, err)

			fresh := openEngineDB(t, tt.kind, inst, "secondpass2")
			require.NoError(t, fresh.PingContext(dbCtx(t)))
			require.NoError(t, fresh.QueryRowContext(dbCtx(t), "SELECT name FROM items WHERE id = 1").Scan(&name))

			old := openEngineDB(t, tt.kind, inst, "firstpass1")
			require.Error(t, old.PingContext(dbCtx(t)))

			ids := rec.containerIDs()
			require.Len(t, ids, 1)
			require.True(t, dockerContainerExists(t, ids[0]))
			assert.Equal(t, "127.0.0.1", dockerPublishedHostIP(t, ids[0]))

			_, err = b.DeleteDBInstanceWithOptions("it1", true, "", true)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return !dockerContainerExists(t, ids[0]) },
				time.Minute, 250*time.Millisecond)
		})
	}
}

// dbCtx bounds a database call so a stalled proxy fails the test instead of hanging to the package timeout.
func dbCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
	t.Cleanup(cancel)

	return ctx
}

func requireNotFailed(t *testing.T, b *rds.InMemoryBackend, id, status string) {
	t.Helper()

	if status == "failed" {
		t.Fatalf("%s reached status failed: %s", id, b.EngineFailure(id))
	}
}
