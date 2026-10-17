package redshift_test

import (
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/redshift"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

const dockerEngineWait = 5 * time.Minute

type redshiftEngines struct{ b *redshift.InMemoryBackend }

func (r redshiftEngines) EngineLogin(_, id string) (redshiftdata.EngineLogin, bool) {
	l, ok := r.b.EngineLogin(id)

	return redshiftdata.EngineLogin(l), ok
}

func dockerContainerState(t *testing.T, id string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-a", "--no-trunc",
		"--filter", "id="+id, "--format", "{{.State}}").Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}

type recordingRuntime struct {
	redshift.EngineRuntime
	ids chan string
}

func (r *recordingRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.EngineRuntime.CreateAndStart(ctx, spec)
	if err == nil {
		r.ids <- id
	}

	return id, err
}

func (r *recordingRuntime) Close() error {
	if c, ok := r.EngineRuntime.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func TestEngineDockerRealPostgres(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(redshift.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingRuntime{EngineRuntime: er, ids: make(chan string, 1)}
	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(redshift.EngineConfig{Runtime: rec})
	t.Cleanup(b.Close)

	_, err = b.CreateCluster("c1", "", "analytics", "admin1", nil, "",
		redshift.CreateClusterOptions{MasterUserPassword: "Passw0rdXyz"})
	require.NoError(t, err)

	c := waitStatusFor(t, b, "available", dockerEngineWait)
	id := <-rec.ids

	assert.Equal(t, "127.0.0.1", c.Endpoint)
	assert.Equal(t, "running", dockerContainerState(t, id))

	data := redshiftdata.NewInMemoryBackend("000000000000", "us-east-1")
	data.SetClusterEngines(redshiftEngines{b: b})

	run := func(sql string, params ...redshiftdata.SQLParameter) *redshiftdata.Statement {
		t.Helper()

		stmt, execErr := data.ExecuteStatement(t.Context(), sql, "c1", "", "", "", "", "", false, "", params, "")
		require.NoError(t, execErr)

		return stmt
	}

	create := run("CREATE TABLE items (id int, name varchar(20), score float8, ok boolean, at timestamp)")
	require.Equal(t, "FINISHED", create.Status, create.Error)
	assert.False(t, create.HasResultSet)

	insert := run(
		"INSERT INTO items VALUES (:id, :name, 1.5, true, '2024-01-02 03:04:05'), (2, NULL, NULL, false, NULL)",
		redshiftdata.SQLParameter{Name: "id", Value: "1"},
		redshiftdata.SQLParameter{Name: "name", Value: "ada"},
	)
	require.Equal(t, "FINISHED", insert.Status, insert.Error)

	sel := run("SELECT id, name, score, ok, at FROM items ORDER BY id")
	require.Equal(t, "FINISHED", sel.Status, sel.Error)
	assert.True(t, sel.HasResultSet)
	assert.Equal(t, int64(2), sel.ResultRows)

	rs, found, err := data.EngineResult(t.Context(), sel.ID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, rs.Columns, 5)
	assert.Equal(t, "int4", rs.Columns[0].TypeName)
	assert.Equal(t, "varchar", rs.Columns[1].TypeName)
	assert.Equal(t, int32(20), rs.Columns[1].Length)
	require.Len(t, rs.Rows, 2)
	assert.Equal(t, int64(1), *rs.Rows[0][0].LongValue)
	assert.Equal(t, "ada", *rs.Rows[0][1].StringValue)
	assert.InDelta(t, 1.5, *rs.Rows[0][2].DoubleValue, 0)
	assert.True(t, *rs.Rows[0][3].BooleanValue)
	assert.Equal(t, "2024-01-02 03:04:05", *rs.Rows[0][4].StringValue)
	assert.True(t, rs.Rows[1][1].IsNull)
	assert.False(t, *rs.Rows[1][3].BooleanValue)

	bad := run("SELECT * FROM missing")
	assert.Equal(t, "FAILED", bad.Status)
	assert.Contains(t, bad.Error, `relation "missing" does not exist`)

	_, err = b.ModifyCluster("c1", redshift.ModifyClusterOptions{MasterUserPassword: "N3wPassword9"})
	require.NoError(t, err)

	login, ok := b.EngineLogin("c1")
	require.True(t, ok)
	assert.Equal(t, "N3wPassword9", login.Password)
	assert.Equal(t, "FINISHED", run("SELECT 1").Status)

	_, err = b.PauseCluster("c1")
	require.NoError(t, err)
	waitStatusFor(t, b, "paused", dockerEngineWait)
	assert.Equal(t, "exited", dockerContainerState(t, id))

	_, err = b.ResumeCluster("c1")
	require.NoError(t, err)

	resumed := waitStatusFor(t, b, "available", dockerEngineWait)
	assert.Equal(t, c.Port, resumed.Port)
	assert.Equal(t, "running", dockerContainerState(t, id))

	_, err = b.DeleteCluster("c1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return dockerContainerState(t, id) == "" }, time.Minute, 250*time.Millisecond)
}
