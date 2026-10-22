package azuresql_test

import (
	"context"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

func sqlcmd(t *testing.T, id, login, password, db, query string) (string, error) {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "exec", id, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
		"-U", login, "-P", password, "-C", "-b", "-h", "-1", "-W", "-d", db, "-Q", "SET NOCOUNT ON; "+query).
		CombinedOutput()

	return strings.TrimSpace(string(out)), err
}

// TestDockerEngine runs a real SQL Server container and is skipped without a container runtime.
func TestDockerEngine(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker engine test skipped in short mode")
	}

	lr := gcpserve.NewLazyRuntime(slog.Default(), "azuresql.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	svc, err := (&azuresql.Provider{}).Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*azuresql.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	const (
		login  = "sqladmin"
		first  = "It's-Passw0rd!"
		second = "N3w-Passw0rd'$(x)"
		dbName = "my db's"
	)

	ep, err := h.Backend.Ensure("srv", azuresql.Spec{AdminLogin: login, AdminPassword: first})
	require.NoError(t, err)
	require.NotZero(t, ep.Port)

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-q", "--filter", "publish="+strconv.Itoa(ep.Port)).
		Output()
	require.NoError(t, err)

	id := strings.TrimSpace(string(out))
	require.NotEmpty(t, id)

	require.NoError(t, h.Backend.EnsureDatabase("srv", dbName, "Latin1_General_CI_AS"))
	require.NoError(t, h.Backend.EnsureDatabase("srv", dbName, "Latin1_General_CI_AS"), "idempotent")

	tests := []struct {
		name     string
		password string
		db       string
		query    string
		want     string
		wantErr  bool
	}{
		{
			name:     "admin logs in with a quote in the password",
			password: first,
			db:       dbName,
			query:    "SELECT DB_NAME()",
			want:     dbName,
		},
		{
			name:     "collation applied",
			password: first,
			db:       dbName,
			query:    "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS varchar(64))",
			want:     "Latin1_General_CI_AS",
		},
		{
			name: "admin can create objects", password: first, db: dbName, want: "7",
			query: "CREATE TABLE t (i int); INSERT t VALUES (7); SELECT i FROM t",
		},
		{name: "wrong password", password: "nope", db: "master", query: "SELECT 1", wantErr: true},
	}

	for _, tt := range tests {
		got, qErr := sqlcmd(t, id, login, tt.password, tt.db, tt.query)
		if tt.wantErr {
			require.Error(t, qErr, tt.name)

			continue
		}

		require.NoError(t, qErr, "%s: %s", tt.name, got)
		assert.Equal(t, tt.want, got, tt.name)
	}

	names, err := h.Backend.Databases("srv")
	require.NoError(t, err)
	assert.Equal(t, []string{dbName}, names)

	_, err = h.Backend.Ensure("srv", azuresql.Spec{AdminLogin: login, AdminPassword: second})
	require.NoError(t, err)

	_, err = sqlcmd(t, id, login, first, "master", "SELECT 1")
	require.Error(t, err, "the old password stops working")

	got, err := sqlcmd(t, id, login, second, "master", "SELECT 1")
	require.NoError(t, err, got)

	require.NoError(t, h.Backend.DropDatabase("srv", dbName))

	names, err = h.Backend.Databases("srv")
	require.NoError(t, err)
	assert.Empty(t, names)

	require.NoError(t, h.Backend.Delete("srv"))

	gone, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--filter", "id="+id).Output()
	require.NoError(t, err)
	assert.Empty(t, strings.TrimSpace(string(gone)))
}
