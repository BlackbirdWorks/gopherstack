package mysqlengine_test

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

const dockerMySQLImage = "mysql:8.4"

func dockerMySQLServer(t *testing.T) *mysqlengine.Server {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine tests are skipped under -short")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", dockerMySQLImage).Run(); err != nil {
		t.Skipf("image %s is not present locally", dockerMySQLImage)
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	e, err := mysqlengine.New(mysqlengine.Config{Runtime: rt, Prefix: "mysqlvartest", StartTimeout: 3 * time.Minute})
	require.NoError(t, err)

	srv, err := e.Launch(
		t.Context(),
		mysqlengine.Spec{ID: "vars", Image: dockerMySQLImage, RootPassword: "pw-1234567890abcdef"},
	)
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close(t.Context(), true) })

	return srv
}

func TestSetVariableValuesRoundTripInMySQL(t *testing.T) {
	t.Parallel()

	srv := dockerMySQLServer(t)

	values := []string{
		"plain",
		"it's",
		"''",
		`a\b`,
		`a\'b`,
		`"; DROP DATABASE mysql; -- `,
		"x\ny",
		"é ü 日本",
		"0x41",
		"1e3",
		"007",
	}

	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES"} {
		require.NoError(t, srv.SetVariable(t.Context(), "sql_mode", mode, false, false))

		for _, v := range values {
			require.NoError(t, srv.SetVariable(t.Context(), "init_connect", v, false, false), "%q in %q", v, mode)

			rows, err := srv.Query(t.Context(), "", "SELECT @@global.init_connect")
			require.NoError(t, err)
			require.Len(t, rows, 2)
			assert.Equal(t, v, rows[1][0], "mode %q", mode)
		}
	}

	rows, err := srv.Query(
		t.Context(),
		"",
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'mysql'",
	)
	require.NoError(t, err)
	assert.Equal(t, "1", rows[1][0])
}
