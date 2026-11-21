package pgengine_test

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
)

const dockerPGImage = "postgres:16"

func dockerPGServer(t *testing.T) (*pgengine.Server, *pgx.Conn) {
	t.Helper()

	if testing.Short() {
		t.Skip("docker engine tests are skipped under -short")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", dockerPGImage).Run(); err != nil {
		t.Skipf("image %s is not present locally", dockerPGImage)
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	e, err := pgengine.New(pgengine.Config{Runtime: rt, Prefix: "pgparamtest", StartTimeout: 3 * time.Minute})
	require.NoError(t, err)

	const pw = "pw-1234567890abcdef"

	srv, err := e.Launch(t.Context(), pgengine.Spec{ID: "params", Image: dockerPGImage, Password: pw})
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close(t.Context(), true) })

	h, p := srv.Endpoint()
	conn, err := pgx.Connect(
		t.Context(),
		"postgres://"+pgengine.Superuser+":"+pw+"@"+h+":"+itoa(p)+"/postgres?sslmode=disable",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(t.Context()) })

	return srv, conn
}

func itoa(n int) string {
	var b []byte

	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}

	return string(b)
}

func TestSetParameterValuesRoundTripInPostgres(t *testing.T) {
	t.Parallel()

	srv, conn := dockerPGServer(t)

	values := []string{
		"plain",
		"it's",
		"''",
		"'; DROP DATABASE postgres; --",
		"$$ x $$",
		"$a$ x $a$",
		"E'x'",
		"U&'d!0061t!+000061' UESCAPE '!'",
		"a b  c",
		"é ü 日本",
		"%q %u %d",
	}

	for _, v := range values {
		require.NoError(t, srv.SetParameter(t.Context(), "log_line_prefix", v, false), v)
		require.NoError(t, srv.ReloadConf(t.Context()))

		var got string

		require.Eventually(t, func() bool {
			if err := conn.QueryRow(t.Context(), "SELECT current_setting('log_line_prefix')").Scan(&got); err != nil {
				return false
			}

			return got == v
		}, 20*time.Second, 100*time.Millisecond, "value %q was %q", v, got)
	}

	var n int

	require.NoError(
		t,
		conn.QueryRow(t.Context(), "SELECT count(*) FROM pg_database WHERE datname = 'postgres'").Scan(&n),
	)
	assert.Equal(t, 1, n)

	require.NoError(t, srv.SetParameter(t.Context(), "log_line_prefix", "", true))
	assert.NotEmpty(t, strings.TrimSpace("x"))
}
