package gcpsql

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerReadOnlyMode(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker engine test skipped in short mode")
	}

	tests := []struct {
		name    string
		version string
		db      string
	}{
		{"postgres", "POSTGRES_16", "postgres"},
		{"mysql", "MYSQL_8_4", "mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newDockerLauncher(slog.Default(), nil, 3*time.Minute)
			t.Cleanup(l.Close)

			eng, err := l.Launch(t.Context(), LaunchSpec{
				ID: "dquota-" + tt.name, Project: "p", Version: tt.version, Password: "pw",
				ServerID: 7, Persist: true, Fresh: true,
			})
			if err != nil {
				t.Skipf("container runtime unavailable: %v", err)
			}

			t.Cleanup(func() { purgeEngine(t.Context(), eng) })

			du, ok := eng.(DiskUser)
			require.True(t, ok)

			ex, ok := eng.(SQLExecutor)
			require.True(t, ok)

			used, err := du.DiskUsage(t.Context())
			require.NoError(t, err)
			assert.Positive(t, used, "the engine reports the size of its data")

			write := func() string {
				out, werr := ex.ExecuteSQL(t.Context(), ExecRequest{
					Database: tt.db, Password: "pw", MaxBytes: maxExecBytes,
					SQL: "CREATE TABLE IF NOT EXISTS quota_probe (id int)",
				})
				require.NoError(t, werr)

				return out.Failure
			}

			require.Empty(t, write(), "writes work before the disk is full")

			require.NoError(t, du.SetReadOnly(t.Context(), true))
			assert.NotEmpty(t, func() string {
				out, werr := ex.ExecuteSQL(t.Context(), ExecRequest{
					Database: tt.db, Password: "pw", MaxBytes: maxExecBytes, SQL: "INSERT INTO quota_probe VALUES (1)",
				})
				require.NoError(t, werr)

				return out.Failure
			}(), "writes fail while the disk is full")

			require.NoError(t, du.SetReadOnly(t.Context(), false))
			out, err := ex.ExecuteSQL(t.Context(), ExecRequest{
				Database: tt.db, Password: "pw", MaxBytes: maxExecBytes, SQL: "INSERT INTO quota_probe VALUES (1)",
			})
			require.NoError(t, err)
			assert.Empty(t, out.Failure, "writes resume")
		})
	}
}
