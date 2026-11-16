package azuresql_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

// fileExec emulates the container shell and sqlcmd over an in-memory file system.
type fileExec struct {
	files map[string][]byte
	sql   []string
	// restored is the backup file content at the moment RESTORE DATABASE ran.
	restored []byte
	// corrupt flips the first uploaded chunk bytes; failRestore makes RESTORE DATABASE exit non-zero.
	corrupt, failRestore bool
	mu                   sync.Mutex
}

func (f *fileExec) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	script, args := cmd[2], cmd[4:]

	switch {
	case strings.Contains(script, "sqlcmd"):
		f.sql = append(f.sql, args[0])

		if strings.Contains(args[0], "RESTORE DATABASE") {
			if f.failRestore {
				return container.ExecResult{ExitCode: 1, Stderr: "bad backup"}, nil
			}

			f.restored = f.files[between(args[0], "N'/var", "'")]
		}

		if strings.Contains(args[0], "BACKUP DATABASE") {
			f.files[between(args[0], "N'/var", "'")] = []byte("backup-bytes")
		}
	case strings.HasPrefix(script, "stat"):
		return container.ExecResult{Stdout: strconv.Itoa(len(f.files[args[0]])) + "\n"}, nil
	case strings.HasPrefix(script, "base64 -w0"):
		return container.ExecResult{Stdout: base64.StdEncoding.EncodeToString(f.files[args[0]])}, nil
	case strings.HasPrefix(script, ": >"):
		f.files[args[0]] = nil
	case strings.HasPrefix(script, "printf"):
		chunk := args[1]
		if f.corrupt && len(f.files[args[0]]) == 0 {
			chunk = "AAAA" + chunk[4:]
		}

		f.files[args[0]] = append(f.files[args[0]], chunk...)
	case strings.HasPrefix(script, "base64 -d"):
		dec, err := base64.StdEncoding.DecodeString(string(f.files[args[0]]))
		if err != nil {
			return container.ExecResult{ExitCode: 1}, nil
		}

		f.files[args[1]] = dec
		sum := sha256.Sum256(dec)

		return container.ExecResult{Stdout: hex.EncodeToString(sum[:]) + "  " + args[1] + "\n"}, nil
	case strings.HasPrefix(script, "rm"):
		delete(f.files, args[0])
		delete(f.files, args[1])
	}

	return container.ExecResult{}, nil
}

func between(s, after, until string) string {
	i := strings.Index(s, after)
	if i < 0 {
		return ""
	}

	rest := s[i+len("N'"):]
	j := strings.Index(rest, until)

	return rest[:j]
}

func TestDockerEngineDataRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxBytes int64
		wantErr  bool
	}{
		{name: "within_cap", maxBytes: 1024},
		{name: "over_cap", maxBytes: 4, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := &fileExec{files: map[string][]byte{}}
			eng := azuresql.NewDockerEngineForTest(fx)

			data, err := eng.BackupDatabase(t.Context(), "app", tt.maxBytes)
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, fx.files, "scratch files must be removed")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []byte("backup-bytes"), data)
			assert.Empty(t, fx.files, "scratch files must be removed")

			big := bytes.Repeat([]byte("x"), 200<<10)
			require.NoError(t, eng.RestoreDatabase(t.Context(), "app", big))
			assert.Empty(t, fx.files)
			assert.Equal(t, big, fx.restored)
			assert.Contains(t, fx.sql[len(fx.sql)-1], "RESTORE DATABASE")
			assert.Contains(t, fx.sql[len(fx.sql)-1], "WITH REPLACE")
		})
	}
}

func TestDockerEngineRestoreFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		corrupt     bool
		failRestore bool
		oversize    bool
		wantMulti   bool
		wantRestore bool
	}{
		{name: "corrupt_upload", corrupt: true},
		{name: "restore_fails", failRestore: true, wantMulti: true},
		{name: "oversize", oversize: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := &fileExec{files: map[string][]byte{}, corrupt: tt.corrupt, failRestore: tt.failRestore}
			eng := azuresql.NewDockerEngineForTest(fx)
			data := bytes.Repeat([]byte("x"), 1024)

			if tt.oversize {
				data = make([]byte, 64<<20+1)
			}

			require.Error(t, eng.RestoreDatabase(t.Context(), "app", data))
			assert.Empty(t, fx.files)

			var multi, restore bool

			for _, q := range fx.sql {
				multi = multi || strings.Contains(q, "SET MULTI_USER'; IF DB_ID")
				restore = restore || strings.Contains(q, "RESTORE DATABASE")
			}

			assert.Equal(t, tt.wantMulti, multi, "multi-user release")
			assert.Equal(t, tt.wantMulti, restore, "restore attempted")
		})
	}
}
