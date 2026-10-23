package codebuild

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tarGzOf(t *testing.T, name, body string, mode int64) []byte {
	t.Helper()

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	hdr := &tar.Header{Name: name, Mode: mode, Size: int64(len(body)), Typeflag: tar.TypeReg}
	require.NoError(t, tw.WriteHeader(hdr))

	_, err := tw.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func TestUntarGzEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entry   string
		wantErr bool
	}{
		{"regular", "a/b.txt", false},
		{"dotdot", "../evil.txt", true},
		{"nested dotdot", "a/../../evil.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			dir := filepath.Join(root, "dest")
			require.NoError(t, os.MkdirAll(dir, dirPerm))

			err := untarGz(tarGzOf(t, tt.entry, "hi", 0o644), dir)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsafePath)
				assert.NoFileExists(t, filepath.Join(root, "evil.txt"))

				return
			}

			require.NoError(t, err)
			assert.FileExists(t, filepath.Join(dir, "a", "b.txt"))
		})
	}
}

func TestUntarGzKeepsExecBit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, untarGz(tarGzOf(t, "run.sh", "x", 0o755), dir))

	st, err := os.Stat(filepath.Join(dir, "run.sh"))
	require.NoError(t, err)
	assert.NotZero(t, st.Mode().Perm()&0o100)
}
