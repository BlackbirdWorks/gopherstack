package gcpfunctions

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func zipOf(t *testing.T, name string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	require.NoError(t, err)

	_, err = w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestExtractArchiveEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entry   string
		wantErr bool
	}{
		{"regular", "pkg/main.py", false},
		{"dotdot", "../evil.py", true},
		{"nested dotdot", "a/../../evil.py", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			dir := filepath.Join(root, "dest")
			require.NoError(t, os.MkdirAll(dir, 0o750))

			err := extractArchive(zipOf(t, tt.entry), dir)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsafePath)
				assert.NoFileExists(t, filepath.Join(root, "evil.py"))

				return
			}

			require.NoError(t, err)
			assert.FileExists(t, filepath.Join(dir, "pkg", "main.py"))
		})
	}
}
