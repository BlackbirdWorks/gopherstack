package transcribe

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnzipSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entry   string
		wantErr bool
	}{
		{name: "nested path", entry: "model/am/final.mdl"},
		{name: "dots inside a name", entry: "model/a..b/final.mdl"},
		{name: "parent", entry: "../x", wantErr: true},
		{name: "absolute", entry: "/abs", wantErr: true},
		{name: "inner escape", entry: "a/../../x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			archive := filepath.Join(dir, "m.zip")

			f, err := os.Create(archive)
			require.NoError(t, err)

			zw := zip.NewWriter(f)
			w, err := zw.Create(tt.entry)
			require.NoError(t, err)

			_, err = w.Write([]byte("data"))
			require.NoError(t, err)
			require.NoError(t, zw.Close())
			require.NoError(t, f.Close())

			dest := filepath.Join(dir, "out")
			require.NoError(t, os.Mkdir(dest, 0o750))

			err = unzipSafe(archive, dest)
			if tt.wantErr {
				require.ErrorIs(t, err, errVoskArchive)
				assert.NoFileExists(t, filepath.Join(dest, tt.entry))

				return
			}

			require.NoError(t, err)
			assert.FileExists(t, filepath.Join(dest, tt.entry))
		})
	}
}
