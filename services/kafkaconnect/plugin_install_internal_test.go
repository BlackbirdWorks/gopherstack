package kafkaconnect

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildZip(t *testing.T, names []string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, n := range names {
		w, err := zw.Create(n)
		require.NoError(t, err)

		_, err = w.Write([]byte("x"))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestUnzipPluginRejectsEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries []string
		wantErr bool
	}{
		{name: "ok", entries: []string{"lib/a.jar"}},
		{name: "parent", entries: []string{"../evil"}, wantErr: true},
		{name: "nested parent", entries: []string{"a/../../evil"}, wantErr: true},
		{name: "dots in name", entries: []string{"a..b/x"}, wantErr: true},
		{name: "absolute", entries: []string{"/abs/evil"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			dst := filepath.Join(root, "plugin")
			require.NoError(t, os.MkdirAll(dst, 0o750))

			err := unzipPlugin(buildZip(t, tc.entries), dst)
			if tc.wantErr {
				require.ErrorIs(t, err, errZipEscape)
				assert.NoFileExists(t, filepath.Join(root, "evil"))

				return
			}

			require.NoError(t, err)
		})
	}
}
