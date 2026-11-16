package ocifilestorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeAt(t *testing.T, dir, name, content string, mtime time.Time) {
	t.Helper()

	full := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	require.NoError(t, os.Chtimes(full, mtime, mtime))
}

func TestSyncTreeTransfersOnlyDifferences(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)

	tests := []struct {
		dstMtime  time.Time
		src       map[string]string
		dst       map[string]string
		wantFiles map[string]string
		name      string
		wantBytes int64
	}{
		{
			name: "unchanged is skipped", src: map[string]string{"a": "AAAA"}, dst: map[string]string{"a": "xxxx"},
			dstMtime: t0, wantBytes: 0, wantFiles: map[string]string{"a": "xxxx"},
		},
		{
			name: "newer mtime recopied", src: map[string]string{"a": "AAAA"}, dst: map[string]string{"a": "xxxx"},
			dstMtime: t0.Add(-time.Hour), wantBytes: 4, wantFiles: map[string]string{"a": "AAAA"},
		},
		{
			name: "size change recopied", src: map[string]string{"a": "AAAAAA"}, dst: map[string]string{"a": "xxxx"},
			dstMtime: t0, wantBytes: 6, wantFiles: map[string]string{"a": "AAAAAA"},
		},
		{
			name: "removed and added", src: map[string]string{"d/new": "NN"},
			dst:      map[string]string{"old": "o", "e/x": "y"},
			dstMtime: t0, wantBytes: 2, wantFiles: map[string]string{"d/new": "NN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, dst := t.TempDir(), t.TempDir()

			for n, c := range tt.src {
				writeAt(t, src, n, c, t0)
			}

			for n, c := range tt.dst {
				writeAt(t, dst, n, c, tt.dstMtime)
			}

			got, err := syncTree(src, dst)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, got)

			files := map[string]string{}

			require.NoError(t, filepath.WalkDir(dst, func(p string, d os.DirEntry, err error) error {
				if err == nil && d.Type().IsRegular() {
					b, rerr := os.ReadFile(p)
					rel, _ := filepath.Rel(dst, p)
					files[rel] = string(b)

					return rerr
				}

				return err
			}))
			assert.Equal(t, tt.wantFiles, files)
		})
	}
}
