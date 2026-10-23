package codebuild

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceTreeConcurrent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		writers int
	}{
		{name: "single", writers: 1},
		{name: "many", writers: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			dst := filepath.Join(root, "cache")

			srcs := make([]string, tt.writers)
			for i := range srcs {
				srcs[i] = filepath.Join(root, "src", string(rune('a'+i)))
				require.NoError(t, os.MkdirAll(srcs[i], 0o750))
				require.NoError(t, os.WriteFile(filepath.Join(srcs[i], "f"), []byte("x"), 0o600))
			}

			errs := make(chan error, tt.writers)

			var wg sync.WaitGroup

			for _, src := range srcs {
				wg.Go(func() { errs <- replaceTree(src, dst) })
			}

			wg.Wait()
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}

			assert.FileExists(t, filepath.Join(dst, "f"))

			entries, err := os.ReadDir(root)
			require.NoError(t, err)

			for _, e := range entries {
				assert.NotContains(t, e.Name(), ".tmp")
			}
		})
	}
}
