package efs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveFileSystemStorageConfined(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		fsID       string
		wantRemain bool
	}{
		{name: "inside root removed", region: "us-east-1", fsID: "fs-1", wantRemain: false},
		{name: "dotdot region kept", region: "..", fsID: "outside", wantRemain: true},
		{name: "dotdot fsid kept", region: "us-east-1", fsID: "../../outside", wantRemain: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := t.TempDir()
			root := filepath.Join(base, "root")
			require.NoError(t, os.MkdirAll(filepath.Join(root, "us-east-1", "fs-1"), 0o755))
			require.NoError(t, os.MkdirAll(filepath.Join(base, "outside"), 0o755))

			b := NewInMemoryBackend("123456789012", "us-east-1")
			b.storageRoot = root

			b.removeFileSystemStorageLocked(tt.region, tt.fsID)

			target := filepath.Join(root, tt.region, tt.fsID)
			if tt.wantRemain {
				target = filepath.Join(base, "outside")
			}

			_, err := os.Stat(target)
			if tt.wantRemain {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}
