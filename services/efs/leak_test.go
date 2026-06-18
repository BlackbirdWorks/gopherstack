package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestEFSAccessPointClientTokenIndex_ClearedOnDelete verifies that the
// accessPointsByClientToken reverse-index entry is removed when an access
// point is deleted, preventing the index from growing without bound as access
// points are created and deleted over the lifetime of a backend.
//
// Note: parity.md §D listed this service under "h.archiveData" (Glacier's
// field — a mislabelling). The actual bounded-growth concern for EFS is the
// client-token idempotency index; this test confirms it is properly evicted.
func TestEFSAccessPointClientTokenIndex_ClearedOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		n    int
	}{
		{name: "single access point", n: 1},
		{name: "many access points", n: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := efs.NewInMemoryBackend("000000000000", "us-east-1")

			// Create a file system to attach access points to.
			fs, err := b.CreateFileSystem(ctx, efs.CreateFileSystemRequest{
				Tags: map[string]string{"Name": "test-fs"},
			})
			require.NoError(t, err)

			ids := make([]string, tc.n)
			for i := range tc.n {
				ap, createErr := b.CreateAccessPoint(ctx, efs.CreateAccessPointRequest{
					ClientToken:  makeToken(i),
					FileSystemID: fs.FileSystemID,
				})
				require.NoError(t, createErr)
				ids[i] = ap.AccessPointID
			}

			beforeCount := efs.AccessPointCount(b)
			require.Equal(t, tc.n, beforeCount, "all access points should be present before delete")

			// Delete every access point.
			for _, id := range ids {
				require.NoError(t, b.DeleteAccessPoint(ctx, id))
			}

			assert.Equal(t, 0, efs.AccessPointCount(b),
				"all access point entries must be removed after delete")
		})
	}
}

// makeToken returns a unique client token string for use in tests.
func makeToken(i int) string {
	const digits = "0123456789abcdef"
	s := make([]byte, 8)
	v := i + 1
	for j := range s {
		s[len(s)-1-j] = digits[v%16]
		v /= 16
	}

	return "tok-" + string(s)
}
