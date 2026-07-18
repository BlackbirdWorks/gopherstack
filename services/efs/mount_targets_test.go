package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestDescribeMountTargets_Pagination verifies MaxItems/Marker pagination.
func TestDescribeMountTargets_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numMTs    int
		maxItems  int
		wantFirst int
		wantNext  bool
	}{
		{
			name:      "fits_in_one_page",
			numMTs:    2,
			maxItems:  5,
			wantFirst: 2,
			wantNext:  false,
		},
		{
			name:      "spans_multiple_pages",
			numMTs:    4,
			maxItems:  2,
			wantFirst: 2,
			wantNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-mt-page-"+tt.name))
			require.NoError(t, err)

			for i := range tt.numMTs {
				_, mtErr := b.CreateMountTarget(
					context.Background(),
					mtReq(fs.FileSystemID, "sn-"+string(rune('a'+i))),
				)
				require.NoError(t, mtErr)
			}

			list, nextMarker, err := b.DescribeMountTargets(
				context.Background(),
				fs.FileSystemID,
				"",
				"",
				tt.maxItems,
			)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantFirst)

			if tt.wantNext {
				assert.NotEmpty(t, nextMarker)
			} else {
				assert.Empty(t, nextMarker)
			}
		})
	}
}

// TestDescribeMountTargets_AccessPointIdFilter verifies the backend
// DescribeAccessPoints can be used to resolve an access point to its file system,
// enabling the AccessPointId filter in the handler layer.
func TestDescribeMountTargets_AccessPointIdFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		apExists bool
	}{
		{name: "existing_access_point_resolves_to_fs", apExists: true},
		{name: "missing_access_point_returns_not_found", apExists: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, createErr := b.CreateFileSystem(context.Background(), fsReq("ap-resolve-"+tt.name))
			require.NoError(t, createErr)

			if tt.apExists {
				ap, apErr := b.CreateAccessPoint(context.Background(), apReq(fs.FileSystemID))
				require.NoError(t, apErr)

				// Access point should resolve to the same file system.
				aps, _, descErr := b.DescribeAccessPoints(context.Background(), "", ap.AccessPointID, "", 1)
				require.NoError(t, descErr)
				require.Len(t, aps, 1)
				assert.Equal(t, fs.FileSystemID, aps[0].FileSystemID)
			} else {
				_, _, descErr := b.DescribeAccessPoints(context.Background(), "", "fsap-missing", "", 1)
				require.ErrorIs(t, descErr, efs.ErrAccessPointNotFound)
			}
		})
	}
}
