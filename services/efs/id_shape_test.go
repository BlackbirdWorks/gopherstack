package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestGeneratedIDShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mint    func(t *testing.T, b *efs.InMemoryBackend) string
		name    string
		pattern string
	}{
		{
			name:    "file system",
			pattern: `^fs-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *efs.InMemoryBackend) string {
				t.Helper()

				fs, err := b.CreateFileSystem(context.Background(), efs.CreateFileSystemRequest{CreationToken: "a"})
				require.NoError(t, err)

				return fs.FileSystemID
			},
		},
		{
			name:    "access point",
			pattern: `^fsap-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *efs.InMemoryBackend) string {
				t.Helper()

				fs, err := b.CreateFileSystem(context.Background(), efs.CreateFileSystemRequest{CreationToken: "b"})
				require.NoError(t, err)

				ap, err := b.CreateAccessPoint(
					context.Background(), efs.CreateAccessPointRequest{FileSystemID: fs.FileSystemID})
				require.NoError(t, err)

				return ap.AccessPointID
			},
		},
		{
			name:    "mount target",
			pattern: `^fsmt-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *efs.InMemoryBackend) string {
				t.Helper()

				return mountTarget(t, b, "c").MountTargetID
			},
		},
		{
			name:    "mount target eni",
			pattern: `^eni-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *efs.InMemoryBackend) string {
				t.Helper()

				return mountTarget(t, b, "d").NetworkInterfaceID
			},
		},
		{
			name:    "replica file system",
			pattern: `^fs-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *efs.InMemoryBackend) string {
				t.Helper()

				fs, err := b.CreateFileSystem(context.Background(), efs.CreateFileSystemRequest{CreationToken: "e"})
				require.NoError(t, err)

				rc, err := b.CreateReplicationConfiguration(
					context.Background(), fs.FileSystemID, []efs.ReplicationDestination{{Region: "us-west-2"}})
				require.NoError(t, err)

				return rc.Destinations[0].FileSystemID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Regexp(t, tt.pattern, tt.mint(t, newTestEFSBackend()))
		})
	}
}

func mountTarget(t *testing.T, b *efs.InMemoryBackend, token string) *efs.MountTarget {
	t.Helper()

	fs, err := b.CreateFileSystem(context.Background(), efs.CreateFileSystemRequest{CreationToken: token})
	require.NoError(t, err)

	mt, err := b.CreateMountTarget(context.Background(), efs.CreateMountTargetRequest{
		FileSystemID: fs.FileSystemID, SubnetID: "subnet-a",
	})
	require.NoError(t, err)

	return mt
}
