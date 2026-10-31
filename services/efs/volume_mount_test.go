package efs_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestResolveVolumeMount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		root      string
		wantSub   string
		useAP     bool
		apOtherFS bool
		unknownFS bool
		unknownAP bool
		wantPosix bool
	}{
		{name: "fs_root", wantSub: ""},
		{name: "fs_subdir", root: "/data/x", wantSub: "data/x"},
		{name: "fs_traversal_contained", root: "/../../etc", wantSub: "etc"},
		{name: "unknown_fs", unknownFS: true, wantErr: efs.ErrNotFound},
		{name: "ap_root_dir", useAP: true, wantSub: "ap", wantPosix: true},
		{name: "ap_root_slash", useAP: true, root: "/", wantSub: "ap", wantPosix: true},
		{name: "ap_rejects_root_dir", useAP: true, root: "/x", wantErr: efs.ErrRootDirectoryWithAccessPoint},
		{name: "ap_unknown", useAP: true, unknownAP: true, wantErr: efs.ErrAccessPointNotFound},
		{name: "ap_other_fs", useAP: true, apOtherFS: true, wantErr: efs.ErrAccessPointFileSystemMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			storage := t.TempDir()
			b.SetStorageRoot(storage)

			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-vol-"+tt.name))
			require.NoError(t, err)

			other, err := b.CreateFileSystem(context.Background(), fsReq("tok-vol-other-"+tt.name))
			require.NoError(t, err)

			apFS := fs.FileSystemID
			if tt.apOtherFS {
				apFS = other.FileSystemID
			}

			ap, err := b.CreateAccessPoint(context.Background(), efs.CreateAccessPointRequest{
				FileSystemID:  apFS,
				RootDirectory: &efs.RootDirectory{Path: "/ap", CreationInfo: &efs.CreationInfo{Permissions: "777"}},
				PosixUser:     &efs.PosixUser{UID: 1001, GID: 1002},
			})
			require.NoError(t, err)

			fsID, apID := fs.FileSystemID, ""
			if tt.useAP {
				apID = ap.AccessPointID
			}

			if tt.unknownFS {
				fsID = "fs-0000000000000000"
			}

			if tt.unknownAP {
				apID = "fsap-0000000000000000"
			}

			m, err := b.ResolveVolumeMount("us-east-1", fsID, apID, tt.root)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, filepath.Join(storage, "us-east-1", fs.FileSystemID, tt.wantSub), m.HostDir)
			assert.Equal(t, tt.wantPosix, m.HasPosixUser)
		})
	}
}
