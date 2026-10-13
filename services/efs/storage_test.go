package efs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestResolveAccessPointMount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		root      *efs.RootDirectory
		name      string
		wantSub   string
		wantPerm  os.FileMode
		checkPerm bool
	}{
		{name: "default_root", wantSub: ""},
		{name: "slash_root", root: &efs.RootDirectory{Path: "/"}, wantSub: ""},
		{
			name: "creation_info", wantSub: "app/data", checkPerm: true, wantPerm: 0o750,
			root: &efs.RootDirectory{
				Path:         "/app/data",
				CreationInfo: &efs.CreationInfo{OwnerUID: 1000, OwnerGID: 1000, Permissions: "750"},
			},
		},
		{
			name: "traversal_contained", wantSub: "etc",
			root: &efs.RootDirectory{
				Path:         "/../../etc",
				CreationInfo: &efs.CreationInfo{Permissions: "755"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			storage := t.TempDir()
			b.SetStorageRoot(storage)

			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-mnt-"+tt.name))
			require.NoError(t, err)

			ap, err := b.CreateAccessPoint(context.Background(), efs.CreateAccessPointRequest{
				FileSystemID: fs.FileSystemID, RootDirectory: tt.root,
				PosixUser: &efs.PosixUser{UID: 1001, GID: 1002},
			})
			require.NoError(t, err)

			m, err := b.ResolveAccessPointMount(ap.AccessPointArn)
			require.NoError(t, err)

			fsDir := filepath.Join(storage, "us-east-1", fs.FileSystemID)
			assert.Equal(t, filepath.Join(fsDir, tt.wantSub), m.HostDir)
			assert.Equal(t, fs.FileSystemID, m.FileSystemID)
			assert.Equal(t, int64(1001), m.UID)
			assert.Equal(t, int64(1002), m.GID)

			info, err := os.Stat(m.HostDir)
			require.NoError(t, err)
			assert.True(t, info.IsDir())

			if tt.checkPerm {
				assert.Equal(t, tt.wantPerm, info.Mode().Perm())
			}

			require.NoError(t, os.WriteFile(filepath.Join(m.HostDir, "f"), []byte("x"), 0o600))

			again, err := b.ResolveAccessPointMount(ap.AccessPointArn)
			require.NoError(t, err)
			assert.Equal(t, m.HostDir, again.HostDir)
			assert.FileExists(t, filepath.Join(again.HostDir, "f"))
		})
	}
}

func TestResolveAccessPointMountErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name: "not_access_point", arn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-1",
			wantErr: efs.ErrInvalidAccessPoint,
		},
		{
			name:    "unknown",
			arn:     "arn:aws:elasticfilesystem:us-east-1:123456789012:access-point/fsap-0123456789abcdef0",
			wantErr: efs.ErrAccessPointNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			b.SetStorageRoot(t.TempDir())

			_, err := b.ResolveAccessPointMount(tt.arn)
			require.ErrorIs(t, err, tt.wantErr)
			require.ErrorIs(t, b.CheckMountTarget(tt.arn, []string{"subnet-1"}), tt.wantErr)
		})
	}
}

func TestDeleteFileSystemRemovesStorage(t *testing.T) {
	t.Parallel()

	b := newTestEFSBackend()
	storage := t.TempDir()
	b.SetStorageRoot(storage)

	fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-del-storage"))
	require.NoError(t, err)

	ap, err := b.CreateAccessPoint(context.Background(), apReq(fs.FileSystemID))
	require.NoError(t, err)

	m, err := b.ResolveAccessPointMount(ap.AccessPointArn)
	require.NoError(t, err)
	require.DirExists(t, m.HostDir)

	require.NoError(t, b.DeleteAccessPoint(context.Background(), ap.AccessPointID))
	require.NoError(t, b.DeleteFileSystem(context.Background(), fs.FileSystemID))
	assert.NoDirExists(t, m.HostDir)
}

func TestCheckMountTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolver *fakeEFSEC2Resolver
		name     string
		mtSubnet string
		fnSubnet string
		wantErr  bool
	}{
		{name: "same_subnet", mtSubnet: "subnet-a", fnSubnet: "subnet-a"},
		{name: "no_mount_target", fnSubnet: "subnet-a", wantErr: true},
		{name: "other_subnet_no_ec2", mtSubnet: "subnet-a", fnSubnet: "subnet-b", wantErr: true},
		{
			name: "same_vpc", mtSubnet: "subnet-a", fnSubnet: "subnet-b",
			resolver: &fakeEFSEC2Resolver{
				vpc: map[string]string{"subnet-a": "vpc-1", "subnet-b": "vpc-1"},
				az:  map[string]string{"subnet-a": "az-a", "subnet-b": "az-b"},
			},
		},
		{
			name: "different_vpc", mtSubnet: "subnet-a", fnSubnet: "subnet-b", wantErr: true,
			resolver: &fakeEFSEC2Resolver{
				vpc: map[string]string{"subnet-a": "vpc-1", "subnet-b": "vpc-2"},
				az:  map[string]string{"subnet-a": "az-a", "subnet-b": "az-b"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			if tt.resolver != nil {
				b.SetEC2Resolver(tt.resolver)
			}

			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-mt-"+tt.name))
			require.NoError(t, err)

			ap, err := b.CreateAccessPoint(context.Background(), apReq(fs.FileSystemID))
			require.NoError(t, err)

			if tt.mtSubnet != "" {
				_, err = b.CreateMountTarget(context.Background(), mtReq(fs.FileSystemID, tt.mtSubnet))
				require.NoError(t, err)
			}

			err = b.CheckMountTarget(ap.AccessPointArn, []string{tt.fnSubnet})
			if tt.wantErr {
				require.ErrorIs(t, err, efs.ErrNoMountTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}
