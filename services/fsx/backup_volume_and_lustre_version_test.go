package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBackup_VolumeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		volume  string // "real", "unknown", or "mismatch"
		wantErr string
	}{
		{name: "ontap volume backup", volume: "real"},
		{name: "unknown volume", volume: "unknown", wantErr: "VolumeNotFound"},
		{name: "file system mismatch", volume: "mismatch", wantErr: "BadRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			vol := createTestOntapVolume(t, client, "bk-vol")
			other := createTestOntapFS(t, client)

			in := &fsxsdk.CreateBackupInput{VolumeId: vol.Volume.VolumeId}

			switch tt.volume {
			case "unknown":
				in.VolumeId = aws.String("fsvol-0123456789abcdef0")
			case "mismatch":
				in.FileSystemId = other.FileSystem.FileSystemId
			}

			out, err := client.CreateBackup(t.Context(), in)
			if tt.wantErr != "" {
				require.Error(t, err)

				var apiErr interface{ ErrorCode() string }
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantErr, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Backup.Volume)
			assert.Equal(t, aws.ToString(vol.Volume.VolumeId), aws.ToString(out.Backup.Volume.VolumeId))
			assert.Equal(t, aws.ToString(vol.Volume.FileSystemId), aws.ToString(out.Backup.FileSystem.FileSystemId))

			plain, err := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
				FileSystemId: other.FileSystem.FileSystemId,
			})
			require.NoError(t, err)
			assert.Nil(t, plain.Backup.Volume)

			desc, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{
				Filters: []types.Filter{{
					Name:   types.FilterNameVolumeId,
					Values: []string{aws.ToString(vol.Volume.VolumeId)},
				}},
			})
			require.NoError(t, err)
			require.Len(t, desc.Backups, 1)
			assert.Equal(t, aws.ToString(out.Backup.BackupId), aws.ToString(desc.Backups[0].BackupId))
		})
	}
}

func TestCreateFileSystem_LustreTypeVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lustre  *types.CreateFileSystemLustreConfiguration
		name    string
		version string
		want    string
		wantErr bool
	}{
		{name: "explicit 2.15", version: "2.15", want: "2.15"},
		{name: "explicit 2.12", version: "2.12", want: "2.12"},
		{name: "omitted", version: "", want: "2.10"},
		{
			name: "persistent2 default", want: "2.12",
			lustre: &types.CreateFileSystemLustreConfiguration{DeploymentType: types.LustreDeploymentTypePersistent2},
		},
		{
			name: "persistent2 metadata mode", want: "2.15",
			lustre: &types.CreateFileSystemLustreConfiguration{
				DeploymentType: types.LustreDeploymentTypePersistent2,
				MetadataConfiguration: &types.CreateFileSystemLustreMetadataConfiguration{
					Mode: types.MetadataConfigurationModeAutomatic,
				},
			},
		},
		{name: "unsupported", version: "9.9", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))

			in := &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeLustre,
				StorageCapacity: aws.Int32(1200),
				SubnetIds:       []string{"subnet-0123abcd"},

				LustreConfiguration: tt.lustre,
			}
			if tt.version != "" {
				in.FileSystemTypeVersion = aws.String(tt.version)
			}

			out, err := client.CreateFileSystem(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.FileSystem.FileSystemTypeVersion))

			desc, err := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
				FileSystemIds: []string{aws.ToString(out.FileSystem.FileSystemId)},
			})
			require.NoError(t, err)
			require.Len(t, desc.FileSystems, 1)
			assert.Equal(t, tt.want, aws.ToString(desc.FileSystems[0].FileSystemTypeVersion))
		})
	}
}

func TestCreateStorageVirtualMachine_SubtypeServerDerived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want types.StorageVirtualMachineSubtype
	}{
		{name: "new svm is default", want: types.StorageVirtualMachineSubtypeDefault},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			fsOut := createTestOntapFS(t, client)

			out, err := client.CreateStorageVirtualMachine(t.Context(), &fsxsdk.CreateStorageVirtualMachineInput{
				FileSystemId: fsOut.FileSystem.FileSystemId,
				Name:         aws.String("svm"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.StorageVirtualMachine.Subtype)
		})
	}
}
