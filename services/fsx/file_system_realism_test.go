package fsx_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func newRealismClient(t *testing.T) (*fsx.InMemoryBackend, *fsxsdk.Client) {
	t.Helper()

	b := fsx.NewInMemoryBackend("000000000000", "us-east-1")

	return b, newTestFSxClient(t, fsx.NewHandler(b))
}

func TestCreateFileSystem_LustreValidation(t *testing.T) {
	t.Parallel()

	lustre := func(dep types.LustreDeploymentType, throughput *int32) *types.CreateFileSystemLustreConfiguration {
		return &types.CreateFileSystemLustreConfiguration{DeploymentType: dep, PerUnitStorageThroughput: throughput}
	}

	tests := []struct {
		cfg      *types.CreateFileSystemLustreConfiguration
		name     string
		capacity int32
		wantErr  bool
	}{
		{name: "persistent1_ok", cfg: lustre("PERSISTENT_1", aws.Int32(50)), capacity: 1200},
		{name: "persistent1_bad_throughput", cfg: lustre("PERSISTENT_1", aws.Int32(77)), capacity: 1200, wantErr: true},
		{name: "persistent1_missing_throughput", cfg: lustre("PERSISTENT_1", nil), capacity: 1200, wantErr: true},
		{name: "persistent2_ok", cfg: lustre("PERSISTENT_2", aws.Int32(250)), capacity: 2400},
		{name: "persistent2_wrong_family", cfg: lustre("PERSISTENT_2", aws.Int32(50)), capacity: 1200, wantErr: true},
		{name: "scratch2_throughput_rejected", cfg: lustre("SCRATCH_2", aws.Int32(50)), capacity: 1200, wantErr: true},
		{name: "scratch2_bad_capacity", cfg: lustre("SCRATCH_2", nil), capacity: 1300, wantErr: true},
		{name: "scratch2_multiple", cfg: lustre("SCRATCH_2", nil), capacity: 4800},
		{name: "scratch1_3600", cfg: lustre("SCRATCH_1", nil), capacity: 3600},
		{name: "scratch1_4800_rejected", cfg: lustre("SCRATCH_1", nil), capacity: 4800, wantErr: true},
		{name: "bad_compression", cfg: &types.CreateFileSystemLustreConfiguration{
			DeploymentType: "SCRATCH_2", DataCompressionType: "GZIP",
		}, capacity: 1200, wantErr: true},
		{name: "bad_maintenance_window", cfg: &types.CreateFileSystemLustreConfiguration{
			DeploymentType: "SCRATCH_2", WeeklyMaintenanceStartTime: aws.String("9:00:00"),
		}, capacity: 1200, wantErr: true},
		{name: "unknown_deployment", cfg: lustre("SCRATCH_9", nil), capacity: 1200, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealismClient(t)
			_, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:      types.FileSystemTypeLustre,
				SubnetIds:           []string{"subnet-0123abcd"},
				StorageCapacity:     aws.Int32(tt.capacity),
				LustreConfiguration: tt.cfg,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateFileSystem_LustreConfigEchoed(t *testing.T) {
	t.Parallel()

	_, client := newRealismClient(t)

	created, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
		FileSystemType:  types.FileSystemTypeLustre,
		SubnetIds:       []string{"subnet-0123abcd"},
		StorageCapacity: aws.Int32(1200),
		LustreConfiguration: &types.CreateFileSystemLustreConfiguration{
			DeploymentType:               types.LustreDeploymentTypePersistent1,
			PerUnitStorageThroughput:     aws.Int32(100),
			DataCompressionType:          types.DataCompressionTypeLz4,
			WeeklyMaintenanceStartTime:   aws.String("2:03:30"),
			AutomaticBackupRetentionDays: aws.Int32(7),
			CopyTagsToBackups:            aws.Bool(true),
			ImportPath:                   aws.String("s3://bucket"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
		FileSystemIds: []string{aws.ToString(created.FileSystem.FileSystemId)},
	})
	require.NoError(t, err)
	require.Len(t, out.FileSystems, 1)

	cfg := out.FileSystems[0].LustreConfiguration
	require.NotNil(t, cfg)
	assert.EqualValues(t, 100, aws.ToInt32(cfg.PerUnitStorageThroughput))
	assert.Equal(t, types.DataCompressionTypeLz4, cfg.DataCompressionType)
	assert.Equal(t, "2:03:30", aws.ToString(cfg.WeeklyMaintenanceStartTime))
	assert.EqualValues(t, 7, aws.ToInt32(cfg.AutomaticBackupRetentionDays))
	assert.True(t, aws.ToBool(cfg.CopyTagsToBackups))
	require.NotNil(t, cfg.DataRepositoryConfiguration)
	assert.Equal(t, "s3://bucket", aws.ToString(cfg.DataRepositoryConfiguration.ImportPath))

	updated, err := client.UpdateFileSystem(t.Context(), &fsxsdk.UpdateFileSystemInput{
		FileSystemId: created.FileSystem.FileSystemId,
		LustreConfiguration: &types.UpdateFileSystemLustreConfiguration{
			DataCompressionType:        types.DataCompressionTypeNone,
			WeeklyMaintenanceStartTime: aws.String("3:04:00"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.DataCompressionTypeNone, updated.FileSystem.LustreConfiguration.DataCompressionType)
	assert.Equal(t, "3:04:00", aws.ToString(updated.FileSystem.LustreConfiguration.WeeklyMaintenanceStartTime))
}

func TestCreateFileSystem_StorageBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *fsxsdk.CreateFileSystemInput
		name    string
		wantErr bool
	}{
		{name: "windows_over_max", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeWindows, StorageCapacity: aws.Int32(65537),
			WindowsConfiguration: &types.CreateFileSystemWindowsConfiguration{ThroughputCapacity: aws.Int32(8)},
		}},
		{name: "windows_hdd_below_2000", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType:  types.FileSystemTypeWindows,
			StorageCapacity: aws.Int32(1000),
			StorageType:     types.StorageTypeHdd,
			WindowsConfiguration: &types.CreateFileSystemWindowsConfiguration{
				ThroughputCapacity: aws.Int32(8), DeploymentType: types.WindowsDeploymentTypeSingleAz2,
			},
		}},
		{name: "openzfs_over_max", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOpenzfs, StorageCapacity: aws.Int32(524289),
			OpenZFSConfiguration: &types.CreateFileSystemOpenZFSConfiguration{
				DeploymentType: types.OpenZFSDeploymentTypeSingleAz1, ThroughputCapacity: aws.Int32(64),
			},
		}},
		{name: "ontap_ha_pairs_need_single_az_2", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOntap, StorageCapacity: aws.Int32(2048),
			OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
				DeploymentType:     types.OntapDeploymentTypeSingleAz1,
				ThroughputCapacity: aws.Int32(128),
				HAPairs:            aws.Int32(2),
			},
		}},
		{name: "ontap_ha_pairs_over_12", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOntap, StorageCapacity: aws.Int32(20000),
			OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
				DeploymentType:              types.OntapDeploymentTypeSingleAz2,
				ThroughputCapacityPerHAPair: aws.Int32(128),
				HAPairs:                     aws.Int32(13),
			},
		}},
		{name: "ontap_capacity_scales_with_pairs", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOntap, StorageCapacity: aws.Int32(1024),
			OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
				DeploymentType:              types.OntapDeploymentTypeSingleAz2,
				ThroughputCapacityPerHAPair: aws.Int32(128),
				HAPairs:                     aws.Int32(2),
			},
		}},
		{name: "ontap_throughput_mismatch", wantErr: true, input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOntap, StorageCapacity: aws.Int32(2048),
			OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
				DeploymentType: types.OntapDeploymentTypeSingleAz2, ThroughputCapacity: aws.Int32(512),
				ThroughputCapacityPerHAPair: aws.Int32(128), HAPairs: aws.Int32(2),
			},
		}},
		{name: "ontap_two_pairs_ok", input: &fsxsdk.CreateFileSystemInput{
			FileSystemType: types.FileSystemTypeOntap, StorageCapacity: aws.Int32(2048),
			OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
				DeploymentType:              types.OntapDeploymentTypeSingleAz2,
				ThroughputCapacityPerHAPair: aws.Int32(128),
				HAPairs:                     aws.Int32(2),
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealismClient(t)
			tt.input.SubnetIds = []string{"subnet-0123abcd"}
			if tt.input.FileSystemType == types.FileSystemTypeOntap &&
				tt.input.OntapConfiguration.DeploymentType != types.OntapDeploymentTypeSingleAz1 &&
				tt.input.OntapConfiguration.DeploymentType != types.OntapDeploymentTypeSingleAz2 {
				tt.input.SubnetIds = append(tt.input.SubnetIds, "subnet-0456efab")
			}

			_, err := client.CreateFileSystem(t.Context(), tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateFileSystem_StorageCapacityRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  int32
		wantErr bool
	}{
		{name: "decrease_rejected", target: 1200 - 1, wantErr: true},
		{name: "valid_increase", target: 2400},
		{name: "invalid_lustre_size", target: 2500, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealismClient(t)
			fs, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeLustre,
				SubnetIds:       []string{"subnet-0123abcd"},
				StorageCapacity: aws.Int32(1200),
				LustreConfiguration: &types.CreateFileSystemLustreConfiguration{
					DeploymentType: types.LustreDeploymentTypeScratch2,
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateFileSystem(t.Context(), &fsxsdk.UpdateFileSystemInput{
				FileSystemId: fs.FileSystem.FileSystemId, StorageCapacity: aws.Int32(tt.target),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestLifecycleDelay_CreatingThenAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst types.FileSystemLifecycle
		delay     time.Duration
	}{
		{name: "instant", delay: 0, wantFirst: types.FileSystemLifecycleAvailable},
		{name: "delayed", delay: 300 * time.Millisecond, wantFirst: types.FileSystemLifecycleCreating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, client := newRealismClient(t)
				b.SetLifecycleDelay(tt.delay)

				fs := createTestLustreFS(t, client)
				assert.Equal(t, tt.wantFirst, fs.FileSystem.Lifecycle)

				bk, err := client.CreateBackup(
					t.Context(),
					&fsxsdk.CreateBackupInput{FileSystemId: fs.FileSystem.FileSystemId},
				)
				require.NoError(t, err)

				want := types.BackupLifecycle(tt.wantFirst)
				assert.Equal(t, want, bk.Backup.Lifecycle)

				require.Eventually(t, func() bool {
					out, descErr := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
						FileSystemIds: []string{aws.ToString(fs.FileSystem.FileSystemId)},
					})

					return descErr == nil && len(out.FileSystems) == 1 &&
						out.FileSystems[0].Lifecycle == types.FileSystemLifecycleAvailable
				}, 5*time.Second, 20*time.Millisecond)
			})
		})
	}
}

func TestDescribe_BadNextTokenRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(client *fsxsdk.Client, token *string) error
		name string
	}{
		{name: "file_systems", call: func(c *fsxsdk.Client, tok *string) error {
			_, err := c.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{NextToken: tok})

			return err
		}},
		{name: "backups", call: func(c *fsxsdk.Client, tok *string) error {
			_, err := c.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{NextToken: tok})

			return err
		}},
		{name: "volumes", call: func(c *fsxsdk.Client, tok *string) error {
			_, err := c.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{NextToken: tok})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealismClient(t)

			var bad *types.BadRequest
			require.ErrorAs(t, tt.call(client, aws.String("not base64 !!")), &bad)
		})
	}
}

func TestDescribe_OpaqueTokenWalk(t *testing.T) {
	t.Parallel()

	_, client := newRealismClient(t)

	for range 3 {
		createTestLustreFS(t, client)
	}

	var (
		seen  int
		token *string
	)

	for {
		out, err := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
			MaxResults: aws.Int32(1), NextToken: token,
		})
		require.NoError(t, err)

		seen += len(out.FileSystems)

		if out.NextToken == nil {
			break
		}

		assert.NotContains(t, aws.ToString(out.NextToken), "fs-")
		token = out.NextToken
	}

	assert.Equal(t, 3, seen)
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	_, client := newRealismClient(t)

	_, err := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
		FileSystemIds: []string{"fs-0123456789abcdef0"},
	})

	var nf *types.FileSystemNotFound
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "No Amazon FSx file systems were found based upon supplied parameters.", aws.ToString(nf.Message))

	_, err = client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
		FileSystemType:      types.FileSystemTypeLustre,
		SubnetIds:           []string{"subnet-0123abcd"},
		StorageCapacity:     aws.Int32(1300),
		LustreConfiguration: &types.CreateFileSystemLustreConfiguration{DeploymentType: "SCRATCH_2"},
	})

	var br *types.BadRequest
	require.ErrorAs(t, err, &br)
	assert.NotContains(t, aws.ToString(br.Message), "BadRequest")
}
