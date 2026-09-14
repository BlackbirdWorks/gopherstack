package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestRealClient_FileSystemConfigurationAndPolicy drives efs's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_FileSystemConfigurationAndPolicy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			backend := efs.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestEFSSDKClient(t, efs.NewHandler(backend))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-tags-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			_, err = client.CreateTags(ctx, &efssdk.CreateTagsInput{
				FileSystemId: aws.String(fsID),
				Tags: []efstypes.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeTags(ctx, &efssdk.DescribeTagsInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			tagMap := map[string]string{}
			for _, tg := range descOut.Tags {
				tagMap[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			assert.Equal(t, "test", tagMap["env"])

			_, err = client.DeleteTags(ctx, &efssdk.DeleteTagsInput{
				FileSystemId: aws.String(fsID),
				TagKeys:      []string{"env"},
			})
			require.NoError(t, err)

			afterOut, err := client.DescribeTags(ctx, &efssdk.DescribeTagsInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			assert.Empty(t, afterOut.Tags)
		}},
		{name: "access points", run: func(t *testing.T) {
			t.Helper()

			backend := efs.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestEFSSDKClient(t, efs.NewHandler(backend))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-ap-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			apOut, err := client.CreateAccessPoint(ctx, &efssdk.CreateAccessPointInput{
				ClientToken:  aws.String("s11-ap-token"),
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			apID := aws.ToString(apOut.AccessPointId)

			descOut, err := client.DescribeAccessPoints(ctx, &efssdk.DescribeAccessPointsInput{
				AccessPointId: aws.String(apID),
			})
			require.NoError(t, err)
			require.Len(t, descOut.AccessPoints, 1)
			assert.Equal(t, fsID, aws.ToString(descOut.AccessPoints[0].FileSystemId))

			_, err = client.DeleteAccessPoint(ctx, &efssdk.DeleteAccessPointInput{
				AccessPointId: aws.String(apID),
			})
			require.NoError(t, err)

			_, err = client.DescribeAccessPoints(ctx, &efssdk.DescribeAccessPointsInput{
				AccessPointId: aws.String(apID),
			})
			assert.Error(t, err, "DescribeAccessPoints on a deleted access point must fail")
		}},
		{name: "account preferences", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			putOut, err := client.PutAccountPreferences(ctx, &efssdk.PutAccountPreferencesInput{
				ResourceIdType: efstypes.ResourceIdTypeLongId,
			})
			require.NoError(t, err)
			require.NotNil(t, putOut.ResourceIdPreference)
			assert.Equal(t, efstypes.ResourceIdTypeLongId, putOut.ResourceIdPreference.ResourceIdType)

			descOut, err := client.DescribeAccountPreferences(ctx, &efssdk.DescribeAccountPreferencesInput{})
			require.NoError(t, err)
			require.NotNil(t, descOut.ResourceIdPreference)
			assert.Equal(t, efstypes.ResourceIdTypeLongId, descOut.ResourceIdPreference.ResourceIdType)
		}},
		{name: "file system policy", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-policy-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			policy := `{"Version":"2012-10-17","Statement":[]}`
			_, err = client.PutFileSystemPolicy(ctx, &efssdk.PutFileSystemPolicyInput{
				FileSystemId: aws.String(fsID),
				Policy:       aws.String(policy),
			})
			require.NoError(t, err)

			getOut, err := client.DescribeFileSystemPolicy(ctx, &efssdk.DescribeFileSystemPolicyInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			assert.JSONEq(t, policy, aws.ToString(getOut.Policy))

			_, err = client.DeleteFileSystemPolicy(ctx, &efssdk.DeleteFileSystemPolicyInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)

			_, err = client.DescribeFileSystemPolicy(ctx, &efssdk.DescribeFileSystemPolicyInput{
				FileSystemId: aws.String(fsID),
			})
			assert.Error(t, err, "DescribeFileSystemPolicy after delete must fail")
		}},
		{name: "lifecycle configuration", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-lifecycle-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			putOut, err := client.PutLifecycleConfiguration(ctx, &efssdk.PutLifecycleConfigurationInput{
				FileSystemId: aws.String(fsID),
				LifecyclePolicies: []efstypes.LifecyclePolicy{
					{TransitionToIA: efstypes.TransitionToIARulesAfter30Days},
				},
			})
			require.NoError(t, err)
			require.Len(t, putOut.LifecyclePolicies, 1)
			assert.Equal(t, efstypes.TransitionToIARulesAfter30Days, putOut.LifecyclePolicies[0].TransitionToIA)

			descOut, err := client.DescribeLifecycleConfiguration(ctx, &efssdk.DescribeLifecycleConfigurationInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			require.Len(t, descOut.LifecyclePolicies, 1)
			assert.Equal(t, efstypes.TransitionToIARulesAfter30Days, descOut.LifecyclePolicies[0].TransitionToIA)
		}},
		{name: "mount target security groups", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-mt-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			mtOut, err := client.CreateMountTarget(ctx, &efssdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID),
				SubnetId:     aws.String("subnet-11111111"),
			})
			require.NoError(t, err)
			mtID := aws.ToString(mtOut.MountTargetId)

			_, err = client.ModifyMountTargetSecurityGroups(ctx, &efssdk.ModifyMountTargetSecurityGroupsInput{
				MountTargetId:  aws.String(mtID),
				SecurityGroups: []string{"sg-abcdef01"},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeMountTargetSecurityGroups(
				ctx,
				&efssdk.DescribeMountTargetSecurityGroupsInput{
					MountTargetId: aws.String(mtID),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, []string{"sg-abcdef01"}, descOut.SecurityGroups)
		}},
		{name: "replication configuration delete and file system protection", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-repl-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			_, err = client.CreateReplicationConfiguration(ctx, &efssdk.CreateReplicationConfigurationInput{
				SourceFileSystemId: aws.String(fsID),
				Destinations:       []efstypes.DestinationToCreate{{}},
			})
			require.NoError(t, err)

			_, err = client.DeleteReplicationConfiguration(ctx, &efssdk.DeleteReplicationConfigurationInput{
				SourceFileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)

			descAfterDelete, err := client.DescribeReplicationConfigurations(
				ctx, &efssdk.DescribeReplicationConfigurationsInput{
					FileSystemId: aws.String(fsID),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, descAfterDelete.Replications)

			_, err = client.UpdateFileSystemProtection(ctx, &efssdk.UpdateFileSystemProtectionInput{
				FileSystemId:                   aws.String(fsID),
				ReplicationOverwriteProtection: efstypes.ReplicationOverwriteProtectionDisabled,
			})
			require.NoError(t, err)

			descOut, err := client.DescribeFileSystems(ctx, &efssdk.DescribeFileSystemsInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			require.Len(t, descOut.FileSystems, 1)
			require.NotNil(t, descOut.FileSystems[0].FileSystemProtection)
			assert.Equal(t, efstypes.ReplicationOverwriteProtectionDisabled,
				descOut.FileSystems[0].FileSystemProtection.ReplicationOverwriteProtection)
		}},
		{name: "update file system", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-update-fs"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			updOut, err := client.UpdateFileSystem(ctx, &efssdk.UpdateFileSystemInput{
				FileSystemId:                 aws.String(fsID),
				ThroughputMode:               efstypes.ThroughputModeProvisioned,
				ProvisionedThroughputInMibps: aws.Float64(10),
			})
			require.NoError(t, err)
			assert.Equal(t, efstypes.ThroughputModeProvisioned, updOut.ThroughputMode)
			assert.InDelta(t, 10.0, aws.ToFloat64(updOut.ProvisionedThroughputInMibps), 0.001)
		}},
		{name: "put backup policy", run: func(t *testing.T) {
			t.Helper()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("s11-backup-policy"),
			})
			require.NoError(t, err)
			fsID := aws.ToString(fsOut.FileSystemId)

			putOut, err := client.PutBackupPolicy(ctx, &efssdk.PutBackupPolicyInput{
				FileSystemId: aws.String(fsID),
				BackupPolicy: &efstypes.BackupPolicy{Status: efstypes.StatusEnabled},
			})
			require.NoError(t, err)
			require.NotNil(t, putOut.BackupPolicy)
			assert.Equal(t, efstypes.StatusEnabled, putOut.BackupPolicy.Status)

			descOut, err := client.DescribeBackupPolicy(ctx, &efssdk.DescribeBackupPolicyInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut.BackupPolicy)
			assert.Equal(t, efstypes.StatusEnabled, descOut.BackupPolicy.Status)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
