package s3files_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/aws-sdk-go-v2/service/s3files/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3files"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	ctx := t.Context()
	fsID := src.fileSystem(t)

	_, err := src.client.PutFileSystemPolicy(ctx, &s3filessdk.PutFileSystemPolicyInput{
		FileSystemId: aws.String(fsID), Policy: aws.String(`{"Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = src.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId: aws.String(fsID), ImportDataRules: []types.ImportDataRule{},
		ExpirationDataRules: []types.ExpirationDataRule{{DaysAfterLastAccess: aws.Int32(7)}},
	})
	require.NoError(t, err)

	mt, err := src.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet),
	})
	require.NoError(t, err)

	ap, err := src.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{
		FileSystemId: aws.String(fsID), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	snap := src.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	dst := newFixtureWith(t, s3files.NewInMemoryBackend("999999999999", "eu-west-1"), testRegion)
	require.NoError(t, dst.backend.Restore(ctx, snap))

	gotAP, err := dst.client.GetAccessPoint(ctx, &s3filessdk.GetAccessPointInput{AccessPointId: ap.AccessPointId})
	require.NoError(t, err)
	assert.Equal(t, []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}, gotAP.Tags)

	gotMT, err := dst.client.GetMountTarget(ctx, &s3filessdk.GetMountTargetInput{MountTargetId: mt.MountTargetId})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(mt.Ipv4Address), aws.ToString(gotMT.Ipv4Address))

	pol, err := dst.client.GetFileSystemPolicy(
		ctx,
		&s3filessdk.GetFileSystemPolicyInput{FileSystemId: aws.String(fsID)},
	)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Statement":[]}`, aws.ToString(pol.Policy))

	cfg, err := dst.client.GetSynchronizationConfiguration(ctx, &s3filessdk.GetSynchronizationConfigurationInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(cfg.LatestVersionNumber))

	dst.backend.Reset()

	_, err = dst.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: aws.String(fsID)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
