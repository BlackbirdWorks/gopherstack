package cloudhsmv2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readyBackup returns the READY backup taken when an initialized cluster gains its first HSM.
func (f *fixture) readyBackup(t *testing.T) (types.Cluster, types.Backup) {
	t.Helper()

	c := f.initializedCluster(t)

	_, err := f.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
		ClusterId: c.ClusterId, AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)
	f.clock.advance(time.Second)

	out, err := f.client.DescribeBackups(t.Context(), &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	require.Len(t, out.Backups, 1)

	return c, out.Backups[0]
}

func TestRealClient_Backups(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testBackupLifecycle, "lifecycle"},
		{testBackupDeleteRestore, "delete_restore"},
		{testBackupRetention, "retention"},
		{testBackupCopy, "copy"},
		{testClusterFromBackup, "cluster_from_backup"},
		{testBackupFilters, "filters"},
		{testResourcePolicy, "policy"},
		{testSharedBackup, "shared"},
		{testBackupErrors, "errors"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testBackupLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	c := f.initializedCluster(t)

	_, err := f.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
		ClusterId: c.ClusterId, AvailabilityZone: aws.String("us-east-1b"),
	})
	require.NoError(t, err)

	out, err := f.client.DescribeBackups(t.Context(), &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	require.Len(t, out.Backups, 1)

	bk := out.Backups[0]
	assert.Regexp(t, `^backup-[0-9a-f]{11}$`, aws.ToString(bk.BackupId))
	assert.Regexp(t, `^arn:aws:cloudhsm:us-east-1:123456789012:backup/backup-`, aws.ToString(bk.BackupArn))
	assert.Equal(t, types.BackupStateCreateInProgress, bk.BackupState)
	assert.Equal(t, aws.ToString(c.ClusterId), aws.ToString(bk.ClusterId))
	assert.Equal(t, "hsm1.medium", aws.ToString(bk.HsmType))
	assert.Equal(t, types.ClusterModeFips, bk.Mode)
	assert.False(t, aws.ToBool(bk.NeverExpires))

	f.clock.advance(time.Second)

	out, err = f.client.DescribeBackups(t.Context(), &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	assert.Equal(t, types.BackupStateReady, out.Backups[0].BackupState)

	del, err := f.client.DeleteHsm(t.Context(), &sdk.DeleteHsmInput{
		ClusterId: c.ClusterId, HsmId: out.Backups[0].BackupId,
	})
	assert.Nil(t, del)
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
}

func testBackupDeleteRestore(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	_, bk := f.readyBackup(t)

	del, err := f.client.DeleteBackup(ctx, &sdk.DeleteBackupInput{BackupId: bk.BackupId})
	require.NoError(t, err)
	assert.Equal(t, types.BackupStatePendingDeletion, del.Backup.BackupState)
	require.NotNil(t, del.Backup.DeleteTimestamp)
	assert.Equal(t, f.clock.now().Add(7*24*time.Hour).Unix(), del.Backup.DeleteTimestamp.Unix())

	f.clock.advance(24 * time.Hour)

	res, err := f.client.RestoreBackup(ctx, &sdk.RestoreBackupInput{BackupId: bk.BackupId})
	require.NoError(t, err)
	assert.Equal(t, types.BackupStateReady, res.Backup.BackupState)
	assert.Nil(t, res.Backup.DeleteTimestamp)

	_, err = f.client.DeleteBackup(ctx, &sdk.DeleteBackupInput{BackupId: bk.BackupId})
	require.NoError(t, err)

	f.clock.advance(8 * 24 * time.Hour)

	_, err = f.client.RestoreBackup(ctx, &sdk.RestoreBackupInput{BackupId: bk.BackupId})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err), "recovery window elapsed")

	list, err := f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Backups)
}

func testBackupRetention(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c := f.initializedCluster(t)

	_, err := f.client.ModifyCluster(ctx, &sdk.ModifyClusterInput{
		ClusterId: c.ClusterId,
		BackupRetentionPolicy: &types.BackupRetentionPolicy{
			Type:  types.BackupRetentionTypeDays,
			Value: aws.String("10"),
		},
	})
	require.NoError(t, err)
	f.clock.advance(time.Second)

	for _, az := range []string{"us-east-1a"} {
		_, err = f.client.CreateHsm(ctx, &sdk.CreateHsmInput{ClusterId: c.ClusterId, AvailabilityZone: aws.String(az)})
		require.NoError(t, err)
	}

	f.clock.advance(time.Second)

	list, err := f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	require.Len(t, list.Backups, 1)

	id := list.Backups[0].BackupId

	kept, err := f.client.ModifyBackupAttributes(
		ctx,
		&sdk.ModifyBackupAttributesInput{BackupId: id, NeverExpires: aws.Bool(true)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(kept.Backup.NeverExpires))

	f.clock.advance(11 * 24 * time.Hour)

	list, err = f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{
		Filters: map[string][]string{"neverExpires": {"true"}},
	})
	require.NoError(t, err)
	require.Len(t, list.Backups, 1, "exempt from the 10 day policy")

	_, err = f.client.ModifyBackupAttributes(
		ctx,
		&sdk.ModifyBackupAttributesInput{BackupId: id, NeverExpires: aws.Bool(false)},
	)
	require.NoError(t, err)

	list, err = f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Backups, "the policy applies again, so the old backup has expired")
}

func testBackupCopy(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c, bk := f.readyBackup(t)

	_, err := f.client.CopyBackupToRegion(ctx, &sdk.CopyBackupToRegionInput{
		BackupId: bk.BackupId, DestinationRegion: aws.String(testRegion),
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "same region")

	_, err = f.client.CopyBackupToRegion(ctx, &sdk.CopyBackupToRegionInput{
		BackupId: bk.BackupId, DestinationRegion: aws.String("nowhere"),
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "bad region")

	out, err := f.client.CopyBackupToRegion(ctx, &sdk.CopyBackupToRegionInput{
		BackupId: bk.BackupId, DestinationRegion: aws.String("us-west-2"),
		TagList: []types.Tag{{Key: aws.String("dr"), Value: aws.String("yes")}},
	})
	require.NoError(t, err)

	dst := out.DestinationBackup
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(dst.SourceBackup))
	assert.Equal(t, testRegion, aws.ToString(dst.SourceRegion))
	assert.Equal(t, aws.ToString(c.ClusterId), aws.ToString(dst.SourceCluster))
	assert.Equal(t, bk.CreateTimestamp.Unix(), dst.CreateTimestamp.Unix())

	local, err := f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	assert.Len(t, local.Backups, 1, "the copy lives in the destination region")

	remote, err := f.client.DescribeBackups(
		ctx,
		&sdk.DescribeBackupsInput{Filters: map[string][]string{"sourceBackupIds": {aws.ToString(bk.BackupId)}}},
		withRegion("us-west-2"),
	)
	require.NoError(t, err)
	require.Len(t, remote.Backups, 1)
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(remote.Backups[0].SourceBackup))
	assert.NotEqual(t, aws.ToString(bk.BackupId), aws.ToString(remote.Backups[0].BackupId))
	assert.Equal(t, testRegion, aws.ToString(remote.Backups[0].SourceRegion))
	require.NotNil(t, remote.Backups[0].CopyTimestamp)
	assert.Equal(t, []types.Tag{{Key: aws.String("dr"), Value: aws.String("yes")}}, remote.Backups[0].TagList)
}

func withRegion(region string) func(*sdk.Options) {
	return func(o *sdk.Options) { o.Region = region }
}

func testClusterFromBackup(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c, bk := f.readyBackup(t)

	restored, err := f.client.CreateCluster(ctx, &sdk.CreateClusterInput{
		HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"}, SourceBackupId: bk.BackupId,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(restored.Cluster.SourceBackupId))

	f.clock.advance(time.Second)

	got := f.describeOne(t, aws.ToString(restored.Cluster.ClusterId))
	assert.Equal(t, types.ClusterStateInitialized, got.State, "a restored cluster is already initialized")
	assert.Equal(t, aws.ToString(c.Certificates.ClusterCertificate), aws.ToString(got.Certificates.ClusterCertificate))

	_, err = f.client.CreateCluster(ctx, &sdk.CreateClusterInput{
		HsmType: aws.String("hsm2m.medium"), SubnetIds: []string{"subnet-a"}, SourceBackupId: bk.BackupId,
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "HSM type must match the backup")
}

func testBackupFilters(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c, bk := f.readyBackup(t)

	cases := []struct {
		filters map[string][]string
		name    string
		want    int
	}{
		{map[string][]string{"backupIds": {aws.ToString(bk.BackupId)}}, "id", 1},
		{map[string][]string{"clusterIds": {aws.ToString(c.ClusterId)}}, "cluster", 1},
		{map[string][]string{"clusterIds": {"cluster-other"}}, "other_cluster", 0},
		{map[string][]string{"states": {"READY"}}, "state", 1},
		{map[string][]string{"states": {"PENDING_DELETION"}}, "other_state", 0},
		{map[string][]string{"modes": {"FIPS"}}, "mode", 1},
		{map[string][]string{"neverExpires": {"true"}}, "never", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := f.client.DescribeBackups(
				ctx,
				&sdk.DescribeBackupsInput{Filters: tc.filters, SortAscending: aws.Bool(true)},
			)
			require.NoError(t, err)
			assert.Len(t, out.Backups, tc.want)
		})
	}

	_, err := f.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{Filters: map[string][]string{"bogus": {"x"}}})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err))
}

const sharePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::210987654321:root"},` +
	`"Action":["cloudhsm:CreateCluster","cloudhsm:DescribeBackups"],"Resource":"*"}]}`

func testResourcePolicy(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	_, bk := f.readyBackup(t)
	arn := bk.BackupArn

	_, err := f.client.GetResourcePolicy(ctx, &sdk.GetResourcePolicyInput{ResourceArn: arn})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))

	_, err = f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: arn, Policy: aws.String("not json")},
	)
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err))

	_, err = f.client.PutResourcePolicy(ctx, &sdk.PutResourcePolicyInput{
		ResourceArn: aws.String(
			"arn:aws:cloudhsm:us-east-1:123456789012:cluster/cluster-1",
		),
		Policy: aws.String(sharePolicy),
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "only backups take policies")

	put, err := f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: arn, Policy: aws.String(sharePolicy)},
	)
	require.NoError(t, err)
	assert.JSONEq(t, sharePolicy, aws.ToString(put.Policy))
	assert.Equal(t, aws.ToString(arn), aws.ToString(put.ResourceArn))

	got, err := f.client.GetResourcePolicy(ctx, &sdk.GetResourcePolicyInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.JSONEq(t, sharePolicy, aws.ToString(got.Policy))

	del, err := f.client.DeleteResourcePolicy(ctx, &sdk.DeleteResourcePolicyInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.JSONEq(t, sharePolicy, aws.ToString(del.Policy))

	_, err = f.client.DeleteResourcePolicy(ctx, &sdk.DeleteResourcePolicyInput{ResourceArn: arn})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
}

func testSharedBackup(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	_, bk := f.readyBackup(t)

	_, err := f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: bk.BackupArn, Policy: aws.String(sharePolicy)},
	)
	require.NoError(t, err)

	other := newOtherAccountClient(t, f)

	none, err := other.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	assert.Empty(t, none.Backups, "own backups only without Shared")

	shared, err := other.DescribeBackups(ctx, &sdk.DescribeBackupsInput{Shared: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, shared.Backups, 1)
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(shared.Backups[0].BackupId))

	_, err = other.DeleteBackup(ctx, &sdk.DeleteBackupInput{BackupId: bk.BackupId})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err), "cross-account delete is not allowed")

	restored, err := other.CreateCluster(ctx, &sdk.CreateClusterInput{
		HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"}, SourceBackupId: bk.BackupArn,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(restored.Cluster.SourceBackupId))

	_, err = f.client.DeleteResourcePolicy(ctx, &sdk.DeleteResourcePolicyInput{ResourceArn: bk.BackupArn})
	require.NoError(t, err)

	_, err = other.CreateCluster(ctx, &sdk.CreateClusterInput{
		HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"}, SourceBackupId: bk.BackupArn,
	})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err), "unshared")
}

func testBackupErrors(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	ghost := aws.String("backup-ffffffffffff")

	_, err := f.client.DeleteBackup(ctx, &sdk.DeleteBackupInput{BackupId: ghost})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))

	_, err = f.client.RestoreBackup(ctx, &sdk.RestoreBackupInput{BackupId: ghost})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))

	_, err = f.client.ModifyBackupAttributes(
		ctx,
		&sdk.ModifyBackupAttributesInput{BackupId: ghost, NeverExpires: aws.Bool(true)},
	)
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))

	_, err = f.client.CopyBackupToRegion(
		ctx,
		&sdk.CopyBackupToRegionInput{BackupId: ghost, DestinationRegion: aws.String("us-west-2")},
	)
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
}
