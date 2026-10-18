package cloudhsmv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hsm "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	c, bk := f.readyBackup(t)

	_, err := f.client.TagResource(
		ctx,
		&sdk.TagResourceInput{ResourceId: c.ClusterId, TagList: []types.Tag{tag("k", "v")}},
	)
	require.NoError(t, err)
	_, err = f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: bk.BackupArn, Policy: aws.String(sharePolicy)},
	)
	require.NoError(t, err)

	snapshot := f.backend.Snapshot(ctx)
	require.NotEmpty(t, snapshot)

	restored := hsm.NewInMemoryBackend("000000000000", "eu-west-1")
	require.NoError(t, restored.Restore(ctx, snapshot))
	assert.Equal(t, 1, restored.ClusterCount())

	r := newFixtureFromBackend(t, restored)
	r.clock.t = f.clock.now()

	got := r.describeOne(t, aws.ToString(c.ClusterId))
	assert.Equal(t, aws.ToString(c.Certificates.ClusterCertificate), aws.ToString(got.Certificates.ClusterCertificate))
	require.Len(t, got.Hsms, 1)
	assert.Equal(t, []types.Tag{tag("k", "v")}, got.TagList)

	pol, err := r.client.GetResourcePolicy(ctx, &sdk.GetResourcePolicyInput{ResourceArn: bk.BackupArn})
	require.NoError(t, err)
	assert.JSONEq(t, sharePolicy, aws.ToString(pol.Policy))

	backups, err := r.client.DescribeBackups(ctx, &sdk.DescribeBackupsInput{})
	require.NoError(t, err)
	require.Len(t, backups.Backups, 1)
	assert.Equal(t, aws.ToString(bk.BackupId), aws.ToString(backups.Backups[0].BackupId))
}

func TestPersistence_RejectsOtherVersion(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.createCluster(t)

	b := hsm.NewInMemoryBackend(testAccount, testRegion)
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":99,"tables":{}}`)))
	assert.Equal(t, 0, b.ClusterCount())
}

func TestReset(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.createCluster(t)
	require.Equal(t, 1, f.backend.ClusterCount())

	hsm.NewHandler(f.backend).Reset()
	assert.Equal(t, 0, f.backend.ClusterCount())
}
