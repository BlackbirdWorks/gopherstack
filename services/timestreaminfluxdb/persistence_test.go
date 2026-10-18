package timestreaminfluxdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	g, err := src.client.CreateDbParameterGroup(t.Context(), &sdk.CreateDbParameterGroupInput{
		Name: aws.String("snap-group"),
		Parameters: &types.ParametersMemberInfluxDBv2{Value: types.InfluxDBv2Parameters{
			LogLevel: types.LogLevelError,
			StorageCacheSnapshotWriteColdDuration: &types.Duration{
				DurationType: types.DurationTypeMinutes,
				Value:        aws.Int64(5),
			},
		}},
	})
	require.NoError(t, err)

	in := instanceInput("snap-db")
	in.DbParameterGroupIdentifier = g.Id
	in.Tags = map[string]string{"k": "v"}
	inst, err := src.client.CreateDbInstance(t.Context(), in)
	require.NoError(t, err)
	src.settle()

	clusterID := src.cluster(t, "snap-cluster")
	bk, err := src.client.CreateDbBackup(
		t.Context(),
		&sdk.CreateDbBackupInput{DbResourceId: inst.Id, Name: aws.String("snap-backup")},
	)
	require.NoError(t, err)
	src.settle()

	data := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newFixtureFromBackend(
		t,
		timestreaminfluxdb.NewInMemoryBackend(testAccount, "us-east-1"),
		&fakeClock{t: src.clock.now()},
	)
	t.Cleanup(dst.backend.Close)
	require.NoError(t, dst.backend.Restore(t.Context(), data))

	got, err := dst.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
	require.NoError(t, err)
	assert.Equal(t, types.StatusAvailable, got.Status)
	assert.Equal(t, aws.ToString(g.Id), aws.ToString(got.DbParameterGroupIdentifier))

	tags, err := dst.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: inst.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)

	grp, err := dst.client.GetDbParameterGroup(t.Context(), &sdk.GetDbParameterGroupInput{Identifier: g.Id})
	require.NoError(t, err)
	assert.Equal(t, types.LogLevelError, grp.Parameters.(*types.ParametersMemberInfluxDBv2).Value.LogLevel)

	members, err := dst.client.ListDbInstancesForCluster(
		t.Context(),
		&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(clusterID)},
	)
	require.NoError(t, err)
	assert.Len(t, members.Items, 3)

	b, err := dst.client.GetDbBackup(t.Context(), &sdk.GetDbBackupInput{Identifier: bk.Id})
	require.NoError(t, err)
	assert.Equal(t, types.DbBackupStatusCompleted, b.Status)
}

func TestRestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.instance(t, "will-vanish")

	require.NoError(t, f.backend.Restore(t.Context(), []byte(`{"version":999}`)))
	assert.Equal(t, 0, f.backend.InstanceCount())
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.instance(t, "reset-db")
	f.backend.Reset()
	assert.Equal(t, 0, f.backend.InstanceCount())
}

func TestAccountAndRegionIsolation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.instance(t, "mine-db")

	other := f.regionClient(t, "eu-west-1")

	_, err := other.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: inst.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := other.ListDbInstances(t.Context(), &sdk.ListDbInstancesInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	_, err = other.CreateDbInstance(t.Context(), instanceInput("mine-db"))
	require.NoError(t, err)
}

func TestProviderInit(t *testing.T) {
	t.Setenv(timestreaminfluxdb.EngineEnv, "stub")

	p := &timestreaminfluxdb.Provider{}
	assert.Equal(t, "TimestreamInfluxDB", p.Name())

	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*timestreaminfluxdb.Handler)
	require.True(t, ok)
	assert.Equal(t, "TimestreamInfluxDB", h.Name())
	assert.Len(t, h.GetSupportedOperations(), 24)
	assert.NotPanics(t, func() { h.Shutdown(t.Context()) })

}
