package rum_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rum"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	id := f.createMonitor(t, "web", func(in *awsrum.CreateAppMonitorInput) {
		in.Tags = map[string]string{"k": "v"}
		in.CwLogEnabled = aws.Bool(true)
	})
	f.putCloudWatchDestination(t)

	_, err := f.client.BatchCreateRumMetricDefinitions(ctx, &awsrum.BatchCreateRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"),
		Destination:    types.MetricDestinationCloudWatch,
		MetricDefinitions: []types.MetricDefinitionRequest{
			{Name: aws.String("Checkout"), Namespace: aws.String("shop")},
		},
	})
	require.NoError(t, err)

	_, err = f.client.PutResourcePolicy(
		ctx,
		&awsrum.PutResourcePolicyInput{Name: aws.String("web"), PolicyDocument: aws.String(allowPolicy)},
	)
	require.NoError(t, err)

	require.NoError(
		t,
		f.putEvents(t, id, "web", eventSpec{id: "e1", typ: evtNav, details: `{"duration":5}`, ts: f.base()}),
	)

	snapshot := f.backend.Snapshot(ctx)
	require.NotEmpty(t, snapshot)

	restored := rum.NewInMemoryBackend("000000000000", "eu-west-1")
	require.NoError(t, restored.Restore(ctx, snapshot))
	assert.Equal(t, 1, restored.MonitorCount())
	assert.Equal(t, 1, restored.EventCount())

	r := newFixtureFromBackend(t, restored)

	got, err := r.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Equal(t, id, aws.ToString(got.AppMonitor.Id))
	assert.Equal(t, map[string]string{"k": "v"}, got.AppMonitor.Tags)
	assert.Equal(t, types.StateEnumActive, got.AppMonitor.State)

	defs, err := r.client.BatchGetRumMetricDefinitions(ctx, &awsrum.BatchGetRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
	})
	require.NoError(t, err)
	assert.Len(t, defs.MetricDefinitions, 1)

	pol, err := r.client.GetResourcePolicy(ctx, &awsrum.GetResourcePolicyInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.JSONEq(t, allowPolicy, aws.ToString(pol.PolicyDocument))

	data, err := r.client.GetAppMonitorData(ctx, &awsrum.GetAppMonitorDataInput{
		Name: aws.String("web"), TimeRange: &types.TimeRange{After: int64(f.base()) - 5},
	})
	require.NoError(t, err)
	assert.Len(t, data.Events, 1)
}

func TestPersistence_RejectsOtherVersion(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.createMonitor(t, "web")

	b := rum.NewInMemoryBackend(testAccount, testRegion)
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":99,"tables":{}}`)))
	assert.Equal(t, 0, b.MonitorCount())
}

func TestReset(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.createMonitor(t, "web")
	require.Equal(t, 1, f.backend.MonitorCount())

	rum.NewHandler(f.backend).Reset()
	assert.Equal(t, 0, f.backend.MonitorCount())
}
