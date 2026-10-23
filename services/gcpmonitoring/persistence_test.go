package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	tg := newTarget(t)
	ch := src.pubsubChannel(t)
	policy := src.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	check := src.createCheck(t, tg.check("health", &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"}))
	group := src.createGroup(t, "g", "", `resource.type = "gce_instance"`)

	for m := 1; m <= 2; m++ {
		src.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	src.handler.Backend.Tick(t.Context())
	require.Len(t, src.handler.Backend.Incidents(testProject), 1)

	snap := src.handler.Backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), snap))

	gotPolicy, err := dst.alerts(t).
		GetAlertPolicy(t.Context(), &monitoringpb.GetAlertPolicyRequest{Name: policy.GetName()})
	require.NoError(t, err)
	assert.Equal(t, policy.GetConditions()[0].GetName(), gotPolicy.GetConditions()[0].GetName())

	gotCheck, err := dst.uptime(t).
		GetUptimeCheckConfig(t.Context(), &monitoringpb.GetUptimeCheckConfigRequest{Name: check.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "health", gotCheck.GetDisplayName())

	_, err = dst.groups(t).GetGroup(t.Context(), &monitoringpb.GetGroupRequest{Name: group.GetName()})
	require.NoError(t, err)

	gotCh, err := dst.channels(t).
		GetNotificationChannel(t.Context(), &monitoringpb.GetNotificationChannelRequest{Name: ch})
	require.NoError(t, err)
	assert.Equal(t, alertTopic, gotCh.GetLabels()["topic"])

	series := dst.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: metricFilter, Interval: interval(epoch(), epoch().Add(3*time.Minute)),
	})
	require.Len(t, series, 1)
	assert.Equal(t, []float64{9, 9}, values(series[0]))

	incs := dst.handler.Backend.Incidents(testProject)
	require.Len(t, incs, 1)
	assert.Equal(t, "open", incs[0].State)

	dst.clk.set(epoch().Add(2 * time.Minute))
	dst.handler.Backend.Tick(t.Context())
	assert.Empty(t, dst.pub.all(), "the restored open incident is not announced again")
}

func TestSnapshotKeepsSensitiveChannelLabels(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	hs, url := newHookServer(t, 200)
	ch, err := src.channels(t).CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "webhook_tokenauth", Labels: map[string]string{"url": url, "auth_token": "tok"},
		},
	})
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))
	dst.createPolicy(t, queuePolicy([]string{ch.GetName()}, monitoringpb.ComparisonType_COMPARISON_GT, 3))

	for m := 1; m <= 2; m++ {
		dst.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	dst.handler.Backend.Tick(t.Context())

	reqs := hs.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "tok", reqs[0].query)
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	t.Parallel()

	b := gcpmonitoring.NewInMemoryBackend()
	assert.Error(t, b.Restore(t.Context(), []byte("not json")))
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.pubsubChannel(t)
	e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	e.handler.Reset()

	it := e.alerts(t).ListAlertPolicies(t.Context(), &monitoringpb.ListAlertPoliciesRequest{Name: proj})
	_, err := it.Next()
	assert.Error(t, err)
}
