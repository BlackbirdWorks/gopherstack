package integration_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newLoggingClients(t *testing.T) (*logging.Client, *logadmin.Client) {
	t.Helper()

	opts := gcpGRPCOpts(t, "8131")

	lc, err := logging.NewClient(t.Context(), "projects/"+gcpProject, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lc.Close() })

	ac, err := logadmin.NewClient(t.Context(), gcpProject, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	return lc, ac
}

func collectEntries(t *testing.T, ac *logadmin.Client, filter string) []*logging.Entry {
	t.Helper()

	var out []*logging.Entry

	it := ac.Entries(t.Context(), logadmin.Filter(filter))

	for {
		e, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, e)
	}
}

func TestIntegration_GCP_Logging_WriteAndFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	lc, ac := newLoggingClients(t)
	logID := gcpName("it-log")
	lg := lc.Logger(logID)
	now := time.Now()

	lg.Log(logging.Entry{Payload: "info line", Severity: logging.Info, Timestamp: now.Add(-2 * time.Second)})
	lg.Log(logging.Entry{
		Payload: map[string]any{"msg": "disk full"}, Severity: logging.Error,
		Labels: map[string]string{"team": "sre"}, Timestamp: now.Add(-time.Second),
	})
	require.NoError(t, lg.Flush())

	base := `logName="projects/` + gcpProject + `/logs/` + logID + `"`

	assert.Len(t, collectEntries(t, ac, base), 2)
	assert.Len(t, collectEntries(t, ac, base+` AND severity>=ERROR`), 1)
	assert.Len(t, collectEntries(t, ac, base+` AND jsonPayload.msg:"DISK"`), 1)
	assert.Len(t, collectEntries(t, ac, base+` AND labels.team="sre"`), 1)
	assert.Empty(t, collectEntries(t, ac, base+` AND NOT severity>=INFO`))

	require.NoError(t, ac.DeleteLog(t.Context(), logID))
	assert.Empty(t, collectEntries(t, ac, base))
}

func TestIntegration_GCP_Logging_SinkExclusionMetric(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	_, ac := newLoggingClients(t)
	ctx := t.Context()
	id := gcpName("it-sink")

	created, err := ac.CreateSinkOpt(ctx, &logadmin.Sink{
		ID: id, Destination: "storage.googleapis.com/" + id, Filter: "severity>=ERROR",
	}, logadmin.SinkOptions{UniqueWriterIdentity: true})
	require.NoError(t, err)
	assert.Contains(t, created.WriterIdentity, "gcp-sa-logging.iam.gserviceaccount.com")

	got, err := ac.Sink(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "severity>=ERROR", got.Filter)
	require.NoError(t, ac.DeleteSink(ctx, id))
	assert.Equal(t, codes.NotFound, status.Code(func() error { _, e := ac.Sink(ctx, id); return e }()))

	mid := gcpName("it-metric")
	require.NoError(t, ac.CreateMetric(ctx, &logadmin.Metric{ID: mid, Filter: "severity>=ERROR", Description: "d"}))

	m, err := ac.Metric(ctx, mid)
	require.NoError(t, err)
	assert.Equal(t, "severity>=ERROR", m.Filter)
	require.NoError(t, ac.DeleteMetric(ctx, mid))
}

func TestIntegration_GCP_Logging_SinkToPubSub(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	lc, ac := newLoggingClients(t)
	ps := newPubSubClient(t)
	ctx := t.Context()
	proj := "projects/" + gcpProject
	topic := proj + "/topics/" + gcpName("it-logtopic")
	sub := proj + "/subscriptions/" + gcpName("it-logsub")

	_, err := ps.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic})
	require.NoError(t, err)
	_, err = ps.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic})
	require.NoError(t, err)

	sinkID := gcpName("it-psink")
	logID := gcpName("it-pslog")
	_, err = ac.CreateSink(ctx, &logadmin.Sink{
		ID: sinkID, Destination: "pubsub.googleapis.com/" + topic,
		Filter: `logName="` + proj + `/logs/` + logID + `" AND severity>=ERROR`,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.DeleteSink(t.Context(), sinkID) })

	lg := lc.Logger(logID)
	lg.Log(logging.Entry{Payload: "ignored", Severity: logging.Info})
	lg.Log(logging.Entry{Payload: "routed to topic", Severity: logging.Error})
	require.NoError(t, lg.Flush())

	msg := receiveOne(t, ps, sub)
	assert.True(t, strings.Contains(string(msg.Data), "routed to topic"), string(msg.Data))
	assert.Contains(t, string(msg.Data), logID)
}
