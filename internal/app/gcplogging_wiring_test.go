package app

import (
	"strings"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type logSinkFixture struct {
	logging *gcploggingbackend.Handler
	pubsub  *gcppubsubbackend.Handler
	gcs     *gcpstoragebackend.Handler
	bq      *gcpbigquerybackend.Handler
}

func newLogSinkFixture(t *testing.T) *logSinkFixture {
	t.Helper()

	f := &logSinkFixture{
		logging: gcploggingbackend.NewHandler(gcploggingbackend.NewInMemoryBackend()),
		pubsub:  gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend()),
		gcs:     gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend()),
		bq:      gcpbigquerybackend.NewHandler(gcpbigquerybackend.NewInMemoryBackend()),
	}

	wireGCPLogging(map[string]service.Registerable{
		"GCPLogging": f.logging, "GCPPubSub": f.pubsub, "GCPStorage": f.gcs, "GCPBigQuery": f.bq,
	})

	return f
}

func (f *logSinkFixture) sink(t *testing.T, name, dest string, opts ...func(*loggingpb.LogSink)) {
	t.Helper()

	s := &loggingpb.LogSink{Name: name, Destination: dest}
	for _, o := range opts {
		o(s)
	}

	_, err := f.logging.Backend.CreateSink(&loggingpb.CreateSinkRequest{Parent: "projects/p", Sink: s})
	require.NoError(t, err)
}

func (f *logSinkFixture) write(t *testing.T, text string) {
	t.Helper()

	err := f.logging.Backend.WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
		LogName:  "projects/p/logs/app",
		Resource: &monitoredres.MonitoredResource{Type: "global"},
		Entries:  []*loggingpb.LogEntry{{Payload: &loggingpb.LogEntry_TextPayload{TextPayload: text}}},
	})
	require.NoError(t, err)
}

func TestLogSinkToPubSub(t *testing.T) {
	t.Parallel()

	f := newLogSinkFixture(t)
	topic := "projects/p/topics/logs"

	_, err := f.pubsub.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
	require.NoError(t, err)

	_, err = f.pubsub.Backend.CreateSubscription(
		&pubsubpb.Subscription{Name: "projects/p/subscriptions/logsub", Topic: topic},
	)
	require.NoError(t, err)

	f.sink(t, "to-topic", "pubsub.googleapis.com/"+topic)
	f.write(t, "hello sink")

	resp, err := f.pubsub.SubscriberAPI().Pull(t.Context(), &pubsubpb.PullRequest{
		Subscription:      "projects/p/subscriptions/logsub",
		MaxMessages:       5,
		ReturnImmediately: true, //nolint:staticcheck // immediate pull
	})
	require.NoError(t, err)
	require.Len(t, resp.GetReceivedMessages(), 1)

	msg := resp.GetReceivedMessages()[0].GetMessage()
	assert.Contains(t, string(msg.GetData()), `"textPayload":"hello sink"`)
	assert.Contains(t, msg.GetAttributes(), "logging.googleapis.com/timestamp")
}

func TestLogSinkToStorage(t *testing.T) {
	t.Parallel()

	f := newLogSinkFixture(t)

	_, err := f.gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": []byte(`"archive"`)})
	require.NoError(t, err)

	f.sink(t, "to-gcs", "storage.googleapis.com/archive")
	f.write(t, "first")
	f.write(t, "second")

	store := gcsObjectStore{b: f.gcs.Backend}
	names, err := store.ListObjects("archive", "app/")
	require.NoError(t, err)
	require.Len(t, names, 1, "both entries land in the same hourly file")
	assert.True(t, strings.HasSuffix(names[0], "_S0.json"))

	data, err := store.ReadObject("archive", names[0])
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], "first")
	assert.Contains(t, lines[1], "second")
}

func TestLogSinkToBigQuery(t *testing.T) {
	t.Parallel()

	f := newLogSinkFixture(t)

	_, err := f.bq.Backend.CreateDataset(
		"p",
		map[string]any{"datasetReference": map[string]any{"projectId": "p", "datasetId": "logs"}},
	)
	require.NoError(t, err)

	f.sink(t, "to-bq", "bigquery.googleapis.com/projects/p/datasets/logs", func(s *loggingpb.LogSink) {
		s.Options = &loggingpb.LogSink_BigqueryOptions{
			BigqueryOptions: &loggingpb.BigQueryOptions{UsePartitionedTables: true},
		}
	})
	f.write(t, "row one")
	f.write(t, "row two")

	res, err := f.bq.Backend.ListData("p", "logs", "app", gcpbigquerybackend.DataOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, "2", res["totalRows"])
}
