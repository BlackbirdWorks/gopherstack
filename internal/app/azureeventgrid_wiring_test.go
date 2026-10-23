package app

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

const egStorageID = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct"

func TestReserveFixedServicePorts_AzureEventGrid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
		want bool
	}{
		{name: "default port inside pool", port: azureeventgrid.DefaultPort, want: true},
		{name: "port outside pool", port: 9996, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(10000, 10100)
			require.NoError(t, err)

			cli := CLI{AzureEventGrid: azureeventgrid.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)
			assert.Equal(t, tt.want, alloc.IsAllocated(tt.port))
		})
	}
}

func TestAzureEventGridDeliversIntoSiblingEmulators(t *testing.T) {
	t.Parallel()

	sb, queues, blobs, hubs := azureservicebus.NewInMemoryBackend(), azurequeue.NewInMemoryBackend(),
		azureblob.NewInMemoryBackend(), azureeventhubs.NewInMemoryBackend()

	_, err := sb.CreateQueue("q1")
	require.NoError(t, err)
	_, err = sb.CreateTopic("tp1")
	require.NoError(t, err)
	_, err = queues.CreateQueue("events")
	require.NoError(t, err)
	require.NoError(t, blobs.CreateContainer("dead"))
	require.NoError(t, hubs.EnsureNamespace("ehns"))
	_, err = hubs.CreateHub(
		azureeventhubs.HubRef{Namespace: "ehns", Hub: "hub1"}, azureeventhubs.HubConfig{PartitionCount: 1},
	)
	require.NoError(t, err)

	eg := azureeventgrid.NewInMemoryBackend()
	eg.SetTargets(&azureEventGridTargets{serviceBus: sb, queues: queues, blobs: blobs, eventHubs: hubs})

	_, err = eg.PutTopic("t1", "/subscriptions/s/resourceGroups/rg/providers/Microsoft.EventGrid/topics/t1", "")
	require.NoError(t, err)

	sbBase := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ServiceBus/namespaces/ns/"
	ehID := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.EventHub/namespaces/ehns/eventhubs/hub1"
	dests := map[string]map[string]any{
		"sbq": {"endpointType": "ServiceBusQueue", "properties": map[string]any{"resourceId": sbBase + "queues/q1"}},
		"sbt": {"endpointType": "ServiceBusTopic", "properties": map[string]any{"resourceId": sbBase + "topics/tp1"}},
		"eh":  {"endpointType": "EventHub", "properties": map[string]any{"resourceId": ehID}},
		"sq": {"endpointType": "StorageQueue", "properties": map[string]any{
			"resourceId": egStorageID, "queueName": "events",
		}},
	}

	for name, dest := range dests {
		_, err = eg.PutSubscription(t.Context(), "t1", name, map[string]any{"destination": dest})
		require.NoError(t, err)
	}

	// A subscription whose target queue does not exist dead-letters after its single attempt.
	missing := map[string]any{"resourceId": egStorageID, "queueName": "missing"}
	deadBlob := map[string]any{"resourceId": egStorageID, "blobContainerName": "dead"}

	_, err = eg.PutSubscription(t.Context(), "t1", "dl", map[string]any{
		"destination":           map[string]any{"endpointType": "StorageQueue", "properties": missing},
		"retryPolicy":           map[string]any{"maxDeliveryAttempts": 1},
		"deadLetterDestination": map[string]any{"endpointType": "StorageBlob", "properties": deadBlob},
	})
	require.NoError(t, err)

	raw := `{"id":"e1","subject":"/s","eventType":"App.Thing","eventTime":"2026-10-20T12:00:00Z",` +
		`"dataVersion":"1","data":{"n":1}}`
	events, err := azureeventgrid.ParseEvents([]byte(raw), azureeventgrid.SchemaEventGrid)
	require.NoError(t, err)
	require.NoError(t, eg.Publish("t1", events))
	require.Equal(t, 5, eg.RunDue(t.Context()))

	msg, err := sb.PeekLock(azureservicebus.EntityRef{Queue: "q1"}, false, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "e1", jsonField(t, msg.Body, "id"))

	got, err := queues.GetMessages("events", 5, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "e1", jsonField(t, []byte(got[0].Text), "id"))

	info, err := hubs.PartitionInfo(azureeventhubs.HubRef{Namespace: "ehns", Hub: "hub1"}, 0)
	require.NoError(t, err)
	assert.False(t, info.Empty)

	list, err := blobs.ListBlobs("dead")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Contains(t, list[0].Name, "t1/dl/")
}

func jsonField(t *testing.T, raw []byte, key string) string {
	t.Helper()

	var m map[string]any

	require.NoError(t, json.Unmarshal(raw, &m))

	s, _ := m[key].(string)

	return s
}

func TestAzureEventGridTargetsWithoutBackends(t *testing.T) {
	t.Parallel()

	tg := &azureEventGridTargets{}
	m := azureeventgrid.Message{Body: []byte("{}")}

	require.ErrorIs(t, tg.SendServiceBus("q", "", m), azureeventgrid.ErrTargetUnavailable)
	require.ErrorIs(t, tg.SendEventHub("ns", "h", m), azureeventgrid.ErrTargetUnavailable)
	require.ErrorIs(t, tg.SendStorageQueue("a", "q", "x", 0), azureeventgrid.ErrTargetUnavailable)
	require.ErrorIs(t, tg.PutBlob("a", "c", "b", nil, ""), azureeventgrid.ErrTargetUnavailable)

	wireAzureEventGrid(map[string]service.Registerable{})
}
