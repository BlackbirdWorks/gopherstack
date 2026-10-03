package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaImage           = "apache/kafka:3.9.1"
	kafkaStartupTimeout  = 3 * time.Minute
	kafkaDeliveryTimeout = 90 * time.Second
)

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return addr.Port
}

// startKafkaBroker runs a single-node KRaft broker advertised on a fixed host port.
func startKafkaBroker(t *testing.T) string {
	t.Helper()

	testcontainers.SkipIfProviderIsNotHealthy(t)

	port := freeTCPPort(t)
	bootstrap := "127.0.0.1:" + strconv.Itoa(port)

	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		Image:        kafkaImage,
		ExposedPorts: []string{"9092/tcp"},
		HostConfigModifier: func(hc *dockercontainer.HostConfig) {
			p, parseErr := network.ParsePort("9092/tcp")
			require.NoError(t, parseErr)

			hc.PortBindings = network.PortMap{p: {{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(port)}}}
		},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + bootstrap,
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(kafkaStartupTimeout),
		Started:    true,
	})
	if ctr != nil {
		testcontainers.CleanupContainer(t, ctr)
	}

	require.NoError(t, err)

	return bootstrap
}

func awaitDelivery(t *testing.T, ch <-chan []byte) map[string]any {
	t.Helper()

	select {
	case raw := <-ch:
		var ev map[string]any

		require.NoError(t, json.Unmarshal(raw, &ev))

		return ev
	case <-time.After(kafkaDeliveryTimeout):
		require.FailNow(t, "timed out waiting for the Kafka batch invocation")

		return nil
	}
}

func eventRecords(t *testing.T, ev map[string]any, key string) []map[string]any {
	t.Helper()

	groups, ok := ev["records"].(map[string]any)
	require.True(t, ok)

	list, ok := groups[key].([]any)
	require.True(t, ok)

	out := make([]map[string]any, 0, len(list))

	for _, r := range list {
		rec, isMap := r.(map[string]any)
		require.True(t, isMap)

		out = append(out, rec)
	}

	return out
}

func TestKafkaESMRealBroker(t *testing.T) {
	t.Parallel()

	bootstrap := startKafkaBroker(t)

	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	b := newKafkaTestBackend(t)
	deliveries := make(chan []byte, 8)
	p := NewEventSourcePoller(b, nil)
	p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
		deliveries <- payload

		return nil
	}

	b.SetKinesisPoller(p)
	b.StartKinesisPoller(context.Background())

	const (
		topic = "orders"
		group = "gopherstack-it-group"
	)

	newESM := func(filter *FilterCriteria) *EventSourceMapping {
		in := selfManagedInput(group)
		in.StartingPosition = "TRIM_HORIZON"
		in.StartingPositionTimestamp = 0
		in.Topics = []string{topic}
		in.BatchSize = 10
		in.MaximumBatchingWindowInSeconds = 1
		in.FilterCriteria = filter
		in.SelfManagedEventSource = &SelfManagedEventSource{
			Endpoints: map[string][]string{kafkaBootstrapEndpointKey: {bootstrap}},
		}

		esm, createErr := b.CreateEventSourceMapping(in)
		require.NoError(t, createErr)

		return esm
	}

	produce := func(key, value string, headers ...kgo.RecordHeader) {
		res := producer.ProduceSync(t.Context(), &kgo.Record{
			Topic: topic, Key: []byte(key), Value: []byte(value), Headers: headers,
		})
		require.NoError(t, res.FirstErr())
	}

	produce("k0", `{"type":"order","id":1}`, kgo.RecordHeader{Key: "h", Value: []byte("v")})
	produce("k1", `{"type":"order","id":2}`)
	produce("k2", `{"type":"refund","id":3}`)

	first := newESM(nil)
	ev := awaitDelivery(t, deliveries)

	assert.Equal(t, "SelfManagedKafka", ev["eventSource"])
	assert.Equal(t, bootstrap, ev["bootstrapServers"])

	recs := eventRecords(t, ev, topic+"-0")
	require.Len(t, recs, 3)

	for i, r := range recs {
		assert.InDelta(t, float64(i), r["offset"], 0)
		assert.Equal(t, topic, r["topic"])
		assert.InDelta(t, 0.0, r["partition"], 0)
		assert.Equal(t, "CREATE_TIME", r["timestampType"])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("k"+strconv.Itoa(i))), r["key"])
	}

	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(`{"type":"order","id":1}`)), recs[0]["value"])
	assert.Equal(t, []any{map[string]any{"h": []any{118.0}}}, recs[0]["headers"])

	awaitProcessed := func(uuid string) {
		require.Eventually(t, func() bool {
			got, getErr := b.GetEventSourceMapping(uuid)

			return getErr == nil && got.LastProcessingResult == "OK"
		}, kafkaDeliveryTimeout, 100*time.Millisecond)
	}

	awaitProcessed(first.UUID)

	_, err = b.DeleteEventSourceMapping(first.UUID)
	require.NoError(t, err)

	produce("k3", `{"type":"refund","id":4}`)
	produce("k4", `{"type":"order","id":5}`)

	filter := &FilterCriteria{Filters: []Filter{{Pattern: `{"value":{"type":["order"]}}`}}}
	second := newESM(filter)

	ev = awaitDelivery(t, deliveries)
	recs = eventRecords(t, ev, topic+"-0")
	require.Len(t, recs, 1, "committed offsets 0-2 must not be redelivered; refund is filtered out")
	assert.InDelta(t, 4.0, recs[0]["offset"], 0)

	awaitProcessed(second.UUID)
}
