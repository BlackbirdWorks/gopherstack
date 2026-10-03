package lambda

import (
	"context"
	"encoding/base64"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

type recordingRuntime struct {
	kafka.BrokerRuntime
	ids []string
	mu  sync.Mutex
}

func (r *recordingRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.BrokerRuntime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingRuntime) Close() error {
	if c, ok := r.BrokerRuntime.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (r *recordingRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func TestMSKESMRealBroker(t *testing.T) {
	t.Parallel()

	testcontainers.SkipIfProviderIsNotHealthy(t)

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	rec := &recordingRuntime{BrokerRuntime: rt}
	kb := kafka.NewInMemoryBackend("000000000000", "us-east-1")
	kb.EnableBrokers(kafka.BrokerConfig{Runtime: rec, StartTimeout: kafkaStartupTimeout})
	t.Cleanup(kb.Close)

	cluster, err := kb.CreateCluster(t.Context(), "real-msk", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterStateCreating, cluster.State)

	require.Eventually(t, func() bool {
		c, descErr := kb.DescribeCluster(t.Context(), cluster.ClusterArn)

		return descErr == nil && c.State == kafka.ClusterStateActive
	}, kafkaStartupTimeout, 500*time.Millisecond)

	servers := kb.BootstrapServers(cluster.ClusterArn)
	require.Len(t, servers, 1)

	producer, err := kgo.NewClient(kgo.SeedBrokers(servers...), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, producer.Ping(t.Context()))

	const topic = "msk-orders"

	for _, v := range []string{`{"id":1}`, `{"id":2}`} {
		res := producer.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte(v)})
		require.NoError(t, res.FirstErr())
	}

	b := newKafkaTestBackend(t)
	deliveries := make(chan []byte, 8)
	p := NewEventSourcePoller(b, nil)
	p.SetMSKBrokerResolver(kb)
	p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
		deliveries <- payload

		return nil
	}
	b.SetKinesisPoller(p)
	b.StartKinesisPoller(context.Background())

	_, err = b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		FunctionName:                   "fn",
		EventSourceARN:                 cluster.ClusterArn,
		Enabled:                        true,
		Topics:                         []string{topic},
		StartingPosition:               "TRIM_HORIZON",
		BatchSize:                      10,
		MaximumBatchingWindowInSeconds: 1,
	})
	require.NoError(t, err)

	ev := awaitDelivery(t, deliveries)
	assert.Equal(t, "aws:kafka", ev["eventSource"])
	assert.Equal(t, cluster.ClusterArn, ev["eventSourceArn"])
	assert.Equal(t, servers[0], ev["bootstrapServers"])

	recs := eventRecords(t, ev, topic+"-0")
	require.Len(t, recs, 2)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(`{"id":1}`)), recs[0]["value"])
	assert.Equal(t, topic, recs[0]["topic"])

	ids := rec.containerIDs()
	require.Len(t, ids, 1)

	dc, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	_, err = dc.ContainerInspect(t.Context(), ids[0], client.ContainerInspectOptions{})
	require.NoError(t, err, "broker container runs while the cluster exists")

	require.NoError(t, kb.DeleteCluster(t.Context(), cluster.ClusterArn))

	require.Eventually(t, func() bool {
		_, inspectErr := dc.ContainerInspect(t.Context(), ids[0], client.ContainerInspectOptions{})

		return inspectErr != nil
	}, time.Minute, 500*time.Millisecond, "DeleteCluster removes the broker container")
}
