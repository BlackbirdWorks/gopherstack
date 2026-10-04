package lambda

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mskTestARN = "arn:aws:kafka:us-east-1:000000000000:cluster/c/uuid"

type fakeMSKResolver struct {
	servers map[string][]string
	mu      sync.Mutex
}

func (f *fakeMSKResolver) BootstrapServers(arn string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.servers[arn]
}

func (f *fakeMSKResolver) set(arn string, servers ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.servers = map[string][]string{arn: servers}
}

func TestMSKESMFollowsClusterBroker(t *testing.T) {
	t.Parallel()

	b := newKafkaTestBackend(t)
	res := &fakeMSKResolver{}
	tracker := &consumerTracker{created: make(chan *fakeKafkaConsumer, 4)}
	deliveries := make(chan []byte, 4)

	p := NewEventSourcePoller(b, nil)
	p.SetMSKBrokerResolver(res)
	p.kafkaFactory = func(cfg kafkaConsumerConfig) (kafkaConsumer, error) {
		tracker.mu.Lock()
		tracker.cfgs = append(tracker.cfgs, cfg)
		tracker.mu.Unlock()

		f := newFakeKafkaConsumer(kafkaOffsets(2))
		tracker.created <- f

		return f, nil
	}
	p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
		deliveries <- payload

		return nil
	}

	t.Cleanup(p.stopAllKafka)

	_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		FunctionName: "fn", EventSourceARN: mskTestARN, Enabled: true, Topics: []string{"orders"}, BatchSize: 2,
	})
	require.NoError(t, err)

	assert.Equal(t, 1, p.poll(t.Context()))
	assert.Empty(t, p.kafkaWorkers, "metadata-only cluster is not polled")
	assert.Len(t, p.kafkaUnsupported, 1)

	res.set(mskTestARN, "127.0.0.1:19092")
	assert.Equal(t, 1, p.poll(t.Context()))

	cons := awaitConsumer(t, tracker.created)

	tracker.mu.Lock()
	cfg := tracker.cfgs[0]
	tracker.mu.Unlock()

	assert.Equal(t, []string{"127.0.0.1:19092"}, cfg.Brokers)
	assert.Equal(t, []string{"orders"}, cfg.Topics)

	var ev map[string]any

	require.NoError(t, json.Unmarshal(<-deliveries, &ev))
	assert.Equal(t, "aws:kafka", ev["eventSource"])
	assert.Equal(t, mskTestARN, ev["eventSourceArn"])
	assert.Equal(t, "127.0.0.1:19092", ev["bootstrapServers"])
	assert.Len(t, eventRecords(t, ev, "t-0"), 2)

	res.set(mskTestARN)
	p.poll(t.Context())
	awaitClosed(t, cons)
	assert.Empty(t, p.kafkaWorkers)
}
