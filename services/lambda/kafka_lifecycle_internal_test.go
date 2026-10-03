package lambda

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kafkaTestDeadline = 10 * time.Second

type consumerTracker struct {
	created chan *fakeKafkaConsumer
	cfgs    []kafkaConsumerConfig
	mu      sync.Mutex
}

func (c *consumerTracker) factory(cfg kafkaConsumerConfig) (kafkaConsumer, error) {
	c.mu.Lock()
	c.cfgs = append(c.cfgs, cfg)
	c.mu.Unlock()

	f := newFakeKafkaConsumer()
	c.created <- f

	return f, nil
}

func awaitConsumer(t *testing.T, ch <-chan *fakeKafkaConsumer) *fakeKafkaConsumer {
	t.Helper()

	select {
	case f := <-ch:
		return f
	case <-time.After(kafkaTestDeadline):
		require.FailNow(t, "timed out waiting for a Kafka consumer")

		return nil
	}
}

func awaitClosed(t *testing.T, f *fakeKafkaConsumer) {
	t.Helper()

	select {
	case <-f.closed:
	case <-time.After(kafkaTestDeadline):
		require.FailNow(t, "timed out waiting for the Kafka consumer to close")
	}
}

func selfManagedInput(group string) *CreateEventSourceMappingInput {
	cfg := &SelfManagedKafkaEventSourceConfig{ConsumerGroupID: group}
	if group == "" {
		cfg = nil
	}

	return &CreateEventSourceMappingInput{
		FunctionName:                      "fn",
		Enabled:                           true,
		StartingPosition:                  "AT_TIMESTAMP",
		Topics:                            []string{"orders"},
		BatchSize:                         5,
		SelfManagedKafkaEventSourceConfig: cfg,
		SelfManagedEventSource: &SelfManagedEventSource{
			Endpoints: map[string][]string{kafkaBootstrapEndpointKey: {"broker-1:9092", "broker-2:9092"}},
		},
		StartingPositionTimestamp: 1700000000.5,
	}
}

func startKafkaPoller(t *testing.T) (*InMemoryBackend, *consumerTracker) {
	t.Helper()

	b := NewInMemoryBackend(nil, nil, DefaultSettings(), "000000000000", "us-east-1")
	tracker := &consumerTracker{created: make(chan *fakeKafkaConsumer, 8)}
	p := NewEventSourcePoller(b, nil)
	p.kafkaFactory = tracker.factory
	b.SetKinesisPoller(p)
	b.StartKinesisPoller(context.Background())

	t.Cleanup(func() { b.Close(context.Background()) })

	return b, tracker
}

func TestKafkaESMLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		group string
		want  string
	}{
		{"consumer group defaults to the mapping uuid", "", ""},
		{"consumer group honors ConsumerGroupId", "my-group", "my-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, tracker := startKafkaPoller(t)

			esm, err := b.CreateEventSourceMapping(selfManagedInput(tt.group))
			require.NoError(t, err)
			assert.Empty(t, esm.EventSourceARN)

			first := awaitConsumer(t, tracker.created)

			want := tt.want
			if want == "" {
				want = esm.UUID
			}

			tracker.mu.Lock()
			cfg := tracker.cfgs[0]
			tracker.mu.Unlock()

			assert.Equal(t, want, cfg.GroupID)
			assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, cfg.Brokers)
			assert.Equal(t, []string{"orders"}, cfg.Topics)
			assert.Equal(t, "AT_TIMESTAMP", cfg.StartingPosition)
			assert.Equal(t, int64(1700000000), cfg.StartTimestamp.Unix())

			disabled := false
			_, err = b.UpdateEventSourceMapping(esm.UUID, &UpdateEventSourceMappingInput{Enabled: &disabled})
			require.NoError(t, err)
			awaitClosed(t, first)

			enabled := true
			_, err = b.UpdateEventSourceMapping(esm.UUID, &UpdateEventSourceMappingInput{Enabled: &enabled})
			require.NoError(t, err)

			second := awaitConsumer(t, tracker.created)

			_, err = b.DeleteEventSourceMapping(esm.UUID)
			require.NoError(t, err)
			awaitClosed(t, second)
		})
	}
}

func TestKafkaESMBatchSizeChangeRestartsWorker(t *testing.T) {
	t.Parallel()

	b, tracker := startKafkaPoller(t)

	esm, err := b.CreateEventSourceMapping(selfManagedInput(""))
	require.NoError(t, err)

	first := awaitConsumer(t, tracker.created)
	size := int32(50)

	_, err = b.UpdateEventSourceMapping(esm.UUID, &UpdateEventSourceMappingInput{BatchSize: &size})
	require.NoError(t, err)

	awaitClosed(t, first)
	awaitConsumer(t, tracker.created)
}

func TestKafkaESMCloseStopsWorkers(t *testing.T) {
	t.Parallel()

	b, tracker := startKafkaPoller(t)

	_, err := b.CreateEventSourceMapping(selfManagedInput(""))
	require.NoError(t, err)

	cons := awaitConsumer(t, tracker.created)

	b.Close(context.Background())
	awaitClosed(t, cons)
}

func TestKafkaESMUnsupportedSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{"msk arn is not polled", "arn:aws:kafka:us-east-1:000000000000:cluster/c/uuid", false},
		{"mq arn is not polled", "arn:aws:mq:us-east-1:000000000000:broker:b:b-123", false},
		{"missing source rejected", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			tracker := &consumerTracker{created: make(chan *fakeKafkaConsumer, 8)}
			p := NewEventSourcePoller(b, nil)
			p.kafkaFactory = tracker.factory

			_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
				FunctionName: "fn", EventSourceARN: tt.arn, Enabled: true, Topics: []string{"t"},
			})
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameterValue)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, p.poll(t.Context()))
			assert.Empty(t, p.kafkaWorkers)
			assert.Empty(t, tracker.created)
			assert.Len(t, p.kafkaUnsupported, 1)
		})
	}
}
