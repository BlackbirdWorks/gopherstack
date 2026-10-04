package kinesis_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestSnapshot_RacesWithConsumerWrites reproduces gopherstack-fwd0g: Snapshot marshals streams under b.mu.RLock alone.
// Register/DeregisterStreamConsumer mutate Stream.Consumers under stream.mu after releasing b.mu; run with -race.
func TestSnapshot_RacesWithConsumerWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, b *kinesis.InMemoryBackend, streamARN string)
		name   string
	}{
		{
			name: "concurrent_snapshot_and_register_deregister_consumer",
			mutate: func(t *testing.T, b *kinesis.InMemoryBackend, streamARN string) {
				t.Helper()

				for i := range 100 {
					name := fmt.Sprintf("consumer-%03d", i)

					_, err := b.RegisterStreamConsumer(t.Context(), &kinesis.RegisterStreamConsumerInput{
						StreamARN:    streamARN,
						ConsumerName: name,
					})
					require.NoError(t, err)

					err = b.DeregisterStreamConsumer(t.Context(), &kinesis.DeregisterStreamConsumerInput{
						StreamARN:    streamARN,
						ConsumerName: name,
					})
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			const streamName = "race-snapshot-stream"

			require.NoError(t, b.CreateStream(t.Context(), &kinesis.CreateStreamInput{
				StreamName: streamName,
				ShardCount: 1,
			}))

			streamARN := arn.Build("kinesis", config.DefaultRegion, config.DefaultAccountID, "stream/"+streamName)

			var wg sync.WaitGroup

			stop := make(chan struct{})

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					_ = b.Snapshot(t.Context())
				}
			})

			tt.mutate(t, b, streamARN)
			close(stop)
			wg.Wait()
		})
	}
}
