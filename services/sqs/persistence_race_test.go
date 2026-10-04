package sqs_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestSnapshot_RacesWithSendReceiveMessage reproduces gopherstack-fwd0g: Snapshot read Queue fields outside q.mu.
// SendMessage/ReceiveMessage mutate those fields, and the *Message values they point at, under q.mu; run with -race.
func TestSnapshot_RacesWithSendReceiveMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, b *sqs.InMemoryBackend, queueURL string)
		name   string
	}{
		{
			name: "concurrent_snapshot_and_send_message",
			mutate: func(t *testing.T, b *sqs.InMemoryBackend, queueURL string) {
				t.Helper()

				for i := range 200 {
					_, err := b.SendMessage(&sqs.SendMessageInput{
						QueueURL:    queueURL,
						MessageBody: fmt.Sprintf("body-%03d", i),
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name: "concurrent_snapshot_and_receive_message",
			mutate: func(t *testing.T, b *sqs.InMemoryBackend, queueURL string) {
				t.Helper()

				for i := range 200 {
					_, err := b.SendMessage(&sqs.SendMessageInput{
						QueueURL:    queueURL,
						MessageBody: fmt.Sprintf("body-%03d", i),
					})
					require.NoError(t, err)
				}

				for range 200 {
					_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueURL:            queueURL,
						MaxNumberOfMessages: 1,
					})
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(b.Close)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "race-snapshot-queue"})
			require.NoError(t, err)

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

			tt.mutate(t, b, out.QueueURL)
			close(stop)
			wg.Wait()
		})
	}
}
