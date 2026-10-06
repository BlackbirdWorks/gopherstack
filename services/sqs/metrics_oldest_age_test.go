package sqs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestMetrics_ApproximateAgeOfOldestMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		send    int
		receive int
		wantMin float64
	}{
		{name: "visible", send: 2, wantMin: 90},
		{name: "in-flight-counts", send: 1, receive: 1, wantMin: 90},
		{name: "empty", wantMin: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			rec := &pointRecorder{}
			b.SetMetricEmitter(rec)

			q, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "age-q"})
			require.NoError(t, err)

			for range tc.send {
				_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: q.QueueURL, MessageBody: "m"})
				require.NoError(t, err)
			}

			if tc.receive > 0 {
				_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL: q.QueueURL, MaxNumberOfMessages: tc.receive, VisibilityTimeout: 600,
				})
				require.NoError(t, err)
			}

			b.RunJanitorOnceForTest(time.Now().Add(90 * time.Second))

			require.Eventually(t, func() bool {
				return len(rec.find("ApproximateAgeOfOldestMessage", "age-q")) > 0
			}, 2*time.Second, 5*time.Millisecond)

			pts := rec.find("ApproximateAgeOfOldestMessage", "age-q")
			last := pts[len(pts)-1]
			assert.Equal(t, "Seconds", last.Unit)
			assert.GreaterOrEqual(t, last.Value, tc.wantMin)

			if tc.send == 0 {
				assert.Zero(t, last.Value)
			}
		})
	}
}
