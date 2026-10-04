package sqs_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

type pointRecorder struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *pointRecorder) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func (r *pointRecorder) find(name, queue string) []cwmetric.Point {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []cwmetric.Point

	for _, p := range r.points {
		if p.Name == name && len(p.Dimensions) == 1 && p.Dimensions[0].Value == queue {
			out = append(out, p)
		}
	}

	return out
}

func TestMetrics_CarryQueueNameDimension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(t *testing.T, b *sqs.InMemoryBackend, url string)
		name     string
		metric   string
		wantUnit string
		wantSum  float64
	}{
		{
			name: "sent", metric: "NumberOfMessagesSent", wantUnit: "Count", wantSum: 2,
			run: func(t *testing.T, b *sqs.InMemoryBackend, url string) {
				t.Helper()

				_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: url, MessageBody: "a"})
				require.NoError(t, err)
				_, err = b.SendMessageBatch(&sqs.SendMessageBatchInput{
					QueueURL: url, Entries: []sqs.SendMessageBatchEntry{{ID: "1", MessageBody: "b"}},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "sent-size", metric: "SentMessageSize", wantUnit: "Bytes", wantSum: 5,
			run: func(t *testing.T, b *sqs.InMemoryBackend, url string) {
				t.Helper()

				_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: url, MessageBody: "abc"})
				require.NoError(t, err)
				_, err = b.SendMessageBatch(&sqs.SendMessageBatchInput{
					QueueURL: url, Entries: []sqs.SendMessageBatchEntry{{ID: "1", MessageBody: "de"}},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "received-and-deleted", metric: "NumberOfMessagesDeleted", wantUnit: "Count", wantSum: 1,
			run: func(t *testing.T, b *sqs.InMemoryBackend, url string) {
				t.Helper()

				_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: url, MessageBody: "a"})
				require.NoError(t, err)
				out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL: url, VisibilityTimeout: sqs.NoVisibilityTimeout,
				})
				require.NoError(t, err)
				require.Len(t, out.Messages, 1)
				require.NoError(t, b.DeleteMessage(&sqs.DeleteMessageInput{
					QueueURL: url, ReceiptHandle: out.Messages[0].ReceiptHandle,
				}))
			},
		},
		{
			name: "empty-receive", metric: "NumberOfEmptyReceives", wantUnit: "Count", wantSum: 1,
			run: func(t *testing.T, b *sqs.InMemoryBackend, url string) {
				t.Helper()

				_, err := b.ReceiveMessage(
					&sqs.ReceiveMessageInput{QueueURL: url, VisibilityTimeout: sqs.NoVisibilityTimeout},
				)
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			rec := &pointRecorder{}
			b.SetMetricEmitter(rec)

			q, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dim-q"})
			require.NoError(t, err)
			_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "other-q"})
			require.NoError(t, err)

			tc.run(t, b, q.QueueURL)

			require.Eventually(t, func() bool {
				sum := 0.0
				for _, p := range rec.find(tc.metric, "dim-q") {
					sum += p.Value
				}

				return sum == tc.wantSum
			}, 2*time.Second, 5*time.Millisecond)

			for _, p := range rec.find(tc.metric, "dim-q") {
				assert.Equal(t, "AWS/SQS", p.Namespace)
				assert.Equal(t, tc.wantUnit, p.Unit)
				assert.Equal(t, "QueueName", p.Dimensions[0].Name)
			}

			assert.Empty(t, rec.find(tc.metric, "other-q"))
		})
	}
}

func TestMetrics_DepthGaugesPerQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queue      string
		send       int
		receive    int
		wantVis    float64
		wantNotVis float64
	}{
		{name: "visible-only", queue: "g-vis", send: 3, wantVis: 3},
		{name: "in-flight", queue: "g-flight", send: 3, receive: 2, wantVis: 1, wantNotVis: 2},
		{name: "idle", queue: "g-idle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			rec := &pointRecorder{}
			b.SetMetricEmitter(rec)

			q, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: tc.queue})
			require.NoError(t, err)

			for range tc.send {
				_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: q.QueueURL, MessageBody: "m"})
				require.NoError(t, err)
			}

			if tc.receive > 0 {
				_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL: q.QueueURL, MaxNumberOfMessages: tc.receive, VisibilityTimeout: sqs.NoVisibilityTimeout,
				})
				require.NoError(t, err)
			}

			b.RunJanitorOnceForTest(time.Now())

			gauge := func(name string) func() float64 {
				return func() float64 {
					pts := rec.find(name, tc.queue)
					if len(pts) == 0 {
						return -1
					}

					return pts[len(pts)-1].Value
				}
			}

			require.Eventually(t, func() bool {
				return gauge("ApproximateNumberOfMessagesVisible")() == tc.wantVis &&
					gauge("ApproximateNumberOfMessagesNotVisible")() == tc.wantNotVis &&
					gauge("ApproximateNumberOfMessagesDelayed")() == 0
			}, 2*time.Second, 5*time.Millisecond)
		})
	}
}
