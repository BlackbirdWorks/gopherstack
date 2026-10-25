package sqs_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestSettings_PurgeCooldown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*sqs.Settings)
		wantErr error
	}{
		{name: "default_enforced", mutate: func(*sqs.Settings) {}, wantErr: sqs.ErrPurgeQueueInProgress},
		{name: "disabled", mutate: func(s *sqs.Settings) { s.DelayPurgeRetry = false }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			s := sqs.DefaultSettings()
			tt.mutate(&s)
			b.ApplySettings(s)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q"})
			require.NoError(t, err)

			require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: out.QueueURL}))
			assert.ErrorIs(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: out.QueueURL}), tt.wantErr)
		})
	}
}

func TestSettings_RecentlyDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*sqs.Settings)
		wantErr error
	}{
		{name: "default_enforced", mutate: func(*sqs.Settings) {}, wantErr: sqs.ErrQueueDeletedRecently},
		{name: "disabled", mutate: func(s *sqs.Settings) { s.DelayRecentlyDeleted = false }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			s := sqs.DefaultSettings()
			tt.mutate(&s)
			b.ApplySettings(s)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q"})
			require.NoError(t, err)
			require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: out.QueueURL}))

			_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q"})
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestSettings_MessageRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		retention    bool
		wantMessages int
	}{
		{name: "enforced", retention: true, wantMessages: 0},
		{name: "disabled", retention: false, wantMessages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now()
			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			sqs.SetNowFunc(b, func() time.Time { return now })

			s := sqs.DefaultSettings()
			s.EnableRetentionPeriod = tt.retention
			b.ApplySettings(s)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q"})
			require.NoError(t, err)

			b.SetRetentionForTest(out.QueueURL, 60)

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: out.QueueURL, MessageBody: "x"})
			require.NoError(t, err)

			now = now.Add(2 * time.Minute)

			got, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: out.QueueURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Len(t, got.Messages, tt.wantMessages)
		})
	}
}

func TestSettings_CloudWatchMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		disable bool
		want    bool
	}{
		{name: "emitted", disable: false, want: true},
		{name: "disabled", disable: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var emitted atomic.Bool

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			b.SetMetricEmitter(cwmetric.EmitterFunc(func(cwmetric.Point) error {
				emitted.Store(true)

				return nil
			}))

			s := sqs.DefaultSettings()
			s.DisableCloudWatchMetric = tt.disable
			b.ApplySettings(s)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q"})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: out.QueueURL, MessageBody: "x"})
			require.NoError(t, err)

			if tt.want {
				assert.Eventually(t, emitted.Load, 2*time.Second, 5*time.Millisecond)
			} else {
				assert.Never(t, emitted.Load, 100*time.Millisecond, 10*time.Millisecond)
			}
		})
	}
}
