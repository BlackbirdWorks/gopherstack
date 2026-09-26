package sqs_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// fakeClock is a mutex-guarded, manually-advanced time source injected into
// InMemoryBackend via sqs.SetNowFunc, proving timing decisions read the
// backend's single clock rather than calling time.Now directly.
type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

func newClockedBackend(t *testing.T) (*sqs.InMemoryBackend, *fakeClock) {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	clock := newFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	sqs.SetNowFunc(b, clock.Now)

	return b, clock
}

func TestClockInjection_VisibilityTimeoutExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		advance     time.Duration
		wantVisible bool
	}{
		{name: "before_timeout_stays_inflight", advance: 4 * time.Second, wantVisible: false},
		{name: "after_timeout_returns_to_queue", advance: 6 * time.Second, wantVisible: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clock := newClockedBackend(t)
			qURL := createTestQueue(t, b, "vis-"+tt.name)

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "body"})
			require.NoError(t, err)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   5,
			})
			require.NoError(t, err)
			require.Len(t, out.Messages, 1)

			clock.Advance(tt.advance)

			out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				assert.Len(t, out2.Messages, 1, "message should be returned to the queue after visibility expiry")
			} else {
				assert.Empty(t, out2.Messages, "message should still be in-flight")
			}
		})
	}
}

func TestClockInjection_RetentionExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		advance     time.Duration
		wantMessage bool
	}{
		{name: "before_retention_message_stays", advance: 4 * time.Second, wantMessage: true},
		{name: "after_retention_message_dropped", advance: 6 * time.Second, wantMessage: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clock := newClockedBackend(t)
			qURL := createTestQueue(t, b, "ret-"+tt.name)
			b.SetRetentionForTest(qURL, 5)

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "body"})
			require.NoError(t, err)

			clock.Advance(tt.advance)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)

			if tt.wantMessage {
				assert.Len(t, out.Messages, 1)
			} else {
				assert.Empty(t, out.Messages, "message should be dropped after retention expiry")
			}
		})
	}
}

func TestClockInjection_DelaySecondsHidesMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		advance     time.Duration
		wantVisible bool
	}{
		{name: "before_delay_hidden", advance: 4 * time.Second, wantVisible: false},
		{name: "after_delay_visible", advance: 6 * time.Second, wantVisible: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clock := newClockedBackend(t)
			qURL := createTestQueue(t, b, "delay-"+tt.name)

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qURL,
				MessageBody:  "body",
				DelaySeconds: 5,
			})
			require.NoError(t, err)

			clock.Advance(tt.advance)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				assert.Len(t, out.Messages, 1)
			} else {
				assert.Empty(t, out.Messages, "message should still be delayed")
			}
		})
	}
}

func TestClockInjection_DedupWindowExpiryAllowsResend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		advance       time.Duration
		wantSameMsgID bool
	}{
		{name: "within_window_returns_original", advance: 1 * time.Second, wantSameMsgID: true},
		{name: "after_window_accepts_resend", advance: 301 * time.Second, wantSameMsgID: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clock := newClockedBackend(t)
			qURL := createTestQueue(t, b, "dedup-"+tt.name+".fifo")

			out1, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "body",
				MessageGroupID:         "group",
				MessageDeduplicationID: "dedup-1",
			})
			require.NoError(t, err)

			clock.Advance(tt.advance)

			out2, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "body",
				MessageGroupID:         "group",
				MessageDeduplicationID: "dedup-1",
			})
			require.NoError(t, err)

			if tt.wantSameMsgID {
				assert.Equal(t, out1.MessageID, out2.MessageID,
					"duplicate within the dedup window should return the original message ID")
			} else {
				assert.NotEqual(t, out1.MessageID, out2.MessageID,
					"resend after the dedup window expired should be treated as a new message")
			}
		})
	}
}
