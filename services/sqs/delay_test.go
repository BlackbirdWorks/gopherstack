package sqs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestDelayQueue_MessageLevelDelay tests that a message with a per-message
// DelaySeconds is hidden until the delay expires.
func TestDelayQueue_MessageLevelDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		delaySeconds int
		waitBefore   time.Duration
		wantVisible  bool
	}{
		{
			name:         "message_not_visible_before_delay",
			delaySeconds: 2,
			waitBefore:   0,
			wantVisible:  false,
		},
		{
			name:         "message_visible_after_delay",
			delaySeconds: 1,
			waitBefore:   1500 * time.Millisecond,
			wantVisible:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			qURL := createTestQueue(t, b, "delay-msg-"+tt.name)

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qURL,
				MessageBody:  "delayed-body",
				DelaySeconds: tt.delaySeconds,
			})
			require.NoError(t, err)

			if tt.waitBefore > 0 {
				time.Sleep(tt.waitBefore)
			}

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				require.Len(t, out.Messages, 1)
				assert.Equal(t, "delayed-body", out.Messages[0].Body)
			} else {
				assert.Empty(t, out.Messages, "message should still be delayed")
			}
		})
	}
}

// TestDelayQueue_QueueLevelDelay tests that the queue's DelaySeconds attribute
// is applied to all messages when no per-message delay is specified.
func TestDelayQueue_QueueLevelDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		queueDelay      string
		msgDelaySeconds int
		waitBefore      time.Duration
		wantVisible     bool
	}{
		{
			name:        "queue_delay_hides_message_immediately",
			queueDelay:  "2",
			waitBefore:  0,
			wantVisible: false,
		},
		{
			name:        "queue_delay_message_visible_after_wait",
			queueDelay:  "1",
			waitBefore:  1500 * time.Millisecond,
			wantVisible: true,
		},
		{
			name:            "message_delay_overrides_zero_queue_delay",
			queueDelay:      "0",
			msgDelaySeconds: 2,
			waitBefore:      0,
			wantVisible:     false,
		},
		{
			name:            "message_delay_overrides_queue_delay",
			queueDelay:      "60",
			msgDelaySeconds: 1,
			waitBefore:      1500 * time.Millisecond,
			wantVisible:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "delay-q-" + tt.name,
				Endpoint:  testEndpoint,
				Attributes: map[string]string{
					"DelaySeconds": tt.queueDelay,
				},
			})
			require.NoError(t, err)

			qURL := out.QueueURL

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qURL,
				MessageBody:  "body",
				DelaySeconds: tt.msgDelaySeconds,
			})
			require.NoError(t, err)

			if tt.waitBefore > 0 {
				time.Sleep(tt.waitBefore)
			}

			recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				require.Len(t, recv.Messages, 1, "message should be visible after delay")
			} else {
				assert.Empty(t, recv.Messages, "message should still be hidden by delay")
			}
		})
	}
}

// TestDelayQueue_ApproximateNumberOfMessagesDelayed verifies the
// ApproximateNumberOfMessagesDelayed attribute reflects delayed message count.
func TestDelayQueue_ApproximateNumberOfMessagesDelayed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantDelayed       string
		wantApproxVisible string
		delaySeconds      int
	}{
		{
			name:              "single_delayed_message",
			delaySeconds:      30,
			wantDelayed:       "1",
			wantApproxVisible: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			qURL := createTestQueue(t, b, "delayed-attr-"+tt.name)

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qURL,
				MessageBody:  "body",
				DelaySeconds: tt.delaySeconds,
			})
			require.NoError(t, err)

			attrOut, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       qURL,
				AttributeNames: []string{"All"},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantDelayed, attrOut.Attributes["ApproximateNumberOfMessagesDelayed"])
			assert.Equal(t, tt.wantApproxVisible, attrOut.Attributes["ApproximateNumberOfMessages"])
		})
	}
}
