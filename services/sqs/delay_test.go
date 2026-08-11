package sqs_test

import (
	"strconv"
	"testing"
	"testing/synctest"
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

			synctest.Test(t, func(t *testing.T) {
				b := sqs.NewInMemoryBackend()
				t.Cleanup(b.Close)
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

			synctest.Test(t, func(t *testing.T) {
				b := sqs.NewInMemoryBackend()
				t.Cleanup(b.Close)

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
		})
	}
}

// TestDelayQueue_InvalidDelaySecondsRange verifies that DelaySeconds outside
// AWS's documented [0, 900] range is rejected by SendMessage, and that a
// SendMessageBatch entry with an out-of-range DelaySeconds is surfaced as a
// per-entry BatchResultErrorEntry rather than being silently accepted.
// Regression test: SendMessageBatch entries bypassed sendMessageLocked's
// range check entirely (the check only lived in the top-level SendMessage
// entry point), so a batch entry with e.g. DelaySeconds: 99999 was silently
// accepted and produced a message effectively delayed by a bogus duration.
func TestDelayQueue_InvalidDelaySecondsRange(t *testing.T) {
	t.Parallel()

	t.Run("SendMessage rejects out-of-range DelaySeconds", func(t *testing.T) {
		t.Parallel()

		b := sqs.NewInMemoryBackend()
		t.Cleanup(b.Close)
		qURL := createTestQueue(t, b, "delay-range-single")

		for _, delay := range []int{-1, 901, 100000} {
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qURL,
				MessageBody:  "body",
				DelaySeconds: delay,
			})
			require.ErrorIs(t, err, sqs.ErrInvalidDelaySeconds, "delay=%d", delay)
		}
	})

	t.Run("SendMessageBatch rejects out-of-range DelaySeconds per entry", func(t *testing.T) {
		t.Parallel()

		b := sqs.NewInMemoryBackend()
		t.Cleanup(b.Close)
		qURL := createTestQueue(t, b, "delay-range-batch")

		out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueURL: qURL,
			Entries: []sqs.SendMessageBatchEntry{
				{ID: "ok", MessageBody: "fine", DelaySeconds: 5},
				{ID: "bad", MessageBody: "bogus", DelaySeconds: 99999},
			},
		})
		require.NoError(t, err)

		require.Len(t, out.Successful, 1)
		assert.Equal(t, "ok", out.Successful[0].ID)

		require.Len(t, out.Failed, 1)
		assert.Equal(t, "bad", out.Failed[0].ID)
		assert.True(t, out.Failed[0].SenderFault)
	})
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
			t.Cleanup(b.Close)
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

func TestQueueDelay_HidesMessageUntilExpiry(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "delay-q",
		Endpoint:   "localhost",
		Attributes: map[string]string{"DelaySeconds": "900"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "delayed")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs, "delayed message should not be visible immediately")

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessagesDelayed")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesDelayed"])
}

func TestMessageDelay_OverridesQueueDelay(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "msg-delay-override",
		Endpoint:   "localhost",
		Attributes: map[string]string{"DelaySeconds": "0"},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:     qURL.QueueURL,
		MessageBody:  "with-delay",
		DelaySeconds: 900,
	})
	require.NoError(t, err)

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs)
}

// TestDelayQueue verifies per-message and queue-level delay.
func TestDelayQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queueDelay  int
		perMsgDelay int
		wantDelayed bool // message not visible immediately
	}{
		{
			name:        "no delay — immediately visible",
			queueDelay:  0,
			perMsgDelay: 0,
			wantDelayed: false,
		},
		{
			name:        "queue-level delay — not visible immediately",
			queueDelay:  900,
			perMsgDelay: 0,
			wantDelayed: true,
		},
		{
			name:        "per-message delay overrides queue delay of zero",
			queueDelay:  0,
			perMsgDelay: 900,
			wantDelayed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			attrs := map[string]string{}
			if tc.queueDelay > 0 {
				attrs["DelaySeconds"] = strconv.Itoa(tc.queueDelay)
			}

			qOut, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "delay-q",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:     qOut.QueueURL,
				MessageBody:  "delayed",
				DelaySeconds: tc.perMsgDelay,
			})
			require.NoError(t, err)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qOut.QueueURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   0,
			})
			require.NoError(t, err)

			if tc.wantDelayed {
				assert.Empty(t, out.Messages, "message must not be visible during delay period")
			} else {
				assert.Len(t, out.Messages, 1, "message must be immediately visible")
			}
		})
	}
}
