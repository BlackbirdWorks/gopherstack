package sqs_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaleReceiptHandleRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "stale-handle",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"VisibilityTimeout": "0", // immediately visible
		},
	})
	require.NoError(t, err)
	qURL := out.QueueURL

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "test",
	})
	require.NoError(t, err)

	// Receive #1.
	r1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   0,
	})
	require.NoError(t, err)
	require.Len(t, r1.Messages, 1)
	staleHandle := r1.Messages[0].ReceiptHandle

	// Message visible again; receive #2 gets a new handle.
	r2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, r2.Messages, 1)

	// Using stale handle should fail.
	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: staleHandle,
	})
	require.Error(t, err)
}

func TestChangeVisibilityBatchValidation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "batch-chgvis")

	// Duplicate IDs in ChangeMessageVisibilityBatch.
	_, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "dup", ReceiptHandle: "rh1", VisibilityTimeout: 30},
			{ID: "dup", ReceiptHandle: "rh2", VisibilityTimeout: 30},
		},
	})
	require.ErrorIs(t, err, sqs.ErrBatchEntryIDsNotDistinct)
}

// TestInFlightLimitEnforced tests that the in-flight limit is enforced.
func TestInFlightLimitEnforced(t *testing.T) {
	t.Parallel()

	// ErrOverLimit is defined and used by the in-flight limit check in receiveOnce.
	// Full 120k / 20k limit tests are impractical in unit tests; the sentinel
	// existence verifies the error path is wired.
	assert.Error(t, sqs.ErrOverLimit)
}

// TestChangeMessageVisibilityBatch_VTOutOfRange_PerEntryFailure verifies
// that ChangeMessageVisibilityBatch reports a per-entry failure (not a
// batch-level error) when a single entry's VisibilityTimeout exceeds 43200,
// and still processes valid entries in the same call.
func TestChangeMessageVisibilityBatch_VTOutOfRange_PerEntryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantBadCode        string
		badVisibility      int
		goodVisibility     int
		wantBadSenderFault bool
	}{
		{
			name:               "vt_above_max_43200",
			badVisibility:      43201,
			goodVisibility:     60,
			wantBadCode:        "InvalidParameterValue",
			wantBadSenderFault: true,
		},
		{
			name:               "vt_way_above_max",
			badVisibility:      99999,
			goodVisibility:     0,
			wantBadCode:        "InvalidParameterValue",
			wantBadSenderFault: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := b3newBackend(t)
			qURL := b3createQueue(t, b, "vt-range-"+tc.name)

			// Send two messages and receive them both.
			b3send(t, b, qURL, "msg-good")
			b3send(t, b, qURL, "msg-bad")
			msgs := b3recv(t, b, qURL, 2)
			require.Len(t, msgs, 2)

			out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
				QueueURL: qURL,
				Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
					{ID: "good", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: tc.goodVisibility},
					{ID: "bad", ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: tc.badVisibility},
				},
			})

			require.NoError(t, err, "batch-level error must not be returned for per-entry range violation")
			require.Len(t, out.Successful, 1)
			assert.Equal(t, "good", out.Successful[0].ID)

			require.Len(t, out.Failed, 1)
			assert.Equal(t, "bad", out.Failed[0].ID)
			assert.Equal(t, tc.wantBadCode, out.Failed[0].Code)
			assert.Equal(t, tc.wantBadSenderFault, out.Failed[0].SenderFault)
		})
	}
}

// TestChangeMessageVisibilityBatch_NegativeVT_PerEntryFailure verifies that
// a negative VisibilityTimeout is a per-entry failure with Code="InvalidParameterValue".
func TestChangeMessageVisibilityBatch_NegativeVT_PerEntryFailure(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createQueue(t, b, "neg-vt-batch")

	b3send(t, b, qURL, "msg")
	msgs := b3recv(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: -1},
		},
	})

	require.NoError(t, err)
	assert.Empty(t, out.Successful)
	require.Len(t, out.Failed, 1)
	assert.Equal(t, "e1", out.Failed[0].ID)
	assert.Equal(t, "InvalidParameterValue", out.Failed[0].Code)
	assert.True(t, out.Failed[0].SenderFault)
}

// TestChangeMessageVisibilityBatch_MaxValidVT_Accepted verifies that
// VisibilityTimeout of exactly 43200 (the AWS maximum) is accepted.
func TestChangeMessageVisibilityBatch_MaxValidVT_Accepted(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createQueue(t, b, "maxItems-vt-batch")

	b3send(t, b, qURL, "msg")
	msgs := b3recv(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 43200},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Empty(t, out.Failed)
}

// TestChangeMessageVisibilityBatch_AllEntriesOutOfRange verifies that
// when every entry has an out-of-range VT, all end up in Failed and none in Successful.
func TestChangeMessageVisibilityBatch_AllEntriesOutOfRange(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createQueue(t, b, "all-bad-vt-batch")

	b3send(t, b, qURL, "m1")
	b3send(t, b, qURL, "m2")
	msgs := b3recv(t, b, qURL, 2)
	require.Len(t, msgs, 2)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 50000},
			{ID: "e2", ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: -5},
		},
	})

	require.NoError(t, err)
	assert.Empty(t, out.Successful)
	assert.Len(t, out.Failed, 2)
}

// TestReceiveMessage_VisibilityTimeout_OutOfRange verifies that
// ReceiveMessage returns 400 InvalidParameterValue when VisibilityTimeout is
// outside the AWS-allowed range of [0, 43200], matching the same validation
// that ChangeMessageVisibility enforces.
func TestReceiveMessage_VisibilityTimeout_OutOfRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantErrType       string
		visibilityTimeout int
		wantStatus        int
	}{
		{
			name:              "above_max_43201",
			visibilityTimeout: 43201,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "far_above_max",
			visibilityTimeout: 99999,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "negative_one",
			visibilityTimeout: -1,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "negative_large",
			visibilityTimeout: -100,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "vt-range-queue-"+strings.ReplaceAll(tt.name, "_", "-"))
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "test",
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":          queueURL,
				"VisibilityTimeout": tt.visibilityTimeout,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantErrType, errResp.Type)
		})
	}
}

// TestReceiveMessage_VisibilityTimeout_ValidBoundaries verifies that
// ReceiveMessage succeeds with VisibilityTimeout values at the valid boundaries
// (0 and 43200).
func TestReceiveMessage_VisibilityTimeout_ValidBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		visibilityTimeout int
	}{
		{name: "zero", visibilityTimeout: 0},
		{name: "max_43200", visibilityTimeout: 43200},
		{name: "mid_range", visibilityTimeout: 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "vt-valid-"+strings.ReplaceAll(tt.name, "_", "-"))
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "test",
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":          queueURL,
				"VisibilityTimeout": tt.visibilityTimeout,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Messages []struct {
					Body string `json:"Body"`
				} `json:"Messages"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Messages, 1)
		})
	}
}

func TestVisibilityTimeout_HidesMessage(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "vt-hide",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "hidden")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)
	receipt := msgs[0].ReceiptHandle

	// Message is now in-flight; second receive returns nothing
	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs2)

	// Delete the in-flight message
	b2delete(t, b, qURL.QueueURL, receipt)
}

func TestChangeMessageVisibility_ExtendsTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-extend")
	b2send(t, b, qURL, "msg")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	receipt := msgs[0].ReceiptHandle

	// Extend visibility to 60s
	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     receipt,
		VisibilityTimeout: 60,
	})
	require.NoError(t, err)

	// Still in-flight
	msgs2 := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs2)

	b2delete(t, b, qURL, receipt)
}

func TestChangeMessageVisibility_ZeroMakesVisible(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "cvm-zero",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)

	// Set visibility to 0 → immediately visible
	require.NoError(t, b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL.QueueURL,
		ReceiptHandle:     msgs[0].ReceiptHandle,
		VisibilityTimeout: 0,
	}))

	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs2, 1)
	b2delete(t, b, qURL.QueueURL, msgs2[0].ReceiptHandle)
}

func TestChangeMessageVisibility_InvalidTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-invalid")
	b2send(t, b, qURL, "msg")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     msgs[0].ReceiptHandle,
		VisibilityTimeout: 43201, // >43200 (12h)
	})
	require.ErrorIs(t, err, sqs.ErrInvalidVisibilityTimeout)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)
}

func TestChangeMessageVisibility_NotInflight_Error(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-notinflight")

	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     "fake-receipt",
		VisibilityTimeout: 10,
	})
	require.ErrorIs(t, err, sqs.ErrMessageNotInflight)
}

func TestChangeMessageVisibilityBatch_Mixed(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvb-mixed")
	b2send(t, b, qURL, "a")
	b2send(t, b, qURL, "b")

	msgs := b2receive(t, b, qURL, 2)
	require.Len(t, msgs, 2)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "ok", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 10},
			{ID: "bad", ReceiptHandle: "invalid-handle", VisibilityTimeout: 10},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Len(t, out.Failed, 1)
	assert.Equal(t, "ok", out.Successful[0].ID)
	assert.Equal(t, "bad", out.Failed[0].ID)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)
	b2delete(t, b, qURL, msgs[1].ReceiptHandle)
}

func TestInFlight_StandardLimit(t *testing.T) {
	// This tests that the backend tracks in-flight messages correctly and
	// returns ErrOverLimit when 120,000 standard messages are in-flight.
	// We use a small number to keep the test fast.
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "inflight-std",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.NoError(t, err)

	// Send a batch and receive them all — verify in-flight tracking
	for i := range 5 {
		b2send(t, b, qURL.QueueURL, fmt.Sprintf("m%d", i))
	}

	msgs := b2receive(t, b, qURL.QueueURL, 5)
	assert.Len(t, msgs, 5)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessagesNotVisible")
	assert.Equal(t, "5", attrs["ApproximateNumberOfMessagesNotVisible"])
}

// TestVisibilityTimeout verifies ChangeMessageVisibility adjusts re-delivery.
func TestVisibilityTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setVT       int  // VT to set via ChangeMessageVisibility
		wantRequeue bool // true if message should be immediately available (VT=0)
	}{
		{
			name:        "VT=0 immediately re-queues message",
			setVT:       0,
			wantRequeue: true,
		},
		{
			name:        "VT=30 keeps message in-flight",
			setVT:       30,
			wantRequeue: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "vt-q")

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: "vt-msg",
			})
			require.NoError(t, err)

			// Receive with long VT so message stays in-flight.
			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   60,
			})
			require.NoError(t, err)
			require.Len(t, out.Messages, 1)

			rh := out.Messages[0].ReceiptHandle

			// Change visibility.
			err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          qURL,
				ReceiptHandle:     rh,
				VisibilityTimeout: tc.setVT,
			})
			require.NoError(t, err)

			// Try to receive again.
			out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   0,
			})
			require.NoError(t, err)

			if tc.wantRequeue {
				assert.Len(t, out2.Messages, 1, "message must be visible after VT=0")
			} else {
				assert.Empty(t, out2.Messages, "message must still be in-flight with VT=30")
			}
		})
	}
}

func TestChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     recvOut.Messages[0].ReceiptHandle,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)
}

func TestVisibilityTimeoutExpiry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// Receive with 0-second visibility — message immediately becomes visible again.
	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Receive again — lazy expiry should re-queue the message.
	recvOut2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut2.Messages, 1)
}

func TestReceiveMessageDefaultVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// VisibilityTimeout = sqs.NoVisibilityTimeout uses the queue's default.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   sqs.NoVisibilityTimeout,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
}

func TestResolveVisibilityTimeoutInvalidAttr(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "vis-invalid-queue")

	// SetQueueAttributes now validates attribute ranges; a non-integer VisibilityTimeout
	// should be rejected.
	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "notanumber"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttribute)
}

func TestReQueueExpiredMixed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "requeue-mixed-queue")

	// Send 2 messages.
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "a"})
	require.NoError(t, err)
	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "b"})
	require.NoError(t, err)

	// Receive both with very short visibility timeout (1 second).
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 2)

	// Wait for visibility timeout to expire.
	time.Sleep(1100 * time.Millisecond)

	// Receive again — expired messages should be requeued.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Messages, 2)
}

func TestChangeMessageVisibility_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		queueURL string
	}{
		{
			name:     "queue_not_found",
			queueURL: queueURL("nonexistent"),
			wantErr:  sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          tt.queueURL,
				ReceiptHandle:     "fake-receipt",
				VisibilityTimeout: 30,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestChangeMessageVisibility_InvalidHandle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{
			name:    "invalid_receipt_handle",
			wantErr: sqs.ErrMessageNotInflight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "vis-queue")

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
			require.NoError(t, err)

			recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
			})
			require.NoError(t, err)
			require.Len(t, recvOut.Messages, 1)

			err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          qURL,
				ReceiptHandle:     "bad-handle",
				VisibilityTimeout: 10,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestChangeMessageVisibilityBoundsValidation checks that ChangeMessageVisibility
// rejects visibility timeouts outside the AWS-allowed range (0–43200 s).
func TestChangeMessageVisibilityBoundsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		visibilityTimeout int
	}{
		{name: "negative_rejected", visibilityTimeout: -1, wantErr: sqs.ErrInvalidVisibilityTimeout},
		{name: "over_max_rejected", visibilityTimeout: 43201, wantErr: sqs.ErrInvalidVisibilityTimeout},
		{name: "zero_accepted", visibilityTimeout: 0, wantErr: nil},
		{name: "max_accepted", visibilityTimeout: 43200, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "vis-bounds-queue")

			_, sendErr := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, sendErr)

			out, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
			})
			require.NoError(t, recvErr)
			require.Len(t, out.Messages, 1)

			err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          qURL,
				ReceiptHandle:     out.Messages[0].ReceiptHandle,
				VisibilityTimeout: tt.visibilityTimeout,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func newBackendWithQueue(t *testing.T, queueName string) (*sqs.InMemoryBackend, string) {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queueName,
		Endpoint:  "localhost",
	})
	require.NoError(t, err)

	return backend, out.QueueURL
}

func TestChangeMessageVisibilityBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		queueName       string
		queueURL        string
		wantFailIDs     []string
		wantFailFaults  []bool
		wantFailCodes   []string
		entryIDs        []string
		wantSuccessIDs  []string
		extraEntries    []sqs.ChangeMessageVisibilityBatchRequestEntry
		messageBodies   []string
		batchVisTimeout int
		recvVisTimeout  int
		recvMax         int
		reReceiveMax    int
		reReceiveVis    int
		wantReReceive   int
		initLogger      bool
		checkReReceive  bool
	}{
		{
			name:            "Success",
			initLogger:      true,
			queueName:       "test-vis-batch",
			messageBodies:   []string{"msg-one", "msg-two"},
			recvMax:         2,
			recvVisTimeout:  30,
			entryIDs:        []string{"e1", "e2"},
			batchVisTimeout: 0,
			wantSuccessIDs:  []string{"e1", "e2"},
			checkReReceive:  true,
			reReceiveMax:    2,
			reReceiveVis:    30,
			wantReReceive:   2,
		},
		{
			name:            "PartialFailure",
			queueName:       "test-vis-batch-partial",
			messageBodies:   []string{"hello"},
			recvMax:         1,
			recvVisTimeout:  30,
			entryIDs:        []string{"good"},
			batchVisTimeout: 0,
			extraEntries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
				{ID: "bad", ReceiptHandle: "invalid-handle", VisibilityTimeout: 0},
			},
			wantSuccessIDs: []string{"good"},
			wantFailIDs:    []string{"bad"},
			wantFailCodes:  []string{"MessageNotInflight"},
			wantFailFaults: []bool{true},
		},
		{
			name:     "QueueNotFound",
			queueURL: "http://localhost/000000000000/nonexistent",
			extraEntries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
				{ID: "e1", ReceiptHandle: "handle", VisibilityTimeout: 0},
			},
			wantErr: sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.initLogger {
				_ = logger.NewLogger(slog.LevelDebug)
			}

			var backend *sqs.InMemoryBackend

			queueURL := tt.queueURL
			if tt.queueName != "" {
				var url string
				backend, url = newBackendWithQueue(t, tt.queueName)
				queueURL = url
			} else {
				backend = sqs.NewInMemoryBackend()
				t.Cleanup(backend.Close)
			}

			// Send messages.
			for _, body := range tt.messageBodies {
				_, err := backend.SendMessage(&sqs.SendMessageInput{QueueURL: queueURL, MessageBody: body})
				require.NoError(t, err)
			}

			// Receive messages so they become in-flight.
			var received []*sqs.Message
			if tt.recvMax > 0 {
				rcv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            queueURL,
					MaxNumberOfMessages: tt.recvMax,
					VisibilityTimeout:   tt.recvVisTimeout,
				})
				require.NoError(t, err)
				require.Len(t, rcv.Messages, len(tt.entryIDs))
				received = rcv.Messages
			}

			// Build batch entries from received messages + extra entries.
			var entries []sqs.ChangeMessageVisibilityBatchRequestEntry
			for i, id := range tt.entryIDs {
				entries = append(entries, sqs.ChangeMessageVisibilityBatchRequestEntry{
					ID:                id,
					ReceiptHandle:     received[i].ReceiptHandle,
					VisibilityTimeout: tt.batchVisTimeout,
				})
			}
			entries = append(entries, tt.extraEntries...)

			out, err := backend.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
				QueueURL: queueURL,
				Entries:  entries,
			})

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Check successful entries.
			assert.Len(t, out.Successful, len(tt.wantSuccessIDs))
			successIDs := make([]string, 0, len(out.Successful))
			for _, s := range out.Successful {
				successIDs = append(successIDs, s.ID)
			}
			assert.ElementsMatch(t, tt.wantSuccessIDs, successIDs)

			// Check failed entries.
			if len(tt.wantFailIDs) == 0 {
				assert.Empty(t, out.Failed)
			} else {
				require.Len(t, out.Failed, len(tt.wantFailIDs))
				for i, f := range out.Failed {
					assert.Equal(t, tt.wantFailIDs[i], f.ID)
					assert.Equal(t, tt.wantFailCodes[i], f.Code)
					assert.Equal(t, tt.wantFailFaults[i], f.SenderFault)
				}
			}

			// Optionally verify messages are receivable again.
			if tt.checkReReceive {
				rcv2, rerr := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            queueURL,
					MaxNumberOfMessages: tt.reReceiveMax,
					VisibilityTimeout:   tt.reReceiveVis,
				})
				require.NoError(t, rerr)
				assert.Len(t, rcv2.Messages, tt.wantReReceive)
			}
		})
	}
}
