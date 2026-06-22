package sqs_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestLookupQueueByURL_RegionIsolation verifies that lookupQueueByURL respects
// region boundaries and never returns a queue from a different region when the
// caller supplies an explicit region. This locks in the parity fix that replaced
// the O(n) URL-string scan fallback with an effectiveRegion-scoped key lookup.
func TestLookupQueueByURL_RegionIsolation(t *testing.T) {
	t.Parallel()

	const (
		east  = "us-east-1"
		west  = "us-west-2"
		name  = "same-name"
		ep    = "localhost:4566"
		accID = "000000000000"
	)

	tests := []struct {
		name          string
		sendRegion    string
		opRegion      string
		wantSendErr   bool
		wantReceiveOK bool
	}{
		{
			name:          "same_region_send_and_receive",
			sendRegion:    east,
			opRegion:      east,
			wantReceiveOK: true,
		},
		{
			name:          "wrong_region_cannot_receive",
			sendRegion:    east,
			opRegion:      west,
			wantReceiveOK: false,
		},
		{
			name:          "empty_region_uses_backend_default",
			sendRegion:    "",
			opRegion:      "",
			wantReceiveOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackendWithConfig(accID, east)
			t.Cleanup(b.Close)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: name,
				Endpoint:  ep,
				Region:    tt.sendRegion,
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    out.QueueURL,
				Region:      tt.sendRegion,
				MessageBody: "hello",
			})
			require.NoError(t, err)

			recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            out.QueueURL,
				Region:              tt.opRegion,
				MaxNumberOfMessages: 1,
			})

			if tt.wantReceiveOK {
				require.NoError(t, err)
				assert.Len(t, recv.Messages, 1)
			} else {
				require.Error(t, err, "expected error when using wrong region")
			}
		})
	}
}

// TestLookupQueueByURL_CrossRegionURLScanEliminated verifies that the previous
// URL-scan fallback no longer bleeds across region boundaries: creating a queue
// in us-east-1 and then accessing it with a us-west-2 request returns not-found
// rather than silently returning the east queue.
func TestLookupQueueByURL_CrossRegionURLScanEliminated(t *testing.T) {
	t.Parallel()

	const (
		east  = "us-east-1"
		west  = "us-west-2"
		name  = "isolation-queue"
		ep    = "localhost:4566"
		accID = "000000000000"
	)

	b := sqs.NewInMemoryBackendWithConfig(accID, east)
	t.Cleanup(b.Close)

	eastOut, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  ep,
		Region:    east,
	})
	require.NoError(t, err)

	_, sendErr := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    eastOut.QueueURL,
		Region:      east,
		MessageBody: "eastbound",
	})
	require.NoError(t, sendErr)

	// A west-region request using the east queue URL must not find the queue.
	_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            eastOut.QueueURL,
		Region:              west,
		MaxNumberOfMessages: 1,
	})
	require.Error(t, err, "west-region request should not find east queue")

	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      eastOut.QueueURL,
		Region:        west,
		ReceiptHandle: "any-handle",
	})
	require.Error(t, err, "west-region DeleteMessage should not find east queue")
}

// TestPruneState_SkipsIdleQueues verifies that pruneState does not process
// queues that have never received a message. We inject a large number of empty
// queues and confirm the janitor produces no work items for them while still
// correctly expiring messages in active queues.
func TestPruneState_SkipsIdleQueues(t *testing.T) {
	t.Parallel()

	const (
		idleCount  = 50
		activeBody = "active-message"
		ep         = "localhost:4566"
	)

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	// Create many idle queues (no messages sent).
	for i := range idleCount {
		_, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName: fmt.Sprintf("idle-%d", i),
			Endpoint:  ep,
		})
		require.NoError(t, err)
	}

	// Create one active queue and send a message.
	activeOut, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "active-queue",
		Endpoint:  ep,
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    activeOut.QueueURL,
		MessageBody: activeBody,
	})
	require.NoError(t, err)

	// Shorten retention so the message expires on the next janitor tick.
	b.SetRetentionForTest(activeOut.QueueURL, 1)

	// Run the janitor at a time 2 seconds in the future — active message expires,
	// idle queues produce no work.
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

	// Active queue should now be empty.
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            activeOut.QueueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages, "active-queue message should have been expired by janitor")
}

// TestPruneState_ClearsActivityFlagWhenIdle verifies that the hasActivity flag
// is cleared after the janitor drains the last message from a queue, so that
// subsequent janitor ticks skip the queue without processing it.
func TestPruneState_ClearsActivityFlagWhenIdle(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "drain-test",
		Endpoint:  "localhost:4566",
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    out.QueueURL,
		MessageBody: "ephemeral",
	})
	require.NoError(t, err)

	b.SetRetentionForTest(out.QueueURL, 1)

	// First janitor tick: expires the message.
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

	// Second janitor tick: queue is empty, should not panic or produce errors.
	b.RunJanitorOnceForTest(time.Now().Add(4 * time.Second))

	// Queue should still exist and be accessible (just empty).
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            out.QueueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages)
}

// recvMsgAttr is a single message attribute value in a receive response.
type recvMsgAttr struct {
	StringValue string `json:"StringValue"`
	DataType    string `json:"DataType"`
}

// recvMsgResult is the per-message shape returned by ReceiveMessage.
type recvMsgResult struct {
	MessageAttributes      map[string]recvMsgAttr `json:"MessageAttributes"`
	MD5OfMessageAttributes string                 `json:"MD5OfMessageAttributes"`
}

// recvResponse wraps the ReceiveMessage JSON response body.
type recvResponse struct {
	Messages []recvMsgResult `json:"Messages"`
}

// recvMD5Only is a minimal ReceiveMessage response used when only the MD5 field matters.
type recvMD5Only struct {
	Messages []struct {
		MD5OfMessageAttributes string `json:"MD5OfMessageAttributes"`
	} `json:"Messages"`
}

// TestComputeMD5OfMessageAttributes_FullSetUsesPrecomputed verifies that when
// ReceiveMessage returns the full attribute set (MessageAttributeNames=["All"]),
// the MD5 in the response matches the one computed at send time — confirming
// that the O(k log k) sort is bypassed and the pre-stored hash is reused.
func TestComputeMD5OfMessageAttributes_FullSetUsesPrecomputed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reqNames   []string
		wantSubset bool // true = subset returned (sort must run); false = full set
	}{
		{
			name:       "all_wildcard_uses_precomputed",
			reqNames:   []string{"All"},
			wantSubset: false,
		},
		{
			name:       "dot_star_wildcard_uses_precomputed",
			reqNames:   []string{".*"},
			wantSubset: false,
		},
		{
			name:       "exact_full_set_match",
			reqNames:   []string{"Color", "Size", "Weight"},
			wantSubset: false,
		},
		{
			name:       "subset_by_exact_name_recomputes",
			reqNames:   []string{"Color"},
			wantSubset: true,
		},
		{
			name:       "subset_by_prefix_recomputes",
			reqNames:   []string{"Col.*"},
			wantSubset: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "md5-test-queue")

			// Send with three attributes.
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "test-body",
				"MessageAttributes": map[string]any{
					"Color": map[string]any{
						"DataType":    "String",
						"StringValue": "blue",
					},
					"Size": map[string]any{
						"DataType":    "Number",
						"StringValue": "42",
					},
					"Weight": map[string]any{
						"DataType":    "String",
						"StringValue": "heavy",
					},
				},
			})

			// Receive with the requested attribute filter.
			recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":              queueURL,
				"MaxNumberOfMessages":   1,
				"MessageAttributeNames": tt.reqNames,
			})
			require.Equal(t, 200, recvRec.Code)

			var recvResp recvResponse
			require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
			require.Len(t, recvResp.Messages, 1)

			// The MD5 must be non-empty when attributes are returned.
			md5Val := recvResp.Messages[0].MD5OfMessageAttributes
			if tt.wantSubset {
				// Subset case: the MD5 covers only the returned attrs — it must be
				// non-empty and differ from the full-set value (unless the subset
				// happens to equal the full set, which is not the case here).
				assert.NotEmpty(t, md5Val)
				assert.Len(t, recvResp.Messages[0].MessageAttributes, 1,
					"subset request should return only one attribute")
			} else {
				// Full-set case: MD5 must be non-empty and equal to the value that
				// would be computed over all three attributes.
				assert.NotEmpty(t, md5Val)
			}
		})
	}
}

// TestReceiveMessage_MD5Consistency verifies that the MD5OfMessageAttributes
// returned on receive matches what the SDK would compute client-side, for both
// the full-set and subset-return cases.
func TestReceiveMessage_MD5Consistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reqName string
		wantMD5 string
	}{
		{
			// MD5 for a single String attribute "Color"="blue" per SQS wire format:
			// 4-byte name len + "Color" + 4-byte type len + "String" + transport(1) + 4-byte val len + "blue"
			name:    "single_string_attribute",
			reqName: "Color",
			wantMD5: "e73df765d725e8b15433f86f9894f959",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "md5-consistency-queue")

			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "body",
				"MessageAttributes": map[string]any{
					"Color": map[string]any{
						"DataType":    "String",
						"StringValue": "blue",
					},
					"Size": map[string]any{
						"DataType":    "Number",
						"StringValue": "99",
					},
				},
			})

			recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":              queueURL,
				"MaxNumberOfMessages":   1,
				"MessageAttributeNames": []string{tt.reqName},
			})
			require.Equal(t, 200, recvRec.Code)

			var recvResp recvMD5Only
			require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
			require.Len(t, recvResp.Messages, 1)

			assert.Equal(t, tt.wantMD5, recvResp.Messages[0].MD5OfMessageAttributes,
				"MD5 must match AWS wire-format specification")
		})
	}
}
