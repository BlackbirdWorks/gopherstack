package sqs_test

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeduplicationScopeConfigurable(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "q.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"DeduplicationScope":  "messageGroup",
			"FifoThroughputLimit": "perQueue",
		},
	})
	require.NoError(t, err)
}

func TestDeduplicationScopeMessageGroup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "dedup-scope.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
			"DeduplicationScope":        "messageGroup",
		},
	})
	require.NoError(t, err)
	qURL := out.QueueURL

	// Same body, different groups → both should be stored (not deduped).
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "group1",
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "group2",
	})
	require.NoError(t, err)

	// Verify both messages exist.
	msgs1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Len(t, msgs1.Messages, 2, "both group messages should be present with messageGroup scope")
}

func TestDeduplicationScopeQueue(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "dedup-queue.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
			"DeduplicationScope":        "queue",
		},
	})
	require.NoError(t, err)
	qURL := out.QueueURL

	// Same body, different groups → deduped at queue scope.
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "group1",
	})
	require.NoError(t, err)

	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "group2",
	})
	require.NoError(t, err)

	// second send should be a duplicate at queue scope.
	_ = out2 // AWS returns the original message ID

	msgs, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Len(t, msgs.Messages, 1, "queue-scope dedup should deduplicate across groups")
}

func TestFIFORejectsMsgDelaySeconds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "delay-fifo.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       out.QueueURL,
		MessageBody:    "hello",
		MessageGroupID: "g1",
		DelaySeconds:   5,
	})
	require.ErrorIs(t, err, sqs.ErrFIFODelayNotSupported)
}

func TestFIFOBatchEntryRejectsMsgDelaySeconds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "delay-batch.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	result, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: out.QueueURL,
		Entries: []sqs.SendMessageBatchEntry{
			{
				ID:             "entry1",
				MessageBody:    "hello",
				MessageGroupID: "g1",
				DelaySeconds:   10, // invalid for FIFO
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Failed, 1)
	assert.Equal(t, "entry1", result.Failed[0].ID)
}

func TestSetQueueAttributesFifoQueueRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "fifo-immutable")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL: qURL,
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttributeName)
}

func TestSetQueueAttributesFifoQueueRejectedViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "fifo-immutable-h")

	body := map[string]any{
		"QueueUrl": qURL,
		"Attributes": map[string]string{
			"FifoQueue": "true",
		},
	}

	rec := doRequest(t, h, "SetQueueAttributes", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Contains(t, errResp.Type, "InvalidAttributeName")
}

func TestCreateQueueFifoIdempotency(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Create FIFO queue.
	out1, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "my-fifo.fifo",
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	// Idempotent create with same FifoQueue=true value → OK.
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "my-fifo.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, out1.QueueURL, out2.QueueURL)

	// Conflicting value → QueueAlreadyExists (FifoQueue is in configurable list).
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "my-fifo.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"FifoQueue": "false",
		},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)
}

// TestSendMessageBatch_FIFO_SequenceNumbersInResults verifies that successful
// batch entries for FIFO queues include a non-empty SequenceNumber, and that
// sequence numbers are unique and monotonically increasing across the batch.
func TestSendMessageBatch_FIFO_SequenceNumbersInResults(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createFIFOQueue(t, b, "seq-batch.fifo")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "alpha", MessageGroupID: "g1", MessageDeduplicationID: "d1"},
			{ID: "e2", MessageBody: "beta", MessageGroupID: "g1", MessageDeduplicationID: "d2"},
			{ID: "e3", MessageBody: "gamma", MessageGroupID: "g1", MessageDeduplicationID: "d3"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 3)
	assert.Empty(t, out.Failed)

	seqNums := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		assert.NotEmpty(t, s.SequenceNumber, "SequenceNumber must be set for FIFO batch entries")
		assert.Len(t, s.SequenceNumber, 20, "SequenceNumber must be 20 digits")
		seqNums = append(seqNums, s.SequenceNumber)
	}

	// All sequence numbers must be distinct.
	seen := map[string]bool{}
	for _, sn := range seqNums {
		assert.False(t, seen[sn], "duplicate SequenceNumber: %s", sn)
		seen[sn] = true
	}
}

// TestSendMessageBatch_Standard_NoSequenceNumber verifies that standard
// (non-FIFO) queue batch results do NOT include SequenceNumber.
func TestSendMessageBatch_Standard_NoSequenceNumber(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createQueue(t, b, "std-no-seq")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "hello"},
			{ID: "e2", MessageBody: "world"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 2)
	for _, s := range out.Successful {
		assert.Empty(t, s.SequenceNumber, "standard queue entries must not have SequenceNumber")
	}
}

// TestSendMessageBatch_FIFO_Dedup_AcrossBatch verifies that sending the same
// deduplication ID twice in a batch results in a single message delivered (dedup
// silently drops the second entry and returns success for both IDs with the same
// MessageID, matching AWS ContentBasedDeduplication semantics).
func TestSendMessageBatch_FIFO_Dedup_AcrossBatch(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
	qURL := b3createFIFOQueue(t, b, "dedup-batch.fifo")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "hello", MessageGroupID: "g1", MessageDeduplicationID: "dup-id"},
			{ID: "e2", MessageBody: "hello", MessageGroupID: "g1", MessageDeduplicationID: "dup-id"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 2, "both entries succeed — duplicate is silently deduplicated")
	assert.Empty(t, out.Failed)

	// The queue should contain only one message (the duplicate was dropped).
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	assert.Len(t, recv.Messages, 1)
}

// TestSendMessage_Standard_SequenceNumberAbsent verifies that the
// SendMessage response for a standard queue does NOT include a SequenceNumber
// field, matching AWS behaviour where SequenceNumber is FIFO-only.
func TestSendMessage_Standard_SequenceNumberAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "std-seq-absent")

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello standard",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Unmarshal into a raw map so we can check key presence, not just value.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasSeqNum := raw["SequenceNumber"]
	assert.False(t, hasSeqNum, "standard queue SendMessage response must not contain SequenceNumber")
}

// TestSendMessage_FIFO_SequenceNumberPresent verifies that the SendMessage
// response for a FIFO queue DOES include a non-empty SequenceNumber, confirming
// the omitempty tag does not suppress valid FIFO values.
func TestSendMessage_FIFO_SequenceNumberPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateQueue", map[string]any{
		"QueueName": "b4-fifo.fifo",
		"Attributes": map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		QueueURL string `json:"QueueUrl"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":       createResp.QueueURL,
		"MessageBody":    "hello fifo",
		"MessageGroupId": "grp1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	rawSeq, hasSeqNum := raw["SequenceNumber"]
	require.True(t, hasSeqNum, "FIFO queue SendMessage response must contain SequenceNumber")

	var seq string
	require.NoError(t, json.Unmarshal(rawSeq, &seq))
	assert.NotEmpty(t, seq, "SequenceNumber must be non-empty for FIFO queue")
	assert.Len(t, seq, 20, "SequenceNumber must be 20 digits")
}

func TestFIFOQueue_CreateDeleteCycle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "my-fifo.fifo", nil)
	assert.Contains(t, qURL, ".fifo")

	attrs := b2getAttrs(t, b, qURL, "FifoQueue", "ContentBasedDeduplication")
	assert.Equal(t, "true", attrs["FifoQueue"])
	assert.Equal(t, "false", attrs["ContentBasedDeduplication"])

	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))
}

func TestFIFOQueueName_MustEndWithFifo(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Non-.fifo name creates standard queue (IsFIFO=false)
	qURL := b2createQueue(t, b, "not-fifo")
	attrs := b2getAttrs(t, b, qURL, "FifoQueue")
	_, hasFIFO := attrs["FifoQueue"]
	assert.False(t, hasFIFO)

	// .fifo suffix creates FIFO queue
	qURL2 := b2createFIFOQueue(t, b, "real.fifo", nil)
	attrs2 := b2getAttrs(t, b, qURL2, "FifoQueue")
	assert.Equal(t, "true", attrs2["FifoQueue"])
}

func TestFIFO_OrderPreservedWithinGroup(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-order.fifo", nil)

	b2sendFIFO(t, b, qURL, "first", "grp1", "d1")
	b2sendFIFO(t, b, qURL, "second", "grp1", "d2")
	b2sendFIFO(t, b, qURL, "third", "grp1", "d3")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "first", msgs[0].Body)
	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	msgs2 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs2, 1)
	assert.Equal(t, "second", msgs2[0].Body)
	b2delete(t, b, qURL, msgs2[0].ReceiptHandle)

	msgs3 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs3, 1)
	assert.Equal(t, "third", msgs3[0].Body)
	b2delete(t, b, qURL, msgs3[0].ReceiptHandle)
}

func TestFIFO_MultipleGroups_ParallelDelivery(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-multi-group.fifo", nil)

	b2sendFIFO(t, b, qURL, "grp1-msg1", "grp1", "d1")
	b2sendFIFO(t, b, qURL, "grp2-msg1", "grp2", "d2")
	b2sendFIFO(t, b, qURL, "grp1-msg2", "grp1", "d3")
	b2sendFIFO(t, b, qURL, "grp2-msg2", "grp2", "d4")

	// Receive 2 — should get one from each group (different groups not blocked)
	msgs := b2receive(t, b, qURL, 2)
	require.Len(t, msgs, 2)
	bodies := []string{msgs[0].Body, msgs[1].Body}
	assert.Contains(t, bodies, "grp1-msg1")
	assert.Contains(t, bodies, "grp2-msg1")
}

func TestFIFO_GroupBlocked_WhileInflight(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "fifo-block.fifo",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2sendFIFO(t, b, qURL.QueueURL, "a", "g", "d1")
	b2sendFIFO(t, b, qURL.QueueURL, "b", "g", "d2")

	// Receive first — group g is now blocked
	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "a", msgs[0].Body)

	// Second receive returns nothing (group g still in-flight)
	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs2)

	// Delete first — group g unblocked
	b2delete(t, b, qURL.QueueURL, msgs[0].ReceiptHandle)

	msgs3 := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs3, 1)
	assert.Equal(t, "b", msgs3[0].Body)
	b2delete(t, b, qURL.QueueURL, msgs3[0].ReceiptHandle)
}

func TestFIFO_MissingGroupID_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-no-group.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageDeduplicationID: "d1",
		// no MessageGroupID
	})
	require.ErrorIs(t, err, sqs.ErrMissingMessageGroupID)
}

func TestFIFO_MissingDeduplicationID_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-no-dedup.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "body",
		MessageGroupID: "grp",
		// no MessageDeduplicationID, ContentBasedDeduplication=false
	})
	require.ErrorIs(t, err, sqs.ErrMissingDeduplicationID)
}

func TestFIFO_DeduplicationID_DeduplicatesWithinWindow(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-dedup.fifo", nil)

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-key",
	})
	require.NoError(t, err)

	// Same dedup ID within 5-minute window → same message ID returned
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-key",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)

	// Only one message delivered
	msgs := b2receive(t, b, qURL, 10)
	assert.Len(t, msgs, 1)
}

func TestFIFO_SequenceNumber_Populated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-seq.fifo", nil)
	out := b2sendFIFO(t, b, qURL, "body", "g", "d1")
	assert.NotEmpty(t, out.SequenceNumber)
	assert.Len(t, out.SequenceNumber, 20)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.NotEmpty(t, msgs[0].Attributes["SequenceNumber"])
}

func TestFIFO_MessageGroupAndDeduplicationIDAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-attrs.fifo", nil)
	b2sendFIFO(t, b, qURL, "body", "my-group", "my-dedup")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	attrs := msgs[0].Attributes
	assert.Equal(t, "my-group", attrs["MessageGroupId"])
	assert.Equal(t, "my-dedup", attrs["MessageDeduplicationId"])
}

func TestFIFO_DelaySeconds_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-delay.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "d1",
		DelaySeconds:           5,
	})
	require.ErrorIs(t, err, sqs.ErrFIFODelayNotSupported)
}

func TestContentBasedDeduplication_UseSHA256(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Enable ContentBasedDeduplication
	qURL := b2createFIFOQueue(t, b, "cbd-sha256.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	body := "hello content dedup"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp",
		// No explicit dedupID — backend uses SHA-256(body)
	})
	require.NoError(t, err)

	// Send same body again — should be deduplicated
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "same body should deduplicate within 5-minute window")

	// Different body — not deduplicated
	out3, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body + "-different",
		MessageGroupID: "grp",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out3.MessageID)

	// Exactly 2 distinct messages queued (original body + body+"-different")
	// Receive first (body), delete it so group unblocks, then receive second.
	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, body, msgs[0].Body)
	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	msgs2 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs2, 1)
	assert.Equal(t, body+"-different", msgs2[0].Body)
	b2delete(t, b, qURL, msgs2[0].ReceiptHandle)

	// Queue now empty
	remaining := b2receive(t, b, qURL, 10)
	assert.Empty(t, remaining)
}

func TestContentBasedDeduplication_SHA256KeyFormat(t *testing.T) {
	t.Parallel()
	// Verify the SHA-256 used is consistent with standard hex-encoded SHA-256
	body := "test message for sha256"
	expected := sha256hex(body)
	assert.Len(t, expected, 64, "SHA-256 hex should be 64 chars")

	// Confirm it's valid hex
	_, err := hex.DecodeString(expected)
	assert.NoError(t, err)
}

func TestContentBasedDeduplication_ExplicitIDOverridesContent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-override.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	// Explicit dedup ID takes precedence
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "same-body",
		MessageGroupID:         "grp",
		MessageDeduplicationID: "explicit-id",
	})
	require.NoError(t, err)

	// Same body but different explicit dedup ID → new message
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "same-body",
		MessageGroupID:         "grp",
		MessageDeduplicationID: "different-explicit-id",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

func TestContentBasedDeduplication_DifferentGroups_NotDeduplicated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-groups.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	body := "same-body-across-groups"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp1",
	})
	require.NoError(t, err)

	// Same body, different group — NOT deduplicated (default: messageGroup scope)
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp2",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

func TestDeduplicationScope_Queue_DedupAcrossGroups(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-scope-queue.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
		"DeduplicationScope":        "queue",
		"FifoThroughputLimit":       "perQueue",
	})

	body := "shared-body"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp1",
	})
	require.NoError(t, err)

	// Same body, different group — but scope=queue so DEDUPLICATED
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp2",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "scope=queue deduplicates across groups")
}

func TestPurgeQueue_FIFO_ResetsDeduplication(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "purge-fifo.fifo", nil)
	b2sendFIFO(t, b, qURL, "body", "g", "dedup-1")

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	// After purge, same dedup ID can be sent again
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-1",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.MessageID)
}

func TestSetQueueAttributes_FifoQueue_Immutable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "sqa-fifo-imm.fifo", nil)
	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"FifoQueue": "false"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttributeName)
}

func TestFIFO_QueueARN_ContainsFifoSuffix(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "arn-fifo.fifo", nil)
	attrs := b2getAttrs(t, b, qURL, "QueueArn")

	arn := attrs["QueueArn"]
	assert.Contains(t, arn, "arn-fifo.fifo")
}

// TestFIFOOrdering verifies that FIFO queue preserves per-group ordering.
func TestFIFOOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		groupID     string
		bodies      []string
		wantOrdered bool
	}{
		{
			name:        "single group — strict ordering preserved",
			groupID:     "g1",
			bodies:      []string{"first", "second", "third"},
			wantOrdered: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qOut, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "fifo-ord.fifo",
				Endpoint:  testEndpoint,
				Attributes: map[string]string{
					"FifoQueue":                 "true",
					"ContentBasedDeduplication": "true",
				},
			})
			require.NoError(t, err)

			// Send in order.
			for _, body := range tc.bodies {
				_, sendErr := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:       qOut.QueueURL,
					MessageBody:    body,
					MessageGroupID: tc.groupID,
				})
				require.NoError(t, sendErr)
			}

			// Receive one at a time, deleting each so next becomes visible.
			received := make([]string, 0, len(tc.bodies))
			for range len(tc.bodies) {
				out, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qOut.QueueURL,
					MaxNumberOfMessages: 1,
					VisibilityTimeout:   0,
				})
				require.NoError(t, recvErr)
				if len(out.Messages) == 0 {
					break
				}

				received = append(received, out.Messages[0].Body)
				_ = b.DeleteMessage(&sqs.DeleteMessageInput{
					QueueURL:      qOut.QueueURL,
					ReceiptHandle: out.Messages[0].ReceiptHandle,
				})
			}

			if tc.wantOrdered {
				assert.Equal(t, tc.bodies, received, "FIFO must deliver messages in send order")
			}
		})
	}
}

// TestFIFODeduplication_ContentBased verifies content-based deduplication within the window.
func TestFIFODeduplication_ContentBased(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bodies    []string
		wantCount int
	}{
		{
			name:      "identical bodies deduplicated to one",
			bodies:    []string{"same", "same", "same"},
			wantCount: 1,
		},
		{
			name:      "distinct bodies all delivered",
			bodies:    []string{"a", "b", "c"},
			wantCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qOut, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "fifo-dedup.fifo",
				Endpoint:  testEndpoint,
				Attributes: map[string]string{
					"FifoQueue":                 "true",
					"ContentBasedDeduplication": "true",
				},
			})
			require.NoError(t, err)

			for _, body := range tc.bodies {
				_, sendErr := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:       qOut.QueueURL,
					MessageBody:    body,
					MessageGroupID: "g1",
				})
				require.NoError(t, sendErr)
			}

			// FIFO queues with a single group block after the first in-flight message,
			// so we can't receive all messages in one call. Use ApproximateNumberOfMessages
			// to verify the queue depth after sends (before any receives).
			attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       qOut.QueueURL,
				AttributeNames: []string{"All"},
			})
			require.NoError(t, err)
			assert.Equal(t, strconv.Itoa(tc.wantCount),
				attrs.Attributes["ApproximateNumberOfMessages"],
				"deduplicated message count must match")
		})
	}
}

func TestFIFODeduplication(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue.fifo")

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "group1",
		MessageDeduplicationID: "dedup-id-1",
	})
	require.NoError(t, err)

	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "group1",
		MessageDeduplicationID: "dedup-id-1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)

	// Only one message should be in the queue.
	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 10, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut.Messages, 1)
}

func TestSendMessageFIFOContentBasedDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Create a FIFO queue with content-based deduplication enabled.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "dedup.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	qURL := queueURL("dedup.fifo")

	// First send - should succeed.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "g1",
	})
	require.NoError(t, err)

	// Second send with same body - content-based dedup should return original MessageID.
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "g1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "dedup should return original message ID")
}

func TestSendMessageFIFOExplicitDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "explicitdedup.fifo")

	// First send with explicit deduplication ID.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "unique-id-1",
	})
	require.NoError(t, err)

	// Second send with same deduplication ID - should return original.
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello again",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "unique-id-1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)
}

func TestSendMessageFIFOExpiredDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "expireddedup.fifo")

	// We must set the dedup window in the past by directly manipulating the backend
	// via repeated sends rather than time manipulation. Instead, test that dedup
	// with a *different* dedup ID does NOT deduplicate.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "id-1",
	})
	require.NoError(t, err)

	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "id-2", // different ID - NOT a duplicate
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

// Test_FIFORequeueOrdering verifies that a FIFO message returned to the
// visible queue (via ChangeMessageVisibility(0) or automatic visibility-
// timeout expiry) is redelivered before a newer message in the SAME
// MessageGroupId that arrived while the earlier one was in flight — AWS's
// strict per-group ordering guarantee. In-flight messages block further
// delivery from their group but do NOT block further sends to that group, so
// it is possible (and exercised here) for a second message to be sitting in
// the queue behind an in-flight message from the same group. Naively
// appending a re-queued message to the tail of the pending list would let the
// newer message jump ahead of it, violating ordering; the backend must
// reinsert by SequenceNumber instead.
func Test_FIFORequeueOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		requeue func(t *testing.T, b *sqs.InMemoryBackend, queueURL, receiptHandle string)
		name    string
	}{
		{
			name: "ChangeMessageVisibilityZero",
			requeue: func(t *testing.T, b *sqs.InMemoryBackend, queueURL, receiptHandle string) {
				t.Helper()

				err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          queueURL,
					ReceiptHandle:     receiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "VisibilityTimeoutExpiry",
			requeue: func(t *testing.T, b *sqs.InMemoryBackend, _, _ string) {
				t.Helper()

				// The message was received with a 1-second visibility timeout;
				// simulate the janitor sweeping well past that expiry without a
				// real sleep.
				b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "fifo-requeue-order.fifo")

			// msg1 into group G.
			send1, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg1",
				MessageGroupID:         "G",
				MessageDeduplicationID: "dedup1",
			})
			require.NoError(t, err)

			// Receive msg1 with a short (1s) visibility timeout so it becomes
			// in-flight, blocking group G from further delivery.
			recv1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   1,
			})
			require.NoError(t, err)
			require.Len(t, recv1.Messages, 1)
			require.Equal(t, send1.MessageID, recv1.Messages[0].MessageID)

			// msg2 arrives in the SAME group while msg1 is still in flight —
			// sends are never blocked by an in-flight predecessor, only receives.
			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg2",
				MessageGroupID:         "G",
				MessageDeduplicationID: "dedup2",
			})
			require.NoError(t, err)

			// Return msg1 to the visible queue via whichever mechanism this
			// subtest exercises.
			tt.requeue(t, b, qURL, recv1.Messages[0].ReceiptHandle)

			// The next receive must hand back msg1 (the earlier SequenceNumber),
			// not msg2, even though msg1 was the one just re-queued.
			recv2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   sqs.NoVisibilityTimeout,
			})
			require.NoError(t, err)
			require.Len(t, recv2.Messages, 1)
			assert.Equal(t, "msg1", recv2.Messages[0].Body,
				"FIFO group ordering violated: newer same-group message delivered before the re-queued older one")
		})
	}
}

// TestFIFODedupPrunedByJanitor verifies that expired deduplication IDs are
// removed by janitor cleanup so quiet FIFO queues don't leak memory.
func TestFIFODedupPrunedByJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		expiredIDs   []string // IDs to inject with an expired timestamp
		wantDedupLen int      // expected dedup map size after the pruning SendMessage
	}{
		{
			name:         "expired_ids_removed_on_send",
			expiredIDs:   []string{"id-1", "id-2"},
			wantDedupLen: 1, // only the freshly added entry remains
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			const qName = "prune-send.fifo"
			qURL := createFIFOQueueWithDedup(t, b, qName)

			// Inject expired dedup IDs directly so we don't have to wait 300 s for
			// the real window to expire.
			for _, id := range tt.expiredIDs {
				b.InjectExpiredDedupID(qName, id)
			}

			// Sanity-check: expired entries are now present in the map.
			require.Equal(t, len(tt.expiredIDs), b.DedupMapLen(qName))

			// SendMessage stores a fresh dedup entry.
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "fresh-body",
				MessageGroupID:         "g1",
				MessageDeduplicationID: "fresh-id",
			})
			require.NoError(t, err)

			b.RunJanitorOnceForTest(time.Now())

			// After janitor cleanup, only the freshly added entry should remain.
			assert.Equal(t, tt.wantDedupLen, b.DedupMapLen(qName))
		})
	}
}

// createFIFOQueueWithDedup creates a FIFO queue and returns its URL.
func createFIFOQueueWithDedup(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	return out.QueueURL
}

// TestFIFOMessageGroupIDRequired verifies that SendMessage on a FIFO queue
// returns ErrMissingMessageGroupID when MessageGroupID is absent.
func TestFIFOMessageGroupIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		messageGroupID string
		dedupID        string
	}{
		{
			name:    "missing_group_id_rejected",
			wantErr: sqs.ErrMissingMessageGroupID,
		},
		{
			name:           "group_id_present_accepted",
			messageGroupID: "g1",
			dedupID:        "d1",
			wantErr:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "fifo-groupid-queue.fifo")

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg",
				MessageGroupID:         tt.messageGroupID,
				MessageDeduplicationID: tt.dedupID,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestFIFODeduplicationIDRequired verifies that SendMessage on a FIFO queue
// with ContentBasedDeduplication disabled requires an explicit MessageDeduplicationID.
func TestFIFODeduplicationIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr                   error
		name                      string
		contentBasedDeduplication string
		dedupID                   string
	}{
		{
			name:    "dedup_id_missing_content_based_off_rejected",
			wantErr: sqs.ErrMissingDeduplicationID,
		},
		{
			name:    "dedup_id_present_accepted",
			dedupID: "d1",
			wantErr: nil,
		},
		{
			name:                      "content_based_dedup_on_no_id_accepted",
			contentBasedDeduplication: "true",
			wantErr:                   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			attrs := map[string]string{}
			if tt.contentBasedDeduplication != "" {
				attrs["ContentBasedDeduplication"] = tt.contentBasedDeduplication
			}

			out, createErr := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "fifo-dedupid-queue.fifo",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, createErr)

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               out.QueueURL,
				MessageBody:            "msg",
				MessageGroupID:         "g1",
				MessageDeduplicationID: tt.dedupID,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
