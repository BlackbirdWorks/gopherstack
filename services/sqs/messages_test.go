package sqs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func newHandlerWithBackend(t *testing.T) (*sqs.Handler, *sqs.InMemoryBackend) {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)
	h := sqs.NewHandler(b)

	return h, b
}

// createQueueForTest creates a queue and returns its URL.
func createQueueForTest(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	return out.QueueURL
}

func TestReceiveRequestAttemptIDReturnsSameMessages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "attempt.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)
	qURL := out.QueueURL

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "body1",
		MessageGroupID: "g1",
	})
	require.NoError(t, err)

	// First receive with attemptId.
	first, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:                qURL,
		MaxNumberOfMessages:     1,
		ReceiveRequestAttemptID: "attempt-abc",
	})
	require.NoError(t, err)
	require.Len(t, first.Messages, 1)

	// Repeat with same attemptId → same messages.
	second, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:                qURL,
		MaxNumberOfMessages:     1,
		ReceiveRequestAttemptID: "attempt-abc",
	})
	require.NoError(t, err)
	require.Len(t, second.Messages, 1)
	assert.Equal(t, first.Messages[0].MessageID, second.Messages[0].MessageID)
	assert.Equal(t, first.Messages[0].ReceiptHandle, second.Messages[0].ReceiptHandle)
}

func TestReceiveRequestAttemptIDDifferentIds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "attempt2.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)
	qURL := out.QueueURL

	for i := range 3 {
		_, err = b.SendMessage(&sqs.SendMessageInput{
			QueueURL:               qURL,
			MessageBody:            fmt.Sprintf("body%d", i),
			MessageGroupID:         fmt.Sprintf("g%d", i),
			MessageDeduplicationID: fmt.Sprintf("dedup%d", i),
		})
		require.NoError(t, err)
	}

	// Different attempt IDs are independent. VisibilityTimeout must be the
	// "unspecified" sentinel (not the Go int zero value) so the first receive
	// keeps its message in flight for the queue's default visibility window
	// instead of an explicit 0-second timeout that makes it immediately
	// redeliverable — see sqs.NoVisibilityTimeout's doc comment.
	r1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:                qURL,
		MaxNumberOfMessages:     1,
		VisibilityTimeout:       sqs.NoVisibilityTimeout,
		ReceiveRequestAttemptID: "attempt-1",
	})
	require.NoError(t, err)

	r2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:                qURL,
		MaxNumberOfMessages:     1,
		VisibilityTimeout:       sqs.NoVisibilityTimeout,
		ReceiveRequestAttemptID: "attempt-2",
	})
	require.NoError(t, err)

	// Different attempt IDs should return different messages.
	if len(r1.Messages) > 0 && len(r2.Messages) > 0 {
		assert.NotEqual(t, r1.Messages[0].MessageID, r2.Messages[0].MessageID)
	}
}

func TestReceiptHandleContainsMessageID(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "gen-handle")

	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "test",
	})
	require.NoError(t, err)

	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)

	// Receipt handle format: <msgID>:<gen>:<uuid>
	handle := recv.Messages[0].ReceiptHandle
	parts := strings.SplitN(handle, ":", 3)
	require.Len(t, parts, 3, "receipt handle must be <msgID>:<gen>:<uuid>")
	assert.Equal(t, out.MessageID, parts[0])
}

func TestFullSendReceiveDeleteCycle(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "full-cycle")

	// Send with valid custom DataType.
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello world",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Greet":   {DataType: "String", StringValue: "Hi"},
			"Count":   {DataType: "Number", StringValue: "7"},
			"Payload": {DataType: "Binary", BinaryValue: []byte{0x01, 0x02}},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.MessageID)

	// Receive.
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:              qURL,
		MaxNumberOfMessages:   1,
		MessageAttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)

	msg := recv.Messages[0]
	assert.Equal(t, "hello world", msg.Body)
	assert.NotEmpty(t, msg.ReceiptHandle)

	// Delete.
	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	require.NoError(t, err)
}

func b3newBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return b
}

func b3createQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: name, Endpoint: "localhost"})
	require.NoError(t, err)

	return out.QueueURL
}

func b3createFIFOQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	return out.QueueURL
}

func b3send(t *testing.T, b *sqs.InMemoryBackend, qURL, body string) {
	t.Helper()
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: body})
	require.NoError(t, err)
}

func b3recv(t *testing.T, b *sqs.InMemoryBackend, qURL string, maxItems int) []*sqs.Message {
	t.Helper()
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: maxItems,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)

	return out.Messages
}

func b2newBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return b
}

func b2createQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: name, Endpoint: "localhost"})
	require.NoError(t, err)

	return out.QueueURL
}

func b2createFIFOQueue(t *testing.T, b *sqs.InMemoryBackend, name string, extraAttrs map[string]string) string {
	t.Helper()
	if !strings.HasSuffix(name, ".fifo") {
		name += ".fifo"
	}
	attrs := map[string]string{}
	maps.Copy(attrs, extraAttrs)
	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  name,
		Endpoint:   "localhost",
		Attributes: attrs,
	})
	require.NoError(t, err)

	return out.QueueURL
}

func b2send(t *testing.T, b *sqs.InMemoryBackend, qURL, body string) *sqs.SendMessageOutput {
	t.Helper()
	out, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: body})
	require.NoError(t, err)

	return out
}

func b2sendFIFO(t *testing.T, b *sqs.InMemoryBackend, qURL, body, groupID, dedupID string) *sqs.SendMessageOutput {
	t.Helper()
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            body,
		MessageGroupID:         groupID,
		MessageDeduplicationID: dedupID,
	})
	require.NoError(t, err)

	return out
}

// b2receive receives up to max messages using the queue's default visibility timeout.
// Passing VisibilityTimeout=-1 tells the backend to use the queue's configured value.
func b2receive(t *testing.T, b *sqs.InMemoryBackend, qURL string, maxMsgs int) []*sqs.Message {
	t.Helper()
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: maxMsgs,
		VisibilityTimeout:   -1,
		AttributeNames:      []string{"All"},
	})
	require.NoError(t, err)

	return out.Messages
}

func b2delete(t *testing.T, b *sqs.InMemoryBackend, qURL, receipt string) {
	t.Helper()
	require.NoError(t, b.DeleteMessage(&sqs.DeleteMessageInput{QueueURL: qURL, ReceiptHandle: receipt}))
}

func b2getAttrs(t *testing.T, b *sqs.InMemoryBackend, qURL string, names ...string) map[string]string {
	t.Helper()
	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: names,
	})
	require.NoError(t, err)

	return out.Attributes
}

func sha256hex(s string) string {
	h := sha256.Sum256([]byte(s))

	return hex.EncodeToString(h[:])
}

func TestSendReceiveDelete_RoundTrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "rtrip")
	sendOut := b2send(t, b, qURL, "hello world")
	assert.NotEmpty(t, sendOut.MessageID)
	assert.NotEmpty(t, sendOut.MD5OfBody)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "hello world", msgs[0].Body)
	assert.Equal(t, sendOut.MessageID, msgs[0].MessageID)
	assert.NotEmpty(t, msgs[0].ReceiptHandle)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	// After delete, queue is empty
	msgs2 := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs2)
}

func TestReceiveMessage_PopulatesSystemAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sysattrs")
	b2send(t, b, qURL, "test body")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	msg := msgs[0]

	attrs := msg.Attributes
	assert.NotEmpty(t, attrs["ApproximateReceiveCount"])
	assert.NotEmpty(t, attrs["SentTimestamp"])
	assert.NotEmpty(t, attrs["ApproximateFirstReceiveTimestamp"])
	assert.Equal(t, "1", attrs["ApproximateReceiveCount"])
}

func TestReceiveMessage_IncrementsReceiveCount(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "recv-count",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "0"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg")

	for i := 1; i <= 3; i++ {
		msgs, err2 := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL.QueueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   0,
			AttributeNames:      []string{"All"},
		})
		require.NoError(t, err2)
		require.Len(t, msgs.Messages, 1)
		assert.Equal(t, strconv.Itoa(i), msgs.Messages[0].Attributes["ApproximateReceiveCount"])
	}
}

func TestReceiveMessage_MaxNumberOfMessages_Max10(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "max10")
	for i := range 12 {
		b2send(t, b, qURL, fmt.Sprintf("msg-%d", i))
	}

	// First receive: capped at 10 even when 12 exist
	msgs := b2receive(t, b, qURL, 10)
	assert.Len(t, msgs, 10)

	// Remaining 2 messages (10 are in-flight with VT=30s): second receive gets ≤2
	msgs2 := b2receive(t, b, qURL, 2)
	assert.LessOrEqual(t, len(msgs2), 2)

	// MaxNumberOfMessages > 10 is rejected
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 11,
		VisibilityTimeout:   -1,
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMaxMessages)
}

func TestReceiveMessage_AttributesAllReturnsAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "filter-attrs")
	b2send(t, b, qURL, "body")

	// Backend always populates all system attributes; handler filters by AttributeNames.
	// When calling backend directly, verify all expected system attributes are present.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   -1,
		AttributeNames:      []string{"All"},
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	attrs := out.Messages[0].Attributes
	assert.Contains(t, attrs, "ApproximateReceiveCount")
	assert.Contains(t, attrs, "SentTimestamp")
	assert.Contains(t, attrs, "ApproximateFirstReceiveTimestamp")
	assert.Equal(t, "1", attrs["ApproximateReceiveCount"])
}

func TestEmptyQueue_ReceiveReturnsEmpty(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "empty-recv")
	msgs := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs)
}

func TestDeleteMessage_InvalidReceiptHandle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "del-invalid")
	b2send(t, b, qURL, "msg")

	err := b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: "totally-invalid-receipt-handle",
	})
	require.Error(t, err)
}

func TestSendMessage_EmptyBody_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "empty-body")
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: ""})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageBody)
}

func TestSendMessage_ExceedsMaxSize_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "small-size",
		Endpoint:   "localhost",
		Attributes: map[string]string{"MaximumMessageSize": "1024"},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL.QueueURL,
		MessageBody: strings.Repeat("x", 1025),
	})
	require.ErrorIs(t, err, sqs.ErrMessageTooLarge)
}

func tagsFromMap(m map[string]string) *tags.Tags {
	return tags.FromMap("test", m)
}

func newQueryVals(action string, params map[string]string) url.Values {
	v := url.Values{"Action": {action}}
	for k, val := range params {
		v.Set(k, val)
	}

	return v
}

type queueURLResult struct {
	XMLName  xml.Name `xml:"CreateQueueResponse"`
	QueueURL string   `xml:"CreateQueueResult>QueueUrl"`
}

type receiveMessageResult struct {
	XMLName  xml.Name `xml:"ReceiveMessageResponse"`
	Messages []struct {
		ReceiptHandle string `xml:"ReceiptHandle"`
	} `xml:"ReceiveMessageResult>Message"`
}

func extractQueueURLFromXML(t *testing.T, body string) string {
	t.Helper()
	var r queueURLResult
	if err := xml.Unmarshal([]byte(body), &r); err == nil && r.QueueURL != "" {
		return r.QueueURL
	}
	// Fallback: look for URL in body
	for line := range strings.SplitSeq(body, "\n") {
		if strings.Contains(line, "localhost") && strings.Contains(line, "/000000000000/") {
			start := strings.Index(line, "http://")
			if start >= 0 {
				end := strings.IndexAny(line[start:], "<\" \t\r\n")
				if end > 0 {
					return line[start : start+end]
				}

				return line[start:]
			}
		}
	}
	t.Fatal("could not extract queue URL from XML: " + body)

	return ""
}

func extractReceiptHandleFromXML(t *testing.T, body string) string {
	t.Helper()
	var r receiveMessageResult
	require.NoError(t, xml.Unmarshal([]byte(body), &r), "parse ReceiveMessage XML")
	require.NotEmpty(t, r.Messages, "expected at least one message in ReceiveMessage response")

	return r.Messages[0].ReceiptHandle
}

// TestMsgAttrs_SystemAttributes verifies that system attributes
// (ApproximateReceiveCount, SentTimestamp, etc.) are returned in the XML response.
func TestMsgAttrs_SystemAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		attrNames  []string
		wantKeys   []string
		wantAbsent []string
	}{
		{
			name:      "All returns all system attrs",
			attrNames: []string{"All"},
			wantKeys:  []string{"ApproximateReceiveCount", "SentTimestamp"},
		},
		{
			name:       "exact name filter returns only that attr",
			attrNames:  []string{"ApproximateReceiveCount"},
			wantKeys:   []string{"ApproximateReceiveCount"},
			wantAbsent: []string{"SentTimestamp"},
		},
		{
			// When no AttributeName.N params are specified, the backend defaults to returning
			// all system attributes (permissive default — distinct from AWS strict behavior).
			name:      "empty filter returns all system attrs (default-all)",
			attrNames: []string{},
			wantKeys:  []string{"ApproximateReceiveCount", "SentTimestamp"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := createQueryQueue(t, h, "sysattr-q")

			sendVals := newQueryVals("SendMessage", map[string]string{
				"QueueUrl":    qURL,
				"MessageBody": "sysattr-body",
			})
			rec := doQueryRequest(t, h, sendVals)
			require.Equal(t, http.StatusOK, rec.Code)

			recvVals := newQueryVals("ReceiveMessage", map[string]string{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": "1",
			})
			for i, name := range tc.attrNames {
				recvVals.Set(fmt.Sprintf("AttributeName.%d", i+1), name)
			}

			rec = doQueryRequest(t, h, recvVals)
			require.Equal(t, http.StatusOK, rec.Code)

			result := parseXMLReceive(t, rec.Body.String())
			require.Len(t, result.Messages, 1)
			msg := result.Messages[0]

			attrMap := make(map[string]string, len(msg.Attributes))
			for _, a := range msg.Attributes {
				attrMap[a.Name] = a.Value
			}

			for _, key := range tc.wantKeys {
				assert.Contains(t, attrMap, key, "system attr %q must be present", key)
			}

			for _, key := range tc.wantAbsent {
				assert.NotContains(t, attrMap, key, "system attr %q must be absent", key)
			}
		})
	}
}

func TestSendAndReceiveMessage(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	sendOut, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello world",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, sendOut.MessageID)
	assert.NotEmpty(t, sendOut.MD5OfBody)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "hello world", recvOut.Messages[0].Body)
	assert.Equal(t, sendOut.MessageID, recvOut.Messages[0].MessageID)
	assert.NotEmpty(t, recvOut.Messages[0].ReceiptHandle)
}

func TestDeleteMessage(t *testing.T) {
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

	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)
}

func TestDeleteMessageInvalidHandle(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: "invalid-handle",
	})
	require.ErrorIs(t, err, sqs.ErrReceiptHandleInvalid)
}

func TestReceiveMessageQueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:        queueURL("nonexistent"),
		WaitTimeSeconds: 0,
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestLongPolling(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	// Send message before calling receive — should return quickly.
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "fast"})
	require.NoError(t, err)

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestLongPollingWakesOnMessageArrival(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newBackend(t)
		qURL := createTestQueue(t, b, "wake-queue")

		// Send a message after a short delay while ReceiveMessage is blocking.
		sendErr := make(chan error, 1)
		go func() {
			time.Sleep(150 * time.Millisecond)
			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "wake"})
			sendErr <- err
		}()

		start := time.Now()

		out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   30,
			WaitTimeSeconds:     5,
		})

		elapsed := time.Since(start)

		require.NoError(t, <-sendErr)
		require.NoError(t, err)
		require.Len(t, out.Messages, 1)
		// Should wake well before the 5-second deadline.
		assert.Less(t, elapsed, 2*time.Second)
	})
}

func TestLongPollingTimesOutWithNoMessages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "timeout-queue")

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     1,
	})

	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, out.Messages)
	// Should have waited approximately WaitTimeSeconds.
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

// TestLongPollingConcurrentReceivers verifies that concurrent long-poll receivers
// do not race or panic due to the old close/recreate channel pattern.
// With the fixed buffered(1) notify channel, receivers are woken without closing
// the channel, so no stale reference can cause a tight loop or panic.
func TestLongPollingConcurrentReceivers(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newBackend(t)
		qURL := createTestQueue(t, b, "concurrent-recv-queue")

		const numReceivers = 3
		const numMessages = 3

		results := make(chan *sqs.ReceiveMessageOutput, numReceivers)
		errs := make(chan error, numReceivers)

		// ready is closed once all receiver goroutines have been launched;
		// each goroutine is guaranteed to start before the first send.
		ready := make(chan struct{})

		var wg sync.WaitGroup

		wg.Add(numReceivers)

		for range numReceivers {
			go func() {
				wg.Done()
				<-ready
				out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: 1,
					VisibilityTimeout:   30,
					WaitTimeSeconds:     5,
				})
				errs <- err
				results <- out
			}()
		}

		// Block until all receiver goroutines have started, signal them to enter
		// ReceiveMessage, then wait until they are durably blocked in the
		// long-poll select before the first message is sent. This matches the
		// approach used in TestLongPollingWakesOnMessageArrival and ensures the
		// test exercises the notify wake-up path rather than the initial
		// receiveOnce fast-path.
		wg.Wait()
		close(ready)
		synctest.Wait()

		for i := range numMessages {
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: fmt.Sprintf("msg-%d", i),
			})
			require.NoError(t, err)
		}

		for range numReceivers {
			require.NoError(t, <-errs)
			out := <-results
			require.Len(t, out.Messages, 1)
		}
	})
}

func TestApproximateFirstReceiveTimestamp_SetOnFirstReceive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "first-receive-ts-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// First receive: ApproximateFirstReceiveTimestamp should be set.
	out1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out1.Messages, 1)

	msg := out1.Messages[0]
	firstTS := msg.Attributes["ApproximateFirstReceiveTimestamp"]
	assert.NotEmpty(t, firstTS, "ApproximateFirstReceiveTimestamp should be set on first receive")

	// The value should be a Unix millisecond timestamp (13-digit integer).
	assert.Len(t, firstTS, 13)

	// Return the message to queue (simulate visibility timeout = 0).
	_ = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: 0,
	})

	// Second receive: ApproximateFirstReceiveTimestamp should NOT change.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out2.Messages, 1)

	secondTS := out2.Messages[0].Attributes["ApproximateFirstReceiveTimestamp"]
	assert.Equal(t, firstTS, secondTS, "ApproximateFirstReceiveTimestamp must not change on subsequent receives")
}

func TestMaxNumberOfMessages_ClampsAt10(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "clamp-queue")

	// Send 15 messages.
	for i := range 15 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: strconv.Itoa(i),
		})
		require.NoError(t, err)
	}

	// MaxNumberOfMessages > 10 should now return an error (matches AWS validation).
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 20})
	require.ErrorIs(t, err, sqs.ErrInvalidMaxMessages)
}

func TestMaxNumberOfMessages_AtBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "boundary-queue")

	for i := range 15 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: strconv.Itoa(i),
		})
		require.NoError(t, err)
	}

	// MaxNumberOfMessages=10 (boundary) should succeed and return 10 messages.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 10})
	require.NoError(t, err)
	assert.Len(t, out.Messages, 10)
}

func TestSentTimestamp_PresentInAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "sent-ts-queue")

	beforeSend := time.Now().UnixMilli()

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "check"})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	sentTSStr := out.Messages[0].Attributes["SentTimestamp"]
	require.NotEmpty(t, sentTSStr)

	sentTS, err := strconv.ParseInt(sentTSStr, 10, 64)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, sentTS, beforeSend)
}

func TestReceiveAndDeleteMessagesLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "local_receive_then_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "local-ops-queue")

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, err)

			msgs, err := b.ReceiveMessagesLocal(qURL, 1)
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			handles := make([]string, 0, len(msgs))
			for _, m := range msgs {
				handles = append(handles, m.ReceiptHandle)
			}

			require.NoError(t, b.DeleteMessagesLocal(qURL, handles), tt.name)
		})
	}
}

// TestReceiveMessageWaitTimeSecondsValidation verifies that WaitTimeSeconds
// outside the valid AWS range (0–20) is rejected.
func TestReceiveMessageWaitTimeSecondsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		waitTimeSeconds int
	}{
		{
			name:            "negative_wait_time",
			waitTimeSeconds: -1,
			wantErr:         sqs.ErrInvalidWaitTime,
		},
		{
			name:            "over_max_wait_time",
			waitTimeSeconds: 21,
			wantErr:         sqs.ErrInvalidWaitTime,
		},
		{
			name:            "max_valid_wait_time",
			waitTimeSeconds: 20,
			wantErr:         nil,
		},
		{
			name:            "zero_wait_time",
			waitTimeSeconds: 0,
			wantErr:         nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "wait-validation-queue")

			// Send a message so a non-zero wait doesn't block forever.
			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, err)

			_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     tt.waitTimeSeconds,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSendMessageSizeValidation verifies that messages exceeding the queue's
// MaximumMessageSize attribute are rejected.
func TestSendMessageSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		maxMsgSize string
		bodySize   int
	}{
		{
			name:     "body_at_default_limit_is_accepted",
			bodySize: 262144,
			wantErr:  nil,
		},
		{
			name:     "body_over_default_limit_rejected",
			bodySize: 262145,
			wantErr:  sqs.ErrMessageTooLarge,
		},
		{
			name:       "body_over_custom_limit_rejected",
			bodySize:   1025,
			maxMsgSize: "1024",
			wantErr:    sqs.ErrMessageTooLarge,
		},
		{
			name:       "body_at_custom_limit_accepted",
			bodySize:   1024,
			maxMsgSize: "1024",
			wantErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			attrs := map[string]string{}
			if tt.maxMsgSize != "" {
				attrs["MaximumMessageSize"] = tt.maxMsgSize
			}

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "size-validation-queue",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    out.QueueURL,
				MessageBody: strings.Repeat("x", tt.bodySize),
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSendMessageSizeIncludesAttributes verifies that SQS counts message
// attribute bytes (name + type + value) toward the queue's MaximumMessageSize,
// matching AWS behaviour.
func TestSendMessageSizeIncludesAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "attr-size-queue",
		Endpoint:   testEndpoint,
		Attributes: map[string]string{"MaximumMessageSize": "1024"},
	})
	require.NoError(t, err)

	// Body (1000) + attribute name (50) + type ("String" = 6) + value (50) = 1106 bytes, over the 1024 limit.
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    out.QueueURL,
		MessageBody: strings.Repeat("x", 1000),
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			strings.Repeat("k", 50): {
				DataType:    "String",
				StringValue: strings.Repeat("v", 50),
			},
		},
	})
	require.ErrorIs(t, err, sqs.ErrMessageTooLarge)

	// Body alone is well below the limit; without attributes the send succeeds.
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    out.QueueURL,
		MessageBody: strings.Repeat("x", 1000),
	})
	require.NoError(t, err)
}

// TestLongPollBroadcastWakeup verifies that all concurrent long-poll receivers
// are woken by a single SendMessage, not just one of them.
func TestLongPollBroadcastWakeup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numReceivers int
		numMessages  int
	}{
		{
			name:         "two_receivers_both_woken_by_messages",
			numReceivers: 2,
			numMessages:  2,
		},
		{
			name:         "three_receivers_all_woken_by_messages",
			numReceivers: 3,
			numMessages:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newBackend(t)
				qURL := createTestQueue(t, b, "broadcast-wake-queue")

				ready := make(chan struct{})
				results := make(chan int, tt.numReceivers)

				var wg sync.WaitGroup

				wg.Add(tt.numReceivers)

				for range tt.numReceivers {
					go func() {
						wg.Done() // signal that this goroutine has started
						<-ready   // wait until released by the test
						out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
							QueueURL:            qURL,
							MaxNumberOfMessages: 1,
							WaitTimeSeconds:     5,
						})
						if err == nil {
							results <- len(out.Messages)
						} else {
							results <- -1
						}
					}()
				}

				// Ensure all goroutines have been scheduled before releasing them.
				wg.Wait()
				close(ready)

				// Wait until all goroutines are durably blocked in the long-poll
				// select before any message is sent.
				synctest.Wait()

				// Send one message per receiver so each one should wake and return a msg.
				for i := range tt.numMessages {
					_, err := b.SendMessage(&sqs.SendMessageInput{
						QueueURL:    qURL,
						MessageBody: strings.Repeat("m", i+1),
					})
					require.NoError(t, err)
				}

				deadline := time.After(3 * time.Second)
				for range tt.numReceivers {
					select {
					case n := <-results:
						assert.Equal(t, 1, n)
					case <-deadline:
						require.FailNow(t, "at least one long-poll receiver did not wake in time")
					}
				}
			})
		})
	}
}

// TestMessageRetentionPeriodExpiry verifies that messages older than the queue's
// MessageRetentionPeriod are silently discarded and never delivered to consumers.
func TestMessageRetentionPeriodExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		retentionSecs int
		wantMsgCount  int
	}{
		{
			name:          "expired_message_not_delivered",
			retentionSecs: 1, // 1 second retention
			wantMsgCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newBackend(t)
				// Create with a valid retention period, then immediately lower it via
				// SetQueueAttributes so the test can use a sub-60-second window without
				// triggering the CreateQueue attribute-range validation.
				out, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "retention-queue",
					Endpoint:  testEndpoint,
				})
				require.NoError(t, err)

				// Bypass the 60-second minimum by injecting via SetQueueAttributes directly.
				// SetQueueAttributes also validates ranges, so force via a direct attribute update here.
				b.SetRetentionForTest(out.QueueURL, tt.retentionSecs)

				_, err = b.SendMessage(&sqs.SendMessageInput{
					QueueURL:    out.QueueURL,
					MessageBody: "old-msg",
				})
				require.NoError(t, err)

				// Wait for the retention period to pass.
				time.Sleep(time.Duration(tt.retentionSecs+1) * time.Second)

				recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            out.QueueURL,
					MaxNumberOfMessages: 10,
				})
				require.NoError(t, err)
				assert.Len(t, recv.Messages, tt.wantMsgCount)
			})
		})
	}
}

// TestReceiveMessage_MaxNumberOfMessages_Validation asserts InvalidParameterValue for >10.
func TestReceiveMessage_MaxNumberOfMessages_Validation(t *testing.T) {
	t.Parallel()

	const wantType = "com.amazonaws.sqs#InvalidParameterValue"

	tests := []struct {
		name                string
		wantType            string
		maxNumberOfMessages int
		wantCode            int
	}{
		{name: "valid_max_10", maxNumberOfMessages: 10, wantCode: http.StatusOK},
		{name: "valid_max_1", maxNumberOfMessages: 1, wantCode: http.StatusOK},
		{name: "too_many_11", maxNumberOfMessages: 11, wantCode: http.StatusBadRequest, wantType: wantType},
		{name: "zero_uses_default", maxNumberOfMessages: 0, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "recv-val-queue")

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":            queueURL,
				"MaxNumberOfMessages": tt.maxNumberOfMessages,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp jsonErr
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}
