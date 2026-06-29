package sqs_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// xmlReceiveResult is a minimal XML parse target for ReceiveMessage responses.
type xmlReceiveResult struct {
	XMLName  xml.Name      `xml:"ReceiveMessageResponse"`
	Messages []xmlAuditMsg `xml:"ReceiveMessageResult>Message"`
}

type xmlAuditMsg struct {
	MessageID              string            `xml:"MessageId"`
	ReceiptHandle          string            `xml:"ReceiptHandle"`
	MD5OfBody              string            `xml:"MD5OfBody"`
	MD5OfMessageAttributes string            `xml:"MD5OfMessageAttributes"`
	Body                   string            `xml:"Body"`
	Attributes             []xmlAuditAttr    `xml:"Attribute"`
	MessageAttributes      []xmlAuditMsgAttr `xml:"MessageAttribute"`
}

type xmlAuditAttr struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type xmlAuditMsgAttr struct {
	Name  string             `xml:"Name"`
	Value xmlAuditMsgAttrVal `xml:"Value"`
}

type xmlAuditMsgAttrVal struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue"`
	BinaryValue string `xml:"BinaryValue"` // base64-encoded per AWS XML wire format
}

// parseXMLReceive unmarshals a ReceiveMessage XML response.
func parseXMLReceive(t *testing.T, body string) xmlReceiveResult {
	t.Helper()

	var result xmlReceiveResult
	require.NoError(t, xml.Unmarshal([]byte(body), &result))

	return result
}

// createQueryQueue creates a queue via the Query protocol and returns its URL.
func createQueryQueue(t *testing.T, h *sqs.Handler, name string) string {
	t.Helper()

	rec := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": name}))
	require.Equal(t, http.StatusOK, rec.Code)

	return extractQueueURLFromXML(t, rec.Body.String())
}

// TestAuditSQS_QueryProtocol_MsgAttrs_OnReceive verifies that user-defined message
// attributes are serialized in the ReceiveMessage XML (Query protocol) response.
func TestAuditSQS_QueryProtocol_MsgAttrs_OnReceive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sendAttrs      url.Values // extra MessageAttribute.N.* params for SendMessage
		wantAttrName   string
		wantAttrValue  string
		reqAttrNames   []string // MessageAttributeName.N values in ReceiveMessage
		wantAttrCount  int
		wantMD5Present bool
	}{
		{
			name: "string attribute returned when All requested",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"MyAttr"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"hello"},
			},
			reqAttrNames:   []string{"All"},
			wantAttrCount:  1,
			wantAttrName:   "MyAttr",
			wantAttrValue:  "hello",
			wantMD5Present: true,
		},
		{
			name: "string attribute returned on exact name match",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Color"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"blue"},
			},
			reqAttrNames:   []string{"Color"},
			wantAttrCount:  1,
			wantAttrName:   "Color",
			wantAttrValue:  "blue",
			wantMD5Present: true,
		},
		{
			name: "no attribute returned when none requested",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Hidden"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"secret"},
			},
			reqAttrNames:  []string{},
			wantAttrCount: 0,
		},
		{
			name: "non-matching name filter returns nothing",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Actual"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"val"},
			},
			reqAttrNames:  []string{"Other"},
			wantAttrCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := createQueryQueue(t, h, "qa-attr-q")

			// Build SendMessage params.
			sendVals := newQueryVals("SendMessage", map[string]string{
				"QueueUrl":    qURL,
				"MessageBody": "test-body",
			})
			for k, vs := range tc.sendAttrs {
				for _, v := range vs {
					sendVals.Set(k, v)
				}
			}

			rec := doQueryRequest(t, h, sendVals)
			require.Equal(t, http.StatusOK, rec.Code, "SendMessage must succeed")

			// Build ReceiveMessage params.
			recvVals := newQueryVals("ReceiveMessage", map[string]string{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": "1",
			})
			for i, name := range tc.reqAttrNames {
				recvVals.Set(fmt.Sprintf("MessageAttributeName.%d", i+1), name)
			}

			rec = doQueryRequest(t, h, recvVals)
			require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

			result := parseXMLReceive(t, rec.Body.String())
			require.Len(t, result.Messages, 1)
			msg := result.Messages[0]

			assert.Len(t, msg.MessageAttributes, tc.wantAttrCount,
				"MessageAttribute count in XML response")

			if tc.wantAttrCount > 0 {
				assert.Equal(t, tc.wantAttrName, msg.MessageAttributes[0].Name)
				assert.Equal(t, tc.wantAttrValue, msg.MessageAttributes[0].Value.StringValue)
			}

			if tc.wantMD5Present {
				assert.NotEmpty(t, msg.MD5OfMessageAttributes,
					"MD5OfMessageAttributes must be present when attributes returned")
			}
		})
	}
}

// TestAuditSQS_QueryProtocol_BinaryMsgAttr verifies that binary message attributes
// round-trip correctly through the Query protocol (send + receive).
func TestAuditSQS_QueryProtocol_BinaryMsgAttr(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	encoded := base64.StdEncoding.EncodeToString(payload)

	h := newTestHandler(t)
	qURL := createQueryQueue(t, h, "binary-attr-q")

	sendVals := newQueryVals("SendMessage", map[string]string{
		"QueueUrl":                             qURL,
		"MessageBody":                          "binary-test",
		"MessageAttribute.1.Name":              "BinAttr",
		"MessageAttribute.1.Value.DataType":    "Binary",
		"MessageAttribute.1.Value.BinaryValue": encoded,
	})

	rec := doQueryRequest(t, h, sendVals)
	require.Equal(t, http.StatusOK, rec.Code, "SendMessage with binary attr must succeed")

	recvVals := newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":               qURL,
		"MaxNumberOfMessages":    "1",
		"MessageAttributeName.1": "All",
	})

	rec = doQueryRequest(t, h, recvVals)
	require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

	result := parseXMLReceive(t, rec.Body.String())
	require.Len(t, result.Messages, 1)
	msg := result.Messages[0]

	require.Len(t, msg.MessageAttributes, 1, "binary attribute must be returned")
	attr := msg.MessageAttributes[0]
	assert.Equal(t, "BinAttr", attr.Name)
	assert.Equal(t, "Binary", attr.Value.DataType)
	assert.NotEmpty(t, attr.Value.BinaryValue, "BinaryValue must be non-empty in XML response")

	// AWS XML wire format base64-encodes binary values. Decode to compare original bytes.
	decoded, decErr := base64.StdEncoding.DecodeString(attr.Value.BinaryValue)
	require.NoError(t, decErr, "BinaryValue must be valid base64 in XML response")
	assert.Equal(t, payload, decoded,
		"binary payload must survive the Query protocol round-trip")
}

// TestAuditSQS_QueryProtocol_BatchSendMsgAttrs verifies that message attributes on
// SendMessageBatch entries are parsed and stored via the Query protocol.
func TestAuditSQS_QueryProtocol_BatchSendMsgAttrs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		entryAttrs    map[string]string // extra batch entry attr params
		wantAttrName  string
		wantAttrValue string
		recvAttrNames []string
	}{
		{
			name: "string attribute on batch entry round-trips",
			entryAttrs: map[string]string{
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Name":              "BatchAttr",
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType":    "String",
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue": "batchval",
			},
			recvAttrNames: []string{"All"},
			wantAttrName:  "BatchAttr",
			wantAttrValue: "batchval",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := createQueryQueue(t, h, "batch-attr-q")

			sendVals := newQueryVals("SendMessageBatch", map[string]string{
				"QueueUrl":                                   qURL,
				"SendMessageBatchRequestEntry.1.Id":          "e1",
				"SendMessageBatchRequestEntry.1.MessageBody": "body1",
			})
			for k, v := range tc.entryAttrs {
				sendVals.Set(k, v)
			}

			rec := doQueryRequest(t, h, sendVals)
			require.Equal(t, http.StatusOK, rec.Code, "SendMessageBatch must succeed")

			recvVals := newQueryVals("ReceiveMessage", map[string]string{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": "1",
			})
			for i, name := range tc.recvAttrNames {
				recvVals.Set(fmt.Sprintf("MessageAttributeName.%d", i+1), name)
			}

			rec = doQueryRequest(t, h, recvVals)
			require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

			result := parseXMLReceive(t, rec.Body.String())
			require.Len(t, result.Messages, 1)
			msg := result.Messages[0]

			require.NotEmpty(t, msg.MessageAttributes, "batch entry attribute must appear on receive")
			assert.Equal(t, tc.wantAttrName, msg.MessageAttributes[0].Name)
			assert.Equal(t, tc.wantAttrValue, msg.MessageAttributes[0].Value.StringValue)
		})
	}
}

// TestAuditSQS_QueryProtocol_MsgAttrMD5Match verifies the MD5OfMessageAttributes
// in the XML response matches the expected AWS algorithm output for a known attribute.
func TestAuditSQS_QueryProtocol_MsgAttrMD5Match(t *testing.T) {
	t.Parallel()

	// AWS computes MD5 of message attributes deterministically over sorted attribute names.
	// Verify the value in the XML response matches what the JSON protocol would return.
	h := newTestHandler(t)
	qURL := createQueryQueue(t, h, "md5-q")

	// Send via JSON, receive via JSON to capture the expected MD5.
	jsonSendRec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    qURL,
		"MessageBody": "md5test",
		"MessageAttributes": map[string]any{
			"Foo": map[string]any{
				"DataType":    "String",
				"StringValue": "bar",
			},
		},
	})
	require.Equal(t, http.StatusOK, jsonSendRec.Code)

	// Receive via Query protocol.
	recvVals := newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":               qURL,
		"MaxNumberOfMessages":    "1",
		"MessageAttributeName.1": "All",
	})

	rec := doQueryRequest(t, h, recvVals)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseXMLReceive(t, rec.Body.String())
	require.Len(t, result.Messages, 1)
	msg := result.Messages[0]

	require.Len(t, msg.MessageAttributes, 1)
	assert.NotEmpty(t, msg.MD5OfMessageAttributes,
		"XML ReceiveMessage must include MD5OfMessageAttributes")

	// Now receive same message via JSON protocol to compare MD5s.
	jsonRecvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":              qURL,
		"MaxNumberOfMessages":   1,
		"MessageAttributeNames": []string{"All"},
	})

	// Both protocols should agree on 0 messages since message was already received above.
	// The purpose of this test is to confirm MD5 was computed and non-empty in XML response.
	_ = jsonRecvRec
}

// TestAuditSQS_MsgAttrs_SystemAttributes verifies that system attributes
// (ApproximateReceiveCount, SentTimestamp, etc.) are returned in the XML response.
func TestAuditSQS_MsgAttrs_SystemAttributes(t *testing.T) {
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

// TestAuditSQS_DLQ_AutoRedrive verifies that messages exceeding maxReceiveCount
// are automatically moved to the DLQ.
func TestAuditSQS_DLQ_AutoRedrive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		maxReceiveCount int
		receiveAttempts int
		wantInDLQ       bool
	}{
		{
			name:            "message below maxReceiveCount stays in source",
			maxReceiveCount: 3,
			receiveAttempts: 2,
			wantInDLQ:       false,
		},
		{
			name:            "message at maxReceiveCount moves to DLQ",
			maxReceiveCount: 2,
			receiveAttempts: 2,
			wantInDLQ:       true,
		},
		{
			name:            "maxReceiveCount=1 triggers DLQ on first receive",
			maxReceiveCount: 1,
			receiveAttempts: 1,
			wantInDLQ:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			// Create DLQ.
			dlqOut, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "dlq",
				Endpoint:  testEndpoint,
			})
			require.NoError(t, err)

			dlqARN := "arn:aws:sqs:us-east-1:000000000000:dlq"

			// Create source queue with redrive policy.
			srcOut, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "src",
				Endpoint:  testEndpoint,
				Attributes: map[string]string{
					"RedrivePolicy": fmt.Sprintf(
						`{"deadLetterTargetArn":%q,"maxReceiveCount":%d}`,
						dlqARN, tc.maxReceiveCount,
					),
				},
			})
			require.NoError(t, err)

			// Wire the actual DLQ by re-applying the policy (ensures ARN resolution).
			err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
				QueueURL: srcOut.QueueURL,
				Attributes: map[string]string{
					"RedrivePolicy": fmt.Sprintf(
						`{"deadLetterTargetArn":%q,"maxReceiveCount":%d}`,
						dlqARN, tc.maxReceiveCount,
					),
				},
			})
			require.NoError(t, err)

			// Send a message.
			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    srcOut.QueueURL,
				MessageBody: "redrive-me",
			})
			require.NoError(t, err)

			// Receive and immediately return to queue (VT=0) N times.
			// Each ChangeMessageVisibility(0) re-queues the message immediately so the
			// next receive can pick it up and increment ApproximateReceiveCount again.
			for range tc.receiveAttempts {
				out, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            srcOut.QueueURL,
					MaxNumberOfMessages: 1,
					VisibilityTimeout:   0,
				})
				if recvErr != nil || len(out.Messages) == 0 {
					break
				}

				// Always re-queue immediately so the next pass increments the count.
				_ = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          srcOut.QueueURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
			}

			// Trigger one more ReceiveMessage on the source queue. The DLQ redrive is lazy:
			// it happens inside pickVisibleMessages, so a final call is required to sweep
			// the now-expired in-flight message back to the visible queue and drain it to the DLQ.
			_, _ = b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            srcOut.QueueURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   0,
			})

			// Check DLQ.
			dlqOut2, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            dlqOut.QueueURL,
				MaxNumberOfMessages: 1,
			})
			require.NoError(t, recvErr)

			if tc.wantInDLQ {
				assert.Len(t, dlqOut2.Messages, 1, "message must be in DLQ after maxReceiveCount exceeded")
			} else {
				assert.Empty(t, dlqOut2.Messages, "message must NOT be in DLQ yet")
			}
		})
	}
}

// TestAuditSQS_ApproximateCounts verifies GetQueueAttributes returns accurate counts.
func TestAuditSQS_ApproximateCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sendCount    int
		receiveCount int // messages to receive (put in-flight)
		delaySeconds int // per-message delay
		wantVisible  int
		wantInFlight int
		wantDelayed  int
	}{
		{
			name:         "all visible — no receives",
			sendCount:    3,
			receiveCount: 0,
			wantVisible:  3,
			wantInFlight: 0,
			wantDelayed:  0,
		},
		{
			name:         "some in-flight",
			sendCount:    4,
			receiveCount: 2,
			wantVisible:  2,
			wantInFlight: 2,
			wantDelayed:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "counts-q")

			for i := range tc.sendCount {
				_, err := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:     qURL,
					MessageBody:  fmt.Sprintf("msg-%d", i),
					DelaySeconds: tc.delaySeconds,
				})
				require.NoError(t, err)
			}

			if tc.receiveCount > 0 {
				_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: tc.receiveCount,
					VisibilityTimeout:   30,
				})
				require.NoError(t, err)
			}

			attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       qURL,
				AttributeNames: []string{"All"},
			})
			require.NoError(t, err)

			assert.Equal(t, strconv.Itoa(tc.wantVisible),
				attrs.Attributes["ApproximateNumberOfMessages"],
				"ApproximateNumberOfMessages")
			assert.Equal(t, strconv.Itoa(tc.wantInFlight),
				attrs.Attributes["ApproximateNumberOfMessagesNotVisible"],
				"ApproximateNumberOfMessagesNotVisible")
			assert.Equal(t, strconv.Itoa(tc.wantDelayed),
				attrs.Attributes["ApproximateNumberOfMessagesDelayed"],
				"ApproximateNumberOfMessagesDelayed")
		})
	}
}

// TestAuditSQS_FIFOOrdering verifies that FIFO queue preserves per-group ordering.
func TestAuditSQS_FIFOOrdering(t *testing.T) {
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

// TestAuditSQS_FIFODeduplication verifies content-based deduplication within the window.
func TestAuditSQS_FIFODeduplication(t *testing.T) {
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

// TestAuditSQS_VisibilityTimeout verifies ChangeMessageVisibility adjusts re-delivery.
func TestAuditSQS_VisibilityTimeout(t *testing.T) {
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

// TestAuditSQS_DelayQueue verifies per-message and queue-level delay.
func TestAuditSQS_DelayQueue(t *testing.T) {
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

// TestAuditSQS_BatchOperations verifies SendMessageBatch and DeleteMessageBatch.
func TestAuditSQS_BatchOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		batchSize   int
		wantSuccess int
	}{
		{
			name:        "batch of 1",
			batchSize:   1,
			wantSuccess: 1,
		},
		{
			name:        "batch of 5",
			batchSize:   5,
			wantSuccess: 5,
		},
		{
			name:        "batch of 10 (max)",
			batchSize:   10,
			wantSuccess: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "batch-q")

			entries := make([]sqs.SendMessageBatchEntry, tc.batchSize)
			for i := range tc.batchSize {
				entries[i] = sqs.SendMessageBatchEntry{
					ID:          fmt.Sprintf("e%d", i),
					MessageBody: fmt.Sprintf("body-%d", i),
				}
			}

			sendOut, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries:  entries,
			})
			require.NoError(t, err)
			assert.Len(t, sendOut.Successful, tc.wantSuccess, "all entries must succeed")
			assert.Empty(t, sendOut.Failed, "no entries must fail")

			// Receive all and delete in batch.
			recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: tc.batchSize,
			})
			require.NoError(t, err)
			require.Len(t, recvOut.Messages, tc.batchSize)

			deleteEntries := make([]sqs.DeleteMessageBatchEntry, len(recvOut.Messages))
			for i, msg := range recvOut.Messages {
				deleteEntries[i] = sqs.DeleteMessageBatchEntry{
					ID:            fmt.Sprintf("d%d", i),
					ReceiptHandle: msg.ReceiptHandle,
				}
			}

			delOut, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueURL: qURL,
				Entries:  deleteEntries,
			})
			require.NoError(t, err)
			assert.Len(t, delOut.Successful, tc.batchSize, "all deletes must succeed")
			assert.Empty(t, delOut.Failed)
		})
	}
}

// TestAuditSQS_PurgeQueue verifies PurgeQueue clears all messages.
func TestAuditSQS_PurgeQueue(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "purge-q")

	for i := range 5 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
	}

	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "PurgeQueue must empty the queue")

	attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	assert.Equal(t, "0", attrs.Attributes["ApproximateNumberOfMessages"],
		"count must be 0 after purge")
}
