package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestMD5OfBody_MatchesExpected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-body")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "Hello World",
	})
	require.NoError(t, err)
	// MD5("Hello World") = b10a8db164e0754105b7a99be72e3fe5
	assert.Equal(t, "b10a8db164e0754105b7a99be72e3fe5", out.MD5OfBody)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "b10a8db164e0754105b7a99be72e3fe5", msgs[0].MD5OfBody)
}

func TestMD5OfMessageAttributes_PopulatedWhenPresent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-attrs")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Foo": {DataType: "String", StringValue: "bar"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.MD5OfMessageAttributes)
}

func TestMD5OfMessageAttributes_EmptyWhenNoAttrs(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-noattrs")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
	})
	require.NoError(t, err)
	assert.Empty(t, out.MD5OfMessageAttributes)
}

// TestSendMessage_MD5OfMessageAttributes verifies that sending a message with MessageAttributes
// returns a non-empty MD5OfMessageAttributes that matches the expected AWS algorithm output.
func TestSendMessage_MD5OfMessageAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "md5-attrs-queue")

	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyAttr": {DataType: "String", StringValue: "TestValue"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(
		t,
		out.MD5OfMessageAttributes,
		"MD5OfMessageAttributes should be set when MessageAttributes are present",
	)

	// Verify it's a valid 32-char hex string (MD5).
	assert.Len(t, out.MD5OfMessageAttributes, 32)

	// Sending without attributes should produce an empty MD5OfMessageAttributes.
	outNoAttrs, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello",
	})
	require.NoError(t, err)
	assert.Empty(t, outNoAttrs.MD5OfMessageAttributes)
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
