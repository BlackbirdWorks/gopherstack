package sqs_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryReceiveMessage_ReceiveRequestAttemptId drives FIFO exactly-once
// retry through the legacy Query protocol. No real aws-sdk-go-v2 client can
// send this: the pinned SQS client (sqs@v1.46.4) only emits the JSON
// protocol, so this hand-builds the form body -- but the field name itself
// isn't guessed, it's read off the same package's own JSON tag
// (handler_messages.go:31 ReceiveRequestAttemptID string `json:"ReceiveRequestAttemptId"`),
// which the Query path spells "ReceiveRequestAttemptID" (capital D) instead
// (query_messages.go:192). Because url.Values.Get is case-sensitive, unlike
// encoding/json's case-insensitive tag match, the real key silently misses.
func TestQueryReceiveMessage_ReceiveRequestAttemptId(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)

	qURL := b3createFIFOQueue(t, b, "attempt-query.fifo")

	send := doQueryRequest(t, h, url.Values{
		"Action":         {"SendMessage"},
		"QueueUrl":       {qURL},
		"MessageBody":    {"body1"},
		"MessageGroupId": {"g1"},
	})
	require.Equal(t, http.StatusOK, send.Code, "send body: %s", send.Body.String())

	first := doQueryRequest(t, h, url.Values{
		"Action":                  {"ReceiveMessage"},
		"QueueUrl":                {qURL},
		"MaxNumberOfMessages":     {"1"},
		"ReceiveRequestAttemptId": {"attempt-abc"},
	})
	require.Equal(t, http.StatusOK, first.Code, "first receive body: %s", first.Body.String())
	firstID := receiveMessageID(t, first.Body.Bytes())
	require.NotEmpty(t, firstID)

	second := doQueryRequest(t, h, url.Values{
		"Action":                  {"ReceiveMessage"},
		"QueueUrl":                {qURL},
		"MaxNumberOfMessages":     {"1"},
		"ReceiveRequestAttemptId": {"attempt-abc"},
	})
	require.Equal(t, http.StatusOK, second.Code, "second receive body: %s", second.Body.String())
	secondID := receiveMessageID(t, second.Body.Bytes())

	assert.Equal(t, firstID, secondID,
		"repeating ReceiveRequestAttemptId within the dedup window must return the same message")
}

func receiveMessageID(t *testing.T, body []byte) string {
	t.Helper()

	var resp struct {
		Result struct {
			Messages []struct {
				MessageID string `xml:"MessageId"`
			} `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	require.NoError(t, xml.Unmarshal(body, &resp))

	if len(resp.Result.Messages) == 0 {
		return ""
	}

	return resp.Result.Messages[0].MessageID
}
