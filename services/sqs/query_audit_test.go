package sqs_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// queryCreateQueue creates a queue via the Query protocol and returns its URL.
// It reuses the shared doQueryRequest helper (defined in accuracy1677_test.go).
func queryCreateQueue(t *testing.T, h *sqs.Handler, name string) string {
	t.Helper()

	rec := doQueryRequest(t, h, url.Values{
		"Action":    {"CreateQueue"},
		"QueueName": {name},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			QueueURL string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Result.QueueURL)

	return resp.Result.QueueURL
}

// TestQueryProtocol_TagListUntagQueue exercises the Query-protocol dispatch for
// TagQueue, ListQueueTags, and UntagQueue. These operations previously routed to
// ErrUnknownAction in the Query protocol despite being advertised, so the test
// asserts a real XML result (not an InvalidAction / unknown-action error).
func TestQueryProtocol_TagListUntagQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := queryCreateQueue(t, h, "query-tag-queue")

	// TagQueue with Tags.member.N.Key / Tags.member.N.Value encoding.
	tagRec := doQueryRequest(t, h, url.Values{
		"Action":              {"TagQueue"},
		"QueueUrl":            {queueURL},
		"Tags.member.1.Key":   {"env"},
		"Tags.member.1.Value": {"prod"},
		"Tags.member.2.Key":   {"team"},
		"Tags.member.2.Value": {"platform"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code, "tag body: %s", tagRec.Body.String())
	assert.NotContains(t, tagRec.Body.String(), "InvalidAction")
	assert.NotContains(t, tagRec.Body.String(), "UnknownOperation")
	assert.Contains(t, tagRec.Body.String(), "TagQueueResponse")

	// ListQueueTags should return the tags we just set.
	listRec := doQueryRequest(t, h, url.Values{
		"Action":   {"ListQueueTags"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, listRec.Code, "list body: %s", listRec.Body.String())

	var listResp struct {
		Result struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tag"`
		} `xml:"ListQueueTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))

	got := make(map[string]string, len(listResp.Result.Tags))
	for _, tg := range listResp.Result.Tags {
		got[tg.Key] = tg.Value
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)

	// UntagQueue with TagKeys.member.N encoding.
	untagRec := doQueryRequest(t, h, url.Values{
		"Action":           {"UntagQueue"},
		"QueueUrl":         {queueURL},
		"TagKeys.member.1": {"team"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code, "untag body: %s", untagRec.Body.String())
	assert.Contains(t, untagRec.Body.String(), "UntagQueueResponse")

	listRec2 := doQueryRequest(t, h, url.Values{
		"Action":   {"ListQueueTags"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, listRec2.Code)
	listResp.Result.Tags = nil
	require.NoError(t, xml.Unmarshal(listRec2.Body.Bytes(), &listResp))
	got = make(map[string]string)
	for _, tg := range listResp.Result.Tags {
		got[tg.Key] = tg.Value
	}
	assert.Equal(t, map[string]string{"env": "prod"}, got)
}

// TestQueryProtocol_ListDeadLetterSourceQueues exercises the Query-protocol
// dispatch for ListDeadLetterSourceQueues, asserting a real (empty) result
// rather than an unknown-action error.
func TestQueryProtocol_ListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := queryCreateQueue(t, h, "query-dlq-source-queue")

	rec := doQueryRequest(t, h, url.Values{
		"Action":   {"ListDeadLetterSourceQueues"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "InvalidAction")
	assert.Contains(t, rec.Body.String(), "ListDeadLetterSourceQueuesResponse")
}

// TestQueryProtocol_MessageMoveTasks exercises the Query-protocol dispatch for
// StartMessageMoveTask, ListMessageMoveTasks, and CancelMessageMoveTask. The key
// assertion is that these no longer fall through to ErrUnknownAction.
func TestQueryProtocol_MessageMoveTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set up a DLQ (source) and a redrive destination queue, then redrive.
	dlqURL := queryCreateQueue(t, h, "query-move-dlq")
	dlqArn := queryQueueArn(t, h, dlqURL)
	destURL := queryCreateQueue(t, h, "query-move-dest")
	destArn := queryQueueArn(t, h, destURL)

	startRec := doQueryRequest(t, h, url.Values{
		"Action":         {"StartMessageMoveTask"},
		"SourceArn":      {dlqArn},
		"DestinationArn": {destArn},
	})
	require.Equal(t, http.StatusOK, startRec.Code, "start body: %s", startRec.Body.String())
	assert.NotContains(t, startRec.Body.String(), "InvalidAction")
	assert.Contains(t, startRec.Body.String(), "StartMessageMoveTaskResponse")

	var startResp struct {
		Result struct {
			TaskHandle string `xml:"TaskHandle"`
		} `xml:"StartMessageMoveTaskResult"`
	}
	require.NoError(t, xml.Unmarshal(startRec.Body.Bytes(), &startResp))

	listRec := doQueryRequest(t, h, url.Values{
		"Action":    {"ListMessageMoveTasks"},
		"SourceArn": {dlqArn},
	})
	require.Equal(t, http.StatusOK, listRec.Code, "list body: %s", listRec.Body.String())
	assert.Contains(t, listRec.Body.String(), "ListMessageMoveTasksResponse")
	assert.NotContains(t, listRec.Body.String(), "InvalidAction")

	if handle := strings.TrimSpace(startResp.Result.TaskHandle); handle != "" {
		cancelRec := doQueryRequest(t, h, url.Values{
			"Action":     {"CancelMessageMoveTask"},
			"TaskHandle": {handle},
		})
		// A real backend result OR a domain error is acceptable here; what must
		// NOT happen is an unknown-action (InvalidAction) routing failure.
		assert.NotContains(t, cancelRec.Body.String(), "InvalidAction")
	}
}

// queryQueueArn fetches the QueueArn attribute for queueURL via the Query protocol.
func queryQueueArn(t *testing.T, h *sqs.Handler, queueURL string) string {
	t.Helper()

	rec := doQueryRequest(t, h, url.Values{
		"Action":          {"GetQueueAttributes"},
		"QueueUrl":        {queueURL},
		"AttributeName.1": {"QueueArn"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "attr body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			Attributes []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
		} `xml:"GetQueueAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	for _, a := range resp.Result.Attributes {
		if a.Name == "QueueArn" {
			return a.Value
		}
	}

	t.Fatalf("QueueArn not found in attributes: %s", rec.Body.String())

	return ""
}
