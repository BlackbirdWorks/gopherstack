package azurequeue_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

const testAccount = "devstoreaccount1"

func newTestHandler(t *testing.T) *azurequeue.Handler {
	t.Helper()

	backend := azurequeue.NewInMemoryBackend()

	return azurequeue.NewHandler(backend)
}

// doRequest builds an echo context for method/path (with optional body) and
// invokes the handler directly, mirroring services/azureblob's doRequest.
func doRequest(t *testing.T, h *azurequeue.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func createQueue(t *testing.T, h *azurequeue.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/"+name, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func putMessage(t *testing.T, h *azurequeue.Handler, queue, text string) *httptest.ResponseRecorder {
	t.Helper()

	body := []byte("<QueueMessage><MessageText>" + text + "</MessageText></QueueMessage>")

	return doRequest(t, h, http.MethodPost, "/"+testAccount+"/"+queue+"/messages", body)
}

func TestQueueLifecycle_CreateListDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_list_delete_queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/myqueue", nil)
			require.Equal(t, http.StatusCreated, rec.Code, tt.name)
			assert.NotEmpty(t, rec.Header().Get("X-Ms-Version"))
			assert.NotEmpty(t, rec.Header().Get("X-Ms-Request-Id"))

			rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"?comp=list", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "<Name>myqueue</Name>")
			assert.Contains(t, rec.Body.String(), "<EnumerationResults")

			rec = doRequest(t, h, http.MethodDelete, "/"+testAccount+"/myqueue", nil)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)

			rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"?comp=list", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.NotContains(t, rec.Body.String(), "<Name>myqueue</Name>")
		})
	}
}

func TestCreateQueue_ExistingIsIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "recreate_same_name_returns_204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "dupe")

			rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/dupe", nil)
			assert.Equal(t, http.StatusNoContent, rec.Code, tt.name)
		})
	}
}

func TestDeleteQueue_MissingReturns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "missing_queue_404"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/does-not-exist", nil)

			require.Equal(t, http.StatusNotFound, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "QueueNotFound")
			assert.Equal(t, "QueueNotFound", rec.Header().Get("X-Ms-Error-Code"), tt.name)
		})
	}
}

func TestMessageLifecycle_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_get_delete_message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			rec := putMessage(t, h, "myqueue", "hello")
			require.Equal(t, http.StatusCreated, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "<MessageId>")
			assert.Contains(t, rec.Body.String(), "<PopReceipt>")
			assert.Contains(t, rec.Body.String(), "<TimeNextVisible>")

			messageID := extractXMLField(t, rec.Body.String(), "MessageId")
			popReceipt := extractXMLField(t, rec.Body.String(), "PopReceipt")

			rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "hello")
			assert.Contains(t, rec.Body.String(), "<DequeueCount>1</DequeueCount>")

			newPopReceipt := extractXMLField(t, rec.Body.String(), "PopReceipt")

			rec = doRequest(t, h, http.MethodDelete,
				"/"+testAccount+"/myqueue/messages/"+messageID+"?popreceipt="+newPopReceipt, nil)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)

			rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages?peekonly=true", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.NotContains(t, rec.Body.String(), "hello", tt.name)

			_ = popReceipt // the original Put pop receipt is stale after Get rotated it
		})
	}
}

func TestPeekMessages_DoesNotAssignPopReceipt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "peek_no_pop_receipt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			putMessage(t, h, "myqueue", "hello")

			rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages?peekonly=true", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.NotContains(t, rec.Body.String(), "<PopReceipt>", tt.name)
			assert.Contains(t, rec.Body.String(), "hello", tt.name)
		})
	}
}

func TestUpdateMessage_ChangesVisibilityAndOptionallyText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
	}{
		{name: "visibility_only", body: nil},
		{name: "replaces_text", body: []byte("<QueueMessage><MessageText>updated</MessageText></QueueMessage>")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			rec := putMessage(t, h, "myqueue", "original")
			messageID := extractXMLField(t, rec.Body.String(), "MessageId")
			popReceipt := extractXMLField(t, rec.Body.String(), "PopReceipt")

			rec = doRequest(t, h, http.MethodPut,
				"/"+testAccount+"/myqueue/messages/"+messageID+"?popreceipt="+popReceipt+"&visibilitytimeout=60",
				tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)
			assert.NotEmpty(t, rec.Header().Get("X-Ms-Popreceipt"), tt.name)
			assert.NotEmpty(t, rec.Header().Get("X-Ms-Time-Next-Visible"), tt.name)
		})
	}
}

func TestUpdateMessage_RequiresPopReceiptAndVisibilityTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "missing_popreceipt", query: "?visibilitytimeout=30"},
		{name: "missing_visibilitytimeout", query: "?popreceipt=x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			rec := putMessage(t, h, "myqueue", "x")
			messageID := extractXMLField(t, rec.Body.String(), "MessageId")

			rec = doRequest(t, h, http.MethodPut, "/"+testAccount+"/myqueue/messages/"+messageID+tt.query, nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "InvalidQueryParameterValue", tt.name)
		})
	}
}

func TestDeleteMessage_RequiresPopReceipt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "missing_popreceipt_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			rec := putMessage(t, h, "myqueue", "x")
			messageID := extractXMLField(t, rec.Body.String(), "MessageId")

			rec = doRequest(t, h, http.MethodDelete, "/"+testAccount+"/myqueue/messages/"+messageID, nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "InvalidQueryParameterValue", tt.name)
		})
	}
}

func TestDeleteMessage_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		messageID  string
		popReceipt string
		wantBody   string
		wantCode   int
	}{
		{name: "unknown_message", messageID: "does-not-exist", popReceipt: "x",
			wantCode: http.StatusNotFound, wantBody: "MessageNotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			rec := doRequest(t, h, http.MethodDelete,
				"/"+testAccount+"/myqueue/messages/"+tt.messageID+"?popreceipt="+tt.popReceipt, nil)
			require.Equal(t, tt.wantCode, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), tt.wantBody, tt.name)
		})
	}
}

func TestDeleteMessage_PopReceiptMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stale_pop_receipt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			rec := putMessage(t, h, "myqueue", "x")
			messageID := extractXMLField(t, rec.Body.String(), "MessageId")

			rec = doRequest(t, h, http.MethodDelete,
				"/"+testAccount+"/myqueue/messages/"+messageID+"?popreceipt=wrong", nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "PopReceiptMismatch", tt.name)
		})
	}
}

func TestClearMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "clear_removes_all_messages"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			putMessage(t, h, "myqueue", "a")
			putMessage(t, h, "myqueue", "b")

			rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/myqueue/messages", nil)
			require.Equal(t, http.StatusNoContent, rec.Code, tt.name)

			rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages?peekonly=true", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.NotContains(t, rec.Body.String(), "<MessageId>", tt.name)
		})
	}
}

func TestPutMessage_MissingQueueReturns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_message_missing_queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := putMessage(t, h, "does-not-exist", "x")
			require.Equal(t, http.StatusNotFound, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "QueueNotFound", tt.name)
		})
	}
}

func TestGetMessages_NumOfMessagesOutOfRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "zero_rejected", query: "?numofmessages=0"},
		{name: "too_many_rejected", query: "?numofmessages=33"},
		{name: "non_numeric_rejected", query: "?numofmessages=abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages"+tt.query, nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "QueryParameterValue", tt.name)
		})
	}
}

func TestPutMessage_VisibilityTimeoutOutOfRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "negative_rejected", query: "?visibilitytimeout=-1"},
		{name: "non_numeric_rejected", query: "?visibilitytimeout=abc"},
		{name: "over_seven_days_rejected", query: "?visibilitytimeout=604801"},
		// Put Message's visibilitytimeout must be strictly less than its own
		// messagettl -- equal is rejected, not just greater.
		{name: "equal_to_ttl_rejected", query: "?visibilitytimeout=60&messagettl=60"},
		{name: "greater_than_ttl_rejected", query: "?visibilitytimeout=120&messagettl=60"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			body := []byte("<QueueMessage><MessageText>x</MessageText></QueueMessage>")
			rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/myqueue/messages"+tt.query, body)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "QueryParameterValue", tt.name)
		})
	}
}

func TestPutMessage_NeverExpireSentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "messagettl_negative_one_accepted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			body := []byte("<QueueMessage><MessageText>x</MessageText></QueueMessage>")
			rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/myqueue/messages?messagettl=-1", body)
			require.Equal(t, http.StatusCreated, rec.Code, tt.name)
		})
	}
}

// TestPutMessage_MessageTTLZeroRejected is a regression test: real Azure
// Queue Storage only accepts messagettl=-1 (never expire) or a strictly
// positive value -- zero would create a message that expires the instant
// it's created.
func TestPutMessage_MessageTTLZeroRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "messagettl_zero_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			body := []byte("<QueueMessage><MessageText>x</MessageText></QueueMessage>")
			rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/myqueue/messages?messagettl=0", body)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "OutOfRangeQueryParameterValue", tt.name)
		})
	}
}

// TestGetMessages_VisibilityTimeoutRange is a regression test: unlike Put/
// Update Message, Get Messages must reject visibilitytimeout=0 (a dequeued
// message must be hidden for at least one second) as well as values above
// the seven-day cap.
func TestGetMessages_VisibilityTimeoutRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "zero_rejected", query: "?visibilitytimeout=0"},
		{name: "over_seven_days_rejected", query: "?visibilitytimeout=604801"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			putMessage(t, h, "myqueue", "x")

			rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/myqueue/messages"+tt.query, nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "OutOfRangeQueryParameterValue", tt.name)
		})
	}
}

// TestUpdateMessage_VisibilityTimeoutRange covers Update Message's
// visibilitytimeout bounds: zero is permitted (unlike Get Messages), but the
// seven-day cap still applies.
func TestUpdateMessage_VisibilityTimeoutRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		visibility string
		wantStatus int
	}{
		{name: "zero_accepted", visibility: "0", wantStatus: http.StatusNoContent},
		{name: "over_seven_days_rejected", visibility: "604801", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")
			rec := putMessage(t, h, "myqueue", "x")
			messageID := extractXMLField(t, rec.Body.String(), "MessageId")
			popReceipt := extractXMLField(t, rec.Body.String(), "PopReceipt")

			rec = doRequest(t, h, http.MethodPut,
				"/"+testAccount+"/myqueue/messages/"+messageID+"?popreceipt="+popReceipt+
					"&visibilitytimeout="+tt.visibility, nil)
			require.Equal(t, tt.wantStatus, rec.Code, tt.name)
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_queues"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createQueue(t, h, "myqueue")

			h.Reset()

			rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"?comp=list", nil)
			require.Equal(t, http.StatusOK, rec.Code, tt.name)
			assert.NotContains(t, rec.Body.String(), "<Name>myqueue</Name>", tt.name)
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "AzureQueue", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "PutMessage")
	assert.Contains(t, ops, "GetMessages")
	assert.Contains(t, ops, "ListQueues")
}

func TestInvalidURI_EmptyAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "root_path_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodGet, "/", nil)
			require.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Contains(t, rec.Body.String(), "InvalidUri", tt.name)
		})
	}
}

// extractXMLField does a minimal, dependency-free extraction of
// "<Field>value</Field>" from an XML body, sufficient for these tests
// without pulling in a full XML decode in the test helper itself.
func extractXMLField(t *testing.T, body, field string) string {
	t.Helper()

	open := "<" + field + ">"
	closeTag := "</" + field + ">"

	start := indexOrFail(t, body, open)
	end := indexOrFail(t, body[start:], closeTag)

	return body[start+len(open) : start+end]
}

func indexOrFail(t *testing.T, s, substr string) int {
	t.Helper()

	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}

	require.FailNowf(t, "substring not found", "substring %q not found in %q", substr, s)

	return -1
}
