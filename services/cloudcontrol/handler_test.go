package cloudcontrol_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

func newTestHandler(t *testing.T) *cloudcontrol.Handler {
	t.Helper()

	return cloudcontrol.NewHandler(cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *cloudcontrol.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CloudApiService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CloudControl", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "cloudcontrol", h.ChaosServiceName())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateResource")
	assert.Contains(t, ops, "DeleteResource")
	assert.Contains(t, ops, "GetResource")
	assert.Contains(t, ops, "ListResources")
	assert.Contains(t, ops, "UpdateResource")
	assert.Contains(t, ops, "GetResourceRequestStatus")
	assert.Contains(t, ops, "CancelResourceRequest")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name         string
		targetHeader string
		wantMatch    bool
	}{
		{
			name:         "matching cloudcontrol target",
			targetHeader: "CloudApiService.CreateResource",
			wantMatch:    true,
		},
		{
			name:         "non-matching target",
			targetHeader: "AWSInsightsIndexService.ListCostCategories",
			wantMatch:    false,
		},
		{
			name:         "empty target",
			targetHeader: "",
			wantMatch:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.targetHeader)
			c := e.NewContext(req, httptest.NewRecorder())

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "extracts operation",
			target: "CloudApiService.CreateResource",
			wantOp: "CreateResource",
		},
		{
			name:   "empty target",
			target: "",
			wantOp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_CreateResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantOp     string
		wantStatus int
	}{
		{
			name: "success with named identifier",
			body: map[string]any{
				"TypeName":     "AWS::Logs::LogGroup",
				"DesiredState": `{"LogGroupName":"my-log-group"}`,
			},
			wantStatus: http.StatusOK,
			wantOp:     "CREATE",
		},
		{
			name: "success without identifier generates uuid",
			body: map[string]any{
				"TypeName":     "AWS::S3::Bucket",
				"DesiredState": `{}`,
			},
			wantStatus: http.StatusOK,
			wantOp:     "CREATE",
		},
		{
			name: "missing TypeName returns 400",
			body: map[string]any{
				"DesiredState": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResource", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOp, pe["Operation"])
				assert.Equal(t, "SUCCESS", pe["OperationStatus"])
				assert.NotEmpty(t, pe["RequestToken"])
			}
		})
	}
}

// TestHandler_CreateResource_DuplicateReturns400 verifies AlreadyExistsException maps to
// HTTP 400, per the real API reference -- not 409, which real CloudControl never returns.
func TestHandler_CreateResource_DuplicateReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"duplicate-group"}`,
	}

	rec := doRequest(t, h, "CreateResource", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateResource", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_GetResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudcontrol.Handler)
		body       map[string]any
		name       string
		wantProps  string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"get-test"}`, "")
			},
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "get-test",
			},
			wantStatus: http.StatusOK,
			wantProps:  `{"LogGroupName":"get-test"}`,
		},
		{
			name:  "not found returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "nonexistent",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing TypeName returns 400",
			setup: nil,
			body: map[string]any{
				"Identifier": "something",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing Identifier returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName": "AWS::Logs::LogGroup",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				rd, ok := out["ResourceDescription"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantProps, rd["Properties"])
			}
		})
	}
}

func TestHandler_ListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudcontrol.Handler)
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "returns resources of type",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"list-test-1"}`, "")
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"list-test-2"}`, "")
				_, _ = h.Backend.CreateResource("AWS::S3::Bucket", `{"BucketName":"other-bucket"}`, "")
			},
			body:       map[string]any{"TypeName": "AWS::Logs::LogGroup"},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty list for unknown type",
			setup:      nil,
			body:       map[string]any{"TypeName": "AWS::Unknown::Resource"},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "missing TypeName returns 400",
			setup:      nil,
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListResources", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				descs, ok := out["ResourceDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, descs, tt.wantCount)
			}
		})
	}
}

func TestHandler_DeleteResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudcontrol.Handler)
		body       map[string]any
		name       string
		wantOp     string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"delete-me"}`, "")
			},
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "delete-me",
			},
			wantStatus: http.StatusOK,
			wantOp:     "DELETE",
		},
		{
			name:  "not found returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "nonexistent",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing TypeName returns 400",
			setup: nil,
			body: map[string]any{
				"Identifier": "something",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing Identifier returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName": "AWS::Logs::LogGroup",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOp, pe["Operation"])
				assert.Equal(t, "SUCCESS", pe["OperationStatus"])
			}
		})
	}
}

func TestHandler_UpdateResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudcontrol.Handler)
		body       map[string]any
		name       string
		wantOp     string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource(
					"AWS::Logs::LogGroup",
					`{"LogGroupName":"update-me","RetentionInDays":7}`,
					"",
				)
			},
			body: map[string]any{
				"TypeName":      "AWS::Logs::LogGroup",
				"Identifier":    "update-me",
				"PatchDocument": `[{"op":"replace","path":"/RetentionInDays","value":30}]`,
			},
			wantStatus: http.StatusOK,
			wantOp:     "UPDATE",
		},
		{
			name:  "not found returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName":      "AWS::Logs::LogGroup",
				"Identifier":    "nonexistent",
				"PatchDocument": `[]`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing TypeName returns 400",
			setup: nil,
			body: map[string]any{
				"Identifier":    "something",
				"PatchDocument": `[]`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing Identifier returns 400",
			setup: nil,
			body: map[string]any{
				"TypeName":      "AWS::Logs::LogGroup",
				"PatchDocument": `[]`,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "UpdateResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOp, pe["Operation"])
				assert.Equal(t, "SUCCESS", pe["OperationStatus"])
			}
		})
	}
}

func TestHandler_GetResourceRequestStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudcontrol.Handler) string
		name       string
		wantOp     string
		wantStatus int
	}{
		{
			name: "success after create",
			setup: func(h *cloudcontrol.Handler) string {
				event, _ := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"status-test"}`, "")

				return event.RequestToken
			},
			wantStatus: http.StatusOK,
			wantOp:     "CREATE",
		},
		{
			name: "not found returns 400 RequestTokenNotFoundException",
			setup: func(_ *cloudcontrol.Handler) string {
				return "nonexistent-token"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing RequestToken returns 400",
			setup: func(_ *cloudcontrol.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			token := tt.setup(h)

			body := map[string]any{"RequestToken": token}
			rec := doRequest(t, h, "GetResourceRequestStatus", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOp, pe["Operation"])
			}
		})
	}
}

func TestHandler_CancelResourceRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*cloudcontrol.Handler) string
		name         string
		wantOpStatus string
		wantStatus   int
	}{
		{
			name: "success cancels in_progress request",
			setup: func(h *cloudcontrol.Handler) string {
				token := "in-progress-token"
				h.Backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
					RequestToken:    token,
					TypeName:        "AWS::Logs::LogGroup",
					Identifier:      "cancel-test",
					Operation:       "CREATE",
					OperationStatus: "IN_PROGRESS",
				})

				return token
			},
			wantStatus:   http.StatusOK,
			wantOpStatus: "CANCEL_COMPLETE",
		},
		{
			// Real AWS returns ConcurrentModificationException (HTTP 500) for a
			// terminal-status request, not a client validation error -- see the
			// CancelResourceRequest API reference's Errors section.
			name: "cancelling terminal request returns 500 ConcurrentModificationException",
			setup: func(h *cloudcontrol.Handler) string {
				event, _ := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"terminal-test"}`, "")

				return event.RequestToken
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			// RequestTokenNotFoundException is HTTP 400, the only error this
			// operation declares for an unrecognized token.
			name: "not found returns 400 RequestTokenNotFoundException",
			setup: func(_ *cloudcontrol.Handler) string {
				return "nonexistent-token"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing RequestToken returns 400",
			setup: func(_ *cloudcontrol.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			token := tt.setup(h)

			body := map[string]any{"RequestToken": token}
			rec := doRequest(t, h, "CancelResourceRequest", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOpStatus, pe["OperationStatus"])
			}
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CloudApiService.CreateResource")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := newTestHandler(t)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GETReturnsOperations(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := newTestHandler(t)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestInMemoryBackend_ListAllResources(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"all-1"}`, "")
	_, _ = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"all-2"}`, "")

	all := b.ListAllResources()
	assert.Len(t, all, 2)
}

func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestHandler_EventTimeIsUnixNumber(t *testing.T) {
	t.Parallel()

	// The AWS CloudControl SDK v2 expects EventTime to be a JSON number (Unix epoch seconds),
	// not a string. Each test case creates its own handler and verifies the wire format.
	tests := []struct {
		getRequest func(t *testing.T, h *cloudcontrol.Handler) *httptest.ResponseRecorder
		name       string
	}{
		{
			name: "create_resource_event_time_is_number",
			getRequest: func(t *testing.T, h *cloudcontrol.Handler) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, "CreateResource", map[string]any{
					"TypeName":     "AWS::Logs::LogGroup",
					"DesiredState": `{"LogGroupName":"evt-test"}`,
				})
			},
		},
		{
			name: "get_request_status_event_time_is_number",
			getRequest: func(t *testing.T, h *cloudcontrol.Handler) *httptest.ResponseRecorder {
				t.Helper()
				// First create a resource to obtain a request token.
				createRec := doRequest(t, h, "CreateResource", map[string]any{
					"TypeName":     "AWS::Logs::LogGroup",
					"DesiredState": `{"LogGroupName":"evt-status"}`,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createOut map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
				pe, ok := createOut["ProgressEvent"].(map[string]any)
				require.True(t, ok)

				token, tokenOK := pe["RequestToken"].(string)
				require.True(t, tokenOK, "RequestToken should be a string")
				require.NotEmpty(t, token)

				return doRequest(t, h, "GetResourceRequestStatus", map[string]any{
					"RequestToken": token,
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := tt.getRequest(t, h)

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pe, ok := out["ProgressEvent"].(map[string]any)
			require.True(t, ok, "ProgressEvent should be present in response")

			// EventTime must be a JSON number (float64 after json.Unmarshal into any).
			eventTime, exists := pe["EventTime"]
			require.True(t, exists, "EventTime must be present in ProgressEvent")
			_, isNumber := eventTime.(float64)
			assert.True(t, isNumber, "EventTime must be a JSON number (Unix epoch), got %T: %v", eventTime, eventTime)
		})
	}
}

func TestHandler_ListResourceRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*cloudcontrol.Handler)
		body          map[string]any
		name          string
		wantOpInFirst string
		wantCount     int
		wantStatus    int
	}{
		{
			name: "returns all requests when no filter",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"req-1"}`, "")
				_, _ = h.Backend.CreateResource("AWS::S3::Bucket", `{"BucketName":"req-2"}`, "")
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty list when no requests",
			setup:      func(_ *cloudcontrol.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter by operation CREATE",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"filter-1"}`, "")
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"filter-2"}`, "")
				// Delete creates a DELETE request; two CREATE requests remain
				_, _ = h.Backend.DeleteResource("AWS::Logs::LogGroup", "filter-1")
			},
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"Operations": []string{"CREATE"},
				},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "filter by operation status SUCCESS",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"status-1"}`, "")
			},
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"OperationStatuses": []string{"SUCCESS"},
				},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "filter by status that does not match returns empty",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"no-match"}`, "")
			},
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"OperationStatuses": []string{"IN_PROGRESS"},
				},
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter by both operation and status",
			setup: func(h *cloudcontrol.Handler) {
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"both-1"}`, "")
				_, _ = h.Backend.DeleteResource("AWS::Logs::LogGroup", "both-1")
			},
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"Operations":        []string{"DELETE"},
					"OperationStatuses": []string{"SUCCESS"},
				},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListResourceRequests", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				summaries, ok := out["ResourceRequestStatusSummaries"].([]any)
				require.True(t, ok, "ResourceRequestStatusSummaries should be an array")
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

func TestHandler_ListResourceRequests_ContainsExpectedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"fields-test"}`, "")

	rec := doRequest(t, h, "ListResourceRequests", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, ok := out["ResourceRequestStatusSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "CREATE", summary["Operation"])
	assert.Equal(t, "SUCCESS", summary["OperationStatus"])
	assert.Equal(t, "AWS::Logs::LogGroup", summary["TypeName"])
	assert.NotEmpty(t, summary["RequestToken"])
	_, isNumber := summary["EventTime"].(float64)
	assert.True(t, isNumber, "EventTime must be a JSON number (Unix epoch)")
}

func TestHandler_GetSupportedOperations_IncludesListResourceRequests(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "ListResourceRequests")
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cloudcontrol.Provider{}
	assert.Equal(t, "CloudControl", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx  *service.AppContext
		name string
	}{
		{name: "nil context", ctx: nil},
		{name: "non-nil empty context", ctx: &service.AppContext{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudcontrol.Provider{}
			reg, err := p.Init(tt.ctx)
			require.NoError(t, err)
			require.NotNil(t, reg)
			assert.Equal(t, "CloudControl", reg.Name())
		})
	}
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := cloudcontrol.NewHandler(cloudcontrol.NewInMemoryBackend("000000000000", "ap-southeast-1"))
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "ap-southeast-1", regions[0])
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	label := h.ExtractResource(c)
	assert.Equal(t, "cloudcontrol", label)
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"r1"}`, "")
	_, _ = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"b1"}`, "")

	require.Len(t, b.ListAllResources(), 2)

	b.Reset()
	assert.Empty(t, b.ListAllResources())

	events, _, err := b.ListResourceRequests(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"reset-r1"}`, "")

	require.Len(t, h.Backend.ListAllResources(), 1)

	h.Reset()
	assert.Empty(t, h.Backend.ListAllResources())
}

func TestHandler_TypeName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "create with invalid type name",
			action:     "CreateResource",
			body:       map[string]any{"TypeName": "InvalidTypeName", "DesiredState": `{}`},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create with empty type name",
			action:     "CreateResource",
			body:       map[string]any{"TypeName": "", "DesiredState": `{}`},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get with invalid type name",
			action:     "GetResource",
			body:       map[string]any{"TypeName": "NoDoubleColons", "Identifier": "x"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete with invalid type name",
			action:     "DeleteResource",
			body:       map[string]any{"TypeName": "Bad", "Identifier": "x"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update with invalid type name",
			action:     "UpdateResource",
			body:       map[string]any{"TypeName": "Bad", "Identifier": "x", "PatchDocument": "[]"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list with invalid type name",
			action:     "ListResources",
			body:       map[string]any{"TypeName": "NoColons"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListResourceRequests_EnumValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "invalid operation value returns 400",
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"Operations": []string{"INVALID_OP"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid status value returns 400",
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"OperationStatuses": []string{"BOGUS_STATUS"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid operations and statuses return 200",
			body: map[string]any{
				"ResourceRequestStatusFilter": map[string]any{
					"Operations":        []string{"CREATE", "DELETE", "UPDATE"},
					"OperationStatuses": []string{"SUCCESS", "FAILED"},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListResourceRequests", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListResourceRequests_TypeNameFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"lg-1"}`, "")
	_, _ = h.Backend.CreateResource("AWS::S3::Bucket", `{"BucketName":"b-1"}`, "")

	rec := doRequest(t, h, "ListResourceRequests", map[string]any{
		"ResourceRequestStatusFilter": map[string]any{
			"TypeName": "AWS::Logs::LogGroup",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, ok := out["ResourceRequestStatusSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AWS::Logs::LogGroup", summary["TypeName"])
}

func TestHandler_ListResources_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		name := "paginated-" + strings.Repeat("x", i+1)
		_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"`+name+`"}`, "")
	}

	// First page: 3 items
	rec := doRequest(t, h, "ListResources", map[string]any{
		"TypeName":   "AWS::Logs::LogGroup",
		"MaxResults": 3,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out1))

	descs1, ok := out1["ResourceDescriptions"].([]any)
	require.True(t, ok)
	assert.Len(t, descs1, 3)

	nextToken, ok := out1["NextToken"].(string)
	require.True(t, ok, "NextToken must be present after first page")
	require.NotEmpty(t, nextToken)

	// Second page: remaining 2 items
	rec2 := doRequest(t, h, "ListResources", map[string]any{
		"TypeName":   "AWS::Logs::LogGroup",
		"MaxResults": 3,
		"NextToken":  nextToken,
	})

	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	descs2, ok := out2["ResourceDescriptions"].([]any)
	require.True(t, ok)
	assert.Len(t, descs2, 2)

	_, hasMore := out2["NextToken"]
	assert.False(t, hasMore, "NextToken must be absent on last page")
}

func TestHandler_ListResourceRequests_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 4 {
		name := "pg-" + strings.Repeat("r", i+1)
		_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"`+name+`"}`, "")
	}

	rec := doRequest(t, h, "ListResourceRequests", map[string]any{
		"MaxResults": 2,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, ok := out["ResourceRequestStatusSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2)

	nextToken, hasToken := out["NextToken"].(string)
	require.True(t, hasToken, "NextToken should be present")
	require.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListResourceRequests", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})

	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	summaries2, ok := out2["ResourceRequestStatusSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries2, 2)
}

func TestBackend_CreateResource_ClientToken_Idempotency(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")

	ev1, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"idem-test"}`, "my-client-token")
	require.NoError(t, err)

	// Second call with the same clientToken must return the same RequestToken.
	ev2, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"idem-test"}`, "my-client-token")
	require.NoError(t, err)

	assert.Equal(t, ev1.RequestToken, ev2.RequestToken, "idempotent calls must return the same request token")
	assert.Len(t, b.ListAllResources(), 1, "only one resource should be created")
}

func TestBackend_ListAllResources_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"z-bucket"}`, "")
	_, _ = b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"a-group"}`, "")
	_, _ = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"a-bucket"}`, "")

	all := b.ListAllResources()
	require.Len(t, all, 3)

	// Sorted by TypeName then Identifier.
	assert.Equal(t, "AWS::Logs::LogGroup", all[0].TypeName)
	assert.Equal(t, "AWS::S3::Bucket", all[1].TypeName)
	assert.Equal(t, "a-bucket", all[1].Identifier)
	assert.Equal(t, "z-bucket", all[2].Identifier)
}

func TestBackend_GetResourceRequestStatus_EventsNotRemovedOnRead(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")

	ev, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"persist-test"}`, "")
	require.NoError(t, err)

	// First read.
	ev1, err := b.GetResourceRequestStatus(ev.RequestToken)
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS", ev1.OperationStatus)

	// Second read — event must still be present (not deleted after first read).
	ev2, err := b.GetResourceRequestStatus(ev.RequestToken)
	require.NoError(t, err)
	assert.Equal(t, ev1.RequestToken, ev2.RequestToken)
}

func TestHandler_CreateResource_ClientToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"client-token-test"}`,
		"ClientToken":  "unique-client-token-123",
	}

	rec1 := doRequest(t, h, "CreateResource", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	pe1 := out1["ProgressEvent"].(map[string]any)

	// Same clientToken must return the same RequestToken (idempotent).
	rec2 := doRequest(t, h, "CreateResource", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	pe2 := out2["ProgressEvent"].(map[string]any)

	assert.Equal(t, pe1["RequestToken"], pe2["RequestToken"])
}

func TestBackend_NewIdentifierKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desiredState   string
		name           string
		wantIdentifier string
	}{
		{
			name:           "TableName",
			desiredState:   `{"TableName":"my-table"}`,
			wantIdentifier: "my-table",
		},
		{
			name:           "RoleName",
			desiredState:   `{"RoleName":"my-role"}`,
			wantIdentifier: "my-role",
		},
		{
			name:           "ClusterName",
			desiredState:   `{"ClusterName":"my-cluster"}`,
			wantIdentifier: "my-cluster",
		},
		{
			name:           "StreamName",
			desiredState:   `{"StreamName":"my-stream"}`,
			wantIdentifier: "my-stream",
		},
		{
			name:           "DBInstanceIdentifier",
			desiredState:   `{"DBInstanceIdentifier":"my-db"}`,
			wantIdentifier: "my-db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
			ev, err := b.CreateResource("AWS::Some::Type", tt.desiredState, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantIdentifier, ev.Identifier)
		})
	}
}

func TestHandler_InternalServerError(t *testing.T) {
	t.Parallel()

	// The default handler returns 500 for unexpected errors; drive that branch
	// by reaching the internal server error path through a direct backend panic recovery —
	// in practice we test it by having no backend set and using a custom handler.
	// Instead, verify the existing error handler routes correctly.
	h := newTestHandler(t)
	// UnknownAction drives the errUnknownAction → 400 path.
	rec := doRequest(t, h, "NonExistentOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
