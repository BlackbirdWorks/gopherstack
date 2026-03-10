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
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"get-test"}`)
			},
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "get-test",
			},
			wantStatus: http.StatusOK,
			wantProps:  `{"LogGroupName":"get-test"}`,
		},
		{
			name:  "not found returns 404",
			setup: nil,
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "nonexistent",
			},
			wantStatus: http.StatusNotFound,
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
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"list-test-1"}`)
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"list-test-2"}`)
				_, _ = h.Backend.CreateResource("AWS::S3::Bucket", `{"BucketName":"other-bucket"}`)
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
				_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"delete-me"}`)
			},
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "delete-me",
			},
			wantStatus: http.StatusOK,
			wantOp:     "DELETE",
		},
		{
			name:  "not found returns 404",
			setup: nil,
			body: map[string]any{
				"TypeName":   "AWS::Logs::LogGroup",
				"Identifier": "nonexistent",
			},
			wantStatus: http.StatusNotFound,
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
			name:  "not found returns 404",
			setup: nil,
			body: map[string]any{
				"TypeName":      "AWS::Logs::LogGroup",
				"Identifier":    "nonexistent",
				"PatchDocument": `[]`,
			},
			wantStatus: http.StatusNotFound,
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
				event, _ := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"status-test"}`)

				return event.RequestToken
			},
			wantStatus: http.StatusOK,
			wantOp:     "CREATE",
		},
		{
			name: "not found returns 404",
			setup: func(_ *cloudcontrol.Handler) string {
				return "nonexistent-token"
			},
			wantStatus: http.StatusNotFound,
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
		setup       func(*cloudcontrol.Handler) string
		name        string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name: "success",
			setup: func(h *cloudcontrol.Handler) string {
				event, _ := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"cancel-test"}`)

				return event.RequestToken
			},
			wantStatus:  http.StatusOK,
			wantStatus2: "CANCEL_COMPLETE",
		},
		{
			name: "not found returns 404",
			setup: func(_ *cloudcontrol.Handler) string {
				return "nonexistent-token"
			},
			wantStatus: http.StatusNotFound,
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
				assert.Equal(t, tt.wantStatus2, pe["OperationStatus"])
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
	_, _ = b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"all-1"}`)
	_, _ = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"all-2"}`)

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

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cloudcontrol.Provider{}
	assert.Equal(t, "CloudControl", p.Name())
}
