package xray_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func newTestHandler(t *testing.T) *xray.Handler {
	t.Helper()

	return xray.NewHandler(xray.NewInMemoryBackend())
}

func doXrayRequest(t *testing.T, h *xray.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.RequestURI = path

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Xray", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "PutTraceSegments")
	assert.Contains(t, ops, "CreateGroup")
	assert.Contains(t, ops, "GetGroups")
	assert.Contains(t, ops, "CreateSamplingRule")
	assert.Contains(t, ops, "GetSamplingRules")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "matches CreateGroup POST", method: http.MethodPost, path: "/CreateGroup", want: true},
		{name: "matches Groups POST", method: http.MethodPost, path: "/Groups", want: true},
		{name: "rejects GET", method: http.MethodGet, path: "/CreateGroup", want: false},
		{name: "rejects unknown path", method: http.MethodPost, path: "/Unknown", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "creates group",
			body:       map[string]any{"GroupName": "my-group"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate group returns 400",
			body:       map[string]any{"GroupName": "dup-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate group returns 400" {
				rec := doXrayRequest(t, h, "/CreateGroup", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/CreateGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupsToSeed []string
		wantStatus   int
		wantCount    int
	}{
		{
			name:       "returns empty list",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:         "returns seeded groups",
			groupsToSeed: []string{"group-a", "group-b"},
			wantStatus:   http.StatusOK,
			wantCount:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.groupsToSeed {
				rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/Groups", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			groups, ok := resp["Groups"].([]any)
			require.True(t, ok)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

func TestHandler_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing group",
			body:       map[string]any{"GroupName": "my-group"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"GroupName": "no-such-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "deletes existing group" {
				rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/DeleteGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "creates rule",
			body:       map[string]any{"SamplingRule": map[string]any{"RuleName": "my-rule", "FixedRate": 0.05, "Priority": 1}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing RuleName returns 400",
			body:       map[string]any{"SamplingRule": map[string]any{}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutTraceSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid segment",
			body: map[string]any{
				"TraceSegmentDocuments": []string{`{"trace_id":"1-abc","id":"s1","name":"test"}`},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty segments",
			body:       map[string]any{"TraceSegmentDocuments": []string{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TraceSegments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutTelemetryRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetTraceSummaries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"StartTime": 1700000000.0,
		"EndTime":   1700001000.0,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/UnknownOp", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
