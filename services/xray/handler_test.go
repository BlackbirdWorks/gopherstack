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
		setup      func(*xray.Handler)
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
			name: "duplicate group returns 400",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("dup-group", "")
			},
			body:       map[string]any{"GroupName": "dup-group"},
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
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
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

			if tt.setup != nil {
				tt.setup(h)
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
			name: "creates rule",
			body: map[string]any{
				"SamplingRule": map[string]any{"RuleName": "my-rule", "FixedRate": 0.05, "Priority": 1},
			},
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

func TestHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "gets existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
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
			body:       map[string]any{"GroupName": "missing-group"},
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

			rec := doXrayRequest(t, h, "/GetGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
			body:       map[string]any{"GroupName": "my-group", "FilterExpression": `service("updated")`},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"GroupName": "missing-group"},
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

			rec := doXrayRequest(t, h, "/UpdateGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSamplingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ruleNames []string
		wantCount int
	}{
		{
			name:      "returns empty list",
			wantCount: 0,
		},
		{
			name:      "returns seeded rules",
			ruleNames: []string{"rule-a", "rule-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, ruleName := range tt.ruleNames {
				rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
					"SamplingRule": map[string]any{"RuleName": ruleName, "FixedRate": 0.05, "Priority": 1},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			records, ok := resp["SamplingRuleRecords"].([]any)
			require.True(t, ok)
			assert.Len(t, records, tt.wantCount)
		})
	}
}

func TestHandler_UpdateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates existing rule",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateSamplingRule(
					xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1},
				)
			},
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{"RuleName": "my-rule", "ServiceName": "updated-svc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing RuleName returns 400",
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found returns 400",
			body: map[string]any{
				"SamplingRuleUpdate": map[string]any{"RuleName": "missing-rule"},
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

			rec := doXrayRequest(t, h, "/UpdateSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing rule",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateSamplingRule(
					xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1},
				)
			},
			body:       map[string]any{"RuleName": "my-rule"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing RuleName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"RuleName": "missing-rule"},
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

			rec := doXrayRequest(t, h, "/DeleteSamplingRule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetTraces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*xray.Handler)
		body        map[string]any
		name        string
		wantStatus  int
		wantTraces  int
		wantMissing int
	}{
		{
			name: "returns known trace",
			setup: func(h *xray.Handler) {
				_ = h.Backend.PutTraceSegments([]string{`{"trace_id":"1-abc123","id":"s1","name":"test"}`})
			},
			body:        map[string]any{"TraceIds": []string{"1-abc123"}},
			wantStatus:  http.StatusOK,
			wantTraces:  1,
			wantMissing: 0,
		},
		{
			name:        "returns unprocessed for unknown trace",
			body:        map[string]any{"TraceIds": []string{"1-unknown"}},
			wantStatus:  http.StatusOK,
			wantTraces:  0,
			wantMissing: 1,
		},
		{
			name:       "empty trace IDs",
			body:       map[string]any{"TraceIds": []string{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/Traces", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantTraces > 0 {
				traces, ok := resp["Traces"].([]any)
				require.True(t, ok)
				assert.Len(t, traces, tt.wantTraces)
			}

			if tt.wantMissing > 0 {
				unprocessed, ok := resp["UnprocessedTraceIds"].([]any)
				require.True(t, ok)
				assert.Len(t, unprocessed, tt.wantMissing)
			}
		})
	}
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "xray", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		path   string
		name   string
		wantOp string
	}{
		{name: "CreateGroup path", path: "/CreateGroup", wantOp: "CreateGroup"},
		{name: "Groups path", path: "/Groups", wantOp: "GetGroups"},
		{name: "TraceSegments path", path: "/TraceSegments", wantOp: "PutTraceSegments"},
		{name: "unknown path returns Unknown", path: "/Unknown", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		body    map[string]any
		name    string
		wantRes string
	}{
		{
			name:    "extracts GroupName",
			body:    map[string]any{"GroupName": "my-group"},
			wantRes: "my-group",
		},
		{
			name:    "extracts RuleName when no GroupName",
			body:    map[string]any{"RuleName": "my-rule"},
			wantRes: "my-rule",
		},
		{
			name:    "returns empty for no resource",
			body:    map[string]any{},
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/CreateGroup", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestHandler_PutTraceSegments_Unprocessed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{"not-valid-json"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedTraceSegments"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 1)
}
