package xray_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

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
		// GetInsight, GetInsightEvents, GetInsightImpactGraph, GetInsightSummaries,
		// GetSamplingStatisticSummaries, and GetSamplingTargets are the six
		// operations whose "Get..." names do NOT match their REST paths (verified
		// against aws-sdk-go-v2/service/xray serializers.go). A real SDK client
		// sends these exact paths; the "/Get..." spellings are never sent on the
		// wire and must NOT match.
		{
			name: "matches GetInsight POST at real SDK path", method: http.MethodPost,
			path: "/Insight", want: true,
		},
		{
			name: "rejects GetInsight at its op-name-shaped path", method: http.MethodPost,
			path: "/GetInsight", want: false,
		},
		{
			name: "matches GetInsightEvents POST at real SDK path", method: http.MethodPost,
			path: "/InsightEvents", want: true,
		},
		{
			name: "rejects GetInsightEvents at its op-name-shaped path", method: http.MethodPost,
			path: "/GetInsightEvents", want: false,
		},
		{
			name: "matches GetInsightImpactGraph POST at real SDK path", method: http.MethodPost,
			path: "/InsightImpactGraph", want: true,
		},
		{
			name: "rejects GetInsightImpactGraph at its op-name-shaped path", method: http.MethodPost,
			path: "/GetInsightImpactGraph", want: false,
		},
		{
			name: "matches GetInsightSummaries POST at real SDK path", method: http.MethodPost,
			path: "/InsightSummaries", want: true,
		},
		{
			name: "rejects GetInsightSummaries at its op-name-shaped path", method: http.MethodPost,
			path: "/GetInsightSummaries", want: false,
		},
		{
			name: "matches GetSamplingStatisticSummaries POST at real SDK path", method: http.MethodPost,
			path: "/SamplingStatisticSummaries", want: true,
		},
		{
			name: "rejects GetSamplingStatisticSummaries at its op-name-shaped path", method: http.MethodPost,
			path: "/GetSamplingStatisticSummaries", want: false,
		},
		{
			name: "matches GetSamplingTargets POST at real SDK path", method: http.MethodPost,
			path: "/SamplingTargets", want: true,
		},
		{
			name: "rejects GetSamplingTargets at its op-name-shaped path", method: http.MethodPost,
			path: "/GetSamplingTargets", want: false,
		},
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

func TestXRay_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createGroups int
		wantAfter    int
	}{
		{
			name:         "reset clears all groups",
			createGroups: 2,
			wantAfter:    0,
		},
		{
			name:         "reset on empty backend is a no-op",
			createGroups: 0,
			wantAfter:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createGroups {
				rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
					"GroupName":        fmt.Sprintf("group-%d", i),
					"FilterExpression": "service(id(name: \"test\"))",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			rec := doXrayRequest(t, h, "/Groups", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Groups []any `json:"Groups"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Groups, tt.wantAfter)
		})
	}
}

// TestHandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 38)
}

// TestSDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestHandlerBackendIsInterface verifies Handler.Backend is StorageBackend.
func TestHandlerBackendIsInterface(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)

	// Handler.Backend must be assignable to the interface.
	_ = h.Backend
}

// TestHandlerOpsLenHelper verifies the HandlerOpsLen export helper.
func TestHandlerOpsLenHelper(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)
	assert.Equal(t, 38, h.HandlerOpsLen())
}
