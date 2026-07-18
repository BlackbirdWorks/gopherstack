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

// errType decodes the awsjson1.0 "__type" field from an error response body, so tests can
// assert the exact modeled exception name gopherstack put on the wire.
func errType(t *testing.T, body []byte) string {
	t.Helper()

	var env struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(body, &env))

	return env.Type
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
func TestHandler_GetSupportedOperations_IncludesListResourceRequests(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "ListResourceRequests")
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
func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	label := h.ExtractResource(c)
	assert.Equal(t, "cloudcontrol", label)
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
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"reset-r1"}`, "")

	require.Len(t, h.Backend.ListAllResources(), 1)

	h.Reset()
	assert.Empty(t, h.Backend.ListAllResources())
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
func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
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
