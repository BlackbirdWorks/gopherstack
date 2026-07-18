package kinesisanalyticsv2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestKAV2Handler(t *testing.T) *kinesisanalyticsv2.Handler {
	t.Helper()

	backend := kinesisanalyticsv2.NewInMemoryBackend(testAccountID, testRegion)

	return kinesisanalyticsv2.NewHandler(backend)
}

func doKAV2Request(t *testing.T, h *kinesisanalyticsv2.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)

		return doRawKAV2Request(t, h, op, bodyBytes)
	}

	return doRawKAV2Request(t, h, op, nil)
}

// doRawKAV2Request dispatches op with a raw (possibly non-JSON) request
// body, for exercising malformed-input error paths.
func doRawKAV2Request(
	t *testing.T, h *kinesisanalyticsv2.Handler, op string, rawBody []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if rawBody != nil {
		req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(rawBody))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	}

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "KinesisAnalytics_20180523."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// createKAV2App creates a Flink application named name via the HTTP handler.
// Shared helper for tests that only need a pre-existing application and don't
// care about its exact configuration.
func createKAV2App(t *testing.T, h *kinesisanalyticsv2.Handler, name string) {
	t.Helper()

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":          name,
		"RuntimeEnvironment":       "FLINK-1_15",
		"ServiceExecutionRole":     "arn:aws:iam::123456789012:role/KinesisRole",
		"ApplicationConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestKAV2_Name(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Equal(t, "KinesisAnalyticsV2", h.Name())
}

func TestKAV2_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "DescribeApplication")
	assert.Contains(t, ops, "ListApplications")
	assert.Contains(t, ops, "DeleteApplication")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

// TestKAV2_GetSupportedOperations_MatchesOpsLen verifies that
// GetSupportedOperations() reports exactly as many operations as are
// pre-built in the handler's internal dispatch map, so the two never drift.
func TestKAV2_GetSupportedOperations_MatchesOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	ops := h.GetSupportedOperations()
	assert.Len(t, ops, kinesisanalyticsv2.HandlerOpsLen(h))
}

func TestKAV2_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Equal(t, "kinesisanalyticsv2", h.ChaosServiceName())
}

func TestKAV2_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		matches bool
	}{
		{
			name:    "matching target",
			target:  "KinesisAnalytics_20180523.CreateApplication",
			matches: true,
		},
		{
			name:    "matching list target",
			target:  "KinesisAnalytics_20180523.ListApplications",
			matches: true,
		},
		{
			name:    "non-matching target",
			target:  "AWSIdentityStore.CreateUser",
			matches: false,
		},
		{
			name:    "empty target",
			target:  "",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}

func TestKAV2_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	rec := doKAV2Request(t, h, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	// No X-Amz-Target header
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "DeleteApplication_not_found",
			op:         "DeleteApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StartApplication_not_found",
			op:         "StartApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StopApplication_not_found",
			op:         "StopApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteApplicationSnapshot_not_found",
			op:         "DeleteApplicationSnapshot",
			body:       map[string]any{"ApplicationName": "no-app", "SnapshotName": "no-snap"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "TagResource_not_found",
			op:   "TagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/no-app",
				"Tags":        []map[string]string{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UntagResource_not_found",
			op:   "UntagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/no-app",
				"TagKeys":     []string{"k"},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			rec := doKAV2Request(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Reset verifies Handler.Reset delegates to the backend.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := kinesisanalyticsv2.NewInMemoryBackend(testAccountID, testRegion)
	h := kinesisanalyticsv2.NewHandler(b)

	_, err := b.CreateApplication(ctx, "reset-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	h.Reset()

	assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
}

// TestHandler_DispatchMapPreBuilt verifies the handler's internal dispatch
// map is built (non-empty) at construction time rather than lazily.
func TestHandler_DispatchMapPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Positive(t, kinesisanalyticsv2.HandlerOpsLen(h))
}
