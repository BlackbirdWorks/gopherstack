package kinesisanalytics_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newBackend() *kinesisanalytics.InMemoryBackend {
	return kinesisanalytics.NewInMemoryBackend(testRegion, testAccountID)
}

func newTestHandlerWithBackend(t *testing.T) (*kinesisanalytics.Handler, *kinesisanalytics.InMemoryBackend) {
	t.Helper()

	backend := kinesisanalytics.NewInMemoryBackend(testRegion, testAccountID)
	h := kinesisanalytics.NewHandler(backend)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, backend
}

func newTestHandler(t *testing.T) *kinesisanalytics.Handler {
	t.Helper()

	h, _ := newTestHandlerWithBackend(t)

	return h
}

func doRequest(t *testing.T, h *kinesisanalytics.Handler, action string, body any) *httptest.ResponseRecorder {
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
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "KinesisAnalytics_20150814."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches kinesis analytics target",
			target:    "KinesisAnalytics_20150814.CreateApplication",
			wantMatch: true,
		},
		{
			name:      "does not match other targets",
			target:    "Firehose_20150804.CreateDeliveryStream",
			wantMatch: false,
		},
		{
			name:      "does not match empty target",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)

			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "NonExistentAction", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ServiceMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "KinesisAnalytics", h.Name())
	assert.Equal(t, "kinesisanalytics", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "us-east-1", h.ChaosRegions()[0])
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "extracts operation from target",
			target: "KinesisAnalytics_20150814.CreateApplication",
			wantOp: "CreateApplication",
		},
		{
			name:   "returns unknown for empty target",
			target: "",
			wantOp: "Unknown",
		},
		{
			name:   "returns unknown for non-matching prefix",
			target: "Firehose_20150804.CreateDeliveryStream",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
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

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "extracts application name",
			body:     `{"ApplicationName":"my-app"}`,
			wantName: "my-app",
		},
		{
			name:     "returns empty for missing name",
			body:     `{}`,
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

// TestErrorTypes verifies that all mutating operations return the correct
// AWS Kinesis Analytics error type strings. This exercises handleError's shared
// error-code-mapping switch across several different operations/families.
func TestErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*kinesisanalytics.Handler, *kinesisanalytics.InMemoryBackend)
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:   "create duplicate app returns ResourceInUseException",
			action: "CreateApplication",
			body:   map[string]any{"ApplicationName": "dup-app"},
			setup: func(_ *kinesisanalytics.Handler, b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
			},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceInUseException",
		},
		{
			name:     "describe missing app returns ResourceNotFoundException",
			action:   "DescribeApplication",
			body:     map[string]any{"ApplicationName": "no-such-app"},
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
		{
			name:   "update with wrong version returns ConcurrentModificationException",
			action: "UpdateApplication",
			body: map[string]any{
				"ApplicationName":             "version-app",
				"CurrentApplicationVersionID": 999,
				"ApplicationUpdate":           map[string]any{"ApplicationCodeUpdate": "x"},
			},
			setup: func(_ *kinesisanalytics.Handler, b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "version-app", "", "", nil)
			},
			wantCode: http.StatusBadRequest,
			wantType: "ConcurrentModificationException",
		},
		{
			name:     "create missing name returns InvalidArgumentException",
			action:   "CreateApplication",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(h, b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}

// TestReset verifies that Reset clears all applications and resets the ID counter.
func TestReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*kinesisanalytics.InMemoryBackend)
		name  string
	}{
		{
			name:  "empty backend",
			setup: func(_ *kinesisanalytics.InMemoryBackend) {},
		},
		{
			name: "with applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-1", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-2", "", "", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)
			b.Reset()

			count := kinesisanalytics.ApplicationCount(b)
			assert.Zero(t, count)

			// After reset, creating an application should work (maps are reinitialized).
			_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "post-reset", "", "", nil)
			require.NoError(t, err)
		})
	}
}

// TestMultipleResetCycle verifies repeated Reset calls leave the backend clean.
func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := newBackend()

	for range 3 {
		_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "temp", "", "", nil)
		b.Reset()
		assert.Zero(t, kinesisanalytics.ApplicationCount(b))
	}
}

// TestHandlerReset verifies Handler.Reset delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "reset-app", "", "", nil)

	h.Reset()

	assert.Zero(t, kinesisanalytics.ApplicationCount(b))
}

// TestHandlerOpsPreBuilt verifies that the dispatch table is pre-built.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	count := kinesisanalytics.HandlerOpsLen(h)
	// 20 total operations
	assert.Equal(t, 20, count)
}

// TestGetSupportedOperations_AllOps verifies all 20 ops are listed.
func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeApplication",
		"ListApplications",
		"StartApplication",
		"StopApplication",
		"UpdateApplication",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationReferenceDataSource",
		"DiscoverInputSchema",
	}

	assert.ElementsMatch(t, expected, ops)
}

// TestHandlerSnapshotRestore verifies Handler.Snapshot/Restore delegates correctly.
func TestHandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "hs-app", "", "", nil)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2, b2 := newTestHandlerWithBackend(t)
	require.NoError(t, h2.Restore(t.Context(), snap))
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b2))
}
