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

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "creates application",
			input:      map[string]any{"ApplicationName": "my-app"},
			wantStatus: http.StatusOK,
			wantKey:    "ApplicationSummary",
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", tt.input)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "describes existing application",
			appName: "existing-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "existing-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "existing-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "DescribeApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing application",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "del-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "del-app", "CreateTimestamp": 0},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "ghost", "CreateTimestamp": 0},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "DeleteApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "lists all applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "app-1", "", "", nil)
				_, _ = b.CreateApplication(testRegion, testAccountID, "app-2", "", "", nil)
			},
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty list",
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "ListApplications", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, _ := resp["ApplicationSummaries"].([]any)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

func TestHandler_StartStopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "starts application",
			op:   "StartApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "start-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "start-app"},
			wantStatus: http.StatusOK,
		},
		{
			name: "stops application",
			op:   "StopApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "stop-app", "", "", nil)
				_ = b.StartApplication("stop-app")
			},
			input:      map[string]any{"ApplicationName": "stop-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "start not found",
			op:         "StartApplication",
			input:      map[string]any{"ApplicationName": "ghost"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, tt.op, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates application",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "upd-app", "", "SELECT 1", nil)
			},
			input: map[string]any{
				"ApplicationName":             "upd-app",
				"CurrentApplicationVersionId": 1,
				"ApplicationUpdate":           map[string]any{"ApplicationCodeUpdate": "SELECT 2"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "ghost", "CurrentApplicationVersionId": 1},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch returns error",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "UpdateApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*kinesisanalytics.InMemoryBackend) string
		tags       []map[string]string
		tagKeys    []string
		wantStatus int
	}{
		{
			name: "list tags returns tags",
			op:   "ListTagsForResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := b.CreateApplication(
					testRegion,
					testAccountID,
					"tag-app",
					"",
					"",
					map[string]string{"env": "test"},
				)

				return app.ApplicationARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag resource succeeds",
			op:   "TagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := b.CreateApplication(testRegion, testAccountID, "tag2-app", "", "", nil)

				return app.ApplicationARN
			},
			tags:       []map[string]string{{"Key": "new", "Value": "val"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "untag resource succeeds",
			op:   "UntagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := b.CreateApplication(
					testRegion,
					testAccountID,
					"untag-app",
					"",
					"",
					map[string]string{"remove": "me"},
				)

				return app.ApplicationARN
			},
			tagKeys:    []string{"remove"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			resourceARN := tt.setup(b)

			var input map[string]any

			switch tt.op {
			case "ListTagsForResource":
				input = map[string]any{"ResourceARN": resourceARN}
			case "TagResource":
				input = map[string]any{"ResourceARN": resourceARN, "Tags": tt.tags}
			case "UntagResource":
				input = map[string]any{"ResourceARN": resourceARN, "TagKeys": tt.tagKeys}
			}

			rec := doRequest(t, h, tt.op, input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
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

func TestHandler_MissingResourceARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list tags missing ARN",
			op:         "ListTagsForResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag resource missing ARN",
			op:         "TagResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag resource missing ARN",
			op:         "UntagResource",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StopApplication_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopApplication", map[string]any{"ApplicationName": "ghost"})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "initializes with defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesisanalytics.Provider{}
			assert.Equal(t, "KinesisAnalytics", p.Name())
		})
	}
}
