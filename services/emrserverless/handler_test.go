package emrserverless_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

func newTestHandler(t *testing.T) *emrserverless.Handler {
	t.Helper()

	return emrserverless.NewHandler(emrserverless.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *emrserverless.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(b)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// createApp is a test helper that creates an application and returns its ID.
func createApp(t *testing.T, h *emrserverless.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":         name,
		"type":         "SPARK",
		"releaseLabel": "emr-6.6.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	require.NotEmpty(t, out["applicationId"])

	return out["applicationId"]
}

// --- CreateApplication ---

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(h *emrserverless.Handler)
		name       string
		wantName   string
		rawBody    string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":         "my-app",
				"type":         "SPARK",
				"releaseLabel": "emr-6.6.0",
				"tags":         map[string]string{"env": "test"},
			},
			wantStatus: http.StatusOK,
			wantName:   "my-app",
		},
		{
			name: "duplicate_name",
			body: map[string]any{
				"name":         "my-app",
				"type":         "SPARK",
				"releaseLabel": "emr-6.6.0",
			},
			wantStatus: http.StatusConflict,
			setup: func(h *emrserverless.Handler) {
				createApp(t, h, "my-app")
			},
		},
		{
			name:       "invalid_body",
			rawBody:    "not-json",
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

			var rec *httptest.ResponseRecorder
			if tt.rawBody != "" {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/applications", strings.NewReader(tt.rawBody))
				req.Header.Set("Content-Type", "application/json")
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			} else {
				rec = doRequest(t, h, http.MethodPost, "/applications", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, tt.wantName, out["name"])
				assert.NotEmpty(t, out["applicationId"])
				assert.NotEmpty(t, out["arn"])
			}
		})
	}
}

// --- GetApplication ---

func TestHandler_GetApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) string
		name       string
		appID      string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "get-app")
			},
		},
		{
			name:       "not_found",
			appID:      "nonexistentid",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)
				assert.Equal(t, appID, app["applicationId"])
			}
		})
	}
}

// --- ListApplications ---

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(h *emrserverless.Handler)
		name        string
		queryString string
		wantCount   int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "two_apps",
			setup: func(h *emrserverless.Handler) {
				createApp(t, h, "app1")
				createApp(t, h, "app2")
			},
			wantCount: 2,
		},
		{
			name: "states_filter_started",
			setup: func(h *emrserverless.Handler) {
				id := createApp(t, h, "started-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				createApp(t, h, "stopped-app")
			},
			queryString: "?states=STARTED",
			wantCount:   1,
		},
		{
			name: "states_filter_multiple",
			setup: func(h *emrserverless.Handler) {
				id := createApp(t, h, "started-app2")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				createApp(t, h, "created-app2")
			},
			queryString: "?states=STARTED,CREATED",
			wantCount:   2,
		},
		{
			name: "states_filter_no_match",
			setup: func(h *emrserverless.Handler) {
				createApp(t, h, "only-creating")
			},
			queryString: "?states=STARTED",
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, "/applications"+tt.queryString, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			apps := out["applications"].([]any)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

// --- UpdateApplication ---

func TestHandler_ListApplicationsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryString   string
		wantCount     int
		wantStatus    int
		wantNextToken bool
	}{
		{
			name:        "no_pagination_returns_all",
			queryString: "",
			wantCount:   4,
			wantStatus:  http.StatusOK,
		},
		{
			name:          "first_page",
			queryString:   "?maxResults=2",
			wantCount:     2,
			wantStatus:    http.StatusOK,
			wantNextToken: true,
		},
		{
			name:        "second_page",
			queryString: "?maxResults=2&nextToken=2",
			wantCount:   2,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "token_beyond_end",
			queryString: "?maxResults=2&nextToken=100",
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "invalid_max_results_rejected",
			queryString: "?maxResults=notanumber",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "max_results_over_bound_rejected",
			queryString: "?maxResults=51",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range 4 {
				createApp(t, h, fmt.Sprintf("app-%d", i))
			}

			rec := doRequest(t, h, http.MethodGet, "/applications"+tt.queryString, nil)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			apps := out["applications"].([]any)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, out["nextToken"])
			}
		})
	}
}

func TestHandler_ListJobRunsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryString   string
		wantCount     int
		wantNextToken bool
	}{
		{
			name:        "no_pagination_returns_all",
			queryString: "",
			wantCount:   4,
		},
		{
			name:          "first_page",
			queryString:   "?maxResults=2",
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:        "second_page",
			queryString: "?maxResults=2&nextToken=2",
			wantCount:   2,
		},
		{
			name:        "token_beyond_end",
			queryString: "?maxResults=2&nextToken=100",
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createApp(t, h, "pagination-app")

			jobBody := map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"}
			for range 4 {
				rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", jobBody)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns"+tt.queryString, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			runs := out["jobRuns"].([]any)
			assert.Len(t, runs, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, out["nextToken"])
			}
		})
	}
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		setup       func(h *emrserverless.Handler) string
		name        string
		appID       string
		wantRelease string
		wantStatus  int
	}{
		{
			name:        "success",
			body:        map[string]any{"releaseLabel": "emr-7.0.0"},
			wantStatus:  http.StatusOK,
			wantRelease: "emr-7.0.0",
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "update-app")
			},
		},
		{
			name:       "not_found",
			appID:      "nonexistentid",
			body:       map[string]any{"releaseLabel": "emr-7.0.0"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPatch, "/applications/"+appID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRelease != "" {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)
				assert.Equal(t, tt.wantRelease, app["releaseLabel"])
			}
		})
	}
}

// --- DeleteApplication ---

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) string
		name       string
		appID      string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "delete-app")
			},
		},
		{
			name:       "not_found",
			appID:      "nonexistentid",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodDelete, "/applications/"+appID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				// Verify deletion.
				rec2 := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			}
		})
	}
}

// --- StartApplication ---

func TestHandler_StartApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) string
		name       string
		appID      string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "start-app")
			},
		},
		{
			name:       "not_found",
			appID:      "nonexistentid",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "already_started",
			wantStatus: http.StatusBadRequest,
			setup: func(h *emrserverless.Handler) string {
				id := createApp(t, h, "already-started-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				return id
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/start", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec2, &out)
				app := out["application"].(map[string]any)
				assert.Equal(t, emrserverless.ApplicationStateStarted, app["state"])
			}
		})
	}
}

// --- StopApplication ---

func TestHandler_StopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) string
		name       string
		appID      string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				id := createApp(t, h, "stop-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				return id
			},
		},
		{
			name:       "not_found",
			appID:      "nonexistentid",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "already_stopped",
			wantStatus: http.StatusBadRequest,
			setup: func(h *emrserverless.Handler) string {
				// Application starts in CREATING state, which is not STARTED;
				// stopping it should be rejected as invalid state transition.
				id := createApp(t, h, "already-stopped-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doRequest(t, h, http.MethodPost, "/applications/"+id+"/stop", nil)
				require.Equal(t, http.StatusOK, rec2.Code)

				return id
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/stop", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec2, &out)
				app := out["application"].(map[string]any)
				assert.Equal(t, emrserverless.ApplicationStateStopped, app["state"])
			}
		})
	}
}

// --- StartJobRun ---

func TestHandler_StartJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(h *emrserverless.Handler) string
		name       string
		appID      string
		rawBody    string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"executionRoleArn": "arn:aws:iam::000000000000:role/my-role",
				"name":             "my-job",
				"tags":             map[string]string{"job": "1"},
			},
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				id := createApp(t, h, "job-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+id+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				return id
			},
		},
		{
			name:       "app_not_found",
			appID:      "nonexistentid",
			body:       map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			rawBody:    "not-json",
			wantStatus: http.StatusBadRequest,
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "job-app-invalid")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.appID
			if tt.setup != nil {
				appID = tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			if tt.rawBody != "" {
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPost,
					"/applications/"+appID+"/jobruns",
					strings.NewReader(tt.rawBody),
				)
				req.Header.Set("Content-Type", "application/json")
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			} else {
				rec = doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["jobRunId"])
				assert.NotEmpty(t, out["arn"])
				assert.Equal(t, appID, out["applicationId"])
			}
		})
	}
}

// --- GetJobRun ---

func TestHandler_GetJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (appID, jobRunID string)
		name       string
		jobRunID   string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "get-jr-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
					"executionRoleArn": "arn:aws:iam::000000000000:role/r",
					"name":             "my-jr",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return appID, out["jobRunId"]
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "nonexistent", "nonexistent"
			},
		},
		{
			name:       "jobrun_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "get-jr-app2")

				return appID, "nonexistentjr"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID, jobRunID := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				jr := out["jobRun"].(map[string]any)
				assert.Equal(t, jobRunID, jr["jobRunId"])
			}
		})
	}
}

// --- ListJobRuns ---

func TestHandler_ListJobRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(h *emrserverless.Handler) string
		name        string
		queryString string
		wantStatus  int
		wantCount   int
	}{
		{
			name:       "empty",
			wantStatus: http.StatusOK,
			wantCount:  0,
			setup: func(h *emrserverless.Handler) string {
				return createApp(t, h, "list-jr-empty")
			},
		},
		{
			name:       "two_job_runs",
			wantStatus: http.StatusOK,
			wantCount:  2,
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "list-jr-app")
				body := map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"}
				rec1 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
				require.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
				require.Equal(t, http.StatusOK, rec2.Code)

				return appID
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) string {
				return "nonexistentid"
			},
		},
		{
			name:        "states_filter_submitted",
			wantStatus:  http.StatusOK,
			wantCount:   1,
			queryString: "?states=SUBMITTED",
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "states-filter-app")
				body := map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"}
				rec1 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
				require.Equal(t, http.StatusOK, rec1.Code)
				var out map[string]string
				mustUnmarshal(t, rec1, &out)
				cancelRec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+out["jobRunId"], nil)
				require.Equal(t, http.StatusOK, cancelRec.Code)
				// Add a second SUBMITTED run
				rec2 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
				require.Equal(t, http.StatusOK, rec2.Code)

				return appID
			},
		},
		{
			name:        "states_filter_cancelled",
			wantStatus:  http.StatusOK,
			wantCount:   1,
			queryString: "?states=CANCELLED",
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "states-filter-cancelled")
				body := map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"}
				rec1 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
				require.Equal(t, http.StatusOK, rec1.Code)
				var out map[string]string
				mustUnmarshal(t, rec1, &out)
				cancelRec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+out["jobRunId"], nil)
				require.Equal(t, http.StatusOK, cancelRec.Code)
				// Add a second non-cancelled run
				doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)

				return appID
			},
		},
		{
			name:        "states_filter_no_match",
			wantStatus:  http.StatusOK,
			wantCount:   0,
			queryString: "?states=SUCCESS",
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "states-filter-nomatch")
				body := map[string]any{"executionRoleArn": "arn:aws:iam::000000000000:role/r"}
				doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)

				return appID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns"+tt.queryString, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				runs := out["jobRuns"].([]any)
				assert.Len(t, runs, tt.wantCount)
			}
		})
	}
}

// --- CancelJobRun ---

func TestHandler_CancelJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (appID, jobRunID string)
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "cancel-jr-app")
				rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
					"executionRoleArn": "arn:aws:iam::000000000000:role/r",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return appID, out["jobRunId"]
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "nonexistent", "nonexistent"
			},
		},
		{
			name:       "jobrun_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "cancel-jr-app2")

				return appID, "nonexistentjr"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID, jobRunID := tt.setup(h)

			rec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, appID, out["applicationId"])
				assert.Equal(t, jobRunID, out["jobRunId"])
			}
		})
	}
}

// --- Tags ---

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		setup      func(h *emrserverless.Handler) string
		name       string
		wantStatus int
	}{
		{
			name:       "success_with_tags",
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "test"},
			setup: func(h *emrserverless.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
					"name":         "tagged-app",
					"type":         "SPARK",
					"releaseLabel": "emr-6.6.0",
					"tags":         map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["arn"]
			},
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) string {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				tags := out["tags"].(map[string]any)
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tags[k])
				}
			}
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		setup      func(h *emrserverless.Handler) string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			tags:       map[string]string{"new-key": "new-val"},
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "tag-app")
				rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)

				return app["arn"].(string)
			},
		},
		{
			name:       "not_found",
			tags:       map[string]string{"k": "v"},
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) string {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent"
			},
		},
		{
			name:       "invalid_body",
			wantStatus: http.StatusBadRequest,
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "tag-app-invalid")
				rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)

				return app["arn"].(string)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			var rec *httptest.ResponseRecorder
			if tt.name == "invalid_body" {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/tags/"+resourceARN, strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			} else {
				rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{"tags": tt.tags})
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (arn, query string)
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) (string, string) {
				rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
					"name":         "untag-app",
					"type":         "SPARK",
					"releaseLabel": "emr-6.6.0",
					"tags":         map[string]string{"remove-me": "val"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["arn"], "?tagKeys=remove-me"
			},
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent", "?tagKeys=k"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN, query := tt.setup(h)

			rec := doRequest(t, h, http.MethodDelete, "/tags/"+resourceARN+query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Routing and meta ---

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	const emrAuth = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/emr-serverless/aws4_request," +
		" SignedHeaders=host, Signature=abc"
	const appConfigAuth = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/appconfig/aws4_request," +
		" SignedHeaders=host, Signature=abc"

	tests := []struct {
		name      string
		path      string
		authHdr   string
		wantMatch bool
	}{
		{
			name:      "applications_exact_with_emr_auth",
			path:      "/applications",
			authHdr:   emrAuth,
			wantMatch: true,
		},
		{
			name:      "applications_with_id_with_emr_auth",
			path:      "/applications/abc123",
			authHdr:   emrAuth,
			wantMatch: true,
		},
		{
			name:      "applications_with_appconfig_auth_no_match",
			path:      "/applications",
			authHdr:   appConfigAuth,
			wantMatch: false,
		},
		{
			name:      "applications_no_auth_no_match",
			path:      "/applications",
			authHdr:   "",
			wantMatch: false,
		},
		{
			name:      "emr_tags",
			path:      "/tags/arn:aws:emr-serverless:us-east-1:000000000000:/applications/xyz",
			authHdr:   "",
			wantMatch: true,
		},
		{
			name:      "backup_tags_no_match",
			path:      "/tags/arn:aws:backup:us-east-1:000000000000:backup-vault/my-vault",
			authHdr:   "",
			wantMatch: false,
		},
		{
			name:      "other_path",
			path:      "/v1/createcomputeenvironment",
			authHdr:   "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.authHdr != "" {
				req.Header.Set("Authorization", tt.authHdr)
			}
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "CreateApplication", method: http.MethodPost, path: "/applications", wantOp: "CreateApplication"},
		{name: "ListApplications", method: http.MethodGet, path: "/applications", wantOp: "ListApplications"},
		{name: "GetApplication", method: http.MethodGet, path: "/applications/abc", wantOp: "GetApplication"},
		{
			name:   "UpdateApplication",
			method: http.MethodPatch,
			path:   "/applications/abc",
			wantOp: "UpdateApplication",
		},
		{
			name:   "DeleteApplication",
			method: http.MethodDelete,
			path:   "/applications/abc",
			wantOp: "DeleteApplication",
		},
		{
			name:   "StartApplication",
			method: http.MethodPost,
			path:   "/applications/abc/start",
			wantOp: "StartApplication",
		},
		{name: "StopApplication", method: http.MethodPost, path: "/applications/abc/stop", wantOp: "StopApplication"},
		{name: "StartJobRun", method: http.MethodPost, path: "/applications/abc/jobruns", wantOp: "StartJobRun"},
		{name: "ListJobRuns", method: http.MethodGet, path: "/applications/abc/jobruns", wantOp: "ListJobRuns"},
		{name: "GetJobRun", method: http.MethodGet, path: "/applications/abc/jobruns/jr1", wantOp: "GetJobRun"},
		{
			name:   "CancelJobRun",
			method: http.MethodDelete,
			path:   "/applications/abc/jobruns/jr1",
			wantOp: "CancelJobRun",
		},
		{
			name:   "GetDashboardForJobRun",
			method: http.MethodGet,
			path:   "/applications/abc/jobruns/jr1/dashboard",
			wantOp: "GetDashboardForJobRun",
		},
		{
			name:   "ListJobRunAttempts",
			method: http.MethodGet,
			path:   "/applications/abc/jobruns/jr1/attempts",
			wantOp: "ListJobRunAttempts",
		},
		{
			name:   "ListTagsForResource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "TagResource",
			method: http.MethodPost,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "TagResource",
		},
		{
			name:   "UntagResource",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "application_id",
			method: http.MethodGet,
			path:   "/applications/abc123",
			want:   "abc123",
		},
		{
			name:   "job_run_id",
			method: http.MethodGet,
			path:   "/applications/abc123/jobruns/jr456",
			want:   "abc123/jr456",
		},
		{
			name:   "tags_arn",
			method: http.MethodGet,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000000000000:/applications/abc",
			want:   "arn:aws:emr-serverless:us-east-1:000000000000:/applications/abc",
		},
		{
			name:   "list_applications",
			method: http.MethodGet,
			path:   "/applications",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_ServiceMeta(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "EmrServerless", h.Name())
	assert.Equal(t, "emr-serverless", h.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())

	ops := h.GetSupportedOperations()
	assert.Len(t, ops, 22)
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "CancelJobRun")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "GetDashboardForJobRun")
	assert.Contains(t, ops, "ListJobRunAttempts")
	assert.Contains(t, ops, "StartSession")
	assert.Contains(t, ops, "GetResourceDashboard")

	chaosOps := h.ChaosOperations()
	assert.Equal(t, ops, chaosOps)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/applications/abc/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagsOnJobRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appID := createApp(t, h, "jr-tags-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
		"name":             "tagged-jr",
		"tags":             map[string]string{"key1": "val1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jrOut map[string]string
	mustUnmarshal(t, rec, &jrOut)
	jrARN := jrOut["arn"]

	// List tags.
	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+jrARN, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagOut map[string]any
	mustUnmarshal(t, rec2, &tagOut)
	tags := tagOut["tags"].(map[string]any)
	assert.Equal(t, "val1", tags["key1"])

	// Add a tag.
	rec3 := doRequest(t, h, http.MethodPost, "/tags/"+jrARN, map[string]any{
		"tags": map[string]string{"key2": "val2"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// Untag.
	rec4 := doRequest(t, h, http.MethodDelete, "/tags/"+jrARN+"?tagKeys=key1", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	// Verify.
	rec5 := doRequest(t, h, http.MethodGet, "/tags/"+jrARN, nil)
	var final map[string]any
	mustUnmarshal(t, rec5, &final)
	finalTags := final["tags"].(map[string]any)
	assert.NotContains(t, finalTags, "key1")
	assert.Equal(t, "val2", finalTags["key2"])
}

// startJobRun is a test helper that starts a job run and returns its ID.
func startJobRun(t *testing.T, h *emrserverless.Handler, appID string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/test-role",
		"name":             "test-job",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	require.NotEmpty(t, out["jobRunId"])

	return out["jobRunId"]
}

// --- GetDashboardForJobRun ---

func TestHandler_GetDashboardForJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (appID, jobRunID string)
		name       string
		wantStatus int
		wantURL    bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantURL:    true,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "dash-app")
				jobRunID := startJobRun(t, h, appID)

				return appID, jobRunID
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "nonexistent-app", "nonexistent-run"
			},
		},
		{
			name:       "job_run_not_found",
			wantStatus: http.StatusNotFound,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "dash-app-2")

				return appID, "nonexistent-run"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID, jobRunID := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet,
				"/applications/"+appID+"/jobruns/"+jobRunID+"/dashboard", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantURL {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				u, ok := out["url"].(string)
				assert.True(t, ok, "url should be a string")
				assert.NotEmpty(t, u, "url should not be empty")
				assert.Contains(t, u, appID)
				assert.Contains(t, u, jobRunID)
			}
		})
	}
}

// --- ListJobRunAttempts ---

func TestHandler_ListJobRunAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (appID, jobRunID string)
		name       string
		query      string
		wantStatus int
		wantCount  int
		wantAppID  bool
	}{
		{
			name:       "success_returns_single_attempt",
			wantStatus: http.StatusOK,
			wantCount:  1,
			wantAppID:  true,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app")
				jobRunID := startJobRun(t, h, appID)

				return appID, jobRunID
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			wantCount:  0,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "nonexistent-app", "nonexistent-run"
			},
		},
		{
			name:       "job_run_not_found",
			wantStatus: http.StatusNotFound,
			wantCount:  0,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app-2")

				return appID, "nonexistent-run"
			},
		},
		{
			name:       "pagination_max_results",
			query:      "?maxResults=1",
			wantStatus: http.StatusOK,
			wantCount:  1,
			wantAppID:  true,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app-3")
				jobRunID := startJobRun(t, h, appID)

				return appID, jobRunID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID, jobRunID := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet,
				"/applications/"+appID+"/jobruns/"+jobRunID+"/attempts"+tt.query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				attempts, ok := out["jobRunAttempts"].([]any)
				require.True(t, ok, "jobRunAttempts should be an array")
				assert.Len(t, attempts, tt.wantCount)

				if tt.wantAppID && len(attempts) > 0 {
					attempt := attempts[0].(map[string]any)
					assert.Equal(t, appID, attempt["applicationId"])
					assert.Equal(t, jobRunID, attempt["id"])
					assert.NotEmpty(t, attempt["state"])
				}
			}
		})
	}
}

// --- CreateApplication / UpdateApplication configuration passthrough ---

// TestHandler_CreateApplication_ConfigPassthrough verifies that application
// configuration sub-objects (maximumCapacity, autoStopConfiguration, etc.)
// supplied on CreateApplication are echoed back verbatim by GetApplication
// instead of being silently discarded.
func TestHandler_CreateApplication_ConfigPassthrough(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	maxCapacity := map[string]any{"cpu": "400 vCPU", "memory": "3000 GB"}
	autoStop := map[string]any{"enabled": true, "idleTimeoutMinutes": float64(20)}

	rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":                  "config-passthrough-app",
		"type":                  "SPARK",
		"releaseLabel":          "emr-6.6.0",
		"maximumCapacity":       maxCapacity,
		"autoStopConfiguration": autoStop,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	mustUnmarshal(t, rec, &created)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+created["applicationId"], nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	mustUnmarshal(t, getRec, &out)
	app := out["application"].(map[string]any)
	assert.Equal(t, maxCapacity, app["maximumCapacity"])
	assert.Equal(t, autoStop, app["autoStopConfiguration"])
}

// TestHandler_UpdateApplication_ConfigMerge verifies that UpdateApplication
// merges newly supplied configuration keys with previously stored ones
// rather than replacing the whole configuration (matching AWS's per-field
// PATCH semantics), and that fields not present in the request body are left
// untouched.
func TestHandler_UpdateApplication_ConfigMerge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	maxCapacity := map[string]any{"cpu": "200 vCPU", "memory": "1000 GB"}
	rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":            "config-merge-app",
		"type":            "SPARK",
		"releaseLabel":    "emr-6.6.0",
		"maximumCapacity": maxCapacity,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	mustUnmarshal(t, rec, &created)
	appID := created["applicationId"]

	autoStop := map[string]any{"enabled": true}
	updateRec := doRequest(t, h, http.MethodPatch, "/applications/"+appID, map[string]any{
		"autoStopConfiguration": autoStop,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	mustUnmarshal(t, getRec, &out)
	app := out["application"].(map[string]any)
	assert.Equal(t, maxCapacity, app["maximumCapacity"], "update must not drop previously stored config")
	assert.Equal(t, autoStop, app["autoStopConfiguration"])
}

// TestHandler_CreateApplication_ClientTokenIdempotent verifies that retrying
// CreateApplication with the same clientToken (as an AWS SDK does on a
// timed-out request) returns the already-created application instead of
// erroring with ConflictException.
func TestHandler_CreateApplication_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"name":         "client-token-app",
		"type":         "SPARK",
		"releaseLabel": "emr-6.6.0",
		"clientToken":  "retry-token-1",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	var out1 map[string]string
	mustUnmarshal(t, rec1, &out1)

	rec2 := doRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusOK, rec2.Code, "retry with same clientToken must not conflict")
	var out2 map[string]string
	mustUnmarshal(t, rec2, &out2)

	assert.Equal(t, out1["applicationId"], out2["applicationId"])
}

// --- StartJobRun jobDriver / configurationOverrides passthrough ---

// TestHandler_StartJobRun_JobDriverWireShape verifies that jobDriver
// (a required field on the real GetJobRun/ListJobRuns response shape) is
// stored and echoed back rather than silently dropped, and that
// configurationOverrides round-trips too.
func TestHandler_StartJobRun_JobDriverWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "jobdriver-wire-app")

	jobDriver := map[string]any{
		"sparkSubmit": map[string]any{
			"entryPoint": "s3://bucket/job.py",
		},
	}
	configOverrides := map[string]any{
		"monitoringConfiguration": map[string]any{"s3MonitoringConfiguration": map[string]any{"logUri": "s3://x"}},
	}

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn":       "arn:aws:iam::000000000000:role/r",
		"name":                   "jobdriver-run",
		"jobDriver":              jobDriver,
		"configurationOverrides": configOverrides,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var started map[string]string
	mustUnmarshal(t, rec, &started)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+started["jobRunId"], nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	mustUnmarshal(t, getRec, &out)
	jr := out["jobRun"].(map[string]any)
	assert.Equal(t, jobDriver, jr["jobDriver"])
	assert.Equal(t, configOverrides, jr["configurationOverrides"])
}

// TestHandler_StartJobRun_ClientTokenIdempotent verifies that retrying
// StartJobRun with the same clientToken returns the already-started job run
// instead of creating a duplicate.
func TestHandler_StartJobRun_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "jobrun-token-app")

	body := map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
		"name":             "token-run",
		"clientToken":      "jr-retry-token-1",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	var out1 map[string]string
	mustUnmarshal(t, rec1, &out1)

	rec2 := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", body)
	require.Equal(t, http.StatusOK, rec2.Code)
	var out2 map[string]string
	mustUnmarshal(t, rec2, &out2)

	assert.Equal(t, out1["jobRunId"], out2["jobRunId"])

	listRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var list map[string]any
	mustUnmarshal(t, listRec, &list)
	assert.Len(t, list["jobRuns"].([]any), 1, "retried StartJobRun must not create a duplicate job run")
}
