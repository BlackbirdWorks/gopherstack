package emrserverless_test

import (
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

// startJobRunWithMode starts a job run via the handler with optional mode.
func startJobRunWithMode(t *testing.T, h *emrserverless.Handler, appID, mode string) string {
	t.Helper()

	body := map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
	}

	if mode != "" {
		body["mode"] = mode
	}

	rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/applications/%s/jobruns", appID), body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	id := out["jobRunId"]
	require.NotEmpty(t, id)

	return id
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

// TestHandler_StartJobRun_DefaultsModeToBATCH verifies omitting mode yields
// mode="BATCH" in GetJobRun, matching real AWS EMR Serverless behavior.
func TestHandler_StartJobRun_DefaultsModeToBATCH(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mode         string
		expectedMode string
	}{
		{name: "no_mode_field", mode: "", expectedMode: "BATCH"},
		{name: "explicit_streaming", mode: "STREAMING", expectedMode: "STREAMING"},
		{name: "explicit_batch", mode: "BATCH", expectedMode: "BATCH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createAppWithArch(t, h, "mode-test-app", "emr-6.9.0", "")
			jrID := startJobRunWithMode(t, h, appID, tt.mode)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/applications/%s/jobruns/%s", appID, jrID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jr, _ := out["jobRun"].(map[string]any)
			assert.Equal(t, tt.expectedMode, jr["mode"])
		})
	}
}

func TestHandler_StartJobRun_RequiresExecutionRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "exec-role-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"name": "no-role-run",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "ValidationException", out["code"])
}

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

// TestHandler_JobRunToMap_WireShape verifies GetJobRun/ListJobRuns emit the
// real AWS response field names -- in particular "executionRole" (NOT
// "executionRoleArn", which is only the *request*-body field name on
// StartJobRunInput; confirmed against
// awsRestjson1_deserializeDocumentJobRun/JobRunSummary in the SDK's
// deserializers.go) -- plus the required "createdBy" field and the
// "executionTimeoutMinutes" field (defaulted to 720 when StartJobRun didn't
// specify one, matching the real API's documented default).
func TestHandler_JobRunToMap_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "wire-shape-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/wire-role",
		"name":             "wire-shape-run",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var started map[string]string
	mustUnmarshal(t, rec, &started)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+started["jobRunId"], nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	mustUnmarshal(t, getRec, &out)
	jr := out["jobRun"].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/wire-role", jr["executionRole"])
	_, hasWrongKey := jr["executionRoleArn"]
	assert.False(t, hasWrongKey, "jobRun response must not use the request-only 'executionRoleArn' key")
	assert.Equal(t, "arn:aws:iam::000000000000:role/wire-role", jr["createdBy"])
	assert.InDelta(t, float64(720), jr["executionTimeoutMinutes"], 0)

	// ListJobRuns (JobRunSummary) uses the same field names.
	listRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var list map[string]any
	mustUnmarshal(t, listRec, &list)
	runs := list["jobRuns"].([]any)
	require.Len(t, runs, 1)
	summary := runs[0].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/wire-role", summary["executionRole"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/wire-role", summary["createdBy"])
}

// TestHandler_StartJobRun_ExecutionTimeoutRetryPolicyPassthrough verifies
// executionIamPolicy, executionTimeoutMinutes, and retryPolicy (all real
// StartJobRunInput fields per the SDK) round-trip through GetJobRun instead
// of being silently dropped.
func TestHandler_StartJobRun_ExecutionTimeoutRetryPolicyPassthrough(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "timeout-retry-app")

	execPolicy := map[string]any{"policy": `{"Version":"2012-10-17","Statement":[]}`}
	retryPolicy := map[string]any{"maxAttempts": float64(3)}

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn":        "arn:aws:iam::000000000000:role/r",
		"name":                    "timeout-retry-run",
		"executionTimeoutMinutes": float64(60),
		"executionIamPolicy":      execPolicy,
		"retryPolicy":             retryPolicy,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var started map[string]string
	mustUnmarshal(t, rec, &started)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+started["jobRunId"], nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	mustUnmarshal(t, getRec, &out)
	jr := out["jobRun"].(map[string]any)

	assert.InDelta(t, float64(60), jr["executionTimeoutMinutes"], 0)
	assert.Equal(t, execPolicy, jr["executionIamPolicy"])
	assert.Equal(t, retryPolicy, jr["retryPolicy"])
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

// TestHandler_GetJobRun_HasAttemptAndReleaseLabel verifies GetJobRun response
// includes attempt=0 and releaseLabel inherited from the application.
func TestHandler_GetJobRun_HasAttemptAndReleaseLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		releaseLabel string
	}{
		{name: "spark_6_9", releaseLabel: "emr-6.9.0"},
		{name: "spark_7_0", releaseLabel: "emr-7.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createAppWithArch(t, h, "jr-rl-app", tt.releaseLabel, "")
			jrID := startJobRunWithMode(t, h, appID, "")

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/applications/%s/jobruns/%s", appID, jrID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jr, _ := out["jobRun"].(map[string]any)

			attempt, hasAttempt := jr["attempt"]
			assert.True(t, hasAttempt, "attempt field must be present")
			assert.EqualValues(t, 0, attempt)
			assert.Equal(t, tt.releaseLabel, jr["releaseLabel"])
		})
	}
}

// TestHandler_JobRunToMap_AlwaysIncludesTags verifies jobRunToMap always
// includes the "tags" key, even when no tags were set.
func TestHandler_JobRunToMap_AlwaysIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "jr-map-tags-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jr := out["jobRun"].(map[string]any)
	_, hasTags := jr["tags"]
	assert.True(t, hasTags, "jobRun response should always include 'tags' key")
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

// TestHandler_CancelJobRun_AllowsRunning verifies a running job run can be cancelled.
func TestHandler_CancelJobRun_AllowsRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "cancel-running-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, appID, out["applicationId"])
	assert.Equal(t, jobRunID, out["jobRunId"])
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
