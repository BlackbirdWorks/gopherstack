package mwaa_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_InvokeRestApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:    "invoke_get",
			envName: "invoke-rest-api-env",
			seed:    true,
			body: map[string]any{
				"Method": "GET",
				"Path":   "/dags",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "invoke_post_with_body",
			envName: "invoke-rest-api-env-post",
			seed:    true,
			body: map[string]any{
				"Method": "POST",
				"Path":   "/dags/run",
				"Body":   map[string]any{"dag_run_id": "run-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "env_not_found",
			envName: "nonexistent-env",
			seed:    false,
			body: map[string]any{
				"Method": "GET",
				"Path":   "/dags",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_method",
			envName:    "missing-method-env",
			seed:       true,
			body:       map[string]any{"Path": "/dags"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_path",
			envName:    "missing-path-env",
			seed:       true,
			body:       map[string]any{"Method": "GET"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			envName:    "invalid-json-env",
			seed:       true,
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "method_not_allowed",
			envName:    "method-not-allowed-env",
			seed:       false,
			body:       nil,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.seed {
				seedRec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
				require.Equal(t, http.StatusOK, seedRec.Code)
			}

			method := http.MethodPost
			if tt.name == "method_not_allowed" {
				method = http.MethodGet
			}

			var body any
			if tt.name == "invalid_json" {
				// Send raw invalid JSON via the recorder directly.
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/restapi/"+tt.envName, strings.NewReader("{invalid"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			body = tt.body

			rec := doMWAARequest(t, h, method, "/restapi/"+tt.envName, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.InDelta(t, float64(200), resp["RestApiStatusCode"], 0)
			}
		})
	}
}

func TestInvokeRestApi_HTTP_Variations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "list_dags",
			body:       map[string]any{"Method": "GET", "Path": "/dags"},
			wantStatus: http.StatusOK,
		},
		{
			name: "trigger_dag_run",
			body: map[string]any{
				"Method": "POST",
				"Path":   "/dags/my_dag/dagRuns",
				"Body":   map[string]any{"dag_run_id": "run-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with_query_params",
			body: map[string]any{
				"Method":          "GET",
				"Path":            "/dags",
				"QueryParameters": map[string]any{"limit": "5"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_method",
			body:       map[string]any{"Path": "/dags"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			doMWAARequest(t, h, http.MethodPut, "/environments/http-restapi-env", map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})

			rec := doMWAARequest(t, h, http.MethodPost, "/restapi/http-restapi-env", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
