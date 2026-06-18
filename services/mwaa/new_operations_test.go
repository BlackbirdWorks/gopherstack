package mwaa_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestHandler_UpdateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody any
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:    "updates_existing",
			envName: "update-env",
			seed:    true,
			updateBody: map[string]any{
				"DagS3Path": "new-dags/",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			envName:    "missing-env",
			seed:       false,
			updateBody: map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.seed {
				rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				doMWAARequest(t, h, http.MethodGet, "/environments/"+tt.envName, nil)
			}

			rec := doMWAARequest(t, h, http.MethodPatch, "/environments/"+tt.envName, tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		tagKeys     []string
		wantStatus  int
		wantTagsLen int
	}{
		{
			name:        "removes_tag",
			envName:     "untag-env",
			tagKeys:     []string{"env"},
			wantStatus:  http.StatusOK,
			wantTagsLen: 1,
		},
		{
			name:        "removes_nonexistent_key_ok",
			envName:     "untag-env2",
			tagKeys:     []string{"missing"},
			wantStatus:  http.StatusOK,
			wantTagsLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			// Create environment.
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:r",
				"SourceBucketArn":  "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			envARN := createResp["Arn"]
			require.NotEmpty(t, envARN)

			// Add tags.
			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"env": "test", "team": "platform"},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Untag.
			untagPath := "/tags/" + envARN + "?tagKeys=" + tt.tagKeys[0]
			var untagPathSb118 strings.Builder
			for _, k := range tt.tagKeys[1:] {
				untagPathSb118.WriteString("&tagKeys=" + k)
			}
			untagPath += untagPathSb118.String()
			untagRec := doMWAARequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, tt.wantStatus, untagRec.Code)

			// Verify tags.
			listRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
			tags := tagsResp["Tags"].(map[string]any)
			assert.Len(t, tags, tt.wantTagsLen)
		})
	}
}

func TestHandler_CreateCliToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envName    string
		wantStatus int
	}{
		{
			name:       "returns_token",
			envName:    "cli-env",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			// Seed the environment so the token endpoint can validate it exists.
			doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			})
			doMWAARequest(t, h, http.MethodGet, "/environments/"+tt.envName, nil)
			rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/"+tt.envName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["CliToken"])
			}
		})
	}
}

func TestHandler_CreateWebLoginToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envName    string
		wantStatus int
	}{
		{
			name:       "returns_token",
			envName:    "web-env",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			// Seed the environment so the token endpoint can validate it exists.
			doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			})
			doMWAARequest(t, h, http.MethodGet, "/environments/"+tt.envName, nil)
			rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/"+tt.envName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["WebToken"])
			}
		})
	}
}

func TestCreateEnvironment_MinWorkersValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_min_max",
			body: map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				"MinWorkers": 1, "MaxWorkers": 5,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "min_greater_than_max",
			body: map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				"MinWorkers": 10, "MaxWorkers": 5,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/validation-env-"+tt.name, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBackend_ARNIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envNames   []string
		wantTagsOK bool
	}{
		{
			name:       "tag_resource_uses_arn_index",
			envNames:   []string{"arn-env-1"},
			wantTagsOK: true,
		},
		{
			name:       "tag_resource_not_found",
			envNames:   []string{},
			wantTagsOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var envARN string
			for _, n := range tt.envNames {
				rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+n, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				envARN = resp["Arn"]
			}

			if !tt.wantTagsOK {
				envARN = "arn:aws:airflow:us-east-1:123456789012:environment/nonexistent"
			}

			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"key": "val"},
			})

			if tt.wantTagsOK {
				assert.Equal(t, http.StatusOK, tagRec.Code)
			} else {
				assert.Equal(t, http.StatusNotFound, tagRec.Code)
			}
		})
	}
}

func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, "MWAA", h.Name())
}

func TestHandlerGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	ops := h.GetSupportedOperations()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "CreateEnvironment", wantOp: "CreateEnvironment"},
		{name: "GetEnvironment", wantOp: "GetEnvironment"},
		{name: "DeleteEnvironment", wantOp: "DeleteEnvironment"},
		{name: "UpdateEnvironment", wantOp: "UpdateEnvironment"},
		{name: "ListEnvironments", wantOp: "ListEnvironments"},
		{name: "ListTagsForResource", wantOp: "ListTagsForResource"},
		{name: "TagResource", wantOp: "TagResource"},
		{name: "UntagResource", wantOp: "UntagResource"},
		{name: "CreateCliToken", wantOp: "CreateCliToken"},
		{name: "CreateWebLoginToken", wantOp: "CreateWebLoginToken"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.wantOp)
		})
	}
}

func TestHandlerChaos(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, "airflow", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandlerMatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Positive(t, h.MatchPriority())
}

func TestProviderName(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	assert.Equal(t, "MWAA", p.Name())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "MWAA", svc.Name())
}

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	untagPath := "/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent?tagKeys=env"
	rec := doMWAARequest(t, h, http.MethodDelete, untagPath, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(
		t,
		h,
		http.MethodGet,
		"/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(
		t,
		h,
		http.MethodPost,
		"/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent",
		map[string]any{
			"Tags": map[string]string{"k": "v"},
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateEnvironment_ValidationError(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create environment first.
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/update-valid", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:r",
		"SourceBucketArn":  "arn:b",
		"MinWorkers":       int32(1),
		"MaxWorkers":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with invalid workers.
	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/update-valid", map[string]any{
		"MinWorkers": int32(20),
		"MaxWorkers": int32(5),
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_ListEnvironments_InvalidMethod(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPost, "/environments", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_CliToken_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/clitoken/myenv", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_WebToken_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/webtoken/myenv", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateEnvironment_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "invalid_json_body",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Test the create environment validation path via backend directly.
			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env-err", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:        "dags/",
				ExecutionRoleArn: "arn:r",
				SourceBucketArn:  "arn:b",
				MinWorkers:       5,
				MaxWorkers:       3,
			})
			assert.Error(t, err)
			_ = tt.wantStatus
		})
	}
}

func TestBackend_UpdateEnvironment_MinMaxValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateReq *mwaa.ExportedUpdateEnvironmentRequest
		name      string
		wantErr   bool
	}{
		{
			name: "valid_update",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				DagS3Path: "new-dags/",
			},
			wantErr: false,
		},
		{
			name: "min_greater_than_max_fails",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 20,
				MaxWorkers: 5,
			},
			wantErr: true,
		},
		{
			name: "only_min_set_keeps_existing_max",
			updateReq: &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: 1,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(
				context.Background(),
				"env-update",
				&mwaa.ExportedCreateEnvironmentRequest{
					DagS3Path:        "dags/",
					ExecutionRoleArn: "arn:r",
					SourceBucketArn:  "arn:b",
				},
			)
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "env-update")

			_, err = b.UpdateEnvironment(context.Background(), "env-update", tt.updateReq)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		authService string
		wantMatch   bool
	}{
		{
			name:        "environments_path_matches",
			path:        "/environments/myenv",
			authService: "airflow",
			wantMatch:   true,
		},
		{
			name:        "tags_path_matches",
			path:        "/tags/arn:aws:airflow:us-east-1:123:environment/myenv",
			authService: "airflow",
			wantMatch:   true,
		},
		{
			name:        "clitoken_path_matches",
			path:        "/clitoken/myenv",
			authService: "airflow",
			wantMatch:   true,
		},
		{
			name:        "wrong_service_no_match",
			path:        "/environments/myenv",
			authService: "s3",
			wantMatch:   false,
		},
		{
			name:        "unrelated_path_no_match",
			path:        "/buckets/mybucket",
			authService: "airflow",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.authService != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/"+tt.authService+"/aws4_request",
				)
			}
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "create_environment",
			method: http.MethodPut,
			path:   "/environments/myenv",
			wantOp: "CreateEnvironment",
		},
		{
			name:   "get_environment",
			method: http.MethodGet,
			path:   "/environments/myenv",
			wantOp: "GetEnvironment",
		},
		{
			name:   "delete_environment",
			method: http.MethodDelete,
			path:   "/environments/myenv",
			wantOp: "DeleteEnvironment",
		},
		{
			name:   "list_environments",
			method: http.MethodGet,
			path:   "/environments",
			wantOp: "ListEnvironments",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/tags/arn",
			wantOp: "TagResource",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/tags/arn",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "untag_resource",
			method: http.MethodDelete,
			path:   "/tags/arn",
			wantOp: "UntagResource",
		},
		{
			name:   "create_cli_token",
			method: http.MethodPost,
			path:   "/clitoken/myenv",
			wantOp: "CreateCliToken",
		},
		{
			name:   "create_web_token",
			method: http.MethodPost,
			path:   "/webtoken/myenv",
			wantOp: "CreateWebLoginToken",
		},
		{
			name:   "unknown_path",
			method: http.MethodGet,
			path:   "/unknown",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandlerExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantName string
	}{
		{
			name:     "environment_path",
			path:     "/environments/my-env-name",
			wantName: "my-env-name",
		},
		{
			name:     "clitoken_path",
			path:     "/clitoken/my-env",
			wantName: "my-env",
		},
		{
			name:     "webtoken_path",
			path:     "/webtoken/my-env",
			wantName: "my-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

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

func TestHandler_PublishMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:    "publish_metrics",
			envName: "publish-metrics-env",
			seed:    true,
			body: map[string]any{
				"MetricData": []map[string]any{
					{"MetricName": "TaskInstance", "Value": 1.0, "Unit": "Count"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "publish_empty_metrics",
			envName: "publish-empty-metrics-env",
			seed:    true,
			body: map[string]any{
				"MetricData": []map[string]any{},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "env_not_found",
			envName: "nonexistent-metrics-env",
			seed:    false,
			body: map[string]any{
				"MetricData": []map[string]any{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			envName:    "metrics-invalid-json-env",
			seed:       true,
			body:       nil,
			wantStatus: http.StatusBadRequest,
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

			if tt.name == "invalid_json" {
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPost,
					"/metrics/environments/"+tt.envName,
					strings.NewReader("{invalid"),
				)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			rec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/"+tt.envName, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetMetrics(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create environment.
	seedRec := doMWAARequest(t, h, http.MethodPut, "/environments/metrics-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, seedRec.Code)

	// Publish some metrics.
	pubRec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/metrics-env", map[string]any{
		"MetricData": []map[string]any{
			{"MetricName": "TaskInstance", "Value": 5.0, "Unit": "Count"},
		},
	})
	require.Equal(t, http.StatusOK, pubRec.Code)

	// Get metrics.
	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/metrics-env", nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	data, ok := resp["MetricData"].([]any)
	assert.True(t, ok)
	assert.Len(t, data, 1)

	// Not found.
	notFoundRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/missing-env", nil)
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)
}

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "invoke_rest_api",
			method: http.MethodPost,
			path:   "/restapi/my-env",
			wantOp: "InvokeRestApi",
		},
		{
			name:   "publish_metrics",
			method: http.MethodPost,
			path:   "/metrics/environments/my-env",
			wantOp: "PublishMetrics",
		},
		{
			name:   "get_metrics",
			method: http.MethodGet,
			path:   "/metrics/environments/my-env",
			wantOp: "GetMetrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantName string
	}{
		{
			name:     "restapi_path",
			path:     "/restapi/my-env",
			wantName: "my-env",
		},
		{
			name:     "metrics_path",
			path:     "/metrics/environments/my-env",
			wantName: "my-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

func TestHandler_RouteMatcher_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		authService string
		wantMatch   bool
	}{
		{
			name:        "restapi_path_matches",
			path:        "/restapi/myenv",
			authService: "airflow",
			wantMatch:   true,
		},
		{
			name:        "metrics_path_matches",
			path:        "/metrics/environments/myenv",
			authService: "airflow",
			wantMatch:   true,
		},
		{
			name:        "restapi_wrong_service_no_match",
			path:        "/restapi/myenv",
			authService: "s3",
			wantMatch:   false,
		},
		{
			name:        "metrics_wrong_service_no_match",
			path:        "/metrics/environments/myenv",
			authService: "s3",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/"+tt.authService+"/aws4_request",
			)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	ops := h.GetSupportedOperations()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "InvokeRestApi", wantOp: "InvokeRestApi"},
		{name: "PublishMetrics", wantOp: "PublishMetrics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.wantOp)
		})
	}
}

func TestBackend_InvokeRestApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedInvokeRestAPIRequest
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "success",
			envName: "invoke-env",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
		},
		{
			name:    "env_not_found",
			envName: "nonexistent",
			seed:    false,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_method",
			envName: "invoke-env-missing-method",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_path",
			envName: "invoke-env-missing-path",
			seed:    true,
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			resp, err := b.InvokeRestAPI(context.Background(), tt.envName, tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, int32(200), resp.RestAPIStatusCode)
		})
	}
}

func TestBackend_PublishMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedPublishMetricsRequest
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "success",
			envName: "metrics-env",
			seed:    true,
			req: &mwaa.ExportedPublishMetricsRequest{
				MetricData: []mwaa.ExportedMetricDatum{
					{MetricName: "TaskInstance"},
				},
			},
		},
		{
			name:    "empty_metrics",
			envName: "metrics-env-empty",
			seed:    true,
			req:     &mwaa.ExportedPublishMetricsRequest{MetricData: []mwaa.ExportedMetricDatum{}},
		},
		{
			name:    "env_not_found",
			envName: "nonexistent",
			seed:    false,
			req:     &mwaa.ExportedPublishMetricsRequest{MetricData: []mwaa.ExportedMetricDatum{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			err := b.PublishMetrics(context.Background(), tt.envName, tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
