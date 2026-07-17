package mwaa_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		envName    string
		wantStatus int
		wantArn    bool
	}{
		{
			name:    "creates_environment",
			envName: "my-env",
			body: map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa-role",
				"SourceBucketArn":  "arn:aws:s3:::my-bucket",
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			// MWAA's API model has no AlreadyExistsException at all --
			// CreateEnvironment's only documented errors are
			// InternalServerException, ServiceUnavailableException, and
			// ValidationException -- so a duplicate name is a 400, not a 409.
			name:    "duplicate_returns_validation_error",
			envName: "dupe-env",
			body: map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa-role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			},
			wantStatus: http.StatusBadRequest,
			wantArn:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.name == "duplicate_returns_validation_error" {
				rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, tt.body)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["Arn"])
			}
		})
	}
}

func TestHandler_GetEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:       "found",
			envName:    "existing-env",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			envName:    "missing-env",
			seed:       false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.seed {
				rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
					"DagS3Path":        "dags/",
					"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
					"SourceBucketArn":  "arn:aws:s3:::bucket",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doMWAARequest(t, h, http.MethodGet, "/environments/"+tt.envName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["Environment"])
			}
		})
	}
}

func TestHandler_ListEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantCount int
	}{
		{
			name:      "empty_list",
			seedNames: []string{},
			wantCount: 0,
		},
		{
			name:      "lists_environments",
			seedNames: []string{"env-a", "env-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for _, n := range tt.seedNames {
				doMWAARequest(t, h, http.MethodPut, "/environments/"+n, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
			}

			rec := doMWAARequest(t, h, http.MethodGet, "/environments", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			envs, ok := resp["Environments"].([]any)
			require.True(t, ok)
			assert.Len(t, envs, tt.wantCount)
		})
	}
}

func TestHandler_DeleteEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:       "deletes_existing",
			envName:    "to-delete",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			envName:    "missing",
			seed:       false,
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
			}

			rec := doMWAARequest(t, h, http.MethodDelete, "/environments/"+tt.envName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateEnvironment_AWSParity(t *testing.T) {
	t.Parallel()

	baseValid := func() map[string]any {
		return map[string]any{
			"DagS3Path":        "dags/",
			"ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa-role",
			"SourceBucketArn":  "arn:aws:s3:::my-bucket",
		}
	}

	tests := []struct {
		mutate     func(map[string]any)
		extra      map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "defaults_ok",
			mutate:     func(_ map[string]any) {},
			wantStatus: http.StatusOK,
		},
		{
			name:       "kms_key_must_be_arn",
			mutate:     func(b map[string]any) { b["KmsKey"] = "alias/foo" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "endpoint_management_invalid",
			mutate:     func(b map[string]any) { b["EndpointManagement"] = "BOGUS" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "plugins_path_requires_version",
			mutate:     func(b map[string]any) { b["PluginsS3Path"] = "plugins.zip" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "weekly_window_invalid",
			mutate:     func(b map[string]any) { b["WeeklyMaintenanceWindowStart"] = "MON-03-30" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "weekly_window_valid",
			mutate:     func(b map[string]any) { b["WeeklyMaintenanceWindowStart"] = "MON:03:30" },
			wantStatus: http.StatusOK,
		},
		{
			name:       "schedulers_out_of_range",
			mutate:     func(b map[string]any) { b["Schedulers"] = 6 },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "max_webservers_out_of_range",
			mutate:     func(b map[string]any) { b["MaxWebservers"] = 99 },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := baseValid()
			tt.mutate(body)

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/env-"+tt.name, body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestGetEnvironment_DerivedFields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createBody := map[string]any{
		"DagS3Path":                   "dags/",
		"ExecutionRoleArn":            "arn:aws:iam::123456789012:role/mwaa-role",
		"SourceBucketArn":             "arn:aws:s3:::my-bucket",
		"AirflowConfigurationOptions": map[string]string{"core.parallelism": "32"},
	}
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/derived-env", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doMWAARequest(t, h, http.MethodGet, "/environments/derived-env", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Environment mwaa.Environment `json:"Environment"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	env := resp.Environment
	assert.Equal(t, int32(2), env.Schedulers)
	assert.Equal(t, "SERVICE", env.EndpointManagement)
	assert.NotEmpty(t, env.ServiceRoleArn)
	assert.NotEmpty(t, env.CeleryExecutorQueue)
	assert.NotEmpty(t, env.DatabaseVpcEndpointService)
	assert.NotEmpty(t, env.WebserverVpcEndpointService)
	assert.Equal(t, "32", env.AirflowConfigurationOptions["core.parallelism"])
}

func TestListEnvironments_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		body := map[string]any{
			"DagS3Path":        "dags/",
			"ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa-role",
			"SourceBucketArn":  "arn:aws:s3:::my-bucket",
		}
		rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+n, body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name       string
		token      string
		maxResults string
		wantNext   string
		wantNames  []string
	}{
		{name: "page1", maxResults: "2", wantNames: []string{"a", "b"}, wantNext: "c"},
		{name: "page2", maxResults: "2", token: "c", wantNames: []string{"c", "d"}, wantNext: "e"},
		{name: "page3_partial", maxResults: "2", token: "e", wantNames: []string{"e"}, wantNext: ""},
		{name: "no_max", wantNames: []string{"a", "b", "c", "d", "e"}, wantNext: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := url.Values{}
			if tt.maxResults != "" {
				q.Set("MaxResults", tt.maxResults)
			}

			if tt.token != "" {
				q.Set("NextToken", tt.token)
			}

			path := "/environments"
			if encoded := q.Encode(); encoded != "" {
				path += "?" + encoded
			}

			rec := doMWAARequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp struct {
				NextToken    string   `json:"NextToken"`
				Environments []string `json:"Environments"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantNames, resp.Environments)
			assert.Equal(t, tt.wantNext, resp.NextToken)
		})
	}
}

func TestListEnvironments_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantStatus int
	}{
		{name: "zero", maxResults: "0", wantStatus: http.StatusBadRequest},
		{name: "negative", maxResults: "-1", wantStatus: http.StatusBadRequest},
		{name: "non_numeric", maxResults: "abc", wantStatus: http.StatusBadRequest},
		{name: "above_max", maxResults: "101", wantStatus: http.StatusBadRequest},
		{name: "valid", maxResults: "10", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodGet, "/environments?MaxResults="+tt.maxResults, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestWeeklyMaint_Create_HTTP_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/wmw-http-env", map[string]any{
		"DagS3Path":                    "dags/",
		"ExecutionRoleArn":             "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":              "arn:aws:s3:::b",
		"WeeklyMaintenanceWindowStart": "MON:25:00",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 2. Create-time MinWorkers > MaxWorkers rejection
// ─────────────────────────────────────────────────────────────

func TestListEnvironments_HTTP_MaxResults_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantCode   int
	}{
		{name: "max_results_1", maxResults: "1", wantCode: http.StatusOK},
		{name: "max_results_50", maxResults: "50", wantCode: http.StatusOK},
		{name: "max_results_100", maxResults: "100", wantCode: http.StatusOK},
		{name: "max_results_101_rejected", maxResults: "101", wantCode: http.StatusBadRequest},
		{name: "max_results_0_rejected", maxResults: "0", wantCode: http.StatusBadRequest},
		{name: "max_results_negative_rejected", maxResults: "-1", wantCode: http.StatusBadRequest},
		{name: "max_results_non_numeric", maxResults: "abc", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodGet,
				"/environments?MaxResults="+tt.maxResults, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestListEnvironments_HTTP_NoMaxResults_UsesDefault(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/environments", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["Environments"]
	assert.True(t, ok, "response must have Environments key")
}

func TestListEnvironments_HTTP_NextToken_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	for i := range 5 {
		rec := doMWAARequest(
			t, h, http.MethodPut,
			fmt.Sprintf("/environments/page-env-%02d", i),
			map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
				"SourceBucketArn":  "arn:aws:s3:::b",
			},
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Request page 1 with size 3.
	rec1 := doMWAARequest(t, h, http.MethodGet, "/environments?MaxResults=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	envs1 := resp1["Environments"].([]any)
	assert.Len(t, envs1, 3)

	nextToken, ok := resp1["NextToken"].(string)
	require.True(t, ok, "page 1 must include NextToken when more results exist")
	require.NotEmpty(t, nextToken)

	// Request page 2 using the NextToken.
	rec2 := doMWAARequest(t, h, http.MethodGet,
		"/environments?MaxResults=3&NextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	envs2 := resp2["Environments"].([]any)
	assert.Len(t, envs2, 2, "second page should have the remaining 2 environments")
	_, hasToken := resp2["NextToken"]
	assert.False(t, hasToken, "last page must not include NextToken")
}

// ─────────────────────────────────────────────────────────────
// 13. UntagResource backend – 404 on unknown ARN
// ─────────────────────────────────────────────────────────────

func TestHTTP_GetEnvironment_DefaultsPresent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/snap-defaults-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Second GET to get AVAILABLE state.
	_ = doMWAARequest(t, h, http.MethodGet, "/environments/snap-defaults-env", nil)
	getRec := doMWAARequest(t, h, http.MethodGet, "/environments/snap-defaults-env", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	env := resp["Environment"].(map[string]any)
	assert.InEpsilon(t, float64(10), env["MaxWorkers"], 0.001, "default MaxWorkers=10 should be visible via HTTP")
	assert.InEpsilon(t, float64(1), env["MinWorkers"], 0.001, "default MinWorkers=1 should be visible via HTTP")
	assert.Equal(t, "2.10.3", env["AirflowVersion"])
	assert.Equal(t, "mw1.small", env["EnvironmentClass"])
	assert.Equal(t, "AVAILABLE", env["Status"])
}

func TestEnvironmentName_HTTPValidation(t *testing.T) {
	t.Parallel()

	// HTTP tests only for names that form valid URL paths (no spaces or
	// URL-reserved chars that would panic httptest.NewRequest).
	tests := []struct {
		name       string
		envName    string
		wantStatus int
	}{
		{
			name:       "starts_with_digit_rejected",
			envName:    "1env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "contains_dot_rejected",
			envName:    "my.env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "too_long_rejected",
			envName:    strings.Repeat("a", 81),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_name_accepted",
			envName:    "valid-env-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_name_with_underscore",
			envName:    "Valid_Env",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestAirflowVersion_HTTPCreate_InvalidVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-test", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "3.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestAirflowVersion_HTTPCreate_ValidVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-test-ok", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "2.9.2",
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestAirflowVersion_Update_HTTP_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-upd-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/ver-upd-env", map[string]any{
		"AirflowVersion": "0.0.1",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 3: MaxWorkers upper bound (25)
// ─────────────────────────────────────────────────────────────

func TestMaxWorkers_HTTP_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"MaxWorkers": 50,
		"MinWorkers": 1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestMaxWorkers_HTTP_AtLimit(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-at-limit", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"MaxWorkers": 25,
		"MinWorkers": 1,
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestMaxWorkers_HTTP_Update_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-upd-http", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/workers-upd-http", map[string]any{
		"MaxWorkers": 30,
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 4: WorkerReplacementStrategy validation
// ─────────────────────────────────────────────────────────────

func TestWorkerReplacementStrategy_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy   string
		name       string
		wantStatus int
	}{
		{name: "forced_valid", strategy: "FORCED", wantStatus: http.StatusOK},
		{name: "drain_valid", strategy: "TERMINATION_WITH_DRAIN", wantStatus: http.StatusOK},
		{name: "invalid", strategy: "IMMEDIATE", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/ws-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/ws-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/ws-"+tt.name, map[string]any{
				"WorkerReplacementStrategy": tt.strategy,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestUpdateWebserverAccessMode_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode       string
		name       string
		wantStatus int
	}{
		{name: "public_valid", mode: "PUBLIC_ONLY", wantStatus: http.StatusOK},
		{name: "private_valid", mode: "PRIVATE_ONLY", wantStatus: http.StatusOK},
		{name: "invalid", mode: "UNKNOWN", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/wam-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/wam-http-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/wam-http-"+tt.name, map[string]any{
				"WebserverAccessMode": tt.mode,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestUpdateEnvironmentClass_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		class      string
		name       string
		wantStatus int
	}{
		{name: "large_valid", class: "mw1.large", wantStatus: http.StatusOK},
		{name: "invalid_class", class: "mw1.huge", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/cls-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/cls-http-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/cls-http-"+tt.name, map[string]any{
				"EnvironmentClass": tt.class,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

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

func TestLoggingConfig_HTTP_InvalidLevel_Create(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"SchedulerLogs": map[string]any{"LogLevel": "BOGUS"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLoggingConfig_HTTP_ValidLevel_Create(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-ok", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"SchedulerLogs": map[string]any{"LogLevel": "INFO"},
			"WorkerLogs":    map[string]any{"LogLevel": "WARNING"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestLoggingConfig_HTTP_InvalidLevel_Update(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-upd-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/http-log-upd-inv", map[string]any{
		"LoggingConfiguration": map[string]any{
			"TaskLogs": map[string]any{"LogLevel": "TRACE"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestLifecycle_HTTP_CreateResponseDoesNotExposeStatus(t *testing.T) {
	t.Parallel()

	// HTTP CreateEnvironment response only contains Arn, not Status.
	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-lc-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["Arn"])
	assert.Nil(t, resp["Status"], "create response must not expose Status")
}

func TestLifecycle_HTTP_GetEnvShowsCreatingThenAvailable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-lc-get-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// First GET: CREATING
	get1 := doMWAARequest(t, h, http.MethodGet, "/environments/http-lc-get-env", nil)
	require.Equal(t, http.StatusOK, get1.Code)

	var resp1 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get1.Body.Bytes(), &resp1))
	assert.Equal(t, "CREATING", resp1.Environment.Status)

	// Second GET: AVAILABLE
	get2 := doMWAARequest(t, h, http.MethodGet, "/environments/http-lc-get-env", nil)
	require.Equal(t, http.StatusOK, get2.Code)

	var resp2 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get2.Body.Bytes(), &resp2))
	assert.Equal(t, "AVAILABLE", resp2.Environment.Status)
}

func TestS3Paths_HTTP_PluginsWithoutVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-s3-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"PluginsS3Path": "plugins.zip",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestS3Paths_HTTP_AllThreeWithVersions(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-s3-ok", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"PluginsS3Path": "plugins.zip", "PluginsS3ObjectVersion": "v1",
		"RequirementsS3Path": "req.txt", "RequirementsS3ObjectVersion": "v2",
		"StartupScriptS3Path": "start.sh", "StartupScriptS3ObjectVersion": "v3",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 4. NetworkConfiguration scenarios
// ─────────────────────────────────────────────────────────────

func TestNetworkConfig_HTTP_UpdateSecurityGroupsOnlyAccepted(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/nc-http-upd", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Promote CREATING → AVAILABLE (UpdateEnvironment only accepts AVAILABLE envs).
	getRec := doMWAARequest(t, h, http.MethodGet, "/environments/nc-http-upd", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/nc-http-upd", map[string]any{
		"NetworkConfiguration": map[string]any{
			"SecurityGroupIds": []string{"sg-1"},
		},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// 5. AirflowConfigurationOptions round-trip and replace semantics
// ─────────────────────────────────────────────────────────────

func TestKmsKey_HTTP_InvalidRejected(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/kms-http-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"KmsKey": "not-an-arn",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 7. EndpointManagement validation and persistence
// ─────────────────────────────────────────────────────────────

func TestEndpointManagement_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mgmt       string
		name       string
		wantStatus int
	}{
		{name: "service_ok", mgmt: "SERVICE", wantStatus: http.StatusOK},
		{name: "customer_ok", mgmt: "CUSTOMER", wantStatus: http.StatusOK},
		{name: "bogus_rejected", mgmt: "HYBRID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/em-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				"EndpointManagement": tt.mgmt,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 8. WeeklyMaintenanceWindowStart on update
// ─────────────────────────────────────────────────────────────

func TestWeeklyMaintenance_HTTP_Update_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/wmw-http-upd", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/wmw-http-upd", map[string]any{
		"WeeklyMaintenanceWindowStart": "TUESDAY:12:00",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHTTP_FullCRUDLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create.
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/full-crud-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "2.9.2",
		"EnvironmentClass": "mw1.medium",
		"MaxWorkers":       5,
		"MinWorkers":       1,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]
	assert.NotEmpty(t, envARN)

	// Get (first call: CREATING).
	get1 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get1.Code)
	var resp1 struct {
		Environment struct {
			Status           string `json:"Status"`
			AirflowVersion   string `json:"AirflowVersion"`
			EnvironmentClass string `json:"EnvironmentClass"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get1.Body.Bytes(), &resp1))
	assert.Equal(t, "CREATING", resp1.Environment.Status)
	assert.Equal(t, "2.9.2", resp1.Environment.AirflowVersion)
	assert.Equal(t, "mw1.medium", resp1.Environment.EnvironmentClass)

	// Get (second call: AVAILABLE).
	get2 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get2.Code)
	var resp2 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get2.Body.Bytes(), &resp2))
	assert.Equal(t, "AVAILABLE", resp2.Environment.Status)

	// Update.
	updRec := doMWAARequest(t, h, http.MethodPatch, "/environments/full-crud-env", map[string]any{
		"EnvironmentClass": "mw1.large",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// Get after update (UPDATING).
	get3 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get3.Code)
	var resp3 struct {
		Environment struct {
			Status           string `json:"Status"`
			EnvironmentClass string `json:"EnvironmentClass"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get3.Body.Bytes(), &resp3))
	assert.Equal(t, "UPDATING", resp3.Environment.Status)
	assert.Equal(t, "mw1.large", resp3.Environment.EnvironmentClass)

	// Tag.
	tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
		"Tags": map[string]string{"test": "value"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// Delete.
	delRec := doMWAARequest(t, h, http.MethodDelete, "/environments/full-crud-env", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Get after delete: 404.
	get4 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	assert.Equal(t, http.StatusNotFound, get4.Code)
}

func TestHTTP_LoggingConfig_AllModules(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-all", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"DagProcessingLogs": map[string]any{"LogLevel": "INFO"},
			"SchedulerLogs":     map[string]any{"LogLevel": "WARNING"},
			"TaskLogs":          map[string]any{"LogLevel": "ERROR"},
			"WebserverLogs":     map[string]any{"LogLevel": "DEBUG"},
			"WorkerLogs":        map[string]any{"LogLevel": "CRITICAL"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Consume CREATING.
	doMWAARequest(t, h, http.MethodGet, "/environments/http-log-all", nil)

	getRec := doMWAARequest(t, h, http.MethodGet, "/environments/http-log-all", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp struct {
		Environment struct {
			LoggingConfiguration *mwaa.LoggingConfiguration `json:"LoggingConfiguration"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	require.NotNil(t, resp.Environment.LoggingConfiguration)
	require.NotNil(t, resp.Environment.LoggingConfiguration.DagProcessingLogs)
	assert.Equal(t, "INFO", resp.Environment.LoggingConfiguration.DagProcessingLogs.LogLevel)
	require.NotNil(t, resp.Environment.LoggingConfiguration.WorkerLogs)
	assert.Equal(t, "CRITICAL", resp.Environment.LoggingConfiguration.WorkerLogs.LogLevel)
}

func TestUpdateEnvironment_HTTP_NonAvailable_Returns400(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	// Create env – stays in CREATING until first GET promotes it.
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/upd-http-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// PATCH while env is still in CREATING state.
	rec = doMWAARequest(t, h, http.MethodPatch, "/environments/upd-http-env", map[string]any{
		"DagS3Path": "new-dags/",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
