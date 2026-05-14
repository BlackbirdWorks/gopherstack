package mwaa_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

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
