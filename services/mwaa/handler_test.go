package mwaa_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newHandlerForTest(t *testing.T) *mwaa.Handler {
	t.Helper()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doMWAARequest(t *testing.T, h *mwaa.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/airflow/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

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
			name:    "duplicate_returns_conflict",
			envName: "dupe-env",
			body: map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/mwaa-role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			},
			wantStatus: http.StatusConflict,
			wantArn:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.name == "duplicate_returns_conflict" {
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

func TestHandler_TagsFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{
			name:    "tag_list_untag",
			envName: "tag-test-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			// Create environment.
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			envARN := createResp["Arn"]
			require.NotEmpty(t, envARN)

			// TagResource.
			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"env": "test"},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// ListTagsForResource.
			listTagRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
			assert.Equal(t, http.StatusOK, listTagRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listTagRec.Body.Bytes(), &tagsResp))
			tags, ok := tagsResp["Tags"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test", tags["env"])
		})
	}
}
