package mwaa_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWebLoginToken_HTTP_NonAvailable_Returns404(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/web-http-env", map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":      "arn:aws:s3:::b",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doMWAARequest(t, h, http.MethodPost, "/webtoken/web-http-env", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 3. UpdateEnvironment – AVAILABLE-state guard
// ─────────────────────────────────────────────────────────────

func TestHandler_WebToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/nonexistent", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_WebToken_HappyPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/web-happy", map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":      "arn:aws:s3:::bucket",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	doMWAARequest(t, h, http.MethodGet, "/environments/web-happy", nil)

	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/web-happy", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["WebToken"])
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
				"DagS3Path":            "dags/",
				"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":      "arn:aws:s3:::bucket",
				"NetworkConfiguration": networkConfigBody(),
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

func TestWebLoginToken_HTTP_ResponseStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/jwt-web-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"NetworkConfiguration": networkConfigBody(),
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/jwt-web-http-env", nil) // promote CREATING → AVAILABLE

	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/jwt-web-http-env", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["WebToken"])
	assert.NotEmpty(t, resp["WebServerHostname"])

	parts := strings.Split(resp["WebToken"], ".")
	assert.Len(t, parts, 3)
}
