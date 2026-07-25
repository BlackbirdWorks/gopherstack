package mwaa_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCliToken_HTTP_NonAvailable_Returns404(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	// Create env – it lands in CREATING state; first GET is not called, so it stays CREATING.
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/cli-http-env", map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":      "arn:aws:s3:::b",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doMWAARequest(t, h, http.MethodPost, "/clitoken/cli-http-env", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 2. CreateWebLoginToken – AVAILABLE-state guard
// ─────────────────────────────────────────────────────────────

func TestHandler_CliToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/nonexistent", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CliToken_HappyPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/cli-happy", map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":      "arn:aws:s3:::bucket",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	doMWAARequest(t, h, http.MethodGet, "/environments/cli-happy", nil)

	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/cli-happy", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["CliToken"])
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
				"DagS3Path":            "dags/",
				"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":      "arn:aws:s3:::bucket",
				"NetworkConfiguration": networkConfigBody(),
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

func TestCliToken_HTTP_ResponseStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/jwt-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"NetworkConfiguration": networkConfigBody(),
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/jwt-http-env", nil) // promote CREATING → AVAILABLE

	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/jwt-http-env", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["CliToken"])
	assert.NotEmpty(t, resp["WebServerHostname"])

	parts := strings.Split(resp["CliToken"], ".")
	assert.Len(t, parts, 3)
}

func TestHTTP_TokensIncludeWebServerHostname(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/http-token-hostname", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"NetworkConfiguration": networkConfigBody(),
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/http-token-hostname", nil) // promote CREATING → AVAILABLE

	tests := []struct {
		name    string
		path    string
		wantKey string
	}{
		{name: "cli_token", path: "/clitoken/http-token-hostname", wantKey: "CliToken"},
		{name: "web_token", path: "/webtoken/http-token-hostname", wantKey: "WebToken"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doMWAARequest(t, h, http.MethodPost, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp[tt.wantKey])
			assert.NotEmpty(t, resp["WebServerHostname"])
			assert.Contains(t, resp["WebServerHostname"], "amazonaws.com")
		})
	}
}
