package codeartifact_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestDomainOwnerQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		owner      string
		wantStatus int
	}{
		{name: "own account", owner: config.DefaultAccountID, wantStatus: http.StatusOK},
		{name: "omitted", owner: "", wantStatus: http.StatusOK},
		{name: "other account", owner: "999999999999", wantStatus: http.StatusNotFound},
		{name: "too short", owner: "1234", wantStatus: http.StatusBadRequest},
		{name: "not digits", owner: "12345678901a", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "d1")

			path := "/v1/domain?domain=d1"
			if tt.owner != "" {
				path += "&domain-owner=" + tt.owner
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestPackageVersionErrors_CarryMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "d1")
	setupRepo(t, h, "d1", "r1")
	seedVersion(t, h, "d1", "r1", "npm", "", "react", "17.0.0")

	rec := doRequest(t, h, http.MethodPost,
		"/v1/package/versions/delete?domain=d1&repository=r1&format=npm&package=react",
		map[string]any{"versions": []string{"99.0.0"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Failed map[string]struct {
			Code    string `json:"errorCode"`
			Message string `json:"errorMessage"`
		} `json:"failedVersions"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Contains(t, out.Failed, "99.0.0")
	assert.Equal(t, "NOT_FOUND", out.Failed["99.0.0"].Code)
	assert.Contains(t, out.Failed["99.0.0"].Message, "99.0.0")
}
