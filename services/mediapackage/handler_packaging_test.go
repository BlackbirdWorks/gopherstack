package mediapackage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func TestPackagingConfiguration_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "missing id returns 422",
			body:     map[string]any{"packagingGroupId": "g1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "with id returns 201 with arn and id",
			body:     map[string]any{"id": "pc1", "packagingGroupId": "g1"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "pc1", resp["id"])
				assert.Contains(
					t,
					resp["arn"],
					"arn:aws:mediapackage:us-east-1:000000000000:packaging_configurations/pc1",
				)
				assert.Equal(t, "g1", resp["packagingGroupId"])
				assert.NotEmpty(t, resp["createdAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/packaging_configurations", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestPackagingConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	backend := h.Backend.(*mediapackage.InMemoryBackend)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/packaging_configurations", map[string]any{
		"id":               "pc1",
		"packagingGroupId": "g1",
		"description":      "test config",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, mediapackage.PackagingConfigCount(backend))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/packaging_configurations/pc1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "pc1", descResp["id"])
	assert.Equal(t, "test config", descResp["description"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/packaging_configurations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["packagingConfigurations"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/packaging_configurations/pc1", nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 0, mediapackage.PackagingConfigCount(backend))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/packaging_configurations/pc1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestChannelLifecyclePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		method      string
		channelID   string
		wantCode    int
		createFirst bool
	}{
		{
			name:      "put lifecycle policy on missing channel returns 404",
			method:    http.MethodPut,
			channelID: "nonexistent",
			body:      map[string]any{"policy": `{"rules":[]}`},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "get lifecycle policy on missing channel returns 404",
			method:    http.MethodGet,
			channelID: "nonexistent",
			body:      nil,
			wantCode:  http.StatusNotFound,
		},
		{
			name:        "put lifecycle policy on existing channel returns 200",
			createFirst: true,
			method:      http.MethodPut,
			channelID:   "ch-lc",
			body:        map[string]any{"policy": `{"rules":[]}`},
			wantCode:    http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.createFirst {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": tc.channelID})
				require.Equal(t, http.StatusCreated, rec.Code)
			}
			rec := doRequest(t, h, tc.method, "/channels/"+tc.channelID+"/lifecycle_policy", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestChannelLifecyclePolicy_PutGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create channel
	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": "ch-lc2"})
	require.Equal(t, http.StatusCreated, rec.Code)

	policy := `{"rules":[{"retention":{"unit":"DAYS","value":7}}]}`

	// Put policy
	rec = doRequest(t, h, http.MethodPut, "/channels/ch-lc2/lifecycle_policy", map[string]any{"policy": policy})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get policy
	rec = doRequest(t, h, http.MethodGet, "/channels/ch-lc2/lifecycle_policy", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, policy, resp["policy"])
}
