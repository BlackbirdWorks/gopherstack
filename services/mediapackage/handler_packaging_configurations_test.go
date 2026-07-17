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

// TestPackagingConfig_DeleteReturns202 verifies delete packaging config returns 202.
func TestPackagingConfig_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete packaging config returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			code, _ := doRequestJSON(t, h, http.MethodPost, "/packaging_configurations", map[string]any{
				"id":               "pc-del",
				"packagingGroupId": "g1",
			})
			require.Equal(t, http.StatusCreated, code)

			code, _ = doRequestJSON(t, h, http.MethodDelete, "/packaging_configurations/pc-del", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}
