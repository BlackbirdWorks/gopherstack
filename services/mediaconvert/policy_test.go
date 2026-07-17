package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestMediaConvert_Policy_TableTests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *mediaconvert.InMemoryBackend)
		name       string
		method     string
		path       string
		wantInBody string
		wantStatus int
	}{
		{
			name:       "get_policy_not_found",
			method:     http.MethodGet,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "get_policy_exists",
			setup: func(b *mediaconvert.InMemoryBackend) {
				b.PutPolicy("ALLOWED", "ALLOWED", "ALLOWED")
			},
			method:     http.MethodGet,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusOK,
			wantInBody: "policy",
		},
		{
			name:       "delete_policy_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete_policy_exists",
			setup: func(b *mediaconvert.InMemoryBackend) {
				b.PutPolicy("ALLOWED", "ALLOWED", "ALLOWED")
			},
			method:     http.MethodDelete,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			if tt.setup != nil {
				tt.setup(b)
			}
			h := mediaconvert.NewHandler(b)

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

func TestMediaConvert_Policy_DeleteAfterGet(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	b.PutPolicy("ALLOWED", "DISALLOWED", "ALLOWED")
	h := mediaconvert.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	policy, ok := out["policy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ALLOWED", policy["httpInputs"])
	assert.Equal(t, "DISALLOWED", policy["httpsInputs"])

	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestPutPolicy_Success verifies PUT /policy sets and returns a policy.
func TestPutPolicy_Success(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/policy",
		map[string]any{"policy": map[string]any{
			"httpInputs":  "ALLOWED",
			"httpsInputs": "ALLOWED",
			"s3Inputs":    "ALLOWED",
		}})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	pol, ok := out["policy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ALLOWED", pol["httpInputs"])
}

// TestPutPolicy_EmptyBody sets an empty policy successfully.
func TestPutPolicy_EmptyBody(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/policy", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetPolicy_AfterPut verifies policy persists after PutPolicy.
func TestGetPolicy_AfterPut(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	doRequest(t, h, http.MethodPut, "/2017-08-29/policy",
		map[string]any{"policy": map[string]any{"s3Inputs": "DISABLED"}})

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	pol := out["policy"].(map[string]any)
	assert.Equal(t, "DISABLED", pol["s3Inputs"])
}
