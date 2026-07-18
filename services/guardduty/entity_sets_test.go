package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThreatEntitySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "create_get_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				// CreateThreatEntitySet
				rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/threatentityset", map[string]any{
					"name":     "my-threat-set",
					"format":   "TXT",
					"location": "s3://my-bucket/threat.txt",
					"activate": true,
					"tags":     map[string]string{"team": "security"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				setID, _ := createResp["threatEntitySetId"].(string)
				require.NotEmpty(t, setID)

				// GetThreatEntitySet
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "my-threat-set", getResp["name"])
				assert.Equal(t, "ACTIVE", getResp["status"])

				// UpdateThreatEntitySet
				activate := false
				_ = activate
				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/threatentityset/"+setID, map[string]any{
					"name":     "updated-threat-set",
					"location": "s3://my-bucket/threat-v2.txt",
					"activate": false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/"+setID, nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "updated-threat-set", getResp["name"])
				assert.Equal(t, "INACTIVE", getResp["status"])

				// ListThreatEntitySets
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatentityset", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				ids, _ := listResp["threatEntitySetIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, setID, ids[0])

				// DeleteThreatEntitySet
				rec = doRequest(t, h, http.MethodDelete, "/detector/"+id+"/threatentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatentityset", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				idsAfter, _ := listResp["threatEntitySetIds"].([]any)
				assert.Empty(t, idsAfter)
			},
		},
		{
			name: "get_not_found",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/no-such-set", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.fn(t, h)
		})
	}
}

func TestTrustedEntitySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "create_get_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				// CreateTrustedEntitySet
				rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/trustedentityset", map[string]any{
					"name":     "my-trusted-set",
					"format":   "TXT",
					"location": "s3://my-bucket/trusted.txt",
					"activate": true,
					"tags":     map[string]string{"team": "security"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				setID, _ := createResp["trustedEntitySetId"].(string)
				require.NotEmpty(t, setID)

				// GetTrustedEntitySet
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "my-trusted-set", getResp["name"])
				assert.Equal(t, "ACTIVE", getResp["status"])

				// UpdateTrustedEntitySet
				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/trustedentityset/"+setID, map[string]any{
					"name":     "updated-trusted-set",
					"location": "s3://my-bucket/trusted-v2.txt",
					"activate": false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/"+setID, nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "updated-trusted-set", getResp["name"])
				assert.Equal(t, "INACTIVE", getResp["status"])

				// ListTrustedEntitySets
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				ids, _ := listResp["trustedEntitySetIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, setID, ids[0])

				// DeleteTrustedEntitySet
				rec = doRequest(t, h, http.MethodDelete, "/detector/"+id+"/trustedentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				idsAfter, _ := listResp["trustedEntitySetIds"].([]any)
				assert.Empty(t, idsAfter)
			},
		},
		{
			name: "get_not_found",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/no-such-set", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.fn(t, h)
		})
	}
}
