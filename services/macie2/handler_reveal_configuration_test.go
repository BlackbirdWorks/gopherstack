package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRevealConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_and_update_reveal_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetRevealConfiguration — default DISABLED
				rec := doRequest(t, h, http.MethodGet, "/reveal-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				cfg, _ := getResp["configuration"].(map[string]any)
				assert.Equal(t, "DISABLED", cfg["status"])

				// UpdateRevealConfiguration
				rec = doRequest(t, h, http.MethodPut, "/reveal-configuration", map[string]any{
					"configuration": map[string]any{
						"status":   "ENABLED",
						"kmsKeyId": "arn:aws:kms:us-east-1:000000000000:key/reveal-key",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/reveal-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				updCfg, _ := updated["configuration"].(map[string]any)
				assert.Equal(t, "ENABLED", updCfg["status"])
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/reveal-key", updCfg["kmsKeyId"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- sensitive data occurrences ---

func TestSensitiveDataOccurrences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_occurrences_and_availability_for_finding",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// Enable Macie and Reveal Config
				doRequest(t, h, http.MethodPost, "/macie", map[string]any{
					"status": "ENABLED",
				})
				doRequest(t, h, http.MethodPut, "/reveal-configuration", map[string]any{
					"configuration": map[string]any{
						"kmsKeyId": "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
						"status":   "ENABLED",
					},
				})

				// Create a sample finding
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Personal"},
				})

				// Get finding IDs
				rec := doRequest(t, h, http.MethodPost, "/findings", nil)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				findingIDs, _ := listResp["findingIds"].([]any)
				require.NotEmpty(t, findingIDs)
				findingID, _ := findingIDs[0].(string)

				// GetSensitiveDataOccurrences
				rec = doRequest(t, h, http.MethodGet, "/findings/"+findingID+"/reveal", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetSensitiveDataOccurrencesAvailability
				rec = doRequest(t, h, http.MethodGet, "/findings/"+findingID+"/reveal/availability", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var avail map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &avail))
				assert.Equal(t, "AVAILABLE", avail["code"])
			},
		},
		{
			name: "missing_finding_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/findings/nonexistent/reveal", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
