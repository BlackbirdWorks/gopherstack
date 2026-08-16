package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestSensitivityInspectionTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "list_get_update_template",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// ListSensitivityInspectionTemplates — auto-creates default
				rec := doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				templates, _ := listResp["sensitivityInspectionTemplates"].([]any)
				require.Len(t, templates, 1)

				tmpl0, _ := templates[0].(map[string]any)
				templateID, _ := tmpl0["id"].(string)
				require.NotEmpty(t, templateID)

				// GetSensitivityInspectionTemplate
				rec = doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/"+templateID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, templateID, getResp["id"])

				// UpdateSensitivityInspectionTemplate
				rec = doRequest(
					t,
					h,
					http.MethodPut,
					"/templates/sensitivity-inspections/"+templateID,
					map[string]any{
						"description": "Updated description",
						"includes": map[string]any{
							"managedDataIdentifierIds": []string{"EMAIL_ADDRESS"},
						},
					},
				)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/"+templateID, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "Updated description", updated["description"])
			},
		},
		{
			name: "get_missing_template_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/nonexistent", nil)
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
