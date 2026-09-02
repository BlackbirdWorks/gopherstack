package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestUpdateSensitivityInspectionTemplate_FieldsSurviveIndependentUpdates
// guards gopherstack-c8ge: UpdateSensitivityInspectionTemplateInput carries
// Description, Excludes, and Includes as independently-optional pointers.
// Updating Includes alone must not wipe Excludes set by an earlier,
// unrelated call.
func TestUpdateSensitivityInspectionTemplate_FieldsSurviveIndependentUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	templates, _ := listResp["sensitivityInspectionTemplates"].([]any)
	require.Len(t, templates, 1)
	tmpl0, _ := templates[0].(map[string]any)
	templateID, _ := tmpl0["id"].(string)
	require.NotEmpty(t, templateID)

	// Update A: set description and excludes.
	rec = doRequest(t, h, http.MethodPut, "/templates/sensitivity-inspections/"+templateID, map[string]any{
		"description": "original description",
		"excludes": map[string]any{
			"managedDataIdentifierIds": []string{"CREDIT_CARD_NUMBER"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update B: set includes only, omitting description and excludes.
	rec = doRequest(t, h, http.MethodPut, "/templates/sensitivity-inspections/"+templateID, map[string]any{
		"includes": map[string]any{
			"managedDataIdentifierIds": []string{"EMAIL_ADDRESS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/"+templateID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))

	assert.Equal(t, "original description", updated["description"],
		"description must survive an Update that never mentioned it")

	excludes, ok := updated["excludes"].(map[string]any)
	require.True(t, ok, "excludes must survive an Update that never mentioned it, got %#v", updated)
	assert.ElementsMatch(t, []any{"CREDIT_CARD_NUMBER"}, excludes["managedDataIdentifierIds"])

	includes, ok := updated["includes"].(map[string]any)
	require.True(t, ok, "B's own field must apply")
	assert.ElementsMatch(t, []any{"EMAIL_ADDRESS"}, includes["managedDataIdentifierIds"])
}

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
				assert.Equal(t, templateID, getResp["sensitivityInspectionTemplateId"])

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
