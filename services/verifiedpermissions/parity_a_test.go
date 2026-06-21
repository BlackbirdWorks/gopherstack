package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_BatchGetPolicyIncludesDefinition verifies that BatchGetPolicy
// results include the definition field (either static or templateLinked).
// Real AWS always returns definition in each result item; the emulator
// previously omitted it, causing callers to see nil definition and panic.
func TestParity_BatchGetPolicyIncludesDefinition(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create a policy store.
	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)

	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))

	policyStoreID, _ := storeResp["policyStoreId"].(string)
	require.NotEmpty(t, policyStoreID)

	// Create a static policy.
	staticRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement":   `permit(principal, action, resource);`,
				"description": "parity static policy",
			},
		},
	})
	require.Equal(t, http.StatusOK, staticRec.Code)

	var staticResp map[string]any
	require.NoError(t, json.Unmarshal(staticRec.Body.Bytes(), &staticResp))

	staticPolicyID, _ := staticResp["policyId"].(string)
	require.NotEmpty(t, staticPolicyID)

	// Create a policy template then a template-linked policy.
	tmplRec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": policyStoreID,
		"statement":     `permit(principal == ?principal, action, resource == ?resource);`,
		"description":   "parity template",
	})
	require.Equal(t, http.StatusOK, tmplRec.Code)

	var tmplResp map[string]any
	require.NoError(t, json.Unmarshal(tmplRec.Body.Bytes(), &tmplResp))

	templateID, _ := tmplResp["policyTemplateId"].(string)
	require.NotEmpty(t, templateID)

	tlRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"templateLinked": map[string]any{
				"policyTemplateId": templateID,
				"principal": map[string]any{
					"entityType": "User",
					"entityId":   "alice",
				},
				"resource": map[string]any{
					"entityType": "Document",
					"entityId":   "doc1",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, tlRec.Code)

	var tlResp map[string]any
	require.NoError(t, json.Unmarshal(tlRec.Body.Bytes(), &tlResp))

	tlPolicyID, _ := tlResp["policyId"].(string)
	require.NotEmpty(t, tlPolicyID)

	tests := []struct {
		name       string
		policyID   string
		wantDefKey string
		wantField  string
		wantValue  string
	}{
		{
			name:       "static_definition_present",
			policyID:   staticPolicyID,
			wantDefKey: "static",
			wantField:  "description",
			wantValue:  "parity static policy",
		},
		{
			name:       "template_linked_definition_present",
			policyID:   tlPolicyID,
			wantDefKey: "templateLinked",
			wantField:  "policyTemplateId",
			wantValue:  templateID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			batchRec := doVPRequest(t, h, "BatchGetPolicy", map[string]any{
				"requests": []map[string]any{
					{
						"policyStoreId": policyStoreID,
						"policyId":      tt.policyID,
					},
				},
			})
			require.Equal(t, http.StatusOK, batchRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &resp))

			results, ok := resp["results"].([]any)
			require.True(t, ok, "results must be an array")
			require.Len(t, results, 1, "expected exactly one result")

			item, ok := results[0].(map[string]any)
			require.True(t, ok)

			def, ok := item["definition"].(map[string]any)
			require.True(t, ok, "definition must be present in batch result item")

			defSubObj, ok := def[tt.wantDefKey].(map[string]any)
			require.True(t, ok, "definition.%s must be present", tt.wantDefKey)

			assert.Equal(t, tt.wantValue, defSubObj[tt.wantField],
				"definition.%s.%s must match", tt.wantDefKey, tt.wantField)
		})
	}
}
