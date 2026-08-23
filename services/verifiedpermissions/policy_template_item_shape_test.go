package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestListPolicyTemplates_ItemHasNoStatement proves ListPolicyTemplates'
// PolicyTemplateItem does not leak the Cedar statement text: the real SDK's
// types.PolicyTemplateItem (verifiedpermissions@v1.36.4: types/types.go:2121)
// has only createdDate/lastUpdatedDate/policyStoreId/policyTemplateId/
// description/name -- no statement field, unlike GetPolicyTemplateOutput's
// sibling shape which does require one. A typed client can't observe an
// over-emitted key (it just ignores unknown JSON fields), so this asserts
// against the raw response body instead.
func TestListPolicyTemplates_ItemHasNoStatement(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": storeID,
		"description":   "leak check",
		"statement":     "permit(principal == ?principal, action, resource);",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doVPRequest(t, h, "ListPolicyTemplates", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		PolicyTemplates []map[string]any `json:"policyTemplates"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.PolicyTemplates, 1)

	item := body.PolicyTemplates[0]
	require.Contains(t, item, "description")
	require.NotContains(t, item, "statement", "real PolicyTemplateItem has no statement field")
}
