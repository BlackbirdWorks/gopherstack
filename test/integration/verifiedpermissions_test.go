package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifiedPermissionsPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "VerifiedPermissions."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func verifiedPermissionsReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_VerifiedPermissions_CreatePolicyStore(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := verifiedPermissionsPost(t, "CreatePolicyStore", map[string]any{
		"description": "integ-test-store",
	})
	body := verifiedPermissionsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "policyStoreId")
	assert.Contains(t, body, "arn")
}

func TestIntegration_VerifiedPermissions_PolicyStoreLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a policy store.
	createResp := verifiedPermissionsPost(t, "CreatePolicyStore", map[string]any{
		"description": "lifecycle-store",
	})
	createBody := verifiedPermissionsReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "create body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	storeID, ok := createResult["policyStoreId"].(string)
	require.True(t, ok, "policyStoreId should be a string")
	require.NotEmpty(t, storeID)

	// Get the policy store.
	getResp := verifiedPermissionsPost(t, "GetPolicyStore", map[string]any{
		"policyStoreId": storeID,
	})
	getBody := verifiedPermissionsReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "get body: %s", getBody)
	assert.Contains(t, getBody, storeID)
	assert.Contains(t, getBody, "lifecycle-store")

	// List policy stores.
	listResp := verifiedPermissionsPost(t, "ListPolicyStores", map[string]any{})
	listBody := verifiedPermissionsReadBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode, "list body: %s", listBody)
	assert.Contains(t, listBody, "policyStores")

	// Update the policy store.
	updateResp := verifiedPermissionsPost(t, "UpdatePolicyStore", map[string]any{
		"policyStoreId": storeID,
		"description":   "updated-description",
	})
	updateBody := verifiedPermissionsReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "update body: %s", updateBody)

	// Delete the policy store.
	deleteResp := verifiedPermissionsPost(t, "DeletePolicyStore", map[string]any{
		"policyStoreId": storeID,
	})
	deleteBody := verifiedPermissionsReadBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode, "delete body: %s", deleteBody)

	// Get after delete should fail.
	getAfterDeleteResp := verifiedPermissionsPost(t, "GetPolicyStore", map[string]any{
		"policyStoreId": storeID,
	})
	assert.Equal(t, http.StatusBadRequest, getAfterDeleteResp.StatusCode)
	getAfterDeleteResp.Body.Close()
}

func TestIntegration_VerifiedPermissions_PolicyLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a policy store first.
	createStoreResp := verifiedPermissionsPost(t, "CreatePolicyStore", map[string]any{
		"description": "policy-lifecycle-store",
	})
	createStoreBody := verifiedPermissionsReadBody(t, createStoreResp)
	require.Equal(t, http.StatusOK, createStoreResp.StatusCode, "create store body: %s", createStoreBody)

	var storeResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createStoreBody), &storeResult))
	storeID := storeResult["policyStoreId"].(string)

	// Create a policy.
	createPolicyResp := verifiedPermissionsPost(t, "CreatePolicy", map[string]any{
		"policyStoreId": storeID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement": "permit(principal, action, resource);",
			},
		},
	})
	createPolicyBody := verifiedPermissionsReadBody(t, createPolicyResp)
	require.Equal(t, http.StatusOK, createPolicyResp.StatusCode, "create policy body: %s", createPolicyBody)

	var policyResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createPolicyBody), &policyResult))
	policyID := policyResult["policyId"].(string)
	require.NotEmpty(t, policyID)

	// Get the policy.
	getPolicyResp := verifiedPermissionsPost(t, "GetPolicy", map[string]any{
		"policyStoreId": storeID,
		"policyId":      policyID,
	})
	getPolicyBody := verifiedPermissionsReadBody(t, getPolicyResp)
	assert.Equal(t, http.StatusOK, getPolicyResp.StatusCode, "get policy body: %s", getPolicyBody)
	assert.Contains(t, getPolicyBody, policyID)

	// List policies.
	listPoliciesResp := verifiedPermissionsPost(t, "ListPolicies", map[string]any{
		"policyStoreId": storeID,
	})
	listPoliciesBody := verifiedPermissionsReadBody(t, listPoliciesResp)
	assert.Equal(t, http.StatusOK, listPoliciesResp.StatusCode, "list policies body: %s", listPoliciesBody)
	assert.Contains(t, listPoliciesBody, "policies")

	// Update policy.
	updatePolicyResp := verifiedPermissionsPost(t, "UpdatePolicy", map[string]any{
		"policyStoreId": storeID,
		"policyId":      policyID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement": "forbid(principal, action, resource);",
			},
		},
	})
	updatePolicyBody := verifiedPermissionsReadBody(t, updatePolicyResp)
	assert.Equal(t, http.StatusOK, updatePolicyResp.StatusCode, "update policy body: %s", updatePolicyBody)

	// Delete policy.
	deletePolicyResp := verifiedPermissionsPost(t, "DeletePolicy", map[string]any{
		"policyStoreId": storeID,
		"policyId":      policyID,
	})
	deletePolicyBody := verifiedPermissionsReadBody(t, deletePolicyResp)
	assert.Equal(t, http.StatusOK, deletePolicyResp.StatusCode, "delete policy body: %s", deletePolicyBody)
}

func TestIntegration_VerifiedPermissions_PolicyTemplateLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a policy store first.
	createStoreResp := verifiedPermissionsPost(t, "CreatePolicyStore", map[string]any{
		"description": "template-lifecycle-store",
	})
	createStoreBody := verifiedPermissionsReadBody(t, createStoreResp)
	require.Equal(t, http.StatusOK, createStoreResp.StatusCode, "create store body: %s", createStoreBody)

	var storeResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createStoreBody), &storeResult))
	storeID := storeResult["policyStoreId"].(string)

	// Create a policy template.
	createTplResp := verifiedPermissionsPost(t, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": storeID,
		"description":   "My template",
		"statement":     "permit(principal == ?principal, action, resource);",
	})
	createTplBody := verifiedPermissionsReadBody(t, createTplResp)
	require.Equal(t, http.StatusOK, createTplResp.StatusCode, "create template body: %s", createTplBody)

	var tplResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createTplBody), &tplResult))
	templateID := tplResult["policyTemplateId"].(string)
	require.NotEmpty(t, templateID)

	// Get the policy template.
	getTplResp := verifiedPermissionsPost(t, "GetPolicyTemplate", map[string]any{
		"policyStoreId":    storeID,
		"policyTemplateId": templateID,
	})
	getTplBody := verifiedPermissionsReadBody(t, getTplResp)
	assert.Equal(t, http.StatusOK, getTplResp.StatusCode, "get template body: %s", getTplBody)
	assert.Contains(t, getTplBody, templateID)
	assert.Contains(t, getTplBody, "My template")

	// List policy templates.
	listTplResp := verifiedPermissionsPost(t, "ListPolicyTemplates", map[string]any{
		"policyStoreId": storeID,
	})
	listTplBody := verifiedPermissionsReadBody(t, listTplResp)
	assert.Equal(t, http.StatusOK, listTplResp.StatusCode, "list templates body: %s", listTplBody)
	assert.Contains(t, listTplBody, "policyTemplates")

	// Update policy template.
	updateTplResp := verifiedPermissionsPost(t, "UpdatePolicyTemplate", map[string]any{
		"policyStoreId":    storeID,
		"policyTemplateId": templateID,
		"description":      "Updated template",
		"statement":        "forbid(principal == ?principal, action, resource);",
	})
	updateTplBody := verifiedPermissionsReadBody(t, updateTplResp)
	assert.Equal(t, http.StatusOK, updateTplResp.StatusCode, "update template body: %s", updateTplBody)

	// Delete policy template.
	deleteTplResp := verifiedPermissionsPost(t, "DeletePolicyTemplate", map[string]any{
		"policyStoreId":    storeID,
		"policyTemplateId": templateID,
	})
	deleteTplBody := verifiedPermissionsReadBody(t, deleteTplResp)
	assert.Equal(t, http.StatusOK, deleteTplResp.StatusCode, "delete template body: %s", deleteTplBody)
}

func TestIntegration_VerifiedPermissions_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := verifiedPermissionsPost(t, "GetPolicyStore", map[string]any{
		"policyStoreId": "nonexistent-store-id",
	})
	body := verifiedPermissionsReadBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceNotFoundException")
}
