package verifiedpermissions_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests lock in the documented per-page default the real SDK states
// for every List* operation's MaxResults ("If you do not specify this
// parameter, the operation defaults to N ... per response"): omitting
// maxResults must cap the page at N, not return every item unbounded.

func TestVPHandler_ListPolicyStores_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	for range 11 {
		createTestPolicyStore(t, h)
	}

	rec := doVPRequest(t, h, "ListPolicyStores", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stores, _ := resp["policyStores"].([]any)
	assert.Len(t, stores, 10, "ListPolicyStores omits maxResults => real SDK defaults to 10 per response")
	assert.NotEmpty(t, resp["nextToken"], "an 11th store must page off, proving the cap was applied")
}

func TestVPHandler_ListPolicies_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	for i := range 11 {
		rec := doVPRequest(t, h, "CreatePolicy", map[string]any{
			"policyStoreId": storeID,
			"definition": map[string]any{
				"static": map[string]any{
					"description": fmt.Sprintf("p%d", i),
					"statement":   "permit(principal, action, resource);",
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	rec := doVPRequest(t, h, "ListPolicies", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policies, _ := resp["policies"].([]any)
	assert.Len(t, policies, 10, "ListPolicies omits maxResults => real SDK defaults to 10 per response")
	assert.NotEmpty(t, resp["nextToken"])
}

func TestVPHandler_ListPolicyTemplates_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	for i := range 11 {
		rec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
			"policyStoreId": storeID,
			"statement":     fmt.Sprintf("permit(principal, action, resource) when { context.n == %d };", i),
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	rec := doVPRequest(t, h, "ListPolicyTemplates", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	templates, _ := resp["policyTemplates"].([]any)
	assert.Len(t, templates, 10, "ListPolicyTemplates omits maxResults => real SDK defaults to 10 per response")
	assert.NotEmpty(t, resp["nextToken"])
}

func TestVPHandler_ListIdentitySources_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	for i := range 11 {
		rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
			"policyStoreId": storeID,
			"configuration": map[string]any{
				"cognitoUserPoolConfiguration": map[string]any{
					"userPoolArn": fmt.Sprintf(
						"arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_pool%d", i,
					),
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	rec := doVPRequest(t, h, "ListIdentitySources", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	sources, _ := resp["identitySources"].([]any)
	assert.Len(t, sources, 10, "ListIdentitySources omits maxResults => real SDK defaults to 10 per response")
	assert.NotEmpty(t, resp["nextToken"])
}

func TestVPHandler_ListPolicyStoreAliases_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	for i := range 6 {
		rec := doVPRequest(t, h, "CreatePolicyStoreAlias", map[string]any{
			"aliasName":     fmt.Sprintf("policy-store-alias/a%d", i),
			"policyStoreId": storeID,
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	rec := doVPRequest(t, h, "ListPolicyStoreAliases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	aliases, _ := resp["policyStoreAliases"].([]any)
	assert.Len(t, aliases, 5, "ListPolicyStoreAliases omits maxResults => real SDK defaults to 5 per response")
	assert.NotEmpty(t, resp["nextToken"])
}

// TestVPHandler_CreateIdentitySource_CognitoGroupEntityTypeDefault locks in
// the real SDK's documented default on CognitoGroupConfiguration.
// GroupEntityType ("Defaults to AWS::CognitoGroup."): a request that supplies
// groupConfiguration but omits groupEntityType inside it must round-trip as
// "AWS::CognitoGroup", not as an empty string.
func TestVPHandler_CreateIdentitySource_CognitoGroupEntityTypeDefault(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId": storeID,
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn":        "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc123",
				"groupConfiguration": map[string]any{},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec = doVPRequest(t, h, "GetIdentitySource", map[string]any{
		"policyStoreId":    storeID,
		"identitySourceId": created["identitySourceId"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cfg, _ := resp["configuration"].(map[string]any)
	cognito, _ := cfg["cognitoUserPoolConfiguration"].(map[string]any)
	groupCfg, _ := cognito["groupConfiguration"].(map[string]any)
	assert.Equal(t, "AWS::CognitoGroup", groupCfg["groupEntityType"])
}
