package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// snapshotHandler snapshots h's backend and returns a fresh handler plus the snapshot bytes.
func snapshotHandler(h *opensearch.Handler) (*opensearch.Handler, []byte) {
	snap := h.Backend.Snapshot()
	fresh := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := opensearch.NewHandler(fresh)
	h2.Backend = fresh

	return h2, snap
}

// ---------------------------------------------------------------------------
// Serverless collections
// ---------------------------------------------------------------------------

func TestServerless_CreateCollection(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
		map[string]any{
			"name":        "my-coll",
			"type":        "SEARCH",
			"description": "test collection",
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	detail, ok := out["createCollectionDetail"].(map[string]any)
	require.True(t, ok, "createCollectionDetail missing")
	assert.Equal(t, "my-coll", detail["name"])
	assert.Equal(t, "SEARCH", detail["type"])
	assert.Equal(t, "ACTIVE", detail["status"])
	assert.NotEmpty(t, detail["id"])
	assert.NotEmpty(t, detail["arn"])
	assert.NotEmpty(t, detail["collectionEndpoint"])
}

func TestServerless_CreateCollection_DefaultType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
		map[string]any{"name": "default-type-coll"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["createCollectionDetail"].(map[string]any)
	assert.Equal(t, "SEARCH", detail["type"])
}

func TestServerless_BatchGetCollections(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 2 collections.
	for _, name := range []string{"coll-a", "coll-b"} {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
			map[string]any{"name": name, "type": "VECTORSEARCH"})
		r.Body.Close()
	}

	// Batch-get by name.
	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collections",
		map[string]any{"names": []string{"coll-a", "coll-b"}})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details, ok := out["collectionDetails"].([]any)
	require.True(t, ok)
	assert.Len(t, details, 2)
}

func TestServerless_ListCollections(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"list-a", "list-b", "list-c"} {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
			map[string]any{"name": name})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet, "/2021-11-01/opensearch/serverless/collections", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	summaries, ok := out["collectionSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 3)
}

func TestServerless_DeleteCollection(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create and capture ID.
	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
		map[string]any{"name": "to-delete"})
	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	id := createOut["createCollectionDetail"].(map[string]any)["id"].(string)

	// Delete.
	delResp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/collection/"+id, nil)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(delResp.Body).Decode(&delOut))
	detail := delOut["deleteCollectionDetail"].(map[string]any)
	assert.Equal(t, "DELETED", detail["status"])

	// List — should be empty.
	listResp := doRequest(t, h, http.MethodGet, "/2021-11-01/opensearch/serverless/collections", nil)
	defer listResp.Body.Close()
	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))
	summaries := listOut["collectionSummaries"].([]any)
	assert.Empty(t, summaries)
}

func TestServerless_DeleteCollection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/collection/nonexistent-id", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Serverless access policies
// ---------------------------------------------------------------------------

func TestServerless_CreateAccessPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
		map[string]any{
			"name": "my-policy",
			"type": "data",
			"policy": `[{"Rules":[{"ResourceType":"collection",` +
				`"Resource":["collection/my-coll"]}],"Principal":["*"],"Description":""}]`,
			"description": "test data access policy",
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	detail, ok := out["accessPolicyDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-policy", detail["name"])
	assert.Equal(t, "data", detail["type"])
	assert.NotEmpty(t, detail["policyVersion"])
}

func TestServerless_GetAccessPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create.
	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
		map[string]any{
			"name":   "get-test",
			"type":   "data",
			"policy": `[{"Rules":[]}]`,
		})
	createResp.Body.Close()

	// Get.
	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/accesspolicies/get-test?type=data", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["accessPolicyDetail"].(map[string]any)
	assert.Equal(t, "get-test", detail["name"])
}

func TestServerless_ListAccessPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"ap-1", "ap-2"} {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
			map[string]any{"name": name, "type": "data", "policy": `[]`})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/accesspolicies?type=data", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	summaries := out["accessPolicySummaries"].([]any)
	assert.Len(t, summaries, 2)
}

func TestServerless_UpdateAccessPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create.
	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
		map[string]any{"name": "update-ap", "type": "data", "policy": `[{"old":true}]`})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(r.Body).Decode(&createOut))
	r.Body.Close()
	version := createOut["accessPolicyDetail"].(map[string]any)["policyVersion"].(string)

	// Update.
	resp := doRequest(t, h, http.MethodPut,
		"/2021-11-01/opensearch/serverless/accesspolicies/update-ap?type=data",
		map[string]any{
			"policy":        `[{"new":true}]`,
			"description":   "updated",
			"policyVersion": version,
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["accessPolicyDetail"].(map[string]any)
	assert.Equal(t, "updated", detail["description"])
}

func TestServerless_DeleteAccessPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
		map[string]any{"name": "del-ap", "type": "data", "policy": `[]`})
	r.Body.Close()

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/accesspolicies/del-ap?type=data", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List — empty.
	listResp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/accesspolicies?type=data", nil)
	defer listResp.Body.Close()
	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))
	assert.Empty(t, listOut["accessPolicySummaries"].([]any))
}

func TestServerless_DeleteAccessPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/accesspolicies/nonexistent?type=data", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Serverless security configs
// ---------------------------------------------------------------------------

func TestServerless_CreateSecurityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/securityconfigs",
		map[string]any{
			"type":        "saml",
			"description": "SAML config for corp IdP",
			"samlOptions": map[string]any{
				"metadata":       "<EntityDescriptor>...</EntityDescriptor>",
				"groupAttribute": "groups",
				"userAttribute":  "email",
			},
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	detail, ok := out["securityConfigDetail"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, detail["id"])
	assert.Equal(t, "saml", detail["type"])
	assert.NotEmpty(t, detail["configVersion"])
}

func TestServerless_GetSecurityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/securityconfigs",
		map[string]any{"type": "saml"})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()
	id := createOut["securityConfigDetail"].(map[string]any)["id"].(string)

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/securityconfigs/"+id, nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, id, out["securityConfigDetail"].(map[string]any)["id"])
}

func TestServerless_ListSecurityConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for range 3 {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/securityconfigs",
			map[string]any{"type": "saml"})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/securityconfigs?type=saml", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	summaries := out["securityConfigSummaries"].([]any)
	assert.Len(t, summaries, 3)
}

func TestServerless_UpdateSecurityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/securityconfigs",
		map[string]any{"type": "saml", "description": "old"})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	id := createOut["securityConfigDetail"].(map[string]any)["id"].(string)
	version := createOut["securityConfigDetail"].(map[string]any)["configVersion"].(string)

	resp := doRequest(t, h, http.MethodPut,
		"/2021-11-01/opensearch/serverless/securityconfigs/"+id,
		map[string]any{
			"description":   "updated desc",
			"configVersion": version,
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["securityConfigDetail"].(map[string]any)
	assert.Equal(t, "updated desc", detail["description"])
}

func TestServerless_DeleteSecurityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/securityconfigs",
		map[string]any{"type": "saml"})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	id := createOut["securityConfigDetail"].(map[string]any)["id"].(string)

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/securityconfigs/"+id, nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Not found.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/securityconfigs/"+id, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

// ---------------------------------------------------------------------------
// Serverless encryption policies
// ---------------------------------------------------------------------------

func TestServerless_CreateEncryptionPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/encryptionpolicies",
		map[string]any{
			"name":   "enc-policy",
			"type":   "encryption",
			"policy": `{"Rules":[{"ResourceType":"collection","Resource":["collection/*"]}],"AWSOwnedKey":true}`,
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["encryptionPolicyDetail"].(map[string]any)
	assert.Equal(t, "enc-policy", detail["name"])
	assert.NotEmpty(t, detail["policyVersion"])
}

func TestServerless_ListEncryptionPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"ep-1", "ep-2"} {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/encryptionpolicies",
			map[string]any{"name": name, "type": "encryption", "policy": `{}`})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/encryptionpolicies?type=encryption", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	summaries := out["encryptionPolicySummaries"].([]any)
	assert.Len(t, summaries, 2)
}

func TestServerless_GetEncryptionPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/encryptionpolicies",
		map[string]any{"name": "get-ep", "type": "encryption", "policy": `{}`})
	r.Body.Close()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/encryptionpolicies/get-ep?type=encryption", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "get-ep", out["encryptionPolicyDetail"].(map[string]any)["name"])
}

func TestServerless_UpdateEncryptionPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/encryptionpolicies",
		map[string]any{"name": "upd-ep", "type": "encryption", "policy": `{"old":true}`})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(r.Body).Decode(&createOut))
	r.Body.Close()
	version := createOut["encryptionPolicyDetail"].(map[string]any)["policyVersion"].(string)

	resp := doRequest(t, h, http.MethodPut,
		"/2021-11-01/opensearch/serverless/encryptionpolicies/upd-ep?type=encryption",
		map[string]any{
			"policy":        `{"new":true}`,
			"description":   "updated",
			"policyVersion": version,
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "updated", out["encryptionPolicyDetail"].(map[string]any)["description"])
}

func TestServerless_DeleteEncryptionPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/encryptionpolicies",
		map[string]any{"name": "del-ep", "type": "encryption", "policy": `{}`})
	r.Body.Close()

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/encryptionpolicies/del-ep?type=encryption", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify gone.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/encryptionpolicies/del-ep?type=encryption", nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

// ---------------------------------------------------------------------------
// Serverless network policies
// ---------------------------------------------------------------------------

func TestServerless_CreateNetworkPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/networksecuritypolicies",
		map[string]any{
			"name":   "net-policy",
			"type":   "network",
			"policy": `[{"Rules":[{"ResourceType":"collection","Resource":["collection/*"]}],"AllowFromPublic":true}]`,
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	detail := out["networkPolicyDetail"].(map[string]any)
	assert.Equal(t, "net-policy", detail["name"])
}

func TestServerless_ListNetworkPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"np-1", "np-2", "np-3"} {
		r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/networksecuritypolicies",
			map[string]any{"name": name, "type": "network", "policy": `[]`})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet,
		"/2021-11-01/opensearch/serverless/networksecuritypolicies?type=network", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	summaries := out["networkPolicySummaries"].([]any)
	assert.Len(t, summaries, 3)
}

func TestServerless_DeleteNetworkPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/networksecuritypolicies",
		map[string]any{"name": "del-np", "type": "network", "policy": `[]`})
	r.Body.Close()

	resp := doRequest(t, h, http.MethodDelete,
		"/2021-11-01/opensearch/serverless/networksecuritypolicies/del-np?type=network", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Domain off-peak window options
// ---------------------------------------------------------------------------

func TestDomain_OffPeakWindowOptions_Create(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName": "opw-domain",
			"OffPeakWindowOptions": map[string]any{
				"Enabled": true,
				"OffPeakWindow": map[string]any{
					"WindowStartTime": map[string]any{
						"Hours":   2,
						"Minutes": 30,
					},
				},
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	st := out["DomainStatus"].(map[string]any)

	opw, ok := st["OffPeakWindowOptions"].(map[string]any)
	require.True(t, ok, "OffPeakWindowOptions missing from response")
	assert.Equal(t, true, opw["Enabled"])

	win := opw["OffPeakWindow"].(map[string]any)
	wst := win["WindowStartTime"].(map[string]any)
	assert.InDelta(t, float64(2), wst["Hours"], 0)
	assert.InDelta(t, float64(30), wst["Minutes"], 0)
}

func TestDomain_OffPeakWindowOptions_UpdateConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "opw-update-domain"})
	createResp.Body.Close()

	upResp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/opw-update-domain/config",
		map[string]any{
			"OffPeakWindowOptions": map[string]any{
				"Enabled": true,
				"OffPeakWindow": map[string]any{
					"WindowStartTime": map[string]any{"Hours": 4, "Minutes": 0},
				},
			},
		})
	defer upResp.Body.Close()
	assert.Equal(t, http.StatusOK, upResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))
	cfg := out["DomainConfig"].(map[string]any)
	opwCfg := cfg["OffPeakWindowOptions"].(map[string]any)
	opwOpts := opwCfg["Options"].(map[string]any)
	assert.Equal(t, true, opwOpts["Enabled"])
}

// ---------------------------------------------------------------------------
// Domain IAM Identity Center options
// ---------------------------------------------------------------------------

func TestDomain_IamIdentityCenterOptions_Create(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName": "idc-domain",
			"IamIdentityCenterOptions": map[string]any{
				"EnabledAPIAccess":                       true,
				"IamIdentityCenterArn":                   "arn:aws:sso:::instance/ssoins-abc123",
				"IamRoleForIdentityCenterApplicationArn": "arn:aws:iam::123456789012:role/MyRole",
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	st := out["DomainStatus"].(map[string]any)

	idcOpts, ok := st["IamIdentityCenterOptions"].(map[string]any)
	require.True(t, ok, "IamIdentityCenterOptions missing")
	assert.Equal(t, true, idcOpts["EnabledAPIAccess"])
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-abc123", idcOpts["IamIdentityCenterArn"])
}

func TestDomain_IamIdentityCenterOptions_UpdateConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "idc-upd-domain"}).Body.Close()

	upResp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/idc-upd-domain/config",
		map[string]any{
			"IamIdentityCenterOptions": map[string]any{
				"EnabledAPIAccess":     true,
				"IamIdentityCenterArn": "arn:aws:sso:::instance/ssoins-xyz",
			},
		})
	defer upResp.Body.Close()
	assert.Equal(t, http.StatusOK, upResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))
	cfg := out["DomainConfig"].(map[string]any)
	idcCfg := cfg["IamIdentityCenterOptions"].(map[string]any)
	idcOpts := idcCfg["Options"].(map[string]any)
	assert.Equal(t, true, idcOpts["EnabledAPIAccess"])
}

// ---------------------------------------------------------------------------
// Domain enable software update options
// ---------------------------------------------------------------------------

func TestDomain_EnableSoftwareUpdateOptions_Create(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName": "sw-update-domain",
			"EnableSoftwareUpdateOptions": map[string]any{
				"AutoSoftwareUpdateEnabled": true,
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	st := out["DomainStatus"].(map[string]any)

	swOpts, ok := st["EnableSoftwareUpdateOptions"].(map[string]any)
	require.True(t, ok, "EnableSoftwareUpdateOptions missing")
	assert.Equal(t, true, swOpts["AutoSoftwareUpdateEnabled"])
}

// ---------------------------------------------------------------------------
// Domain blue-green deployment options (via ClusterConfig)
// ---------------------------------------------------------------------------

func TestDomain_BlueGreenDeploymentOptions_Create(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName": "bg-domain",
			"ClusterConfig": map[string]any{
				"InstanceType":  "r6g.large.search",
				"InstanceCount": 3,
				"BlueGreenDeploymentOptions": map[string]any{
					"Enabled": true,
				},
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	st := out["DomainStatus"].(map[string]any)
	cc := st["ClusterConfig"].(map[string]any)

	bgOpts, ok := cc["BlueGreenDeploymentOptions"].(map[string]any)
	require.True(t, ok, "BlueGreenDeploymentOptions missing from ClusterConfig")
	assert.Equal(t, true, bgOpts["Enabled"])
}

func TestDomain_BlueGreenDeploymentOptions_UpdateConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "bg-upd-domain"}).Body.Close()

	upResp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/bg-upd-domain/config",
		map[string]any{
			"ClusterConfig": map[string]any{
				"BlueGreenDeploymentOptions": map[string]any{"Enabled": true},
			},
		})
	defer upResp.Body.Close()
	assert.Equal(t, http.StatusOK, upResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))
	cfg := out["DomainConfig"].(map[string]any)
	ccCfg := cfg["ClusterConfig"].(map[string]any)
	opts := ccCfg["Options"].(map[string]any)
	bgOpts := opts["BlueGreenDeploymentOptions"].(map[string]any)
	assert.Equal(t, true, bgOpts["Enabled"])
}

// ---------------------------------------------------------------------------
// Package with custom source (dictionaries)
// ---------------------------------------------------------------------------

func TestPackage_CreateWithSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages",
		map[string]any{
			"PackageName":        "my-dictionary",
			"PackageType":        "TXT-DICTIONARY",
			"PackageDescription": "custom synonym dictionary",
			"PackageSource": map[string]any{
				"S3BucketName": "my-bucket",
				"S3Key":        "dictionaries/synonyms.txt",
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	pkg := out["PackageDetails"].(map[string]any)
	assert.Equal(t, "my-dictionary", pkg["PackageName"])
	assert.Equal(t, "TXT-DICTIONARY", pkg["PackageType"])
	assert.NotEmpty(t, pkg["PackageID"])

	src, ok := pkg["PackageSource"].(map[string]any)
	require.True(t, ok, "PackageSource missing from response")
	assert.Equal(t, "my-bucket", src["S3BucketName"])
	assert.Equal(t, "dictionaries/synonyms.txt", src["S3Key"])
}

func TestPackage_CreateWithEncryptionOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages",
		map[string]any{
			"PackageName": "enc-pkg",
			"PackageType": "ZIP-PLUGIN",
			"PackageEncryptionOptions": map[string]any{
				"EncryptionEnabled": true,
				"KmsKeyIdentifier":  "arn:aws:kms:us-east-1:123456789012:key/my-key",
			},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	pkg := out["PackageDetails"].(map[string]any)
	encOpts, ok := pkg["PackageEncryptionOptions"].(map[string]any)
	require.True(t, ok, "PackageEncryptionOptions missing")
	assert.Equal(t, true, encOpts["EncryptionEnabled"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", encOpts["KmsKeyIdentifier"])
}

func TestPackage_AvailablePackageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages",
		map[string]any{"PackageName": "versioned-pkg", "PackageType": "TXT-DICTIONARY"})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	pkg := out["PackageDetails"].(map[string]any)
	assert.Equal(t, "1", pkg["AvailablePackageVersion"])
}

// ---------------------------------------------------------------------------
// Serverless persistence round-trip
// ---------------------------------------------------------------------------

func TestServerless_Persistence_CollectionsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a collection.
	createResp := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/collection",
		map[string]any{"name": "persist-coll", "type": "TIMESERIES"})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	origID := createOut["createCollectionDetail"].(map[string]any)["id"].(string)

	// Snapshot the backend.
	h2, snap := snapshotHandler(h)

	// Restore into a new handler.
	require.NoError(t, h2.Backend.Restore(snap))

	// List collections — should still be there.
	listResp := doRequest(t, h2, http.MethodGet, "/2021-11-01/opensearch/serverless/collections", nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))
	summaries := listOut["collectionSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, origID, summaries[0].(map[string]any)["id"])
}

func TestServerless_Persistence_AccessPoliciesRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r := doRequest(t, h, http.MethodPost, "/2021-11-01/opensearch/serverless/accesspolicies",
		map[string]any{"name": "persist-ap", "type": "data", "policy": `[{"test":true}]`})
	r.Body.Close()

	h2, snap := snapshotHandler(h)
	require.NoError(t, h2.Backend.Restore(snap))

	listResp := doRequest(t, h2, http.MethodGet,
		"/2021-11-01/opensearch/serverless/accesspolicies?type=data", nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))
	summaries := listOut["accessPolicySummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "persist-ap", summaries[0].(map[string]any)["name"])
}
