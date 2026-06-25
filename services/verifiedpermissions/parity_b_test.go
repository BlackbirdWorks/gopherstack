package verifiedpermissions_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestJWT builds a minimal unsigned JWT whose payload contains the given claims.
func makeTestJWT(claims map[string]any) string {
	b64 := func(v any) string {
		raw, _ := json.Marshal(v)

		return base64.RawURLEncoding.EncodeToString(raw)
	}

	header := b64(map[string]string{"alg": "none", "typ": "JWT"})
	payload := b64(claims)

	return fmt.Sprintf("%s.%s.fakesig", header, payload)
}

// --- UnknownOperationException ---

func TestParity_UnknownOp_ReturnsUnknownOperationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{name: "completely_unknown_op", target: "FrobnicatePolicy"},
		{name: "misspelled_op", target: "CreatPolicyStore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, tt.target, map[string]any{})

			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "UnknownOperationException", resp["__type"], "body: %s", rec.Body.String())
		})
	}
}

// --- Tags for non-policyStore resource types ---

func TestParity_Tags_PolicyResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create policy store.
	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	// Get policy store ARN.
	getStoreRec := doVPRequest(t, h, "GetPolicyStore", map[string]any{
		"policyStoreId": policyStoreID,
	})
	require.Equal(t, http.StatusOK, getStoreRec.Code)
	var getStoreResp map[string]any
	require.NoError(t, json.Unmarshal(getStoreRec.Body.Bytes(), &getStoreResp))
	storeARN := getStoreResp["arn"].(string)
	_ = storeARN

	// Create a policy.
	policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement":   `permit(principal, action, resource);`,
				"description": "tag-test policy",
			},
		},
	})
	require.Equal(t, http.StatusOK, policyRec.Code)
	var policyResp map[string]any
	require.NoError(t, json.Unmarshal(policyRec.Body.Bytes(), &policyResp))
	policyID := policyResp["policyId"].(string)

	// Build the policy ARN (format: arn:aws:verifiedpermissions::<acct>:policy/<storeID>/<policyID>)
	policyARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:policy/%s/%s",
		policyStoreID, policyID,
	)

	// Tag the policy.
	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": policyARN,
		"tags":        map[string]string{"Env": "test"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code, "TagResource body: %s", tagRec.Body.String())

	listRec1 := doVPRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": policyARN})
	require.Equal(t, http.StatusOK, listRec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(listRec1.Body.Bytes(), &resp1))
	tags1, _ := resp1["tags"].(map[string]any)
	assert.Equal(t, "test", tags1["Env"])

	// Overwrite the tag.
	overwriteRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": policyARN,
		"tags":        map[string]string{"Env": "prod"},
	})
	require.Equal(t, http.StatusOK, overwriteRec.Code, "overwrite TagResource body: %s", overwriteRec.Body.String())

	listRec2 := doVPRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": policyARN})
	require.Equal(t, http.StatusOK, listRec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &resp2))
	tags2, _ := resp2["tags"].(map[string]any)
	assert.Equal(t, "prod", tags2["Env"])

	// Untag it.
	untagRec := doVPRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": policyARN,
		"tagKeys":     []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code, "UntagResource body: %s", untagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": policyARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	_, envPresent := tags["Env"]
	assert.False(t, envPresent, "Env tag should be removed after UntagResource")
}

func TestParity_Tags_PolicyTemplateResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	tmplRec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": policyStoreID,
		"statement":     `permit(principal == ?principal, action, resource == ?resource);`,
		"description":   "tag-test template",
	})
	require.Equal(t, http.StatusOK, tmplRec.Code)
	var tmplResp map[string]any
	require.NoError(t, json.Unmarshal(tmplRec.Body.Bytes(), &tmplResp))
	templateID := tmplResp["policyTemplateId"].(string)

	templateARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:policy-template/%s/%s",
		policyStoreID, templateID,
	)

	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": templateARN,
		"tags":        map[string]string{"Purpose": "parity-test"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code, "body: %s", tagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": templateARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "parity-test", tags["Purpose"])
}

func TestParity_Tags_IdentitySourceResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       policyStoreID,
		"principalEntityType": "User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc",
				"clientIds":   []string{"client1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, isRec.Code, "CreateIdentitySource body: %s", isRec.Body.String())
	var isResp map[string]any
	require.NoError(t, json.Unmarshal(isRec.Body.Bytes(), &isResp))
	identitySourceID := isResp["identitySourceId"].(string)

	identitySourceARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:identity-source/%s/%s",
		policyStoreID, identitySourceID,
	)

	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": identitySourceARN,
		"tags":        map[string]string{"Owner": "parity"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code, "TagResource body: %s", tagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": identitySourceARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "parity", tags["Owner"])
}

// --- Opaque base64 pagination tokens ---

func TestParity_Pagination_OpaqueTokens(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	// Create 3 policies.
	for i := range 3 {
		rec := doVPRequest(t, h, "CreatePolicy", map[string]any{
			"policyStoreId": policyStoreID,
			"definition": map[string]any{
				"static": map[string]any{
					"statement":   `permit(principal, action, resource) when { true };`,
					"description": fmt.Sprintf("policy-%d", i),
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: maxResults=2.
	page1Rec := doVPRequest(t, h, "ListPolicies", map[string]any{
		"policyStoreId": policyStoreID,
		"maxResults":    2,
	})
	require.Equal(t, http.StatusOK, page1Rec.Code)
	var page1 map[string]any
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1))

	nextToken, _ := page1["nextToken"].(string)
	require.NotEmpty(t, nextToken, "nextToken must be present when more results exist")

	// Token must be opaque base64 (not a raw UUID).
	_, err := base64.StdEncoding.DecodeString(nextToken)
	require.NoError(t, err, "nextToken should be valid base64")
	assert.NotContains(t, nextToken, "-", "nextToken should be opaque (no raw UUID dashes)")

	// Page 2 using the token.
	page2Rec := doVPRequest(t, h, "ListPolicies", map[string]any{
		"policyStoreId": policyStoreID,
		"maxResults":    2,
		"nextToken":     nextToken,
	})
	require.Equal(t, http.StatusOK, page2Rec.Code, "page2 body: %s", page2Rec.Body.String())
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2))

	page2Items, _ := page2["policies"].([]any)
	assert.Len(t, page2Items, 1, "page2 should have the remaining 1 policy")
	_, hasMore := page2["nextToken"]
	assert.False(t, hasMore, "no nextToken on last page")
}

// --- IsAuthorizedWithToken JWT principal extraction ---

func TestParity_IsAuthorizedWithToken_PrincipalFromJWT(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Set up policy store with a Cedar policy and an OIDC identity source.
	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	// Create a permissive policy so we can verify ALLOW.
	policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement": `permit(principal, action, resource);`,
			},
		},
	})
	require.Equal(t, http.StatusOK, policyRec.Code)

	// Create an OIDC identity source so principalFromToken can resolve the type.
	isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       policyStoreID,
		"principalEntityType": "User",
		"configuration": map[string]any{
			"openIdConnectConfiguration": map[string]any{
				"issuer": "https://example.com",
				"tokenSelection": map[string]any{
					"accessTokenOnly": map[string]any{
						"principalIdClaim": "sub",
						"audiences":        []string{"myapp"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, isRec.Code, "CreateIdentitySource body: %s", isRec.Body.String())

	tests := []struct {
		claims     map[string]any
		wantDec    string
		name       string
		wantStatus int
	}{
		{
			name:       "valid_token_with_sub_claim_allows",
			claims:     map[string]any{"sub": "user-alice", "iss": "https://example.com"},
			wantStatus: http.StatusOK,
			wantDec:    "ALLOW",
		},
		{
			name:       "valid_token_missing_sub_still_evaluates",
			claims:     map[string]any{"email": "alice@example.com"},
			wantStatus: http.StatusOK,
			wantDec:    "ALLOW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token := makeTestJWT(tt.claims)
			rec := doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
				"policyStoreId": policyStoreID,
				"accessToken":   token,
				"action": map[string]any{
					"actionType": "Action",
					"actionId":   "view",
				},
				"resource": map[string]any{
					"entityType": "Resource",
					"entityId":   "res1",
				},
			})

			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())

			if tt.wantDec != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantDec, resp["decision"])
			}
		})
	}
}

func TestParity_IsAuthorizedWithToken_MissingToken(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	rec := doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
		"policyStoreId": policyStoreID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
}

// --- PolicySet caching: dirty-flag invalidation ---

func TestParity_PolicySetCache_DirtyOnPolicyMutation(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	authReq := map[string]any{
		"policyStoreId": policyStoreID,
		"principal": map[string]any{
			"entityType": "User",
			"entityId":   "alice",
		},
		"action": map[string]any{
			"actionType": "Action",
			"actionId":   "view",
		},
		"resource": map[string]any{
			"entityType": "Resource",
			"entityId":   "doc1",
		},
	}

	// Before any policy: should DENY.
	rec := doVPRequest(t, h, "IsAuthorized", authReq)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DENY", resp["decision"], "no policies → DENY")

	// Add a permissive policy.
	policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement": `permit(principal, action, resource);`,
			},
		},
	})
	require.Equal(t, http.StatusOK, policyRec.Code)
	var policyResp map[string]any
	require.NoError(t, json.Unmarshal(policyRec.Body.Bytes(), &policyResp))
	policyID := policyResp["policyId"].(string)

	// After adding policy: cache must invalidate, now ALLOW.
	rec2 := doVPRequest(t, h, "IsAuthorized", authReq)
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Equal(t, "ALLOW", resp2["decision"], "after CreatePolicy → ALLOW (cache invalidated)")

	// Delete the policy: cache must invalidate again, back to DENY.
	delRec := doVPRequest(t, h, "DeletePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"policyId":      policyID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	rec3 := doVPRequest(t, h, "IsAuthorized", authReq)
	require.Equal(t, http.StatusOK, rec3.Code)
	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	assert.Equal(t, "DENY", resp3["decision"], "after DeletePolicy → DENY (cache invalidated)")
}
