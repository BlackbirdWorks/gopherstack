package verifiedpermissions_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
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

func TestVPHandler_BatchIsAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "batch authorization with requests",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"requests": []any{
						map[string]any{
							"principal": map[string]any{"entityType": "User", "entityId": "alice"},
							"action":    map[string]any{"actionType": "Action", "actionId": "view"},
							"resource":  map[string]any{"entityType": "Photo", "entityId": "photo1"},
						},
					},
				}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"requests": []any{},
				}
			},
			wantCode: http.StatusBadRequest,
			wantLen:  0,
		},
		{
			name: "non-existent policy store",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"policyStoreId": "nonexistent",
					"requests":      []any{},
				}
			},
			wantCode: http.StatusBadRequest,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			body := tt.setup(t, h)

			rec := doVPRequest(t, h, "BatchIsAuthorized", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantLen > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				results := resp["results"].([]any)
				assert.Len(t, results, tt.wantLen)

				first := results[0].(map[string]any)
				assert.Contains(t, []string{"ALLOW", "DENY"}, first["decision"])
			}
		})
	}
}

func TestVPHandler_BatchIsAuthorizedWithToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "with access token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"accessToken":   "fake-access-token",
					"requests": []any{
						map[string]any{
							"action":   map[string]any{"actionType": "Action", "actionId": "view"},
							"resource": map[string]any{"entityType": "Photo", "entityId": "photo1"},
						},
					},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "with identity token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"identityToken": "fake-identity-token",
					"requests":      []any{},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"requests":      []any{},
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"accessToken": "fake-token",
					"requests":    []any{},
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			body := tt.setup(t, h)

			rec := doVPRequest(t, h, "BatchIsAuthorizedWithToken", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_IsAuthorized_DeterminingPolicies_ObjectShape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": storeID,
		"definition": map[string]any{
			"static": map[string]any{"statement": "permit(principal, action, resource);"},
		},
	})
	require.Equal(t, http.StatusOK, policyRec.Code)

	var policyResp map[string]any
	require.NoError(t, json.Unmarshal(policyRec.Body.Bytes(), &policyResp))
	policyID := policyResp["policyId"].(string)

	rec := doVPRequest(t, h, "IsAuthorized", map[string]any{
		"policyStoreId": storeID,
		"principal":     map[string]any{"entityType": "User", "entityId": "alice"},
		"action":        map[string]any{"actionType": "Action", "actionId": "view"},
		"resource":      map[string]any{"entityType": "Resource", "entityId": "res1"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ALLOW", resp["decision"])

	determining := resp["determiningPolicies"].([]any)
	require.Len(t, determining, 1)
	item, ok := determining[0].(map[string]any)
	require.True(t, ok, "determiningPolicies elements must be objects, got: %v", determining[0])
	assert.Equal(t, policyID, item["policyId"])

	assert.Empty(t, resp["errors"])
}

// TestVPHandler_BatchIsAuthorized_RequestEcho_Shape verifies each result's
// echoed "request" nests principal/action/resource as objects (matching the
// real SDK's BatchIsAuthorizedInputItem), not the internal flat
// principalEntityType/actionType/... representation.

func TestVPHandler_BatchIsAuthorized_RequestEcho_Shape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "BatchIsAuthorized", map[string]any{
		"policyStoreId": storeID,
		"requests": []any{
			map[string]any{
				"principal": map[string]any{"entityType": "User", "entityId": "alice"},
				"action":    map[string]any{"actionType": "Action", "actionId": "view"},
				"resource":  map[string]any{"entityType": "Resource", "entityId": "res1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results := resp["results"].([]any)
	require.Len(t, results, 1)

	result := results[0].(map[string]any)
	request, ok := result["request"].(map[string]any)
	require.True(t, ok)

	principal, ok := request["principal"].(map[string]any)
	require.True(t, ok, "request.principal must be a nested object, got: %v", request)
	assert.Equal(t, "User", principal["entityType"])
	assert.Equal(t, "alice", principal["entityId"])

	action, ok := request["action"].(map[string]any)
	require.True(t, ok, "request.action must be a nested object, got: %v", request)
	assert.Equal(t, "Action", action["actionType"])
	assert.Equal(t, "view", action["actionId"])

	resource, ok := request["resource"].(map[string]any)
	require.True(t, ok, "request.resource must be a nested object, got: %v", request)
	assert.Equal(t, "Resource", resource["entityType"])
	assert.Equal(t, "res1", resource["entityId"])
}

func TestVPHandler_IsAuthorizedWithToken_PrincipalFromJWT(t *testing.T) {
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

func TestVPHandler_IsAuthorizedWithToken_MissingToken(t *testing.T) {
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

func TestVPHandler_IsAuthorized_PolicySetCacheDirtyOnMutation(t *testing.T) {
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

func TestVPHandler_IsAuthorized_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name: "allow on existing store",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
				"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusOK,
			wantField:  "decision",
		},
		{
			name: "missing policyStoreId",
			body: map[string]any{
				"action": map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown store",
			body: map[string]any{
				"policyStoreId": "unknown-store",
				"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			ps := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, ps.Code)

			var psResp map[string]any
			_ = json.NewDecoder(ps.Body).Decode(&psResp)

			if v, ok := tt.body["policyStoreId"]; ok && v == "__REPLACE__" {
				tt.body["policyStoreId"] = psResp["policyStoreId"]
			}

			rec := doVPRequest(t, h, "IsAuthorized", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

func TestVPHandler_IsAuthorizedWithToken_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "allow with access token",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
				"accessToken":   "eyJfake.token.here",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing token",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			body: map[string]any{
				"accessToken": "eyJfake.token.here",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			ps := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, ps.Code)

			var psResp map[string]any
			_ = json.NewDecoder(ps.Body).Decode(&psResp)

			if v, ok := tt.body["policyStoreId"]; ok && v == "__REPLACE__" {
				tt.body["policyStoreId"] = psResp["policyStoreId"]
			}

			rec := doVPRequest(t, h, "IsAuthorizedWithToken", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestVPHandler_IsAuthorizedWithToken_MultipleIdentitySources_MatchByIssuer
// locks in a principalFromToken improvement: with more than one identity
// source configured on a policy store, the principal's entity type must be
// resolved from whichever identity source's issuer matches the token's
// "iss" claim -- not always the first identity source created, which was
// gopherstack's previous (documented-simplification) behavior.
func TestVPHandler_IsAuthorizedWithToken_MultipleIdentitySources_MatchByIssuer(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	// First identity source created: issuer A -> principal type "Employee".
	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "Employee",
		"configuration": map[string]any{
			"openIdConnectConfiguration": map[string]any{
				"issuer": "https://issuer-a.example.com",
				"tokenSelection": map[string]any{
					"accessTokenOnly": map[string]any{"principalIdClaim": "sub"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	// Second identity source created: issuer B -> principal type "Customer".
	rec = doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "Customer",
		"configuration": map[string]any{
			"openIdConnectConfiguration": map[string]any{
				"issuer": "https://issuer-b.example.com",
				"tokenSelection": map[string]any{
					"accessTokenOnly": map[string]any{"principalIdClaim": "sub"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	// A permissive policy so the decision reflects which principal type was resolved.
	rec = doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": storeID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement": `permit(principal is Customer, action, resource);`,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	// A token from issuer B (the SECOND identity source created) must still
	// resolve to "Customer", not "Employee" (the first-created source).
	token := makeTestJWT(map[string]any{"sub": "cust-1", "iss": "https://issuer-b.example.com"})

	rec = doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
		"policyStoreId": storeID,
		"accessToken":   token,
		"action":        map[string]any{"actionType": "Action", "actionId": "view"},
		"resource":      map[string]any{"entityType": "Resource", "entityId": "res1"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ALLOW", resp["decision"],
		"token from issuer B must resolve to the Customer identity source, not the first-created Employee one")
}

// TestVPHandler_IsAuthorizedWithToken_ClientIDValidation locks in real AWS's
// documented client ID / audience validation (cognito-validation.html,
// oidc-validation.html): when an identity source has one or more configured
// client IDs/audiences, a token whose aud/client_id claim isn't among them
// must not resolve a principal from that source (DENY), matching the
// security direction real AWS takes -- a token meant for a different app
// must not be accepted just because the issuer matches.
func TestVPHandler_IsAuthorizedWithToken_ClientIDValidation(t *testing.T) {
	t.Parallel()

	cognitoIssuer := "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_test"
	cognitoArn := "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test"

	tests := []struct {
		isCfg   map[string]any
		claims  map[string]any
		name    string
		wantDec string
	}{
		{
			name: "cognito client id matches allows",
			isCfg: map[string]any{
				"cognitoUserPoolConfiguration": map[string]any{
					"userPoolArn": cognitoArn,
					"clientIds":   []string{"app-client-1"},
				},
			},
			claims:  map[string]any{"sub": "alice", "iss": cognitoIssuer, "client_id": "app-client-1"},
			wantDec: "ALLOW",
		},
		{
			name: "cognito client id mismatch denies",
			isCfg: map[string]any{
				"cognitoUserPoolConfiguration": map[string]any{
					"userPoolArn": cognitoArn,
					"clientIds":   []string{"app-client-1"},
				},
			},
			claims:  map[string]any{"sub": "alice", "iss": cognitoIssuer, "client_id": "some-other-client"},
			wantDec: "DENY",
		},
		{
			name: "cognito unconfigured client ids accepts any",
			isCfg: map[string]any{
				"cognitoUserPoolConfiguration": map[string]any{
					"userPoolArn": cognitoArn,
				},
			},
			claims:  map[string]any{"sub": "alice", "iss": cognitoIssuer, "client_id": "anything"},
			wantDec: "ALLOW",
		},
		{
			name: "oidc access token audience mismatch denies",
			isCfg: map[string]any{
				"openIdConnectConfiguration": map[string]any{
					"issuer": "https://issuer.example.com",
					"tokenSelection": map[string]any{
						"accessTokenOnly": map[string]any{
							"principalIdClaim": "sub",
							"audiences":        []string{"https://myapp.example.com"},
						},
					},
				},
			},
			claims: map[string]any{
				"sub": "alice",
				"iss": "https://issuer.example.com",
				"aud": "https://otherapp.example.com",
			},
			wantDec: "DENY",
		},
		{
			name: "oidc access token audience match allows",
			isCfg: map[string]any{
				"openIdConnectConfiguration": map[string]any{
					"issuer": "https://issuer.example.com",
					"tokenSelection": map[string]any{
						"accessTokenOnly": map[string]any{
							"principalIdClaim": "sub",
							"audiences":        []string{"https://myapp.example.com"},
						},
					},
				},
			},
			claims: map[string]any{
				"sub": "alice",
				"iss": "https://issuer.example.com",
				"aud": "https://myapp.example.com",
			},
			wantDec: "ALLOW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID := createTestPolicyStore(t, h)

			policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
				"policyStoreId": storeID,
				"definition": map[string]any{
					// Scoped to "principal is User" -- an unqualified
					// "permit(principal, ...)" would ALLOW even with no
					// principal resolved at all, defeating this test.
					"static": map[string]any{"statement": `permit(principal is User, action, resource);`},
				},
			})
			require.Equal(t, http.StatusOK, policyRec.Code, "policy body: %s", policyRec.Body.String())

			isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
				"policyStoreId":       storeID,
				"principalEntityType": "User",
				"configuration":       tt.isCfg,
			})
			require.Equal(t, http.StatusOK, isRec.Code, "identity source body: %s", isRec.Body.String())

			token := makeTestJWT(tt.claims)
			rec := doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
				"policyStoreId": storeID,
				"accessToken":   token,
				"action":        map[string]any{"actionType": "Action", "actionId": "view"},
				"resource":      map[string]any{"entityType": "Resource", "entityId": "res1"},
			})
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantDec, resp["decision"])
		})
	}
}

// TestVPHandler_IsAuthorizedWithToken_ResponseIncludesPrincipal locks in the
// real SDK's IsAuthorizedWithTokenOutput.Principal field (EntityIdentifier,
// optional): present with the resolved principal when the token yields one,
// absent when it doesn't (e.g. an unparseable token).
func TestVPHandler_IsAuthorizedWithToken_ResponseIncludesPrincipal(t *testing.T) {
	t.Parallel()

	t.Run("resolved token echoes principal", func(t *testing.T) {
		t.Parallel()

		h := newTestVPHandler(t)
		storeID := createTestPolicyStore(t, h)

		isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
			"policyStoreId":       storeID,
			"principalEntityType": "User",
			"configuration": map[string]any{
				"openIdConnectConfiguration": map[string]any{
					"issuer": "https://issuer.example.com",
					"tokenSelection": map[string]any{
						"accessTokenOnly": map[string]any{"principalIdClaim": "sub"},
					},
				},
			},
		})
		require.Equal(t, http.StatusOK, isRec.Code, "body: %s", isRec.Body.String())

		token := makeTestJWT(map[string]any{"sub": "alice", "iss": "https://issuer.example.com"})
		rec := doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
			"policyStoreId": storeID,
			"accessToken":   token,
			"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			"resource":      map[string]any{"entityType": "Resource", "entityId": "res1"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		principal, ok := resp["principal"].(map[string]any)
		require.True(t, ok, "response must echo the resolved principal, got: %v", resp)
		assert.Equal(t, "User", principal["entityType"])
		assert.Equal(t, "alice", principal["entityId"])
	})

	t.Run("unparseable token omits principal", func(t *testing.T) {
		t.Parallel()

		h := newTestVPHandler(t)
		storeID := createTestPolicyStore(t, h)

		rec := doVPRequest(t, h, "IsAuthorizedWithToken", map[string]any{
			"policyStoreId": storeID,
			"accessToken":   "not-a-jwt",
			"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			"resource":      map[string]any{"entityType": "Resource", "entityId": "res1"},
		})
		require.Equal(
			t,
			http.StatusOK,
			rec.Code,
			"malformed token must fail closed, not error: body: %s",
			rec.Body.String(),
		)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "DENY", resp["decision"])
		assert.NotContains(t, resp, "principal")
	})
}

// TestVPHandler_BatchIsAuthorizedWithToken_RequestEcho_Shape verifies each
// result's echoed "request" carries only action/resource (matching the real
// SDK's BatchIsAuthorizedWithTokenInputItem, which -- unlike
// BatchIsAuthorizedInputItem -- has no principal field: the principal comes
// from the token, not the per-item request), and that the resolved principal
// is instead echoed once at the top level (BatchIsAuthorizedWithTokenOutput.Principal).
func TestVPHandler_BatchIsAuthorizedWithToken_RequestEcho_Shape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "User",
		"configuration": map[string]any{
			"openIdConnectConfiguration": map[string]any{
				"issuer": "https://issuer.example.com",
				"tokenSelection": map[string]any{
					"accessTokenOnly": map[string]any{"principalIdClaim": "sub"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, isRec.Code, "body: %s", isRec.Body.String())

	token := makeTestJWT(map[string]any{"sub": "alice", "iss": "https://issuer.example.com"})
	rec := doVPRequest(t, h, "BatchIsAuthorizedWithToken", map[string]any{
		"policyStoreId": storeID,
		"accessToken":   token,
		"requests": []any{
			map[string]any{
				"action":   map[string]any{"actionType": "Action", "actionId": "view"},
				"resource": map[string]any{"entityType": "Resource", "entityId": "res1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	principal, ok := resp["principal"].(map[string]any)
	require.True(t, ok, "top-level principal must be echoed, got: %v", resp)
	assert.Equal(t, "User", principal["entityType"])
	assert.Equal(t, "alice", principal["entityId"])

	results := resp["results"].([]any)
	require.Len(t, results, 1)
	result := results[0].(map[string]any)
	request, ok := result["request"].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, request, "principal",
		"BatchIsAuthorizedWithTokenInputItem has no principal field -- it must not be echoed per-item")
}
