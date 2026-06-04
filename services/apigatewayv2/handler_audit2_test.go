package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestAudit2_VpcLinkSecurityGroupIDsNullBug verifies that VpcLink responses always
// include "securityGroupIds" as [] when no security groups are provided.
// AWS always returns securityGroupIds:[] even when the field is empty.
func TestAudit2_VpcLinkSecurityGroupIDsNullBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantSecGroups []string
	}{
		{
			name:          "no_security_groups_returns_empty_array",
			body:          map[string]any{"name": "my-link", "subnetIds": []string{"subnet-abc"}},
			wantSecGroups: []string{},
		},
		{
			name: "with_security_groups_returns_them",
			body: map[string]any{
				"name":             "my-link-2",
				"subnetIds":        []string{"subnet-abc"},
				"securityGroupIds": []string{"sg-111", "sg-222"},
			},
			wantSecGroups: []string{"sg-111", "sg-222"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/vpclinks", tt.body)
			require.Equal(t, http.StatusCreated, rr.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

			_, ok := raw["securityGroupIds"]
			assert.True(t, ok, "securityGroupIds key must always be present in VpcLink response")

			var link apigatewayv2.VpcLink
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &link))
			assert.Equal(t, tt.wantSecGroups, link.SecurityGroupIDs)
		})
	}
}

// TestAudit2_VpcLinkSubnetIDsNullBug verifies that SubnetIDs is always present in
// VpcLink responses (never absent via omitempty).
func TestAudit2_VpcLinkSubnetIDsNullBug(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rr := doRequest(t, h, http.MethodPost, "/v2/vpclinks", map[string]any{
		"name":      "test-link",
		"subnetIds": []string{"subnet-aaa", "subnet-bbb"},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

	_, ok := raw["subnetIds"]
	assert.True(t, ok, "subnetIds key must always be present in VpcLink response")
}

// TestAudit2_DomainNameConfigurationsNullBug verifies that DomainName responses always
// include "domainNameConfigurations" as [] when no configurations are provided.
// AWS always returns domainNameConfigurations:[] even when empty.
func TestAudit2_DomainNameConfigurationsNullBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantConfigsEmpty bool
	}{
		{
			name:             "no_configs_returns_empty_array",
			body:             map[string]any{"domainName": "api.no-configs.example.com"},
			wantConfigsEmpty: true,
		},
		{
			name: "with_configs_returns_them",
			body: map[string]any{
				"domainName": "api.with-configs.example.com",
				"domainNameConfigurations": []map[string]any{
					{
						"certificateArn": "arn:aws:acm:us-east-1:123:certificate/abc",
						"endpointType":   "REGIONAL",
					},
				},
			},
			wantConfigsEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)
			require.Equal(t, http.StatusCreated, rr.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

			_, hasKey := raw["domainNameConfigurations"]
			assert.True(t, hasKey, "domainNameConfigurations key must always be present in DomainName response")

			var dn apigatewayv2.DomainName
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))

			if tt.wantConfigsEmpty {
				assert.Empty(t, dn.DomainNameConfigurations,
					"domainNameConfigurations should be empty array, not absent")
			} else {
				assert.NotEmpty(t, dn.DomainNameConfigurations)
			}
		})
	}
}

// TestAudit2_UpdateRouteClearsAuthorizerIDOnNone verifies that when UpdateRoute
// patches AuthorizationType to "NONE", the AuthorizerID is cleared.
// AWS clears authorizerId when authorizationType is set to NONE.
func TestAudit2_UpdateRouteClearsAuthorizerIDOnNone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		patchBody       map[string]any
		wantAuthType    string
		wantAuthIDEmpty bool
	}{
		{
			name:            "patch_to_none_clears_authorizer_id",
			patchBody:       map[string]any{"authorizationType": "NONE"},
			wantAuthType:    "NONE",
			wantAuthIDEmpty: true,
		},
		{
			name:            "patch_to_jwt_preserves_authorizer_id",
			patchBody:       map[string]any{"authorizationType": "JWT", "authorizerId": "new-auth-id"},
			wantAuthType:    "JWT",
			wantAuthIDEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := createAPI(t, h, "test-api")

			// Create a JWT authorizer first so we have a valid authorizer ID.
			authRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", map[string]any{
				"name":           "my-authorizer",
				"authorizerType": "JWT",
				"identitySource": []string{"$request.header.Authorization"},
				"jwtConfiguration": map[string]any{
					"issuer":   "https://example.com",
					"audience": []string{"my-app"},
				},
			})
			require.Equal(t, http.StatusCreated, authRR.Code)

			var authResult map[string]any
			require.NoError(t, json.Unmarshal(authRR.Body.Bytes(), &authResult))
			authID := authResult["authorizerId"].(string)

			// Create a route with JWT authorization.
			routeRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey":          "GET /items",
				"authorizationType": "JWT",
				"authorizerId":      authID,
			})
			require.Equal(t, http.StatusCreated, routeRR.Code)

			var routeResult map[string]any
			require.NoError(t, json.Unmarshal(routeRR.Body.Bytes(), &routeResult))
			routeID := routeResult["routeId"].(string)

			// Patch the route with the test body.
			patchBody := tt.patchBody
			if !tt.wantAuthIDEmpty {
				// Keep authorizer ID for non-NONE test.
				patchBody["authorizerId"] = authID
			}

			patchRR := doRequest(t, h, http.MethodPatch, "/v2/apis/"+apiID+"/routes/"+routeID, patchBody)
			require.Equal(t, http.StatusOK, patchRR.Code)

			var updated map[string]any
			require.NoError(t, json.Unmarshal(patchRR.Body.Bytes(), &updated))

			assert.Equal(t, tt.wantAuthType, updated["authorizationType"])

			if tt.wantAuthIDEmpty {
				authIDVal, hasKey := updated["authorizerId"]
				if hasKey {
					assert.Empty(t, authIDVal,
						"authorizerId should be empty after setting authorizationType to NONE")
				}
			}
		})
	}
}

// TestAudit2_UpdateRouteClearsAuthorizerIDRoundtrip verifies the full round-trip:
// create route with JWT, patch to NONE, GET should show empty authorizerId.
func TestAudit2_UpdateRouteClearsAuthorizerIDRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "roundtrip-api")

	// Create authorizer.
	authRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", map[string]any{
		"name":           "rt-authorizer",
		"authorizerType": "JWT",
		"identitySource": []string{"$request.header.Authorization"},
		"jwtConfiguration": map[string]any{
			"issuer":   "https://example.com",
			"audience": []string{"my-app"},
		},
	})
	require.Equal(t, http.StatusCreated, authRR.Code)

	var authResult map[string]any
	require.NoError(t, json.Unmarshal(authRR.Body.Bytes(), &authResult))
	authID := authResult["authorizerId"].(string)

	// Create route with JWT auth.
	routeRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
		"routeKey":          "POST /orders",
		"authorizationType": "JWT",
		"authorizerId":      authID,
	})
	require.Equal(t, http.StatusCreated, routeRR.Code)

	var routeResult map[string]any
	require.NoError(t, json.Unmarshal(routeRR.Body.Bytes(), &routeResult))
	routeID := routeResult["routeId"].(string)

	// Verify initial state: JWT auth with authorizer ID set.
	assert.Equal(t, "JWT", routeResult["authorizationType"])
	assert.Equal(t, authID, routeResult["authorizerId"])

	// Patch to NONE.
	patchRR := doRequest(t, h, http.MethodPatch, "/v2/apis/"+apiID+"/routes/"+routeID,
		map[string]any{"authorizationType": "NONE"})
	require.Equal(t, http.StatusOK, patchRR.Code)

	// GET route and verify authorizerId is cleared.
	getRR := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/routes/"+routeID, nil)
	require.Equal(t, http.StatusOK, getRR.Code)

	var getResult map[string]any
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &getResult))

	assert.Equal(t, "NONE", getResult["authorizationType"])
	assert.Empty(t, getResult["authorizerId"], "authorizerId should be empty after UpdateRoute to NONE")
}

// TestAudit2_VpcLinkGetReturnsArrayFields verifies that GetVpcLink also returns
// securityGroupIds and subnetIds as arrays.
func TestAudit2_VpcLinkGetReturnsArrayFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRR := doRequest(t, h, http.MethodPost, "/v2/vpclinks", map[string]any{
		"name":      "get-test-link",
		"subnetIds": []string{"subnet-xyz"},
	})
	require.Equal(t, http.StatusCreated, createRR.Code)

	var created apigatewayv2.VpcLink
	require.NoError(t, json.Unmarshal(createRR.Body.Bytes(), &created))

	getRR := doRequest(t, h, http.MethodGet, "/v2/vpclinks/"+created.VpcLinkID, nil)
	require.Equal(t, http.StatusOK, getRR.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &raw))

	_, hasSec := raw["securityGroupIds"]
	assert.True(t, hasSec, "securityGroupIds key must be present in GetVpcLink response")

	_, hasSub := raw["subnetIds"]
	assert.True(t, hasSub, "subnetIds key must be present in GetVpcLink response")
}
