package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVPHandler_IdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*testing.T, *verifiedpermissions.Handler) (string, string)
		check     func(*testing.T, map[string]any)
		name      string
		operation string
		wantCode  int
	}{
		{
			name:      "create with Cognito config",
			operation: "CreateIdentitySource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), ""
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				assert.NotEmpty(t, resp["identitySourceId"])
				assert.NotEmpty(t, resp["policyStoreId"])
			},
		},
		{
			name:      "create with OIDC config",
			operation: "CreateIdentitySource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "oidc"
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				assert.NotEmpty(t, resp["identitySourceId"])
			},
		},
		{
			name:      "create missing policyStoreId",
			operation: "CreateIdentitySource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) (string, string) {
				return "", ""
			},
			wantCode: http.StatusBadRequest,
			check:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, configType := tt.setup(t, h)

			var body map[string]any

			switch {
			case storeID == "":
				body = map[string]any{
					"configuration": map[string]any{},
				}
			case configType == "oidc":
				body = map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"openIdConnectConfiguration": map[string]any{
							"issuer": "https://example.com",
						},
					},
				}
			default:
				body = map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
							"clientIds":   []string{"client1"},
						},
					},
				}
			}

			rec := doVPRequest(t, h, tt.operation, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

func TestVPHandler_GetIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) (string, string)
		name     string
		wantCode int
	}{
		{
			name: "get existing identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				storeID := createTestPolicyStore(t, h)
				rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var isResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &isResp))

				return storeID, isResp["identitySourceId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get non-existent identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing identitySourceId",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, isID := tt.setup(t, h)

			rec := doVPRequest(t, h, "GetIdentitySource", map[string]any{
				"policyStoreId":    storeID,
				"identitySourceId": isID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_DeleteIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) (string, string)
		name     string
		wantCode int
	}{
		{
			name: "delete existing identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				storeID := createTestPolicyStore(t, h)
				rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var isResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &isResp))

				return storeID, isResp["identitySourceId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, isID := tt.setup(t, h)

			rec := doVPRequest(t, h, "DeleteIdentitySource", map[string]any{
				"policyStoreId":    storeID,
				"identitySourceId": isID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListIdentitySources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numSrcs   int
		wantCode  int
		wantCount int
	}{
		{
			name:      "list empty",
			numSrcs:   0,
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name:      "list with identity sources",
			numSrcs:   2,
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID := createTestPolicyStore(t, h)

			for range tt.numSrcs {
				doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
			}

			rec := doVPRequest(t, h, "ListIdentitySources", map[string]any{"policyStoreId": storeID})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			sources := resp["identitySources"].([]any)
			assert.Len(t, sources, tt.wantCount)
		})
	}
}

func TestVPHandler_CreateIdentitySource_MissingConfig(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration":       map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_CreateIdentitySource_MissingUserPoolArn(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "",
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_CreateIdentitySource_OIDCIdentityTokenClientIDs(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"openIdConnectConfiguration": map[string]any{
				"issuer": "https://example.com",
				"tokenSelection": map[string]any{
					"identityTokenOnly": map[string]any{
						"principalIdClaim": "sub",
						"clientIds":        []string{"client-a", "client-b"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotContains(t, createResp, "configuration",
		"CreateIdentitySourceOutput has no configuration field in the real SDK")

	// The real SDK only echoes configuration back on GetIdentitySource (and
	// ListIdentitySources), not on CreateIdentitySource itself.
	rec = doVPRequest(t, h, "GetIdentitySource", map[string]any{
		"policyStoreId":    storeID,
		"identitySourceId": createResp["identitySourceId"],
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	oidc := resp["configuration"].(map[string]any)["openIdConnectConfiguration"].(map[string]any)
	tokenSelection := oidc["tokenSelection"].(map[string]any)
	identityTokenOnly, ok := tokenSelection["identityTokenOnly"].(map[string]any)
	require.True(t, ok, "expected identityTokenOnly in response, got: %v", tokenSelection)

	clientIDs, ok := identityTokenOnly["clientIds"].([]any)
	require.True(t, ok, "expected identityTokenOnly.clientIds, got: %v", identityTokenOnly)
	assert.ElementsMatch(t, []any{"client-a", "client-b"}, clientIDs)
	assert.NotContains(t, identityTokenOnly, "audiences", "identityTokenOnly must not use the audiences key")
}

// TestVPHandler_CreateIdentitySource_CognitoIssuer verifies the response
// includes configuration.cognitoUserPoolConfiguration.issuer, a required
// field in the real SDK that AWS derives from the user pool ARN.

func TestVPHandler_CreateIdentitySource_CognitoIssuer(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotContains(t, createResp, "configuration",
		"CreateIdentitySourceOutput has no configuration field in the real SDK")

	// The real SDK only echoes configuration (and thus the derived issuer)
	// back on GetIdentitySource, not on CreateIdentitySource itself.
	rec = doVPRequest(t, h, "GetIdentitySource", map[string]any{
		"policyStoreId":    storeID,
		"identitySourceId": createResp["identitySourceId"],
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cognito := resp["configuration"].(map[string]any)["cognitoUserPoolConfiguration"].(map[string]any)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_test", cognito["issuer"])
}

// TestVPHandler_ListIdentitySources_FilterByPrincipalEntityType verifies the
// wire "filters" list narrows results by principalEntityType.

func TestVPHandler_ListIdentitySources_FilterByPrincipalEntityType(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	for _, principalType := range []string{"MyCorp::User", "MyCorp::Service"} {
		rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
			"policyStoreId":       storeID,
			"principalEntityType": principalType,
			"configuration": map[string]any{
				"cognitoUserPoolConfiguration": map[string]any{
					"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doVPRequest(t, h, "ListIdentitySources", map[string]any{
		"policyStoreId": storeID,
		"filters": []any{
			map[string]any{"principalEntityType": "MyCorp::User"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sources := resp["identitySources"].([]any)
	require.Len(t, sources, 1)
	assert.Equal(t, "MyCorp::User", sources[0].(map[string]any)["principalEntityType"])
}

// TestVPHandler_ListPolicies_FilterByPrincipalIdentifier verifies the
// filter.principal wire shape is the EntityReference union
// ({"identifier": {...}} or {"unspecified": true}), not a flat
// entityIdentifier.

func TestVPHandler_UpdateIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildBody  func(psID, isID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update cognito config",
			buildBody: func(psID, isID string) map[string]any {
				return map[string]any{
					"policyStoreId":    psID,
					"identitySourceId": isID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing policyStoreId",
			buildBody: func(_, isID string) map[string]any {
				return map[string]any{
					"identitySourceId": isID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing identitySourceId",
			buildBody: func(psID, _ string) map[string]any {
				return map[string]any{
					"policyStoreId": psID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing updateConfiguration",
			buildBody: func(psID, isID string) map[string]any {
				return map[string]any{
					"policyStoreId":    psID,
					"identitySourceId": isID,
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)

			psRec := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, psRec.Code)

			var psResp map[string]any
			require.NoError(t, json.NewDecoder(psRec.Body).Decode(&psResp))

			psID := psResp["policyStoreId"].(string)

			isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
				"policyStoreId":       psID,
				"principalEntityType": "User",
				"configuration": map[string]any{
					"cognitoUserPoolConfiguration": map[string]any{
						"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/original",
					},
				},
			})
			require.Equal(t, http.StatusOK, isRec.Code)

			var isResp map[string]any
			require.NoError(t, json.NewDecoder(isRec.Body).Decode(&isResp))

			isID := isResp["identitySourceId"].(string)

			rec := doVPRequest(t, h, "UpdateIdentitySource", tt.buildBody(psID, isID))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestVPHandler_CreateUpdateIdentitySource_WireShape locks in a wire-shape
// bug fix: the real SDK's CreateIdentitySourceOutput and
// UpdateIdentitySourceOutput are both minimal (id/policyStoreId/timestamps
// only) -- neither echoes principalEntityType or configuration, unlike
// GetIdentitySource/ListIdentitySources' fuller item shape. gopherstack
// previously returned the full shape (with those two extra fields) from
// both Create and Update.
func TestVPHandler_CreateUpdateIdentitySource_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	createBody := map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/wire-shape",
			},
		},
	}

	createRec := doVPRequest(t, h, "CreateIdentitySource", createBody)
	require.Equal(t, http.StatusOK, createRec.Code, "body: %s", createRec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	assert.NotContains(t, createResp, "principalEntityType")
	assert.NotContains(t, createResp, "configuration")
	assert.NotEmpty(t, createResp["identitySourceId"])
	assert.NotEmpty(t, createResp["policyStoreId"])
	assert.NotEmpty(t, createResp["createdDate"])
	assert.NotEmpty(t, createResp["lastUpdatedDate"])

	updateRec := doVPRequest(t, h, "UpdateIdentitySource", map[string]any{
		"policyStoreId":    storeID,
		"identitySourceId": createResp["identitySourceId"],
		"updateConfiguration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/wire-shape-2",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "body: %s", updateRec.Body.String())

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.NotContains(t, updateResp, "principalEntityType")
	assert.NotContains(t, updateResp, "configuration")
	assert.Equal(t, createResp["identitySourceId"], updateResp["identitySourceId"])
}
