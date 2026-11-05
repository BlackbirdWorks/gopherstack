package cognitoidentity_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestHandler_SetGetIdentityPoolRoles_WithRoleMappings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-mappings-handler-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))
	poolID := created["IdentityPoolId"].(string)

	setRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles": map[string]string{
			"authenticated":   "arn:aws:iam::000000000000:role/Auth",
			"unauthenticated": "arn:aws:iam::000000000000:role/Unauth",
		},
		"RoleMappings": map[string]any{
			"accounts.google.com": map[string]any{
				"Type":                    "Token",
				"AmbiguousRoleResolution": "AuthenticatedRole",
			},
		},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out, jsonx.V1()))

	roles, ok := out["Roles"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::000000000000:role/Auth", roles["authenticated"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/Unauth", roles["unauthenticated"])

	roleMappings, ok := out["RoleMappings"].(map[string]any)
	require.True(t, ok, "RoleMappings must be present in GetIdentityPoolRoles response")
	require.Contains(t, roleMappings, "accounts.google.com")

	mapping, _ := roleMappings["accounts.google.com"].(map[string]any)
	assert.Equal(t, "Token", mapping["Type"])
	assert.Equal(t, "AuthenticatedRole", mapping["AmbiguousRoleResolution"])
}

func TestHandler_SetGetIdentityPoolRoles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "roles-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))

	setRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Roles": map[string]string{
			"authenticated":   "arn:aws:iam::000000000000:role/AuthRole",
			"unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
		},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out, jsonx.V1()))

	roles, ok := out["Roles"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthRole", roles["authenticated"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/UnauthRole", roles["unauthenticated"])
}

func TestHandler_SetIdentityPoolRoles_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "empty_pool_id",
			body: map[string]any{
				"IdentityPoolId": "",
				"Roles": map[string]string{
					"authenticated": "arn:aws:iam::000000000000:role/Auth",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "no_roles",
			body: map[string]any{
				"IdentityPoolId": "us-east-1:some-pool-id",
				"Roles":          map[string]string{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "pool_not_found",
			body: map[string]any{
				"IdentityPoolId": "us-east-1:nonexistent",
				"Roles": map[string]string{
					"authenticated": "arn:aws:iam::000000000000:role/Auth",
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetIdentityPoolRoles_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetPrincipalTagAttributeMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		poolID       string
		wantCode     int
		wantDefaults bool
	}{
		{name: "returns_defaults_when_not_set", wantCode: http.StatusOK, wantDefaults: true},
		{name: "pool_not_found", poolID: "us-east-1:nonexistent", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.poolID

			if tt.name == "returns_defaults_when_not_set" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "principal-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))
				poolID = created["IdentityPoolId"].(string)
			}

			rec := doCognitoIdentityRequest(t, h, "GetPrincipalTagAttributeMap", map[string]any{
				"IdentityPoolId":       poolID,
				"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))
				assert.Equal(t, tt.wantDefaults, out["UseDefaults"])
			}
		})
	}
}

func TestHandler_SetPrincipalTagAttributeMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{
			name: "pool_not_found",
			body: map[string]any{
				"IdentityPoolId":       "us-east-1:nonexistent",
				"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
				"UseDefaults":          false,
				"PrincipalTags": map[string]string{
					"sub": "user_id",
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "principal-tags-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))
				poolID := created["IdentityPoolId"].(string)

				body = map[string]any{
					"IdentityPoolId":       poolID,
					"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
					"UseDefaults":          false,
					"PrincipalTags": map[string]string{
						"sub": "user_id",
					},
				}
			}

			rec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))
				assert.Equal(t, false, out["UseDefaults"])

				tags, _ := out["PrincipalTags"].(map[string]any)
				assert.Equal(t, "user_id", tags["sub"])
			}
		})
	}
}

func TestHandler_SetGetPrincipalTagAttributeMap_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "roundtrip-principal-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))
	poolID := created["IdentityPoolId"].(string)

	providerName := "cognito-idp.us-east-1.amazonaws.com/us-east-1_Pool1"

	setRec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": providerName,
		"UseDefaults":          false,
		"PrincipalTags": map[string]string{
			"sub":   "cognito:username",
			"email": "email",
		},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doCognitoIdentityRequest(t, h, "GetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": providerName,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out, jsonx.V1()))
	assert.Equal(t, poolID, out["IdentityPoolId"])
	assert.Equal(t, providerName, out["IdentityProviderName"])
	assert.Equal(t, false, out["UseDefaults"])

	tags, _ := out["PrincipalTags"].(map[string]any)
	assert.Equal(t, "cognito:username", tags["sub"])
	assert.Equal(t, "email", tags["email"])
}

func TestHandler_SetIdentityPoolRoles_SingleRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "single-role-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))

	rec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Roles": map[string]string{
			"authenticated": "arn:aws:iam::000000000000:role/AuthRole",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetPrincipalTagAttributeMap_EmptyProvider_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "ptag-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created, jsonx.V1()))

	rec := doCognitoIdentityRequest(t, h, "GetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       created["IdentityPoolId"],
		"IdentityProviderName": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetIdentityPoolRoles_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetIdentityPoolRoles_OmitsEmptyRoles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pool and set only the authenticated role.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-omit-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(createRec.Body), &createOut, jsonx.V1()))
	poolID := createOut["IdentityPoolId"].(string)

	setRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles": map[string]string{
			"authenticated": "arn:aws:iam::000000000000:role/Auth",
		},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(getRec.Body), &out, jsonx.V1()))
	roles := out["Roles"].(map[string]any)

	assert.Contains(t, roles, "authenticated")
	assert.NotContains(t, roles, "unauthenticated", "empty unauthenticated role must be omitted")
}

func TestHandler_SetIdentityPoolRoles_PreservesExistingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-preserve-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(createRec.Body), &createOut, jsonx.V1()))
	poolID := createOut["IdentityPoolId"].(string)

	// Set both roles.
	doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles": map[string]string{
			"authenticated":   "arn:aws:iam::000000000000:role/Auth",
			"unauthenticated": "arn:aws:iam::000000000000:role/Unauth",
		},
	})

	// Update only authenticated.
	doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles": map[string]string{
			"authenticated": "arn:aws:iam::000000000000:role/AuthV2",
		},
	})

	getRec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(getRec.Body), &out, jsonx.V1()))
	roles := out["Roles"].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthV2", roles["authenticated"])
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/Unauth",
		roles["unauthenticated"],
		"unauthenticated role must be preserved after partial update",
	)
}
