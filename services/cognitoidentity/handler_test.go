package cognitoidentity_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func newTestHandler(t *testing.T) *cognitoidentity.Handler {
	t.Helper()

	backend := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	return cognitoidentity.NewHandler(backend, "us-east-1")
}

func doCognitoIdentityRequest(
	t *testing.T,
	h *cognitoidentity.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CognitoIdentity", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateIdentityPool", "DeleteIdentityPool", "DescribeIdentityPool",
		"ListIdentityPools", "UpdateIdentityPool",
		"GetId", "GetCredentialsForIdentity", "GetOpenIdToken",
		"SetIdentityPoolRoles", "GetIdentityPoolRoles",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching_target",
			target: "AWSCognitoIdentityService.CreateIdentityPool",
			want:   true,
		},
		{
			name:   "non_matching_target",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			want:   false,
		},
		{
			name:   "empty_target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "cognito-identity", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_CreateIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolName string
		wantCode int
	}{
		{
			name:     "success",
			poolName: "my-identity-pool",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name",
			poolName: "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               tt.poolName,
				"AllowUnauthenticatedIdentities": true,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.poolName, out["IdentityPoolName"])
				assert.NotEmpty(t, out["IdentityPoolId"])
			}
		})
	}
}

func TestHandler_DeleteIdentityPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		notFound bool
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest, notFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var poolID string

			if !tt.notFound {
				rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "del-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				poolID = out["IdentityPoolId"].(string)
			} else {
				poolID = "us-east-1:nonexistent"
			}

			rec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
				"IdentityPoolId": poolID,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeIdentityPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "desc-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "desc-pool", out["IdentityPoolName"])
}

func TestHandler_ListIdentityPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pool-1", "pool-2"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	pools, ok := out["IdentityPools"].([]any)
	require.True(t, ok)
	assert.Len(t, pools, 2)
}

func TestHandler_UpdateIdentityPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "update-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 created["IdentityPoolId"],
		"IdentityPoolName":               "update-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, false, out["AllowUnauthenticatedIdentities"])
}

func TestHandler_GetID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "getid-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
		"Logins":         map[string]string{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["IdentityId"])
}

func TestHandler_GetCredentialsForIdentity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "creds-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, idRec.Code)

	var idOut map[string]any
	require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

	rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", map[string]any{
		"IdentityId": idOut["IdentityId"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["IdentityId"])

	creds, ok := out["Credentials"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, creds["AccessKeyId"])
	assert.NotEmpty(t, creds["SecretKey"])
	assert.NotEmpty(t, creds["SessionToken"])
}

func TestHandler_GetOpenIDToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "oidc-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, idRec.Code)

	var idOut map[string]any
	require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

	rec := doCognitoIdentityRequest(t, h, "GetOpenIdToken", map[string]any{
		"IdentityId": idOut["IdentityId"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["Token"])
	assert.Equal(t, idOut["IdentityId"], out["IdentityId"])
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
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

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
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

	roles, ok := out["Roles"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthRole", roles["authenticated"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/UnauthRole", roles["unauthenticated"])
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "NonExistentAction", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid_action",
			target: "AWSCognitoIdentityService.CreateIdentityPool",
			want:   "CreateIdentityPool",
		},
		{
			name:   "empty_target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "no_prefix",
			target: "SomeOtherService.SomeAction",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with_identity_pool_id",
			body: `{"IdentityPoolId":"us-east-1:abc123"}`,
			want: "us-east-1:abc123",
		},
		{
			name: "with_identity_id",
			body: `{"IdentityId":"us-east-1:ident456"}`,
			want: "us-east-1:ident456",
		},
		{
			name: "empty_body",
			body: `{}`,
			want: "",
		},
		{
			name: "invalid_json",
			body: `not-json`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_DescribeIdentityPool_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateIdentityPool_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 "us-east-1:nonexistent",
		"IdentityPoolName":               "new-name",
		"AllowUnauthenticatedIdentities": false,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetID_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetCredentialsForIdentity_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", map[string]any{
		"IdentityId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetOpenIDToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetOpenIdToken", map[string]any{
		"IdentityId": "us-east-1:nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`not-valid-json`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityService.CreateIdentityPool")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_WithProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "provider-pool",
		"AllowUnauthenticatedIdentities": false,
		"CognitoIdentityProviders": []map[string]any{
			{
				"ProviderName":         "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
				"ClientId":             "client123",
				"ServerSideTokenCheck": true,
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "provider-pool", out["IdentityPoolName"])

	providers, ok := out["CognitoIdentityProviders"].([]any)
	require.True(t, ok)
	assert.Len(t, providers, 1)
}
