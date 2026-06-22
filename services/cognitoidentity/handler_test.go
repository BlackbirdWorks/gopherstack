package cognitoidentity_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestHandler_DeleteIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "success_deletes_identities",
			body: nil, // set in test
			// wantLen 0 unprocessed = success
			wantCode: http.StatusOK,
			wantLen:  0,
		},
		{
			name: "empty_ids_rejected",
			body: map[string]any{
				"IdentityIdsToDelete": []string{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "nonexistent_ids_silently_ignored",
			body: map[string]any{
				"IdentityIdsToDelete": []string{"us-east-1:nonexistent"},
			},
			wantCode: http.StatusOK,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := tt.body

			if tt.name == "success_deletes_identities" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "del-id-pool",
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

				body = map[string]any{
					"IdentityIdsToDelete": []string{idOut["IdentityId"].(string)},
				}
			}

			rec := doCognitoIdentityRequest(t, h, "DeleteIdentities", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				unprocessed, _ := out["UnprocessedIdentityIds"].([]any)
				assert.Len(t, unprocessed, tt.wantLen)
			}
		})
	}
}

func TestHandler_DescribeIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identityID string
		wantCode   int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", identityID: "us-east-1:nonexistent", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			identityID := tt.identityID

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "desc-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

				idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
					"AccountId":      "000000000000",
					"IdentityPoolId": created["IdentityPoolId"],
					"Logins": map[string]string{
						"accounts.google.com": "google-token-123",
					},
				})
				require.Equal(t, http.StatusOK, idRec.Code)

				var idOut map[string]any
				require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))
				identityID = idOut["IdentityId"].(string)
			}

			rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
				"IdentityId": identityID,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, identityID, out["IdentityId"])
				assert.NotEmpty(t, out["CreationDate"])
				logins, _ := out["Logins"].([]any)
				assert.Contains(t, logins, "accounts.google.com")
			}
		})
	}
}

func TestHandler_GetOpenIdTokenForDeveloperIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{name: "success_new_identity", wantCode: http.StatusOK},
		{
			name: "pool_not_found",
			body: map[string]any{
				"IdentityPoolId": "us-east-1:nonexistent",
				"Logins": map[string]string{
					"developer.example.com": "user-001",
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

			if tt.name == "success_new_identity" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "dev-token-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

				body = map[string]any{
					"IdentityPoolId": created["IdentityPoolId"],
					"Logins": map[string]string{
						"developer.example.com": "user-001",
					},
					"TokenDuration": 86400,
				}
			}

			rec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["IdentityId"])
				assert.NotEmpty(t, out["Token"])
			}
		})
	}
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
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID = created["IdentityPoolId"].(string)
			}

			rec := doCognitoIdentityRequest(t, h, "GetPrincipalTagAttributeMap", map[string]any{
				"IdentityPoolId":       poolID,
				"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantDefaults, out["UseDefaults"])
			}
		})
	}
}

func TestHandler_ListIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolID   string
		wantCode int
		wantLen  int
	}{
		{name: "success_with_identities", wantCode: http.StatusOK, wantLen: 2},
		{name: "pool_not_found", poolID: "us-east-1:nonexistent", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.poolID

			if tt.name == "success_with_identities" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "list-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID = created["IdentityPoolId"].(string)

				for _, login := range []map[string]string{
					{"provider.a.com": "token1"},
					{"provider.b.com": "token2"},
				} {
					idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
						"AccountId":      "000000000000",
						"IdentityPoolId": poolID,
						"Logins":         login,
					})
					require.Equal(t, http.StatusOK, idRec.Code)
				}
			}

			rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
				"IdentityPoolId": poolID,
				"MaxResults":     10,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, poolID, out["IdentityPoolId"])

				identities, _ := out["Identities"].([]any)
				assert.Len(t, identities, tt.wantLen)
			}
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{
			name:     "not_found",
			arn:      "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.arn

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "tags-pool",
					"AllowUnauthenticatedIdentities": true,
					"IdentityPoolTags": map[string]string{
						"env": "test",
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID := created["IdentityPoolId"].(string)
				arn = "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID
			}

			rec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": arn,
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "test", tags["env"])
			}
		})
	}
}

func TestHandler_TagResource_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tagging-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	// TagResource.
	tagRec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags": map[string]string{
			"team":  "backend",
			"owner": "alice",
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Verify tags were set.
	listRec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags, _ := listOut["Tags"].(map[string]any)
	assert.Equal(t, "backend", tags["team"])
	assert.Equal(t, "alice", tags["owner"])

	// UntagResource.
	untagRec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"owner"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Verify tag removed.
	listRec2 := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listOut2))
	tags2, _ := listOut2["Tags"].(map[string]any)
	assert.Equal(t, "backend", tags2["team"])
	assert.NotContains(t, tags2, "owner")
}

func TestHandler_LookupDeveloperIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{name: "lookup_by_developer_user_id", wantCode: http.StatusOK},
		{name: "lookup_by_identity_id", wantCode: http.StatusOK},
		{
			name: "pool_not_found",
			body: map[string]any{
				"IdentityPoolId":          "us-east-1:nonexistent",
				"DeveloperUserIdentifier": "user-001",
				"DeveloperProviderName":   "developer.example.com",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "no_identifier_provided",
			body:     nil, // will be set below with no IdentityId or DeveloperUserIdentifier
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var poolID, identityID string

			if tt.name == "lookup_by_developer_user_id" || tt.name == "lookup_by_identity_id" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "lookup-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID = created["IdentityPoolId"].(string)

				devRec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
					"IdentityPoolId": poolID,
					"Logins": map[string]string{
						"developer.example.com": "user-001",
					},
				})
				require.Equal(t, http.StatusOK, devRec.Code)

				var devOut map[string]any
				require.NoError(t, json.Unmarshal(devRec.Body.Bytes(), &devOut))
				identityID = devOut["IdentityId"].(string)
			}

			body := tt.body

			switch tt.name {
			case "lookup_by_developer_user_id":
				body = map[string]any{
					"IdentityPoolId":          poolID,
					"DeveloperUserIdentifier": "user-001",
					"DeveloperProviderName":   "developer.example.com",
				}
			case "lookup_by_identity_id":
				body = map[string]any{
					"IdentityPoolId":        poolID,
					"IdentityId":            identityID,
					"DeveloperProviderName": "developer.example.com",
				}
			case "no_identifier_provided":
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "no-id-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

				body = map[string]any{
					"IdentityPoolId": created["IdentityPoolId"],
				}
			}

			rec := doCognitoIdentityRequest(t, h, "LookupDeveloperIdentity", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["IdentityId"])
			}
		})
	}
}

func TestHandler_MergeDeveloperIdentities(t *testing.T) {
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
				"IdentityPoolId":            "us-east-1:nonexistent",
				"SourceUserIdentifier":      "user-src",
				"DestinationUserIdentifier": "user-dst",
				"DeveloperProviderName":     "developer.example.com",
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
					"IdentityPoolName":               "merge-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID := created["IdentityPoolId"].(string)

				for _, userID := range []string{"user-src", "user-dst"} {
					devRec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
						"IdentityPoolId": poolID,
						"Logins": map[string]string{
							"developer.example.com": userID,
						},
					})
					require.Equal(t, http.StatusOK, devRec.Code)
				}

				body = map[string]any{
					"IdentityPoolId":            poolID,
					"SourceUserIdentifier":      "user-src",
					"DestinationUserIdentifier": "user-dst",
					"DeveloperProviderName":     "developer.example.com",
				}
			}

			rec := doCognitoIdentityRequest(t, h, "MergeDeveloperIdentities", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["IdentityId"])
			}
		})
	}
}

func TestHandler_UnlinkIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{
			name: "identity_not_found",
			body: map[string]any{
				"IdentityId": "us-east-1:missing",
				"Logins": map[string]string{
					"accounts.google.com": "token",
				},
				"LoginsToRemove": []string{"accounts.google.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_logins_to_remove",
			body: map[string]any{
				"IdentityId": "us-east-1:any",
				"Logins": map[string]string{
					"accounts.google.com": "token",
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
			var identityID string

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "unlink-identity-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID := created["IdentityPoolId"].(string)

				getIDRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
					"IdentityPoolId": poolID,
					"Logins": map[string]string{
						"accounts.google.com": "google-token",
						"graph.facebook.com":  "facebook-token",
					},
				})
				require.Equal(t, http.StatusOK, getIDRec.Code)

				var getIDOut map[string]any
				require.NoError(t, json.Unmarshal(getIDRec.Body.Bytes(), &getIDOut))
				identityID = getIDOut["IdentityId"].(string)

				body = map[string]any{
					"IdentityId": identityID,
					"Logins": map[string]string{
						"accounts.google.com": "google-token",
					},
					"LoginsToRemove": []string{"accounts.google.com"},
				}
			}

			rec := doCognitoIdentityRequest(t, h, "UnlinkIdentity", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				descRec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
					"IdentityId": identityID,
				})
				require.Equal(t, http.StatusOK, descRec.Code)

				var descOut map[string]any
				require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
				logins, ok := descOut["Logins"].([]any)
				require.True(t, ok)
				assert.Equal(t, []any{"graph.facebook.com"}, logins)
			}
		})
	}
}

func TestHandler_UnlinkDeveloperIdentity(t *testing.T) {
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
				"IdentityId":              "us-east-1:missing",
				"IdentityPoolId":          "us-east-1:missing-pool",
				"DeveloperProviderName":   "developer.example.com",
				"DeveloperUserIdentifier": "user-001",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body
			var poolID, identityID string

			if tt.name == "success" {
				createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
					"IdentityPoolName":               "unlink-developer-pool",
					"AllowUnauthenticatedIdentities": true,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
				poolID = created["IdentityPoolId"].(string)

				tokenRec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
					"IdentityPoolId": poolID,
					"Logins": map[string]string{
						"developer.example.com": "user-001",
					},
				})
				require.Equal(t, http.StatusOK, tokenRec.Code)

				var tokenOut map[string]any
				require.NoError(t, json.Unmarshal(tokenRec.Body.Bytes(), &tokenOut))
				identityID = tokenOut["IdentityId"].(string)

				body = map[string]any{
					"IdentityId":              identityID,
					"IdentityPoolId":          poolID,
					"DeveloperProviderName":   "developer.example.com",
					"DeveloperUserIdentifier": "user-001",
				}
			}

			rec := doCognitoIdentityRequest(t, h, "UnlinkDeveloperIdentity", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				lookupRec := doCognitoIdentityRequest(t, h, "LookupDeveloperIdentity", map[string]any{
					"IdentityPoolId":        poolID,
					"IdentityId":            identityID,
					"DeveloperProviderName": "developer.example.com",
				})
				require.Equal(t, http.StatusOK, lookupRec.Code)

				var lookupOut map[string]any
				require.NoError(t, json.Unmarshal(lookupRec.Body.Bytes(), &lookupOut))
				ids, ok := lookupOut["DeveloperUserIdentifierList"].([]any)
				require.True(t, ok)
				assert.Empty(t, ids)
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
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
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
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
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
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
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
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, poolID, out["IdentityPoolId"])
	assert.Equal(t, providerName, out["IdentityProviderName"])
	assert.Equal(t, false, out["UseDefaults"])

	tags, _ := out["PrincipalTags"].(map[string]any)
	assert.Equal(t, "cognito:username", tags["sub"])
	assert.Equal(t, "email", tags["email"])
}

func TestHandler_NewOperations_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"DeleteIdentities",
		"DescribeIdentity",
		"GetOpenIdTokenForDeveloperIdentity",
		"GetPrincipalTagAttributeMap",
		"ListIdentities",
		"ListTagsForResource",
		"LookupDeveloperIdentity",
		"MergeDeveloperIdentities",
		"SetPrincipalTagAttributeMap",
		"TagResource",
		"UnlinkDeveloperIdentity",
		"UnlinkIdentity",
		"UntagResource",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
		"Tags": map[string]string{
			"key": "value",
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/nonexistent",
		"TagKeys":     []string{"key"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetOpenIdTokenForDeveloperIdentity_ExistingIdentity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "dev-existing-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	// First call creates a new identity.
	rec1 := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"developer.example.com": "user-abc",
		},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	identityID1 := out1["IdentityId"].(string)

	// Second call with same logins should re-use the same identity.
	rec2 := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"developer.example.com": "user-abc",
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	identityID2 := out2["IdentityId"].(string)

	assert.Equal(t, identityID1, identityID2)
}

// --- Refinement Check 1 Tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pool.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "reset-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Verify it exists.
	assert.Equal(t, 1, h.Backend.PoolCount())

	// Reset.
	h.Reset()

	// Should be empty.
	assert.Equal(t, 0, h.Backend.PoolCount())
	assert.Equal(t, 0, h.Backend.IdentityCount())
}

func TestRefinement1_SortedListIdentityPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"ccc-pool", "aaa-pool", "bbb-pool"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	pools, _ := out["IdentityPools"].([]any)
	require.Len(t, pools, 3)

	names := make([]string, len(pools))
	for i, p := range pools {
		pm, _ := p.(map[string]any)
		names[i] = pm["IdentityPoolName"].(string)
	}

	assert.Equal(t, []string{"aaa-pool", "bbb-pool", "ccc-pool"}, names)
}

func TestRefinement1_SortedListIdentities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "sorted-id-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	for _, login := range []map[string]string{
		{"provider.a.com": "t1"},
		{"provider.b.com": "t2"},
		{"provider.c.com": "t3"},
	} {
		idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
			"AccountId":      "000000000000",
			"IdentityPoolId": poolID,
			"Logins":         login,
		})
		require.Equal(t, http.StatusOK, idRec.Code)
	}

	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))

	identities, _ := listOut["Identities"].([]any)
	require.Len(t, identities, 3)

	returnedIDs := make([]string, len(identities))
	for i, identity := range identities {
		im, _ := identity.(map[string]any)
		returnedIDs[i] = im["IdentityId"].(string)
	}

	// Verify IDs are sorted.
	for i := 1; i < len(returnedIDs); i++ {
		assert.Less(t, returnedIDs[i-1], returnedIDs[i], "identities should be sorted by IdentityId")
	}
}

func TestRefinement1_TagsInIdentityPoolOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tags-output-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags": map[string]string{
			"env":  "test",
			"team": "backend",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	// Tags should be in the CreateIdentityPool response.
	tags, _ := created["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "backend", tags["team"])

	// Tags should be in DescribeIdentityPool response too.
	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	descTags, _ := descOut["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "test", descTags["env"])
}

func TestRefinement1_DeleteIdentityPool_CleansTagsAndPrincipalTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "cascade-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	// Add principal tag mapping.
	setRec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx",
		"UseDefaults":          false,
		"PrincipalTags":        map[string]string{"sub": "user_id"},
	})
	require.Equal(t, http.StatusOK, setRec.Code)
	assert.Equal(t, 1, h.Backend.PrincipalTagCount())

	// Tag the pool.
	tagRec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Delete the pool.
	delRec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
		"IdentityPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Principal tags for this pool should be cleaned up.
	assert.Equal(t, 0, h.Backend.PrincipalTagCount())
	assert.Equal(t, 0, h.Backend.PoolCount())
}

func TestRefinement1_DeleteIdentities_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 61 IDs should be rejected.
	ids := make([]string, 61)
	for i := range ids {
		ids[i] = fmt.Sprintf("us-east-1:fake-id-%d", i)
	}

	rec := doCognitoIdentityRequest(t, h, "DeleteIdentities", map[string]any{
		"IdentityIdsToDelete": ids,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_ListIdentities_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCode   int
	}{
		{name: "valid_60", maxResults: 60, wantCode: http.StatusOK},
		{name: "zero_rejected", maxResults: 0, wantCode: http.StatusBadRequest},
		{name: "too_large_61", maxResults: 61, wantCode: http.StatusBadRequest},
		{name: "negative", maxResults: -1, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "max-results-pool",
				"AllowUnauthenticatedIdentities": true,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

			rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
				"IdentityPoolId": created["IdentityPoolId"],
				"MaxResults":     tt.maxResults,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_DescribeIdentity_EmptyIdRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
		"IdentityId": "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_SortedLogins(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "sorted-logins-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"zzz.provider.com": "token3",
			"aaa.provider.com": "token1",
			"mmm.provider.com": "token2",
		},
	})
	require.Equal(t, http.StatusOK, idRec.Code)

	var idOut map[string]any
	require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))
	identityID := idOut["IdentityId"].(string)

	rec := doCognitoIdentityRequest(t, h, "DescribeIdentity", map[string]any{
		"IdentityId": identityID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))

	logins, _ := descOut["Logins"].([]any)
	require.Len(t, logins, 3)

	loginStrs := make([]string, len(logins))
	for i, l := range logins {
		loginStrs[i] = l.(string)
	}

	// Verify logins are sorted.
	for i := 1; i < len(loginStrs); i++ {
		assert.Less(t, loginStrs[i-1], loginStrs[i], "logins should be sorted")
	}
}

func TestRefinement1_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListIdentityPools with no pools should return [] not null.
	listPoolsRec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 0,
	})
	require.Equal(t, http.StatusOK, listPoolsRec.Code)

	var listPoolsOut map[string]any
	require.NoError(t, json.Unmarshal(listPoolsRec.Body.Bytes(), &listPoolsOut))
	pools, poolsOK := listPoolsOut["IdentityPools"].([]any)
	require.True(t, poolsOK, "IdentityPools should be a non-null array")
	assert.Empty(t, pools)

	// Create a pool then ListIdentities with no identities returns [] not null.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "non-nil-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	listIDsRec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"MaxResults":     10,
	})
	require.Equal(t, http.StatusOK, listIDsRec.Code)

	var listIDsOut map[string]any
	require.NoError(t, json.Unmarshal(listIDsRec.Body.Bytes(), &listIDsOut))
	identities, idsOK := listIDsOut["Identities"].([]any)
	require.True(t, idsOK, "Identities should be a non-null array")
	assert.Empty(t, identities)
}

func TestRefinement1_ListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "empty-tags-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	poolID := created["IdentityPoolId"].(string)
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	rec := doCognitoIdentityRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Tags should always be present (even if empty), not omitted.
	tags, hasKey := out["Tags"]
	require.True(t, hasKey, "Tags key should always be present in ListTagsForResource response")
	tagsMap, isMap := tags.(map[string]any)
	require.True(t, isMap, "Tags should be a map")
	assert.Empty(t, tagsMap)
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed a pool directly.
	pool := &cognitoidentity.IdentityPool{
		IdentityPoolID:                 "us-east-1:seed-pool-id",
		IdentityPoolName:               "seeded-pool",
		ARN:                            "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/us-east-1:seed-pool-id",
		AllowUnauthenticatedIdentities: true,
	}
	h.Backend.AddPoolInternal(pool)

	assert.Equal(t, 1, h.Backend.PoolCount())

	// Verify it's accessible.
	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "us-east-1:seed-pool-id",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "seeded-pool", descOut["IdentityPoolName"])

	// Seed an identity.
	identity := &cognitoidentity.Identity{
		IdentityID:     "us-east-1:seed-identity-id",
		IdentityPoolID: "us-east-1:seed-pool-id",
	}
	h.Backend.AddIdentityInternal(identity)

	assert.Equal(t, 1, h.Backend.IdentityCount())
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 0, h.Backend.PoolCount())
	assert.Equal(t, 0, h.Backend.IdentityCount())
	assert.Equal(t, 0, h.Backend.PrincipalTagCount())

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "count-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	assert.Equal(t, 1, h.Backend.PoolCount())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": poolID,
	})

	assert.Equal(t, 1, h.Backend.IdentityCount())

	doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_Test",
		"UseDefaults":          false,
	})

	assert.Equal(t, 1, h.Backend.PrincipalTagCount())
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// The ops map should be pre-built - verify by counting supported operations.
	ops := h.GetSupportedOperations()
	assert.GreaterOrEqual(t, len(ops), 21)

	// Two handler instances should each dispatch correctly (no shared mutable state).
	h2 := newTestHandler(t)
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{"MaxResults": 0})
	rec2 := doCognitoIdentityRequest(t, h2, "ListIdentityPools", map[string]any{"MaxResults": 0})

	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestRefinement1_PersistenceRoundTripWithPrincipalTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "persist-pt-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	setRec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_P",
		"UseDefaults":          false,
		"PrincipalTags":        map[string]string{"sub": "user_id"},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.PoolCount())
	assert.Equal(t, 1, h2.Backend.PrincipalTagCount())

	getRec := doCognitoIdentityRequest(t, h2, "GetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_P",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	tags, _ := getOut["PrincipalTags"].(map[string]any)
	assert.Equal(t, "user_id", tags["sub"])
}

func TestRefinement1_ProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cognitoidentity.Provider{}
	reg, err := p.Init(nil)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 3 {
		doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               "reset-cycle-pool",
			"AllowUnauthenticatedIdentities": true,
		})
	}

	// First pool created; second fails due to name conflict.
	assert.Equal(t, 1, h.Backend.PoolCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.PoolCount())

	// Can create again after reset.
	rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "reset-cycle-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.PoolCount())
}

func TestHandler_Refinement1_UnlinkIdentity_TokenMismatch_Returns403(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "auth-pool-r1",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
		"Logins": map[string]string{
			"accounts.google.com": "correct-token",
		},
	})
	require.Equal(t, http.StatusOK, idRec.Code)

	var idOut map[string]any
	require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

	rec := doCognitoIdentityRequest(t, h, "UnlinkIdentity", map[string]any{
		"IdentityId": idOut["IdentityId"],
		"Logins": map[string]string{
			"accounts.google.com": "wrong-token",
		},
		"LoginsToRemove": []string{"accounts.google.com"},
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandler_Refinement1_UnlinkDeveloperIdentity_WrongUser_Returns403(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "unlink-dev-403-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	devRec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"developer.example.com": "user-001",
		},
	})
	require.Equal(t, http.StatusOK, devRec.Code)

	var devOut map[string]any
	require.NoError(t, json.Unmarshal(devRec.Body.Bytes(), &devOut))

	rec := doCognitoIdentityRequest(t, h, "UnlinkDeveloperIdentity", map[string]any{
		"IdentityId":              devOut["IdentityId"],
		"IdentityPoolId":          poolID,
		"DeveloperProviderName":   "developer.example.com",
		"DeveloperUserIdentifier": "wrong-user",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandler_Refinement1_DeveloperProviderName_InCreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "dev-provider-pool",
		"AllowUnauthenticatedIdentities": false,
		"DeveloperProviderName":          "developer.myapp.com",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	assert.Equal(t, "developer.myapp.com", created["DeveloperProviderName"])

	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "developer.myapp.com", descOut["DeveloperProviderName"])
}

func TestHandler_Refinement1_UpdateIdentityPool_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tag-update-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "dev"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	updateRec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 poolID,
		"IdentityPoolName":               "tag-update-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "prod", "team": "backend"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updated))
	tags, _ := updated["IdentityPoolTags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "backend", tags["team"])
}

func TestHandler_Refinement1_GetOpenIdTokenForDeveloperIdentity_InvalidDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "dur-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Logins": map[string]string{
			"developer.example.com": "user-001",
		},
		"TokenDuration": 99999,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_SetIdentityPoolRoles_SingleRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "single-role-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Roles": map[string]string{
			"authenticated": "arn:aws:iam::000000000000:role/AuthRole",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_Refinement1_TagResource_EmptyArn_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "",
		"Tags":        map[string]string{"k": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_UntagResource_EmptyArn_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_GetPrincipalTagAttributeMap_EmptyProvider_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "ptag-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       created["IdentityPoolId"],
		"IdentityProviderName": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_ListIdentities_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": "",
		"MaxResults":     10,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_GetCredentialsForIdentity_EmptyId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", map[string]any{
		"IdentityId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_GetOpenIdToken_EmptyId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetOpenIdToken", map[string]any{
		"IdentityId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement1_MergeDeveloperIdentities_MissingRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_pool_id",
			body: map[string]any{
				"IdentityPoolId":            "",
				"SourceUserIdentifier":      "src",
				"DestinationUserIdentifier": "dst",
				"DeveloperProviderName":     "developer.example.com",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_provider_name",
			body: map[string]any{
				"IdentityPoolId":            "us-east-1:pool",
				"SourceUserIdentifier":      "src",
				"DestinationUserIdentifier": "dst",
				"DeveloperProviderName":     "",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_source",
			body: map[string]any{
				"IdentityPoolId":            "us-east-1:pool",
				"SourceUserIdentifier":      "",
				"DestinationUserIdentifier": "dst",
				"DeveloperProviderName":     "developer.example.com",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCognitoIdentityRequest(t, h, "MergeDeveloperIdentities", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Refinement check 2 handler tests ---

func TestHandler_Refinement2_GetID_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement2_DescribeIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement2_DeleteIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "DeleteIdentityPool", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement2_UpdateIdentityPool_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 "",
		"IdentityPoolName":               "name",
		"AllowUnauthenticatedIdentities": true,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement2_GetIdentityPoolRoles_EmptyPoolId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Refinement2_GetIdentityPoolRoles_OmitsEmptyRoles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pool and set only the authenticated role.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-omit-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
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
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	roles := out["Roles"].(map[string]any)

	assert.Contains(t, roles, "authenticated")
	assert.NotContains(t, roles, "unauthenticated", "empty unauthenticated role must be omitted")
}

func TestHandler_Refinement2_SetIdentityPoolRoles_PreservesExistingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-preserve-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
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
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	roles := out["Roles"].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthV2", roles["authenticated"])
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/Unauth",
		roles["unauthenticated"],
		"unauthenticated role must be preserved after partial update",
	)
}

func TestHandler_Refinement2_GetID_MergesLoginProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "login-merge-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	poolID := createOut["IdentityPoolId"].(string)

	// First call: only google.
	rec1 := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": poolID,
		"Logins":         map[string]string{"accounts.google.com": "g-tok"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	id1 := out1["IdentityId"].(string)

	// Second call: same google + new facebook → must return same ID.
	rec2 := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"IdentityPoolId": poolID,
		"Logins": map[string]string{
			"accounts.google.com": "g-tok",
			"graph.facebook.com":  "fb-tok",
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	id2 := out2["IdentityId"].(string)

	assert.Equal(t, id1, id2, "GetId must return the same identity when any login provider matches")
}

func TestHandler_Refinement2_ListIdentityPools_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pool-a", "pool-b", "pool-c"} {
		rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": false,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1.
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	assert.Len(t, out1["IdentityPools"], 2)
	nextToken, _ := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Page 2 using the cursor.
	rec2 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	assert.Len(t, out2["IdentityPools"], 1)
}

func TestHandler_Refinement2_ListIdentities_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "page-id-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	poolID := createOut["IdentityPoolId"].(string)

	for i := range 3 {
		doCognitoIdentityRequest(t, h, "GetId", map[string]any{
			"IdentityPoolId": poolID,
			"Logins":         map[string]string{"accounts.google.com": fmt.Sprintf("tok-%d", i)},
		})
	}

	// Page 1.
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	assert.Len(t, out1["Identities"], 2)
	nextToken, _ := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Page 2.
	rec2 := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     2,
		"NextToken":      nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	assert.Len(t, out2["Identities"], 1)
}
