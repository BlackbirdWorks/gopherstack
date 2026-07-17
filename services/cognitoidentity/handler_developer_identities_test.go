package cognitoidentity_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_UnlinkIdentity_TokenMismatch_Returns403(t *testing.T) {
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

func TestHandler_UnlinkDeveloperIdentity_WrongUser_Returns403(t *testing.T) {
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

func TestHandler_MergeDeveloperIdentities_MissingRequired(t *testing.T) {
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
