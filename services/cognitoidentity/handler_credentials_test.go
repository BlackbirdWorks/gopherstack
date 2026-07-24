package cognitoidentity_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aws-sdk-go-v2's cognitoidentity deserializer parses Credentials.Expiration with
// smithytime.ParseEpochSeconds, which treats the wire value as seconds (with
// optional fractional component) since the Unix epoch -- not milliseconds. A
// prior pass got this backwards; the correct wire value for "~1 hour from now"
// is a 10-digit number of seconds, not a 13-digit number of milliseconds.
func TestGetCredentialsForIdentity_ExpirationIsEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "expiry-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rolesRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Roles": map[string]string{
			"unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
		},
	})
	require.Equal(t, http.StatusOK, rolesRec.Code)

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

	creds := out["Credentials"].(map[string]any)
	expiration, ok := creds["Expiration"].(float64)
	require.True(t, ok, "Expiration must be a number")

	// Second epoch is 10 digits; millisecond epoch is 13 digits.
	// Threshold: 1e11 seconds corresponds to roughly year 5138, well after any
	// valid ~1-hour-from-now expiry, so anything at or above it must be in
	// milliseconds by mistake.
	const maxSecondsEpoch = 1e11
	assert.Less(
		t,
		expiration,
		maxSecondsEpoch,
		"Expiration must be in seconds (10-digit epoch), not milliseconds",
	)
	assert.InDelta(
		t,
		float64(time.Now().Add(time.Hour).Unix()),
		expiration,
		float64(2*time.Minute/time.Second),
		"Expiration must be ~1 hour from now in seconds",
	)
}

// TestGetCredentialsForIdentity_CustomRoleArn verifies that
// GetCredentialsForIdentity accepts a CustomRoleArn field without error.
func TestGetCredentialsForIdentity_CustomRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		customRoleArn string
	}{
		{
			name:          "with_custom_role",
			customRoleArn: "arn:aws:iam::000000000000:role/MyCustomRole",
		},
		{
			name:          "without_custom_role",
			customRoleArn: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
				"IdentityPoolName":               "cust-role-pool-" + tt.name,
				"AllowUnauthenticatedIdentities": true,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

			rolesRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
				"IdentityPoolId": created["IdentityPoolId"],
				"Roles": map[string]string{
					"unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
				},
			})
			require.Equal(t, http.StatusOK, rolesRec.Code)

			idRec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
				"AccountId":      "000000000000",
				"IdentityPoolId": created["IdentityPoolId"],
			})
			require.Equal(t, http.StatusOK, idRec.Code)

			var idOut map[string]any
			require.NoError(t, json.Unmarshal(idRec.Body.Bytes(), &idOut))

			req := map[string]any{"IdentityId": idOut["IdentityId"]}
			if tt.customRoleArn != "" {
				req["CustomRoleArn"] = tt.customRoleArn
			}

			rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", req)
			assert.Equal(
				t,
				http.StatusOK,
				rec.Code,
				"GetCredentialsForIdentity with CustomRoleArn: %s",
				rec.Body.String(),
			)
		})
	}
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

	rolesRec := doCognitoIdentityRequest(t, h, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"Roles": map[string]string{
			"unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
		},
	})
	require.Equal(t, http.StatusOK, rolesRec.Code)

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

func TestHandler_GetOpenIdTokenForDeveloperIdentity_InvalidDuration(t *testing.T) {
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

func TestHandler_GetCredentialsForIdentity_EmptyId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetCredentialsForIdentity", map[string]any{
		"IdentityId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetOpenIdToken_EmptyId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "GetOpenIdToken", map[string]any{
		"IdentityId": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
