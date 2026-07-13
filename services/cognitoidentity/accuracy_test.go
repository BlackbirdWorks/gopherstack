package cognitoidentity_test

// accuracy_test.go covers the 11 AWS-accuracy gaps from issue #1701.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// ---- Gap 1: AllowUnauthenticatedIdentities enforced in GetID ----

func TestAccuracy_GetID_UnauthDisabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "no-unauth-pool", false, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.Error(t, err)
	assert.ErrorIs(
		t,
		err,
		cognitoidentity.ErrNotAuthorized,
		"empty logins + AllowUnauthenticated=false must return NotAuthorizedException",
	)
}

func TestAccuracy_GetID_UnauthEnabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "unauth-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, identity.IdentityID)
}

func TestAccuracy_GetID_UnauthDisabled_WithLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "no-unauth-with-logins", false, false, "", nil, nil, nil)
	require.NoError(t, err)

	logins := map[string]string{"accounts.google.com": "google-token-abc"}
	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", logins)
	require.NoError(t, err, "authenticated GetId must succeed even when AllowUnauthenticated=false")
	assert.NotEmpty(t, identity.IdentityID)
}

func TestAccuracy_Handler_GetID_UnauthDisabled_EmptyLogins(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "no-unauth-handler-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": created["IdentityPoolId"],
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// ---- Gap 2: GetCredentialsForIdentity login validation ----

func TestAccuracy_GetCredentials_LoginMismatch(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "creds-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	logins := map[string]string{"accounts.google.com": "real-token"}
	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", logins)
	require.NoError(t, err)

	_, err = b.GetCredentialsForIdentity(context.Background(), identity.IdentityID, map[string]string{
		"accounts.google.com": "wrong-token",
	})
	require.Error(t, err)
	assert.ErrorIs(
		t,
		err,
		cognitoidentity.ErrNotAuthorized,
		"mismatched login token must return NotAuthorizedException",
	)
}

func TestAccuracy_GetCredentials_LoginProviderAbsent(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "creds-pool-2", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	logins := map[string]string{"accounts.google.com": "real-token"}
	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", logins)
	require.NoError(t, err)

	_, err = b.GetCredentialsForIdentity(context.Background(), identity.IdentityID, map[string]string{
		"login.facebook.com": "fb-token",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrNotAuthorized)
}

func TestAccuracy_GetCredentials_MatchingLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "creds-pool-3", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	logins := map[string]string{"accounts.google.com": "real-token"}
	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", logins)
	require.NoError(t, err)

	creds, err := b.GetCredentialsForIdentity(context.Background(), identity.IdentityID, logins)
	require.NoError(t, err, "matching login tokens must succeed")
	assert.NotEmpty(t, creds.AccessKeyID)
}

func TestAccuracy_GetCredentials_NilLogins(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "creds-pool-nil", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "000000000000", nil)
	require.NoError(t, err)

	creds, err := b.GetCredentialsForIdentity(context.Background(), identity.IdentityID, nil)
	require.NoError(t, err, "nil logins must succeed (unauthenticated identity)")
	assert.NotEmpty(t, creds.AccessKeyID)
}

// ---- Gap 3: Identity disabled status / HideDisabled ----

func TestAccuracy_ListIdentities_HideDisabled(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "hide-disabled-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	id1, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p1": "t1"})
	require.NoError(t, err)

	id2, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p2": "t2"})
	require.NoError(t, err)

	b.SetIdentityEnabled(id2.IdentityID, false)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, true, "")
	require.NoError(t, err)
	require.Len(t, result.Identities, 1)
	assert.Equal(
		t,
		id1.IdentityID,
		result.Identities[0].IdentityID,
		"disabled identity must be hidden",
	)
}

func TestAccuracy_ListIdentities_ShowDisabled(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "show-disabled-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	id1, err := b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p1": "t1"})
	require.NoError(t, err)

	_, err = b.GetID(context.Background(), pool.IdentityPoolID, "", map[string]string{"p2": "t2"})
	require.NoError(t, err)

	b.SetIdentityEnabled(id1.IdentityID, false)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, false, "")
	require.NoError(t, err)
	assert.Len(t, result.Identities, 2, "HideDisabled=false must include disabled identities")
}

func TestAccuracy_NewIdentity_EnabledByDefault(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "enabled-default-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(context.Background(), pool.IdentityPoolID, "", nil)
	require.NoError(t, err)

	result, err := b.ListIdentities(context.Background(), pool.IdentityPoolID, 10, true, "")
	require.NoError(t, err)
	require.Len(t, result.Identities, 1, "new identity must be enabled by default")
	assert.Equal(t, identity.IdentityID, result.Identities[0].IdentityID)
}

// ---- Gap 7: Expiration timestamp uses epoch seconds ----
//
// aws-sdk-go-v2's cognitoidentity deserializer parses Credentials.Expiration with
// smithytime.ParseEpochSeconds, which treats the wire value as seconds (with
// optional fractional component) since the Unix epoch -- not milliseconds. A
// prior pass got this backwards; the correct wire value for "~1 hour from now"
// is a 10-digit number of seconds, not a 13-digit number of milliseconds.
func TestAccuracy_GetCredentials_ExpirationIsEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "expiry-pool",
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

// ---- Gap 11: ListIdentities MaxResults=0 rejected ----

func TestAccuracy_ListIdentities_MaxResultsZero(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "maxresults-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.ListIdentities(context.Background(), pool.IdentityPoolID, 0, false, "")
	require.Error(t, err)
	assert.ErrorIs(
		t,
		err,
		cognitoidentity.ErrInvalidParameter,
		"MaxResults=0 must return InvalidParameterException",
	)
}

func TestAccuracy_Handler_ListIdentities_MaxResultsZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "maxresults-handler-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	rec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"MaxResults":     0,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Gap 6: SetIdentityPoolRoles / GetIdentityPoolRoles RoleMappings ----

func TestAccuracy_SetGetIdentityPoolRoles_WithRoleMappings(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "role-mappings-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	roleMappings := map[string]cognitoidentity.RoleMapping{
		"accounts.google.com": {
			Type:                    "Rules",
			AmbiguousRoleResolution: "Deny",
			RulesConfiguration: &cognitoidentity.RulesConfiguration{
				Rules: []cognitoidentity.MappingRule{
					{
						Claim:     "sub",
						MatchType: "Equals",
						Value:     "user123",
						RoleARN:   "arn:aws:iam::000000000000:role/GoogleUser",
					},
				},
			},
		},
	}

	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/Auth",
		"arn:aws:iam::000000000000:role/Unauth",
		roleMappings,
	)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::000000000000:role/Auth", roles.AuthenticatedRoleARN)
	assert.Equal(t, "arn:aws:iam::000000000000:role/Unauth", roles.UnauthenticatedRoleARN)
	require.Contains(t, roles.RoleMappings, "accounts.google.com")

	mapping := roles.RoleMappings["accounts.google.com"]
	assert.Equal(t, "Rules", mapping.Type)
	assert.Equal(t, "Deny", mapping.AmbiguousRoleResolution)
	require.NotNil(t, mapping.RulesConfiguration)
	require.Len(t, mapping.RulesConfiguration.Rules, 1)

	rule := mapping.RulesConfiguration.Rules[0]
	assert.Equal(t, "sub", rule.Claim)
	assert.Equal(t, "Equals", rule.MatchType)
	assert.Equal(t, "user123", rule.Value)
	assert.Equal(t, "arn:aws:iam::000000000000:role/GoogleUser", rule.RoleARN)
}

func TestAccuracy_Handler_SetGetIdentityPoolRoles_WithRoleMappings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "role-mappings-handler-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
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
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))

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

func TestAccuracy_SetIdentityPoolRoles_RoleMappingsNilPreservesExisting(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	pool, err := b.CreateIdentityPool(context.Background(), "role-preserve-pool", true, false, "", nil, nil, nil)
	require.NoError(t, err)

	roleMappings := map[string]cognitoidentity.RoleMapping{
		"accounts.google.com": {Type: "Token", AmbiguousRoleResolution: "AuthenticatedRole"},
	}

	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/Auth",
		"",
		roleMappings,
	)
	require.NoError(t, err)

	// Update with nil roleMappings — existing mappings must be preserved.
	err = b.SetIdentityPoolRoles(context.Background(),
		pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/AuthV2",
		"",
		nil,
	)
	require.NoError(t, err)

	roles, err := b.GetIdentityPoolRoles(context.Background(), pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/AuthV2", roles.AuthenticatedRoleARN)
	assert.Contains(
		t,
		roles.RoleMappings,
		"accounts.google.com",
		"nil roleMappings must not clear existing mappings",
	)
}
