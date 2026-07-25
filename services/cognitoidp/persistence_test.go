package cognitoidp_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestInMemoryBackend_SnapshotRestore is a regression guard for the Phase 3.3
// pkgs/store conversion: it seeds one instance of every resource kind the
// backend owns (both store.Table-backed and the maps left raw -- see
// store_setup.go's registerAllTables doc for why each raw map can't be a
// store.Table) and checks that every one of them survives a Snapshot/Restore
// round trip into a fresh backend byte-for-byte equivalent.
func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		setup  func(t *testing.T, b *cognitoidp.InMemoryBackend)
		verify func(t *testing.T, b *cognitoidp.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend_round_trip",
			setup: func(*testing.T, *cognitoidp.InMemoryBackend) {},
			verify: func(t *testing.T, b *cognitoidp.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.ListUserPools())
			},
		},
		{
			name: "full_state_round_trip",
			setup: func(t *testing.T, b *cognitoidp.InMemoryBackend) {
				t.Helper()

				pool, err := b.CreateUserPool("test-pool")
				require.NoError(t, err)

				client, err := b.CreateUserPoolClient(pool.ID, "test-client")
				require.NoError(t, err)

				user, err := b.AdminCreateUserWithPolicy(pool.ID, "alice", "TempPass123!", map[string]string{
					"email": "alice@example.com",
				})
				require.NoError(t, err)
				require.NotEmpty(t, user.PasswordHash)

				// Give the user a permanent password so USER_PASSWORD_AUTH succeeds
				// below and mints a refresh token to round-trip too.
				require.NoError(t, b.AdminSetUserPassword(pool.ID, "alice", "Permanent123!", true))

				_, err = b.CreateGroup(pool.ID, "admins", "administrators", 1)
				require.NoError(t, err)
				require.NoError(t, b.AdminAddUserToGroup(pool.ID, "alice", "admins"))

				_, err = b.CreateResourceServer(pool.ID, "https://api.example.com", "API", nil)
				require.NoError(t, err)

				_, err = b.CreateIdentityProvider(pool.ID, "Google", "Google", map[string]string{"client_id": "abc"})
				require.NoError(t, err)

				_, err = b.CreateUserPoolDomain(pool.ID, "test-domain")
				require.NoError(t, err)

				_, err = b.CreateTerms(pool.ID, "accept these terms")
				require.NoError(t, err)

				_, err = b.CreateUserImportJob(pool.ID, "import-1")
				require.NoError(t, err)

				_, err = b.CreateManagedLoginBranding(pool.ID, client.ClientID)
				require.NoError(t, err)

				_, err = b.SetUICustomization(pool.ID, client.ClientID, "body{color:red}")
				require.NoError(t, err)

				require.NoError(t, b.SetTypedRiskConfiguration(&cognitoidp.TypedRiskConfiguration{
					UserPoolID: pool.ID,
					ClientID:   client.ClientID,
				}))

				poolARN := "arn:aws:cognito-idp:us-east-1:000000000000:userpool/" + pool.ID
				b.TagResource(poolARN, map[string]string{"env": "test"})

				// USER_PASSWORD_AUTH mints a refresh token -- exercises the raw
				// refreshTokens/refreshTokensByClient/refreshTokensByUser maps.
				_, err = b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Permanent123!")
				require.NoError(t, err)

				// Exercises the userPoolReplicas store.Table (parity-4).
				_, err = b.CreateUserPoolReplica(pool.ID, "us-west-2", nil)
				require.NoError(t, err)

				// Exercises the raw provisionedLimits map (parity-4).
				_, err = b.UpdateProvisionedLimit(
					"API_CATEGORY", map[string]string{"Category": "UserAuthentication"}, 250,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cognitoidp.InMemoryBackend) {
				t.Helper()

				pools := b.ListUserPools()
				require.Len(t, pools, 1)
				poolID := pools[0].ID
				assert.Equal(t, "test-pool", pools[0].Name)

				clients, err := b.ListUserPoolClients(poolID)
				require.NoError(t, err)
				require.Len(t, clients, 1)
				clientID := clients[0].ClientID

				user, err := b.AdminGetUser(poolID, "alice")
				require.NoError(t, err)
				assert.NotEmpty(t, user.PasswordHash, "password hash must survive restore")
				assert.Equal(t, "alice@example.com", user.Attributes["email"])

				groups, err := b.AdminListGroupsForUser(poolID, "alice")
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, "admins", groups[0].GroupName)

				servers, err := b.ListResourceServers(poolID)
				require.NoError(t, err)
				require.Len(t, servers, 1)
				assert.Equal(t, "https://api.example.com", servers[0].Identifier)

				idps, err := b.ListIdentityProviders(poolID)
				require.NoError(t, err)
				require.Len(t, idps, 1)
				assert.Equal(t, "Google", idps[0].ProviderName)

				domain, err := b.DescribeUserPoolDomain("test-domain")
				require.NoError(t, err)
				assert.Equal(t, poolID, domain.UserPoolID)

				terms, err := b.DescribeTerms(poolID)
				require.NoError(t, err)
				assert.Equal(t, "accept these terms", terms.Text)

				jobs, err := b.ListUserImportJobs(poolID)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				mlb, err := b.DescribeManagedLoginBrandingByClient(poolID, clientID)
				require.NoError(t, err)
				assert.Equal(t, clientID, mlb.ClientID)

				ui, err := b.GetUICustomization(poolID, clientID)
				require.NoError(t, err)
				assert.Equal(t, "body{color:red}", ui.CSS)

				risk, err := b.GetTypedRiskConfiguration(poolID, clientID)
				require.NoError(t, err)
				assert.Equal(t, poolID, risk.UserPoolID)

				poolARN := "arn:aws:cognito-idp:us-east-1:000000000000:userpool/" + poolID
				assert.Equal(t, "test", b.ListTagsForResource(poolARN)["env"])

				assert.Positive(t, b.RefreshTokenCount(), "refresh token must survive restore")

				replicas, err := b.ListUserPoolReplicas(poolID)
				require.NoError(t, err)
				require.Len(t, replicas, 1, "userPoolReplicas table must survive restore")
				assert.Equal(t, "us-west-2", replicas[0].RegionName)

				limit, err := b.GetProvisionedLimit(
					"API_CATEGORY", map[string]string{"Category": "UserAuthentication"},
				)
				require.NoError(t, err)
				assert.EqualValues(t, 250, limit.ProvisionedLimitValue, "provisionedLimits map must survive restore")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
			tc.setup(t, b)

			data := b.Snapshot(ctx)
			require.NotEmpty(t, data)

			restored := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
			require.NoError(t, restored.Restore(ctx, data))

			tc.verify(t, restored)
		})
	}
}

// TestInMemoryBackend_RestoreVersionGuard verifies the snapshot version guard:
// malformed JSON is a hard error, while a well-formed but version-mismatched
// (or pre-Phase-3.3, version-less) snapshot is discarded cleanly -- Restore
// resets the backend to empty and returns nil rather than partially decoding
// an incompatible shape.
func TestInMemoryBackend_RestoreVersionGuard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "malformed_json_errors",
			data:    []byte(`{not valid json`),
			wantErr: true,
		},
		{
			name:    "missing_version_resets_cleanly",
			data:    []byte(`{"accountId":"000000000000"}`),
			wantErr: false,
		},
		{
			name:    "future_version_resets_cleanly",
			data:    []byte(`{"version":999999,"accountId":"000000000000"}`),
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

			_, err := b.CreateUserPool("pre-existing-pool")
			require.NoError(t, err)

			err = b.Restore(ctx, tc.data)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, b.ListUserPools(), "incompatible/malformed-version snapshot must reset to empty")
		})
	}
}
func TestPersistence_NewFieldsSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateUserPool("persist-pool")
	require.NoError(t, err)

	// Set password policy (MFA not enabled so auth still issues tokens directly).
	require.NoError(t, b.UpdateUserPoolWithOpts(pool.ID, "", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{MinimumLength: 12, RequireUppercase: true},
	}))

	// Create a resource server.
	_, err = b.CreateResourceServer(pool.ID, "https://persist.example.com", "Persist API", nil)
	require.NoError(t, err)

	// Create user + sign out to populate tokenRevokedBefore.
	client, err := b.CreateUserPoolClient(pool.ID, "pc")
	require.NoError(t, err)

	// Use SignUp (no policy validation) so any password works.
	user, err := b.SignUp(client.ClientID, "persist-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "persist-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "persist-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNilf(t, result.Tokens, "expected tokens but got challenge: %s", result.ChallengeName)

	require.NoError(t, b.GlobalSignOut(result.Tokens.AccessToken))

	// Take snapshot.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into fresh backend.
	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify resource server survived.
	rs, err := b2.DescribeResourceServer(pool.ID, "https://persist.example.com")
	require.NoError(t, err)
	assert.Equal(t, "Persist API", rs.Name)

	// Verify password policy survived.
	restoredPool, err := b2.DescribeUserPool(pool.ID)
	require.NoError(t, err)
	require.NotNil(t, restoredPool.PasswordPolicy)
	assert.Equal(t, 12, restoredPool.PasswordPolicy.MinimumLength)
	assert.True(t, restoredPool.PasswordPolicy.RequireUppercase)
}

func TestPersistence_RefreshTokensSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("rt-persist-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "rpc")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "rt-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "rt-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "rt-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	// Snapshot with active refresh token.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Refresh token count must survive.
	assert.Equal(t, b.RefreshTokenCount(), b2.RefreshTokenCount())
	assert.Equal(t, 1, b2.RefreshTokenCount())
}

func TestCognitoIDP_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	// Use AdminCreateUser to create a confirmed user without going through SignUp.
	_, err = b.AdminCreateUser(pool.ID, "alice", "Password123!", map[string]string{"email": "alice@example.com"})
	require.NoError(t, err)

	require.NoError(t, b.AdminSetUserPassword(pool.ID, "alice", "Password123!", true))

	_ = client

	h := cognitoidp.NewHandler(b, "us-east-1")
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	h2 := cognitoidp.NewHandler(b2, "us-east-1")
	require.NoError(t, h2.Restore(t.Context(), snap))

	pools := b2.ListUserPools()
	require.Len(t, pools, 1)
	assert.Equal(t, "my-pool", pools[0].Name)

	clients, err := b2.ListUserPoolClients(pool.ID)
	require.NoError(t, err)
	require.Len(t, clients, 1)
	assert.Equal(t, "my-client", clients[0].ClientName)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "persist-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "persist-client",
	})
	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "persist-user",
		"TemporaryPassword": "TempPass123!",
	})
	doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "persist-user",
	})

	snap := h.Backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.UserPoolCount())
	assert.Equal(t, 1, h2.Backend.UserCount())
	assert.Equal(t, 1, h2.Backend.ClientCount())

	// Disabled state should survive round-trip.
	getUserRec := doCognitoRequest(t, h2, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "persist-user",
	})
	var getUserResp map[string]any
	require.NoError(t, json.Unmarshal(getUserRec.Body.Bytes(), &getUserResp))
	assert.False(t, getUserResp["Enabled"].(bool))
}
