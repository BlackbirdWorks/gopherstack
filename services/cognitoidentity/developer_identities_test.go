package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestInMemoryBackend_DeveloperLoginsFrom_EmptyProviderName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	pool, err := b.CreateIdentityPool(
		context.Background(),
		"dev-logins-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	devRec, err := b.GetOpenIDTokenForDeveloperIdentity(context.Background(),
		pool.IdentityPoolID,
		"",
		map[string]string{"developer.example.com": "user-001"},
		0,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, devRec.IdentityID)

	// LookupDeveloperIdentity with empty provider name returns all dev user IDs.
	result, err := b.LookupDeveloperIdentity(context.Background(),
		pool.IdentityPoolID,
		devRec.IdentityID,
		"",
		"",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, result.DeveloperUserIdentifierList)
}

func TestInMemoryBackend_UnlinkIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget             error
		logins                map[string]string
		name                  string
		identityID            string
		setupIdentityProvider string
		wantProviderRemoved   string
		loginsToRemove        []string
		wantErr               bool
	}{
		{
			name:                  "success",
			identityID:            "use-created-identity",
			logins:                map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove:        []string{"accounts.google.com"},
			setupIdentityProvider: "accounts.google.com",
			wantProviderRemoved:   "accounts.google.com",
		},
		{
			name:       "identity_not_found",
			identityID: "us-east-1:missing",
			logins:     map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:   "missing_identity_id",
			logins: map[string]string{"accounts.google.com": "google-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrInvalidParameter,
		},
		{
			name:       "missing_logins_to_remove",
			identityID: "use-created-identity",
			logins:     map[string]string{"accounts.google.com": "google-token"},
			wantErr:    true,
			errTarget:  cognitoidentity.ErrInvalidParameter,
		},
		{
			name:       "token_mismatch",
			identityID: "use-created-identity",
			logins:     map[string]string{"accounts.google.com": "wrong-token"},
			loginsToRemove: []string{
				"accounts.google.com",
			},
			wantErr:   true,
			errTarget: cognitoidentity.ErrNotAuthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateIdentityPool(
				context.Background(),
				"unlink-identity-pool-"+tt.name,
				true,
				false,
				"",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			identity, err := b.GetID(
				context.Background(),
				pool.IdentityPoolID,
				"000000000000",
				map[string]string{
					"accounts.google.com": "google-token",
					"graph.facebook.com":  "facebook-token",
				},
			)
			require.NoError(t, err)

			identityID := tt.identityID
			if identityID == "use-created-identity" {
				identityID = identity.IdentityID
			}

			err = b.UnlinkIdentity(context.Background(), identityID, tt.logins, tt.loginsToRemove)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			desc, err := b.DescribeIdentity(context.Background(), identity.IdentityID)
			require.NoError(t, err)
			assert.NotContains(t, desc.Logins, tt.wantProviderRemoved)
		})
	}
}

func TestInMemoryBackend_UnlinkDeveloperIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget               error
		name                    string
		identityID              string
		poolID                  string
		developerProviderName   string
		developerUserIdentifier string
		wantErr                 bool
	}{
		{
			name:                    "success",
			identityID:              "use-created-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
		},
		{
			name:                    "pool_not_found",
			identityID:              "use-created-identity",
			poolID:                  "us-east-1:missing-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:                    "identity_not_found",
			identityID:              "us-east-1:missing-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrIdentityPoolNotFound,
		},
		{
			name:                    "user_identifier_not_found",
			identityID:              "use-created-identity",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "missing-user",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrNotAuthorized,
		},
		{
			name:                    "missing_identity_id",
			poolID:                  "use-created-pool",
			developerProviderName:   "developer.example.com",
			developerUserIdentifier: "user-001",
			wantErr:                 true,
			errTarget:               cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateIdentityPool(
				context.Background(),
				"unlink-dev-pool-"+tt.name,
				true,
				false,
				"",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			devToken, err := b.GetOpenIDTokenForDeveloperIdentity(context.Background(),
				pool.IdentityPoolID,
				"",
				map[string]string{"developer.example.com": "user-001"},
				0,
			)
			require.NoError(t, err)

			identityID := tt.identityID
			if identityID == "use-created-identity" {
				identityID = devToken.IdentityID
			}

			poolID := tt.poolID
			if poolID == "use-created-pool" {
				poolID = pool.IdentityPoolID
			}

			err = b.UnlinkDeveloperIdentity(context.Background(),
				identityID,
				poolID,
				tt.developerProviderName,
				tt.developerUserIdentifier,
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			result, err := b.LookupDeveloperIdentity(context.Background(),
				pool.IdentityPoolID,
				devToken.IdentityID,
				"",
				"developer.example.com",
			)
			require.NoError(t, err)
			assert.Empty(t, result.DeveloperUserIdentifierList)
		})
	}
}

func TestInMemoryBackend_LookupDeveloperIdentity_EmptyPoolID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.LookupDeveloperIdentity(
		context.Background(),
		"",
		"",
		"user-001",
		"developer.example.com",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_MergeDeveloperIdentities_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget             error
		name                  string
		poolID                string
		developerProviderName string
		sourceUserID          string
		destUserID            string
		wantErr               bool
	}{
		{
			name:                  "missing_pool_id",
			poolID:                "",
			developerProviderName: "developer.example.com",
			sourceUserID:          "src",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_developer_provider_name",
			poolID:                "us-east-1:pool",
			developerProviderName: "",
			sourceUserID:          "src",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_source_user_id",
			poolID:                "us-east-1:pool",
			developerProviderName: "developer.example.com",
			sourceUserID:          "",
			destUserID:            "dst",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
		{
			name:                  "missing_dest_user_id",
			poolID:                "us-east-1:pool",
			developerProviderName: "developer.example.com",
			sourceUserID:          "src",
			destUserID:            "",
			wantErr:               true,
			errTarget:             cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.MergeDeveloperIdentities(
				context.Background(),
				tt.sourceUserID,
				tt.destUserID,
				tt.developerProviderName,
				tt.poolID,
			)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errTarget)
		})
	}
}

func TestInMemoryBackend_UnlinkIdentity_ProviderNotLinked_Returns_NotAuthorized(
	t *testing.T,
) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"unlink-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	identity, err := b.GetID(
		context.Background(),
		pool.IdentityPoolID,
		"000000000000",
		map[string]string{
			"graph.facebook.com": "fb-token",
		},
	)
	require.NoError(t, err)

	// Try to unlink a provider that isn't linked (google was never linked).
	err = b.UnlinkIdentity(context.Background(),
		identity.IdentityID,
		map[string]string{"accounts.google.com": "some-token"},
		[]string{"accounts.google.com"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrNotAuthorized)
}

func TestInMemoryBackend_LastModifiedDate_UpdatedOnUnlink(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"lmd-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	identity, err := b.GetID(
		context.Background(),
		pool.IdentityPoolID,
		"000000000000",
		map[string]string{
			"accounts.google.com": "google-token",
		},
	)
	require.NoError(t, err)

	// Describe before unlink.
	desc1, err := b.DescribeIdentity(context.Background(), identity.IdentityID)
	require.NoError(t, err)
	createdAt := desc1.CreationDate

	// Unlink the google login.
	err = b.UnlinkIdentity(context.Background(),
		identity.IdentityID,
		map[string]string{"accounts.google.com": "google-token"},
		[]string{"accounts.google.com"},
	)
	require.NoError(t, err)

	// Describe after unlink.
	desc2, err := b.DescribeIdentity(context.Background(), identity.IdentityID)
	require.NoError(t, err)
	assert.False(
		t,
		desc2.LastModifiedDate.Before(createdAt),
		"LastModifiedDate should not be before CreatedAt",
	)
}
