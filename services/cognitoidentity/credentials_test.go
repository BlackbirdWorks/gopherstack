package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestInMemoryBackend_GetCredentialsForIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "identity_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var identityID string

			if tt.name == "success" {
				pool, poolErr := b.CreateIdentityPool(
					context.Background(),
					"creds-pool",
					true,
					false,
					"",
					nil,
					nil,
					nil,
				)
				require.NoError(t, poolErr)

				identity, idErr := b.GetID(
					context.Background(),
					pool.IdentityPoolID,
					"000000000000",
					nil,
				)
				require.NoError(t, idErr)
				identityID = identity.IdentityID
			} else {
				identityID = "us-east-1:nonexistent"
			}

			creds, err := b.GetCredentialsForIdentity(context.Background(), identityID, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, creds.AccessKeyID)
			assert.NotEmpty(t, creds.SecretAccessKey)
			assert.NotEmpty(t, creds.SessionToken)
			assert.Equal(t, identityID, creds.IdentityID)
		})
	}
}

func TestInMemoryBackend_GetOpenIDToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{
			name:      "identity_not_found",
			wantErr:   true,
			errTarget: cognitoidentity.ErrIdentityPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var identityID string

			if tt.name == "success" {
				pool, poolErr := b.CreateIdentityPool(
					context.Background(),
					"oidc-pool",
					true,
					false,
					"",
					nil,
					nil,
					nil,
				)
				require.NoError(t, poolErr)

				identity, idErr := b.GetID(
					context.Background(),
					pool.IdentityPoolID,
					"000000000000",
					nil,
				)
				require.NoError(t, idErr)
				identityID = identity.IdentityID
			} else {
				identityID = "us-east-1:nonexistent"
			}

			token, err := b.GetOpenIDToken(context.Background(), identityID, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, token.Token)
			assert.Equal(t, identityID, token.IdentityID)
		})
	}
}

func TestInMemoryBackend_GetCredentialsForIdentity_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetCredentialsForIdentity(context.Background(), "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_GetOpenIDToken_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.GetOpenIDToken(context.Background(), "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_GetOpenIDTokenForDeveloperIdentity_InvalidDuration(
	t *testing.T,
) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateIdentityPool(
		context.Background(),
		"dur-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		duration int64
		wantErr  bool
	}{
		{name: "zero_valid", duration: 0, wantErr: false},
		{name: "max_valid", duration: 86400, wantErr: false},
		{name: "negative", duration: -1, wantErr: true},
		{name: "too_large", duration: 86401, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, tokenErr := b.GetOpenIDTokenForDeveloperIdentity(context.Background(),
				pool.IdentityPoolID,
				"",
				map[string]string{"developer.example.com": "user-" + tt.name},
				tt.duration,
			)

			if tt.wantErr {
				require.Error(t, tokenErr)
				assert.ErrorIs(t, tokenErr, cognitoidentity.ErrInvalidParameter)
			} else {
				require.NoError(t, tokenErr)
			}
		})
	}
}

func TestInMemoryBackend_RandomAlphanumeric_NoBias(t *testing.T) {
	t.Parallel()

	// Verify that randomAlphanumeric produces strings of the requested length
	// and only uses alphanumeric characters.  We call it many times to exercise
	// the batch-read path and the rare fall-back path.
	for range 50 {
		s, err := cognitoidentity.ExportedRandomAlphanumeric(64)
		require.NoError(t, err)
		require.Len(t, s, 64)

		for _, c := range s {
			assert.True(
				t,
				(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'),
				"character %q is not alphanumeric", c,
			)
		}
	}
}

func TestGetCredentialsForIdentity_LoginMismatch(t *testing.T) {
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

func TestGetCredentialsForIdentity_LoginProviderAbsent(t *testing.T) {
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

func TestGetCredentialsForIdentity_MatchingLogins(t *testing.T) {
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

func TestGetCredentialsForIdentity_NilLogins(t *testing.T) {
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

func TestGetCredentialsForIdentity_EmptyLoginsBypass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seedLogins map[string]string
		reqLogins  map[string]string
		errTarget  error
		name       string
		wantErr    bool
	}{
		{
			name:       "authenticated_identity_empty_logins_rejected",
			seedLogins: map[string]string{"accounts.google.com": "google-token"},
			reqLogins:  nil,
			wantErr:    true,
			errTarget:  cognitoidentity.ErrNotAuthorized,
		},
		{
			name:       "authenticated_identity_matching_login_ok",
			seedLogins: map[string]string{"accounts.google.com": "google-token"},
			reqLogins:  map[string]string{"accounts.google.com": "google-token"},
			wantErr:    false,
		},
		{
			name:       "authenticated_identity_wrong_login_rejected",
			seedLogins: map[string]string{"accounts.google.com": "google-token"},
			reqLogins:  map[string]string{"accounts.google.com": "wrong"},
			wantErr:    true,
			errTarget:  cognitoidentity.ErrNotAuthorized,
		},
		{
			name:       "unauthenticated_identity_empty_logins_ok",
			seedLogins: nil,
			reqLogins:  nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

			pool, err := b.CreateIdentityPool(
				context.Background(),
				"creds-bypass-"+tt.name,
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
				tt.seedLogins,
			)
			require.NoError(t, err)

			creds, err := b.GetCredentialsForIdentity(
				context.Background(),
				identity.IdentityID,
				tt.reqLogins,
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, creds.AccessKeyID)
		})
	}
}
