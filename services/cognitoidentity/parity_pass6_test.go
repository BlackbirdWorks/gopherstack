package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// TestParity_GetCredentialsForIdentity_EmptyLoginsBypass verifies that an
// authenticated identity (one with logins on record) cannot obtain credentials
// with an empty Logins map, while an unauthenticated identity still can.
func TestParity_GetCredentialsForIdentity_EmptyLoginsBypass(t *testing.T) {
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
