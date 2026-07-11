package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// Test_CreateSecret_ClientRequestTokenIdempotency verifies the real AWS CreateSecret
// idempotency contract documented on CreateSecretInput.ClientRequestToken
// (aws-sdk-go-v2/service/secretsmanager/api_op_CreateSecret.go):
//
//   - If the token isn't already associated with a version, a new version is created.
//   - If a version with this token exists and its content matches the request, the
//     request is ignored (idempotent retry succeeds without creating anything new).
//   - If a version with this token exists but its content differs, the request fails
//     (CreateSecret cannot modify an existing version).
//
// This mock previously ignored ClientRequestToken entirely on name collisions, always
// returning ResourceExistsException even for a verbatim retry of a prior successful call.
func Test_CreateSecret_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wantErrIs      error
		name           string
		retryToken     string
		retrySecretStr string
		wantSameVerID  bool
	}{
		{
			name:           "same_token_same_content_is_ignored",
			retryToken:     "req-token-1",
			retrySecretStr: "hunter2",
			wantSameVerID:  true,
		},
		{
			name:           "same_token_different_content_fails",
			retryToken:     "req-token-1",
			retrySecretStr: "different-value",
			wantErrIs:      sm.ErrInvalidParameter,
		},
		{
			name:           "different_token_fails_as_resource_exists",
			retryToken:     "req-token-2",
			retrySecretStr: "hunter2",
			wantErrIs:      sm.ErrSecretAlreadyExists,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			secretName := "idempotent-" + tc.name

			first, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
				Name:               secretName,
				SecretString:       "hunter2",
				ClientRequestToken: "req-token-1",
			})
			require.NoError(t, err)

			second, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
				Name:               secretName,
				SecretString:       tc.retrySecretStr,
				ClientRequestToken: tc.retryToken,
			})

			if tc.wantErrIs != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErrIs)

				return
			}

			require.NoError(t, err)
			if tc.wantSameVerID {
				assert.Equal(t, first.VersionID, second.VersionID, "idempotent retry must return the same VersionId")
				assert.Equal(t, first.ARN, second.ARN)
			}

			// The idempotent retry must not have created a second version.
			out, err := b.ListSecretVersionIDs(context.Background(), &sm.ListSecretVersionIDsInput{
				SecretID: secretName,
			})
			require.NoError(t, err)
			assert.Len(t, out.Versions, 1, "idempotent retry must not create a new version")
		})
	}
}
