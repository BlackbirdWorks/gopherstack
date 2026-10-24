package cognitoidp_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestAdminDeleteSoftwareToken(t *testing.T) {
	t.Parallel()

	b, pool, uc := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, uc.ClientID, "tina")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(b, "us-east-1"))

	secret, _, err := b.AssociateSoftwareToken(tokens.AccessToken, "")
	require.NoError(t, err)

	code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	_, err = b.VerifySoftwareToken(tokens.AccessToken, "", code)
	require.NoError(t, err)
	require.NoError(t, b.SetUserMFAPreference(tokens.AccessToken, false, true, "SOFTWARE_TOKEN_MFA"))

	_, err = client.AdminDeleteSoftwareToken(t.Context(), &cognitoidpsdk.AdminDeleteSoftwareTokenInput{
		UserPoolId: aws.String(pool.ID), Username: aws.String("tina"),
	})
	require.NoError(t, err)

	got, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(pool.ID), Username: aws.String("tina"),
	})
	require.NoError(t, err)
	assert.Empty(t, got.UserMFASettingList)
	assert.Empty(t, aws.ToString(got.PreferredMfaSetting))

	_, err = b.VerifySoftwareToken(tokens.AccessToken, "", code)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)

	_, err = client.AdminDeleteSoftwareToken(t.Context(), &cognitoidpsdk.AdminDeleteSoftwareTokenInput{
		UserPoolId: aws.String(pool.ID), Username: aws.String("tina"),
	})
	require.NoError(t, err)
}

func TestAdminDeleteSoftwareToken_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantType    any
		name        string
		username    string
		unknownPool bool
	}{
		{name: "unknown pool", unknownPool: true, username: "u", wantType: new(*types.ResourceNotFoundException)},
		{name: "unknown user", username: "ghost", wantType: new(*types.UserNotFoundException)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, pool, _ := setupTestPoolAndClient(t)
			client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(b, "us-east-1"))

			poolID := pool.ID
			if tc.unknownPool {
				poolID = "us-east-1_nope"
			}

			_, err := client.AdminDeleteSoftwareToken(t.Context(), &cognitoidpsdk.AdminDeleteSoftwareTokenInput{
				UserPoolId: aws.String(poolID), Username: aws.String(tc.username),
			})
			require.Error(t, err)

			switch want := tc.wantType.(type) {
			case **types.ResourceNotFoundException:
				require.ErrorAs(t, err, want)
			case **types.UserNotFoundException:
				require.ErrorAs(t, err, want)
			}
		})
	}
}
