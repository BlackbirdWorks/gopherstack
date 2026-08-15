package cognitoidp_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListUserPoolClients_SummaryShape proves ListUserPoolClients returns the
// real, minimal UserPoolClientDescription shape (ClientId/ClientName/UserPoolId
// only) rather than the full client record -- the real op never echoes
// ClientSecret or OAuth configuration in a list response
// (cognitoidentityprovider@v1.67.4 types.UserPoolClientDescription has no
// other members). Pre-fix, gopherstack emitted the full client record
// (including ClientSecret) for every list item.
func TestListUserPoolClients_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("summary-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	created, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId:     aws.String(poolID),
		ClientName:     aws.String("secret-client"),
		GenerateSecret: true,
	})
	require.NoError(t, err)
	clientID := aws.ToString(created.UserPoolClient.ClientId)
	require.NotEmpty(t, aws.ToString(created.UserPoolClient.ClientSecret), "sanity: create must generate a secret")

	listed, err := client.ListUserPoolClients(t.Context(), &cognitoidpsdk.ListUserPoolClientsInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, listed.UserPoolClients, 1)

	item := listed.UserPoolClients[0]
	assert.Equal(t, clientID, aws.ToString(item.ClientId))
	assert.Equal(t, "secret-client", aws.ToString(item.ClientName))
	assert.Equal(t, poolID, aws.ToString(item.UserPoolId))

	// Raw-body check: the real UserPoolClientDescription type has no
	// ClientSecret member at all, so a typed client can't observe a leak --
	// assert directly on the wire body that no such key is emitted.
	rec := doCognitoRequest(t, h, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	rawClients, ok := raw["UserPoolClients"].([]any)
	require.True(t, ok)
	require.Len(t, rawClients, 1)
	rawItem, ok := rawClients[0].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, rawItem, "ClientSecret")
	assert.NotContains(t, rawItem, "AllowedOAuthFlows")
}

// TestListUsers_MFAOptionsPopulated proves ListUsers emits MFAOptions, a
// real, non-deprecated UserType member the backend already tracks (set via
// AdminSetUserSettings/SetUserSettings) but never wired into the List
// response. Unlike GetUser/AdminGetUser's MFAOptions (explicitly documented
// by AWS as "no longer supported"), UserType.MFAOptions carries no such
// deprecation note.
func TestListUsers_MFAOptionsPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	poolID, clientID := setupHandlerPoolAndClient(t, h, "mfa-list-pool")

	_, err := client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientID),
		Username: aws.String("mfauser"),
		Password: aws.String("Pass1234!"),
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserSettings(t.Context(), &cognitoidpsdk.AdminSetUserSettingsInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("mfauser"),
		MFAOptions: []types.MFAOptionType{
			{DeliveryMedium: types.DeliveryMediumTypeSms, AttributeName: aws.String("phone_number")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, listed.Users, 1)

	got := listed.Users[0].MFAOptions
	require.Len(t, got, 1)
	assert.Equal(t, types.DeliveryMediumTypeSms, got[0].DeliveryMedium)
	assert.Equal(t, "phone_number", aws.ToString(got[0].AttributeName))
}

// TestListUsersInGroup_MFAOptionsPopulated is the same finding as
// TestListUsers_MFAOptionsPopulated, on ListUsersInGroup's adminUserJSON
// item shape (a separate struct from userSummary, and separately missing
// the field pre-fix).
func TestListUsersInGroup_MFAOptionsPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	poolID, clientID := setupHandlerPoolAndClient(t, h, "mfa-group-pool")

	_, err := client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientID),
		Username: aws.String("groupmfauser"),
		Password: aws.String("Pass1234!"),
	})
	require.NoError(t, err)

	_, err = client.CreateGroup(t.Context(), &cognitoidpsdk.CreateGroupInput{
		UserPoolId: aws.String(poolID),
		GroupName:  aws.String("mfa-group"),
	})
	require.NoError(t, err)

	_, err = client.AdminAddUserToGroup(t.Context(), &cognitoidpsdk.AdminAddUserToGroupInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("groupmfauser"),
		GroupName:  aws.String("mfa-group"),
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserSettings(t.Context(), &cognitoidpsdk.AdminSetUserSettingsInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("groupmfauser"),
		MFAOptions: []types.MFAOptionType{
			{DeliveryMedium: types.DeliveryMediumTypeSms, AttributeName: aws.String("phone_number")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListUsersInGroup(t.Context(), &cognitoidpsdk.ListUsersInGroupInput{
		UserPoolId: aws.String(poolID),
		GroupName:  aws.String("mfa-group"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Users, 1)

	got := listed.Users[0].MFAOptions
	require.Len(t, got, 1)
	assert.Equal(t, types.DeliveryMediumTypeSms, got[0].DeliveryMedium)
	assert.Equal(t, "phone_number", aws.ToString(got[0].AttributeName))
}
