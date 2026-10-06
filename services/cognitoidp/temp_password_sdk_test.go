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

const tempPwd = "Tmp-Secret-9876!x"

func attrNames(attrs []types.AttributeType) []string {
	names := make([]string, 0, len(attrs))
	for _, a := range attrs {
		names = append(names, aws.ToString(a.Name))
	}

	return names
}

func TestTemporaryPassword_NotReturnedByUserOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(t *testing.T, c *cognitoidpsdk.Client, poolID string) []types.AttributeType
		name string
	}{
		{name: "admin_create_user", op: func(
			t *testing.T, c *cognitoidpsdk.Client, poolID string,
		) []types.AttributeType {
			t.Helper()

			out, err := c.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("fresh"), TemporaryPassword: aws.String(tempPwd),
			})
			require.NoError(t, err)

			return out.User.Attributes
		}},
		{name: "admin_get_user", op: func(
			t *testing.T, c *cognitoidpsdk.Client, poolID string,
		) []types.AttributeType {
			t.Helper()

			out, err := c.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u"),
			})
			require.NoError(t, err)

			return out.UserAttributes
		}},
		{name: "list_users", op: func(
			t *testing.T, c *cognitoidpsdk.Client, poolID string,
		) []types.AttributeType {
			t.Helper()

			out, err := c.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{UserPoolId: aws.String(poolID)})
			require.NoError(t, err)
			require.NotEmpty(t, out.Users)

			var all []types.AttributeType
			for _, u := range out.Users {
				all = append(all, u.Attributes...)
			}

			return all
		}},
		{name: "list_users_in_group", op: func(
			t *testing.T, c *cognitoidpsdk.Client, poolID string,
		) []types.AttributeType {
			t.Helper()

			_, err := c.CreateGroup(t.Context(), &cognitoidpsdk.CreateGroupInput{
				UserPoolId: aws.String(poolID), GroupName: aws.String("g"),
			})
			require.NoError(t, err)
			_, err = c.AdminAddUserToGroup(t.Context(), &cognitoidpsdk.AdminAddUserToGroupInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u"), GroupName: aws.String("g"),
			})
			require.NoError(t, err)

			out, err := c.ListUsersInGroup(t.Context(), &cognitoidpsdk.ListUsersInGroupInput{
				UserPoolId: aws.String(poolID), GroupName: aws.String("g"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.Users)

			return out.Users[0].Attributes
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCognitoIDPClient(t, newTestHandler(t))
			pool, err := c.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			poolID := aws.ToString(pool.UserPool.Id)
			_, err = c.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u"), TemporaryPassword: aws.String(tempPwd),
			})
			require.NoError(t, err)

			attrs := tt.op(t, c, poolID)
			raw, err := json.Marshal(attrs)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), tempPwd)
			assert.NotContains(t, attrNames(attrs), "custom:temporaryPassword")
		})
	}
}

func TestTemporaryPassword_AdminSetNonPermanentNotReturned(t *testing.T) {
	t.Parallel()

	c := newTestCognitoIDPClient(t, newTestHandler(t))
	pool, err := c.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	_, err = c.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"), MessageAction: types.MessageActionTypeSuppress,
	})
	require.NoError(t, err)

	_, err = c.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"), Password: aws.String(tempPwd),
	})
	require.NoError(t, err)

	got, err := c.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
	})
	require.NoError(t, err)
	assert.NotContains(t, attrNames(got.UserAttributes), "custom:temporaryPassword")
	assert.Equal(t, types.UserStatusTypeForceChangePassword, got.UserStatus)
}

func TestTemporaryPassword_NewPasswordChallengeStillWorks(t *testing.T) {
	t.Parallel()

	c := newTestCognitoIDPClient(t, newTestHandler(t))
	pool, err := c.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	app, err := c.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)

	_, err = c.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"), TemporaryPassword: aws.String(tempPwd),
	})
	require.NoError(t, err)

	auth, err := c.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: app.UserPoolClient.ClientId, AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": "u", "PASSWORD": tempPwd},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeNewPasswordRequired, auth.ChallengeName)

	done, err := c.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId: app.UserPoolClient.ClientId, ChallengeName: types.ChallengeNameTypeNewPasswordRequired,
		Session:            auth.Session,
		ChallengeResponses: map[string]string{"USERNAME": "u", "NEW_PASSWORD": "Brand-New-Pw-1!"},
	})
	require.NoError(t, err)
	require.NotNil(t, done.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(done.AuthenticationResult.AccessToken))
}

func TestTemporaryPassword_LegacySnapshotMigrates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	c := newTestCognitoIDPClient(t, h)
	pool, err := c.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	app, err := c.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)

	_, err = c.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"), TemporaryPassword: aws.String(tempPwd),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("u@example.com")}},
	})
	require.NoError(t, err)

	var snap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(h.Snapshot(t.Context()), &snap))

	var tables map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap["tables"], &tables))

	var users []map[string]any
	require.NoError(t, json.Unmarshal(tables["users"], &users))
	require.Len(t, users, 1)

	delete(users[0], "temporaryPassword")
	users[0]["attributes"].(map[string]any)["custom:temporaryPassword"] = tempPwd

	tables["users"], err = json.Marshal(users)
	require.NoError(t, err)
	snap["tables"], err = json.Marshal(tables)
	require.NoError(t, err)

	old, err := json.Marshal(snap)
	require.NoError(t, err)

	fresh := newTestHandler(t)
	require.NoError(t, fresh.Restore(t.Context(), old))

	c2 := newTestCognitoIDPClient(t, fresh)
	got, err := c2.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
	})
	require.NoError(t, err)
	assert.NotContains(t, attrNames(got.UserAttributes), "custom:temporaryPassword")
	assert.Contains(t, attrNames(got.UserAttributes), "email")

	auth, err := c2.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: app.UserPoolClient.ClientId, AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": "u", "PASSWORD": tempPwd},
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChallengeNameTypeNewPasswordRequired, auth.ChallengeName)
}
