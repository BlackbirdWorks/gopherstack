package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroups_RealClient covers GetGroup, UpdateGroup, DeleteGroup, ListGroups,
// AdminListGroupsForUser and AdminRemoveUserFromGroup -- 6 ops with no typed
// coverage anywhere in the repo (gopherstack-n3zi slice 1).
func TestGroups_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("group-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.CreateGroup(t.Context(), &cognitoidpsdk.CreateGroupInput{
		UserPoolId:  aws.String(poolID),
		GroupName:   aws.String("admins"),
		Description: aws.String("original"),
		Precedence:  aws.Int32(5),
	})
	require.NoError(t, err)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("groupuser"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminAddUserToGroup(t.Context(), &cognitoidpsdk.AdminAddUserToGroupInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username), GroupName: aws.String("admins"),
	})
	require.NoError(t, err)

	got, err := client.GetGroup(t.Context(), &cognitoidpsdk.GetGroupInput{
		UserPoolId: aws.String(poolID), GroupName: aws.String("admins"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Group)
	assert.Equal(t, "original", aws.ToString(got.Group.Description))
	assert.EqualValues(t, 5, aws.ToInt32(got.Group.Precedence))

	inGroups, err := client.AdminListGroupsForUser(t.Context(), &cognitoidpsdk.AdminListGroupsForUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	require.Len(t, inGroups.Groups, 1)
	assert.Equal(t, "admins", aws.ToString(inGroups.Groups[0].GroupName))

	_, err = client.UpdateGroup(t.Context(), &cognitoidpsdk.UpdateGroupInput{
		UserPoolId: aws.String(poolID), GroupName: aws.String("admins"),
		Description: aws.String("updated"), Precedence: aws.Int32(9),
	})
	require.NoError(t, err)

	updated, err := client.GetGroup(t.Context(), &cognitoidpsdk.GetGroupInput{
		UserPoolId: aws.String(poolID), GroupName: aws.String("admins"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updated.Group.Description))
	assert.EqualValues(t, 9, aws.ToInt32(updated.Group.Precedence))

	_, err = client.AdminRemoveUserFromGroup(t.Context(), &cognitoidpsdk.AdminRemoveUserFromGroupInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username), GroupName: aws.String("admins"),
	})
	require.NoError(t, err)

	afterRemoval, err := client.AdminListGroupsForUser(t.Context(), &cognitoidpsdk.AdminListGroupsForUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.Empty(t, afterRemoval.Groups)

	listed, err := client.ListGroups(t.Context(), &cognitoidpsdk.ListGroupsInput{UserPoolId: aws.String(poolID)})
	require.NoError(t, err)
	require.Len(t, listed.Groups, 1)

	_, err = client.DeleteGroup(t.Context(), &cognitoidpsdk.DeleteGroupInput{
		UserPoolId: aws.String(poolID), GroupName: aws.String("admins"),
	})
	require.NoError(t, err)

	listed2, err := client.ListGroups(t.Context(), &cognitoidpsdk.ListGroupsInput{UserPoolId: aws.String(poolID)})
	require.NoError(t, err)
	assert.Empty(t, listed2.Groups)
}

// TestIdentityProviders_RealClient covers Create/Describe/Update/Delete
// IdentityProvider, ListIdentityProviders and GetIdentityProviderByIdentifier.
func TestIdentityProviders_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("idp-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	created, err := client.CreateIdentityProvider(t.Context(), &cognitoidpsdk.CreateIdentityProviderInput{
		UserPoolId:   aws.String(poolID),
		ProviderName: aws.String("MyOIDC"),
		ProviderType: types.IdentityProviderTypeTypeOidc,
		ProviderDetails: map[string]string{
			"client_id":     "abc123",
			"client_secret": "shh",
			"oidc_issuer":   "https://issuer.example.com",
		},
		IdpIdentifiers: []string{"my-oidc-identifier"},
	})
	require.NoError(t, err)
	require.NotNil(t, created.IdentityProvider)
	assert.Equal(t, "abc123", created.IdentityProvider.ProviderDetails["client_id"])

	desc, err := client.DescribeIdentityProvider(t.Context(), &cognitoidpsdk.DescribeIdentityProviderInput{
		UserPoolId: aws.String(poolID), ProviderName: aws.String("MyOIDC"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.IdentityProviderTypeTypeOidc, desc.IdentityProvider.ProviderType)

	byIdentifier, err := client.GetIdentityProviderByIdentifier(
		t.Context(), &cognitoidpsdk.GetIdentityProviderByIdentifierInput{
			UserPoolId: aws.String(poolID), IdpIdentifier: aws.String("my-oidc-identifier"),
		})
	require.NoError(t, err)
	assert.Equal(t, "MyOIDC", aws.ToString(byIdentifier.IdentityProvider.ProviderName))

	listed, err := client.ListIdentityProviders(t.Context(), &cognitoidpsdk.ListIdentityProvidersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, listed.Providers, 1)
	assert.Equal(t, "MyOIDC", aws.ToString(listed.Providers[0].ProviderName))

	_, err = client.UpdateIdentityProvider(t.Context(), &cognitoidpsdk.UpdateIdentityProviderInput{
		UserPoolId: aws.String(poolID), ProviderName: aws.String("MyOIDC"),
		ProviderDetails: map[string]string{
			"client_id":     "abc123",
			"client_secret": "shh",
			"oidc_issuer":   "https://new-issuer.example.com",
		},
	})
	require.NoError(t, err)

	updatedDesc, err := client.DescribeIdentityProvider(t.Context(), &cognitoidpsdk.DescribeIdentityProviderInput{
		UserPoolId: aws.String(poolID), ProviderName: aws.String("MyOIDC"),
	})
	require.NoError(t, err)
	assert.Equal(t, "https://new-issuer.example.com", updatedDesc.IdentityProvider.ProviderDetails["oidc_issuer"])

	_, err = client.DeleteIdentityProvider(t.Context(), &cognitoidpsdk.DeleteIdentityProviderInput{
		UserPoolId: aws.String(poolID), ProviderName: aws.String("MyOIDC"),
	})
	require.NoError(t, err)

	listed2, err := client.ListIdentityProviders(t.Context(), &cognitoidpsdk.ListIdentityProvidersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.Providers)
}

// TestResourceServers_RealClient covers ListResourceServers,
// UpdateResourceServer and DeleteResourceServer.
func TestResourceServers_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("rs-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.CreateResourceServer(t.Context(), &cognitoidpsdk.CreateResourceServerInput{
		UserPoolId: aws.String(poolID), Identifier: aws.String("myapi"), Name: aws.String("My API"),
		Scopes: []types.ResourceServerScopeType{
			{ScopeName: aws.String("read"), ScopeDescription: aws.String("read access")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListResourceServers(t.Context(), &cognitoidpsdk.ListResourceServersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, listed.ResourceServers, 1)
	assert.Equal(t, "My API", aws.ToString(listed.ResourceServers[0].Name))

	updated, err := client.UpdateResourceServer(t.Context(), &cognitoidpsdk.UpdateResourceServerInput{
		UserPoolId: aws.String(poolID), Identifier: aws.String("myapi"), Name: aws.String("My API v2"),
		Scopes: []types.ResourceServerScopeType{
			{ScopeName: aws.String("read"), ScopeDescription: aws.String("read access")},
			{ScopeName: aws.String("write"), ScopeDescription: aws.String("write access")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.ResourceServer)
	assert.Equal(t, "My API v2", aws.ToString(updated.ResourceServer.Name))
	assert.Len(t, updated.ResourceServer.Scopes, 2)

	_, err = client.DeleteResourceServer(t.Context(), &cognitoidpsdk.DeleteResourceServerInput{
		UserPoolId: aws.String(poolID), Identifier: aws.String("myapi"),
	})
	require.NoError(t, err)

	listed2, err := client.ListResourceServers(t.Context(), &cognitoidpsdk.ListResourceServersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.ResourceServers)
}

// TestUserPoolClientAndDomain_RealClient covers UpdateUserPoolClient,
// DeleteUserPoolClient, DescribeUserPoolDomain, UpdateUserPoolDomain and
// DeleteUserPoolDomain.
func TestUserPoolClientAndDomain_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("domain-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	createdClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("orig-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(createdClient.UserPoolClient.ClientId)

	_, err = client.UpdateUserPoolClient(t.Context(), &cognitoidpsdk.UpdateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		ClientName: aws.String("renamed-client"),
	})
	require.NoError(t, err)

	describedClients, err := client.ListUserPoolClients(t.Context(), &cognitoidpsdk.ListUserPoolClientsInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, describedClients.UserPoolClients, 1)
	assert.Equal(t, "renamed-client", aws.ToString(describedClients.UserPoolClients[0].ClientName))

	_, err = client.DeleteUserPoolClient(t.Context(), &cognitoidpsdk.DeleteUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
	})
	require.NoError(t, err)

	afterDelete, err := client.ListUserPoolClients(t.Context(), &cognitoidpsdk.ListUserPoolClientsInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Empty(t, afterDelete.UserPoolClients)

	_, err = client.CreateUserPoolDomain(t.Context(), &cognitoidpsdk.CreateUserPoolDomainInput{
		UserPoolId: aws.String(poolID), Domain: aws.String("my-test-domain-xyz"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeUserPoolDomain(t.Context(), &cognitoidpsdk.DescribeUserPoolDomainInput{
		Domain: aws.String("my-test-domain-xyz"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.DomainDescription)
	assert.Equal(t, poolID, aws.ToString(desc.DomainDescription.UserPoolId))

	updated, err := client.UpdateUserPoolDomain(t.Context(), &cognitoidpsdk.UpdateUserPoolDomainInput{
		UserPoolId: aws.String(poolID), Domain: aws.String("my-test-domain-xyz"),
		CustomDomainConfig: &types.CustomDomainConfigType{
			CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/abc"),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updated.CloudFrontDomain))

	_, err = client.DeleteUserPoolDomain(t.Context(), &cognitoidpsdk.DeleteUserPoolDomainInput{
		UserPoolId: aws.String(poolID), Domain: aws.String("my-test-domain-xyz"),
	})
	require.NoError(t, err)

	// Real DescribeUserPoolDomain returns 200 with an empty DomainDescription
	// for an unknown domain rather than an error (documented AWS behavior,
	// mirrored by gopherstack's handleDescribeUserPoolDomain).
	afterDomainDelete, err := client.DescribeUserPoolDomain(t.Context(), &cognitoidpsdk.DescribeUserPoolDomainInput{
		Domain: aws.String("my-test-domain-xyz"),
	})
	require.NoError(t, err)
	require.NotNil(t, afterDomainDelete.DomainDescription)
	assert.Empty(t, aws.ToString(afterDomainDelete.DomainDescription.Domain))
}

// TestTagResource_RealClient covers TagResource and UntagResource.
func TestTagResource_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("tag-pool"),
	})
	require.NoError(t, err)
	poolArn := pool.UserPool.Arn

	_, err = client.TagResource(t.Context(), &cognitoidpsdk.TagResourceInput{
		ResourceArn: poolArn, Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(t.Context(), &cognitoidpsdk.ListTagsForResourceInput{
		ResourceArn: poolArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "test", listed.Tags["env"])

	_, err = client.UntagResource(t.Context(), &cognitoidpsdk.UntagResourceInput{
		ResourceArn: poolArn, TagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listed2, err := client.ListTagsForResource(t.Context(), &cognitoidpsdk.ListTagsForResourceInput{
		ResourceArn: poolArn,
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.Tags)
}

// TestAdminUserLifecycle_RealClient covers AdminDisableUser, AdminEnableUser,
// AdminUpdateUserAttributes, AdminDeleteUserAttributes, AdminUserGlobalSignOut
// and AdminResetUserPassword.
func TestAdminUserLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("admin-lifecycle-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("admin-lifecycle-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("admin-lc-user"),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("orig@example.com")}},
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		Password: aws.String("Perm1Pass!"), Permanent: true,
	})
	require.NoError(t, err)

	_, err = client.AdminUpdateUserAttributes(t.Context(), &cognitoidpsdk.AdminUpdateUserAttributesInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("updated@example.com")}},
	})
	require.NoError(t, err)

	afterUpdate, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	var email string
	for _, a := range afterUpdate.UserAttributes {
		if aws.ToString(a.Name) == "email" {
			email = aws.ToString(a.Value)
		}
	}
	assert.Equal(t, "updated@example.com", email)

	_, err = client.AdminDeleteUserAttributes(t.Context(), &cognitoidpsdk.AdminDeleteUserAttributesInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username), UserAttributeNames: []string{"email"},
	})
	require.NoError(t, err)

	afterDelete, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	for _, a := range afterDelete.UserAttributes {
		assert.NotEqual(t, "email", aws.ToString(a.Name))
	}

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": username, "PASSWORD": "Perm1Pass!"},
	})
	require.NoError(t, err)
	require.NotNil(t, auth.AuthenticationResult)

	_, err = client.AdminUserGlobalSignOut(t.Context(), &cognitoidpsdk.AdminUserGlobalSignOutInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)

	_, err = client.AdminDisableUser(t.Context(), &cognitoidpsdk.AdminDisableUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)

	disabled, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	_, err = client.AdminEnableUser(t.Context(), &cognitoidpsdk.AdminEnableUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)

	enabled, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)

	_, err = client.AdminResetUserPassword(t.Context(), &cognitoidpsdk.AdminResetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)

	afterReset, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.Equal(t, types.UserStatusTypeForceChangePassword, afterReset.UserStatus)
}

// TestSelfServiceUser_RealClient covers GetUser, UpdateUserAttributes,
// DeleteUserAttributes, ChangePassword, GetUserAttributeVerificationCode,
// VerifyUserAttribute, GlobalSignOut and DeleteUser.
func TestSelfServiceUser_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("self-service-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("self-service-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("self-user"),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("self@example.com")}},
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		Password: aws.String("Perm1Pass!"), Permanent: true,
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": username, "PASSWORD": "Perm1Pass!"},
	})
	require.NoError(t, err)
	accessToken := aws.ToString(auth.AuthenticationResult.AccessToken)

	gotUser, err := client.GetUser(t.Context(), &cognitoidpsdk.GetUserInput{AccessToken: aws.String(accessToken)})
	require.NoError(t, err)
	assert.Equal(t, username, aws.ToString(gotUser.Username))

	_, err = client.UpdateUserAttributes(t.Context(), &cognitoidpsdk.UpdateUserAttributesInput{
		AccessToken: aws.String(accessToken),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("given_name"), Value: aws.String("Ada")},
		},
	})
	require.NoError(t, err)

	afterUpdate, err := client.GetUser(t.Context(), &cognitoidpsdk.GetUserInput{AccessToken: aws.String(accessToken)})
	require.NoError(t, err)
	var givenName string
	for _, a := range afterUpdate.UserAttributes {
		if aws.ToString(a.Name) == "given_name" {
			givenName = aws.ToString(a.Value)
		}
	}
	assert.Equal(t, "Ada", givenName)

	_, err = client.DeleteUserAttributes(t.Context(), &cognitoidpsdk.DeleteUserAttributesInput{
		AccessToken: aws.String(accessToken), UserAttributeNames: []string{"given_name"},
	})
	require.NoError(t, err)

	afterDelete, err := client.GetUser(t.Context(), &cognitoidpsdk.GetUserInput{AccessToken: aws.String(accessToken)})
	require.NoError(t, err)
	for _, a := range afterDelete.UserAttributes {
		assert.NotEqual(t, "given_name", aws.ToString(a.Name))
	}

	_, err = client.ChangePassword(t.Context(), &cognitoidpsdk.ChangePasswordInput{
		AccessToken: aws.String(accessToken), PreviousPassword: aws.String("Perm1Pass!"),
		ProposedPassword: aws.String("Perm2Pass!"),
	})
	require.NoError(t, err)

	verCode, err := client.GetUserAttributeVerificationCode(
		t.Context(), &cognitoidpsdk.GetUserAttributeVerificationCodeInput{
			AccessToken: aws.String(accessToken), AttributeName: aws.String("email"),
		})
	require.NoError(t, err)
	require.NotNil(t, verCode.CodeDeliveryDetails)
	assert.Equal(t, types.DeliveryMediumTypeEmail, verCode.CodeDeliveryDetails.DeliveryMedium)

	// The real op never echoes the code (delivered out-of-band) -- fetch the
	// same value gopherstack's backend generated via the direct backend call,
	// same pattern as this package's existing attributes_test.go.
	code, _, _, err := h.Backend.GetUserAttributeVerificationCode(accessToken, "email")
	require.NoError(t, err)

	_, err = client.VerifyUserAttribute(t.Context(), &cognitoidpsdk.VerifyUserAttributeInput{
		AccessToken: aws.String(accessToken), AttributeName: aws.String("email"), Code: aws.String(code),
	})
	require.NoError(t, err)

	_, err = client.GlobalSignOut(t.Context(), &cognitoidpsdk.GlobalSignOutInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)

	_, err = client.GetUser(t.Context(), &cognitoidpsdk.GetUserInput{AccessToken: aws.String(accessToken)})
	require.Error(t, err, "access token must be revoked after GlobalSignOut")

	auth2, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": username, "PASSWORD": "Perm2Pass!"},
	})
	require.NoError(t, err)
	accessToken2 := aws.ToString(auth2.AuthenticationResult.AccessToken)

	_, err = client.DeleteUser(t.Context(), &cognitoidpsdk.DeleteUserInput{AccessToken: aws.String(accessToken2)})
	require.NoError(t, err)

	_, err = client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.Error(t, err, "user must be gone after DeleteUser")
}

// TestSignUpConfirmationFlow_RealClient covers ConfirmSignUp,
// ResendConfirmationCode, ForgotPassword and ConfirmForgotPassword.
func TestSignUpConfirmationFlow_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("confirm-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("confirm-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	_, err = client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientID), Username: aws.String("confirm-user"), Password: aws.String("Pass1234!"),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("cu@example.com")}},
	})
	require.NoError(t, err)

	// ResendConfirmationCode covers its own op; the actual code used to
	// confirm is fetched via the backend since real AWS never echoes it.
	_, err = client.ResendConfirmationCode(t.Context(), &cognitoidpsdk.ResendConfirmationCodeInput{
		ClientId: aws.String(clientID), Username: aws.String("confirm-user"),
	})
	require.NoError(t, err)

	user, err := h.Backend.AdminGetUser(poolID, "confirm-user")
	require.NoError(t, err)

	_, err = client.ConfirmSignUp(t.Context(), &cognitoidpsdk.ConfirmSignUpInput{
		ClientId: aws.String(clientID), Username: aws.String("confirm-user"),
		ConfirmationCode: aws.String(user.ConfirmCode),
	})
	require.NoError(t, err)

	confirmed, err := client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("confirm-user"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.UserStatusTypeConfirmed, confirmed.UserStatus)

	_, err = client.ForgotPassword(t.Context(), &cognitoidpsdk.ForgotPasswordInput{
		ClientId: aws.String(clientID), Username: aws.String("confirm-user"),
	})
	require.NoError(t, err)

	resetCode, err := h.Backend.ForgotPassword(clientID, "confirm-user")
	require.NoError(t, err)

	_, err = client.ConfirmForgotPassword(t.Context(), &cognitoidpsdk.ConfirmForgotPasswordInput{
		ClientId: aws.String(clientID), Username: aws.String("confirm-user"),
		ConfirmationCode: aws.String(resetCode), Password: aws.String("NewPass123!"),
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": "confirm-user", "PASSWORD": "NewPass123!"},
	})
	require.NoError(t, err)
	require.NotNil(t, auth.AuthenticationResult, "login with the reset password must succeed")
}

// TestDevices_RealClient covers GetDevice, ListDevices, UpdateDeviceStatus,
// ForgetDevice, AdminUpdateDeviceStatus and AdminForgetDevice.
func TestDevices_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("device-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("device-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("device-user"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		Password: aws.String("Perm1Pass!"), Permanent: true,
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": username, "PASSWORD": "Perm1Pass!"},
	})
	require.NoError(t, err)
	accessToken := aws.ToString(auth.AuthenticationResult.AccessToken)

	confirmed, err := client.ConfirmDevice(t.Context(), &cognitoidpsdk.ConfirmDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
		DeviceName: aws.String("laptop"),
	})
	require.NoError(t, err)
	_ = confirmed

	got, err := client.GetDevice(t.Context(), &cognitoidpsdk.GetDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Device)
	assert.Equal(t, "device-key-1", aws.ToString(got.Device.DeviceKey))

	listed, err := client.ListDevices(t.Context(), &cognitoidpsdk.ListDevicesInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	require.Len(t, listed.Devices, 1)

	_, err = client.UpdateDeviceStatus(t.Context(), &cognitoidpsdk.UpdateDeviceStatusInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
		DeviceRememberedStatus: types.DeviceRememberedStatusTypeRemembered,
	})
	require.NoError(t, err)

	afterUpdate, err := client.GetDevice(t.Context(), &cognitoidpsdk.GetDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
	})
	require.NoError(t, err)
	var deviceStatus string
	for _, a := range afterUpdate.Device.DeviceAttributes {
		if aws.ToString(a.Name) == "device_status" {
			deviceStatus = aws.ToString(a.Value)
		}
	}
	assert.Equal(t, "remembered", deviceStatus,
		"a real client must be able to observe UpdateDeviceStatus's effect via DeviceAttributes")

	_, err = client.AdminUpdateDeviceStatus(t.Context(), &cognitoidpsdk.AdminUpdateDeviceStatusInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username), DeviceKey: aws.String("device-key-1"),
		DeviceRememberedStatus: types.DeviceRememberedStatusTypeNotRemembered,
	})
	require.NoError(t, err)

	_, err = client.ForgetDevice(t.Context(), &cognitoidpsdk.ForgetDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
	})
	require.NoError(t, err)

	_, err = client.GetDevice(t.Context(), &cognitoidpsdk.GetDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-1"),
	})
	require.Error(t, err)

	_, err = client.ConfirmDevice(t.Context(), &cognitoidpsdk.ConfirmDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-2"),
		DeviceName: aws.String("phone"),
	})
	require.NoError(t, err)

	_, err = client.AdminForgetDevice(t.Context(), &cognitoidpsdk.AdminForgetDeviceInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username), DeviceKey: aws.String("device-key-2"),
	})
	require.NoError(t, err)

	_, err = client.GetDevice(t.Context(), &cognitoidpsdk.GetDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: aws.String("device-key-2"),
	})
	require.Error(t, err)
}

// TestMFAPreferences_RealClient covers SetUserMFAPreference,
// GetUserAuthFactors, AdminSetUserMFAPreference, AdminGetUserAuthFactors and
// GetUserPoolMfaConfig.
func TestMFAPreferences_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("mfa-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("mfa-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("mfa-user"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		Password: aws.String("Perm1Pass!"), Permanent: true,
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": username, "PASSWORD": "Perm1Pass!"},
	})
	require.NoError(t, err)
	accessToken := aws.ToString(auth.AuthenticationResult.AccessToken)

	_, err = client.SetUserMFAPreference(t.Context(), &cognitoidpsdk.SetUserMFAPreferenceInput{
		AccessToken: aws.String(accessToken),
		SMSMfaSettings: &types.SMSMfaSettingsType{
			Enabled: true, PreferredMfa: true,
		},
	})
	require.NoError(t, err)

	factors, err := client.GetUserAuthFactors(t.Context(), &cognitoidpsdk.GetUserAuthFactorsInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	assert.Equal(t, username, aws.ToString(factors.Username))

	_, err = client.AdminSetUserMFAPreference(t.Context(), &cognitoidpsdk.AdminSetUserMFAPreferenceInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		SMSMfaSettings: &types.SMSMfaSettingsType{Enabled: true, PreferredMfa: true},
	})
	require.NoError(t, err)

	adminFactors, err := client.AdminGetUserAuthFactors(t.Context(), &cognitoidpsdk.AdminGetUserAuthFactorsInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.Equal(t, username, aws.ToString(adminFactors.Username))

	_, err = client.UpdateUserPool(t.Context(), &cognitoidpsdk.UpdateUserPoolInput{
		UserPoolId: aws.String(poolID), MfaConfiguration: types.UserPoolMfaTypeOptional,
	})
	require.NoError(t, err)

	mfaCfg, err := client.GetUserPoolMfaConfig(t.Context(), &cognitoidpsdk.GetUserPoolMfaConfigInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.UserPoolMfaTypeOptional, mfaCfg.MfaConfiguration)
}

// TestUICustomization_RealClient covers GetUICustomization and
// SetUICustomization.
func TestUICustomization_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("ui-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("ui-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	_, err = client.SetUICustomization(t.Context(), &cognitoidpsdk.SetUICustomizationInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		CSS: aws.String(".label-customizable {font-weight: 400;}"),
	})
	require.NoError(t, err)

	got, err := client.GetUICustomization(t.Context(), &cognitoidpsdk.GetUICustomizationInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
	})
	require.NoError(t, err)
	require.NotNil(t, got.UICustomization)
	assert.Equal(t, ".label-customizable {font-weight: 400;}", aws.ToString(got.UICustomization.CSS))
}
