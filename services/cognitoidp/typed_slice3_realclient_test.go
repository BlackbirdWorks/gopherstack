package cognitoidp_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/document"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestSlice3_Cognitoidp_RealClient covers cognitoidp's remaining 32
// typed-client-uncovered ops (gopherstack-n3zi slice 3), grouped by shared
// setup into subtests -- each subtest creates real state through the
// typed aws-sdk-go-v2 client and asserts decoded response values.
func TestSlice3_Cognitoidp_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testManagedLoginBrandingRealClient, "managed_login_branding"},
		{testTermsRealClient, "terms"},
		{testUserImportJobsRealClient, "user_import_jobs"},
		{testUserPoolReplicasRealClient, "user_pool_replicas"},
		{testWebAuthnRealClient, "webauthn"},
		{testAuthEventFeedbackRealClient, "auth_event_feedback"},
		{testAdminProviderLinkingRealClient, "admin_provider_linking"},
		{testAdminRespondToAuthChallengeRealClient, "admin_respond_to_auth_challenge"},
		{testProvisionedLimitsRealClient, "provisioned_limits"},
		{testLogDeliveryConfigurationRealClient, "log_delivery_configuration"},
		{testGetTokensFromRefreshTokenRealClient, "get_tokens_from_refresh_token"},
		{testSetUserSettingsRealClient, "set_user_settings"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// decodeCognitoDocument unmarshals a response smithy document.Interface field
// back to its JSON text so tests can assert on its content.
func decodeCognitoDocument(t *testing.T, doc interface{ UnmarshalSmithyDocument(v any) error }) string {
	t.Helper()

	var v any
	require.NoError(t, doc.UnmarshalSmithyDocument(&v))
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

// testManagedLoginBrandingRealClient covers CreateManagedLoginBranding,
// DescribeManagedLoginBranding, DescribeManagedLoginBrandingByClient,
// UpdateManagedLoginBranding and DeleteManagedLoginBranding.
func testManagedLoginBrandingRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("mlb-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("mlb-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.CreateManagedLoginBranding(t.Context(), &cognitoidpsdk.CreateManagedLoginBrandingInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(clientID),
		Settings:   document.NewLazyDocument(map[string]any{"components": map[string]any{"favicon": "on"}}),
		Assets: []types.AssetType{
			{Category: types.AssetCategoryTypeFaviconIco, ColorMode: types.ColorSchemeModeTypeLight,
				Extension: types.AssetExtensionTypeIco, ResourceId: aws.String("favicon-1")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.ManagedLoginBranding)
	brandingID := aws.ToString(created.ManagedLoginBranding.ManagedLoginBrandingId)
	require.NotEmpty(t, brandingID)
	require.Len(t, created.ManagedLoginBranding.Assets, 1)
	assert.Equal(t, types.AssetCategoryTypeFaviconIco, created.ManagedLoginBranding.Assets[0].Category)
	assert.Equal(t, "favicon-1", aws.ToString(created.ManagedLoginBranding.Assets[0].ResourceId))
	assert.JSONEq(t, `{"components":{"favicon":"on"}}`, decodeCognitoDocument(t, created.ManagedLoginBranding.Settings))

	described, err := client.DescribeManagedLoginBranding(t.Context(), &cognitoidpsdk.DescribeManagedLoginBrandingInput{
		UserPoolId: aws.String(poolID), ManagedLoginBrandingId: aws.String(brandingID),
	})
	require.NoError(t, err)
	assert.Equal(t, brandingID, aws.ToString(described.ManagedLoginBranding.ManagedLoginBrandingId))

	byClient, err := client.DescribeManagedLoginBrandingByClient(
		t.Context(), &cognitoidpsdk.DescribeManagedLoginBrandingByClientInput{
			UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		})
	require.NoError(t, err)
	assert.Equal(t, brandingID, aws.ToString(byClient.ManagedLoginBranding.ManagedLoginBrandingId))

	updated, err := client.UpdateManagedLoginBranding(t.Context(), &cognitoidpsdk.UpdateManagedLoginBrandingInput{
		UserPoolId:               aws.String(poolID),
		ManagedLoginBrandingId:   aws.String(brandingID),
		UseCognitoProvidedValues: true,
	})
	require.NoError(t, err)
	assert.True(t, updated.ManagedLoginBranding.UseCognitoProvidedValues)

	_, err = client.DeleteManagedLoginBranding(t.Context(), &cognitoidpsdk.DeleteManagedLoginBrandingInput{
		UserPoolId: aws.String(poolID), ManagedLoginBrandingId: aws.String(brandingID),
	})
	require.NoError(t, err)

	_, err = client.DescribeManagedLoginBranding(t.Context(), &cognitoidpsdk.DescribeManagedLoginBrandingInput{
		UserPoolId: aws.String(poolID), ManagedLoginBrandingId: aws.String(brandingID),
	})
	require.Error(t, err)
}

// testTermsRealClient covers DeleteTerms, ListTerms and UpdateTerms (CreateTerms
// is already typed-covered elsewhere and is only used here for setup).
func testTermsRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("terms-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("terms-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.CreateTerms(t.Context(), &cognitoidpsdk.CreateTermsInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		TermsName: aws.String("privacy-v1"), TermsSource: types.TermsSourceTypeLink,
		Enforcement: types.TermsEnforcementTypeNone,
		Links:       map[string]string{"en-us": "https://example.com/privacy"},
	})
	require.NoError(t, err)
	termsID := aws.ToString(created.Terms.TermsId)
	require.NotEmpty(t, termsID)

	listed, err := client.ListTerms(t.Context(), &cognitoidpsdk.ListTermsInput{UserPoolId: aws.String(poolID)})
	require.NoError(t, err)
	require.Len(t, listed.Terms, 1)
	assert.Equal(t, "privacy-v1", aws.ToString(listed.Terms[0].TermsName))

	updated, err := client.UpdateTerms(t.Context(), &cognitoidpsdk.UpdateTermsInput{
		UserPoolId: aws.String(poolID), TermsId: aws.String(termsID),
		TermsName: aws.String("privacy-v2"),
		Links:     map[string]string{"en-us": "https://example.com/privacy-v2"},
	})
	require.NoError(t, err)
	assert.Equal(t, "privacy-v2", aws.ToString(updated.Terms.TermsName))
	assert.Equal(t, "https://example.com/privacy-v2", updated.Terms.Links["en-us"])

	_, err = client.DeleteTerms(t.Context(), &cognitoidpsdk.DeleteTermsInput{
		UserPoolId: aws.String(poolID), TermsId: aws.String(termsID),
	})
	require.NoError(t, err)

	listed2, err := client.ListTerms(t.Context(), &cognitoidpsdk.ListTermsInput{UserPoolId: aws.String(poolID)})
	require.NoError(t, err)
	assert.Empty(t, listed2.Terms)
}

// testUserImportJobsRealClient covers CreateUserImportJob, DescribeUserImportJob,
// ListUserImportJobs, StartUserImportJob, StopUserImportJob and GetCSVHeader.
func testUserImportJobsRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("import-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	header, err := client.GetCSVHeader(t.Context(), &cognitoidpsdk.GetCSVHeaderInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Contains(t, header.CSVHeader, "cognito:username")

	created, err := client.CreateUserImportJob(t.Context(), &cognitoidpsdk.CreateUserImportJobInput{
		UserPoolId: aws.String(poolID), JobName: aws.String("bulk-1"),
		CloudWatchLogsRoleArn: aws.String("arn:aws:iam::000000000000:role/import-role"),
	})
	require.NoError(t, err)
	jobID := aws.ToString(created.UserImportJob.JobId)
	require.NotEmpty(t, jobID)
	assert.Equal(t, "bulk-1", aws.ToString(created.UserImportJob.JobName))

	described, err := client.DescribeUserImportJob(t.Context(), &cognitoidpsdk.DescribeUserImportJobInput{
		UserPoolId: aws.String(poolID), JobId: aws.String(jobID),
	})
	require.NoError(t, err)
	assert.Equal(t, jobID, aws.ToString(described.UserImportJob.JobId))

	started, err := client.StartUserImportJob(t.Context(), &cognitoidpsdk.StartUserImportJobInput{
		UserPoolId: aws.String(poolID), JobId: aws.String(jobID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, started.UserImportJob.Status)

	listed, err := client.ListUserImportJobs(t.Context(), &cognitoidpsdk.ListUserImportJobsInput{
		UserPoolId: aws.String(poolID), MaxResults: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, listed.UserImportJobs, 1)
	assert.Equal(t, jobID, aws.ToString(listed.UserImportJobs[0].JobId))

	stopped, err := client.StopUserImportJob(t.Context(), &cognitoidpsdk.StopUserImportJobInput{
		UserPoolId: aws.String(poolID), JobId: aws.String(jobID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, stopped.UserImportJob.Status)
}

// testUserPoolReplicasRealClient covers CreateUserPoolReplica, ListUserPoolReplicas,
// UpdateUserPoolReplica and DeleteUserPoolReplica.
func testUserPoolReplicasRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("replica-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	created, err := client.CreateUserPoolReplica(t.Context(), &cognitoidpsdk.CreateUserPoolReplicaInput{
		UserPoolId: aws.String(poolID), RegionName: aws.String("us-west-2"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.UserPoolReplica)
	assert.Equal(t, "us-west-2", aws.ToString(created.UserPoolReplica.RegionName))

	listed, err := client.ListUserPoolReplicas(t.Context(), &cognitoidpsdk.ListUserPoolReplicasInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, listed.UserPoolReplicas, 1)
	assert.Equal(t, "us-west-2", aws.ToString(listed.UserPoolReplicas[0].RegionName))

	updated, err := client.UpdateUserPoolReplica(t.Context(), &cognitoidpsdk.UpdateUserPoolReplicaInput{
		UserPoolId: aws.String(poolID), RegionName: aws.String("us-west-2"),
		Status: types.UpdateReplicaStatusTypeActive,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ReplicaStatusTypeActive, updated.UserPoolReplica.Status)

	_, err = client.DeleteUserPoolReplica(t.Context(), &cognitoidpsdk.DeleteUserPoolReplicaInput{
		UserPoolId: aws.String(poolID), RegionName: aws.String("us-west-2"),
	})
	require.NoError(t, err)

	listed2, err := client.ListUserPoolReplicas(t.Context(), &cognitoidpsdk.ListUserPoolReplicasInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.UserPoolReplicas)
}

// testWebAuthnRealClient covers CompleteWebAuthnRegistration and
// DeleteWebAuthnCredential (StartWebAuthnRegistration/ListWebAuthnCredentials
// are already typed-covered and used here only for setup/verification).
func testWebAuthnRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	accessToken := selfServiceAccessToken(t, client, "webauthn-pool", "webauthn-user")

	opts, err := client.StartWebAuthnRegistration(t.Context(), &cognitoidpsdk.StartWebAuthnRegistrationInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	require.NotNil(t, opts.CredentialCreationOptions)

	credential := map[string]any{
		"id": "cred-1",
		"response": map[string]any{
			"transports": []string{"internal"},
		},
	}

	_, err = client.CompleteWebAuthnRegistration(t.Context(), &cognitoidpsdk.CompleteWebAuthnRegistrationInput{
		AccessToken: aws.String(accessToken),
		Credential:  document.NewLazyDocument(credential),
	})
	require.NoError(t, err)

	listed, err := client.ListWebAuthnCredentials(t.Context(), &cognitoidpsdk.ListWebAuthnCredentialsInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	require.Len(t, listed.Credentials, 1)
	assert.Equal(t, "cred-1", aws.ToString(listed.Credentials[0].CredentialId))

	_, err = client.DeleteWebAuthnCredential(t.Context(), &cognitoidpsdk.DeleteWebAuthnCredentialInput{
		AccessToken: aws.String(accessToken), CredentialId: aws.String("cred-1"),
	})
	require.NoError(t, err)

	listed2, err := client.ListWebAuthnCredentials(t.Context(), &cognitoidpsdk.ListWebAuthnCredentialsInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.Credentials)
}

// testAuthEventFeedbackRealClient covers AdminListUserAuthEvents,
// AdminUpdateAuthEventFeedback and UpdateAuthEventFeedback. This backend does
// not hook sign-in flows to synthesize risk events (see auth_events.go), so
// events are seeded directly via the test-only SeedAuthEventForTest.
func testAuthEventFeedbackRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("auth-event-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("event-user"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	h.Backend.SeedAuthEventForTest(poolID, username, &cognitoidp.AuthEvent{
		EventID: "event-1", EventType: "SignIn", EventResponse: "Pass",
	})

	listed, err := client.AdminListUserAuthEvents(t.Context(), &cognitoidpsdk.AdminListUserAuthEventsInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	require.Len(t, listed.AuthEvents, 1)
	assert.Equal(t, "event-1", aws.ToString(listed.AuthEvents[0].EventId))
	assert.Equal(t, types.EventResponseTypePass, listed.AuthEvents[0].EventResponse)

	_, err = client.AdminUpdateAuthEventFeedback(t.Context(), &cognitoidpsdk.AdminUpdateAuthEventFeedbackInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		EventId: aws.String("event-1"), FeedbackValue: types.FeedbackValueTypeValid,
	})
	require.NoError(t, err)

	afterAdminFeedback, err := client.AdminListUserAuthEvents(t.Context(), &cognitoidpsdk.AdminListUserAuthEventsInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	require.NotNil(t, afterAdminFeedback.AuthEvents[0].EventFeedback)
	assert.Equal(t, types.FeedbackValueTypeValid, afterAdminFeedback.AuthEvents[0].EventFeedback.FeedbackValue)

	_, err = client.UpdateAuthEventFeedback(t.Context(), &cognitoidpsdk.UpdateAuthEventFeedbackInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
		EventId: aws.String("event-1"), FeedbackValue: types.FeedbackValueTypeInvalid,
		FeedbackToken: aws.String("out-of-band-token"),
	})
	require.NoError(t, err)

	afterFeedback, err := client.AdminListUserAuthEvents(t.Context(), &cognitoidpsdk.AdminListUserAuthEventsInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	assert.Equal(t, types.FeedbackValueTypeInvalid, afterFeedback.AuthEvents[0].EventFeedback.FeedbackValue)
}

// testAdminProviderLinkingRealClient covers AdminDisableProviderForUser and
// AdminLinkProviderForUser. Neither op's effect is exposed on any read op's
// wire shape today (LinkedProviders is stored internally but never
// serialized -- see identity_providers.go), so these are legitimate
// void-result assertions: a real client must see the call succeed and must
// see it fail against a nonexistent pool/user.
func testAdminProviderLinkingRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("provider-link-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("link-user"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	_, err = client.AdminDisableProviderForUser(t.Context(), &cognitoidpsdk.AdminDisableProviderForUserInput{
		UserPoolId: aws.String(poolID),
		User: &types.ProviderUserIdentifierType{
			ProviderName: aws.String("Facebook"), ProviderAttributeValue: aws.String(username),
		},
	})
	require.NoError(t, err)

	_, err = client.AdminLinkProviderForUser(t.Context(), &cognitoidpsdk.AdminLinkProviderForUserInput{
		UserPoolId:      aws.String(poolID),
		DestinationUser: &types.ProviderUserIdentifierType{ProviderAttributeValue: aws.String(username)},
		SourceUser: &types.ProviderUserIdentifierType{
			ProviderName: aws.String("Facebook"), ProviderAttributeName: aws.String("Cognito_Subject"),
			ProviderAttributeValue: aws.String("fb-1234"),
		},
	})
	require.NoError(t, err)

	_, err = client.AdminLinkProviderForUser(t.Context(), &cognitoidpsdk.AdminLinkProviderForUserInput{
		UserPoolId:      aws.String(poolID),
		DestinationUser: &types.ProviderUserIdentifierType{ProviderAttributeValue: aws.String("no-such-user")},
		SourceUser: &types.ProviderUserIdentifierType{
			ProviderName: aws.String("Facebook"), ProviderAttributeName: aws.String("Cognito_Subject"),
			ProviderAttributeValue: aws.String("fb-5678"),
		},
	})
	require.Error(t, err)
}

// testAdminRespondToAuthChallengeRealClient covers AdminRespondToAuthChallenge
// by completing a real NEW_PASSWORD_REQUIRED challenge triggered from
// AdminInitiateAuth against a user created with a temporary password.
func testAdminRespondToAuthChallengeRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("admin-challenge-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("admin-challenge-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("challenge-user"),
		TemporaryPassword: aws.String("TempPass1!"),
	})
	require.NoError(t, err)
	username := aws.ToString(created.User.Username)

	auth, err := client.AdminInitiateAuth(t.Context(), &cognitoidpsdk.AdminInitiateAuthInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": username, "PASSWORD": "TempPass1!",
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeNewPasswordRequired, auth.ChallengeName)
	require.NotEmpty(t, aws.ToString(auth.Session))

	responded, err := client.AdminRespondToAuthChallenge(t.Context(), &cognitoidpsdk.AdminRespondToAuthChallengeInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeNewPasswordRequired,
		Session:       auth.Session,
		ChallengeResponses: map[string]string{
			"NEW_PASSWORD": "NewPass1!", "USERNAME": username,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, responded.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(responded.AuthenticationResult.AccessToken))

	relogin, err := client.AdminInitiateAuth(t.Context(), &cognitoidpsdk.AdminInitiateAuthInput{
		UserPoolId: aws.String(poolID), ClientId: aws.String(clientID),
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": username, "PASSWORD": "NewPass1!",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, relogin.AuthenticationResult, "login with the new password must succeed")
}

// testProvisionedLimitsRealClient covers GetProvisionedLimit and UpdateProvisionedLimit.
func testProvisionedLimitsRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	limitDef := types.LimitDefinitionType{
		LimitClass: types.LimitClassApiCategory,
		Attributes: map[string]string{"Category": "UserAuthentication"},
	}

	got, err := client.GetProvisionedLimit(t.Context(), &cognitoidpsdk.GetProvisionedLimitInput{
		LimitDefinition: &limitDef,
	})
	require.NoError(t, err)
	require.NotNil(t, got.Limit)
	assert.EqualValues(t, 120, got.Limit.FreeLimitValue)
	assert.EqualValues(t, 120, got.Limit.ProvisionedLimitValue)

	updated, err := client.UpdateProvisionedLimit(t.Context(), &cognitoidpsdk.UpdateProvisionedLimitInput{
		LimitDefinition:     &limitDef,
		RequestedLimitValue: 500,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 500, updated.Limit.ProvisionedLimitValue)
	assert.EqualValues(t, 120, updated.Limit.FreeLimitValue)

	got2, err := client.GetProvisionedLimit(t.Context(), &cognitoidpsdk.GetProvisionedLimitInput{
		LimitDefinition: &limitDef,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 500, got2.Limit.ProvisionedLimitValue)

	_, err = client.UpdateProvisionedLimit(t.Context(), &cognitoidpsdk.UpdateProvisionedLimitInput{
		LimitDefinition: &types.LimitDefinitionType{
			LimitClass: types.LimitClassApiCategory,
			Attributes: map[string]string{"Category": "UserAuthentication"},
		},
		RequestedLimitValue: 999999,
	})
	require.Error(t, err, "exceeding the account-level max must be rejected")
}

// testLogDeliveryConfigurationRealClient covers GetLogDeliveryConfiguration
// and SetLogDeliveryConfiguration.
func testLogDeliveryConfigurationRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("log-delivery-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	// Before any SetLogDeliveryConfiguration call, the backend's raw store is
	// a genuinely empty map; since neither UserPoolId nor LogConfigurations
	// has been populated, the top-level LogDeliveryConfiguration wrapper
	// itself is correctly absent from the wire (nil), not merely empty.
	empty, err := client.GetLogDeliveryConfiguration(
		t.Context(), &cognitoidpsdk.GetLogDeliveryConfigurationInput{UserPoolId: aws.String(poolID)},
	)
	require.NoError(t, err)
	assert.Nil(t, empty.LogDeliveryConfiguration)

	set, err := client.SetLogDeliveryConfiguration(t.Context(), &cognitoidpsdk.SetLogDeliveryConfigurationInput{
		UserPoolId: aws.String(poolID),
		LogConfigurations: []types.LogConfigurationType{
			{
				EventSource: types.EventSourceNameUserNotification,
				LogLevel:    types.LogLevelError,
				CloudWatchLogsConfiguration: &types.CloudWatchLogsConfigurationType{
					LogGroupArn: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:cognito"),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, set.LogDeliveryConfiguration.LogConfigurations, 1)
	assert.Equal(t,
		types.EventSourceNameUserNotification, set.LogDeliveryConfiguration.LogConfigurations[0].EventSource)
	assert.Equal(t, poolID, aws.ToString(set.LogDeliveryConfiguration.UserPoolId))

	got, err := client.GetLogDeliveryConfiguration(
		t.Context(), &cognitoidpsdk.GetLogDeliveryConfigurationInput{UserPoolId: aws.String(poolID)},
	)
	require.NoError(t, err)
	require.Len(t, got.LogDeliveryConfiguration.LogConfigurations, 1)
	assert.Equal(t, types.LogLevelError, got.LogDeliveryConfiguration.LogConfigurations[0].LogLevel)
	require.NotNil(t, got.LogDeliveryConfiguration.LogConfigurations[0].CloudWatchLogsConfiguration)
	assert.Equal(t, "arn:aws:logs:us-east-1:000000000000:log-group:cognito",
		aws.ToString(got.LogDeliveryConfiguration.LogConfigurations[0].CloudWatchLogsConfiguration.LogGroupArn))
}

// testGetTokensFromRefreshTokenRealClient covers GetTokensFromRefreshToken.
func testGetTokensFromRefreshTokenRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	clientID, refreshToken := selfServiceRefreshToken(t, client, "refresh-pool", "refresh-user")

	tokens, err := client.GetTokensFromRefreshToken(t.Context(), &cognitoidpsdk.GetTokensFromRefreshTokenInput{
		ClientId: aws.String(clientID), RefreshToken: aws.String(refreshToken),
	})
	require.NoError(t, err)
	require.NotNil(t, tokens.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(tokens.AuthenticationResult.AccessToken))
	assert.NotEmpty(t, aws.ToString(tokens.AuthenticationResult.IdToken))

	_, err = client.GetTokensFromRefreshToken(t.Context(), &cognitoidpsdk.GetTokensFromRefreshTokenInput{
		ClientId: aws.String(clientID), RefreshToken: aws.String("not-a-real-token"),
	})
	require.Error(t, err)
}

// testSetUserSettingsRealClient covers SetUserSettings.
func testSetUserSettingsRealClient(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	accessToken := selfServiceAccessToken(t, client, "settings-pool", "settings-user")

	_, err := client.SetUserSettings(t.Context(), &cognitoidpsdk.SetUserSettingsInput{
		AccessToken: aws.String(accessToken),
		MFAOptions: []types.MFAOptionType{
			{DeliveryMedium: types.DeliveryMediumTypeSms, AttributeName: aws.String("phone_number")},
		},
	})
	require.NoError(t, err)

	got, err := client.GetUser(t.Context(), &cognitoidpsdk.GetUserInput{AccessToken: aws.String(accessToken)})
	require.NoError(t, err)
	require.Len(t, got.MFAOptions, 1)
	assert.Equal(t, types.DeliveryMediumTypeSms, got.MFAOptions[0].DeliveryMedium)
}

// selfServiceAccessToken creates a pool+client (with USER_PASSWORD_AUTH allowed),
// an admin-created user with a permanent password, and returns a real access
// token from InitiateAuth for that user.
func selfServiceAccessToken(t *testing.T, client *cognitoidpsdk.Client, poolName, username string) string {
	t.Helper()

	_, tokens := selfServiceLogin(t, client, poolName, username)

	return aws.ToString(tokens.AccessToken)
}

// selfServiceRefreshToken is selfServiceAccessToken's counterpart for tests
// that need the client ID and refresh token instead.
func selfServiceRefreshToken(t *testing.T, client *cognitoidpsdk.Client, poolName, username string) (string, string) {
	t.Helper()

	clientID, tokens := selfServiceLogin(t, client, poolName, username)

	return clientID, aws.ToString(tokens.RefreshToken)
}

func selfServiceLogin(
	t *testing.T, client *cognitoidpsdk.Client, poolName, username string,
) (string, *types.AuthenticationResultType) {
	t.Helper()

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String(poolName),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cognitoClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String(poolName + "-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cognitoClient.UserPoolClient.ClientId)

	created, err := client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(username),
	})
	require.NoError(t, err)
	realUsername := aws.ToString(created.User.Username)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(realUsername),
		Password: aws.String("Perm1Pass!"), Permanent: true,
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": realUsername, "PASSWORD": "Perm1Pass!"},
	})
	require.NoError(t, err)
	require.NotNil(t, auth.AuthenticationResult)

	return clientID, auth.AuthenticationResult
}
