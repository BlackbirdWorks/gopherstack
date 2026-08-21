package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateTerms_LinksNeverNil proves types.TermsType.Links (required per
// cognitoidentityprovider@v1.67.4 types/types.go:2225, "This member is
// required.") survives a real SDK client round trip even when the caller
// never supplies any Links -- CreateTermsInput.Links is optional, so a real
// client can create Terms with none. Before the fix, toTermsType passed
// through a nil map and termsType.Links was tagged omitempty, so the whole
// key vanished from the response; a real client's CreateTermsOutput.Terms.Links
// decoded to a nil map instead of the required-but-empty map{}.
// gopherstack-r80d batch 19.
func TestCreateTerms_LinksNeverNil(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	poolID, clientID := setupHandlerPoolAndClient(t, h, "terms-links-pool")

	created, err := client.CreateTerms(ctx, &cognitoidpsdk.CreateTermsInput{
		UserPoolId:  aws.String(poolID),
		ClientId:    aws.String(clientID),
		TermsName:   aws.String("terms-of-use"),
		Enforcement: types.TermsEnforcementTypeNone,
		TermsSource: types.TermsSourceTypeLink,
		// Links deliberately omitted: CreateTermsInput.Links is optional.
	})
	require.NoError(t, err)
	require.NotNil(t, created.Terms)
	assert.NotNil(t, created.Terms.Links, "CreateTerms must emit Links as {} , not omit the required key")
	assert.Empty(t, created.Terms.Links)

	described, err := client.DescribeTerms(ctx, &cognitoidpsdk.DescribeTermsInput{
		UserPoolId: aws.String(poolID),
		TermsId:    created.Terms.TermsId,
	})
	require.NoError(t, err)
	require.NotNil(t, described.Terms)
	assert.NotNil(t, described.Terms.Links, "DescribeTerms must emit Links as {}, not omit the required key")
}

// TestListWebAuthnCredentials_EmptyCredentialsNeverNil proves
// ListWebAuthnCredentialsOutput.Credentials (required) survives a real SDK
// client round trip for a user with zero registered passkeys. Before the
// fix, listWebAuthnCredentialsOutput.Credentials was tagged omitempty despite
// the handler already building a non-nil empty slice, so the key vanished
// and a real client's response decoded Credentials as nil instead of [].
// gopherstack-r80d batch 19.
func TestListWebAuthnCredentials_EmptyCredentialsNeverNil(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	poolID, clientID := setupHandlerPoolAndClient(t, h, "webauthn-empty-pool")
	signUpAndAdminConfirm(t, h, clientID, poolID, "webauthn-user")
	accessToken := loginViaHandler(t, h, clientID, "webauthn-user")

	resp, err := client.ListWebAuthnCredentials(ctx, &cognitoidpsdk.ListWebAuthnCredentialsInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.Credentials, "Credentials must be emitted as [], never omitted")
	assert.Empty(t, resp.Credentials)
}

// TestCreateResourceServer_EmptyScopeStringsRoundTrip proves
// types.ResourceServerScopeType.ScopeName/.ScopeDescription (both required)
// survive a real SDK client round trip when empty. The real SDK's own
// client-side validator (validateResourceServerScopeType) only null-checks
// the *string pointer, not its content, so a real caller can send an
// explicit empty-string scope name/description and have it accepted before
// the request is ever sent. Before the fix, resourceServerScopeType tagged
// both members omitempty, dropping them whenever a scope's name/description
// happened to be empty. gopherstack-r80d batch 19.
func TestCreateResourceServer_EmptyScopeStringsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	poolID, _ := setupHandlerPoolAndClient(t, h, "resource-server-scope-pool")

	created, err := client.CreateResourceServer(ctx, &cognitoidpsdk.CreateResourceServerInput{
		UserPoolId: aws.String(poolID),
		Identifier: aws.String("https://api.example.com"),
		Name:       aws.String("Example API"),
		Scopes: []types.ResourceServerScopeType{
			{ScopeName: aws.String(""), ScopeDescription: aws.String("")},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.ResourceServer.Scopes, 1)
	scope := created.ResourceServer.Scopes[0]
	assert.NotNil(t, scope.ScopeName, "ScopeName must round trip even when empty")
	assert.NotNil(t, scope.ScopeDescription, "ScopeDescription must round trip even when empty")

	described, err := client.DescribeResourceServer(ctx, &cognitoidpsdk.DescribeResourceServerInput{
		UserPoolId: aws.String(poolID),
		Identifier: aws.String("https://api.example.com"),
	})
	require.NoError(t, err)
	require.Len(t, described.ResourceServer.Scopes, 1)
	assert.NotNil(t, described.ResourceServer.Scopes[0].ScopeName)
	assert.NotNil(t, described.ResourceServer.Scopes[0].ScopeDescription)
}

// TestSetRiskConfiguration_EmptySourceArnRoundTrip proves
// types.NotifyConfigurationType.SourceArn (required) survives a real SDK
// client round trip when empty. The real SDK's own client-side validator
// (validateNotifyConfigurationType) only null-checks the *string pointer,
// not its content, so a real caller can set NotifyConfiguration with an
// explicit empty-string SourceArn. Before the fix, notifyConfigJSON tagged
// SourceArn omitempty, dropping it whenever it happened to be empty.
// gopherstack-r80d batch 19.
func TestSetRiskConfiguration_EmptySourceArnRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-config-sourcearn-pool")

	set, err := client.SetRiskConfiguration(ctx, &cognitoidpsdk.SetRiskConfigurationInput{
		UserPoolId: aws.String(poolID),
		AccountTakeoverRiskConfiguration: &types.AccountTakeoverRiskConfigurationType{
			Actions: &types.AccountTakeoverActionsType{
				LowAction: &types.AccountTakeoverActionType{
					EventAction: types.AccountTakeoverEventActionTypeNoAction,
					Notify:      false,
				},
			},
			NotifyConfiguration: &types.NotifyConfigurationType{
				SourceArn: aws.String(""),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, set.RiskConfiguration.AccountTakeoverRiskConfiguration)
	require.NotNil(t, set.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration)
	assert.NotNil(t, set.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration.SourceArn,
		"SourceArn must round trip even when empty")

	described, err := client.DescribeRiskConfiguration(ctx, &cognitoidpsdk.DescribeRiskConfigurationInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.NotNil(t, described.RiskConfiguration.AccountTakeoverRiskConfiguration)
	require.NotNil(t, described.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration)
	assert.NotNil(t, described.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration.SourceArn)
}
