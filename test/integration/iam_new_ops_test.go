package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_IAM_SAMLProvider(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	name := "test-saml-" + uuid.NewString()[:8]
	samlDoc := `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://example.com/saml"/>`

	// CreateSAMLProvider
	createOut, err := client.CreateSAMLProvider(ctx, &iamsdk.CreateSAMLProviderInput{
		Name:                 aws.String(name),
		SAMLMetadataDocument: aws.String(samlDoc),
	})
	require.NoError(t, err)

	providerArn := *createOut.SAMLProviderArn
	assert.Contains(t, providerArn, name)

	// GetSAMLProvider
	getOut, err := client.GetSAMLProvider(ctx, &iamsdk.GetSAMLProviderInput{
		SAMLProviderArn: aws.String(providerArn),
	})
	require.NoError(t, err)
	assert.Equal(t, samlDoc, *getOut.SAMLMetadataDocument)
	assert.NotNil(t, getOut.CreateDate)

	// ListSAMLProviders
	listOut, err := client.ListSAMLProviders(ctx, &iamsdk.ListSAMLProvidersInput{})
	require.NoError(t, err)

	found := false

	for _, p := range listOut.SAMLProviderList {
		if *p.Arn == providerArn {
			found = true

			break
		}
	}

	assert.True(t, found, "created SAML provider should appear in ListSAMLProviders")

	// UpdateSAMLProvider
	updatedDoc := `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"` +
		` entityID="https://updated.example.com/saml"/>`
	updateOut, err := client.UpdateSAMLProvider(ctx, &iamsdk.UpdateSAMLProviderInput{
		SAMLProviderArn:      aws.String(providerArn),
		SAMLMetadataDocument: aws.String(updatedDoc),
	})
	require.NoError(t, err)
	assert.Equal(t, providerArn, *updateOut.SAMLProviderArn)

	// DeleteSAMLProvider
	_, err = client.DeleteSAMLProvider(ctx, &iamsdk.DeleteSAMLProviderInput{
		SAMLProviderArn: aws.String(providerArn),
	})
	require.NoError(t, err)
}

func TestIntegration_IAM_OIDCProvider(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	providerURL := "https://token-" + uuid.NewString()[:8] + ".actions.githubusercontent.com"
	thumbprint := "6938fd4d98bab03faadb97b34396831e3780aea1"

	// CreateOpenIDConnectProvider
	createOut, err := client.CreateOpenIDConnectProvider(ctx, &iamsdk.CreateOpenIDConnectProviderInput{
		Url:            aws.String(providerURL),
		ThumbprintList: []string{thumbprint},
		ClientIDList:   []string{"sts.amazonaws.com"},
	})
	require.NoError(t, err)

	providerArn := *createOut.OpenIDConnectProviderArn
	assert.Contains(t, providerArn, "oidc-provider")

	// GetOpenIDConnectProvider
	getOut, err := client.GetOpenIDConnectProvider(ctx, &iamsdk.GetOpenIDConnectProviderInput{
		OpenIDConnectProviderArn: aws.String(providerArn),
	})
	require.NoError(t, err)
	assert.Equal(t, providerURL, *getOut.Url)
	assert.Equal(t, []string{"sts.amazonaws.com"}, getOut.ClientIDList)
	assert.Equal(t, []string{thumbprint}, getOut.ThumbprintList)

	// ListOpenIDConnectProviders
	listOut, err := client.ListOpenIDConnectProviders(ctx, &iamsdk.ListOpenIDConnectProvidersInput{})
	require.NoError(t, err)

	found := false

	for _, p := range listOut.OpenIDConnectProviderList {
		if *p.Arn == providerArn {
			found = true

			break
		}
	}

	assert.True(t, found, "created OIDC provider should appear in list")

	// UpdateOpenIDConnectProviderThumbprint
	newThumbprint := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, err = client.UpdateOpenIDConnectProviderThumbprint(ctx, &iamsdk.UpdateOpenIDConnectProviderThumbprintInput{
		OpenIDConnectProviderArn: aws.String(providerArn),
		ThumbprintList:           []string{newThumbprint},
	})
	require.NoError(t, err)

	// DeleteOpenIDConnectProvider
	_, err = client.DeleteOpenIDConnectProvider(ctx, &iamsdk.DeleteOpenIDConnectProviderInput{
		OpenIDConnectProviderArn: aws.String(providerArn),
	})
	require.NoError(t, err)
}

func TestIntegration_IAM_LoginProfile(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	userName := "test-login-" + uuid.NewString()[:8]

	// CreateUser first
	_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	// CreateLoginProfile
	createOut, err := client.CreateLoginProfile(ctx, &iamsdk.CreateLoginProfileInput{
		UserName:              aws.String(userName),
		Password:              aws.String("Password123!"),
		PasswordResetRequired: false,
	})
	require.NoError(t, err)
	assert.Equal(t, userName, *createOut.LoginProfile.UserName)

	// GetLoginProfile
	getOut, err := client.GetLoginProfile(ctx, &iamsdk.GetLoginProfileInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Equal(t, userName, *getOut.LoginProfile.UserName)

	// UpdateLoginProfile
	_, err = client.UpdateLoginProfile(ctx, &iamsdk.UpdateLoginProfileInput{
		UserName:              aws.String(userName),
		Password:              aws.String("NewPassword456!"),
		PasswordResetRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	// DeleteLoginProfile
	_, err = client.DeleteLoginProfile(ctx, &iamsdk.DeleteLoginProfileInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Cleanup user
	_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
}

func TestIntegration_IAM_Misc(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	// GetServiceLastAccessedDetails returns COMPLETED for any job ID
	out, err := client.GetServiceLastAccessedDetails(ctx, &iamsdk.GetServiceLastAccessedDetailsInput{
		JobId: aws.String("test-job-id"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.JobStatusTypeCompleted, out.JobStatus)

	// SetSecurityTokenServicePreferences is a no-op
	_, err = client.SetSecurityTokenServicePreferences(ctx, &iamsdk.SetSecurityTokenServicePreferencesInput{
		GlobalEndpointTokenVersion: types.GlobalEndpointTokenVersionV2Token,
	})
	require.NoError(t, err)
}
