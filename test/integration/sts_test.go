package integration_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	stsstypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_STS_GetCallerIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	out, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotNil(t, out)

	// Mock returns all-zeros account ID
	assert.Equal(t, "000000000000", *out.Account)
	assert.NotEmpty(t, *out.Arn)
	assert.NotEmpty(t, *out.UserId)
}

func TestIntegration_STS_AssumeRole(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleARN := "arn:aws:iam::000000000000:role/test-role-" + uuid.NewString()
	sessionName := "test-session-" + uuid.NewString()

	out, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.AssumedRoleUser)

	// Verify assumed role ARN contains session name
	assert.Contains(t, *out.AssumedRoleUser.Arn, sessionName)
}

func TestIntegration_STS_CredentialFormatValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleARN := "arn:aws:iam::000000000000:role/format-check-role"
	sessionName := "format-check-session"

	out, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)

	creds := out.Credentials

	// Access key ID should start with "ASIA" and be non-empty
	require.NotEmpty(t, *creds.AccessKeyId)
	assert.True(t, strings.HasPrefix(*creds.AccessKeyId, "ASIA"),
		"access key ID should start with ASIA, got: %s", *creds.AccessKeyId)

	// Secret key should be non-empty
	assert.NotEmpty(t, *creds.SecretAccessKey)

	// Session token should be non-empty
	assert.NotEmpty(t, *creds.SessionToken)

	// Expiration should be set
	assert.NotNil(t, creds.Expiration)
}

// TestIntegration_STS_AssumeRole_ExternalID_NoValidation verifies that AssumeRole
// with ExternalId succeeds when the role is not registered in IAM (no validation).
func TestIntegration_STS_AssumeRole_ExternalID_NoValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleARN := "arn:aws:iam::000000000000:role/ext-id-role-" + uuid.NewString()[:8]
	sessionName := "ext-id-session"

	// No IAM role exists with an ExternalId condition, so any ExternalId passes.
	out, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
		ExternalId:      aws.String("any-external-id"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
}

// TestIntegration_STS_AssumeRole_ExternalID_Validation verifies that AssumeRole
// with a wrong ExternalId returns AccessDenied when the IAM role requires one.
func TestIntegration_STS_AssumeRole_ExternalID_Validation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	iamClient := createIAMClient(t)
	stsClient := createSTSClient(t)
	ctx := t.Context()

	roleName := "ext-id-validated-role-" + uuid.NewString()[:8]
	externalID := "required-ext-id-" + uuid.NewString()[:8]

	// Build a trust policy that requires an ExternalId.
	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{` +
		`"StringEquals":{"sts:ExternalId":"` + externalID + `"}}}]}`

	roleOut, err := iamClient.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(trustDoc),
	})
	require.NoError(t, err)

	roleARN := *roleOut.Role.Arn

	t.Cleanup(func() {
		_, _ = iamClient.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	})

	// Correct ExternalId: should succeed.
	out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("session"),
		ExternalId:      aws.String(externalID),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)

	// Wrong ExternalId: should fail with AccessDenied.
	_, err = stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("session"),
		ExternalId:      aws.String("wrong-id"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDenied")

	// Missing ExternalId: should also fail.
	_, err = stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("session"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDenied")
}

// TestIntegration_STS_GetCallerIdentity_AssumedRole verifies that GetCallerIdentity
// returns the assumed-role ARN (not root) when called with assumed-role credentials.
func TestIntegration_STS_GetCallerIdentity_AssumedRole(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleName := "caller-id-role-" + uuid.NewString()[:8]
	sessionName := "caller-id-session"
	roleARN := "arn:aws:iam::000000000000:role/" + roleName

	// First AssumeRole to get credentials.
	assumeOut, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
	})
	require.NoError(t, err)
	require.NotNil(t, assumeOut.Credentials)

	// Build a new STS client using the assumed-role credentials.
	assumedClient := createSTSClientWithCreds(
		t,
		*assumeOut.Credentials.AccessKeyId,
		*assumeOut.Credentials.SecretAccessKey,
		*assumeOut.Credentials.SessionToken,
	)

	ciOut, err := assumedClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotNil(t, ciOut)

	assert.Equal(t, "000000000000", *ciOut.Account)
	assert.Contains(t, *ciOut.Arn, "assumed-role")
	assert.Contains(t, *ciOut.Arn, roleName)
	assert.Contains(t, *ciOut.Arn, sessionName)
	assert.Truef(t, strings.HasPrefix(*ciOut.UserId, "AROA"),
		"expected UserId to start with AROA, got: %s", *ciOut.UserId)
	assert.Contains(t, *ciOut.UserId, sessionName)
}

func TestIntegration_STS_AssumeRoleWithSAML(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleARN := "arn:aws:iam::000000000000:role/saml-role-" + uuid.NewString()[:8]
	principalARN := "arn:aws:iam::000000000000:saml-provider/MyIdP"
	// Minimal base64-encoded SAML assertion for mock testing.
	samlAssertion := "dGVzdC1zYW1sLWFzc2VydGlvbg=="

	out, err := client.AssumeRoleWithSAML(ctx, &sts.AssumeRoleWithSAMLInput{
		RoleArn:       aws.String(roleARN),
		PrincipalArn:  aws.String(principalARN),
		SAMLAssertion: aws.String(samlAssertion),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.AssumedRoleUser)

	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
	assert.NotEmpty(t, out.Issuer)
	assert.NotEmpty(t, out.NameQualifier)
	assert.Contains(t, *out.AssumedRoleUser.Arn, "assumed-role")

	assumedClient := createSTSClientWithCreds(
		t,
		*out.Credentials.AccessKeyId,
		*out.Credentials.SecretAccessKey,
		*out.Credentials.SessionToken,
	)

	identity, err := assumedClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Contains(t, *identity.Arn, "assumed-role")
}

func TestIntegration_STS_AssumeRoleWithWebIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	roleARN := "arn:aws:iam::000000000000:role/web-identity-role-" + uuid.NewString()[:8]
	webIdentityToken := "eyJhbGciOiJub25lIn0." +
		"eyJzdWIiOiJ3ZWItaWRlbnRpdHktdXNlciIsImlzcyI6Imh0dHBzOi8vaWRwLmV4YW1wbGUuY29tIiwiYXVkIjoidGVzdC1hdWQifQ."

	out, err := client.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(roleARN),
		RoleSessionName:  aws.String("web-identity-session"),
		WebIdentityToken: aws.String(webIdentityToken),
		ProviderId:       aws.String("https://idp.example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.AssumedRoleUser)

	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
	assert.Contains(t, aws.ToString(out.AssumedRoleUser.Arn), "assumed-role")
	assert.Equal(t, "web-identity-user", aws.ToString(out.SubjectFromWebIdentityToken))
	assert.Equal(t, "https://idp.example.com", aws.ToString(out.Provider))

	assumedClient := createSTSClientWithCreds(
		t,
		*out.Credentials.AccessKeyId,
		*out.Credentials.SecretAccessKey,
		*out.Credentials.SessionToken,
	)

	identity, err := assumedClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Contains(t, *identity.Arn, "assumed-role")
}

func TestIntegration_STS_AssumeRoot(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	targetAccount := "000000000000"
	taskPolicyARN := "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials"

	out, err := client.AssumeRoot(ctx, &sts.AssumeRootInput{
		TargetPrincipal: aws.String(targetAccount),
		TaskPolicyArn: &stsstypes.PolicyDescriptorType{
			Arn: aws.String(taskPolicyARN),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)

	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"),
		"expected access key to start with ASIA, got: %s", *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
}

func TestIntegration_STS_GetDelegatedAccessToken(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	out, err := client.GetDelegatedAccessToken(ctx, &sts.GetDelegatedAccessTokenInput{
		TradeInToken: aws.String("test-trade-in-token-" + uuid.NewString()),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)

	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"),
		"expected access key to start with ASIA, got: %s", *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
}

func TestIntegration_STS_GetWebIdentityToken(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSTSClient(t)
	ctx := t.Context()

	out, err := client.GetWebIdentityToken(ctx, &sts.GetWebIdentityTokenInput{
		Audience:         []string{"https://example.com"},
		SigningAlgorithm: aws.String("RS256"),
	})
	require.NoError(t, err)
	require.NotNil(t, out)

	require.NotNil(t, out.WebIdentityToken)
	assert.NotNil(t, out.Expiration)
	// JWT has 3 dot-separated parts.
	parts := strings.Split(*out.WebIdentityToken, ".")
	assert.Len(t, parts, 3, "expected JWT with 3 parts")
}
