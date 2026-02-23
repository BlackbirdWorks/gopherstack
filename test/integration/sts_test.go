package integration_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
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
