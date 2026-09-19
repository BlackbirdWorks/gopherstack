package iam_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const testTemplateArn = "arn:aws:iam::aws:role-template/iam.amazonaws.com/TestRoleTemplate:1"

func seedRoleTemplate(t *testing.T, b *iam.InMemoryBackend) {
	t.Helper()

	b.AddRoleTemplateVersionInternal(iam.RoleTemplateVersion{
		TemplateArn:         testTemplateArn,
		TemplateName:        "TestRoleTemplate",
		Description:         "a template used for tests",
		MajorVersion:        1,
		MinorVersion:        0,
		DefaultMinorVersion: 0,
		Enabled:             true,
		MaxSessionDuration:  3600,
		RoleNamePattern:     "acquired-@{ServiceName}-role",
		RolePathPattern:     "/acquired/",
		AssumeRolePolicyDocumentTemplate: `{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Principal":{"Service":"@{ServiceName}"},"Action":"sts:AssumeRole"}]}`,
		ParametersDefinition: []iam.RoleTemplateParameter{
			{Name: "ServiceName", Type: "String", IsRequired: true},
		},
		InlinePolicyTemplates: []iam.RoleTemplateInlinePolicy{
			{
				PolicyName: "InlinePolicy",
				PolicyDocument: `{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
			},
		},
		RoleTagsTemplate: []iam.RoleTemplateTag{{Key: "ManagedBy", Value: "RoleManager"}},
	})
}

func TestRealClient_GetRoleTemplateVersion(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	seedRoleTemplate(t, backend)

	out, err := client.GetRoleTemplateVersion(ctx, &iamsdk.GetRoleTemplateVersionInput{
		TemplateArn: aws.String(testTemplateArn),
	})
	require.NoError(t, err)
	require.NotNil(t, out.RoleTemplateVersion)
	assert.Equal(t, testTemplateArn, aws.ToString(out.RoleTemplateVersion.TemplateArn))
	assert.True(t, out.RoleTemplateVersion.Enabled)
	require.Len(t, out.RoleTemplateVersion.ParametersDefinition, 1)
	assert.Equal(t, "ServiceName", aws.ToString(out.RoleTemplateVersion.ParametersDefinition[0].Name))

	_, err = client.GetRoleTemplateVersion(ctx, &iamsdk.GetRoleTemplateVersionInput{
		TemplateArn: aws.String("arn:aws:iam::aws:role-template/iam.amazonaws.com/NoSuchTemplate:1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())
}

func TestRealClient_AcquireRole(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	seedRoleTemplate(t, backend)

	out, err := client.AcquireRole(ctx, &iamsdk.AcquireRoleInput{
		TemplateArn: aws.String(testTemplateArn),
		ReplacementValues: map[string]types.ReplacementValueEntry{
			"ServiceName": {Values: []string{"lambda.amazonaws.com"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Role)
	assert.Equal(t, "acquired-lambda.amazonaws.com-role", aws.ToString(out.Role.RoleName))
	assert.Equal(t, "/acquired/", aws.ToString(out.Role.Path))
	assert.Equal(t, int32(3600), aws.ToInt32(out.Role.MaxSessionDuration))

	// Idempotent get-or-create: real AWS "returns that role" when the
	// resolved name already exists (IAM User Guide, "Manage access to role
	// manager") -- a second AcquireRole with the same parameters must
	// return the SAME role, not EntityAlreadyExists.
	again, err := client.AcquireRole(ctx, &iamsdk.AcquireRoleInput{
		TemplateArn: aws.String(testTemplateArn),
		ReplacementValues: map[string]types.ReplacementValueEntry{
			"ServiceName": {Values: []string{"lambda.amazonaws.com"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(out.Role.Arn), aws.ToString(again.Role.Arn))
	assert.Equal(t, aws.ToString(out.Role.RoleId), aws.ToString(again.Role.RoleId))

	// The inline policy and tag from the template were actually applied.
	getRole, err := client.GetRole(ctx, &iamsdk.GetRoleInput{RoleName: out.Role.RoleName})
	require.NoError(t, err)
	require.Len(t, getRole.Role.Tags, 1)
	assert.Equal(t, "ManagedBy", aws.ToString(getRole.Role.Tags[0].Key))

	policyOut, err := client.GetRolePolicy(ctx, &iamsdk.GetRolePolicyInput{
		RoleName:   out.Role.RoleName,
		PolicyName: aws.String("InlinePolicy"),
	})
	require.NoError(t, err)

	// Real AWS returns GetRolePolicy's PolicyDocument URL-encoded; the SDK
	// client does not auto-decode it (matches GetRole's own encoding, see
	// handler.go's encodePolicyDocument).
	decoded, err := url.QueryUnescape(aws.ToString(policyOut.PolicyDocument))
	require.NoError(t, err)
	assert.Contains(t, decoded, "s3:GetObject")
}

func TestRealClient_AcquireRole_MissingRequiredParameter(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	seedRoleTemplate(t, backend)

	_, err := client.AcquireRole(ctx, &iamsdk.AcquireRoleInput{
		TemplateArn: aws.String(testTemplateArn),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInput", apiErr.ErrorCode())
}

func TestRealClient_AcquireRole_DisabledTemplate(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	backend.AddRoleTemplateVersionInternal(iam.RoleTemplateVersion{
		TemplateArn:     "arn:aws:iam::aws:role-template/iam.amazonaws.com/DisabledTemplate:1",
		Enabled:         false,
		RoleNamePattern: "disabled-role",
	})

	_, err := client.AcquireRole(ctx, &iamsdk.AcquireRoleInput{
		TemplateArn: aws.String("arn:aws:iam::aws:role-template/iam.amazonaws.com/DisabledTemplate:1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RoleTemplateDisabled", apiErr.ErrorCode())
}
