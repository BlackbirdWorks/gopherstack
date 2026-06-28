package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CFNAudit_PublishTypeMissing verifies that PublishType returns
// an error (TypeNotFoundException) when the type is not registered.
func TestIntegration_CFNAudit_PublishTypeMissing(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCloudFormationClient(t)

	_, err := client.PublishType(ctx, &cloudformationsdk.PublishTypeInput{
		TypeName: aws.String("AWS::Audit::Missing" + cfnStackID()),
		Type:     "RESOURCE",
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TypeNotFoundException", apiErr.ErrorCode())
}

// TestIntegration_CFNAudit_DeleteGeneratedTemplateMissing verifies that deleting
// a non-existent generated template returns GeneratedTemplateNotFoundException.
func TestIntegration_CFNAudit_DeleteGeneratedTemplateMissing(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCloudFormationClient(t)

	_, err := client.DeleteGeneratedTemplate(ctx, &cloudformationsdk.DeleteGeneratedTemplateInput{
		GeneratedTemplateName: aws.String("audit-missing-" + cfnStackID()),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "GeneratedTemplateNotFoundException", apiErr.ErrorCode())
}

// TestIntegration_CFNAudit_DeactivateTypeMissing verifies that deactivating a
// type that was never activated returns TypeNotFoundException.
func TestIntegration_CFNAudit_DeactivateTypeMissing(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCloudFormationClient(t)

	_, err := client.DeactivateType(ctx, &cloudformationsdk.DeactivateTypeInput{
		TypeName: aws.String("AWS::Audit::NotActivated" + cfnStackID()),
		Type:     "RESOURCE",
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TypeNotFoundException", apiErr.ErrorCode())
}

// TestIntegration_CFNAudit_UpdateTerminationProtectionStackID verifies that
// UpdateTerminationProtection populates the StackId in its response.
func TestIntegration_CFNAudit_UpdateTerminationProtectionStackID(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCloudFormationClient(t)

	stackName := "audit-tp-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Topic": {
"Type": "AWS::SNS::Topic",
"Properties": {}
}
}
}`

	createOut, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.StackId)

	t.Cleanup(func() {
		_, _ = client.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{
			StackName: aws.String(stackName),
		})
	})

	out, err := client.UpdateTerminationProtection(
		ctx,
		&cloudformationsdk.UpdateTerminationProtectionInput{
			StackName:                   aws.String(stackName),
			EnableTerminationProtection: aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.StackId)
	assert.NotEmpty(t, aws.ToString(out.StackId))
	assert.Equal(t, aws.ToString(createOut.StackId), aws.ToString(out.StackId))
}
