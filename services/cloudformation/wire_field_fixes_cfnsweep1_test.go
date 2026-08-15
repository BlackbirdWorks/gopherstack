package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cfnSweep1Template = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`

// TestCreateUpdateRollbackStack_OperationID_RealClient drives CreateStack,
// UpdateStack and RollbackStack through the real aws-sdk-go-v2 client
// (gopherstack-7185). All three real outputs carry OperationId alongside
// StackId (cloudformation@v1.76.1 api_op_CreateStack.go /
// api_op_UpdateStack.go / api_op_RollbackStack.go); gopherstack emitted only
// StackId (RollbackStack emitted neither), confirmed by hand-reverting.
func TestCreateUpdateRollbackStack_OperationID_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	created, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("sweep1-stack"),
		TemplateBody: aws.String(cfnSweep1Template),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.OperationId), "CreateStack: OperationId empty")
	require.NotEmpty(t, aws.ToString(created.StackId))

	updated, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName:    aws.String("sweep1-stack"),
		TemplateBody: aws.String(cfnSweep1Template),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(updated.OperationId), "UpdateStack: OperationId empty")
	assert.Equal(t, aws.ToString(created.StackId), aws.ToString(updated.StackId))

	rolled, err := client.RollbackStack(t.Context(), &cfnsdk.RollbackStackInput{
		StackName: aws.String("sweep1-stack"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(rolled.OperationId), "RollbackStack: OperationId empty")
	assert.Equal(t, aws.ToString(created.StackId), aws.ToString(rolled.StackId),
		"RollbackStack: StackId empty or mismatched")
}

// TestTypeRegistryOps_RealClient drives ActivateType, PublishType and
// SetTypeConfiguration through the real client. gopherstack returned an
// empty envelope for all three where the real outputs carry Arn /
// PublicTypeArn / ConfigurationArn respectively (cloudformation@v1.76.1
// api_op_ActivateType.go / api_op_PublishType.go /
// api_op_SetTypeConfiguration.go), confirmed by hand-reverting.
func TestTypeRegistryOps_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("activate type returns arn", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
			TypeName: aws.String("AWS::Sweep1::ActivateType"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(out.Arn), "ActivateType: Arn empty")
	})

	t.Run("publish type returns public type arn", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
			TypeName:             aws.String("AWS::Sweep1::PublishType"),
			SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
		})
		require.NoError(t, err)

		out, err := client.PublishType(t.Context(), &cfnsdk.PublishTypeInput{
			TypeName: aws.String("AWS::Sweep1::PublishType"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(out.PublicTypeArn), "PublishType: PublicTypeArn empty")
	})

	t.Run("set type configuration returns configuration arn", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.SetTypeConfiguration(t.Context(), &cfnsdk.SetTypeConfigurationInput{
			TypeName:      aws.String("AWS::Sweep1::Configured"),
			Configuration: aws.String(`{"Key":"Value"}`),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(out.ConfigurationArn), "SetTypeConfiguration: ConfigurationArn empty")
	})
}

// TestUpdateGeneratedTemplate_ReturnsID_RealClient drives
// UpdateGeneratedTemplate through the real client. gopherstack returned an
// empty envelope where the real output carries GeneratedTemplateId
// (cloudformation@v1.76.1 api_op_UpdateGeneratedTemplate.go), confirmed by
// hand-reverting.
func TestUpdateGeneratedTemplate_ReturnsID_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	created, err := client.CreateGeneratedTemplate(t.Context(), &cfnsdk.CreateGeneratedTemplateInput{
		GeneratedTemplateName: aws.String("sweep1-gt"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.GeneratedTemplateId))

	updated, err := client.UpdateGeneratedTemplate(t.Context(), &cfnsdk.UpdateGeneratedTemplateInput{
		GeneratedTemplateName:    aws.String("sweep1-gt"),
		NewGeneratedTemplateName: aws.String("sweep1-gt-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.GeneratedTemplateId), aws.ToString(updated.GeneratedTemplateId),
		"UpdateGeneratedTemplate: GeneratedTemplateId empty or mismatched")
}

// TestStackSetOps_OperationID_RealClient drives UpdateStackSet and
// ImportStacksToStackSet through the real client. gopherstack returned an
// empty envelope for both where the real outputs carry OperationId
// (cloudformation@v1.76.1 api_op_UpdateStackSet.go /
// api_op_ImportStacksToStackSet.go) -- the backend already generated the ID
// internally via recordStackSetOperation, it just never reached the
// response. Confirmed by hand-reverting.
func TestStackSetOps_OperationID_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("sweep1-ss"),
		TemplateBody: aws.String(cfnSweep1Template),
	})
	require.NoError(t, err)

	updated, err := client.UpdateStackSet(t.Context(), &cfnsdk.UpdateStackSetInput{
		StackSetName: aws.String("sweep1-ss"),
		Description:  aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(updated.OperationId), "UpdateStackSet: OperationId empty")

	imported, err := client.ImportStacksToStackSet(t.Context(), &cfnsdk.ImportStacksToStackSetInput{
		StackSetName: aws.String("sweep1-ss"),
		StackIds:     []string{"arn:aws:cloudformation:us-east-1:123456789012:stack/sweep1-import/abc"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(imported.OperationId), "ImportStacksToStackSet: OperationId empty")
}

// TestValidateTemplate_Capabilities_RealClient drives ValidateTemplate
// through the real client with a template that declares an IAM resource and
// a Transform. gopherstack's ValidateTemplateResult always reported an empty
// Capabilities/DeclaredTransforms/CapabilitiesReason, matching neither
// signal a real caller relies on to decide whether CAPABILITY_IAM /
// CAPABILITY_AUTO_EXPAND must be passed to CreateStack (cloudformation@
// v1.76.1 api_op_ValidateTemplate.go), confirmed by hand-reverting.
func TestValidateTemplate_Capabilities_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	tmpl := `{
		"Transform": "AWS::Serverless-2016-10-31",
		"Resources": {
			"Role": {"Type": "AWS::IAM::Role", "Properties": {"AssumeRolePolicyDocument": {}}}
		}
	}`

	out, err := client.ValidateTemplate(t.Context(), &cfnsdk.ValidateTemplateInput{
		TemplateBody: aws.String(tmpl),
	})
	require.NoError(t, err)
	assert.Contains(
		t,
		out.Capabilities,
		types.Capability("CAPABILITY_IAM"),
		"ValidateTemplate: Capabilities missing CAPABILITY_IAM",
	)
	assert.NotEmpty(t, aws.ToString(out.CapabilitiesReason), "ValidateTemplate: CapabilitiesReason empty")
	assert.Contains(t, out.DeclaredTransforms, "AWS::Serverless-2016-10-31",
		"ValidateTemplate: DeclaredTransforms missing the template's Transform")
}
