package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeRegistry_IdentifyByArn drives ActivateType and DeactivateType
// through the real client using only the ARN identifier (no TypeName),
// which is one of the two documented ways to identify a type. gopherstack
// read form key "TypeArn" for both ops where the pinned serializer sends
// "PublicTypeArn" (ActivateType) and "Arn" (DeactivateType)
// (cloudformation@v1.76.1 serializers.go:7181 and :7751), so an ARN-only
// caller had the value silently dropped (gopherstack-vc2g).
func TestTypeRegistry_IdentifyByArn(t *testing.T) {
	t.Parallel()

	t.Run("deactivate type by arn only", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		typeName := "AWS::VC2G::DeactivateByArn"
		typeArn := "arn:aws:cloudformation:::type/resource/" + typeName

		_, err := client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
			TypeName: aws.String(typeName),
		})
		require.NoError(t, err)

		_, err = client.DeactivateType(t.Context(), &cfnsdk.DeactivateTypeInput{
			Arn: aws.String(typeArn),
		})
		require.NoError(t, err, "DeactivateType by Arn alone should succeed")

		desc, err := client.DescribeType(t.Context(), &cfnsdk.DescribeTypeInput{
			Arn: aws.String(typeArn),
		})
		require.NoError(t, err)
		require.False(t, aws.ToBool(desc.IsActivated), "type should be deactivated")
	})

	t.Run("activate type by public type arn only", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		typeName := "AWS::VC2G::ActivateByArn"
		typeArn := "arn:aws:cloudformation:::type/resource/" + typeName

		_, err := client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
			TypeName: aws.String(typeName),
		})
		require.NoError(t, err)

		_, err = client.DeactivateType(t.Context(), &cfnsdk.DeactivateTypeInput{
			TypeName: aws.String(typeName),
		})
		require.NoError(t, err)

		_, err = client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
			PublicTypeArn: aws.String(typeArn),
		})
		require.NoError(t, err, "ActivateType by PublicTypeArn alone should succeed")

		desc, err := client.DescribeType(t.Context(), &cfnsdk.DescribeTypeInput{
			Arn: aws.String(typeArn),
		})
		require.NoError(t, err)
		require.True(t, aws.ToBool(desc.IsActivated),
			"type should be reactivated via its original arn, not a new empty-key entry")
	})
}

// TestBatchDescribeTypeConfigurations_Arn drives SetTypeConfiguration then
// BatchDescribeTypeConfigurations through the real client (gopherstack-eamp).
// types.TypeConfigurationDetails carries both Arn ("the ARN for the
// configuration data, in this account and Region",
// cloudformation@v1.76.1 types/types.go:3324-3325, deserialized at
// deserializers.go:25335) and TypeArn (the extension's own ARN, types.go:
// 3343-3351, deserializers.go:25394) -- two distinct ARNs.
// gopherstack derives the configuration ARN the same way
// SetTypeConfigurationOutput.ConfigurationArn already is (backend.
// SetTypeConfiguration, type_registry.go), so the two must agree.
func TestBatchDescribeTypeConfigurations_Arn(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	typeName := "AWS::Eamp::ConfigArn"

	_, err := client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
		TypeName:             aws.String(typeName),
		SchemaHandlerPackage: aws.String("s3://bucket/eamp.zip"),
	})
	require.NoError(t, err)

	setOut, err := client.SetTypeConfiguration(t.Context(), &cfnsdk.SetTypeConfigurationInput{
		TypeName:      aws.String(typeName),
		Configuration: aws.String(`{"key":"value"}`),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(setOut.ConfigurationArn))

	out, err := client.BatchDescribeTypeConfigurations(t.Context(), &cfnsdk.BatchDescribeTypeConfigurationsInput{
		TypeConfigurationIdentifiers: []cfntypes.TypeConfigurationIdentifier{
			{TypeName: aws.String(typeName)},
		},
	})
	require.NoError(t, err)
	require.Empty(t, out.Errors)
	require.Empty(t, out.UnprocessedTypeConfigurations)
	require.Len(t, out.TypeConfigurations, 1)

	detail := out.TypeConfigurations[0]
	assert.NotEmpty(t, aws.ToString(detail.Arn), "TypeConfigurationDetails.Arn empty")
	assert.NotEqual(t, aws.ToString(detail.TypeArn), aws.ToString(detail.Arn),
		"configuration Arn must differ from the type's own TypeArn")
	assert.Equal(t, aws.ToString(setOut.ConfigurationArn), aws.ToString(detail.Arn),
		"BatchDescribeTypeConfigurations' Arn must match SetTypeConfiguration's ConfigurationArn")
}
