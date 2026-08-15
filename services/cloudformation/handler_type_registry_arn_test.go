package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
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
