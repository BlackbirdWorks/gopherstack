package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	s3ctypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestCreateAccessPointForObjectLambda_Alias_RealSDKClient proves
// CreateAccessPointForObjectLambdaOutput now echoes Alias. The backend
// (object_lambda.go, commit fb80d66cd) has synthesized ObjectLambdaAccessPoint.
// Alias -- using the real "--ol-s3" suffix convention -- since 2026-08-17, and
// Get/ListAccessPointsForObjectLambda already returned it, but the Create
// response handler (handler_object_lambda.go) never echoed the field it had
// just set. Real CreateAccessPointForObjectLambdaOutput
// (api_op_CreateAccessPointForObjectLambda.go:77) has Alias +
// ObjectLambdaAccessPointArn only.
func TestCreateAccessPointForObjectLambda_Alias_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	client := newTestS3ControlClient(t, h)

	out, err := client.CreateAccessPointForObjectLambda(t.Context(), &s3csdk.CreateAccessPointForObjectLambdaInput{
		AccountId: aws.String(createTagsTestAccountID),
		Name:      aws.String("my-olap"),
		Configuration: &s3ctypes.ObjectLambdaConfiguration{
			SupportingAccessPoint: aws.String("arn:aws:s3:us-east-1:123456789012:accesspoint/base-ap"),
			TransformationConfigurations: []s3ctypes.ObjectLambdaTransformationConfiguration{
				{
					Actions: []s3ctypes.ObjectLambdaTransformationConfigurationAction{
						s3ctypes.ObjectLambdaTransformationConfigurationActionGetObject,
					},
					ContentTransformation: &s3ctypes.ObjectLambdaContentTransformationMemberAwsLambda{
						Value: s3ctypes.AwsLambdaTransformation{
							FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Alias)
	assert.NotEmpty(t, aws.ToString(out.Alias.Value))
	assert.Equal(t, "READY", string(out.Alias.Status))
}
