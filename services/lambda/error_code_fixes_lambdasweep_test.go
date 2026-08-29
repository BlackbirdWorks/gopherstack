package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/require"
)

// TestPutFunctionCodeSigningConfig_UnknownCSC_RealClient drives
// PutFunctionCodeSigningConfig through the real client with a function that
// exists but a CodeSigningConfigArn that doesn't. lambda@v1.101.2's own
// deserializeOpErrorPutFunctionCodeSigningConfig models both
// ResourceNotFoundException (function not found) and
// CodeSigningConfigNotFoundException (CSC not found) as distinct shapes;
// gopherstack's backend conflated both conditions into the same
// ResourceNotFoundException sentinel (confirmed by hand-reverting).
func TestPutFunctionCodeSigningConfig_UnknownCSC_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("csc-put-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	_, err = client.PutFunctionCodeSigningConfig(ctx, &lambdasdk.PutFunctionCodeSigningConfigInput{
		FunctionName:         aws.String("csc-put-fn"),
		CodeSigningConfigArn: aws.String("arn:aws:lambda:us-east-1:000000000000:code-signing-config:no-such-csc"),
	})
	require.Error(t, err)

	var nf *types.CodeSigningConfigNotFoundException
	require.ErrorAs(t, err, &nf, "expected a real CodeSigningConfigNotFoundException from the SDK deserializer")
}

// TestGetProvisionedConcurrencyConfig_UnknownQualifier_RealClient drives
// GetProvisionedConcurrencyConfig through the real client for a function
// that exists but has no provisioned concurrency config for the requested
// qualifier. lambda@v1.101.2's own deserializeOpErrorGetProvisionedConcurrencyConfig
// models both ResourceNotFoundException and
// ProvisionedConcurrencyConfigNotFoundException as distinct shapes;
// gopherstack used the generic ResourceNotFoundException for this condition
// even though the sentinel is literally named
// ErrProvisionedConcurrencyConfigNotFound (confirmed by hand-reverting).
func TestGetProvisionedConcurrencyConfig_UnknownQualifier_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("pcc-get-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	_, err = client.GetProvisionedConcurrencyConfig(ctx, &lambdasdk.GetProvisionedConcurrencyConfigInput{
		FunctionName: aws.String("pcc-get-fn"),
		Qualifier:    aws.String("no-such-qualifier"),
	})
	require.Error(t, err)

	var nf *types.ProvisionedConcurrencyConfigNotFoundException
	require.ErrorAs(t, err, &nf,
		"expected a real ProvisionedConcurrencyConfigNotFoundException from the SDK deserializer")
}
