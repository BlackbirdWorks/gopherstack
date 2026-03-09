package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdaclientsdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambdaProvisionedConcurrency_FullLifecycle exercises the complete lifecycle of
// provisioned concurrency: put, get, list, and delete via the AWS SDK against a running
// Gopherstack container.
func TestLambdaProvisionedConcurrency_FullLifecycle(t *testing.T) {
	t.Parallel()

	client := createLambdaClient(t)
	ctx := t.Context()

	funcName := "prov-concurrency-lifecycle"
	zipData := buildMinimalZip("index.py", "def handler(e, c): return {}")

	// Create the function.
	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(funcName),
		PackageType:  lambdatypes.PackageTypeZip,
		Code:         &lambdatypes.FunctionCode{ZipFile: zipData},
		Role:         aws.String("arn:aws:iam::000000000000:role/test"),
		Handler:      aws.String("index.handler"),
		Runtime:      lambdatypes.RuntimePython312,
	})
	require.NoError(t, err, "CreateFunction should succeed")

	// Publish a version so we have a valid qualifier.
	publishOut, err := client.PublishVersion(ctx, &lambdaclientsdk.PublishVersionInput{
		FunctionName: aws.String(funcName),
	})
	require.NoError(t, err, "PublishVersion should succeed")

	qualifier := aws.ToString(publishOut.Version)

	// --- PutProvisionedConcurrencyConfig ---
	putOut, err := client.PutProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.PutProvisionedConcurrencyConfigInput{
		FunctionName:                    aws.String(funcName),
		Qualifier:                       aws.String(qualifier),
		ProvisionedConcurrentExecutions: aws.Int32(5),
	})
	require.NoError(t, err, "PutProvisionedConcurrencyConfig should succeed")
	assert.Equal(t, int32(5), aws.ToInt32(putOut.RequestedProvisionedConcurrentExecutions))
	assert.Equal(t, int32(5), aws.ToInt32(putOut.AllocatedProvisionedConcurrentExecutions))
	assert.Equal(t, lambdatypes.ProvisionedConcurrencyStatusEnumReady, putOut.Status)
	assert.NotEmpty(t, aws.ToString(putOut.LastModified))

	// --- GetProvisionedConcurrencyConfig ---
	getOut, err := client.GetProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.GetProvisionedConcurrencyConfigInput{
		FunctionName: aws.String(funcName),
		Qualifier:    aws.String(qualifier),
	})
	require.NoError(t, err, "GetProvisionedConcurrencyConfig should succeed")
	assert.Equal(t, int32(5), aws.ToInt32(getOut.RequestedProvisionedConcurrentExecutions))
	assert.Equal(t, lambdatypes.ProvisionedConcurrencyStatusEnumReady, getOut.Status)

	// --- ListProvisionedConcurrencyConfigs ---
	listOut, err := client.ListProvisionedConcurrencyConfigs(
		ctx,
		&lambdaclientsdk.ListProvisionedConcurrencyConfigsInput{
			FunctionName: aws.String(funcName),
		},
	)
	require.NoError(t, err, "ListProvisionedConcurrencyConfigs should succeed")
	assert.Len(t, listOut.ProvisionedConcurrencyConfigs, 1, "should have one provisioned concurrency config")

	found := false

	for _, cfg := range listOut.ProvisionedConcurrencyConfigs {
		if aws.ToInt32(cfg.RequestedProvisionedConcurrentExecutions) == 5 {
			found = true

			break
		}
	}

	assert.True(t, found, "ListProvisionedConcurrencyConfigs should contain the config we created")

	// --- DeleteProvisionedConcurrencyConfig ---
	_, err = client.DeleteProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.DeleteProvisionedConcurrencyConfigInput{
		FunctionName: aws.String(funcName),
		Qualifier:    aws.String(qualifier),
	})
	require.NoError(t, err, "DeleteProvisionedConcurrencyConfig should succeed")

	// Verify it was deleted — Get should return not found.
	_, err = client.GetProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.GetProvisionedConcurrencyConfigInput{
		FunctionName: aws.String(funcName),
		Qualifier:    aws.String(qualifier),
	})
	require.Error(t, err, "GetProvisionedConcurrencyConfig after delete should fail")

	// Verify list is now empty.
	listOut2, err := client.ListProvisionedConcurrencyConfigs(
		ctx,
		&lambdaclientsdk.ListProvisionedConcurrencyConfigsInput{
			FunctionName: aws.String(funcName),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, listOut2.ProvisionedConcurrencyConfigs, "list should be empty after delete")

	// Cleanup.
	_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{
		FunctionName: aws.String(funcName),
	})
}

// TestLambdaProvisionedConcurrency_MultipleQualifiers verifies that provisioned concurrency
// can be set independently for multiple qualifiers of the same function.
func TestLambdaProvisionedConcurrency_MultipleQualifiers(t *testing.T) {
	t.Parallel()

	client := createLambdaClient(t)
	ctx := t.Context()

	funcName := "prov-concurrency-multi-qual"
	zipData := buildMinimalZip("index.py", "def handler(e, c): return {}")

	// Create the function.
	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(funcName),
		PackageType:  lambdatypes.PackageTypeZip,
		Code:         &lambdatypes.FunctionCode{ZipFile: zipData},
		Role:         aws.String("arn:aws:iam::000000000000:role/test"),
		Handler:      aws.String("index.handler"),
		Runtime:      lambdatypes.RuntimePython312,
	})
	require.NoError(t, err)

	// Publish two versions.
	v1Out, err := client.PublishVersion(ctx, &lambdaclientsdk.PublishVersionInput{
		FunctionName: aws.String(funcName),
	})
	require.NoError(t, err)

	v2Out, err := client.PublishVersion(ctx, &lambdaclientsdk.PublishVersionInput{
		FunctionName: aws.String(funcName),
	})
	require.NoError(t, err)

	v1 := aws.ToString(v1Out.Version)
	v2 := aws.ToString(v2Out.Version)

	// Set provisioned concurrency for both versions.
	_, err = client.PutProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.PutProvisionedConcurrencyConfigInput{
		FunctionName:                    aws.String(funcName),
		Qualifier:                       aws.String(v1),
		ProvisionedConcurrentExecutions: aws.Int32(3),
	})
	require.NoError(t, err)

	_, err = client.PutProvisionedConcurrencyConfig(ctx, &lambdaclientsdk.PutProvisionedConcurrencyConfigInput{
		FunctionName:                    aws.String(funcName),
		Qualifier:                       aws.String(v2),
		ProvisionedConcurrentExecutions: aws.Int32(7),
	})
	require.NoError(t, err)

	// List should return both configs.
	listOut, err := client.ListProvisionedConcurrencyConfigs(
		ctx,
		&lambdaclientsdk.ListProvisionedConcurrencyConfigsInput{
			FunctionName: aws.String(funcName),
		},
	)
	require.NoError(t, err)
	assert.Len(t, listOut.ProvisionedConcurrencyConfigs, 2, "should have two provisioned concurrency configs")

	// Cleanup.
	_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{
		FunctionName: aws.String(funcName),
	})
}
