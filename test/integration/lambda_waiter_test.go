package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdaclientsdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Lambda_FunctionExistsWaiter verifies that FunctionExistsWaiter
// succeeds immediately after CreateFunction.
func TestIntegration_Lambda_FunctionExistsWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createLambdaClient(t)
	ctx := t.Context()

	fnName := "waiter-exists-" + uuid.NewString()[:8]

	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Role:         aws.String("arn:aws:iam::123456789012:role/test-role"),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("public.ecr.aws/lambda/provided:al2")},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{FunctionName: aws.String(fnName)})
	})

	waiter := lambdaclientsdk.NewFunctionExistsWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "FunctionExistsWaiter should succeed after function is created")
	assert.Less(t, elapsed, 2*time.Second, "FunctionExistsWaiter should complete quickly, took %v", elapsed)
}

// TestIntegration_Lambda_FunctionActiveV2Waiter verifies that FunctionActiveV2Waiter
// succeeds immediately after CreateFunction because State is Active.
func TestIntegration_Lambda_FunctionActiveV2Waiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createLambdaClient(t)
	ctx := t.Context()

	fnName := "waiter-active-" + uuid.NewString()[:8]

	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Role:         aws.String("arn:aws:iam::123456789012:role/test-role"),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("public.ecr.aws/lambda/provided:al2")},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{FunctionName: aws.String(fnName)})
	})

	// Verify the State is Active
	out, err := client.GetFunction(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)})
	require.NoError(t, err)
	require.NotNil(t, out.Configuration)
	assert.Equal(t, lambdatypes.StateActive, out.Configuration.State)

	waiter := lambdaclientsdk.NewFunctionActiveV2Waiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "FunctionActiveV2Waiter should succeed immediately after creation")
	assert.Less(t, elapsed, 2*time.Second, "FunctionActiveV2Waiter should complete quickly, took %v", elapsed)
}

// TestIntegration_Lambda_FunctionUpdatedV2Waiter verifies that FunctionUpdatedV2Waiter
// succeeds immediately after CreateFunction because LastUpdateStatus is Successful.
func TestIntegration_Lambda_FunctionUpdatedV2Waiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createLambdaClient(t)
	ctx := t.Context()

	fnName := "waiter-updated-" + uuid.NewString()[:8]

	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Role:         aws.String("arn:aws:iam::123456789012:role/test-role"),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("public.ecr.aws/lambda/provided:al2")},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{FunctionName: aws.String(fnName)})
	})

	// Verify LastUpdateStatus is Successful
	out, err := client.GetFunction(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)})
	require.NoError(t, err)
	require.NotNil(t, out.Configuration)
	assert.Equal(t, lambdatypes.LastUpdateStatusSuccessful, out.Configuration.LastUpdateStatus)

	waiter := lambdaclientsdk.NewFunctionUpdatedV2Waiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "FunctionUpdatedV2Waiter should succeed immediately after creation")
	assert.Less(t, elapsed, 2*time.Second, "FunctionUpdatedV2Waiter should complete quickly, took %v", elapsed)
}

// TestIntegration_Lambda_FunctionUpdatedV2Waiter_AfterUpdate verifies that
// FunctionUpdatedV2Waiter succeeds after UpdateFunctionConfiguration.
func TestIntegration_Lambda_FunctionUpdatedV2Waiter_AfterUpdate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createLambdaClient(t)
	ctx := t.Context()

	fnName := "waiter-upd-cfg-" + uuid.NewString()[:8]

	_, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Role:         aws.String("arn:aws:iam::123456789012:role/test-role"),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("public.ecr.aws/lambda/provided:al2")},
		Description:  aws.String("original"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{FunctionName: aws.String(fnName)})
	})

	// Update the function configuration
	_, err = client.UpdateFunctionConfiguration(ctx, &lambdaclientsdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		Description:  aws.String("updated"),
	})
	require.NoError(t, err)

	// Verify LastUpdateStatus is Successful after update
	out, err := client.GetFunction(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)})
	require.NoError(t, err)
	require.NotNil(t, out.Configuration)
	assert.Equal(t, lambdatypes.LastUpdateStatusSuccessful, out.Configuration.LastUpdateStatus)

	waiter := lambdaclientsdk.NewFunctionUpdatedV2Waiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &lambdaclientsdk.GetFunctionInput{FunctionName: aws.String(fnName)}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "FunctionUpdatedV2Waiter should succeed after UpdateFunctionConfiguration")
	assert.Less(t, elapsed, 2*time.Second, "FunctionUpdatedV2Waiter should complete quickly, took %v", elapsed)
}
