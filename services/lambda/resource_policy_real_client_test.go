package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourcePolicy_RealSDKClient drives Get->Put->Get->Delete->not-found
// through the real lambda client, plus the declared PreconditionFailedException
// error path for a stale RevisionId.
func TestResourcePolicy_RealSDKClient(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("rp-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	fnArn := "arn:aws:lambda:us-east-1:000000000000:function:rp-fn"

	_, err = client.GetResourcePolicy(ctx, &lambdasdk.GetResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)

	put, err := client.PutResourcePolicy(ctx, &lambdasdk.PutResourcePolicyInput{
		ResourceArn: aws.String(fnArn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	firstRevision := aws.ToString(put.RevisionId)
	assert.NotEmpty(t, firstRevision)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(put.Policy))

	got, err := client.GetResourcePolicy(ctx, &lambdasdk.GetResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.NoError(t, err)
	assert.Equal(t, firstRevision, aws.ToString(got.RevisionId))

	_, err = client.PutResourcePolicy(ctx, &lambdasdk.PutResourcePolicyInput{
		ResourceArn: aws.String(fnArn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		RevisionId:  aws.String("stale-revision"),
	})
	require.Error(t, err)

	var precond *types.PreconditionFailedException
	require.ErrorAs(t, err, &precond)

	_, err = client.DeleteResourcePolicy(ctx, &lambdasdk.DeleteResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(ctx, &lambdasdk.GetResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.Error(t, err)
	require.ErrorAs(t, err, &nf)
}

// TestResourcePolicy_ReplacesAddPermissionStatements asserts PutResourcePolicy's
// documented behavior: it fully replaces any policy previously built via
// AddPermission, and a subsequent GetPolicy/GetResourcePolicy no longer sees
// the old statements.
func TestResourcePolicy_ReplacesAddPermissionStatements(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("rp-replace-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	_, err = client.AddPermission(ctx, &lambdasdk.AddPermissionInput{
		FunctionName: aws.String("rp-replace-fn"),
		StatementId:  aws.String("stmt1"),
		Action:       aws.String("lambda:InvokeFunction"),
		Principal:    aws.String("s3.amazonaws.com"),
	})
	require.NoError(t, err)

	fnArn := "arn:aws:lambda:us-east-1:000000000000:function:rp-replace-fn"

	before, err := client.GetResourcePolicy(ctx, &lambdasdk.GetResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(before.Policy), "stmt1")

	_, err = client.PutResourcePolicy(ctx, &lambdasdk.PutResourcePolicyInput{
		ResourceArn: aws.String(fnArn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	after, err := client.GetResourcePolicy(ctx, &lambdasdk.GetResourcePolicyInput{ResourceArn: aws.String(fnArn)})
	require.NoError(t, err)
	assert.NotContains(t, aws.ToString(after.Policy), "stmt1")

	// GetPolicy (legacy) and GetResourcePolicy read the same underlying
	// policy document, so the override must be visible through GetPolicy too.
	legacy, err := client.GetPolicy(ctx, &lambdasdk.GetPolicyInput{FunctionName: aws.String("rp-replace-fn")})
	require.NoError(t, err)
	assert.NotContains(t, aws.ToString(legacy.Policy), "stmt1")
}
