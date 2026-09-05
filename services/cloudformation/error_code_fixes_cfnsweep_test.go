package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/require"
)

// TestGetHookResult_UnknownID_RealClient drives GetHookResult through the
// real client with an unknown HookResultId. cloudformation@v1.76.1's
// deserializeOpErrorGetHookResult models HookResultNotFound; gopherstack
// returned a bare "SUCCEEDED" response instead (confirmed by hand-reverting).
func TestGetHookResult_UnknownID_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.GetHookResult(t.Context(), &cfnsdk.GetHookResultInput{
		HookResultId: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var nf *types.HookResultNotFoundException
	require.ErrorAs(t, err, &nf, "expected a real HookResultNotFoundException from the SDK deserializer")
}

// TestDescribeStackInstance_UnknownStackSet_RealClient drives
// DescribeStackInstance through the real client against a StackSetName that
// was never created. cloudformation@v1.76.1's
// deserializeOpErrorDescribeStackInstance models both
// StackInstanceNotFoundException and StackSetNotFoundException;
// gopherstack always emitted StackInstanceNotFoundException, even for a
// wholly unknown stack set (confirmed by hand-reverting).
func TestDescribeStackInstance_UnknownStackSet_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String("no-such-stack-set"),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.Error(t, err)

	var nf *types.StackSetNotFoundException
	require.ErrorAs(t, err, &nf, "expected StackSetNotFoundException, not StackInstanceNotFoundException")
}

// TestDescribeStackInstance_KnownStackSetUnknownInstance_RealClient covers
// the sibling case: a real stack set exists but no instance matches the
// requested account/region, which must still surface
// StackInstanceNotFoundException.
func TestDescribeStackInstance_KnownStackSetUnknownInstance_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("sweep-ss"),
		TemplateBody: aws.String(cfnSweep1Template),
	})
	require.NoError(t, err)

	_, err = client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String("sweep-ss"),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.Error(t, err)

	var nf *types.StackInstanceNotFoundException
	require.ErrorAs(t, err, &nf, "expected StackInstanceNotFoundException for a known stack set")
}

// TestListStackSetOperationResults_UnknownStackSet_RealClient drives
// ListStackSetOperationResults through the real client against a
// StackSetName that was never created. cloudformation@v1.76.1's
// deserializeOpErrorListStackSetOperationResults models
// StackSetNotFoundException; gopherstack's backend silently returned an
// empty result list instead of erroring (confirmed by hand-reverting).
func TestListStackSetOperationResults_UnknownStackSet_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.ListStackSetOperationResults(t.Context(), &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("no-such-stack-set"),
		OperationId:  aws.String("op-1"),
	})
	require.Error(t, err)

	var nf *types.StackSetNotFoundException
	require.ErrorAs(t, err, &nf, "expected a real StackSetNotFoundException from the SDK deserializer")
}

// TestListStackSetOperationResults_KnownStackSetUnknownOperation_RealClient
// covers the sibling case: a real stack set exists but the operation ID
// doesn't, which must surface OperationNotFoundException.
func TestListStackSetOperationResults_KnownStackSetUnknownOperation_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("sweep-ss-2"),
		TemplateBody: aws.String(cfnSweep1Template),
	})
	require.NoError(t, err)

	_, err = client.ListStackSetOperationResults(t.Context(), &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("sweep-ss-2"),
		OperationId:  aws.String("no-such-op"),
	})
	require.Error(t, err)

	var nf *types.OperationNotFoundException
	require.ErrorAs(t, err, &nf, "expected a real OperationNotFoundException from the SDK deserializer")
}
