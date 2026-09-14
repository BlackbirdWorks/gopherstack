package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestListGeneratedTemplates_MaxResults covers gopherstack-sd1a7:
// handleListGeneratedTemplates read NextToken but always passed 0 for
// MaxResults, so ListGeneratedTemplatesInput.MaxResults (real field,
// cloudformation@v1.76.1 api_op_ListGeneratedTemplates.go:35) was inert.
func TestListGeneratedTemplates_MaxResults(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	ids := make([]string, 0, 3)
	for _, name := range []string{"gt-a", "gt-b", "gt-c"} {
		gt, err := backend.CreateGeneratedTemplate(name, nil)
		require.NoError(t, err)
		ids = append(ids, gt.GeneratedTemplateID)
	}

	page1, err := client.ListGeneratedTemplates(ctx, &cfnsdk.ListGeneratedTemplatesInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListGeneratedTemplates(
		ctx, &cfnsdk.ListGeneratedTemplatesInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListGeneratedTemplates(
		ctx, &cfnsdk.ListGeneratedTemplatesInput{MaxResults: aws.Int32(1), NextToken: page2.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.TemplateSummary{page1.Summaries, page2.Summaries, page3.Summaries} {
		for _, s := range p {
			got = append(got, aws.ToString(s.GeneratedTemplateId))
		}
	}
	assert.ElementsMatch(t, ids, got)
}

// TestListResourceScans_MaxResults covers gopherstack-sd1a7:
// handleListResourceScans read NextToken but always passed 0 for MaxResults,
// so ListResourceScansInput.MaxResults (real field, cloudformation@v1.76.1
// api_op_ListResourceScans.go:35) was inert.
func TestListResourceScans_MaxResults(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	ids := make([]string, 0, 3)
	for range 3 {
		scanID, err := backend.StartResourceScan()
		require.NoError(t, err)
		ids = append(ids, scanID)
	}

	page1, err := client.ListResourceScans(ctx, &cfnsdk.ListResourceScansInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.ResourceScanSummaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListResourceScans(
		ctx, &cfnsdk.ListResourceScansInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.ResourceScanSummaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListResourceScans(
		ctx, &cfnsdk.ListResourceScansInput{MaxResults: aws.Int32(1), NextToken: page2.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page3.ResourceScanSummaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.ResourceScanSummary{
		page1.ResourceScanSummaries, page2.ResourceScanSummaries, page3.ResourceScanSummaries,
	} {
		for _, s := range p {
			got = append(got, aws.ToString(s.ResourceScanId))
		}
	}
	assert.ElementsMatch(t, ids, got)
}

// TestListStackSets_MaxResults covers gopherstack-sd1a7: handleListStackSets
// read NextToken but always passed 0 for MaxResults, so
// ListStackSetsInput.MaxResults (real field, cloudformation@v1.76.1
// api_op_ListStackSets.go:69) was inert.
func TestListStackSets_MaxResults(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	names := []string{"maxres-ss-a", "maxres-ss-b", "maxres-ss-c"}
	for _, name := range names {
		_, err := backend.CreateStackSet(name, "test", simpleTemplate, cloudformation.StackSetOptions{})
		require.NoError(t, err)
	}

	page1, err := client.ListStackSets(ctx, &cfnsdk.ListStackSetsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackSets(
		ctx, &cfnsdk.ListStackSetsInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackSets(
		ctx, &cfnsdk.ListStackSetsInput{MaxResults: aws.Int32(1), NextToken: page2.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.StackSetSummary{page1.Summaries, page2.Summaries, page3.Summaries} {
		for _, s := range p {
			got = append(got, aws.ToString(s.StackSetName))
		}
	}
	assert.ElementsMatch(t, names, got)
}

// TestListStackSetOperations_MaxResults covers gopherstack-sd1a7:
// handleListStackSetOperations read NextToken but always passed 0 for
// MaxResults, so ListStackSetOperationsInput.MaxResults (real field,
// cloudformation@v1.76.1 api_op_ListStackSetOperations.go:63) was inert.
func TestListStackSetOperations_MaxResults(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	_, err := backend.CreateStackSet("maxres-ops-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	// DetectStackSetDrift records one StackSetOperation per call
	// (recordStackSetOperation, stack_sets.go), the simplest way to produce
	// three distinct operations without touching stack instances.
	for range 3 {
		_, driftErr := client.DetectStackSetDrift(
			ctx, &cfnsdk.DetectStackSetDriftInput{StackSetName: aws.String("maxres-ops-ss")},
		)
		require.NoError(t, driftErr)
	}

	page1, err := client.ListStackSetOperations(ctx, &cfnsdk.ListStackSetOperationsInput{
		StackSetName: aws.String("maxres-ops-ss"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackSetOperations(ctx, &cfnsdk.ListStackSetOperationsInput{
		StackSetName: aws.String("maxres-ops-ss"), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackSetOperations(ctx, &cfnsdk.ListStackSetOperationsInput{
		StackSetName: aws.String("maxres-ops-ss"), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make(map[string]bool, 3)
	for _, p := range [][]cfnsdktypes.StackSetOperationSummary{page1.Summaries, page2.Summaries, page3.Summaries} {
		for _, s := range p {
			got[aws.ToString(s.OperationId)] = true
		}
	}
	assert.Len(t, got, 3, "each operation must appear exactly once across the page walk")
}

// TestListStackInstances_MaxResults covers gopherstack-sd1a7:
// handleListStackInstances read NextToken but always passed 0 for
// MaxResults, so ListStackInstancesInput.MaxResults (real field,
// cloudformation@v1.76.1 api_op_ListStackInstances.go:65) was inert.
func TestListStackInstances_MaxResults(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	_, err := backend.CreateStackSet("maxres-inst-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	accounts := []string{"111111111111", "222222222222", "333333333333"}
	_, err = backend.CreateStackInstances(ctx, "maxres-inst-ss", accounts, nil, []string{"us-east-1"})
	require.NoError(t, err)

	page1, err := client.ListStackInstances(ctx, &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String("maxres-inst-ss"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackInstances(ctx, &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String("maxres-inst-ss"), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackInstances(ctx, &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String("maxres-inst-ss"), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.StackInstanceSummary{page1.Summaries, page2.Summaries, page3.Summaries} {
		for _, s := range p {
			got = append(got, aws.ToString(s.Account))
		}
	}
	assert.ElementsMatch(t, accounts, got)
}
