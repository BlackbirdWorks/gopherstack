package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestListSummaryShapes proves the overwidecandidates-flagged List ops emit
// the real SDK Summary shape, not a leaked Describe-shaped struct: a real
// member round-trips through the typed client, and (where a leak was found
// and fixed) the raw wire body no longer carries the removed field.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testListHookResultsHookResultID, "list_hook_results_hook_result_id"},
		{testListResourceScansPercentageCompleted, "list_resource_scans_percentage_completed"},
		{testListStackInstanceResourceDriftsNarrowShape, "list_stack_instance_resource_drifts_narrow_shape"},
		{testListStackSetsAutoDeploymentAndPermissionModel, "list_stack_sets_auto_deployment_and_permission_model"},
		{testListStacksParentRootAndTemplateDescription, "list_stacks_parent_root_and_template_description"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// testListHookResultsHookResultID proves ListHookResults now emits
// HookResultId, sourced from HookResult.Token (the store's own key), instead
// of silently dropping it.
func testListHookResultsHookResultID(t *testing.T) {
	t.Helper()

	backend, client := newTestHandlerAndClientWithBackend(t)
	backend.AddHookResultInternal(cloudformation.HookResult{
		Token:      "hook-result-abc",
		HookStatus: "HOOK_COMPLETE_SUCCEEDED",
	})

	out, err := client.ListHookResults(t.Context(), &cfnsdk.ListHookResultsInput{})
	require.NoError(t, err)
	require.Len(t, out.HookResults, 1)
	assert.Equal(t, "hook-result-abc", aws.ToString(out.HookResults[0].HookResultId))
	assert.Equal(t, types.HookStatusHookCompleteSucceeded, out.HookResults[0].Status)
}

// testListResourceScansPercentageCompleted proves ListResourceScans emits
// PercentageCompleted, already tracked on the ResourceScan model and
// returned by DescribeResourceScan, but previously omitted here.
func testListResourceScansPercentageCompleted(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.StartResourceScan(ctx, &cfnsdk.StartResourceScanInput{})
	require.NoError(t, err)

	out, err := client.ListResourceScans(ctx, &cfnsdk.ListResourceScansInput{})
	require.NoError(t, err)
	require.Len(t, out.ResourceScanSummaries, 1)
	require.NotNil(t, out.ResourceScanSummaries[0].PercentageCompleted)
	assert.InDelta(t, 100.0, *out.ResourceScanSummaries[0].PercentageCompleted, 0.001)
}

// testListStackInstanceResourceDriftsNarrowShape proves the fix for the one
// real over-wide leak this sweep found: the handler used to emit the wider
// StackResourceDrift wire struct directly (ExpectedProperties/
// ActualProperties included), but the real op's Summaries member is
// StackInstanceResourceDriftsSummary (cloudformation@v1.76.1
// types/types.go:1975), which has neither field.
func testListStackInstanceResourceDriftsNarrowShape(t *testing.T) {
	t.Helper()

	backend := cloudformation.NewInMemoryBackend()
	ctx := t.Context()

	_, err := backend.CreateStackSet("drift-summary-ss", "desc", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)
	_, err = backend.CreateStackInstances(ctx, "drift-summary-ss", []string{"111111111111"}, nil, []string{"us-east-1"})
	require.NoError(t, err)

	instances, err := backend.ListStackInstances(
		"drift-summary-ss", 0, "", cloudformation.ListStackInstancesFilter{},
	)
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	childStackID := instances.Data[0].StackID

	// Force a drift with real ExpectedProperties/ActualProperties content
	// (DetectStackDrift populates resourceDriftDetail with both) so the
	// pre-fix leak would have been observable on the wire.
	require.NoError(t, backend.RecordResourceMutation(childStackID, "MyBucket", map[string]any{"Mutated": true}))
	_, err = backend.DetectStackDrift(childStackID)
	require.NoError(t, err)

	// Raw body: the leaked fields must be gone.
	h := cloudformation.NewHandler(backend)
	rec := postForm(t, h, url.Values{
		"Action":               []string{"ListStackInstanceResourceDrifts"},
		"StackSetName":         []string{"drift-summary-ss"},
		"StackInstanceAccount": []string{"111111111111"},
		"StackInstanceRegion":  []string{"us-east-1"},
	}.Encode())
	body := rec.Body.String()
	assert.NotContains(t, body, "ExpectedProperties")
	assert.NotContains(t, body, "ActualProperties")
	assert.Contains(t, body, "<StackResourceDriftStatus>MODIFIED</StackResourceDriftStatus>")

	// Typed round trip: a real Summary member is populated correctly.
	client := newTestClientForBackend(t, backend)
	out, err := client.ListStackInstanceResourceDrifts(ctx, &cfnsdk.ListStackInstanceResourceDriftsInput{
		StackSetName:         aws.String("drift-summary-ss"),
		OperationId:          aws.String("unused-op-id"),
		StackInstanceAccount: aws.String("111111111111"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Summaries, 1)
	assert.Equal(t, types.StackResourceDriftStatusModified, out.Summaries[0].StackResourceDriftStatus)
	assert.Equal(t, "MyBucket", aws.ToString(out.Summaries[0].LogicalResourceId))
}

// testListStackSetsAutoDeploymentAndPermissionModel proves ListStackSets now
// emits AutoDeployment/ManagedExecution/PermissionModel, all already tracked
// on the StackSet model and returned by DescribeStackSet, but previously
// dropped from the List response's narrower summary type.
func testListStackSetsAutoDeploymentAndPermissionModel(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
		StackSetName:    aws.String("summary-fields-ss"),
		TemplateBody:    aws.String(simpleTemplate),
		PermissionModel: types.PermissionModelsServiceManaged,
		AutoDeployment: &types.AutoDeployment{
			Enabled:                      aws.Bool(true),
			RetainStacksOnAccountRemoval: aws.Bool(true),
		},
		ManagedExecution: &types.ManagedExecution{Active: aws.Bool(true)},
	})
	require.NoError(t, err)

	out, err := client.ListStackSets(ctx, &cfnsdk.ListStackSetsInput{})
	require.NoError(t, err)
	require.Len(t, out.Summaries, 1)
	s := out.Summaries[0]
	assert.Equal(t, types.PermissionModelsServiceManaged, s.PermissionModel)
	require.NotNil(t, s.AutoDeployment)
	assert.True(t, aws.ToBool(s.AutoDeployment.Enabled))
	assert.True(t, aws.ToBool(s.AutoDeployment.RetainStacksOnAccountRemoval))
	require.NotNil(t, s.ManagedExecution)
	assert.True(t, aws.ToBool(s.ManagedExecution.Active))
}

// testListStacksParentRootAndTemplateDescription proves ListStacks now
// emits ParentId/RootId (already tracked on the Stack model for nested
// stacks) and TemplateDescription (sourced from the same stack.Description
// field CreateStack already populates from the template's own Description).
func testListStacksParentRootAndTemplateDescription(t *testing.T) {
	t.Helper()

	// A real ResourceCreator (not the bare NewInMemoryBackend() other cases
	// use) is required for AWS::CloudFormation::Stack to provision an
	// actual nested stack instead of a stub physical ID.
	backend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(nil),
	)
	client := newTestClientForBackend(t, backend)
	ctx := t.Context()

	childTemplate := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"ChildBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`
	parentTemplate := `{"AWSTemplateFormatVersion":"2010-09-09","Description":"parent desc",` +
		`"Resources":{"Child":{"Type":"AWS::CloudFormation::Stack","Properties":{` +
		`"TemplateBody":` + quoteJSON(childTemplate) + `}}}}`

	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("parent-summary-stack"),
		TemplateBody: aws.String(parentTemplate),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("parent-summary-stack"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	parentID := aws.ToString(desc.Stacks[0].StackId)

	out, err := client.ListStacks(ctx, &cfnsdk.ListStacksInput{})
	require.NoError(t, err)

	var parent, child *types.StackSummary
	for i := range out.StackSummaries {
		s := &out.StackSummaries[i]
		switch aws.ToString(s.StackName) {
		case "parent-summary-stack":
			parent = s
		default:
			if aws.ToString(s.ParentId) == parentID {
				child = s
			}
		}
	}

	require.NotNil(t, parent)
	assert.Equal(t, "parent desc", aws.ToString(parent.TemplateDescription))

	require.NotNil(t, child)
	assert.Equal(t, parentID, aws.ToString(child.ParentId))
	assert.Equal(t, parentID, aws.ToString(child.RootId))
}
