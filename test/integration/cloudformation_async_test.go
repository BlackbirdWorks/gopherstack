package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cfnAsyncTopicTemplate = `{"Resources":{"Topic":{"Type":"AWS::SNS::Topic"}}}`

func cfnStackEventStatuses(t *testing.T, client *cloudformationsdk.Client, stack string) []string {
	t.Helper()

	out, err := client.DescribeStackEvents(t.Context(), &cloudformationsdk.DescribeStackEventsInput{
		StackName: aws.String(stack),
	})
	require.NoError(t, err)

	var got []string

	for _, ev := range slices.Backward(out.StackEvents) {
		if aws.ToString(ev.ResourceType) == "AWS::CloudFormation::Stack" {
			got = append(got, string(ev.ResourceStatus))
		}
	}

	return got
}

func TestIntegration_CloudFormation_DeleteStackWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()
	name := "async-delete-" + cfnStackID()

	created, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(cfnAsyncTopicTemplate),
	})
	require.NoError(t, err)
	waitStackCreated(t, client, name)

	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(name)})
	require.NoError(t, err)

	first, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{StackName: created.StackId})
	require.NoError(t, err)
	require.Len(t, first.Stacks, 1)
	assert.Contains(t,
		[]cftypes.StackStatus{cftypes.StackStatusDeleteInProgress, cftypes.StackStatusDeleteComplete},
		first.Stacks[0].StackStatus,
	)

	waitStackDeleted(t, client, name)

	last, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{StackName: created.StackId})
	require.NoError(t, err, "a deleted stack stays describable by StackId")
	assert.Equal(t, cftypes.StackStatusDeleteComplete, last.Stacks[0].StackStatus)

	_, err = client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.ErrorContains(t, err, "does not exist", "a deleted stack is not describable by name")

	assert.Equal(t, []string{"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "DELETE_IN_PROGRESS", "DELETE_COMPLETE"},
		cfnStackEventStatuses(t, client, aws.ToString(created.StackId)))
}

func TestIntegration_CloudFormation_ExecuteChangeSetWindow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()
	name := "async-changeset-" + cfnStackID()

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(name)})
	})

	_, err := client.CreateChangeSet(ctx, &cloudformationsdk.CreateChangeSetInput{
		StackName: aws.String(name), ChangeSetName: aws.String("initial"),
		TemplateBody: aws.String(cfnAsyncTopicTemplate), ChangeSetType: cftypes.ChangeSetTypeCreate,
	})
	require.NoError(t, err)

	require.NoError(t, cloudformationsdk.NewChangeSetCreateCompleteWaiter(client).Wait(
		ctx,
		&cloudformationsdk.DescribeChangeSetInput{StackName: aws.String(name), ChangeSetName: aws.String("initial")},
		cfnWaitMax,
		func(o *cloudformationsdk.ChangeSetCreateCompleteWaiterOptions) {
			o.MinDelay, o.MaxDelay = cfnWaitMinDelay, cfnWaitMaxDelay
		},
	))

	_, err = client.ExecuteChangeSet(ctx, &cloudformationsdk.ExecuteChangeSetInput{
		StackName: aws.String(name), ChangeSetName: aws.String("initial"),
	})
	require.NoError(t, err)

	waitStackCreated(t, client, name)

	require.Eventually(t, func() bool {
		cs, descErr := client.DescribeChangeSet(ctx, &cloudformationsdk.DescribeChangeSetInput{
			StackName: aws.String(name), ChangeSetName: aws.String("initial"),
		})

		return descErr == nil && cs.ExecutionStatus == cftypes.ExecutionStatusExecuteComplete
	}, cfnWaitMax, cfnWaitMinDelay, "executed change set should reach EXECUTE_COMPLETE")
}

func TestIntegration_CloudFormation_RollbackWindow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()
	name := "async-rollback-" + cfnStackID()

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(name)})
	})

	out, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName: aws.String(name),
		TemplateBody: aws.String(
			`{"Resources":{"Topic":{"Type":"AWS::SNS::Topic","Properties":` +
				`{"TopicName":{"Fn::ImportValue":"missing-export"}}}}}`,
		),
	})
	require.NoError(t, err)

	assert.Equal(t, "ROLLBACK_COMPLETE", waitForStackStatus(t, client, name, cfnWaitMax))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", "CREATE_FAILED", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE"},
		cfnStackEventStatuses(t, client, aws.ToString(out.StackId)))
}

func TestIntegration_CloudFormation_NestedStackWindow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()
	name := "async-nested-" + cfnStackID()

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(name)})
	})

	child, err := json.Marshal(cfnAsyncTopicTemplate)
	require.NoError(t, err)

	out, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName: aws.String(name),
		TemplateBody: aws.String(
			`{"Resources":{"Child":{"Type":"AWS::CloudFormation::Stack","Properties":{"TemplateBody":` +
				string(child) + `}}}}`,
		),
	})
	require.NoError(t, err)

	waitStackCreated(t, client, name)

	all, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{})
	require.NoError(t, err)

	var children []cftypes.Stack

	for _, s := range all.Stacks {
		if aws.ToString(s.ParentId) == aws.ToString(out.StackId) {
			children = append(children, s)
		}
	}

	require.Len(t, children, 1)
	assert.Equal(t, cftypes.StackStatusCreateComplete, children[0].StackStatus,
		"the parent only completes after its child has")
}

func TestIntegration_CloudFormation_StackSetInstanceStatusWindow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	ctx := t.Context()
	name := "async-ss-" + cfnStackID()

	_, err := client.CreateStackSet(ctx, &cloudformationsdk.CreateStackSetInput{
		StackSetName: aws.String(name), TemplateBody: aws.String(cfnAsyncTopicTemplate),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		del, delErr := client.DeleteStackInstances(cleanupCtx, &cloudformationsdk.DeleteStackInstancesInput{
			StackSetName: aws.String(name), Accounts: []string{"111111111111"},
			Regions: []string{"us-east-1"}, RetainStacks: aws.Bool(false),
		})
		if delErr == nil {
			waitStackSetOperation(cleanupCtx, t, client, name, aws.ToString(del.OperationId))
		}

		_, _ = client.DeleteStackSet(cleanupCtx, &cloudformationsdk.DeleteStackSetInput{StackSetName: aws.String(name)})
	})

	op, err := client.CreateStackInstances(ctx, &cloudformationsdk.CreateStackInstancesInput{
		StackSetName: aws.String(name), Accounts: []string{"111111111111"}, Regions: []string{"us-east-1"},
	})
	require.NoError(t, err)

	waitStackSetOperation(ctx, t, client, name, aws.ToString(op.OperationId))

	list, err := client.ListStackInstances(ctx, &cloudformationsdk.ListStackInstancesInput{
		StackSetName: aws.String(name),
	})
	require.NoError(t, err)
	require.Len(t, list.Summaries, 1)
	require.NotNil(t, list.Summaries[0].StackInstanceStatus)
	assert.Equal(t, cftypes.StackInstanceDetailedStatusSucceeded, list.Summaries[0].StackInstanceStatus.DetailedStatus)
}

func waitStackSetOperation(
	ctx context.Context, t *testing.T, client *cloudformationsdk.Client, stackSet, opID string,
) {
	t.Helper()

	assert.Eventually(t, func() bool {
		out, err := client.DescribeStackSetOperation(ctx, &cloudformationsdk.DescribeStackSetOperationInput{
			StackSetName: aws.String(stackSet), OperationId: aws.String(opID),
		})

		return err == nil && out.StackSetOperation.Status == cftypes.StackSetOperationStatusSucceeded
	}, cfnWaitMax, 2*cfnWaitMinDelay, "stack set operation %s should succeed", opID)
}
