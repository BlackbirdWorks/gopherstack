package cloudformation_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

const asyncTemplate = `{"Resources":{"B":{"Type":"AWS::SNS::Topic"}}}`

const asyncTemplateV2 = `{"Resources":{"B":{"Type":"AWS::SNS::Topic"},"C":{"Type":"AWS::SNS::Topic"}}}`

type manualScheduler struct{ queue chan func() }

func newManualScheduler(b *cloudformation.InMemoryBackend) *manualScheduler {
	s := &manualScheduler{queue: make(chan func(), 16)}
	b.SetProvisioningScheduler(func(run func()) { s.queue <- run })

	return s
}

func (s *manualScheduler) runNext(t *testing.T) {
	t.Helper()

	select {
	case run := <-s.queue:
		run()
	default:
		require.Fail(t, "no scheduled provisioning step")
	}
}

func stackStatus(t *testing.T, client *cfnsdk.Client, name string) string {
	t.Helper()

	out, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, out.Stacks, 1)

	return string(out.Stacks[0].StackStatus)
}

func eventStatuses(t *testing.T, client *cfnsdk.Client, name string) []string {
	t.Helper()

	out, err := client.DescribeStackEvents(t.Context(), &cfnsdk.DescribeStackEventsInput{StackName: aws.String(name)})
	require.NoError(t, err)

	var got []string

	for _, e := range out.StackEvents {
		if aws.ToString(e.ResourceType) == "AWS::CloudFormation::Stack" {
			got = append(got, string(e.ResourceStatus))
		}
	}

	return got
}

func TestAsyncProvisioning_StatusTransitions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, b *cloudformation.InMemoryBackend, c *cfnsdk.Client, s *manualScheduler)
		name string
	}{
		{name: "create", run: asyncCreate},
		{name: "update", run: asyncUpdate},
		{name: "create_rollback", run: asyncCreateRollback},
		{name: "update_rejected_in_progress", run: asyncUpdateRejected},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, client := newTestHandlerAndClientWithBackend(t)
			s := newManualScheduler(b)
			t.Cleanup(b.WaitForStackProvisioning)
			tc.run(t, b, client, s)
		})
	}
}

func asyncCreate(t *testing.T, _ *cloudformation.InMemoryBackend, c *cfnsdk.Client, s *manualScheduler) {
	t.Helper()

	_, err := c.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-create"), TemplateBody: aws.String(asyncTemplate),
	})
	require.NoError(t, err)

	assert.Equal(t, "CREATE_IN_PROGRESS", stackStatus(t, c, "async-create"))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS"}, eventStatuses(t, c, "async-create"))

	s.runNext(t)

	assert.Equal(t, "CREATE_COMPLETE", stackStatus(t, c, "async-create"))
	assert.Equal(t, []string{"CREATE_COMPLETE", "CREATE_IN_PROGRESS"}, eventStatuses(t, c, "async-create"))
}

func asyncUpdate(t *testing.T, _ *cloudformation.InMemoryBackend, c *cfnsdk.Client, s *manualScheduler) {
	t.Helper()

	_, err := c.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-update"), TemplateBody: aws.String(asyncTemplate),
	})
	require.NoError(t, err)
	s.runNext(t)

	_, err = c.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("async-update"), TemplateBody: aws.String(asyncTemplateV2),
	})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_IN_PROGRESS", stackStatus(t, c, "async-update"))

	s.runNext(t)
	assert.Equal(t, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", stackStatus(t, c, "async-update"))

	s.runNext(t)
	assert.Equal(t, "UPDATE_COMPLETE", stackStatus(t, c, "async-update"))

	res, err := c.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String("async-update"),
	})
	require.NoError(t, err)
	assert.Len(t, res.StackResources, 2)
}

func asyncCreateRollback(t *testing.T, _ *cloudformation.InMemoryBackend, c *cfnsdk.Client, s *manualScheduler) {
	t.Helper()

	tmpl := `{"Resources":{"B":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":{"Fn::ImportValue":"missing"}}}}}`

	_, err := c.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-rollback"), TemplateBody: aws.String(tmpl),
	})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_IN_PROGRESS", stackStatus(t, c, "async-rollback"))

	s.runNext(t)
	assert.Contains(t, []string{"ROLLBACK_COMPLETE", "CREATE_FAILED"}, stackStatus(t, c, "async-rollback"))
}

func asyncUpdateRejected(t *testing.T, _ *cloudformation.InMemoryBackend, c *cfnsdk.Client, s *manualScheduler) {
	t.Helper()

	_, err := c.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-busy"), TemplateBody: aws.String(asyncTemplate),
	})
	require.NoError(t, err)

	_, err = c.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("async-busy"), TemplateBody: aws.String(asyncTemplateV2),
	})
	require.ErrorContains(t, err, "CREATE_IN_PROGRESS state and can not be updated")

	s.runNext(t)
	assert.Equal(t, "CREATE_COMPLETE", stackStatus(t, c, "async-busy"))
}

func TestAsyncProvisioning_WaiterCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, client := newTestHandlerAndClientWithBackend(t)
		b.SetProvisioningDelay(time.Millisecond)
		t.Cleanup(b.WaitForStackProvisioning)

		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName: aws.String("async-waiter"), TemplateBody: aws.String(asyncTemplate),
		})
		require.NoError(t, err)

		waiter := cfnsdk.NewStackCreateCompleteWaiter(client, func(o *cfnsdk.StackCreateCompleteWaiterOptions) {
			o.MinDelay = time.Millisecond
			o.MaxDelay = 5 * time.Millisecond
		})
		require.NoError(t, waiter.Wait(t.Context(), &cfnsdk.DescribeStacksInput{
			StackName: aws.String("async-waiter"),
		}, 30*time.Second))
	})
}

func TestAsyncProvisioning_SnapshotResumesInFlight(t *testing.T) {
	t.Parallel()

	src, client := newTestHandlerAndClientWithBackend(t)
	srcSched := newManualScheduler(src)
	t.Cleanup(src.WaitForStackProvisioning)

	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-persist"), TemplateBody: aws.String(asyncTemplate),
	})
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())
	require.NotNil(t, snap)

	dst, dstClient := newTestHandlerAndClientWithBackend(t)
	dstSched := newManualScheduler(dst)
	t.Cleanup(dst.WaitForStackProvisioning)

	require.NoError(t, dst.Restore(t.Context(), snap))
	assert.Equal(t, "CREATE_IN_PROGRESS", stackStatus(t, dstClient, "async-persist"))

	dstSched.runNext(t)
	assert.Equal(t, "CREATE_COMPLETE", stackStatus(t, dstClient, "async-persist"))

	srcSched.runNext(t)
}

func TestAsyncProvisioning_RestoreWithoutSchedulerSettles(t *testing.T) {
	t.Parallel()

	src, client := newTestHandlerAndClientWithBackend(t)
	srcSched := newManualScheduler(src)
	t.Cleanup(src.WaitForStackProvisioning)

	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("async-settle"), TemplateBody: aws.String(asyncTemplate),
	})
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())

	dst, dstClient := newTestHandlerAndClientWithBackend(t)
	require.NoError(t, dst.Restore(t.Context(), snap))
	assert.Equal(t, "CREATE_COMPLETE", stackStatus(t, dstClient, "async-settle"))

	srcSched.runNext(t)
}
