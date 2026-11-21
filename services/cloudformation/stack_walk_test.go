package cloudformation_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	waveTemplate = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SQS::Queue","DependsOn":"A"},` +
		`"C":{"Type":"AWS::SNS::Topic","DependsOn":"B"},"D":{"Type":"AWS::SNS::Topic"}}}`
	middleFailTemplate = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SNS::Topic","DependsOn":"A"},` +
		`"M":{"Type":"AWS::SQS::Queue","DependsOn":"B"},"C":{"Type":"AWS::SNS::Topic","DependsOn":"M"}}}`
	replaceV1 = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"walk-old"}},` +
		`"T":{"Type":"AWS::SNS::Topic"}}}`
	replaceV2 = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"walk-new"}}}}`
	retainTpl = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SQS::Queue","DependsOn":"A","DeletionPolicy":"Retain"}}}`
	cancelFrom = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"}}}`
	cancelTo   = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SNS::Topic","DependsOn":"A"},"C":{"Type":"AWS::SNS::Topic","DependsOn":"B"}}}`
	rollbackFailTpl = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"` + bucketName + `"}},` +
		`"Q":{"Type":"AWS::SQS::Queue","DependsOn":"Bucket"}}}`
)

func (e *transitionEnv) resourceStatuses(t *testing.T, stack string) map[string]string {
	t.Helper()

	out, err := e.client.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String(stack),
	})
	require.NoError(t, err)

	got := make(map[string]string, len(out.StackResources))
	for _, r := range out.StackResources {
		got[aws.ToString(r.LogicalResourceId)] = string(r.ResourceStatus)
	}

	return got
}

func (e *transitionEnv) drain(t *testing.T) int {
	t.Helper()

	steps := 0

	for len(e.sched.queue) > 0 {
		e.sched.runNext(t)

		steps++
	}

	return steps
}

func (e *transitionEnv) fillBucket(t *testing.T) {
	t.Helper()

	_, err := e.backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String("k"), Body: strings.NewReader("x"),
	})
	assert.NoError(t, err)
}

func (e *transitionEnv) emptyBucket(t *testing.T) {
	t.Helper()

	_, err := e.backends.S3.Backend.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String("k"),
	})
	require.NoError(t, err)
}

func TestAsyncWalk_CreateWaves(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("waves"), TemplateBody: aws.String(waveTemplate),
	})
	require.NoError(t, err)

	steps := []struct {
		resources map[string]string
		stack     string
	}{
		{
			stack:     "CREATE_IN_PROGRESS",
			resources: map[string]string{"A": "CREATE_IN_PROGRESS", "D": "CREATE_IN_PROGRESS"},
		},
		{
			stack: "CREATE_IN_PROGRESS",
			resources: map[string]string{
				"A": "CREATE_COMPLETE", "D": "CREATE_COMPLETE", "B": "CREATE_IN_PROGRESS",
			},
		},
		{
			stack: "CREATE_IN_PROGRESS",
			resources: map[string]string{
				"A": "CREATE_COMPLETE", "D": "CREATE_COMPLETE", "B": "CREATE_COMPLETE", "C": "CREATE_IN_PROGRESS",
			},
		},
		{
			stack: "CREATE_COMPLETE",
			resources: map[string]string{
				"A": "CREATE_COMPLETE", "D": "CREATE_COMPLETE", "B": "CREATE_COMPLETE", "C": "CREATE_COMPLETE",
			},
		},
	}

	for i, want := range steps {
		if i > 0 {
			e.sched.runNext(t)
		}

		assert.Equal(t, want.stack, e.status(t, "waves"), "stack after step %d", i)
		assert.Equal(t, want.resources, e.resourceStatuses(t, "waves"), "resources after step %d", i)
	}

	assert.Empty(t, e.sched.queue)
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", initiated, "CREATE_COMPLETE"}, e.events(t, "waves", "C"))
	assert.Less(t, e.order(t, "waves", "A", "CREATE_COMPLETE"), e.order(t, "waves", "B", "CREATE_IN_PROGRESS"))
	assert.Less(t, e.order(t, "waves", "B", "CREATE_COMPLETE"), e.order(t, "waves", "C", "CREATE_IN_PROGRESS"))
	assert.Less(t, e.order(t, "waves", "D", "CREATE_IN_PROGRESS"), e.order(t, "waves", "B", "CREATE_IN_PROGRESS"),
		"the independent resource starts in the first wave")
}

func TestAsyncWalk_InFlightResourceShapes(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("shapes"), TemplateBody: aws.String(waveTemplate),
	})
	require.NoError(t, err)

	one, err := e.client.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
		StackName: aws.String("shapes"), LogicalResourceId: aws.String("A"),
	})
	require.NoError(t, err)
	assert.Equal(t, cfntypes.ResourceStatusCreateInProgress, one.StackResourceDetail.ResourceStatus)
	assert.Empty(t, aws.ToString(one.StackResourceDetail.PhysicalResourceId), "no physical id before creation")

	list, err := e.client.ListStackResources(t.Context(), &cfnsdk.ListStackResourcesInput{
		StackName: aws.String("shapes"),
	})
	require.NoError(t, err)
	require.Len(t, list.StackResourceSummaries, 2)

	for _, s := range list.StackResourceSummaries {
		assert.Equal(t, cfntypes.ResourceStatusCreateInProgress, s.ResourceStatus)
	}

	_, err = e.client.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
		StackName: aws.String("shapes"), LogicalResourceId: aws.String("C"),
	})
	require.Error(t, err, "a resource that has not started is not listed yet")
}

func TestAsyncWalk_MiddleFailureRollsBackCreated(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.failQueues(nil)
	e.schedule(t)

	out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("middle"), TemplateBody: aws.String(middleFailTemplate),
	})
	require.NoError(t, err)

	id := aws.ToString(out.StackId)

	for range 3 {
		e.sched.runNext(t)
	}

	assert.Equal(t, "ROLLBACK_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, map[string]string{"A": "CREATE_COMPLETE", "B": "DELETE_IN_PROGRESS"}, e.resourceStatuses(t, id),
		"only what was created is rolled back, newest dependents first")
	assert.Empty(t, e.events(t, id, "C"), "resources after the failed one never start")
	assert.Equal(t, "CREATE_FAILED", e.statuses(t, id, "M")[len(e.statuses(t, id, "M"))-1])

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, map[string]string{"A": "DELETE_IN_PROGRESS"}, e.resourceStatuses(t, id))

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_COMPLETE", e.status(t, id))
	assert.Empty(t, e.resourceIDs(t, id))
	assert.Less(t, e.order(t, id, "B", "DELETE_COMPLETE"), e.order(t, id, "A", "DELETE_IN_PROGRESS"))
	assert.Equal(t,
		[]string{"CREATE_IN_PROGRESS", initiated, "CREATE_COMPLETE", "DELETE_IN_PROGRESS", "DELETE_COMPLETE"},
		e.statusesWithReason(t, id, "A"))
}

func (e *transitionEnv) statusesWithReason(t *testing.T, stack, logicalID string) []string {
	t.Helper()

	got := e.events(t, stack, logicalID)
	for i, entry := range got {
		if entry == "CREATE_IN_PROGRESS|Resource creation Initiated" {
			got[i] = initiated
		}
	}

	return got
}

func TestAsyncWalk_UpdateReplacementAndCleanup(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	id := e.createSync(t, "replace", replaceV1)
	e.schedule(t)

	_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("replace"), TemplateBody: aws.String(replaceV2),
	})
	require.NoError(t, err)

	assert.Equal(t, "UPDATE_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, "UPDATE_IN_PROGRESS", e.resourceStatuses(t, id)["Bucket"])

	var trail []string

	for len(e.sched.queue) > 0 {
		e.sched.runNext(t)

		trail = append(trail, e.status(t, id))
	}

	assert.Equal(t, []string{
		"UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
		"UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_COMPLETE",
	}, trail, "replaced and removed resources are deleted during the cleanup phase, one wave per step")

	bucket := e.events(t, id, "Bucket")
	assert.Equal(t, []string{
		"UPDATE_IN_PROGRESS|Requested update requires the creation of a new physical resource; " +
			"hence creating one.",
		"UPDATE_IN_PROGRESS|Resource creation Initiated", "UPDATE_COMPLETE",
		"DELETE_IN_PROGRESS", "DELETE_COMPLETE",
	}, bucket[3:])
	assert.Less(t, e.order(t, id, "Bucket", "UPDATE_COMPLETE"), e.order(t, id, "Bucket", "DELETE_IN_PROGRESS"))
	assert.Less(t, e.order(t, id, "Bucket", "DELETE_COMPLETE"), e.order(t, id, "T", "DELETE_IN_PROGRESS"),
		"replaced resources are cleaned up before dropped ones")
	assert.Equal(t, []string{"Bucket"}, e.resourceIDs(t, id))
	assert.Equal(t, "UPDATE_COMPLETE", e.resourceStatuses(t, id)["Bucket"])
}

func TestAsyncWalk_DeleteOrderWithRetain(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	id := e.createSync(t, "retain", retainTpl)
	e.schedule(t)

	require.NoError(t, e.deleteStack(t, "retain"))
	assert.Equal(t, "DELETE_IN_PROGRESS", e.resourceStatuses(t, id)["B"])
	assert.Equal(t, "CREATE_COMPLETE", e.resourceStatuses(t, id)["A"])

	e.sched.runNext(t)
	assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, map[string]string{"A": "DELETE_IN_PROGRESS"}, e.resourceStatuses(t, id),
		"the retained resource leaves the stack, its dependency is next")

	e.sched.runNext(t)
	assert.Equal(t, "DELETE_COMPLETE", e.status(t, id))
	assert.Equal(t, "DELETE_SKIPPED", e.statuses(t, id, "B")[len(e.statuses(t, id, "B"))-1])
	assert.Less(t, e.order(t, id, "B", "DELETE_SKIPPED"), e.order(t, id, "A", "DELETE_IN_PROGRESS"))
	assert.Equal(t, "DELETE_COMPLETE", e.statuses(t, id, "A")[len(e.statuses(t, id, "A"))-1])
}

func TestAsyncWalk_CancelBetweenResources(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	id := e.createSync(t, "cancel", cancelFrom)
	e.schedule(t)

	_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("cancel"), TemplateBody: aws.String(cancelTo),
	})
	require.NoError(t, err)

	e.sched.runNext(t)
	assert.Equal(t, "UPDATE_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, "CREATE_COMPLETE", e.resourceStatuses(t, id)["B"])
	assert.Equal(t, "CREATE_IN_PROGRESS", e.resourceStatuses(t, id)["C"])

	_, err = e.client.CancelUpdateStack(t.Context(), &cfnsdk.CancelUpdateStackInput{StackName: aws.String("cancel")})
	require.NoError(t, err)

	assert.Equal(t, "UPDATE_ROLLBACK_IN_PROGRESS", e.status(t, id))
	assert.Equal(t, "CREATE_FAILED|Resource creation cancelled", e.events(t, id, "C")[1])
	assert.Equal(t, map[string]string{"A": "CREATE_COMPLETE", "B": "DELETE_IN_PROGRESS"}, e.resourceStatuses(t, id))

	e.drain(t)

	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", e.status(t, id))
	assert.Equal(t, []string{"A"}, e.resourceIDs(t, id))
	assert.Equal(t, []string{
		"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS",
		"UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
	}, e.stackStatuses(t, id))

	tmpl, err := e.client.GetTemplate(t.Context(), &cfnsdk.GetTemplateInput{StackName: aws.String("cancel")})
	require.NoError(t, err)
	assert.JSONEq(t, cancelFrom, aws.ToString(tmpl.TemplateBody))
}

func failedRollbackEnv(t *testing.T) (*transitionEnv, string) {
	t.Helper()

	e := newTransitionEnv(t)
	id := e.createSync(t, "ur", cancelFrom)
	e.failQueues(func() { e.fillBucket(t) })
	e.schedule(t)

	_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("ur"), TemplateBody: aws.String(rollbackFailTpl),
	})
	require.NoError(t, err)

	e.drain(t)
	require.Equal(t, "UPDATE_ROLLBACK_FAILED", e.status(t, id))

	return e, id
}

func TestAsyncWalk_UpdateRollbackFailed(t *testing.T) {
	t.Parallel()

	e, id := failedRollbackEnv(t)

	assert.Equal(t, "UPDATE_FAILED", e.resourceStatuses(t, id)["Bucket"])
	assert.Equal(t, "CREATE_COMPLETE", e.resourceStatuses(t, id)["A"])
	assert.NotContains(t, e.resourceStatuses(t, id), "Q")

	trail := e.statuses(t, id, "Bucket")
	assert.Equal(t, []string{"DELETE_IN_PROGRESS", "UPDATE_FAILED"}, trail[len(trail)-2:])
	assert.Equal(t, []string{
		"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS",
		"UPDATE_ROLLBACK_FAILED",
	}, e.stackStatuses(t, id))
}

func TestAsyncWalk_ContinueUpdateRollback(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fix        func(t *testing.T, e *transitionEnv)
		wantRes    map[string]string
		in         cfnsdk.ContinueUpdateRollbackInput
		name       string
		wantErr    string
		wantStatus string
	}{
		{
			name:       "retry_after_fix",
			fix:        func(t *testing.T, e *transitionEnv) { t.Helper(); e.emptyBucket(t) },
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
			wantRes:    map[string]string{"A": "CREATE_COMPLETE"},
		},
		{
			name:       "retry_fails_again",
			wantStatus: "UPDATE_ROLLBACK_FAILED",
			wantRes:    map[string]string{"A": "CREATE_COMPLETE", "Bucket": "UPDATE_FAILED"},
		},
		{
			name:       "skip_resource",
			in:         cfnsdk.ContinueUpdateRollbackInput{ResourcesToSkip: []string{"Bucket"}},
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
			wantRes:    map[string]string{"A": "CREATE_COMPLETE", "Bucket": "UPDATE_COMPLETE"},
		},
		{
			name:       "force_rollback",
			in:         cfnsdk.ContinueUpdateRollbackInput{ForceRollback: aws.Bool(true)},
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
			wantRes:    map[string]string{"A": "CREATE_COMPLETE", "Bucket": "UPDATE_COMPLETE"},
		},
		{
			name:    "skip_healthy_resource",
			in:      cfnsdk.ContinueUpdateRollbackInput{ResourcesToSkip: []string{"A"}},
			wantErr: "cannot be skipped",
		},
		{
			name: "force_and_skip",
			in: cfnsdk.ContinueUpdateRollbackInput{
				ForceRollback: aws.Bool(true), ResourcesToSkip: []string{"Bucket"},
			},
			wantErr: "mutually exclusive",
		},
		{
			name:    "nested_entry_without_nested_stack",
			in:      cfnsdk.ContinueUpdateRollbackInput{ResourcesToSkip: []string{"NoSuchStack.Bucket"}},
			wantErr: "not a nested stack",
		},
		{
			name:    "bad_role",
			in:      cfnsdk.ContinueUpdateRollbackInput{RoleARN: aws.String("not-an-arn")},
			wantErr: "role",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e, id := failedRollbackEnv(t)

			if tc.fix != nil {
				tc.fix(t, e)
			}

			in := tc.in
			in.StackName = aws.String(id)

			_, err := e.client.ContinueUpdateRollback(t.Context(), &in)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Equal(t, "UPDATE_ROLLBACK_FAILED", e.status(t, id), "a rejected request changes nothing")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "UPDATE_ROLLBACK_IN_PROGRESS", e.status(t, id))

			e.drain(t)

			assert.Equal(t, tc.wantStatus, e.status(t, id))
			assert.Equal(t, tc.wantRes, e.resourceStatuses(t, id))
		})
	}
}

func TestAsyncWalk_ContinueUpdateRollbackEvents(t *testing.T) {
	t.Parallel()

	e, id := failedRollbackEnv(t)
	e.emptyBucket(t)

	_, err := e.client.ContinueUpdateRollback(t.Context(), &cfnsdk.ContinueUpdateRollbackInput{
		StackName: aws.String(id), RoleARN: aws.String("arn:aws:iam::000000000000:role/rb"),
	})
	require.NoError(t, err)

	e.drain(t)

	assert.Equal(t, []string{
		"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS",
		"UPDATE_ROLLBACK_FAILED", "UPDATE_ROLLBACK_IN_PROGRESS",
		"UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
	}, e.stackStatuses(t, id))

	trail := e.statuses(t, id, "Bucket")
	assert.Equal(t, []string{"DELETE_IN_PROGRESS", "UPDATE_FAILED", "DELETE_IN_PROGRESS", "DELETE_COMPLETE"},
		trail[len(trail)-4:])
	assert.Equal(t, "arn:aws:iam::000000000000:role/rb", aws.ToString(describeOne(t, e.client, id).RoleARN))

	_, err = e.client.ContinueUpdateRollback(t.Context(), &cfnsdk.ContinueUpdateRollbackInput{
		StackName: aws.String(id),
	})
	require.ErrorContains(t, err, "is in UPDATE_ROLLBACK_COMPLETE state and can not be continue rolled back")
}

func TestAsyncWalk_ContinueUpdateRollbackInvalidState(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	id := e.createSync(t, "valid", cancelFrom)

	_, err := e.client.ContinueUpdateRollback(t.Context(), &cfnsdk.ContinueUpdateRollbackInput{
		StackName: aws.String(id),
	})
	require.ErrorContains(t, err, "is in CREATE_COMPLETE state and can not be continue rolled back")
	assert.Contains(t, err.Error(), "ValidationError")

	_, err = e.client.ContinueUpdateRollback(t.Context(), &cfnsdk.ContinueUpdateRollbackInput{
		StackName: aws.String("missing"),
	})
	require.Error(t, err)
}

func TestAsyncWalk_SyncEventOrderMatchesAsync(t *testing.T) {
	t.Parallel()

	signature := func(t *testing.T, e *transitionEnv, stack string) []string {
		t.Helper()

		events := e.allEvents(t, stack)
		out := make([]string, 0, len(events))

		for _, ev := range events {
			out = append(out, aws.ToString(ev.LogicalResourceId)+"|"+string(ev.ResourceStatus)+"|"+
				aws.ToString(ev.ResourceStatusReason))
		}

		return out
	}

	run := func(t *testing.T, async bool) []string {
		t.Helper()

		e := newTransitionEnv(t)
		if async {
			e.schedule(t)
		}

		_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName: aws.String("order"), TemplateBody: aws.String(waveTemplate),
		})
		require.NoError(t, err)

		if async {
			e.drain(t)
		}

		require.NoError(t, e.deleteStack(t, "order"))

		if async {
			e.drain(t)
		}

		return signature(t, e, "order")
	}

	assert.Equal(t, run(t, false), run(t, true))
}

func TestAsyncWalk_CloseAndReset(t *testing.T) {
	t.Parallel()

	cases := []struct {
		stop       func(ctx context.Context, b *cloudformation.InMemoryBackend)
		name       string
		wantStatus string
		wantGone   bool
	}{
		{
			name: "backend_close", wantStatus: "CREATE_IN_PROGRESS",
			stop: func(_ context.Context, b *cloudformation.InMemoryBackend) { b.Close() },
		},
		{
			name: "backend_reset", wantGone: true,
			stop: func(_ context.Context, b *cloudformation.InMemoryBackend) { b.Reset() },
		},
		{
			name: "handler_shutdown", wantStatus: "CREATE_IN_PROGRESS",
			stop: func(ctx context.Context, b *cloudformation.InMemoryBackend) {
				cloudformation.NewHandler(b).Shutdown(ctx)
			},
		},
		{
			name: "handler_reset", wantGone: true,
			stop: func(_ context.Context, b *cloudformation.InMemoryBackend) { cloudformation.NewHandler(b).Reset() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, client := newTestHandlerAndClientWithBackend(t)
				b.SetProvisioningDelay(time.Second)

				_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
					StackName: aws.String("stopped"), TemplateBody: aws.String(depChain),
				})
				require.NoError(t, err)

				tc.stop(t.Context(), b)
				b.WaitForStackProvisioning()

				time.Sleep(time.Minute)
				synctest.Wait()

				out, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
					StackName: aws.String("stopped"),
				})

				if tc.wantGone {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, out.Stacks, 1)
				assert.Equal(t, tc.wantStatus, string(out.Stacks[0].StackStatus), "no step ran after the stop")
			})
		})
	}
}

func TestAsyncWalk_ResetDropsQueuedSteps(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("queued"), TemplateBody: aws.String(depChain),
	})
	require.NoError(t, err)

	e.backend.Reset()
	assert.Equal(t, 1, e.drain(t), "the queued step still runs and finds nothing to do")

	_, err = e.client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("queued")})
	require.Error(t, err)
}

func TestAsyncWalk_SnapshotMidFlight(t *testing.T) {
	t.Parallel()

	cases := []struct {
		start      func(t *testing.T, e *transitionEnv) string
		finish     func(t *testing.T, e *transitionEnv, id string)
		name       string
		wantMid    string
		wantStatus string
		wantRes    []string
		steps      int
	}{
		{
			name: "create",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				e.schedule(t)

				out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
					StackName: aws.String("snap"), TemplateBody: aws.String(waveTemplate),
				})
				require.NoError(t, err)
				e.sched.runNext(t)

				return aws.ToString(out.StackId)
			},
			wantMid: "CREATE_IN_PROGRESS", wantStatus: "CREATE_COMPLETE",
			wantRes: []string{"A", "B", "C", "D"}, steps: 2,
		},
		{
			name: "update",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				id := e.createSync(t, "snap", cancelFrom)
				e.schedule(t)

				_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
					StackName: aws.String("snap"), TemplateBody: aws.String(cancelTo),
				})
				require.NoError(t, err)
				e.sched.runNext(t)

				return id
			},
			wantMid: "UPDATE_IN_PROGRESS", wantStatus: "UPDATE_COMPLETE",
			wantRes: []string{"A", "B", "C"}, steps: 2,
		},
		{
			name: "delete",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				id := e.createSync(t, "snap", retainTpl)
				e.schedule(t)
				require.NoError(t, e.deleteStack(t, "snap"))
				e.sched.runNext(t)

				return id
			},
			wantMid: "DELETE_IN_PROGRESS", wantStatus: "DELETE_COMPLETE", steps: 1,
		},
		{
			name: "rollback_failed",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				e.failQueues(func() { e.fillBucket(t) })
				e.createSync(t, "snap", cancelFrom)
				e.schedule(t)

				out, err := e.client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
					StackName: aws.String("snap"),
				})
				require.NoError(t, err)

				_, err = e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
					StackName: aws.String("snap"), TemplateBody: aws.String(rollbackFailTpl),
				})
				require.NoError(t, err)
				e.drain(t)

				return aws.ToString(out.Stacks[0].StackId)
			},
			finish: func(t *testing.T, e *transitionEnv, id string) {
				t.Helper()

				_, err := e.client.ContinueUpdateRollback(t.Context(), &cfnsdk.ContinueUpdateRollbackInput{
					StackName: aws.String(id), ForceRollback: aws.Bool(true),
				})
				require.NoError(t, err)
			},
			wantMid: "UPDATE_ROLLBACK_FAILED", wantStatus: "UPDATE_ROLLBACK_COMPLETE",
			wantRes: []string{"A", "Bucket"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newTransitionEnv(t)
			id := tc.start(t, src)
			assert.Equal(t, tc.wantMid, src.status(t, id))

			snap := src.backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			inline := newTransitionEnv(t)
			require.NoError(t, inline.backend.Restore(t.Context(), snap))

			if tc.finish == nil {
				assert.Equal(t, tc.wantStatus, inline.status(t, id), "restore without a scheduler settles it")
				assert.ElementsMatch(t, tc.wantRes, inline.resourceIDsIfLive(t, id))
			} else {
				assert.Equal(t, tc.wantMid, inline.status(t, id))
				tc.finish(t, inline, id)
				assert.Equal(t, tc.wantStatus, inline.status(t, id))
				assert.ElementsMatch(t, tc.wantRes, inline.resourceIDs(t, id))
			}

			if tc.steps == 0 {
				return
			}

			stepped := newTransitionEnv(t)
			stepped.schedule(t)
			require.NoError(t, stepped.backend.Restore(t.Context(), snap))
			assert.Equal(t, tc.wantMid, stepped.status(t, id), "restore with a scheduler resumes step by step")
			assert.Equal(t, tc.steps, stepped.drain(t))
			assert.Equal(t, tc.wantStatus, stepped.status(t, id))
		})
	}
}

func (e *transitionEnv) resourceIDsIfLive(t *testing.T, stack string) []string {
	t.Helper()

	out, err := e.client.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String(stack),
	})
	if err != nil {
		return nil
	}

	ids := make([]string, 0, len(out.StackResources))
	for _, r := range out.StackResources {
		ids = append(ids, aws.ToString(r.LogicalResourceId))
	}

	slices.Sort(ids)

	return ids
}

const (
	queueV1 = `{"Resources":{"Q1":{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":"1"}}}}`
	queueV2 = `{"Resources":{"Q1":{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":"5"}},` +
		`"Q2":{"Type":"AWS::SQS::Queue","DependsOn":"Q1"}}}`
)

func (e *transitionEnv) queueDelay(t *testing.T, queueURL string) (string, error) {
	t.Helper()

	out, err := e.backends.SQS.Backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
		QueueURL: queueURL, Region: "us-east-1", AttributeNames: []string{"DelaySeconds"},
	})
	if err != nil {
		return "", err
	}

	return out.Attributes["DelaySeconds"], nil
}

func TestAsyncWalk_RollbackRevertsModifiedResource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		wantStatus string
		wantQ1     string
		wantTrail  []string
		deleteQ    bool
	}{
		{
			name:       "restored",
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
			wantQ1:     "CREATE_COMPLETE",
			wantTrail:  []string{"UPDATE_IN_PROGRESS", "UPDATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_COMPLETE"},
		},
		{
			name:       "revert_fails",
			deleteQ:    true,
			wantStatus: "UPDATE_ROLLBACK_FAILED",
			wantQ1:     "UPDATE_FAILED",
			wantTrail:  []string{"UPDATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			id := e.createSync(t, "revert", queueV1)

			res, err := e.client.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
				StackName: aws.String(id), LogicalResourceId: aws.String("Q1"),
			})
			require.NoError(t, err)

			url := aws.ToString(res.StackResourceDetail.PhysicalResourceId)

			e.failQueues(func() {
				if !tc.deleteQ {
					return
				}

				assert.NoError(t, e.backends.SQS.Backend.DeleteQueue(&sqsbackend.DeleteQueueInput{
					QueueURL: url, Region: "us-east-1",
				}))
			})
			e.schedule(t)

			_, err = e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("revert"), TemplateBody: aws.String(queueV2),
			})
			require.NoError(t, err)

			e.drain(t)

			assert.Equal(t, tc.wantStatus, e.status(t, id))
			assert.Equal(t, tc.wantQ1, e.resourceStatuses(t, id)["Q1"])
			assert.NotContains(t, e.resourceStatuses(t, id), "Q2")

			trail := e.statuses(t, id, "Q1")
			assert.Equal(t, tc.wantTrail, trail[len(trail)-len(tc.wantTrail):])

			if tc.deleteQ {
				return
			}

			delay, delayErr := e.queueDelay(t, url)
			require.NoError(t, delayErr)
			assert.Equal(t, "1", delay, "the in-place update is reverted")
		})
	}
}
