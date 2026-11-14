package cloudformation_test

import (
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
)

const (
	topicOnly = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"}}}`
	depChain  = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SQS::Queue","DependsOn":"A"}}}`
	depChainExtra = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"B":{"Type":"AWS::SQS::Queue","DependsOn":"A"},"C":{"Type":"AWS::SNS::Topic"}}}`
	failingUpdate = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"C":{"Type":"AWS::SNS::Topic"},"B":{"Type":"AWS::SQS::Queue","DependsOn":"C"}}}`
	bucketName     = "transition-bucket"
	bucketTemplate = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"` + bucketName + `"}}}}`
	bucketThenFail = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"` + bucketName +
		`"}},"Q":{"Type":"AWS::SQS::Queue","DependsOn":"Bucket"}}}`

	initiated = "CREATE_IN_PROGRESS|Resource creation Initiated"
)

type transitionEnv struct {
	backend  *cloudformation.InMemoryBackend
	client   *cfnsdk.Client
	creator  *cloudformation.ResourceCreator
	backends *cloudformation.ServiceBackends
	sched    *manualScheduler
}

func newTransitionEnv(t *testing.T) *transitionEnv {
	t.Helper()

	backends := newServiceBackends(t)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	t.Cleanup(backend.WaitForStackSetOperations)

	return &transitionEnv{
		backend:  backend,
		client:   newTestClientForBackend(t, backend),
		creator:  creator,
		backends: backends,
	}
}

// schedule turns async provisioning on; stacks made before it were created synchronously.
func (e *transitionEnv) schedule(t *testing.T) {
	t.Helper()

	e.sched = newManualScheduler(e.backend)

	t.Cleanup(e.backend.WaitForStackProvisioning)
	t.Cleanup(func() {
		for len(e.sched.queue) > 0 {
			e.sched.runNext(t)
		}
	})
}

// failQueues makes every SQS queue creation fail, running before (when set) each attempt.
func (e *transitionEnv) failQueues(before func()) {
	e.creator.InjectCreateHook(func(resourceType string) error {
		if resourceType != "AWS::SQS::Queue" {
			return nil
		}

		if before != nil {
			before()
		}

		return errSimulatedCreate
	})
}

func (e *transitionEnv) createSync(t *testing.T, name, template string) string {
	t.Helper()

	out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	return aws.ToString(out.StackId)
}

func (e *transitionEnv) status(t *testing.T, nameOrID string) string {
	t.Helper()

	return stackStatus(t, e.client, nameOrID)
}

func (e *transitionEnv) allEvents(t *testing.T, stack string) []cfntypes.StackEvent {
	t.Helper()

	out, err := e.client.DescribeStackEvents(t.Context(), &cfnsdk.DescribeStackEventsInput{
		StackName: aws.String(stack),
	})
	require.NoError(t, err)

	slices.Reverse(out.StackEvents)

	return out.StackEvents
}

// events returns "STATUS" or "STATUS|reason" per event of logicalID, oldest first.
func (e *transitionEnv) events(t *testing.T, stack, logicalID string) []string {
	t.Helper()

	var got []string

	for _, ev := range e.allEvents(t, stack) {
		if aws.ToString(ev.LogicalResourceId) != logicalID {
			continue
		}

		entry := string(ev.ResourceStatus)
		if reason := aws.ToString(ev.ResourceStatusReason); reason != "" {
			entry += "|" + reason
		}

		got = append(got, entry)
	}

	return got
}

// statuses is events without the reasons.
func (e *transitionEnv) statuses(t *testing.T, stack, logicalID string) []string {
	t.Helper()

	got := e.events(t, stack, logicalID)
	for i, entry := range got {
		got[i], _, _ = strings.Cut(entry, "|")
	}

	return got
}

// stackStatuses returns the stack-level event statuses, oldest first.
func (e *transitionEnv) stackStatuses(t *testing.T, stack string) []string {
	t.Helper()

	var got []string

	for _, ev := range e.allEvents(t, stack) {
		if aws.ToString(ev.ResourceType) == "AWS::CloudFormation::Stack" {
			got = append(got, string(ev.ResourceStatus))
		}
	}

	return got
}

// order returns the chronological index of the first event of logicalID with status, or -1.
func (e *transitionEnv) order(t *testing.T, stack, logicalID, status string) int {
	t.Helper()

	for i, ev := range e.allEvents(t, stack) {
		if aws.ToString(ev.LogicalResourceId) == logicalID && string(ev.ResourceStatus) == status {
			return i
		}
	}

	return -1
}

func (e *transitionEnv) deleteStack(t *testing.T, name string, in ...func(*cfnsdk.DeleteStackInput)) error {
	t.Helper()

	input := &cfnsdk.DeleteStackInput{StackName: aws.String(name)}
	for _, f := range in {
		f(input)
	}

	_, err := e.client.DeleteStack(t.Context(), input)

	return err
}

func (e *transitionEnv) resourceIDs(t *testing.T, stack string) []string {
	t.Helper()

	out, err := e.client.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String(stack),
	})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.StackResources))
	for _, r := range out.StackResources {
		ids = append(ids, aws.ToString(r.LogicalResourceId))
	}

	slices.Sort(ids)

	return ids
}

func TestAsyncCreate_PerResourceEvents(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("per-resource"), TemplateBody: aws.String(depChain),
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"CREATE_IN_PROGRESS"}, e.events(t, "per-resource", "A"), "root announced in the window")
	assert.Empty(t, e.events(t, "per-resource", "B"), "dependent waits for its dependency")

	e.sched.runNext(t)

	want := []string{"CREATE_IN_PROGRESS", initiated, "CREATE_COMPLETE"}
	assert.Equal(t, want, e.events(t, "per-resource", "A"))
	assert.Equal(t, want, e.events(t, "per-resource", "B"))
	assert.Less(t,
		e.order(t, "per-resource", "A", "CREATE_COMPLETE"), e.order(t, "per-resource", "B", "CREATE_IN_PROGRESS"))
	assert.Equal(t, "CREATE_COMPLETE", e.status(t, "per-resource"))
}

func TestAsyncUpdate_PerResourceEvents(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.createSync(t, "upd-events", depChain)
	e.schedule(t)

	_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("upd-events"), TemplateBody: aws.String(depChainExtra),
	})
	require.NoError(t, err)

	assert.Equal(t, "UPDATE_IN_PROGRESS", e.status(t, "upd-events"))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS"}, e.events(t, "upd-events", "C"), "added root announced")

	e.sched.runNext(t)
	assert.Equal(t, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", e.status(t, "upd-events"))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", initiated, "CREATE_COMPLETE"}, e.events(t, "upd-events", "C"))

	e.sched.runNext(t)
	assert.Equal(t, "UPDATE_COMPLETE", e.status(t, "upd-events"))
}

func TestAsyncDelete_Sequence(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	id := e.createSync(t, "del-seq", depChain)
	e.schedule(t)

	require.NoError(t, e.deleteStack(t, "del-seq"))

	assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, "del-seq"))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", initiated, "CREATE_COMPLETE", "DELETE_IN_PROGRESS"},
		e.events(t, "del-seq", "B"), "the resource nothing depends on is announced in the window")
	assert.NotContains(t, e.statuses(t, "del-seq", "A"), "DELETE_IN_PROGRESS")

	require.NoError(t, e.deleteStack(t, "del-seq"), "deleting a stack that is being deleted is a no-op")
	assert.Len(t, e.sched.queue, 1)

	e.sched.runNext(t)

	assert.Equal(t, "DELETE_COMPLETE", e.status(t, id))
	assert.Less(t, e.order(t, id, "B", "DELETE_COMPLETE"), e.order(t, id, "A", "DELETE_IN_PROGRESS"),
		"dependents are deleted before what they depend on")
	assert.Equal(t,
		[]string{"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "DELETE_IN_PROGRESS", "DELETE_COMPLETE"},
		e.stackStatuses(t, id))

	_, err := e.client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("del-seq")})
	require.ErrorContains(t, err, "does not exist", "a deleted stack is not describable by name")

	list, err := e.client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Stacks, "DescribeStacks without a name skips deleted stacks")
}

func TestAsyncDelete_FailedRetryRetainForce(t *testing.T) {
	t.Parallel()

	cases := []struct {
		retry      func(in *cfnsdk.DeleteStackInput)
		name       string
		wantStatus string
		wantTrail  []string
	}{
		{
			name:       "retain_resources",
			retry:      func(in *cfnsdk.DeleteStackInput) { in.RetainResources = []string{"Bucket"} },
			wantStatus: "DELETE_COMPLETE",
			wantTrail:  []string{"DELETE_IN_PROGRESS", "DELETE_FAILED", "DELETE_IN_PROGRESS", "DELETE_SKIPPED"},
		},
		{
			name:       "force_delete",
			retry:      func(in *cfnsdk.DeleteStackInput) { in.DeletionMode = cfntypes.DeletionModeForceDeleteStack },
			wantStatus: "DELETE_COMPLETE",
			wantTrail:  []string{"DELETE_IN_PROGRESS", "DELETE_FAILED", "DELETE_IN_PROGRESS", "DELETE_FAILED"},
		},
		{
			name:       "plain_retry_fails_again",
			retry:      func(*cfnsdk.DeleteStackInput) {},
			wantStatus: "DELETE_FAILED",
			wantTrail:  []string{"DELETE_IN_PROGRESS", "DELETE_FAILED", "DELETE_IN_PROGRESS", "DELETE_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			id := e.createSync(t, "bucket-stack", bucketTemplate)
			_, err := e.backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String(bucketName), Key: aws.String("k"), Body: strings.NewReader("x"),
			})
			require.NoError(t, err)

			e.schedule(t)

			require.NoError(t, e.deleteStack(t, "bucket-stack"))
			assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, "bucket-stack"))
			e.sched.runNext(t)
			assert.Equal(t, "DELETE_FAILED", e.status(t, "bucket-stack"))
			assert.Equal(t, []string{"Bucket"}, e.resourceIDs(t, "bucket-stack"), "the failed resource stays")

			require.NoError(t, e.deleteStack(t, "bucket-stack", tc.retry))
			assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, id))
			e.sched.runNext(t)

			assert.Equal(t, tc.wantStatus, e.status(t, id))

			trail := e.statuses(t, id, "Bucket")
			assert.Equal(t, tc.wantTrail, trail[len(trail)-len(tc.wantTrail):])
		})
	}
}

func TestAsyncDelete_RetainOnlyForFailedStacks(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.createSync(t, "healthy", topicOnly)
	e.schedule(t)

	err := e.deleteStack(t, "healthy", func(in *cfnsdk.DeleteStackInput) { in.RetainResources = []string{"A"} })
	require.ErrorContains(t, err, "DELETE_FAILED")
	assert.Equal(t, "CREATE_COMPLETE", e.status(t, "healthy"))

	err = e.deleteStack(t, "healthy", func(in *cfnsdk.DeleteStackInput) {
		in.DeletionMode = cfntypes.DeletionMode("BOGUS")
	})
	require.ErrorContains(t, err, "deletionMode")
}

func TestAsyncCreate_Rollback(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input      func(in *cfnsdk.CreateStackInput)
		name       string
		wantErr    string
		wantStatus []string
		wantStack  []string
		wantRes    []string
		wantDRB    bool
	}{
		{
			name:       "rollback",
			input:      func(*cfnsdk.CreateStackInput) {},
			wantStatus: []string{"ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE"},
			wantStack:  []string{"CREATE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE"},
		},
		{
			name:       "on_failure_rollback",
			input:      func(in *cfnsdk.CreateStackInput) { in.OnFailure = cfntypes.OnFailureRollback },
			wantStatus: []string{"ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE"},
			wantStack:  []string{"CREATE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE"},
		},
		{
			name:       "disable_rollback",
			input:      func(in *cfnsdk.CreateStackInput) { in.DisableRollback = aws.Bool(true) },
			wantStatus: []string{"CREATE_FAILED"},
			wantStack:  []string{"CREATE_IN_PROGRESS"},
			wantRes:    []string{"A"},
			wantDRB:    true,
		},
		{
			name:       "on_failure_do_nothing",
			input:      func(in *cfnsdk.CreateStackInput) { in.OnFailure = cfntypes.OnFailureDoNothing },
			wantStatus: []string{"CREATE_FAILED"},
			wantStack:  []string{"CREATE_IN_PROGRESS"},
			wantRes:    []string{"A"},
			wantDRB:    true,
		},
		{
			name:       "on_failure_delete",
			input:      func(in *cfnsdk.CreateStackInput) { in.OnFailure = cfntypes.OnFailureDelete },
			wantStatus: []string{"ROLLBACK_IN_PROGRESS", "DELETE_IN_PROGRESS", "DELETE_COMPLETE"},
			wantStack: []string{
				"CREATE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE",
				"DELETE_IN_PROGRESS", "DELETE_COMPLETE",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			e.failQueues(nil)
			e.schedule(t)

			in := &cfnsdk.CreateStackInput{StackName: aws.String("rb"), TemplateBody: aws.String(depChain)}
			tc.input(in)

			out, err := e.client.CreateStack(t.Context(), in)
			require.NoError(t, err)

			id := aws.ToString(out.StackId)
			assert.Equal(t, "CREATE_IN_PROGRESS", e.status(t, id))

			for i, want := range tc.wantStatus {
				e.sched.runNext(t)
				assert.Equal(t, want, e.status(t, id), "after step %d", i+1)

				if i == 0 && want == "ROLLBACK_IN_PROGRESS" {
					assert.Equal(t, []string{"A"}, e.resourceIDs(t, id), "rollback has not deleted anything yet")
					assert.Contains(t, e.events(t, id, "B")[len(e.events(t, id, "B"))-1], "CREATE_FAILED|")
				}
			}

			assert.Empty(t, e.sched.queue)
			assert.Equal(t, tc.wantStack, e.stackStatuses(t, id))

			if tc.wantRes != nil {
				assert.Equal(t, tc.wantRes, e.resourceIDs(t, id))
				assert.Equal(t, tc.wantDRB, aws.ToBool(describeOne(t, e.client, id).DisableRollback))
			}

			if strings.Contains(tc.name, "rollback") && tc.wantRes == nil && tc.name != "on_failure_delete" {
				assert.Empty(t, e.resourceIDs(t, id), "rollback deleted the topic")
				assert.Equal(
					t,
					[]string{
						"CREATE_IN_PROGRESS",
						initiated,
						"CREATE_COMPLETE",
						"DELETE_IN_PROGRESS",
						"DELETE_COMPLETE",
					},
					e.events(t, id, "A"),
				)
			}
		})
	}
}

func describeOne(t *testing.T, client *cfnsdk.Client, nameOrID string) cfntypes.Stack {
	t.Helper()

	out, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(nameOrID)})
	require.NoError(t, err)
	require.Len(t, out.Stacks, 1)

	return out.Stacks[0]
}

func TestAsyncCreate_OnFailureValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input   func(in *cfnsdk.CreateStackInput)
		name    string
		wantErr string
	}{
		{
			name: "both_set",
			input: func(in *cfnsdk.CreateStackInput) {
				in.DisableRollback = aws.Bool(true)
				in.OnFailure = cfntypes.OnFailureDelete
			},
			wantErr: "either DisableRollback or OnFailure",
		},
		{
			name:    "bad_enum",
			input:   func(in *cfnsdk.CreateStackInput) { in.OnFailure = cfntypes.OnFailure("EXPLODE") },
			wantErr: "onFailure",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			in := &cfnsdk.CreateStackInput{StackName: aws.String("bad-opts"), TemplateBody: aws.String(topicOnly)}
			tc.input(in)

			_, err := e.client.CreateStack(t.Context(), in)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestAsyncCreate_RollbackFailed(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.failQueues(func() {
		_, err := e.backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String("k"), Body: strings.NewReader("x"),
		})
		assert.NoError(t, err)
	})
	e.schedule(t)

	out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("rb-failed"), TemplateBody: aws.String(bucketThenFail),
	})
	require.NoError(t, err)

	id := aws.ToString(out.StackId)

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_IN_PROGRESS", e.status(t, id))

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_FAILED", e.status(t, id))
	assert.Equal(t, []string{"Bucket"}, e.resourceIDs(t, id))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS", "ROLLBACK_FAILED"}, e.stackStatuses(t, id))
}

func TestAsyncUpdate_Rollback(t *testing.T) {
	t.Parallel()

	cases := []struct {
		trigger    func(t *testing.T, e *transitionEnv)
		name       string
		wantStatus []string
		wantStack  []string
		wantRes    []string
	}{
		{
			name: "failed_update",
			trigger: func(t *testing.T, e *transitionEnv) {
				t.Helper()

				_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
					StackName: aws.String("upd-rb"), TemplateBody: aws.String(failingUpdate),
				})
				require.NoError(t, err)
				assert.Equal(t, "UPDATE_IN_PROGRESS", e.status(t, "upd-rb"))
			},
			wantStatus: []string{
				"UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
				"UPDATE_ROLLBACK_COMPLETE",
			},
			wantStack: []string{
				"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS",
				"UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
			},
			wantRes: []string{"A"},
		},
		{
			name: "cancel_update",
			trigger: func(t *testing.T, e *transitionEnv) {
				t.Helper()

				_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
					StackName: aws.String("upd-rb"), TemplateBody: aws.String(depChainExtra),
				})
				require.NoError(t, err)

				_, err = e.client.CancelUpdateStack(t.Context(), &cfnsdk.CancelUpdateStackInput{
					StackName: aws.String("upd-rb"),
				})
				require.NoError(t, err)
				assert.Equal(t, "UPDATE_ROLLBACK_IN_PROGRESS", e.status(t, "upd-rb"))

				tmpl, err := e.client.GetTemplate(
					t.Context(),
					&cfnsdk.GetTemplateInput{StackName: aws.String("upd-rb")},
				)
				require.NoError(t, err)
				assert.JSONEq(t, topicOnly, aws.ToString(tmpl.TemplateBody), "the old template is back")
			},
			wantStatus: []string{"UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE"},
			wantStack: []string{
				"CREATE_IN_PROGRESS", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS",
				"UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE",
			},
			wantRes: []string{"A"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			e.createSync(t, "upd-rb", topicOnly)
			e.failQueues(nil)
			e.schedule(t)

			tc.trigger(t, e)

			if tc.name == "failed_update" {
				e.sched.runNext(t)
				assert.Equal(t, tc.wantStatus[0], e.status(t, "upd-rb"))
				assert.Contains(t, e.resourceIDs(t, "upd-rb"), "C", "rollback has not deleted the new topic yet")

				for i, want := range tc.wantStatus[1:] {
					e.sched.runNext(t)
					assert.Equal(t, want, e.status(t, "upd-rb"), "after rollback step %d", i+1)
				}
			} else {
				for i, want := range tc.wantStatus {
					e.sched.runNext(t)
					assert.Equal(t, want, e.status(t, "upd-rb"), "after rollback step %d", i+1)
				}
			}

			assert.Equal(t, tc.wantRes, e.resourceIDs(t, "upd-rb"))
			assert.Equal(t, tc.wantStack, e.stackStatuses(t, "upd-rb"))
		})
	}
}

func TestAsyncExecuteChangeSet_Sequence(t *testing.T) {
	t.Parallel()

	executionStatus := func(t *testing.T, e *transitionEnv, stack, cs string) cfntypes.ExecutionStatus {
		t.Helper()

		out, err := e.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
			StackName: aws.String(stack), ChangeSetName: aws.String(cs),
		})
		require.NoError(t, err)

		return out.ExecutionStatus
	}

	t.Run("create", func(t *testing.T) {
		t.Parallel()

		e := newTransitionEnv(t)
		e.schedule(t)

		_, err := e.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
			StackName: aws.String("cs-create"), ChangeSetName: aws.String("first"),
			TemplateBody: aws.String(topicOnly), ChangeSetType: cfntypes.ChangeSetTypeCreate,
		})
		require.NoError(t, err)
		_, err = e.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
			StackName: aws.String("cs-create"), ChangeSetName: aws.String("sibling"),
			TemplateBody: aws.String(depChain), ChangeSetType: cfntypes.ChangeSetTypeCreate,
		})
		require.NoError(t, err)

		require.NoError(t, cfnsdk.NewChangeSetCreateCompleteWaiter(e.client).Wait(
			t.Context(),
			&cfnsdk.DescribeChangeSetInput{StackName: aws.String("cs-create"), ChangeSetName: aws.String("first")},
			time.Minute, func(o *cfnsdk.ChangeSetCreateCompleteWaiterOptions) { o.MinDelay = time.Millisecond },
		))
		assert.Equal(t, cfntypes.ExecutionStatusAvailable, executionStatus(t, e, "cs-create", "first"))

		_, err = e.client.ExecuteChangeSet(t.Context(), &cfnsdk.ExecuteChangeSetInput{
			StackName: aws.String("cs-create"), ChangeSetName: aws.String("first"),
		})
		require.NoError(t, err)

		assert.Equal(t, cfntypes.ExecutionStatusExecuteInProgress, executionStatus(t, e, "cs-create", "first"))
		assert.Equal(t, "CREATE_IN_PROGRESS", e.status(t, "cs-create"))

		e.sched.runNext(t)

		assert.Equal(t, cfntypes.ExecutionStatusExecuteComplete, executionStatus(t, e, "cs-create", "first"))
		assert.Equal(t, "CREATE_COMPLETE", e.status(t, "cs-create"))

		_, err = e.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
			StackName: aws.String("cs-create"), ChangeSetName: aws.String("sibling"),
		})
		require.ErrorContains(t, err, "does not exist", "sibling change sets are discarded")
	})

	t.Run("update", func(t *testing.T) {
		t.Parallel()

		e := newTransitionEnv(t)
		e.createSync(t, "cs-update", topicOnly)
		e.schedule(t)

		_, err := e.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
			StackName: aws.String("cs-update"), ChangeSetName: aws.String("grow"),
			TemplateBody: aws.String(depChain),
		})
		require.NoError(t, err)

		_, err = e.client.ExecuteChangeSet(t.Context(), &cfnsdk.ExecuteChangeSetInput{
			StackName: aws.String("cs-update"), ChangeSetName: aws.String("grow"),
		})
		require.NoError(t, err)
		assert.Equal(t, cfntypes.ExecutionStatusExecuteInProgress, executionStatus(t, e, "cs-update", "grow"))
		assert.Equal(t, "UPDATE_IN_PROGRESS", e.status(t, "cs-update"))

		e.sched.runNext(t)
		assert.Equal(t, cfntypes.ExecutionStatusExecuteInProgress, executionStatus(t, e, "cs-update", "grow"))
		assert.Equal(t, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", e.status(t, "cs-update"))

		e.sched.runNext(t)
		assert.Equal(t, cfntypes.ExecutionStatusExecuteComplete, executionStatus(t, e, "cs-update", "grow"))
		assert.Equal(t, "UPDATE_COMPLETE", e.status(t, "cs-update"))
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		e := newTransitionEnv(t)
		e.failQueues(nil)
		e.schedule(t)

		_, err := e.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
			StackName: aws.String("cs-fail"), ChangeSetName: aws.String("boom"),
			TemplateBody: aws.String(depChain), ChangeSetType: cfntypes.ChangeSetTypeCreate,
		})
		require.NoError(t, err)

		_, err = e.client.ExecuteChangeSet(t.Context(), &cfnsdk.ExecuteChangeSetInput{
			StackName: aws.String("cs-fail"), ChangeSetName: aws.String("boom"),
		})
		require.NoError(t, err)

		e.sched.runNext(t)
		assert.Equal(t, cfntypes.ExecutionStatusExecuteInProgress, executionStatus(t, e, "cs-fail", "boom"),
			"still rolling back")
		assert.Equal(t, "ROLLBACK_IN_PROGRESS", e.status(t, "cs-fail"))

		e.sched.runNext(t)
		assert.Equal(t, cfntypes.ExecutionStatusExecuteFailed, executionStatus(t, e, "cs-fail", "boom"))
		assert.Equal(t, "ROLLBACK_COMPLETE", e.status(t, "cs-fail"))
	})
}

func nestedParent(t *testing.T, childTemplate string) string {
	t.Helper()

	return `{"Resources":{"Child":{"Type":"AWS::CloudFormation::Stack","Properties":{"TemplateBody":` +
		quoteJSON(childTemplate) + `}}}}`
}

func TestAsyncNestedStack_ParentWaitsForChild(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	parent := nestedParent(t, topicOnly)

	out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("nest-parent"), TemplateBody: aws.String(parent), DisableRollback: aws.Bool(true),
	})
	require.NoError(t, err)

	parentID := aws.ToString(out.StackId)

	e.sched.runNext(t)

	assert.Equal(t, "CREATE_IN_PROGRESS", e.status(t, parentID), "the parent waits for its child")

	children := e.childStacks(t, parentID)
	require.Len(t, children, 1)
	assert.Equal(t, cfntypes.StackStatusCreateInProgress, children[0].StackStatus)
	assert.Equal(t, parentID, aws.ToString(children[0].ParentId))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS"}, e.stackStatuses(t, aws.ToString(children[0].StackId)))

	e.sched.runNext(t)
	childID := aws.ToString(children[0].StackId)
	assert.Equal(t, "CREATE_COMPLETE", e.status(t, childID))
	assert.Equal(t, "CREATE_IN_PROGRESS", e.status(t, parentID), "the parent settles on its own step")

	e.sched.runNext(t)
	assert.Equal(t, "CREATE_COMPLETE", e.status(t, parentID))
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", "CREATE_COMPLETE"}, e.stackStatuses(t, childID))

	require.NoError(t, e.deleteStack(t, "nest-parent"))
	e.sched.runNext(t)

	assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, parentID), "the parent waits for the child delete")
	assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, childID))

	e.sched.runNext(t)
	assert.Equal(t, "DELETE_COMPLETE", e.status(t, childID))
	assert.Equal(t, "DELETE_IN_PROGRESS", e.status(t, parentID))

	e.sched.runNext(t)
	assert.Equal(t, "DELETE_COMPLETE", e.status(t, parentID))
}

func (e *transitionEnv) childStacks(t *testing.T, parentID string) []cfntypes.Stack {
	t.Helper()

	out, err := e.client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{})
	require.NoError(t, err)

	var children []cfntypes.Stack

	for _, s := range out.Stacks {
		if aws.ToString(s.ParentId) == parentID {
			children = append(children, s)
		}
	}

	return children
}

func TestAsyncNestedStack_ChildFailureFailsParent(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.failQueues(nil)
	e.schedule(t)

	out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("nest-fail"),
		TemplateBody: aws.String(nestedParent(t, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`)),
	})
	require.NoError(t, err)

	parentID := aws.ToString(out.StackId)

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_IN_PROGRESS", e.status(t, parentID))

	e.sched.runNext(t)
	assert.Equal(t, "ROLLBACK_COMPLETE", e.status(t, parentID))

	child := e.events(t, parentID, "Child")
	require.NotEmpty(t, child)
	assert.Contains(t, child[len(child)-1], "was not successfully created")
}

func TestAsyncDelay_WaitersWithFakeClock(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, client := newTestHandlerAndClientWithBackend(t)
		b.SetProvisioningDelay(time.Second)
		t.Cleanup(b.WaitForStackProvisioning)

		create, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName: aws.String("fake-clock"), TemplateBody: aws.String(depChain),
		})
		require.NoError(t, err)

		fast := func(o *cfnsdk.StackCreateCompleteWaiterOptions) { o.MinDelay = 100 * time.Millisecond }
		require.NoError(t, cfnsdk.NewStackCreateCompleteWaiter(client, fast).Wait(
			t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("fake-clock")}, time.Minute,
		))

		_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("fake-clock")})
		require.NoError(t, err)
		assert.Equal(t, "DELETE_IN_PROGRESS", stackStatus(t, client, "fake-clock"))

		require.NoError(t, cfnsdk.NewStackDeleteCompleteWaiter(client,
			func(o *cfnsdk.StackDeleteCompleteWaiterOptions) { o.MinDelay = 100 * time.Millisecond },
		).Wait(t.Context(), &cfnsdk.DescribeStacksInput{StackName: create.StackId}, time.Minute))

		assert.Equal(t, "DELETE_COMPLETE", stackStatus(t, client, aws.ToString(create.StackId)))
	})
}

func TestStackSetInstances_DetailedStatusOverWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, client := newTestHandlerAndClientWithBackend(t)
		b.SetProvisioningDelay(time.Second)

		_, err := b.CreateStackSet("window-ss", "", topicOnly, cloudformation.StackSetOptions{})
		require.NoError(t, err)

		opID, err := b.CreateStackInstances(
			t.Context(), "window-ss", []string{"111111111111", "222222222222"}, nil, []string{"us-east-1"}, "",
		)
		require.NoError(t, err)

		detail := func() map[string]string {
			list, listErr := b.ListStackInstances("window-ss", 0, "", cloudformation.ListStackInstancesFilter{})
			require.NoError(t, listErr)

			got := make(map[string]string)
			for _, s := range list.Data {
				got[s.Account] = s.DetailedStatus
			}

			return got
		}

		synctest.Wait()
		assert.Equal(t, "RUNNING", opStatus(t, b, "window-ss", opID))
		assert.Equal(t, map[string]string{"111111111111": "RUNNING", "222222222222": "PENDING"}, detail())

		time.Sleep(time.Second)
		synctest.Wait()
		assert.Equal(t, "RUNNING", opStatus(t, b, "window-ss", opID))
		assert.Equal(t, map[string]string{"111111111111": "SUCCEEDED", "222222222222": "RUNNING"}, detail())

		time.Sleep(time.Second)
		synctest.Wait()
		assert.Equal(t, "SUCCEEDED", opStatus(t, b, "window-ss", opID))
		assert.Equal(t, map[string]string{"111111111111": "SUCCEEDED", "222222222222": "SUCCEEDED"}, detail())

		list, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
			StackSetName: aws.String("window-ss"),
		})
		require.NoError(t, err)
		require.Len(t, list.Summaries, 2)

		for _, s := range list.Summaries {
			require.NotNil(t, s.StackInstanceStatus)
			assert.Equal(t, cfntypes.StackInstanceDetailedStatusSucceeded, s.StackInstanceStatus.DetailedStatus)
		}

		failed, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
			StackSetName: aws.String("window-ss"),
			Filters: []cfntypes.StackInstanceFilter{
				{Name: cfntypes.StackInstanceFilterNameDetailedStatus, Values: aws.String("FAILED")},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, failed.Summaries)

		one, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
			StackSetName: aws.String("window-ss"), StackInstanceAccount: aws.String("111111111111"),
			StackInstanceRegion: aws.String("us-east-1"),
		})
		require.NoError(t, err)
		require.NotNil(t, one.StackInstance.StackInstanceStatus)
		assert.Equal(
			t,
			cfntypes.StackInstanceDetailedStatusSucceeded,
			one.StackInstance.StackInstanceStatus.DetailedStatus,
		)
	})
}

func TestAsyncTransitions_SnapshotResumes(t *testing.T) {
	t.Parallel()

	const badImport = `{"Resources":{"T":{"Type":"AWS::SNS::Topic","Properties":` +
		`{"TopicName":{"Fn::ImportValue":"missing"}}}}}`

	cases := []struct {
		start      func(t *testing.T, e *transitionEnv) string
		name       string
		wantMid    string
		wantStatus string
	}{
		{
			name: "delete",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				id := e.createSync(t, "snap-delete", depChain)
				e.schedule(t)
				require.NoError(t, e.deleteStack(t, "snap-delete"))

				return id
			},
			wantMid:    "DELETE_IN_PROGRESS",
			wantStatus: "DELETE_COMPLETE",
		},
		{
			name: "rollback",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				e.schedule(t)

				out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
					StackName: aws.String("snap-rollback"), TemplateBody: aws.String(badImport),
				})
				require.NoError(t, err)
				e.sched.runNext(t)

				return aws.ToString(out.StackId)
			},
			wantMid:    "ROLLBACK_IN_PROGRESS",
			wantStatus: "ROLLBACK_COMPLETE",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newTransitionEnv(t)
			id := tc.start(t, src)
			assert.Equal(t, tc.wantMid, src.status(t, id))

			dst, dstClient := newTestHandlerAndClientWithBackend(t)
			require.NoError(t, dst.Restore(t.Context(), src.backend.Snapshot(t.Context())))

			assert.Equal(t, tc.wantStatus, stackStatus(t, dstClient, id), "restore finishes the in-flight step")
		})
	}
}
