package cloudformation_test

import (
	jsonv2 "encoding/json/v2"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	siblingTemplate = `{"Resources":{"A":{"Type":"AWS::SNS::Topic"},` +
		`"Q":{"Type":"AWS::SQS::Queue"},"Z":{"Type":"AWS::SNS::Topic"}}}`
	siblingBase   = `{"Resources":{"Base":{"Type":"AWS::SNS::Topic"}}}`
	siblingUpdate = `{"Resources":{"Base":{"Type":"AWS::SNS::Topic"},"A":{"Type":"AWS::SNS::Topic"},` +
		`"Q":{"Type":"AWS::SQS::Queue"},"Z":{"Type":"AWS::SNS::Topic"}}}`

	reasonCreateCancelledText = "Resource creation cancelled"
)

func countTopics(e *transitionEnv) *atomic.Int32 {
	var topics atomic.Int32

	e.creator.InjectCreateHook(func(resourceType string) error {
		switch resourceType {
		case "AWS::SNS::Topic":
			topics.Add(1)
		case "AWS::SQS::Queue":
			return errSimulatedCreate
		}

		return nil
	})

	return &topics
}

func TestWalk_FailedWaveCancelsUnstartedSiblings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		final  string
		async  bool
		update bool
	}{
		{name: "create sync", final: "ROLLBACK_COMPLETE"},
		{name: "create async", async: true, final: "ROLLBACK_COMPLETE"},
		{name: "update sync", update: true, final: "UPDATE_ROLLBACK_COMPLETE"},
		{name: "update async", update: true, async: true, final: "UPDATE_ROLLBACK_COMPLETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)

			var id string

			if tt.update {
				id = e.createSync(t, "sib", siblingBase)
			}

			topics := countTopics(e)

			if tt.async {
				e.schedule(t)
			}

			if tt.update {
				_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
					StackName: aws.String("sib"), TemplateBody: aws.String(siblingUpdate),
				})
				require.NoError(t, err)
			} else {
				id = e.createSync(t, "sib", siblingTemplate)
			}

			if tt.async {
				e.drain(t)
			}

			assert.Equal(t, tt.final, e.status(t, id))
			assert.Equal(t, int32(1), topics.Load(), "the sibling after the failed resource is never created")

			z := e.events(t, id, "Z")
			require.NotEmpty(t, z)
			assert.Equal(t, "CREATE_FAILED|"+reasonCreateCancelledText, z[len(z)-1])
			assert.NotContains(t, e.statuses(t, id, "Z"), "CREATE_COMPLETE")

			q := e.events(t, id, "Q")
			assert.Contains(t, q[len(q)-1], "CREATE_FAILED|"+errSimulatedCreate.Error())
			assert.Contains(t, e.statuses(t, id, "A"), "DELETE_COMPLETE", "completed siblings are rolled back")
		})
	}
}

func TestWalk_DeleteDuringUpdateRemovesReplacedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps int
	}{
		{name: "replacement applied", steps: 1},
		{name: "mid cleanup", steps: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTransitionEnv(t)
			id := e.createSync(t, "repl", replaceV1)
			e.schedule(t)

			var buckets atomic.Int32

			e.creator.InjectDeleteHook(func(resourceType string) {
				if resourceType == "AWS::S3::Bucket" {
					buckets.Add(1)
				}
			})

			_, err := e.client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("repl"), TemplateBody: aws.String(replaceV2),
			})
			require.NoError(t, err)

			for range tt.steps {
				e.sched.runNext(t)
			}

			require.Equal(t, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", e.status(t, id))

			cleaned := buckets.Load()

			require.NoError(t, e.deleteStack(t, "repl"))
			e.drain(t)

			assert.Equal(t, "DELETE_COMPLETE", e.status(t, id))
			assert.Equal(t, int32(2), buckets.Load(), "old and new buckets are both deleted (%d before)", cleaned)
		})
	}
}

// legacyOps rewrites the in-flight operations of a snapshot into the shape written before
// wave tracking: no remaining work, template or in-flight set, only "announced" keys.
func legacyOps(t *testing.T, snap []byte) []byte {
	t.Helper()

	var doc map[string]any

	require.NoError(t, jsonv2.Unmarshal(snap, &doc))

	ops, ok := doc["pendingStackOps"].(map[string]any)
	require.True(t, ok, "snapshot carries an in-flight operation")

	for _, v := range ops {
		op, isMap := v.(map[string]any)
		require.True(t, isMap)

		if inFlight, has := op["inFlight"].(map[string]any); has {
			keys := make([]any, 0, len(inFlight))

			for id, status := range inFlight {
				keys = append(keys, id+"|"+status.(string))
			}

			op["announced"] = keys
		}

		for _, k := range []string{"inFlight", "remaining", "templateBody", "physicalIds", "done"} {
			delete(op, k)
		}
	}

	out, err := jsonv2.Marshal(doc)
	require.NoError(t, err)

	return out
}

func TestWalk_RestoresSnapshotWrittenBeforeWaveTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start      func(t *testing.T, e *transitionEnv) string
		name       string
		wantStatus string
		wantTopics int32
	}{
		{
			name: "delete",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				id := e.createSync(t, "old-delete", waveTemplate)
				e.schedule(t)
				require.NoError(t, e.deleteStack(t, "old-delete"))

				return id
			},
			wantStatus: "DELETE_COMPLETE",
			wantTopics: 3,
		},
		{
			name: "create rollback",
			start: func(t *testing.T, e *transitionEnv) string {
				t.Helper()

				e.failQueues(nil)
				e.schedule(t)

				out, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
					StackName: aws.String("old-rollback"), TemplateBody: aws.String(middleFailTemplate),
				})
				require.NoError(t, err)

				for range 3 {
					e.sched.runNext(t)
				}

				return aws.ToString(out.StackId)
			},
			wantStatus: "ROLLBACK_COMPLETE",
			wantTopics: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newTransitionEnv(t)
			id := tt.start(t, src)
			require.Contains(t, src.status(t, id), "_IN_PROGRESS")

			snap := legacyOps(t, src.backend.Snapshot(t.Context()))
			src.backend.SetProvisioningScheduler(nil)

			var topics atomic.Int32

			src.creator.InjectDeleteHook(func(resourceType string) {
				if resourceType == "AWS::SNS::Topic" {
					topics.Add(1)
				}
			})

			require.NoError(t, src.backend.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantStatus, src.status(t, id))
			assert.Equal(t, tt.wantTopics, topics.Load(), "every created resource is deleted")
		})
	}
}

func TestWalk_SnapshotCarriesNoRuntimeCache(t *testing.T) {
	t.Parallel()

	e := newTransitionEnv(t)
	e.schedule(t)

	_, err := e.client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("rt"), TemplateBody: aws.String(waveTemplate),
	})
	require.NoError(t, err)
	e.sched.runNext(t)

	var doc struct {
		Ops map[string]map[string]any `json:"pendingStackOps"`
	}

	require.NoError(t, jsonv2.Unmarshal(e.backend.Snapshot(t.Context()), &doc))
	require.Len(t, doc.Ops, 1)

	for _, op := range doc.Ops {
		for _, key := range []string{"rt", "tmpl", "uc", "params"} {
			assert.NotContains(t, op, key)
		}
	}
}
