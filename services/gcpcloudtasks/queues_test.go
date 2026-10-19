package gcpcloudtasks_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestQueueDefaultsAndLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "defaults")

	assert.Equal(t, cloudtaskspb.Queue_RUNNING, q.GetState())
	assert.InDelta(t, 500, q.GetRateLimits().GetMaxDispatchesPerSecond(), 0)
	assert.EqualValues(t, 1000, q.GetRateLimits().GetMaxConcurrentDispatches())
	assert.EqualValues(t, 100, q.GetRetryConfig().GetMaxAttempts())
	assert.Equal(t, 100*time.Millisecond, q.GetRetryConfig().GetMinBackoff().AsDuration())
	assert.Equal(t, time.Hour, q.GetRetryConfig().GetMaxBackoff().AsDuration())
	assert.EqualValues(t, 16, q.GetRetryConfig().GetMaxDoublings())

	_, err := e.client.CreateQueue(t.Context(), &cloudtaskspb.CreateQueueRequest{
		Parent: location, Queue: &cloudtaskspb.Queue{Name: q.GetName()},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	paused, err := e.client.PauseQueue(t.Context(), &cloudtaskspb.PauseQueueRequest{Name: q.GetName()})
	require.NoError(t, err)
	assert.Equal(t, cloudtaskspb.Queue_PAUSED, paused.GetState())

	resumed, err := e.client.ResumeQueue(t.Context(), &cloudtaskspb.ResumeQueueRequest{Name: q.GetName()})
	require.NoError(t, err)
	assert.Equal(t, cloudtaskspb.Queue_RUNNING, resumed.GetState())

	require.NoError(t, e.client.DeleteQueue(t.Context(), &cloudtaskspb.DeleteQueueRequest{Name: q.GetName()}))

	_, err = e.client.GetQueue(t.Context(), &cloudtaskspb.GetQueueRequest{Name: q.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.CreateQueue(t.Context(), &cloudtaskspb.CreateQueueRequest{
		Parent: location, Queue: &cloudtaskspb.Queue{Name: q.GetName()},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	e.clk.advance(7*24*time.Hour + time.Second)
	e.queue(t, "defaults")
}

func TestQueueCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		queue *cloudtaskspb.Queue
		name  string
	}{
		{&cloudtaskspb.Queue{Name: "bad"}, "bad name"},
		{&cloudtaskspb.Queue{Name: "projects/p/locations/other/queues/q"}, "wrong parent"},
		{&cloudtaskspb.Queue{Name: location + "/queues/has_underscore"}, "bad id"},
		{&cloudtaskspb.Queue{
			Name: location + "/queues/q", RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 501},
		}, "rate too high"},
		{&cloudtaskspb.Queue{
			Name: location + "/queues/q", RateLimits: &cloudtaskspb.RateLimits{MaxConcurrentDispatches: 5001},
		}, "concurrency too high"},
		{&cloudtaskspb.Queue{
			Name: location + "/queues/q", RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: -2},
		}, "attempts too low"},
		{&cloudtaskspb.Queue{
			Name: location + "/queues/q",
			RetryConfig: &cloudtaskspb.RetryConfig{
				MinBackoff: durationpb.New(time.Minute), MaxBackoff: durationpb.New(time.Second),
			},
		}, "min above max"},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateQueue(
				t.Context(),
				&cloudtaskspb.CreateQueueRequest{Parent: location, Queue: tt.queue},
			)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestQueueUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, q *cloudtaskspb.Queue)
		in    *cloudtaskspb.Queue
		name  string
		mask  []string
		code  codes.Code
	}{
		{
			name: "single subfield",
			in: &cloudtaskspb.Queue{
				RateLimits: &cloudtaskspb.RateLimits{MaxConcurrentDispatches: 7, MaxDispatchesPerSecond: 3},
			},
			mask: []string{"rate_limits.max_concurrent_dispatches"},
			check: func(t *testing.T, q *cloudtaskspb.Queue) {
				t.Helper()
				assert.EqualValues(t, 7, q.GetRateLimits().GetMaxConcurrentDispatches())
				assert.InDelta(t, 500, q.GetRateLimits().GetMaxDispatchesPerSecond(), 0)
			},
		},
		{
			name: "whole retry config",
			in:   &cloudtaskspb.Queue{RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: 4}},
			mask: []string{"retry_config"},
			check: func(t *testing.T, q *cloudtaskspb.Queue) {
				t.Helper()
				assert.EqualValues(t, 4, q.GetRetryConfig().GetMaxAttempts())
			},
		},
		{name: "state not updatable", in: &cloudtaskspb.Queue{}, mask: []string{"state"}, code: codes.InvalidArgument},
		{
			name: "invalid value",
			in:   &cloudtaskspb.Queue{RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 9999}},
			mask: []string{"rate_limits"},
			code: codes.InvalidArgument,
		},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := e.queue(t, "upd-"+strings.ReplaceAll(tt.name, " ", "-"))
			tt.in.Name = q.GetName()

			out, err := e.client.UpdateQueue(t.Context(), &cloudtaskspb.UpdateQueueRequest{
				Queue: tt.in, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)
			tt.check(t, out)
		})
	}

	t.Run("upsert creates", func(t *testing.T) {
		t.Parallel()

		out, err := e.client.UpdateQueue(t.Context(), &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name:        location + "/queues/upserted",
				RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: 2},
			},
		})
		require.NoError(t, err)
		assert.EqualValues(t, 2, out.GetRetryConfig().GetMaxAttempts())
		assert.Equal(t, cloudtaskspb.Queue_RUNNING, out.GetState())
	})
}

func TestListQueues(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for _, id := range []string{"alpha", "beta", "gamma"} {
		e.queue(t, id)
	}

	_, err := e.client.PauseQueue(t.Context(), &cloudtaskspb.PauseQueueRequest{Name: location + "/queues/beta"})
	require.NoError(t, err)

	tests := []struct {
		name     string
		filter   string
		want     []string
		pageSize int32
		bad      bool
	}{
		{name: "all", want: []string{"alpha", "beta", "gamma"}},
		{name: "paged", pageSize: 2, want: []string{"alpha", "beta", "gamma"}},
		{name: "state filter", filter: "state=PAUSED", want: []string{"beta"}},
		{name: "name contains", filter: "name:amm", want: []string{"gamma"}},
		{name: "unsupported", filter: "rateLimits>3", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListQueues(t.Context(), &cloudtaskspb.ListQueuesRequest{
				Parent: location, Filter: tt.filter, PageSize: tt.pageSize,
			})

			var got []string

			for {
				q, nextErr := it.Next()
				if errors.Is(nextErr, iterator.Done) {
					break
				}

				if tt.bad {
					assert.Equal(t, codes.InvalidArgument, status.Code(nextErr))

					return
				}

				require.NoError(t, nextErr)

				got = append(got, q.GetName()[len(location+"/queues/"):])
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQueueIAMAndCmek(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "iam")

	empty, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())

	policy := &iampb.Policy{
		Bindings: []*iampb.Binding{{Role: "roles/cloudtasks.viewer", Members: []string{"user:a@b.c"}}},
	}
	set, err := e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{Resource: q.GetName(), Policy: policy})
	require.NoError(t, err)
	assert.Len(t, set.GetBindings(), 1)

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: q.GetName(), Policy: &iampb.Policy{Etag: []byte("stale")},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	got, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: q.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "roles/cloudtasks.viewer", got.GetBindings()[0].GetRole())

	perms, err := e.client.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
		Resource: q.GetName(), Permissions: []string{"cloudtasks.tasks.create"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"cloudtasks.tasks.create"}, perms.GetPermissions())

	_, err = e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: location + "/queues/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	cmekName := location + "/cmekConfig"
	key := "projects/p/locations/l/keyRings/r/cryptoKeys/k"

	cfg, err := e.client.GetCmekConfig(t.Context(), &cloudtaskspb.GetCmekConfigRequest{Name: cmekName})
	require.NoError(t, err)
	assert.Empty(t, cfg.GetKmsKey())

	_, err = e.client.UpdateCmekConfig(t.Context(), &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: cmekName, KmsKey: "nope"},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.client.UpdateCmekConfig(t.Context(), &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: cmekName, KmsKey: key},
	})
	require.NoError(t, err)

	cfg, err = e.client.GetCmekConfig(t.Context(), &cloudtaskspb.GetCmekConfigRequest{Name: cmekName})
	require.NoError(t, err)
	assert.Equal(t, key, cfg.GetKmsKey())
}
