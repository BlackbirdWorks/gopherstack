package gcpcloudbuild_test

import (
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

// pendingBuild runs an approval-gated trigger and returns the PENDING build.
func pendingBuild(t *testing.T, e *env, ttl time.Duration) *cloudbuildpb.Build {
	t.Helper()

	trig := approvalTrigger("gated")
	if ttl > 0 {
		trig.GetBuild().QueueTtl = durationpb.New(ttl)
	}

	created, err := e.client.CreateBuildTrigger(t.Context(),
		&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: trig})
	require.NoError(t, err)

	op, err := e.client.RunBuildTrigger(t.Context(),
		&cloudbuildpb.RunBuildTriggerRequest{Name: created.GetResourceName()})
	require.NoError(t, err)

	meta, err := op.Metadata()
	require.NoError(t, err)

	pending := e.get(t, meta.GetBuild().GetName())
	require.Equal(t, cloudbuildpb.Build_PENDING, pending.GetStatus())

	return pending
}

func TestQueueTTLExpiresPendingBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{name: "default one hour", ttl: time.Hour},
		{name: "custom ttl", ttl: 90 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})
			fixed := time.Now()
			e.backend.SetClock(func() time.Time { return fixed }, e.timers.start)

			pending := pendingBuild(t, e, tt.ttl)
			require.True(t, e.timers.fire(tt.ttl), "queue expiry timer must be armed for the queue ttl")

			require.Eventually(t, func() bool {
				return e.get(t, pending.GetName()).GetStatus() == cloudbuildpb.Build_EXPIRED
			}, 5*time.Second, 5*time.Millisecond)

			got := e.get(t, pending.GetName())
			assert.Contains(t, got.GetStatusDetail(), "queue_ttl")
			assert.Empty(t, e.rt.startedImages())
			assert.NotNil(t, got.GetFinishTime())
		})
	}
}

func TestQueueTTLExpiredBeforeApprovalStarts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	var offset atomic.Int64

	base := time.Now()
	e.backend.SetClock(func() time.Time { return base.Add(time.Duration(offset.Load())) }, e.timers.start)

	pending := pendingBuild(t, e, 10*time.Minute)
	offset.Store(int64(11 * time.Minute))

	op, err := e.client.ApproveBuild(t.Context(), &cloudbuildpb.ApproveBuildRequest{
		Name:           pending.GetName(),
		ApprovalResult: &cloudbuildpb.ApprovalResult{Decision: cloudbuildpb.ApprovalResult_APPROVED},
	})
	require.NoError(t, err)

	_, _ = op.Poll(t.Context())
	e.backend.Wait()

	got := e.get(t, pending.GetName())
	assert.Equal(t, cloudbuildpb.Build_EXPIRED, got.GetStatus())
	assert.Empty(t, e.rt.startedImages())
}

func TestQueueTTLNotExpiredWhenStartedInTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	pending := pendingBuild(t, e, 10*time.Minute)

	op, err := e.client.ApproveBuild(t.Context(), &cloudbuildpb.ApproveBuildRequest{
		Name:           pending.GetName(),
		ApprovalResult: &cloudbuildpb.ApprovalResult{Decision: cloudbuildpb.ApprovalResult_APPROVED},
	})
	require.NoError(t, err)

	_, _ = op.Poll(t.Context())
	e.backend.Wait()

	assert.Equal(t, cloudbuildpb.Build_SUCCESS, e.get(t, pending.GetName()).GetStatus())
}
