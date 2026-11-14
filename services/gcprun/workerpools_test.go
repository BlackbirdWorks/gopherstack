package gcprun_test

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func poolSpec(image string, instances int32) *runpb.WorkerPool {
	return &runpb.WorkerPool{
		Template: &runpb.WorkerPoolRevisionTemplate{Containers: []*runpb.Container{{Image: image}}},
		Scaling:  &runpb.WorkerPoolScaling{ManualInstanceCount: &instances},
	}
}

func (e *env) createPool(t *testing.T, id string, wp *runpb.WorkerPool) *runpb.WorkerPool {
	t.Helper()

	op, err := e.pools.CreateWorkerPool(t.Context(), &runpb.CreateWorkerPoolRequest{
		Parent: parent, WorkerPoolId: id, WorkerPool: wp,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func TestWorkerPool_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wp := e.createPool(t, "queue-worker", poolSpec("example/worker:1", 2))

	assert.Equal(t, parent+"/workerPools/queue-worker", wp.GetName())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, wp.GetTerminalCondition().GetState())
	assert.False(t, wp.GetReconciling())
	assert.Equal(t, int64(1), wp.GetGeneration())
	assert.Equal(t, wp.GetLatestCreatedRevision(), wp.GetLatestReadyRevision())
	require.Len(t, wp.GetInstanceSplitStatuses(), 1)
	assert.Equal(t, int32(100), wp.GetInstanceSplitStatuses()[0].GetPercent())
	assert.Equal(t, 2, e.rt.running(), "manual_instance_count containers run")

	spec := e.rt.specs()[0]
	assert.Equal(t, "queue-worker", envOf(spec, "CLOUD_RUN_WORKER_POOL"))
	assert.Equal(t, lastRev(wp.GetLatestReadyRevision()), envOf(spec, "CLOUD_RUN_REVISION"))
	assert.Empty(t, envOf(spec, "PORT"), "worker pools have no endpoint")
	assert.Empty(t, spec.Ports, "no port is published")

	got, err := e.pools.GetWorkerPool(t.Context(), &runpb.GetWorkerPoolRequest{Name: wp.GetName()})
	require.NoError(t, err)
	assert.Equal(t, wp.GetUid(), got.GetUid())

	rev, err := e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: wp.GetLatestReadyRevision()})
	require.NoError(t, err)
	assert.Equal(t, "example/worker:1", rev.GetContainers()[0].GetImage())

	revs := e.revs.ListRevisions(t.Context(), &runpb.ListRevisionsRequest{Parent: wp.GetName()})
	first, err := revs.Next()
	require.NoError(t, err)
	assert.Equal(t, rev.GetName(), first.GetName())

	_, err = revs.Next()
	require.ErrorIs(t, err, iterator.Done)

	it := e.pools.ListWorkerPools(t.Context(), &runpb.ListWorkerPoolsRequest{Parent: parent})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, wp.GetName(), listed.GetName())

	del, err := e.pools.DeleteWorkerPool(t.Context(), &runpb.DeleteWorkerPoolRequest{Name: wp.GetName()})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, deleted.GetDeleteTime())
	assert.NotNil(t, deleted.GetExpireTime())
	assert.Equal(t, 0, e.rt.running())

	_, err = e.pools.GetWorkerPool(t.Context(), &runpb.GetWorkerPoolRequest{Name: wp.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: rev.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	all := e.pools.ListWorkerPools(t.Context(), &runpb.ListWorkerPoolsRequest{Parent: parent, ShowDeleted: true})
	tomb, err := all.Next()
	require.NoError(t, err)
	assert.Equal(t, wp.GetName(), tomb.GetName())
	assert.NotNil(t, tomb.GetDeleteTime())
}

func scaled(name string, n *int32) *runpb.WorkerPool {
	return &runpb.WorkerPool{Name: name, Scaling: &runpb.WorkerPoolScaling{ManualInstanceCount: n}}
}

func lastRev(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[i+1:]
		}
	}

	return name
}

func TestWorkerPool_CreateErrors(t *testing.T) {
	t.Parallel()

	neg := int32(-1)
	tests := []struct {
		pool func() *runpb.WorkerPool
		name string
		id   string
	}{
		{func() *runpb.WorkerPool { return poolSpec("i", 1) }, "bad id", "Bad_ID"},
		{func() *runpb.WorkerPool { return &runpb.WorkerPool{} }, "no template", "ok"},
		{func() *runpb.WorkerPool { return poolSpec("", 1) }, "no image", "ok"},
		{func() *runpb.WorkerPool {
			wp := poolSpec("i", 1)
			wp.Scaling.ManualInstanceCount = &neg

			return wp
		}, "negative count", "ok"},
		{func() *runpb.WorkerPool {
			wp := poolSpec("i", 1)
			wp.InstanceSplits = []*runpb.InstanceSplit{{Percent: 60}}

			return wp
		}, "split sum", "ok"},
		{func() *runpb.WorkerPool {
			wp := poolSpec("i", 1)
			wp.InstanceSplits = []*runpb.InstanceSplit{{Revision: "nope", Percent: 100}}

			return wp
		}, "unknown revision", "ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.pools.CreateWorkerPool(t.Context(), &runpb.CreateWorkerPoolRequest{
				Parent: parent, WorkerPoolId: tt.id, WorkerPool: tt.pool(),
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestWorkerPool_NameRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createService(t, "taken", "example/web:1")
	e.createPool(t, "dup", poolSpec("i", 0))

	for name, id := range map[string]string{"service name": "taken", "duplicate": "dup"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := e.pools.CreateWorkerPool(t.Context(), &runpb.CreateWorkerPoolRequest{
				Parent: parent, WorkerPoolId: id, WorkerPool: poolSpec("i", 0),
			})
			assert.Equal(t, codes.AlreadyExists, status.Code(err))
		})
	}
}

func TestWorkerPool_ValidateOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	op, err := e.pools.CreateWorkerPool(t.Context(), &runpb.CreateWorkerPoolRequest{
		Parent: parent, WorkerPoolId: "dry", WorkerPool: poolSpec("i", 1), ValidateOnly: true,
	})
	require.NoError(t, err)
	require.True(t, op.Done())

	_, err = e.pools.GetWorkerPool(t.Context(), &runpb.GetWorkerPoolRequest{Name: parent + "/workerPools/dry"})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, 0, e.rt.running())
}

func TestWorkerPool_UpdateRevisionsAndSplits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wp := e.createPool(t, "wp", poolSpec("example/worker:1", 1))
	rev1 := wp.GetLatestReadyRevision()

	next := poolSpec("example/worker:2", 1)
	next.Name = wp.GetName()

	op, err := e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{
		WorkerPool: next, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}},
	})
	require.NoError(t, err)

	upd, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), upd.GetGeneration())
	assert.NotEqual(t, rev1, upd.GetLatestReadyRevision())
	assert.Equal(t, 1, e.rt.running(), "the superseded revision no longer gets instances")

	got, err := e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: upd.GetLatestReadyRevision()})
	require.NoError(t, err)
	assert.Equal(t, "example/worker:2", got.GetContainers()[0].GetImage())

	_, err = e.revs.DeleteRevision(t.Context(), &runpb.DeleteRevisionRequest{Name: upd.GetLatestReadyRevision()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a revision that holds the split cannot go")

	del, err := e.revs.DeleteRevision(t.Context(), &runpb.DeleteRevisionRequest{Name: rev1})
	require.NoError(t, err)
	_, err = del.Wait(t.Context())
	require.NoError(t, err)

	split := &runpb.WorkerPool{
		Name:           wp.GetName(),
		InstanceSplits: []*runpb.InstanceSplit{{Revision: "nope", Percent: 100}},
	}
	_, err = e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{
		WorkerPool: split, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"instance_splits"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestWorkerPool_ScaleAndEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wp := e.createPool(t, "scaled", poolSpec("i", 1))

	three := int32(3)
	op, err := e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{
		WorkerPool: scaled(wp.GetName(), &three),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scaling"}},
	})
	require.NoError(t, err)

	upd, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), upd.GetGeneration())

	require.Eventually(t, func() bool { return e.rt.running() == 3 }, 5e9, 1e7)

	one := int32(1)
	op, err = e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{
		WorkerPool: scaled(wp.GetName(), &one),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scaling"}},
	})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e.rt.running() == 1 }, 5e9, 1e7)

	_, err = e.pools.DeleteWorkerPool(t.Context(), &runpb.DeleteWorkerPoolRequest{Name: wp.GetName(), Etag: "stale"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestWorkerPool_AllowMissing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wp := poolSpec("i", 1)
	wp.Name = parent + "/workerPools/upsert"

	_, err := e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{WorkerPool: wp})
	assert.Equal(t, codes.NotFound, status.Code(err))

	op, err := e.pools.UpdateWorkerPool(t.Context(), &runpb.UpdateWorkerPoolRequest{WorkerPool: wp, AllowMissing: true})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, wp.GetName(), got.GetName())
}

func TestWorkerPool_NoRuntime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	wp := e.createPool(t, "norun", poolSpec("i", 1))
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, wp.GetTerminalCondition().GetState())
}

func TestWorkerPool_RuntimeFailure(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.failRun = assert.AnError

	op, err := e.pools.CreateWorkerPool(t.Context(), &runpb.CreateWorkerPoolRequest{
		Parent: parent, WorkerPoolId: "bad", WorkerPool: poolSpec("i", 1),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	got, err := e.pools.GetWorkerPool(t.Context(), &runpb.GetWorkerPoolRequest{Name: parent + "/workerPools/bad"})
	require.NoError(t, err)
	assert.Equal(t, runpb.Condition_CONDITION_FAILED, got.GetTerminalCondition().GetState())
}

func TestWorkerPool_IAM(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wp := e.createPool(t, "iam", poolSpec("i", 0))

	empty, err := e.pools.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: wp.GetName()})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())

	pol, err := e.pools.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: wp.GetName(),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{
			{Role: "roles/run.invoker", Members: []string{"user:a@example.com"}},
		}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, pol.GetEtag())

	_, err = e.pools.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: wp.GetName(),
		Policy:   &iampb.Policy{Etag: []byte("stale")},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	perms, err := e.pools.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
		Resource: wp.GetName(), Permissions: []string{"run.workerpools.get", "run.services.get"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"run.workerpools.get"}, perms.GetPermissions())

	_, err = e.pools.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: parent + "/workerPools/nope"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	del, err := e.pools.DeleteWorkerPool(t.Context(), &runpb.DeleteWorkerPoolRequest{Name: wp.GetName()})
	require.NoError(t, err)
	_, err = del.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.pools.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: wp.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestWorkerPool_Persistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	wp := src.createPool(t, "kept", poolSpec("example/worker:1", 1))

	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.pools.GetWorkerPool(t.Context(), &runpb.GetWorkerPoolRequest{Name: wp.GetName()})
	require.NoError(t, err)
	assert.Equal(t, wp.GetUid(), got.GetUid())
	assert.Equal(t, wp.GetLatestReadyRevision(), got.GetLatestReadyRevision())

	_, err = dst.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: wp.GetLatestReadyRevision()})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return dst.rt.running() == 1 }, 5e9, 1e7)
}
