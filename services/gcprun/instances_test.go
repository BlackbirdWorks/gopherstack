package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func instSpec(image string, port int32) *runpb.Instance {
	c := &runpb.Container{Image: image}
	if port > 0 {
		c.Ports = []*runpb.ContainerPort{{ContainerPort: port}}
	}

	return &runpb.Instance{Containers: []*runpb.Container{c}}
}

func (e *env) createInstance(t *testing.T, id string, in *runpb.Instance) *runpb.Instance {
	t.Helper()

	op, err := e.insts.CreateInstance(t.Context(), &runpb.CreateInstanceRequest{
		Parent: parent, InstanceId: id, Instance: in,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func TestInstance_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	in := e.createInstance(t, "box", instSpec("example/box:1", 8080))

	assert.Equal(t, parent+"/instances/box", in.GetName())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, in.GetTerminalCondition().GetState())
	assert.False(t, in.GetReconciling())
	assert.Equal(t, int64(1), in.GetGeneration())
	assert.Equal(t, int64(1), in.GetObservedGeneration())
	require.Len(t, in.GetUrls(), 1)
	assert.Equal(t, 1, e.rt.running())

	code, body := e.get(t, in.GetUrls()[0][len("http://"):], "/hi")
	assert.Equal(t, 200, code)
	assert.Contains(t, body, "path=/hi")

	got, err := e.insts.GetInstance(t.Context(), &runpb.GetInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)
	assert.Equal(t, in.GetUid(), got.GetUid())

	it := e.insts.ListInstances(t.Context(), &runpb.ListInstancesRequest{Parent: parent})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, in.GetName(), listed.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	stop, err := e.insts.StopInstance(t.Context(), &runpb.StopInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)

	stopped, err := stop.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), stopped.GetGeneration())
	assert.Equal(t, runpb.Condition_CONDITION_PENDING, stopped.GetTerminalCondition().GetState())
	assert.Equal(t, 0, e.rt.running())

	status503, _ := e.get(t, in.GetUrls()[0][len("http://"):], "/")
	assert.Equal(t, 503, status503, "a stopped instance serves nothing")

	start, err := e.insts.StartInstance(t.Context(), &runpb.StartInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)

	started, err := start.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, started.GetTerminalCondition().GetState())
	assert.Equal(t, int64(3), started.GetGeneration())
	assert.Equal(t, 1, e.rt.running())

	del, err := e.insts.DeleteInstance(t.Context(), &runpb.DeleteInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, deleted.GetDeleteTime())
	assert.NotNil(t, deleted.GetExpireTime())
	assert.Equal(t, 0, e.rt.running())

	_, err = e.insts.GetInstance(t.Context(), &runpb.GetInstanceRequest{Name: in.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	all := e.insts.ListInstances(t.Context(), &runpb.ListInstancesRequest{Parent: parent, ShowDeleted: true})
	tomb, err := all.Next()
	require.NoError(t, err)
	assert.NotNil(t, tomb.GetDeleteTime())
}

func TestInstance_CreateErrors(t *testing.T) {
	t.Parallel()

	two := instSpec("a", 0)
	two.Containers = append(two.Containers, &runpb.Container{Image: "b"})

	tests := []struct {
		inst *runpb.Instance
		name string
		id   string
	}{
		{instSpec("i", 0), "bad id", "Bad_ID"},
		{nil, "no instance", "ok"},
		{instSpec("", 0), "no image", "ok"},
		{two, "two containers", "ok"},
		{&runpb.Instance{}, "no container", "ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.insts.CreateInstance(t.Context(), &runpb.CreateInstanceRequest{
				Parent: parent, InstanceId: tt.id, Instance: tt.inst,
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestInstance_DuplicateValidateOnlyEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	in := e.createInstance(t, "one", instSpec("i", 0))

	_, err := e.insts.CreateInstance(t.Context(), &runpb.CreateInstanceRequest{
		Parent: parent, InstanceId: "one", Instance: instSpec("i", 0),
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	dry, err := e.insts.CreateInstance(t.Context(), &runpb.CreateInstanceRequest{
		Parent: parent, InstanceId: "dry", Instance: instSpec("i", 0), ValidateOnly: true,
	})
	require.NoError(t, err)
	assert.True(t, dry.Done())

	_, err = e.insts.GetInstance(t.Context(), &runpb.GetInstanceRequest{Name: parent + "/instances/dry"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.insts.StopInstance(t.Context(), &runpb.StopInstanceRequest{Name: in.GetName(), Etag: "stale"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = e.insts.DeleteInstance(t.Context(), &runpb.DeleteInstanceRequest{Name: in.GetName(), Etag: "stale"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = e.insts.StartInstance(t.Context(), &runpb.StartInstanceRequest{Name: parent + "/instances/nope"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestInstance_StartFailure(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.failRun = assert.AnError

	op, err := e.insts.CreateInstance(t.Context(), &runpb.CreateInstanceRequest{
		Parent: parent, InstanceId: "bad", Instance: instSpec("i", 0),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	got, err := e.insts.GetInstance(t.Context(), &runpb.GetInstanceRequest{Name: parent + "/instances/bad"})
	require.NoError(t, err)
	assert.Equal(t, runpb.Condition_CONDITION_FAILED, got.GetTerminalCondition().GetState())

	e.rt.failRun = nil

	start, err := e.insts.StartInstance(t.Context(), &runpb.StartInstanceRequest{Name: got.GetName()})
	require.NoError(t, err)

	ok, err := start.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, ok.GetTerminalCondition().GetState())
}

func TestInstance_Persistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	in := src.createInstance(t, "kept", instSpec("i", 0))

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	got, err := dst.insts.GetInstance(t.Context(), &runpb.GetInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)
	assert.Equal(t, in.GetUid(), got.GetUid())
	assert.Equal(t, runpb.Condition_CONDITION_PENDING, got.GetTerminalCondition().GetState(),
		"containers do not survive a restart")
	assert.Equal(t, 0, dst.rt.running())

	start, err := dst.insts.StartInstance(t.Context(), &runpb.StartInstanceRequest{Name: in.GetName()})
	require.NoError(t, err)

	_, err = start.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, dst.rt.running())
}
