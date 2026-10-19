package gcpcloudtasks_test

import (
	"testing"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func createReq(parent, name string) *cloudtaskspb.CreateTaskRequest {
	t := &cloudtaskspb.Task{Name: name}
	httpTask("https://example.com/h")(t)

	return &cloudtaskspb.CreateTaskRequest{Parent: parent, Task: t}
}

func TestBatchCreateTasks(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "batchc").GetName()
	e.task(t, q, httpTask("https://example.com/h"), func(task *cloudtaskspb.Task) { task.Name = q + "/tasks/dup" })

	tests := []struct {
		wantFail map[int32]codes.Code
		name     string
		reqs     []*cloudtaskspb.CreateTaskRequest
		wantOK   int
	}{
		{
			name:   "all succeed",
			reqs:   []*cloudtaskspb.CreateTaskRequest{createReq(q, q+"/tasks/a"), createReq(q, "")},
			wantOK: 2,
		},
		{
			name: "partial failure",
			reqs: []*cloudtaskspb.CreateTaskRequest{
				createReq(q, q+"/tasks/b"),
				createReq(q, q+"/tasks/dup"),
				{Parent: q, Task: &cloudtaskspb.Task{}},
				{Parent: q},
			},
			wantOK: 1,
			wantFail: map[int32]codes.Code{
				1: codes.AlreadyExists, 2: codes.InvalidArgument, 3: codes.InvalidArgument,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, err := e.client.BatchCreateTasks(t.Context(), &cloudtaskspb.BatchCreateTasksRequest{
				Parent: q, Requests: tt.reqs,
			})
			require.NoError(t, err)

			resp, err := op.Wait(t.Context())
			require.NoError(t, err)
			assert.Len(t, resp.GetTasks(), tt.wantOK)

			meta, err := op.Metadata()
			require.NoError(t, err)
			assert.NotNil(t, meta.GetStartTime())
			assert.NotNil(t, meta.GetEndTime())
			require.Len(t, meta.GetFailedRequests(), len(tt.wantFail))

			for idx, code := range tt.wantFail {
				assert.Equal(t, int32(code), meta.GetFailedRequests()[idx].GetCode())
			}

			for _, task := range resp.GetTasks() {
				_, err = e.getTask(t, task.GetName())
				require.NoError(t, err)
			}

			got, err := e.client.LROClient.GetOperation(
				t.Context(),
				&longrunningpb.GetOperationRequest{Name: op.Name()},
			)
			require.NoError(t, err)
			assert.True(t, got.GetDone())
			assert.Equal(t, op.Name(), got.GetName())
		})
	}
}

func TestBatchCreateTasksValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "batchv").GetName()
	many := make([]*cloudtaskspb.CreateTaskRequest, 101)

	for i := range many {
		many[i] = createReq(q, "")
	}

	tests := []struct {
		req  *cloudtaskspb.BatchCreateTasksRequest
		name string
		code codes.Code
	}{
		{name: "empty", req: &cloudtaskspb.BatchCreateTasksRequest{Parent: q}, code: codes.InvalidArgument},
		{
			name: "too many",
			req:  &cloudtaskspb.BatchCreateTasksRequest{Parent: q, Requests: many},
			code: codes.InvalidArgument,
		},
		{name: "bad parent", req: &cloudtaskspb.BatchCreateTasksRequest{Parent: "x"}, code: codes.InvalidArgument},
		{
			name: "mismatched parent",
			req: &cloudtaskspb.BatchCreateTasksRequest{
				Parent: q, Requests: []*cloudtaskspb.CreateTaskRequest{createReq(location+"/queues/other", "")},
			},
			code: codes.InvalidArgument,
		},
		{
			name: "missing queue",
			req: &cloudtaskspb.BatchCreateTasksRequest{
				Parent:   location + "/queues/nope",
				Requests: []*cloudtaskspb.CreateTaskRequest{createReq(location+"/queues/nope", "")},
			},
			code: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.BatchCreateTasks(t.Context(), tt.req)
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestBatchDeleteTasks(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "batchd").GetName()

	names := make([]string, 0, 3)

	for _, id := range []string{"a", "b", "c"} {
		n := q + "/tasks/" + id
		e.task(t, q, httpTask("https://example.com/h"), func(task *cloudtaskspb.Task) { task.Name = n })
		names = append(names, n)
	}

	req := &cloudtaskspb.BatchDeleteTasksRequest{
		Parent: q,
		Names:  []string{names[0], q + "/tasks/missing", names[1], location + "/queues/other/tasks/x"},
	}

	op, err := e.client.BatchDeleteTasks(t.Context(), req)
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	meta, err := op.Metadata()
	require.NoError(t, err)
	require.Len(t, meta.GetFailedRequests(), 2)
	assert.Equal(t, int32(codes.NotFound), meta.GetFailedRequests()[1].GetCode())
	assert.Equal(t, int32(codes.InvalidArgument), meta.GetFailedRequests()[3].GetCode())

	for _, n := range names[:2] {
		_, err = e.getTask(t, n)
		assert.Equal(t, codes.NotFound, status.Code(err))
	}

	_, err = e.getTask(t, names[2])
	require.NoError(t, err)

	_, err = e.client.BatchDeleteTasks(t.Context(), &cloudtaskspb.BatchDeleteTasksRequest{Parent: q})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestBatchRequestIDIdempotent(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "batchi").GetName()
	req := &cloudtaskspb.BatchCreateTasksRequest{
		Parent: q, RequestId: "req-1", Requests: []*cloudtaskspb.CreateTaskRequest{createReq(q, "")},
	}

	first, err := e.client.BatchCreateTasks(t.Context(), req)
	require.NoError(t, err)

	second, err := e.client.BatchCreateTasks(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, first.Name(), second.Name())

	it := e.client.ListTasks(t.Context(), &cloudtaskspb.ListTasksRequest{Parent: q})
	n := 0

	for _, err = it.Next(); err == nil; _, err = it.Next() {
		n++
	}

	require.ErrorIs(t, err, iterator.Done)
	assert.Equal(t, 1, n)
}

func TestOperationsAPI(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "ops").GetName()
	lro := e.client.LROClient

	op, err := e.client.BatchCreateTasks(t.Context(), &cloudtaskspb.BatchCreateTasksRequest{
		Parent: q, Requests: []*cloudtaskspb.CreateTaskRequest{createReq(q, "")},
	})
	require.NoError(t, err)

	waited, err := lro.WaitOperation(t.Context(), &longrunningpb.WaitOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, waited.GetDone())

	list := lro.ListOperations(t.Context(), &longrunningpb.ListOperationsRequest{Name: q})
	listed, err := list.Next()
	require.NoError(t, err)
	assert.Equal(t, op.Name(), listed.GetName())

	_, err = list.Next()
	require.ErrorIs(t, err, iterator.Done)

	require.NoError(t, lro.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: op.Name()}))
	require.NoError(t, lro.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: op.Name()}))

	_, err = lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(
		t,
		codes.NotFound,
		status.Code(lro.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: op.Name()})),
	)
}

func TestOperationsSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	q := src.queue(t, "opsnap").GetName()

	op, err := src.client.BatchCreateTasks(t.Context(), &cloudtaskspb.BatchCreateTasksRequest{
		Parent: q, Requests: []*cloudtaskspb.CreateTaskRequest{createReq(q, "")},
	})
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	got, err := dst.client.LROClient.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())
}
