package gcpdatastore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type fakeTimers struct {
	fns []func()
	mu  sync.Mutex
}

func (f *fakeTimers) after(_ time.Duration, fn func()) func() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.fns = append(f.fns, fn)

	return func() {}
}

func (f *fakeTimers) fire() {
	f.mu.Lock()
	fns := f.fns
	f.fns = nil
	f.mu.Unlock()

	for _, fn := range fns {
		fn()
	}
}

func newAsyncEnv(t *testing.T) (*env, *fakeTimers, longrunningpb.OperationsClient) {
	t.Helper()

	e := newEnv(t)
	ft := &fakeTimers{}
	e.handler.Backend.SetIndexBuild(time.Minute, ft.after)

	return e, ft, longrunningpb.NewOperationsClient(e.conn)
}

func (e *env) startIndex(t *testing.T) *longrunningpb.Operation {
	t.Helper()

	op, err := e.admin.CreateIndex(t.Context(), &adminpb.CreateIndexRequest{
		ProjectId: project,
		Index: &adminpb.Index{
			Kind: "Task", Ancestor: adminpb.Index_NONE,
			Properties: []*adminpb.Index_IndexedProperty{prop("Done", false), prop("Priority", false)},
		},
	})
	require.NoError(t, err)

	return op
}

func (e *env) indexState(t *testing.T, id string) adminpb.Index_State {
	t.Helper()

	got, err := e.admin.GetIndex(t.Context(), &adminpb.GetIndexRequest{ProjectId: project, IndexId: id})
	require.NoError(t, err)

	return got.GetState()
}

func TestAsyncCreateIndex(t *testing.T) {
	t.Parallel()

	q := datastore.NewQuery("Task").FilterField("Done", "=", true).Order("Priority").KeysOnly()

	t.Run("creating then ready", func(t *testing.T) {
		t.Parallel()

		e, ft, ops := newAsyncEnv(t)
		op := e.startIndex(t)
		require.False(t, op.GetDone())

		meta := &adminpb.IndexOperationMetadata{}
		require.NoError(t, op.GetMetadata().UnmarshalTo(meta))
		assert.Equal(t, adminpb.CommonMetadata_PROCESSING, meta.GetCommon().GetState())
		assert.Nil(t, meta.GetCommon().GetEndTime())

		assert.Equal(t, adminpb.Index_CREATING, e.indexState(t, meta.GetIndexId()))

		_, err := e.client.GetAll(t.Context(), q, nil)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		ft.fire()

		got, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
		require.NoError(t, err)
		require.True(t, got.GetDone())

		idx := &adminpb.Index{}
		require.NoError(t, got.GetResponse().UnmarshalTo(idx))
		assert.Equal(t, adminpb.Index_READY, idx.GetState())
		assert.Equal(t, adminpb.Index_READY, e.indexState(t, meta.GetIndexId()))

		_, err = e.client.GetAll(t.Context(), q, nil)
		require.NoError(t, err)
	})

	t.Run("wait returns when finished", func(t *testing.T) {
		t.Parallel()

		e, ft, ops := newAsyncEnv(t)
		op := e.startIndex(t)

		done := make(chan *longrunningpb.Operation, 1)

		go func() {
			got, err := ops.WaitOperation(context.WithoutCancel(t.Context()),
				&longrunningpb.WaitOperationRequest{Name: op.GetName(), Timeout: durationpb.New(time.Minute)})
			if err == nil {
				done <- got
			}
		}()

		for {
			ft.fire()

			select {
			case got := <-done:
				assert.True(t, got.GetDone())

				return
			case <-t.Context().Done():
				t.Fatal("wait never returned")
			default:
			}
		}
	})

	t.Run("cancel leaves the index in error", func(t *testing.T) {
		t.Parallel()

		e, ft, ops := newAsyncEnv(t)
		op := e.startIndex(t)

		meta := &adminpb.IndexOperationMetadata{}
		require.NoError(t, op.GetMetadata().UnmarshalTo(meta))

		_, err := ops.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: op.GetName()})
		require.NoError(t, err)

		got, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
		require.NoError(t, err)
		require.True(t, got.GetDone())
		assert.EqualValues(t, codes.Canceled, got.GetError().GetCode())
		assert.Equal(t, adminpb.Index_ERROR, e.indexState(t, meta.GetIndexId()))

		ft.fire()
		assert.Equal(t, adminpb.Index_ERROR, e.indexState(t, meta.GetIndexId()))

		_, err = e.client.GetAll(t.Context(), q, nil)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("delete operation survives completion", func(t *testing.T) {
		t.Parallel()

		e, ft, ops := newAsyncEnv(t)
		op := e.startIndex(t)

		_, err := ops.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: op.GetName()})
		require.NoError(t, err)

		ft.fire()

		_, err = ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("list shows pending", func(t *testing.T) {
		t.Parallel()

		e, _, ops := newAsyncEnv(t)
		op := e.startIndex(t)

		list, err := ops.ListOperations(t.Context(), &longrunningpb.ListOperationsRequest{Name: "projects/" + project})
		require.NoError(t, err)
		require.Len(t, list.GetOperations(), 1)
		assert.Equal(t, op.GetName(), list.GetOperations()[0].GetName())
		assert.False(t, list.GetOperations()[0].GetDone())
	})
}

func TestAsyncDeleteIndex(t *testing.T) {
	t.Parallel()

	e, ft, ops := newAsyncEnv(t)
	created := e.startIndex(t)
	ft.fire()

	meta := &adminpb.IndexOperationMetadata{}
	require.NoError(t, created.GetMetadata().UnmarshalTo(meta))
	id := meta.GetIndexId()

	op, err := e.admin.DeleteIndex(t.Context(), &adminpb.DeleteIndexRequest{ProjectId: project, IndexId: id})
	require.NoError(t, err)
	require.False(t, op.GetDone())
	assert.Equal(t, adminpb.Index_DELETING, e.indexState(t, id))

	_, err = e.admin.DeleteIndex(t.Context(), &adminpb.DeleteIndexRequest{ProjectId: project, IndexId: id})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	ft.fire()

	got, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())

	_, err = e.admin.GetIndex(t.Context(), &adminpb.GetIndexRequest{ProjectId: project, IndexId: id})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestAsyncIndexSurvivesRestoreAsError(t *testing.T) {
	t.Parallel()

	src, _, _ := newAsyncEnv(t)
	op := src.startIndex(t)

	meta := &adminpb.IndexOperationMetadata{}
	require.NoError(t, op.GetMetadata().UnmarshalTo(meta))

	snap := src.handler.Snapshot(t.Context())

	dst, _, ops := newAsyncEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	assert.Equal(t, adminpb.Index_ERROR, dst.indexState(t, meta.GetIndexId()))

	got, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())
	assert.EqualValues(t, codes.Aborted, got.GetError().GetCode())
}
