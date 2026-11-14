package gcpeventarc_test

import (
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestOperationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act      func(t *testing.T, e *env, ops longrunningpb.OperationsClient, name string)
		name     string
		wantCode codes.Code
		wantDone bool
	}{
		{
			name: "completes when the clock fires", wantDone: true,
			act: func(t *testing.T, e *env, _ longrunningpb.OperationsClient, _ string) {
				t.Helper()
				assert.Equal(t, 30*time.Second, e.now.fireNext(t))
			},
		},
		{
			name: "cancel finishes the operation as cancelled", wantDone: true, wantCode: codes.Canceled,
			act: func(t *testing.T, _ *env, ops longrunningpb.OperationsClient, name string) {
				t.Helper()
				_, err := ops.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: name})
				require.NoError(t, err)
			},
		},
		{
			name: "cancel after completion changes nothing", wantDone: true,
			act: func(t *testing.T, e *env, ops longrunningpb.OperationsClient, name string) {
				t.Helper()
				e.now.fireNext(t)
				_, err := ops.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: name})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetOperationDelay(30 * time.Second)
			e.handler.Backend.SetAfterFunc(e.now.after)

			ops := longrunningpb.NewOperationsClient(e.conn)
			op, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
				Parent: parent, MessageBusId: "lro-bus", MessageBus: &eventarcpb.MessageBus{},
			})
			require.NoError(t, err)
			require.False(t, op.Done())

			before, err := ops.GetOperation(t.Context(), opRequest(op.Name()))
			require.NoError(t, err)
			assert.False(t, before.GetDone())

			tt.act(t, e, ops, op.Name())

			after, err := ops.GetOperation(t.Context(), opRequest(op.Name()))
			require.NoError(t, err)
			assert.Equal(t, tt.wantDone, after.GetDone())

			var meta eventarcpb.OperationMetadata
			require.NoError(t, after.GetMetadata().UnmarshalTo(&meta))
			assert.Equal(t, "create", meta.GetVerb())
			assert.NotNil(t, meta.GetEndTime())

			if tt.wantCode != codes.OK {
				assert.Equal(t, int32(tt.wantCode), after.GetError().GetCode())
				assert.True(t, meta.GetRequestedCancellation())

				return
			}

			assert.Nil(t, after.GetError())
			assert.NotNil(t, after.GetResponse())
		})
	}
}

func TestOperationWaitAndUnknown(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetOperationDelay(30 * time.Second)
	e.handler.Backend.SetAfterFunc(e.now.after)

	ops := longrunningpb.NewOperationsClient(e.conn)

	op, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
		Parent: parent, MessageBusId: "wait-bus", MessageBus: &eventarcpb.MessageBus{},
	})
	require.NoError(t, err)

	// Not done: Wait returns after its timeout with the operation still pending.
	got, err := ops.WaitOperation(t.Context(), &longrunningpb.WaitOperationRequest{
		Name: op.Name(), Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, got.GetDone())

	done := make(chan *longrunningpb.Operation, 1)

	go func() {
		o, _ := ops.WaitOperation(t.Context(), &longrunningpb.WaitOperationRequest{Name: op.Name()})
		done <- o
	}()

	require.Eventually(t, func() bool { return e.now.pending() > 0 }, waitFor, tick)
	e.now.fireNext(t)

	select {
	case o := <-done:
		assert.True(t, o.GetDone())
	case <-time.After(waitFor):
		t.Fatal("WaitOperation did not return after completion")
	}

	_, err = ops.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: parent + "/operations/nope"})
	assert.Equal(t, codes.NotFound, code(err))
}
