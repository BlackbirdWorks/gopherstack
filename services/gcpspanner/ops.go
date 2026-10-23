package gcpspanner

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

type workFn func() (proto.Message, error)

type metaFn func(end *timestamppb.Timestamp) proto.Message

var errGone = status.Error(codes.Aborted, "Instance was deleted or the emulator was reset while the operation ran.")

func opName(parent, id string) string {
	if id == "" {
		id = "operation-" + uuid.NewString()
	}

	return parent + "/operations/" + id
}

// begin records a pending operation and finishes it in the background.
func (b *InMemoryBackend) begin(name string, meta metaFn, work workFn) (*longrunningpb.Operation, error) {
	m, err := anypb.New(meta(nil))
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	op := b.ops.Put(&longrunningpb.Operation{Name: name, Metadata: m})

	b.wg.Go(func() {
		resp, workErr := work()
		if workErr != nil {
			b.failOp(name, meta, status.Convert(workErr))

			return
		}

		if done, doneErr := gcplro.Done(name, meta(b.ts()), resp); doneErr == nil {
			b.ops.Put(done)
		}
	})

	return op, nil
}

func (b *InMemoryBackend) failOp(name string, meta metaFn, st *status.Status) {
	m, err := anypb.New(meta(b.ts()))
	if err != nil {
		return
	}

	b.ops.Put(&longrunningpb.Operation{
		Name: name, Metadata: m, Done: true, Result: &longrunningpb.Operation_Error{Error: st.Proto()},
	})
}

// finished records an already successful operation.
func (b *InMemoryBackend) finished(name string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(name, meta, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return b.ops.Put(op), nil
}

func statusOf(err error) *status.Status { return status.Convert(err) }
