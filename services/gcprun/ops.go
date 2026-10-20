package gcprun

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

func opName(parent string) string { return parent + "/operations/" + uuid.NewString() }

func (b *InMemoryBackend) pendingOp(name string, meta proto.Message) (*longrunningpb.Operation, error) {
	m, err := anypb.New(meta)
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	return b.ops.Put(&longrunningpb.Operation{Name: name, Metadata: m}), nil
}

func (b *InMemoryBackend) doneOp(name string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(name, meta, resp)
	if err != nil {
		return nil, err
	}

	return b.ops.Put(op), nil
}

// finishOp resolves a pending operation successfully with resp.
func (b *InMemoryBackend) finishOp(name string, meta, resp proto.Message) {
	if op, err := gcplro.Done(name, meta, resp); err == nil {
		b.ops.Put(op)
	}
}

// failOp resolves a pending operation with st.
func (b *InMemoryBackend) failOp(name string, meta proto.Message, st *status.Status) {
	m, err := anypb.New(meta)
	if err != nil {
		return
	}

	b.ops.Put(&longrunningpb.Operation{
		Name: name, Metadata: m, Done: true, Result: &longrunningpb.Operation_Error{Error: st.Proto()},
	})
}
