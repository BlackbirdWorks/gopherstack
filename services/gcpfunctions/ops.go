package gcpfunctions

import (
	"fmt"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

type opKind int

func (k opKind) verb() string { return [...]string{"create", "update", "delete"}[k] }

func (k opKind) typ() functionspb.OperationType {
	return [...]functionspb.OperationType{
		functionspb.OperationType_CREATE_FUNCTION,
		functionspb.OperationType_UPDATE_FUNCTION,
		functionspb.OperationType_DELETE_FUNCTION,
	}[k]
}

const (
	opCreate opKind = iota
	opUpdate
	opDelete
)

type job struct {
	created *timestamppb.Timestamp
	op      string
	name    string
	kind    opKind
}

func (j job) meta(end *timestamppb.Timestamp) *functionspb.OperationMetadata {
	return &functionspb.OperationMetadata{
		CreateTime: j.created, EndTime: end, Target: j.name, Verb: j.kind.verb(),
		ApiVersion: "v2", OperationType: j.kind.typ(),
	}
}

// begin records a pending operation and runs the job in the background.
func (b *InMemoryBackend) begin(parent string, j job) (*longrunningpb.Operation, error) {
	j.op = parent + "/operations/" + uuid.NewString()

	m, err := anypb.New(j.meta(nil))
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	op := b.ops.Put(&longrunningpb.Operation{Name: j.op, Metadata: m})
	b.wg.Go(func() { b.run(j) })

	return op, nil
}

func (b *InMemoryBackend) finishOp(j job, resp proto.Message) {
	if op, err := gcplro.Done(j.op, j.meta(b.ts()), resp); err == nil {
		b.ops.Put(op)
	}
}

func (b *InMemoryBackend) failOp(j job, st *status.Status) {
	m, err := anypb.New(j.meta(b.ts()))
	if err != nil {
		return
	}

	b.ops.Put(&longrunningpb.Operation{
		Name: j.op, Metadata: m, Done: true, Result: &longrunningpb.Operation_Error{Error: st.Proto()},
	})
}
