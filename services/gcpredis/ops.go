package gcpredis

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

// workFn is the background half of a long-running operation; its message becomes the operation response.
type workFn func() (proto.Message, error)

type job struct {
	created *timestamppb.Timestamp
	op      string
	target  string
	verb    string
}

func (j job) meta(end *timestamppb.Timestamp) *redispb.OperationMetadata {
	return &redispb.OperationMetadata{
		CreateTime: j.created, EndTime: end, Target: j.target, Verb: j.verb, ApiVersion: "v1",
	}
}

// begin records a pending operation under parent and runs work in the background.
func (b *InMemoryBackend) begin(parent, verb, target string, work workFn) (*longrunningpb.Operation, error) {
	j := job{op: parent + "/operations/operation-" + uuid.NewString(), created: b.ts(), target: target, verb: verb}

	m, err := anypb.New(j.meta(nil))
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	op := b.ops.Put(&longrunningpb.Operation{Name: j.op, Metadata: m})

	b.wg.Go(func() {
		if b.gate != nil {
			b.gate()
		}

		b.finish(j, work)
	})

	return op, nil
}

func (b *InMemoryBackend) finish(j job, work workFn) {
	resp, err := work()
	if err != nil {
		b.failOp(j, status.Convert(err))

		return
	}

	if op, doneErr := gcplro.Done(j.op, j.meta(b.ts()), resp); doneErr == nil {
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

var errGone = status.Error(codes.Aborted, "Instance was deleted or the emulator was reset while the operation ran.")
