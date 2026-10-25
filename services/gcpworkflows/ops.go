package gcpworkflows

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const apiVersion = "v1"

func (b *InMemoryBackend) metadata(target, verb string) *workflowspb.OperationMetadata {
	now := b.ts()

	return &workflowspb.OperationMetadata{
		CreateTime: now, EndTime: now, Target: target, Verb: verb, ApiVersion: apiVersion,
	}
}

// finish records an operation that has already completed and returns it.
func (b *InMemoryBackend) finish(
	project, location, target, verb string, resp proto.Message,
) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(
		locationName(project, location)+"/operations/"+uuid.NewString(), b.metadata(target, verb), resp)
	if err != nil {
		return nil, fmt.Errorf("record operation: %w", err)
	}

	return b.ops.Put(op), nil
}

// failed records an operation that completed with an error.
func (b *InMemoryBackend) failed(
	project, location, target, verb string, code codes.Code, msg string,
) (*longrunningpb.Operation, error) {
	meta, err := anypb.New(b.metadata(target, verb))
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	op := &longrunningpb.Operation{
		Name:     locationName(project, location) + "/operations/" + uuid.NewString(),
		Metadata: meta,
		Done:     true,
		Result:   &longrunningpb.Operation_Error{Error: status.New(code, msg).Proto()},
	}

	return b.ops.Put(op), nil
}
