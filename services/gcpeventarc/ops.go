package gcpeventarc

import (
	"fmt"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const apiVersion = "v1"

// finish records an operation that has already completed and returns it.
func (b *InMemoryBackend) finish(
	project, location, target, verb string, resp proto.Message,
) (*longrunningpb.Operation, error) {
	now := b.ts()
	meta := &eventarcpb.OperationMetadata{
		CreateTime: now, EndTime: now, Target: target, Verb: verb, ApiVersion: apiVersion,
	}

	op, err := gcplro.Done(childName(project, location, "operations", uuid.NewString()), meta, resp)
	if err != nil {
		return nil, fmt.Errorf("record operation: %w", err)
	}

	return b.ops.Put(op), nil
}
