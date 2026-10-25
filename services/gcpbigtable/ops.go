package gcpbigtable

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

// finish records an operation that has already completed and returns it.
func (b *InMemoryBackend) finish(instance string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done(instance+"/operations/"+uuid.NewString(), meta, resp)
	if err != nil {
		return nil, fmt.Errorf("record operation: %w", err)
	}

	return b.ops.Put(op), nil
}
