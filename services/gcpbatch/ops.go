package gcpbatch

import (
	"context"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type opRec struct {
	Created time.Time `json:"created"`
	EndAt   time.Time `json:"endAt"`
	Name    string    `json:"name"`
	Target  string    `json:"target"`
	Verb    string    `json:"verb"`
	Resp    []byte    `json:"resp,omitempty"`
}

func (o *opRec) done(now time.Time) bool { return !now.Before(o.EndAt) }

type opSpec struct {
	end      time.Time
	resp     proto.Message
	target   string
	verb     string
	project  string
	location string
}

func (b *InMemoryBackend) newOpLocked(s opSpec) (*longrunningpb.Operation, error) {
	o := &opRec{
		Name:    locationName(s.project, s.location) + "/" + kindOperations + "/operation-" + uuid.NewString(),
		Target:  s.target,
		Verb:    s.verb,
		Created: b.now(),
		EndAt:   s.end,
	}

	a, err := anypb.New(s.resp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation response: %v", err)
	}

	if o.Resp, err = proto.Marshal(a); err != nil {
		return nil, status.Errorf(codes.Internal, "record operation response: %v", err)
	}

	b.ops[o.Name] = o

	return b.opViewLocked(o), nil
}

func (b *InMemoryBackend) opViewLocked(o *opRec) *longrunningpb.Operation {
	now := b.now()
	meta := &batchpb.OperationMetadata{
		CreateTime: timestamppb.New(o.Created), Target: o.Target, Verb: o.Verb, ApiVersion: apiVersion,
	}
	op := &longrunningpb.Operation{Name: o.Name}

	if o.done(now) {
		meta.EndTime = timestamppb.New(o.EndAt)
		op.Done = true

		resp := &anypb.Any{}
		if proto.Unmarshal(o.Resp, resp) == nil {
			op.Result = &longrunningpb.Operation_Response{Response: resp}
		}
	}

	if a, err := anypb.New(meta); err == nil {
		op.Metadata = a
	}

	return op
}

// GetOperation returns one operation.
func (b *InMemoryBackend) GetOperation(name string) (*longrunningpb.Operation, error) {
	b.mu.Lock("GetOperation")
	defer b.mu.Unlock()

	b.advanceLocked()

	o, ok := b.ops[name]
	if !ok {
		return nil, errNotFound("Operation %q not found.", name)
	}

	return b.opViewLocked(o), nil
}

// WaitOperation returns once the operation is done or the timeout passes, whichever comes first.
func (b *InMemoryBackend) WaitOperation(ctx context.Context, in *longrunningpb.WaitOperationRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.RLock("WaitOperation")
	o, ok := b.ops[in.GetName()]

	var wait time.Duration
	if ok {
		wait = max(o.EndAt.Sub(b.now()), 0)
	}

	b.mu.RUnlock()

	if !ok {
		return nil, errNotFound("Operation %q not found.", in.GetName())
	}

	if in.GetTimeout() != nil {
		wait = min(wait, in.GetTimeout().AsDuration())
	}

	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()

		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}

	return b.GetOperation(in.GetName())
}

// ListOperations lists the operations of one location, ordered by name.
func (b *InMemoryBackend) ListOperations(in *longrunningpb.ListOperationsRequest) (
	*longrunningpb.ListOperationsResponse, error,
) {
	if in.GetFilter() != "" {
		return nil, errInvalid("ListOperations filter is not supported.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if _, _, err := parseParent(strings.TrimSuffix(in.GetName(), "/")); err != nil {
		return nil, err
	}

	prefix := strings.TrimSuffix(in.GetName(), "/") + "/" + kindOperations + "/"

	b.mu.Lock("ListOperations")
	b.advanceLocked()

	names := make([]string, 0, len(b.ops))

	for n := range b.ops {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}

	slices.Sort(names)

	all := make([]*longrunningpb.Operation, 0, len(names))
	for _, n := range names {
		all = append(all, b.opViewLocked(b.ops[n]))
	}

	b.mu.Unlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &longrunningpb.ListOperationsResponse{Operations: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteOperation removes a finished operation.
func (b *InMemoryBackend) DeleteOperation(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteOperation")
	defer b.mu.Unlock()

	o, ok := b.ops[name]
	if !ok {
		return nil, errNotFound("Operation %q not found.", name)
	}

	if !o.done(b.now()) {
		return nil, errPrecondition("Operation %q is still running.", name)
	}

	delete(b.ops, name)

	return &emptypb.Empty{}, nil
}
