// Package gcplro is an in-memory google.longrunning.Operations server for the GCP emulators.
// Operations are recorded already finished, so Wait and Cancel never block or change state.
package gcplro

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultPageSize = 100

// Store holds operations by full name and serves them over the Operations gRPC API.
type Store struct {
	longrunningpb.UnimplementedOperationsServer
	mu  *lockmetrics.RWMutex
	ops map[string]*longrunningpb.Operation
}

// New creates an empty Store.
func New(name string) *Store {
	return &Store{mu: lockmetrics.New(name + ".lro"), ops: make(map[string]*longrunningpb.Operation)}
}

// Done builds a successfully finished operation.
func Done(name string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	m, err := anypb.New(meta)
	if err != nil {
		return nil, fmt.Errorf("operation metadata: %w", err)
	}

	r, err := anypb.New(resp)
	if err != nil {
		return nil, fmt.Errorf("operation response: %w", err)
	}

	return &longrunningpb.Operation{
		Name: name, Metadata: m, Done: true, Result: &longrunningpb.Operation_Response{Response: r},
	}, nil
}

func clone(op *longrunningpb.Operation) *longrunningpb.Operation {
	out, _ := proto.Clone(op).(*longrunningpb.Operation)

	return out
}

// Put records op and returns a copy of it.
func (s *Store) Put(op *longrunningpb.Operation) *longrunningpb.Operation {
	s.mu.Lock("Put")
	defer s.mu.Unlock()

	s.ops[op.GetName()] = clone(op)

	return clone(op)
}

// Lookup returns a copy of the named operation.
func (s *Store) Lookup(name string) (*longrunningpb.Operation, bool) {
	s.mu.RLock("Lookup")
	defer s.mu.RUnlock()

	op, ok := s.ops[name]
	if !ok {
		return nil, false
	}

	return clone(op), true
}

// AbortPending resolves every unfinished operation with st, for operations whose worker did not survive a restore.
func (s *Store) AbortPending(st *status.Status) {
	s.mu.Lock("AbortPending")
	defer s.mu.Unlock()

	for _, op := range s.ops {
		if !op.GetDone() {
			op.Done = true
			op.Result = &longrunningpb.Operation_Error{Error: st.Proto()}
		}
	}
}

// Reset drops every operation.
func (s *Store) Reset() {
	s.mu.Lock("Reset")
	defer s.mu.Unlock()

	s.ops = make(map[string]*longrunningpb.Operation)
}

// Snapshot serialises every operation as protojson.
func (s *Store) Snapshot() map[string]json.RawMessage {
	s.mu.RLock("Snapshot")
	defer s.mu.RUnlock()

	out := make(map[string]json.RawMessage, len(s.ops))

	for name, op := range s.ops {
		if raw, err := protojson.Marshal(op); err == nil {
			out[name] = raw
		}
	}

	return out
}

// Restore replaces the stored operations with a Snapshot result.
func (s *Store) Restore(snap map[string]json.RawMessage) error {
	ops := make(map[string]*longrunningpb.Operation, len(snap))

	for name, raw := range snap {
		op := &longrunningpb.Operation{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, op); err != nil {
			return fmt.Errorf("gcplro: restore operation %q: %w", name, err)
		}

		ops[name] = op
	}

	s.mu.Lock("Restore")
	defer s.mu.Unlock()

	s.ops = ops

	return nil
}

func notFound(name string) error {
	return status.Errorf(codes.NotFound, "Operation %q not found.", name)
}

// GetOperation returns the named operation.
func (s *Store) GetOperation(
	_ context.Context,
	in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	op, ok := s.Lookup(in.GetName())
	if !ok {
		return nil, notFound(in.GetName())
	}

	return op, nil
}

// WaitOperation returns the named operation at once because every operation is already finished.
func (s *Store) WaitOperation(
	_ context.Context,
	in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	op, ok := s.Lookup(in.GetName())
	if !ok {
		return nil, notFound(in.GetName())
	}

	return op, nil
}

// ListOperations lists the operations directly under in.Name ("<parent>/operations/<id>"), ordered by name.
func (s *Store) ListOperations(
	_ context.Context, in *longrunningpb.ListOperationsRequest,
) (*longrunningpb.ListOperationsResponse, error) {
	if in.GetFilter() != "" {
		return nil, status.Error(codes.InvalidArgument, "ListOperations filter is not supported.")
	}

	if in.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative.")
	}

	prefix := strings.TrimSuffix(in.GetName(), "/") + "/operations/"

	s.mu.RLock("ListOperations")

	names := make([]string, 0, len(s.ops))

	for name := range s.ops {
		if strings.HasPrefix(name, prefix) && !strings.Contains(name[len(prefix):], "/") {
			names = append(names, name)
		}
	}

	slices.Sort(names)

	all := make([]*longrunningpb.Operation, 0, len(names))
	for _, n := range names {
		all = append(all, clone(s.ops[n]))
	}

	s.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &longrunningpb.ListOperationsResponse{Operations: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteOperation removes the named operation.
func (s *Store) DeleteOperation(_ context.Context, in *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	s.mu.Lock("DeleteOperation")
	defer s.mu.Unlock()

	if _, ok := s.ops[in.GetName()]; !ok {
		return nil, notFound(in.GetName())
	}

	delete(s.ops, in.GetName())

	return &emptypb.Empty{}, nil
}

// CancelOperation succeeds without effect: a finished operation cannot be cancelled.
func (s *Store) CancelOperation(_ context.Context, in *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	if _, ok := s.Lookup(in.GetName()); !ok {
		return nil, notFound(in.GetName())
	}

	return &emptypb.Empty{}, nil
}
