package gcpspanner

import (
	"context"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// operationsServer serves the backend's own operations and forwards the rest (backups and other emulator
// work) to the emulator when one is running.
type operationsServer struct {
	longrunningpb.UnimplementedOperationsServer
	b *InMemoryBackend
}

func (s *operationsServer) remote(err error) (longrunningpb.OperationsClient, bool) {
	if status.Code(err) != codes.NotFound {
		return nil, false
	}

	cc := s.b.emu.current()
	if cc == nil {
		return nil, false
	}

	return longrunningpb.NewOperationsClient(cc), true
}

func (s *operationsServer) GetOperation(
	ctx context.Context, in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	op, err := s.b.ops.GetOperation(ctx, in)
	if rc, ok := s.remote(err); ok {
		return rc.GetOperation(forwardCtx(ctx), in)
	}

	return op, err
}

func (s *operationsServer) WaitOperation(
	ctx context.Context, in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	op, err := s.b.ops.WaitOperation(ctx, in)
	if rc, ok := s.remote(err); ok {
		return rc.WaitOperation(forwardCtx(ctx), in)
	}

	return op, err
}

func (s *operationsServer) DeleteOperation(
	ctx context.Context, in *longrunningpb.DeleteOperationRequest,
) (*emptypb.Empty, error) {
	out, err := s.b.ops.DeleteOperation(ctx, in)
	if rc, ok := s.remote(err); ok {
		return rc.DeleteOperation(forwardCtx(ctx), in)
	}

	return out, err
}

func (s *operationsServer) CancelOperation(
	ctx context.Context, in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	out, err := s.b.ops.CancelOperation(ctx, in)
	if rc, ok := s.remote(err); ok {
		return rc.CancelOperation(forwardCtx(ctx), in)
	}

	return out, err
}

// ListOperations lists local operations; a database-scoped list with none falls through to the emulator.
func (s *operationsServer) ListOperations(
	ctx context.Context, in *longrunningpb.ListOperationsRequest,
) (*longrunningpb.ListOperationsResponse, error) {
	resp, err := s.b.ops.ListOperations(ctx, in)
	if err != nil || len(resp.GetOperations()) > 0 || !strings.Contains(in.GetName(), "/databases/") {
		return resp, err
	}

	if cc := s.b.emu.current(); cc != nil {
		return longrunningpb.NewOperationsClient(cc).ListOperations(forwardCtx(ctx), in)
	}

	return resp, nil
}
