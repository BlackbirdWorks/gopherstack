package gcpartifactregistry

import (
	"context"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

// pendingOp is an operation that has not finished yet; finish produces its result once the clock reaches at.
type pendingOp struct {
	at         time.Time
	meta       proto.Message
	finish     func() (proto.Message, error)
	cancelable bool
}

// SetOperationDelay makes every long-running operation start not-done and finish that long after it was
// started on the backend clock; zero (the default) finishes them before the call returns.
func (b *InMemoryBackend) SetOperationDelay(d time.Duration) {
	b.mu.Lock("SetOperationDelay")
	defer b.mu.Unlock()

	b.opDelay = d
}

// startOp records an operation under parent. finish computes the response (and applies its side effects) when
// the operation completes: at once without a delay, else when a read of the operation finds the clock past it.
func (b *InMemoryBackend) startOp(
	parent string, meta proto.Message, cancelable bool, finish func() (proto.Message, error),
) (*longrunningpb.Operation, error) {
	name := parent + "/operations/operation-" + uuid.NewString()

	m, err := anypb.New(meta)
	if err != nil {
		return nil, errInternal(err)
	}

	b.mu.Lock("startOp")
	delay := b.opDelay

	if delay > 0 {
		b.pending[name] = &pendingOp{at: b.now().Add(delay), meta: meta, finish: finish, cancelable: cancelable}
	}
	b.mu.Unlock()

	op := &longrunningpb.Operation{Name: name, Metadata: m}
	if delay <= 0 {
		completeOp(op, finish)
	}

	return b.ops.Put(op), nil
}

func completeOp(op *longrunningpb.Operation, finish func() (proto.Message, error)) {
	op.Done = true

	resp, err := finish()
	if err == nil {
		var a *anypb.Any

		if a, err = anypb.New(resp); err == nil {
			op.Result = &longrunningpb.Operation_Response{Response: a}

			return
		}
	}

	op.Result = &longrunningpb.Operation_Error{Error: errStatus(err)}
}

// settle completes every pending operation whose time has come.
func (b *InMemoryBackend) settle() {
	b.settleMu.Lock("settle")
	defer b.settleMu.Unlock()

	b.mu.Lock("settle")
	now := b.now()
	due := map[string]*pendingOp{}

	for name, p := range b.pending {
		if !p.at.After(now) {
			due[name] = p

			delete(b.pending, name)
		}
	}
	b.mu.Unlock()

	for name, p := range due {
		b.finishPending(name, p)
	}
}

func (b *InMemoryBackend) finishPending(name string, p *pendingOp) {
	op, ok := b.ops.Lookup(name)
	if !ok {
		return
	}

	completeOp(op, p.finish)
	b.ops.Put(op)
}

func (b *InMemoryBackend) cancelPending(name string) {
	b.settleMu.Lock("cancelPending")
	defer b.settleMu.Unlock()

	b.mu.Lock("cancelPending")
	p, ok := b.pending[name]

	if ok && p.cancelable {
		delete(b.pending, name)
	} else {
		ok = false
	}
	b.mu.Unlock()

	if !ok {
		return
	}

	if op, found := b.ops.Lookup(name); found {
		op.Done = true
		op.Result = &longrunningpb.Operation_Error{Error: &status.Status{
			Code: int32(codes.Canceled), Message: "Operation was cancelled.",
		}}
		b.ops.Put(op)
	}
}

// opsServer serves google.longrunning.Operations, finishing due operations before every read.
type opsServer struct {
	*gcplro.Store
	b *InMemoryBackend
}

func (s opsServer) GetOperation(
	ctx context.Context,
	in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	s.b.settle()

	return s.Store.GetOperation(ctx, in)
}

func (s opsServer) WaitOperation(
	ctx context.Context,
	in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	s.b.settle()

	return s.Store.WaitOperation(ctx, in)
}

func (s opsServer) ListOperations(
	ctx context.Context,
	in *longrunningpb.ListOperationsRequest,
) (*longrunningpb.ListOperationsResponse, error) {
	s.b.settle()

	return s.Store.ListOperations(ctx, in)
}

func (s opsServer) CancelOperation(
	ctx context.Context,
	in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	s.b.settle()
	s.b.cancelPending(in.GetName())

	return s.Store.CancelOperation(ctx, in)
}

func (s opsServer) DeleteOperation(
	ctx context.Context,
	in *longrunningpb.DeleteOperationRequest,
) (*emptypb.Empty, error) {
	s.b.settleNow(in.GetName())

	return s.Store.DeleteOperation(ctx, in)
}

// settleNow finishes one pending operation regardless of the clock, so deleting its record keeps its effect.
func (b *InMemoryBackend) settleNow(name string) {
	b.settleMu.Lock("settleNow")
	defer b.settleMu.Unlock()

	b.mu.Lock("settleNow")
	p, ok := b.pending[name]
	delete(b.pending, name)
	b.mu.Unlock()

	if ok {
		b.finishPending(name, p)
	}
}

func metaOp() proto.Message { return &artifactregistrypb.OperationMetadata{} }
