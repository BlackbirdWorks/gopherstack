package gcpeventarc

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const (
	apiVersion            = "v1"
	defaultOperationDelay = time.Second
)

// AfterFunc runs f once after d and returns a function that cancels it.
type AfterFunc func(d time.Duration, f func()) (stop func())

func realAfter(d time.Duration, f func()) func() {
	t := time.AfterFunc(d, f)

	return func() { t.Stop() }
}

type pendingOp struct {
	stop func()
	done chan struct{}
	meta *eventarcpb.OperationMetadata
	resp proto.Message
}

// SetOperationDelay sets how long new long-running operations stay not done; zero finishes them at once.
func (b *InMemoryBackend) SetOperationDelay(d time.Duration) {
	b.opMu.Lock("SetOperationDelay")
	defer b.opMu.Unlock()

	b.opDelay = d
}

// SetAfterFunc replaces the timer used for operation completion and Advanced retries (tests inject a fake).
func (b *InMemoryBackend) SetAfterFunc(f AfterFunc) {
	b.opMu.Lock("SetAfterFunc")
	defer b.opMu.Unlock()

	b.after = f
}

func (b *InMemoryBackend) delay() (time.Duration, AfterFunc) {
	b.opMu.RLock("delay")
	defer b.opMu.RUnlock()

	return b.opDelay, b.after
}

// finish records the operation for a change that was applied: done at once without a delay, otherwise not done
// until the delay has elapsed on the injected timer.
func (b *InMemoryBackend) finish(
	project, location, target, verb string, resp proto.Message,
) (*longrunningpb.Operation, error) {
	now := b.ts()
	name := childName(project, location, "operations", uuid.NewString())
	meta := &eventarcpb.OperationMetadata{CreateTime: now, Target: target, Verb: verb, ApiVersion: apiVersion}

	delay, after := b.delay()
	if delay <= 0 {
		meta.EndTime = now

		return b.putDone(name, meta, resp)
	}

	m, err := anypb.New(meta)
	if err != nil {
		return nil, fmt.Errorf("record operation: %w", err)
	}

	p := &pendingOp{done: make(chan struct{}), meta: meta, resp: resp}

	b.opMu.Lock("finish")
	b.pendingOps[name] = p
	b.opMu.Unlock()

	op := b.ops.Put(&longrunningpb.Operation{Name: name, Metadata: m})
	stop := after(delay, func() { b.completeOp(name) })

	b.opMu.Lock("finish")
	if q, ok := b.pendingOps[name]; ok {
		q.stop = stop
	}
	b.opMu.Unlock()

	return op, nil
}

func (b *InMemoryBackend) putDone(name string, meta *eventarcpb.OperationMetadata, resp proto.Message) (
	*longrunningpb.Operation, error,
) {
	op, err := gcplro.Done(name, meta, resp)
	if err != nil {
		return nil, fmt.Errorf("record operation: %w", err)
	}

	return b.ops.Put(op), nil
}

func (b *InMemoryBackend) takePending(name string) *pendingOp {
	b.opMu.Lock("takePending")
	defer b.opMu.Unlock()

	p := b.pendingOps[name]
	delete(b.pendingOps, name)

	return p
}

func (b *InMemoryBackend) completeOp(name string) {
	p := b.takePending(name)
	if p == nil {
		return
	}

	p.meta.EndTime = b.ts()

	if _, err := b.putDone(name, p.meta, p.resp); err != nil {
		return
	}

	close(p.done)
}

// cancelOp finishes a not-done operation as CANCELLED; the change it describes is not rolled back.
func (b *InMemoryBackend) cancelOp(name string) error {
	if _, ok := b.ops.Lookup(name); !ok {
		return errNotFound("Operation %q not found.", name)
	}

	p := b.takePending(name)
	if p == nil {
		return nil
	}

	if p.stop != nil {
		p.stop()
	}

	p.meta.RequestedCancellation = true
	p.meta.EndTime = b.ts()

	m, err := anypb.New(p.meta)
	if err != nil {
		return fmt.Errorf("record operation: %w", err)
	}

	b.ops.Put(&longrunningpb.Operation{
		Name: name, Metadata: m, Done: true,
		Result: &longrunningpb.Operation_Error{Error: &rpcstatus.Status{
			Code: int32(codes.Canceled), Message: "Operation was cancelled.",
		}},
	})
	close(p.done)

	return nil
}

// stopOps cancels every timer and abandons not-done operations.
func (b *InMemoryBackend) stopOps() {
	b.opMu.Lock("stopOps")
	pending, timers := b.pendingOps, b.timers
	b.pendingOps = make(map[string]*pendingOp)
	b.timers = make(map[uint64]func())
	b.opMu.Unlock()

	for _, stop := range timers {
		stop()
	}

	for _, p := range pending {
		if p.stop != nil {
			p.stop()
		}

		close(p.done)
	}
}

type opsServer struct {
	*gcplro.Store

	b *InMemoryBackend
}

func (s opsServer) CancelOperation(
	_ context.Context,
	in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	if err := s.b.cancelOp(in.GetName()); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s opsServer) WaitOperation(ctx context.Context, in *longrunningpb.WaitOperationRequest) (
	*longrunningpb.Operation, error,
) {
	s.b.opMu.RLock("WaitOperation")
	p := s.b.pendingOps[in.GetName()]
	s.b.opMu.RUnlock()

	if p != nil {
		if d := in.GetTimeout().AsDuration(); d > 0 {
			var cancel context.CancelFunc

			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}

		select {
		case <-p.done:
		case <-ctx.Done():
		}
	}

	op, ok := s.b.ops.Lookup(in.GetName())
	if !ok {
		return nil, errNotFound("Operation %q not found.", in.GetName())
	}

	return op, nil
}
