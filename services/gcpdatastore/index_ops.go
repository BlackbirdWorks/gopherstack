package gcpdatastore

import (
	"context"
	"strconv"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const defaultIndexBuildDelay = time.Second

// AfterFunc runs f once after d and returns a function that cancels it.
type AfterFunc func(d time.Duration, f func()) (stop func())

func realAfter(d time.Duration, f func()) func() {
	t := time.AfterFunc(d, f)

	return func() { t.Stop() }
}

type pendingIndexOp struct {
	stop func()
	done chan struct{}
	meta *adminpb.IndexOperationMetadata
	key  string
	del  bool
}

// SetIndexBuild sets how long CreateIndex and DeleteIndex stay not done and the timer that finishes them
// (nil keeps the wall clock). A zero delay finishes them at once.
func (b *InMemoryBackend) SetIndexBuild(delay time.Duration, after AfterFunc) {
	b.mu.Lock("SetIndexBuild")
	defer b.mu.Unlock()

	b.indexDelay = delay
	if after != nil {
		b.after = after
	}
}

func (b *InMemoryBackend) indexOpNameLocked(project string) string {
	b.exportSeq++

	return "projects/" + project + "/operations/" + strconv.FormatInt(b.wallLocked().UnixNano(), 36) +
		strconv.FormatInt(b.exportSeq, 36)
}

// startIndexOpLocked records the operation of a create (del false) or delete of idx: finished at once without a
// delay, otherwise not done until the delay has elapsed. The caller has already put idx in its in-flight state.
func (b *InMemoryBackend) startIndexOpLocked(
	project, key string, del bool, idx *adminpb.Index,
) (*longrunningpb.Operation, error) {
	opType := adminpb.OperationType_CREATE_INDEX
	if del {
		opType = adminpb.OperationType_DELETE_INDEX
	}

	now := timestamppb.New(b.wallLocked())
	name := b.indexOpNameLocked(project)
	meta := &adminpb.IndexOperationMetadata{
		Common: &adminpb.CommonMetadata{
			StartTime: now, OperationType: opType, State: adminpb.CommonMetadata_PROCESSING,
		},
		IndexId: idx.GetIndexId(),
	}

	if b.indexDelay <= 0 {
		b.settleLocked(key, del)
		meta.Common.EndTime, meta.Common.State = now, adminpb.CommonMetadata_SUCCESSFUL

		return b.putIndexOpDone(name, meta, b.indexResultLocked(key, del, idx))
	}

	m, err := anypb.New(meta)
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	p := &pendingIndexOp{done: make(chan struct{}), meta: meta, key: key, del: del}
	p.stop = b.after(b.indexDelay, func() { b.completeIndexOp(name) })
	b.pendingIdx[name] = p

	return b.ops.Put(&longrunningpb.Operation{Name: name, Metadata: m}), nil
}

func (b *InMemoryBackend) putIndexOpDone(
	name string, meta *adminpb.IndexOperationMetadata, idx *adminpb.Index,
) (*longrunningpb.Operation, error) {
	o, err := gcplro.Done(name, meta, idx)
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	return b.ops.Put(o), nil
}

// settleLocked applies the end state of a finished create (READY) or delete (index removed).
func (b *InMemoryBackend) settleLocked(key string, del bool) {
	if del {
		return
	}

	if idx, ok := b.indexes[key]; ok {
		idx.State = adminpb.Index_READY
	}
}

func (b *InMemoryBackend) indexResultLocked(key string, del bool, fallback *adminpb.Index) *adminpb.Index {
	if del {
		delete(b.indexes, key)

		return fallback
	}

	if idx, ok := b.indexes[key]; ok {
		return cloneIndex(idx)
	}

	return fallback
}

func (b *InMemoryBackend) completeIndexOp(name string) {
	b.mu.Lock("completeIndexOp")
	defer b.mu.Unlock()

	p, ok := b.pendingIdx[name]
	if !ok {
		return
	}

	delete(b.pendingIdx, name)
	close(p.done)

	var fallback *adminpb.Index
	if idx, found := b.indexes[p.key]; found {
		fallback = cloneIndex(idx)
	}

	b.settleLocked(p.key, p.del)

	res := b.indexResultLocked(p.key, p.del, fallback)
	if res == nil {
		return
	}

	p.meta.Common.EndTime, p.meta.Common.State = timestamppb.New(b.wallLocked()), adminpb.CommonMetadata_SUCCESSFUL

	if _, live := b.ops.Lookup(name); live {
		_, _ = b.putIndexOpDone(name, p.meta, res)
	}
}

// cancelIndexOp finishes a not-done operation as CANCELLED and leaves its index in ERROR, the documented state of
// an index whose create or delete went wrong. A finished operation is left alone.
func (b *InMemoryBackend) cancelIndexOp(name string) error {
	if _, ok := b.ops.Lookup(name); !ok {
		return errNotFound("Operation %q not found.", name)
	}

	b.mu.Lock("cancelIndexOp")
	defer b.mu.Unlock()

	p, ok := b.pendingIdx[name]
	if !ok {
		return nil
	}

	p.stop()
	delete(b.pendingIdx, name)
	close(p.done)

	if idx, found := b.indexes[p.key]; found {
		idx.State = adminpb.Index_ERROR
	}

	p.meta.Common.EndTime, p.meta.Common.State = timestamppb.New(b.wallLocked()), adminpb.CommonMetadata_CANCELLED

	m, err := anypb.New(p.meta)
	if err != nil {
		return errInvalid("%v", err)
	}

	b.ops.Put(&longrunningpb.Operation{
		Name: name, Metadata: m, Done: true,
		Result: &longrunningpb.Operation_Error{Error: &rpcstatus.Status{
			Code: int32(codes.Canceled), Message: "Operation was cancelled.",
		}},
	})

	return nil
}

// stopPendingIndexOps cancels every timer; the in-flight indexes stay as they are until Reset or Restore.
func (b *InMemoryBackend) stopPendingIndexOps() {
	b.mu.Lock("stopPendingIndexOps")
	defer b.mu.Unlock()

	for name, p := range b.pendingIdx {
		p.stop()
		close(p.done)
		delete(b.pendingIdx, name)
	}
}

// failInFlightIndexesLocked marks CREATING and DELETING indexes ERROR and aborts their operations, for state that
// has no worker behind it (restore after a restart).
func (b *InMemoryBackend) failInFlightIndexesLocked() {
	for _, idx := range b.indexes {
		if idx.GetState() == adminpb.Index_CREATING || idx.GetState() == adminpb.Index_DELETING {
			idx.State = adminpb.Index_ERROR
		}
	}
}

type opsServer struct {
	*gcplro.Store

	b *InMemoryBackend
}

func (s opsServer) CancelOperation(
	_ context.Context, in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	if err := s.b.cancelIndexOp(in.GetName()); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s opsServer) WaitOperation(
	ctx context.Context, in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	s.b.mu.RLock("WaitOperation")
	p := s.b.pendingIdx[in.GetName()]
	s.b.mu.RUnlock()

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
