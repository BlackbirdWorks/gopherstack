package gcpsql

import (
	"context"
	"errors"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxOps         = 2000
	defaultPage    = 100
	errKind        = "sql#operationError"
	errorsKind     = "sql#operationErrors"
	opErrorDefault = "UNKNOWN"
)

var errCancelled = errors.New("operation was cancelled")

type opMeta struct {
	export map[string]any
	imp    map[string]any
	backup map[string]any
}

func (b *InMemoryBackend) newOpLocked(project, instance, opType string, m opMeta) *Operation {
	now := stamp(b.now())
	name := uuid.NewString()
	op := &Operation{
		Kind: kindOperation, Name: name, OperationType: opType, Status: opPending, TargetID: instance,
		TargetProject: project, InsertTime: now, User: "gopherstack@localhost",
		TargetLink: instanceLink(
			project,
			instance,
		), SelfLink: baseLink + "/projects/" + project + "/operations/" + name,
		ExportContext: m.export, ImportContext: m.imp, BackupContext: m.backup,
	}
	b.seq++
	op.seq = b.seq
	b.ops[instKey(project, name)] = op
	b.pruneOpsLocked()

	return op
}

func (b *InMemoryBackend) pruneOpsLocked() {
	if len(b.ops) <= maxOps {
		return
	}

	done := make([]*Operation, 0, len(b.ops))

	for _, o := range b.ops {
		if o.Status == opDone {
			done = append(done, o)
		}
	}

	sort.Slice(done, func(i, j int) bool { return done[i].InsertTime < done[j].InsertTime })

	for _, o := range done[:min(len(done), len(b.ops)-maxOps)] {
		delete(b.ops, instKey(o.TargetProject, o.Name))
	}
}

// submitLocked records an operation and queues fn on rec; it returns a copy of the pending operation.
func (b *InMemoryBackend) submitLocked(
	rec *instRec, project, opType string, m opMeta, fn func(ctx context.Context) error,
) *Operation {
	op := b.newOpLocked(project, rec.Inst.Name, opType, m)
	b.enqueueLocked(rec, task{op: op.Name, fn: fn})

	cp := *op

	return &cp
}

func (b *InMemoryBackend) enqueueLocked(rec *instRec, t task) {
	rec.queue = append(rec.queue, t)
	if rec.draining {
		return
	}

	rec.draining = true

	b.wg.Add(1)

	go b.drain(rec)
}

func (b *InMemoryBackend) drain(rec *instRec) {
	defer b.wg.Done()

	for {
		b.mu.Lock("drain")

		if len(rec.queue) == 0 {
			rec.draining = false
			b.mu.Unlock()

			return
		}

		t := rec.queue[0]
		rec.queue = rec.queue[1:]
		ctx := b.runCtx
		b.mu.Unlock()

		b.runTask(ctx, rec, t)
	}
}

func (b *InMemoryBackend) runTask(ctx context.Context, rec *instRec, t task) {
	if t.op == "" {
		_ = t.fn(ctx)

		return
	}

	project := rec.Inst.Project

	b.mu.Lock("runTask")

	op := b.ops[instKey(project, t.op)]
	if op == nil || op.Status != opPending {
		b.mu.Unlock()

		return
	}

	op.Status, op.StartTime = opRunning, stamp(b.now())
	b.mu.Unlock()

	err := t.fn(ctx)

	b.mu.Lock("runTask")
	op.Status, op.EndTime = opDone, stamp(b.now())

	if err == nil && len(rec.Inst.ReplicaNames) > 0 {
		b.syncReplicasLocked(rec)
	}

	if err != nil {
		op.Error = opError(err)
		logger.Load(ctx).WarnContext(ctx, "gcpsql: operation failed", "operation", op.OperationType,
			"instance", op.TargetID, "error", err)
	}
	b.mu.Unlock()
}

func opError(err error) *OperationErrors {
	code := opErrorDefault

	if ae, ok := errors.AsType[*APIError](err); ok {
		code = ae.Reason
	}

	return &OperationErrors{
		Kind:   errorsKind,
		Errors: []OperationError{{Kind: errKind, Code: code, Message: err.Error()}},
	}
}

// GetOperation returns an operation.
func (b *InMemoryBackend) GetOperation(project, name string) (*Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	op, ok := b.ops[instKey(project, name)]
	if !ok {
		return nil, errNotFound("Operation %q does not exist.", name)
	}

	cp := *op

	return &cp, nil
}

// ListOperations lists operations of a project, newest first, optionally of one instance.
func (b *InMemoryBackend) ListOperations(project, instance, token string, limit int) page.Page[*Operation] {
	b.mu.RLock("ListOperations")

	var all []*Operation

	for _, op := range b.ops {
		if op.TargetProject == project && (instance == "" || op.TargetID == instance) {
			cp := *op
			all = append(all, &cp)
		}
	}
	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool {
		if all[i].InsertTime != all[j].InsertTime {
			return all[i].InsertTime > all[j].InsertTime
		}

		if all[i].seq != all[j].seq {
			return all[i].seq > all[j].seq
		}

		return all[i].Name < all[j].Name
	})

	return page.New(all, token, limit, defaultPage)
}

// CancelOperation cancels an operation that has not started; running or finished operations are unchanged.
func (b *InMemoryBackend) CancelOperation(project, name string) error {
	b.mu.Lock("CancelOperation")
	defer b.mu.Unlock()

	op, ok := b.ops[instKey(project, name)]
	if !ok {
		return errNotFound("Operation %q does not exist.", name)
	}

	if op.Status == opPending && (op.OperationType == "IMPORT" || op.OperationType == "EXPORT") {
		op.Status, op.EndTime = opDone, stamp(b.now())
		op.Error = opError(errCancelled)
	}

	return nil
}
