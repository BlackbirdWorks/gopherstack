package gcpcompute

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	opPending = "PENDING"
	opRunning = "RUNNING"
	opDone    = "DONE"
	opUser    = "gopherstack@localhost"
	opHalf    = 50
	opFull    = 100
)

type target struct {
	project    string
	scope      string
	loc        string
	collection string
	name       string
	id         uint64
}

func (b *InMemoryBackend) newOpLocked(opType string, t target, done time.Duration) *computeapi.Operation {
	now := b.now().UTC()

	var suffix [4]byte

	_, _ = rand.Read(suffix[:])

	name := "operation-" + strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(suffix[:])
	op := &computeapi.Operation{
		Kind: "compute#operation", Id: newID(), Name: name, OperationType: opType, Status: opPending,
		User: opUser, TargetLink: link(t.project, t.scope, t.loc, t.collection, t.name), TargetId: t.id,
		InsertTime: now.Format(timeLayout), CreationTimestamp: now.Format(timeLayout),
		SelfLink: link(t.project, t.scope, t.loc, "operations", name),
	}

	switch t.scope {
	case scopeZone:
		op.Zone = scopeURL(t.project, scopeZone, t.loc)
	case scopeRegion:
		op.Region = scopeURL(t.project, scopeRegion, t.loc)
	}

	b.ops[key(t.project, t.scope, t.loc, name)] = &opRec{
		Op: op, Project: t.project, Created: now, Pending: b.timing.Pending, Done: max(done, b.timing.Pending),
	}

	return b.opViewLocked(b.ops[key(t.project, t.scope, t.loc, name)])
}

func (b *InMemoryBackend) opViewLocked(r *opRec) *computeapi.Operation {
	op := clone(r.Op)
	el := b.now().UTC().Sub(r.Created)

	switch {
	case el < r.Pending:
		op.Status, op.Progress = opPending, 0
	case el < r.Done:
		op.Status, op.Progress = opRunning, opHalf
		op.StartTime = r.Created.Add(r.Pending).Format(timeLayout)
	default:
		op.Status, op.Progress = opDone, opFull
		op.StartTime = r.Created.Add(r.Pending).Format(timeLayout)
		op.EndTime = r.Created.Add(r.Done).Format(timeLayout)
	}

	return op
}

func (b *InMemoryBackend) getOpLocked(project, scope, loc, name string) (*opRec, *APIError) {
	r, ok := b.ops[key(project, scope, loc, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scope, loc, "operations", name))
	}

	return r, nil
}

func (b *InMemoryBackend) getOperation(project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	r, err := b.getOpLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	return b.opViewLocked(r), nil
}

func (b *InMemoryBackend) deleteOperation(project, scope, loc, name string) error {
	b.mu.Lock("DeleteOperation")
	defer b.mu.Unlock()

	if _, err := b.getOpLocked(project, scope, loc, name); err != nil {
		return err
	}

	delete(b.ops, key(project, scope, loc, name))

	return nil
}

func (b *InMemoryBackend) listOperations(project, scope, loc string) []entry {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	var out []entry

	for _, r := range b.ops {
		if r.Project != project || !inScope(r.Op, scope, loc) {
			continue
		}

		out = append(out, entry{scope: opScopeKey(r.Op), item: b.opViewLocked(r)})
	}

	return out
}

func inScope(op *computeapi.Operation, scope, loc string) bool {
	switch scope {
	case "":
		return true
	case scopeGlobal:
		return op.Zone == "" && op.Region == ""
	case scopeZone:
		return lastSegment(op.Zone) == loc
	case scopeRegion:
		return lastSegment(op.Region) == loc
	}

	return false
}

func opScopeKey(op *computeapi.Operation) string {
	switch {
	case op.Zone != "":
		return scopeZone + "/" + lastSegment(op.Zone)
	case op.Region != "":
		return scopeRegion + "/" + lastSegment(op.Region)
	}

	return scopeGlobal
}

// waitOperation blocks on the backend clock until the operation is DONE and returns it.
func (b *InMemoryBackend) waitOperation(ctx context.Context, project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.RLock("WaitOperation")
	r, err := b.getOpLocked(project, scope, loc, name)

	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	op := b.opViewLocked(r)
	remaining := r.Created.Add(r.Done).Sub(b.now().UTC())
	sleep := b.sleep
	b.mu.RUnlock()

	if op.Status == opDone {
		return op, nil
	}

	sleep(ctx, remaining)

	return b.getOperation(project, scope, loc, name)
}
