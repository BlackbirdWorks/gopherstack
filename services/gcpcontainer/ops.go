//nolint:staticcheck // legacy request and operation fields are deprecated in the proto but still served
package gcpcontainer

import (
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const timeLayout = time.RFC3339Nano

func (b *InMemoryBackend) opName() string {
	var suffix [4]byte

	_, _ = rand.Read(suffix[:])

	return "operation-" + strconv.FormatInt(b.now().UnixMilli(), 10) + "-" + hex.EncodeToString(suffix[:])
}

// newOpLocked records an operation running until dur has passed on the backend clock, or until finishOpLocked
// when manual.
func (b *InMemoryBackend) newOpLocked(
	r ref, typ containerpb.Operation_Type, kind opKind, target string, dur time.Duration, manual bool,
) *opRec {
	now := b.now().UTC()
	name := b.opName()
	selfLink := apiHost + "/" + locationPath(r.project, r.location) + "/" + kindOps + "/" + name

	o := &opRec{
		Op: &containerpb.Operation{
			Name: name, Zone: r.location, Location: r.location, OperationType: typ,
			Status: containerpb.Operation_RUNNING, SelfLink: selfLink, TargetLink: target,
			StartTime: now.Format(timeLayout),
		},
		EndAt: now.Add(dur), Project: r.project, Location: r.location, Cluster: r.cluster, Pool: r.pool,
		Kind: kind, Manual: manual,
	}
	b.ops[opKey(r.project, r.location, name)] = o

	return o
}

func opKey(project, location, name string) string { return project + "/" + location + "/" + name }

func (b *InMemoryBackend) opDoneLocked(o *opRec) bool {
	return o.Finished || (!o.Manual && !b.now().Before(o.EndAt))
}

// finishOpLocked ends an operation now, recording failure when non-empty.
func (b *InMemoryBackend) finishOpLocked(o *opRec, failure string) {
	if o.Finished {
		return
	}

	o.Finished = true
	o.Failure = failure
	o.EndAt = b.now().UTC()
}

func (b *InMemoryBackend) opViewLocked(o *opRec) *containerpb.Operation {
	op := clone(o.Op)
	if !b.opDoneLocked(o) {
		return op
	}

	op.Status = containerpb.Operation_DONE
	op.EndTime = o.EndAt.UTC().Format(timeLayout)

	if o.Failure != "" {
		op.StatusMessage = o.Failure
		op.Error = status.New(codes.Internal, o.Failure).Proto()
	}

	return op
}

func (b *InMemoryBackend) activeOpsLocked(project, location, cluster string) []*opRec {
	var out []*opRec

	for _, o := range b.ops {
		if o.Project == project && o.Location == location && o.Cluster == cluster && !b.opDoneLocked(o) {
			out = append(out, o)
		}
	}

	slices.SortFunc(out, func(a, c *opRec) int { return a.EndAt.Compare(c.EndAt) })

	return out
}

// conflictLocked rejects an operation that cannot run next to the cluster's running ones. A node pool operation
// (pool != "") only conflicts with cluster-wide operations and operations on the same pool.
func (b *InMemoryBackend) conflictLocked(r ref, pool string) error {
	for _, o := range b.activeOpsLocked(r.project, r.location, r.cluster) {
		if o.Kind == kindPoolOp && pool != "" && o.Pool != pool {
			continue
		}

		return errBusy(r.clusterName(), o.Op.GetName())
	}

	return nil
}

// sweepLocked drops clusters and node pools whose delete operation has finished.
func (b *InMemoryBackend) sweepLocked() {
	for k, c := range b.clusters {
		if c.DeleteOp != "" && b.deleteDoneLocked(k, c.DeleteOp) {
			delete(b.clusters, k)

			continue
		}

		c.Pools = slices.DeleteFunc(c.Pools, func(p *poolRec) bool {
			return p.DeleteOp != "" && b.deleteDoneLocked(k, p.DeleteOp)
		})
	}
}

func (b *InMemoryBackend) deleteDoneLocked(clusterKey, opName string) bool {
	for _, o := range b.ops {
		if o.Op.GetName() == opName && o.Project+"/"+o.Location+"/"+o.Cluster == clusterKey {
			return b.opDoneLocked(o)
		}
	}

	return true
}

// GetOperation returns one operation.
func (b *InMemoryBackend) GetOperation(req *containerpb.GetOperationRequest) (*containerpb.Operation, error) {
	r, name, err := resolveOperation(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetOperationId())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	o, ok := b.ops[opKey(r.project, r.location, name)]
	if !ok {
		return nil, errNotFound(locationPath(r.project, r.location) + "/" + kindOps + "/" + name)
	}

	return b.opViewLocked(o), nil
}

// ListOperations lists the operations of a location, or of every location for "-".
func (b *InMemoryBackend) ListOperations(
	req *containerpb.ListOperationsRequest,
) (*containerpb.ListOperationsResponse, error) {
	project, location, err := resolveLocation(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	resp := &containerpb.ListOperationsResponse{}

	for _, o := range b.ops {
		if o.Project == project && (location == wildcard || o.Location == location) {
			resp.Operations = append(resp.Operations, b.opViewLocked(o))
		}
	}

	slices.SortFunc(resp.GetOperations(), func(a, c *containerpb.Operation) int {
		if d := strings.Compare(a.GetStartTime(), c.GetStartTime()); d != 0 {
			return d
		}

		return strings.Compare(a.GetName(), c.GetName())
	})

	return resp, nil
}
