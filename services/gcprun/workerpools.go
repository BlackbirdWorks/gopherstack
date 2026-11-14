package gcprun

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

const (
	defaultPoolInstances = 1
	maxDescriptionLen    = 512
)

type poolRec struct {
	pb      *runpb.WorkerPool
	failure string
	revSeq  int
	pending int
}

func workerPoolMutable() []string {
	return commonMutable("description", "instance_splits", "scaling", "custom_audiences")
}

// poolInstanceCount is the number of instances each serving revision of the pool runs.
func poolInstanceCount(pb *runpb.WorkerPool) int32 {
	if sc := pb.GetScaling(); sc != nil && sc.ManualInstanceCount != nil {
		return sc.GetManualInstanceCount()
	}

	return defaultPoolInstances
}

func shapeOf(cs []*runpb.Container, vols []*runpb.Volume) *runpb.RevisionTemplate {
	return &runpb.RevisionTemplate{Containers: cs, Volumes: vols}
}

func validateWorkerPool(wp *runpb.WorkerPool) error {
	t := wp.GetTemplate()
	if t == nil {
		return errInvalid("template is required.")
	}

	if len(wp.GetDescription()) > maxDescriptionLen {
		return errInvalid("description is limited to %d characters.", maxDescriptionLen)
	}

	if wp.GetScaling().ManualInstanceCount != nil && wp.GetScaling().GetManualInstanceCount() < 0 {
		return errInvalid("manual_instance_count must not be negative.")
	}

	if err := validateContainers(t.GetContainers(), true); err != nil {
		return err
	}

	return validateRuntimeShape(shapeOf(t.GetContainers(), t.GetVolumes()))
}

func newPoolRevisionLocked(
	rec *poolRec, n resName, tmpl *runpb.WorkerPoolRevisionTemplate, now *timestamppb.Timestamp,
) (*revRec, error) {
	id := n.ids[0]
	rec.revSeq++

	short := tmpl.GetRevision()
	if short == "" {
		short = fmt.Sprintf("%s-%05d-%s", id, rec.revSeq, shortHash(rec.pb.GetUid(), strconv.Itoa(rec.revSeq))[:3])
	} else if !strings.HasPrefix(short, id+"-") {
		return nil, errInvalid("Revision name %q must be prefixed with the worker pool name %q.", short, id)
	}

	uid := newUID()
	pb := &runpb.Revision{
		Name:                          n.parent() + "/workerPools/" + id + "/revisions/" + short,
		Uid:                           uid,
		Generation:                    1,
		Labels:                        tmpl.GetLabels(),
		Annotations:                   tmpl.GetAnnotations(),
		CreateTime:                    now,
		UpdateTime:                    now,
		LaunchStage:                   rec.pb.GetLaunchStage(),
		VpcAccess:                     tmpl.GetVpcAccess(),
		ServiceAccount:                tmpl.GetServiceAccount(),
		Containers:                    tmpl.GetContainers(),
		Volumes:                       tmpl.GetVolumes(),
		EncryptionKey:                 tmpl.GetEncryptionKey(),
		EncryptionKeyRevocationAction: tmpl.GetEncryptionKeyRevocationAction(),
		EncryptionKeyShutdownDuration: tmpl.GetEncryptionKeyShutdownDuration(),
		NodeSelector:                  tmpl.GetNodeSelector(),
		GpuZonalRedundancyDisabled:    tmpl.GpuZonalRedundancyDisabled,
		Reconciling:                   true,
		ObservedGeneration:            1,
		Conditions: []*runpb.Condition{
			newCondition(condReady, runpb.Condition_CONDITION_RECONCILING, "Revision is deploying.", now),
		},
	}
	pb.Etag = etag(uid, 1, now)

	return &revRec{pb: pb, seq: rec.revSeq}, nil
}

func (b *InMemoryBackend) buildPoolLocked(n resName, req *runpb.WorkerPool) (*poolRec, *revRec, error) {
	now := b.ts()
	pb := clone(req)
	pb.Name = n.parent() + "/workerPools/" + n.ids[0]
	pb.Uid, pb.Generation, pb.CreateTime, pb.UpdateTime = newUID(), 1, now, now
	pb.DeleteTime, pb.ExpireTime = nil, nil
	pb.LatestReadyRevision, pb.InstanceSplitStatuses = "", nil

	rec := &poolRec{pb: pb}

	rev, err := newPoolRevisionLocked(rec, n, pb.GetTemplate(), now)
	if err != nil {
		return nil, nil, err
	}

	known := map[string]bool{lastSegment(rev.pb.GetName()): true}
	if pb.InstanceSplits, err = normalizeSplits(req.GetInstanceSplits(), known); err != nil {
		return nil, nil, err
	}

	pb.LatestCreatedRevision = rev.pb.GetName()
	rec.pending++
	b.refreshPoolLocked(rec)

	return rec, rev, nil
}

func splitTypeOf(s *runpb.InstanceSplit) runpb.InstanceSplitAllocationType {
	switch {
	case s.GetType() != runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_UNSPECIFIED:
		return s.GetType()
	case s.GetRevision() != "":
		return runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_REVISION
	default:
		return runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_LATEST
	}
}

// normalizeSplits validates instance splits against known revision short names and fills types and the default.
func normalizeSplits(in []*runpb.InstanceSplit, known map[string]bool) ([]*runpb.InstanceSplit, error) {
	if len(in) == 0 {
		return []*runpb.InstanceSplit{{
			Type:    runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_LATEST,
			Percent: fullPercent,
		}}, nil
	}

	var (
		sum    int32
		latest bool
	)

	out := make([]*runpb.InstanceSplit, 0, len(in))

	for _, s := range in {
		s = clone(s)
		s.Type = splitTypeOf(s)

		if s.GetPercent() < 0 || s.GetPercent() > fullPercent {
			return nil, errInvalid("Instance split percent %d is out of range.", s.GetPercent())
		}

		if s.GetType() == runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_REVISION {
			if !known[s.GetRevision()] {
				return nil, errInvalid("Revision %q referenced by instance splits does not exist.", s.GetRevision())
			}
		} else {
			if latest {
				return nil, errInvalid("Only one instance split may point at the latest revision.")
			}

			latest, s.Revision = true, ""
		}

		sum += s.GetPercent()
		out = append(out, s)
	}

	if sum != fullPercent {
		return nil, errInvalid("Instance split percentages must sum to 100, got %d.", sum)
	}

	return out, nil
}

func splitStatuses(splits []*runpb.InstanceSplit, latestReady string) []*runpb.InstanceSplitStatus {
	out := make([]*runpb.InstanceSplitStatus, 0, len(splits))

	for _, s := range splits {
		st := &runpb.InstanceSplitStatus{Type: s.GetType(), Revision: s.GetRevision(), Percent: s.GetPercent()}

		if s.GetType() == runpb.InstanceSplitAllocationType_INSTANCE_SPLIT_ALLOCATION_TYPE_LATEST {
			if latestReady == "" {
				continue
			}

			st.Revision = latestReady
		}

		out = append(out, st)
	}

	return out
}

// refreshPoolLocked recomputes the output-only status fields of a worker pool.
func (b *InMemoryBackend) refreshPoolLocked(rec *poolRec) {
	pb, at := rec.pb, b.ts()

	pb.InstanceSplitStatuses = splitStatuses(pb.GetInstanceSplits(), lastSegment(pb.GetLatestReadyRevision()))
	pb.Reconciling = rec.pending > 0

	state, msg := runpb.Condition_CONDITION_SUCCEEDED, ""

	switch {
	case rec.pending > 0:
		state, msg = runpb.Condition_CONDITION_RECONCILING, "Deploying revision."
	case rec.failure != "":
		state, msg = runpb.Condition_CONDITION_FAILED, rec.failure
	default:
		pb.ObservedGeneration = pb.GetGeneration()
	}

	ready := newCondition(condReady, state, msg, at)
	pb.TerminalCondition = ready
	pb.Conditions = []*runpb.Condition{ready, newCondition("InstanceSplitsReady", state, msg, at)}
	pb.UpdateTime = at
	pb.Etag = etag(pb.GetUid(), pb.GetGeneration(), at)
}

func (b *InMemoryBackend) serviceNameTakenLocked(n resName) bool {
	_, taken := b.services[n.parent()+"/services/"+n.ids[0]]

	return taken
}

// CreateWorkerPool stores a worker pool with its first revision and starts its instances in the background.
func (b *InMemoryBackend) CreateWorkerPool(in *runpb.CreateWorkerPoolRequest) (*longrunningpb.Operation, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateID(in.GetWorkerPoolId(), "worker_pool_id"); err != nil {
		return nil, err
	}

	if err = validateWorkerPool(in.GetWorkerPool()); err != nil {
		return nil, err
	}

	n := resName{project: p.project, location: p.location, ids: []string{in.GetWorkerPoolId()}}
	name := n.parent() + "/workerPools/" + n.ids[0]

	b.mu.Lock("CreateWorkerPool")

	if _, exists := b.workerPools[name]; exists {
		b.mu.Unlock()

		return nil, errExists("Resource '%s' already exists.", n.ids[0])
	}

	if b.serviceNameTakenLocked(n) {
		b.mu.Unlock()

		return nil, errExists("A service named '%s' already exists; a worker pool cannot reuse its name.", n.ids[0])
	}

	rec, rev, err := b.buildPoolLocked(n, in.GetWorkerPool())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() {
		view := clone(rec.pb)
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	b.workerPools[name], b.revisions[rev.pb.GetName()] = rec, rev
	view := clone(rec.pb)
	b.mu.Unlock()

	return b.beginPoolDeploy(n.parent(), name, rev.pb.GetName(), view)
}

func (b *InMemoryBackend) beginPoolDeploy(
	opParent, poolName, revName string, meta *runpb.WorkerPool,
) (*longrunningpb.Operation, error) {
	name := opName(opParent)

	op, err := b.pendingOp(name, meta)
	if err != nil {
		return nil, err
	}

	b.wg.Go(func() { b.deployPool(name, poolName, revName) })

	return op, nil
}

func (b *InMemoryBackend) deployPool(op, poolName, revName string) {
	err := b.ensure(b.baseCtx(), revName, b.poolCount(poolName))

	b.mu.Lock("deployPool")

	rec, rev := b.workerPools[poolName], b.revisions[revName]
	if rec == nil || rev == nil {
		if rec != nil {
			rec.pending--
			b.refreshPoolLocked(rec)
		}
		b.mu.Unlock()
		gone := status.New(codes.Aborted, "Worker pool was deleted during deployment.")
		b.failOp(op, &runpb.WorkerPool{Name: poolName}, gone)

		return
	}

	failure := b.settlePoolLocked(rec, rev, err)
	view := clone(rec.pb)
	idle := b.idlePoolRevsLocked(rec)
	b.mu.Unlock()

	for _, p := range idle {
		p.Drain()
	}

	b.warmWorkerPool(poolName)

	if failure != "" {
		b.failOp(op, view, status.New(codes.FailedPrecondition, failure))

		return
	}

	b.finishOp(op, view, view)
}

func (b *InMemoryBackend) poolCount(poolName string) int {
	b.mu.RLock("poolCount")
	defer b.mu.RUnlock()

	if rec := b.workerPools[poolName]; rec != nil {
		return int(poolInstanceCount(rec.pb))
	}

	return 0
}

// settlePoolLocked records the outcome of starting rev and returns a failure message, empty on success.
func (b *InMemoryBackend) settlePoolLocked(rec *poolRec, rev *revRec, err error) string {
	rec.pending--
	failure := b.settleRevision(rev, err, "run", "")

	if failure == "" {
		rec.failure = ""

		if cur := b.revisions[rec.pb.GetLatestReadyRevision()]; cur == nil || rev.seq >= cur.seq {
			rec.pb.LatestReadyRevision = rev.pb.GetName()
		}
	} else if rev.pb.GetName() == rec.pb.GetLatestCreatedRevision() {
		rec.failure = failure
	}

	b.refreshPoolLocked(rec)

	return failure
}

func (b *InMemoryBackend) poolServingLocked(rec *poolRec, revName string) bool {
	short := lastSegment(revName)

	for _, st := range rec.pb.GetInstanceSplitStatuses() {
		if st.GetRevision() == short && st.GetPercent() > 0 {
			return true
		}
	}

	return false
}

// idlePoolRevsLocked returns the instance pools of revisions without an instance split; drain them unlocked.
func (b *InMemoryBackend) idlePoolRevsLocked(rec *poolRec) []*gcpscale.Pool[*inst] {
	var idle []*gcpscale.Pool[*inst]

	for name, r := range b.revisions {
		if ownerOf(name) == rec.pb.GetName() && r.pool != nil && !b.poolServingLocked(rec, name) {
			idle = append(idle, r.pool)
		}
	}

	return idle
}

// warmWorkerPool brings every revision of the pool that receives instances up to the pool's instance count.
func (b *InMemoryBackend) warmWorkerPool(poolName string) {
	b.mu.RLock("warmWorkerPool")

	rec := b.workerPools[poolName]
	if rec == nil {
		b.mu.RUnlock()

		return
	}

	want := int(poolInstanceCount(rec.pb))

	var revs []string

	for name := range b.revisions {
		if ownerOf(name) == poolName && b.poolServingLocked(rec, name) {
			revs = append(revs, name)
		}
	}
	b.mu.RUnlock()

	for _, name := range revs {
		pool, _, err := b.poolFor(name)
		if err != nil {
			continue
		}

		if live := pool.Live(); live > want {
			excess := live - want
			pool.Remove(func(*inst) bool {
				excess--

				return excess >= 0
			})
		} else if live < want {
			b.wg.Go(func() { _ = b.ensure(b.baseCtx(), name, want) })
		}
	}
}

// GetWorkerPool returns the named worker pool.
func (b *InMemoryBackend) GetWorkerPool(name string) (*runpb.WorkerPool, error) {
	n, err := parseName(name, "workerPools")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetWorkerPool")
	defer b.mu.RUnlock()

	rec, ok := b.workerPools[name]
	if !ok {
		return nil, errNotFound("WORKER_POOL", n.ids[0], n.location, n.project)
	}

	return clone(rec.pb), nil
}

// ListWorkerPools lists the worker pools under a location ordered by name.
func (b *InMemoryBackend) ListWorkerPools(in *runpb.ListWorkerPoolsRequest) ([]*runpb.WorkerPool, string, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListWorkerPools")

	prefix := p.parent() + "/workerPools/"

	all := collect(b.workerPools, prefix, func(r *poolRec) *runpb.WorkerPool { return clone(r.pb) })
	if in.GetShowDeleted() {
		all = mergeDeleted(b, all, b.deleted.workerPools, prefix, clone[*runpb.WorkerPool])
	}
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// UpdateWorkerPool applies the masked fields; a changed template deploys a new revision.
func (b *InMemoryBackend) UpdateWorkerPool(in *runpb.UpdateWorkerPoolRequest) (*longrunningpb.Operation, error) {
	name := in.GetWorkerPool().GetName()

	n, err := parseName(name, "workerPools")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWorkerPool")

	rec, ok := b.workerPools[name]
	if !ok {
		b.mu.Unlock()

		if in.GetAllowMissing() {
			return b.CreateWorkerPool(&runpb.CreateWorkerPoolRequest{
				Parent: n.parent(), WorkerPoolId: n.ids[0], WorkerPool: in.GetWorkerPool(),
				ValidateOnly: in.GetValidateOnly(),
			})
		}

		return nil, errNotFound("WORKER_POOL", n.ids[0], n.location, n.project)
	}

	return b.updatePoolLocked(n, rec, in)
}

// updatePoolLocked finishes UpdateWorkerPool; it is entered with b.mu held and releases it.
func (b *InMemoryBackend) updatePoolLocked(
	n resName, rec *poolRec, in *runpb.UpdateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	work := clone(rec.pb)

	if err := applyMask(work, in.GetWorkerPool(), in.GetUpdateMask().GetPaths(), workerPoolMutable()); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if err := validateWorkerPool(work); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	newTemplate := in.GetForceNewRevision() || !protoEqual(work.GetTemplate(), rec.pb.GetTemplate())

	var rev *revRec

	if newTemplate {
		var err error

		scratch := &poolRec{pb: work, revSeq: rec.revSeq}
		if rev, err = newPoolRevisionLocked(scratch, n, work.GetTemplate(), b.ts()); err != nil {
			b.mu.Unlock()

			return nil, err
		}

		if b.revisions[rev.pb.GetName()] != nil {
			b.mu.Unlock()

			return nil, errExists("Revision '%s' already exists.", lastSegment(rev.pb.GetName()))
		}
	}

	known := b.knownPoolRevisionsLocked(rec)
	if rev != nil {
		known[lastSegment(rev.pb.GetName())] = true
	}

	splits, err := normalizeSplits(work.GetInstanceSplits(), known)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() {
		work.InstanceSplits = splits
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), work, work)
	}

	return b.commitPoolUpdateLocked(n, rec, work, splits, rev)
}

func (b *InMemoryBackend) knownPoolRevisionsLocked(rec *poolRec) map[string]bool {
	known := map[string]bool{}

	for name := range b.revisions {
		if ownerOf(name) == rec.pb.GetName() {
			known[lastSegment(name)] = true
		}
	}

	return known
}

func (b *InMemoryBackend) commitPoolUpdateLocked(
	n resName, rec *poolRec, work *runpb.WorkerPool, splits []*runpb.InstanceSplit, rev *revRec,
) (*longrunningpb.Operation, error) {
	work.InstanceSplits = splits
	work.Generation++
	work.Etag = ""
	rec.pb = work

	if rev != nil {
		rec.revSeq = rev.seq
		work.LatestCreatedRevision = rev.pb.GetName()
		b.revisions[rev.pb.GetName()] = rev
		rec.pending++
	}

	b.refreshPoolLocked(rec)
	view := clone(rec.pb)

	var idle []*gcpscale.Pool[*inst]
	if rev == nil {
		idle = b.idlePoolRevsLocked(rec)
	}
	b.mu.Unlock()

	for _, p := range idle {
		p.Drain()
	}

	if rev != nil {
		return b.beginPoolDeploy(n.parent(), work.GetName(), rev.pb.GetName(), view)
	}

	b.warmWorkerPool(work.GetName())

	return b.doneOp(opName(n.parent()), view, view)
}

// DeleteWorkerPool removes the worker pool, its revisions and their containers.
func (b *InMemoryBackend) DeleteWorkerPool(in *runpb.DeleteWorkerPoolRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "workerPools")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteWorkerPool")

	rec, ok := b.workerPools[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("WORKER_POOL", n.ids[0], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != rec.pb.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the worker pool's current etag.", in.GetEtag())
	}

	view := clone(rec.pb)
	view.DeleteTime = b.ts()

	if in.GetValidateOnly() {
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	removed := b.removePoolLocked(in.GetName())
	view.ExpireTime = stampExpiry(view.GetDeleteTime())
	b.sweepLocked()
	b.deleted.workerPools[tombKey(view.GetName(), view.GetUid())] = clone(view)
	b.mu.Unlock()

	for _, r := range removed {
		b.teardown(r)
	}

	return b.doneOp(opName(n.parent()), view, view)
}

func (b *InMemoryBackend) removePoolLocked(name string) []*revRec {
	delete(b.workerPools, name)
	b.iam.Drop(name)

	var removed []*revRec

	for rn, r := range b.revisions {
		if ownerOf(rn) == name {
			removed = append(removed, r)
			delete(b.revisions, rn)
		}
	}

	return removed
}
