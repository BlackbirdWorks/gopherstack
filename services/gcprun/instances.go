package gcprun

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type instRec struct {
	pb      *runpb.Instance
	run     *inst
	failure string
	pending int
	stopped bool
}

func validateInstance(in *runpb.Instance) error {
	if in == nil {
		return errInvalid("instance is required.")
	}

	if len(in.GetDescription()) > maxDescriptionLen {
		return errInvalid("description is limited to %d characters.", maxDescriptionLen)
	}

	if len(in.GetContainers()) != 1 {
		return errInvalid("An instance runs exactly one container.")
	}

	if err := validateContainers(in.GetContainers(), true); err != nil {
		return err
	}

	return validateRuntimeShape(shapeOf(in.GetContainers(), in.GetVolumes()))
}

func instanceRevision(pb *runpb.Instance) *runpb.Revision {
	return &runpb.Revision{Name: pb.GetName(), Containers: pb.GetContainers(), Volumes: pb.GetVolumes()}
}

// instanceView returns a copy of the instance with its serving URL filled in.
func (b *InMemoryBackend) instanceView(rec *instRec) *runpb.Instance {
	v := clone(rec.pb)

	if n, err := parseName(v.GetName(), "instances"); err == nil {
		v.Urls = []string{"http://" + b.host(n)}
	}

	return v
}

func (b *InMemoryBackend) refreshInstanceLocked(rec *instRec) {
	pb, at := rec.pb, b.ts()
	pb.Reconciling = rec.pending > 0

	state, msg := runpb.Condition_CONDITION_SUCCEEDED, ""

	switch {
	case rec.pending > 0:
		state, msg = runpb.Condition_CONDITION_RECONCILING, "Starting instance."
	case rec.failure != "":
		state, msg = runpb.Condition_CONDITION_FAILED, rec.failure
	case rec.stopped:
		state, msg = runpb.Condition_CONDITION_PENDING, "Instance is stopped."
	default:
		pb.ObservedGeneration = pb.GetGeneration()
	}

	ready := newCondition(condReady, state, msg, at)
	pb.TerminalCondition, pb.Conditions = ready, []*runpb.Condition{ready}
	pb.UpdateTime = at
	pb.Etag = etag(pb.GetUid(), pb.GetGeneration(), at)
}

// CreateInstance stores an instance and starts its container in the background.
func (b *InMemoryBackend) CreateInstance(in *runpb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateID(in.GetInstanceId(), "instance_id"); err != nil {
		return nil, err
	}

	if err = validateInstance(in.GetInstance()); err != nil {
		return nil, err
	}

	name := p.parent() + "/instances/" + in.GetInstanceId()
	now := b.ts()
	pb := clone(in.GetInstance())
	pb.Name, pb.Uid, pb.Generation, pb.CreateTime, pb.UpdateTime = name, newUID(), 1, now, now
	pb.DeleteTime, pb.ExpireTime, pb.Urls, pb.ContainerStatuses = nil, nil, nil, nil

	if pb.GetIngress() == runpb.IngressTraffic_INGRESS_TRAFFIC_UNSPECIFIED {
		pb.Ingress = runpb.IngressTraffic_INGRESS_TRAFFIC_ALL
	}

	rec := &instRec{pb: pb, pending: 1}

	b.mu.Lock("CreateInstance")

	if _, exists := b.instances[name]; exists {
		b.mu.Unlock()

		return nil, errExists("Resource '%s' already exists.", in.GetInstanceId())
	}

	b.refreshInstanceLocked(rec)

	if in.GetValidateOnly() {
		view := b.instanceView(rec)
		b.mu.Unlock()

		return b.doneOp(opName(p.parent()), view, view)
	}

	b.instances[name] = rec
	view := b.instanceView(rec)
	b.mu.Unlock()

	return b.beginInstanceStart(p.parent(), name, view)
}

func (b *InMemoryBackend) beginInstanceStart(
	opParent, name string, meta *runpb.Instance,
) (*longrunningpb.Operation, error) {
	op := opName(opParent)

	pending, err := b.pendingOp(op, meta)
	if err != nil {
		return nil, err
	}

	b.wg.Go(func() { b.runInstance(op, name) })

	return pending, nil
}

func (b *InMemoryBackend) runInstance(op, name string) {
	b.mu.RLock("runInstance")

	rec := b.instances[name]
	if rec == nil {
		b.mu.RUnlock()
		b.failOp(op, &runpb.Instance{Name: name}, status.New(codes.Aborted, "Instance was deleted while starting."))

		return
	}

	rev := instanceRevision(rec.pb)
	b.mu.RUnlock()

	run, err := b.launchInstance(rev)

	b.mu.Lock("runInstance.settle")

	rec = b.instances[name]
	if rec == nil {
		b.mu.Unlock()
		b.teardownInst(run)
		b.failOp(op, &runpb.Instance{Name: name}, status.New(codes.Aborted, "Instance was deleted while starting."))

		return
	}

	failure := b.settleInstanceLocked(rec, run, err)
	view := b.instanceView(rec)
	b.mu.Unlock()

	if failure != "" {
		b.failOp(op, view, status.New(codes.FailedPrecondition, failure))

		return
	}

	b.finishOp(op, view, view)
}

func (b *InMemoryBackend) launchInstance(rev *runpb.Revision) (*inst, error) {
	run := &inst{seq: b.instSeq.Add(1)}
	run.mctx, run.cancel = context.WithCancel(b.baseCtx())

	if err := b.startInstance(run, rev); err != nil {
		b.teardownInst(run)

		return nil, err
	}

	return run, nil
}

// settleInstanceLocked records the outcome of starting the container and returns a failure message.
func (b *InMemoryBackend) settleInstanceLocked(rec *instRec, run *inst, err error) string {
	rec.pending--

	switch {
	case err == nil:
		rec.run, rec.failure, rec.stopped = run, "", false
	case errors.Is(err, errNoRuntime):
		rec.failure, rec.stopped = "", false
	default:
		rec.failure = fmt.Sprintf("Instance '%s' failed to start. %s", lastSegment(rec.pb.GetName()), err)
	}

	b.refreshInstanceLocked(rec)

	return rec.failure
}

// GetInstance returns the named instance.
func (b *InMemoryBackend) GetInstance(name string) (*runpb.Instance, error) {
	n, err := parseName(name, "instances")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	rec, ok := b.instances[name]
	if !ok {
		return nil, errNotFound("INSTANCE", n.ids[0], n.location, n.project)
	}

	return b.instanceView(rec), nil
}

// ListInstances lists the instances under a location ordered by name.
func (b *InMemoryBackend) ListInstances(in *runpb.ListInstancesRequest) ([]*runpb.Instance, string, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListInstances")

	prefix := p.parent() + "/instances/"

	all := collect(b.instances, prefix, b.instanceView)
	if in.GetShowDeleted() {
		all = mergeDeleted(b, all, b.deleted.instances, prefix, clone[*runpb.Instance])
	}
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// DeleteInstance stops the instance's container and removes it.
func (b *InMemoryBackend) DeleteInstance(in *runpb.DeleteInstanceRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "instances")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteInstance")

	rec, ok := b.instances[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("INSTANCE", n.ids[0], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != rec.pb.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the instance's current etag.", in.GetEtag())
	}

	view := b.instanceView(rec)
	view.DeleteTime = b.ts()

	if in.GetValidateOnly() {
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	run := rec.run
	delete(b.instances, in.GetName())
	view.ExpireTime = stampExpiry(view.GetDeleteTime())
	b.sweepLocked()
	b.deleted.instances[tombKey(view.GetName(), view.GetUid())] = clone(view)
	b.mu.Unlock()

	b.teardownInst(run)

	return b.doneOp(opName(n.parent()), view, view)
}

func (b *InMemoryBackend) checkedInstanceLocked(name, etagIn string) (*instRec, resName, error) {
	n, err := parseName(name, "instances")
	if err != nil {
		return nil, n, err
	}

	rec, ok := b.instances[name]
	if !ok {
		return nil, n, errNotFound("INSTANCE", n.ids[0], n.location, n.project)
	}

	if etagIn != "" && etagIn != rec.pb.GetEtag() {
		return nil, n, errPrecondition("The etag %q does not match the instance's current etag.", etagIn)
	}

	return rec, n, nil
}

// StopInstance stops the instance's container; the instance stays and can be started again.
func (b *InMemoryBackend) StopInstance(in *runpb.StopInstanceRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("StopInstance")

	rec, n, err := b.checkedInstanceLocked(in.GetName(), in.GetEtag())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() {
		view := b.instanceView(rec)
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	run := rec.run
	rec.run, rec.stopped, rec.failure = nil, true, ""

	if rec.pending == 0 {
		rec.pb.Generation++
	}

	b.refreshInstanceLocked(rec)
	view := b.instanceView(rec)
	b.mu.Unlock()

	b.teardownInst(run)

	return b.doneOp(opName(n.parent()), view, view)
}

// StartInstance starts the container of a stopped instance.
func (b *InMemoryBackend) StartInstance(in *runpb.StartInstanceRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("StartInstance")

	rec, n, err := b.checkedInstanceLocked(in.GetName(), in.GetEtag())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() || (!rec.stopped && rec.failure == "") {
		view := b.instanceView(rec)
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	rec.pending++
	rec.pb.Generation++
	b.refreshInstanceLocked(rec)
	view := b.instanceView(rec)
	b.mu.Unlock()

	return b.beginInstanceStart(n.parent(), in.GetName(), view)
}
