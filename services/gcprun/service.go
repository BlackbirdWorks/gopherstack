package gcprun

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

func serviceMutable() []string {
	return []string{
		"description", "labels", "annotations", "client", "client_version", "ingress", "launch_stage",
		"binary_authorization", "template", "traffic", "scaling", "invoker_iam_disabled", "default_uri_disabled",
		"custom_audiences", "iap_enabled",
	}
}

func (b *InMemoryBackend) host(n resName) string {
	h := hostLabel(n) + ".localhost"
	if b.ingressPort > 0 {
		h = fmt.Sprintf("%s:%d", h, b.ingressPort)
	}

	return h
}

// viewLocked returns a copy of the service with its serving URIs filled in.
func (b *InMemoryBackend) viewLocked(rec *svcRec) *runpb.Service {
	v := clone(rec.pb)

	n, err := parseName(v.GetName(), "services")
	if err != nil {
		return v
	}

	host := b.host(n)
	v.Uri = "http://" + host
	v.Urls = []string{v.GetUri()}

	for _, st := range v.GetTrafficStatuses() {
		if st.GetTag() != "" {
			st.Uri = "http://" + st.GetTag() + tagSeparator + host
		}
	}

	return v
}

func knownRevisions(rec *svcRec, revs map[string]*revRec, extra string) map[string]bool {
	known := map[string]bool{}
	if extra != "" {
		known[extra] = true
	}

	for name, r := range revs {
		if r.pb.GetService() == rec.pb.GetName() {
			known[lastSegment(name)] = true
		}
	}

	return known
}

func (b *InMemoryBackend) buildServiceLocked(n resName, req *runpb.Service) (*svcRec, *revRec, error) {
	now := b.ts()
	pb := clone(req)
	pb.Name = n.parent() + "/services/" + n.ids[0]
	pb.Uid, pb.Generation, pb.CreateTime, pb.UpdateTime = newUID(), 1, now, now
	pb.DeleteTime, pb.ExpireTime, pb.Uri, pb.Urls = nil, nil, "", nil
	pb.LatestReadyRevision, pb.TrafficStatuses = "", nil

	if pb.GetIngress() == runpb.IngressTraffic_INGRESS_TRAFFIC_UNSPECIFIED {
		pb.Ingress = runpb.IngressTraffic_INGRESS_TRAFFIC_ALL
	}

	rec := &svcRec{pb: pb}

	rev, err := newRevisionLocked(rec, n, pb.GetTemplate(), now)
	if err != nil {
		return nil, nil, err
	}

	if pb.Traffic, err = normalizeTraffic(
		req.GetTraffic(),
		map[string]bool{lastSegment(rev.pb.GetName()): true},
	); err != nil {
		return nil, nil, err
	}

	pb.LatestCreatedRevision = rev.pb.GetName()
	rec.pending++
	b.refreshLocked(rec)

	return rec, rev, nil
}

// refreshLocked recomputes the output-only status fields of a service.
func (b *InMemoryBackend) refreshLocked(rec *svcRec) {
	pb, at := rec.pb, b.ts()

	pb.TrafficStatuses = trafficStatuses(pb.GetTraffic(), lastSegment(pb.GetLatestReadyRevision()))
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
	pb.Conditions = []*runpb.Condition{
		ready,
		newCondition("RoutesReady", state, msg, at),
		newCondition("ConfigurationsReady", state, msg, at),
	}
	pb.UpdateTime = at
	pb.Etag = etag(pb.GetUid(), pb.GetGeneration(), at)
}

// CreateService stores a service with its first revision and deploys it in the background.
func (b *InMemoryBackend) CreateService(in *runpb.CreateServiceRequest) (*longrunningpb.Operation, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateID(in.GetServiceId(), "service_id"); err != nil {
		return nil, err
	}

	if err = validateTemplate(in.GetService().GetTemplate()); err != nil {
		return nil, err
	}

	n, err := parseName(p.parent()+"/services/"+in.GetServiceId(), "services")
	if err != nil {
		return nil, err
	}

	return b.createService(n, in.GetService(), in.GetValidateOnly())
}

func (b *InMemoryBackend) createService(
	n resName,
	req *runpb.Service,
	validateOnly bool,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("CreateService")

	name := n.parent() + "/services/" + n.ids[0]
	if _, exists := b.services[name]; exists {
		b.mu.Unlock()

		return nil, errExists("Resource '%s' already exists.", n.ids[0])
	}

	rec, rev, err := b.buildServiceLocked(n, req)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if validateOnly {
		view := b.viewLocked(rec)
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	b.services[name], b.revisions[rev.pb.GetName()] = rec, rev
	view := b.viewLocked(rec)
	b.mu.Unlock()

	return b.beginDeploy(n.parent(), name, rev.pb.GetName(), view)
}

func (b *InMemoryBackend) beginDeploy(
	opParent, svcName, revName string, meta *runpb.Service,
) (*longrunningpb.Operation, error) {
	name := opName(opParent)

	op, err := b.pendingOp(name, meta)
	if err != nil {
		return nil, err
	}

	b.wg.Go(func() { b.deploy(name, svcName, revName) })

	return op, nil
}

func (b *InMemoryBackend) deploy(op, svcName, revName string) {
	err := b.startRevision(b.baseCtx(), revName)

	b.mu.Lock("deploy")

	rec, rev := b.services[svcName], b.revisions[revName]
	if rec == nil || rev == nil {
		if rec != nil {
			rec.pending--
			b.refreshLocked(rec)
		}
		b.mu.Unlock()
		b.failOp(op, &runpb.Service{Name: svcName}, status.New(codes.Aborted, "Service was deleted during deployment."))

		return
	}

	failure := b.settleLocked(rec, rev, err)
	view := b.viewLocked(rec)
	idle := b.reapIdleLocked(rec)
	b.mu.Unlock()

	for _, p := range idle {
		p.Drain()
	}

	b.warmServing(svcName)

	if failure != "" {
		b.failOp(op, view, status.New(codes.FailedPrecondition, failure))

		return
	}

	b.finishOp(op, view, view)
}

// settleLocked records the outcome of starting rev and returns a failure message, empty on success.
func (b *InMemoryBackend) settleLocked(rec *svcRec, rev *revRec, err error) string {
	at := b.ts()
	rec.pending--
	rev.pb.Reconciling = false
	rev.pb.UpdateTime = at

	var failure string

	switch {
	case err == nil:
		rev.pb.Conditions = setCondition(
			rev.pb.GetConditions(),
			newCondition(condReady, runpb.Condition_CONDITION_SUCCEEDED, "", at),
		)
	case errors.Is(err, errNoRuntime):
		msg := "Revision is deployed but cannot serve: " + err.Error()
		rev.pb.Conditions = setCondition(
			rev.pb.GetConditions(),
			newCondition(condReady, runpb.Condition_CONDITION_SUCCEEDED, msg, at),
		)
	default:
		failure = fmt.Sprintf(
			"Revision '%s' is not ready and cannot serve traffic. %s",
			lastSegment(rev.pb.GetName()),
			err,
		)
		rev.pb.Conditions = setCondition(
			rev.pb.GetConditions(),
			newCondition(condReady, runpb.Condition_CONDITION_FAILED, failure, at),
		)
	}

	if failure == "" {
		rec.failure = ""

		if cur := b.revisions[rec.pb.GetLatestReadyRevision()]; cur == nil || rev.seq >= cur.seq {
			rec.pb.LatestReadyRevision = rev.pb.GetName()
		}
	} else if rev.pb.GetName() == rec.pb.GetLatestCreatedRevision() {
		rec.failure = failure
	}

	b.refreshLocked(rec)

	return failure
}

// reapIdleLocked returns the pools of revisions that receive no traffic; drain them outside the lock, they
// restart on demand.
func (b *InMemoryBackend) reapIdleLocked(rec *svcRec) []*gcpscale.Pool[*inst] {
	serving := map[string]bool{}
	for _, st := range rec.pb.GetTrafficStatuses() {
		if st.GetPercent() > 0 || st.GetTag() != "" {
			serving[st.GetRevision()] = true
		}
	}

	var idle []*gcpscale.Pool[*inst]

	for name, r := range b.revisions {
		if r.pb.GetService() != rec.pb.GetName() || serving[lastSegment(name)] || r.pool == nil {
			continue
		}

		idle = append(idle, r.pool)
	}

	return idle
}

// GetService returns the named service.
func (b *InMemoryBackend) GetService(name string) (*runpb.Service, error) {
	n, err := parseName(name, "services")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetService")
	defer b.mu.RUnlock()

	rec, ok := b.services[name]
	if !ok {
		return nil, errNotFound("SERVICE", n.ids[0], n.location, n.project)
	}

	return b.viewLocked(rec), nil
}

// ListServices lists the services under a location ordered by name.
func (b *InMemoryBackend) ListServices(in *runpb.ListServicesRequest) ([]*runpb.Service, string, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListServices")

	all := collect(b.services, p.parent()+"/services/", b.viewLocked)
	if in.GetShowDeleted() {
		all = mergeDeleted(b, all, b.deleted.services, p.parent()+"/services/", clone[*runpb.Service])
	}
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// UpdateService applies the masked fields; a changed template deploys a new revision.
func (b *InMemoryBackend) UpdateService(in *runpb.UpdateServiceRequest) (*longrunningpb.Operation, error) {
	name := in.GetService().GetName()

	n, err := parseName(name, "services")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateService")

	rec, ok := b.services[name]
	if !ok {
		b.mu.Unlock()

		if in.GetAllowMissing() {
			return b.CreateService(&runpb.CreateServiceRequest{
				Parent: n.parent(), ServiceId: n.ids[0], Service: in.GetService(), ValidateOnly: in.GetValidateOnly(),
			})
		}

		return nil, errNotFound("SERVICE", n.ids[0], n.location, n.project)
	}

	return b.updateLocked(n, rec, in)
}

// updateLocked finishes UpdateService; it is entered with b.mu held and releases it.
func (b *InMemoryBackend) updateLocked(
	n resName,
	rec *svcRec,
	in *runpb.UpdateServiceRequest,
) (*longrunningpb.Operation, error) {
	work := clone(rec.pb)

	if err := applyMask(work, in.GetService(), in.GetUpdateMask().GetPaths(), serviceMutable()); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	newTemplate := !protoEqual(work.GetTemplate(), rec.pb.GetTemplate())

	if err := validateTemplate(work.GetTemplate()); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	var rev *revRec

	if newTemplate {
		var err error
		if rev, err = newRevisionLocked(
			&svcRec{pb: work, revSeq: rec.revSeq},
			n,
			work.GetTemplate(),
			b.ts(),
		); err != nil {
			b.mu.Unlock()

			return nil, err
		}
	}

	if rev != nil && b.revisions[rev.pb.GetName()] != nil {
		b.mu.Unlock()

		return nil, errExists("Revision '%s' already exists.", lastSegment(rev.pb.GetName()))
	}

	known := knownRevisions(rec, b.revisions, "")
	if rev != nil {
		known[lastSegment(rev.pb.GetName())] = true
	}

	traffic, err := normalizeTraffic(work.GetTraffic(), known)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() {
		work.Traffic = traffic
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), work, work)
	}

	return b.commitUpdateLocked(n, rec, work, traffic, rev)
}

func (b *InMemoryBackend) commitUpdateLocked(
	n resName, rec *svcRec, work *runpb.Service, traffic []*runpb.TrafficTarget, rev *revRec,
) (*longrunningpb.Operation, error) {
	work.Traffic = traffic
	work.Generation++
	work.Etag, work.Uri, work.Urls = "", "", nil
	rec.pb = work

	if rev != nil {
		rec.revSeq = rev.seq
		work.LatestCreatedRevision = rev.pb.GetName()
		b.revisions[rev.pb.GetName()] = rev
		rec.pending++
	}

	b.refreshLocked(rec)
	view := b.viewLocked(rec)

	var idle []*gcpscale.Pool[*inst]
	if rev == nil {
		idle = b.reapIdleLocked(rec)
	}
	b.mu.Unlock()

	for _, p := range idle {
		p.Drain()
	}

	if rev == nil {
		b.warmServing(work.GetName())
	}

	if rev != nil {
		return b.beginDeploy(n.parent(), work.GetName(), rev.pb.GetName(), view)
	}

	return b.doneOp(opName(n.parent()), view, view)
}

// DeleteService removes the service, its revisions and their containers.
func (b *InMemoryBackend) DeleteService(in *runpb.DeleteServiceRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "services")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteService")

	rec, ok := b.services[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("SERVICE", n.ids[0], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != rec.pb.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the service's current etag.", in.GetEtag())
	}

	view := b.viewLocked(rec)
	view.DeleteTime = b.ts()

	if in.GetValidateOnly() {
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	removed := b.removeServiceLocked(in.GetName())
	b.entombServiceLocked(view)
	b.mu.Unlock()

	for _, r := range removed {
		b.teardown(r)
	}

	return b.doneOp(opName(n.parent()), view, view)
}

func (b *InMemoryBackend) removeServiceLocked(name string) []*revRec {
	delete(b.services, name)
	b.iam.Drop(name)

	var removed []*revRec

	for rn, r := range b.revisions {
		if r.pb.GetService() == name {
			removed = append(removed, r)
			delete(b.revisions, rn)
		}
	}

	return removed
}

func sortedKeys[V any](m map[string]V, prefix string) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	slices.Sort(keys)

	return keys
}
