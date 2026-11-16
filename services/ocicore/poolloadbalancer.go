package ocicore

import (
	"context"
	"errors"
	"net"
	"net/http"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	lbAttaching = "ATTACHING"
	lbAttached  = "ATTACHED"
	lbDetaching = "DETACHING"
	lbDetached  = "DETACHED"

	primaryVnic = "PrimaryVnic"
)

// ErrTargetGone is returned by a LoadBalancerClient when the load balancer or its backend set no longer exists.
var ErrTargetGone = errors.New("load balancer or backend set not found")

// BackendEndpoint is the address of one backend.
type BackendEndpoint struct {
	IP   string
	Port int
}

// BackendChange adds and removes backends of one backend set.
type BackendChange struct {
	Set    string
	Add    []BackendEndpoint
	Remove []BackendEndpoint
}

// LoadBalancerClient is the part of OCI Load Balancer that instance pools use.
type LoadBalancerClient interface {
	// HasBackendSet reports whether the load balancer exists and has the backend set.
	HasBackendSet(lbID, set string) bool
	// ApplyBackends applies every change in one load balancer update. It wraps ErrTargetGone when the load
	// balancer or a backend set is missing. Adding an existing backend or removing a missing one is not an error.
	ApplyBackends(lbID string, changes []BackendChange) error
}

// PoolLoadBalancer is an instance pool load balancer attachment (InstancePoolLoadBalancerAttachment).
type PoolLoadBalancer struct {
	ID             string   `json:"id"`
	InstancePoolID string   `json:"instancePoolId"`
	LoadBalancerID string   `json:"loadBalancerId"`
	BackendSetName string   `json:"backendSetName"`
	VnicSelection  string   `json:"vnicSelection"`
	LifecycleState string   `json:"lifecycleState"`
	Backends       []string `json:"backends,omitempty"`
	Port           int      `json:"port"`
}

// AttachLoadBalancerIn is AttachLoadBalancerDetails; it is also an element of CreateInstancePoolDetails.loadBalancers.
type AttachLoadBalancerIn struct {
	Port           *int   `json:"port"`
	LoadBalancerID string `json:"loadBalancerId"`
	BackendSetName string `json:"backendSetName"`
	VnicSelection  string `json:"vnicSelection"`
}

// DetachLoadBalancerIn is DetachLoadBalancerDetails.
type DetachLoadBalancerIn struct {
	LoadBalancerID string `json:"loadBalancerId"`
	BackendSetName string `json:"backendSetName"`
}

// SetLoadBalancerClient wires instance pool load balancer attachments to OCI Load Balancer; nil unwires them.
func (b *InMemoryBackend) SetLoadBalancerClient(c LoadBalancerClient) {
	b.mu.Lock("SetLoadBalancerClient")
	b.lbClient = c
	b.mu.Unlock()
}

// Close stops background work: load balancer synchronisation returns as soon as it is observed.
func (b *InMemoryBackend) Close() { b.stopLife() }

func (p *InstancePool) activeAttachment(lbID, set string) *PoolLoadBalancer {
	for i := range p.LoadBalancers {
		a := &p.LoadBalancers[i]
		if a.LoadBalancerID == lbID && a.BackendSetName == set && a.LifecycleState != lbDetached {
			return a
		}
	}

	return nil
}

func checkAttachIn(in AttachLoadBalancerIn) *APIError {
	switch {
	case in.LoadBalancerID == "":
		return errMissing("loadBalancerId is required")
	case in.BackendSetName == "":
		return errMissing("backendSetName is required")
	case in.VnicSelection == "":
		return errMissing("vnicSelection is required")
	case in.Port == nil:
		return errMissing("port is required")
	case *in.Port < 1 || *in.Port > maxPort:
		return errParam("port must be between 1 and 65535")
	}

	return nil
}

// checkLoadBalancerTargets validates attachments and that their backend sets exist; it must not hold b.mu.
func (b *InMemoryBackend) checkLoadBalancerTargets(ins []AttachLoadBalancerIn) *APIError {
	if len(ins) == 0 {
		return nil
	}

	b.mu.RLock("checkLoadBalancerTargets")
	client := b.lbClient
	b.mu.RUnlock()

	if client == nil {
		return errParam("OCI Load Balancer is not available to attach instance pools to")
	}

	for _, in := range ins {
		if aerr := checkAttachIn(in); aerr != nil {
			return aerr
		}

		if !client.HasBackendSet(in.LoadBalancerID, in.BackendSetName) {
			return errNotFound(in.LoadBalancerID)
		}
	}

	return nil
}

// configVnicNames lists the displayName of each secondary VNIC of an instance configuration.
func configVnicNames(cfg *InstanceConfiguration) []string {
	var out []string

	list, _ := cfg.InstanceDetails["secondaryVnics"].([]any)
	for _, v := range list {
		if m, ok := v.(map[string]any); ok {
			if n, _ := m["displayName"].(string); n != "" {
				out = append(out, n)
			}
		}
	}

	return out
}

// newPoolAttachments builds the attachments of a pool; callers hold b.mu.
func (b *InMemoryBackend) newPoolAttachments(
	p *InstancePool, cfg *InstanceConfiguration, ins []AttachLoadBalancerIn,
) *APIError {
	for _, in := range ins {
		if aerr := checkAttachIn(in); aerr != nil {
			return aerr
		}

		if in.VnicSelection != primaryVnic && !slices.Contains(configVnicNames(cfg), in.VnicSelection) {
			return errParam("vnicSelection must be " + primaryVnic + " or the displayName of a secondary VNIC of " +
				"the instance configuration")
		}

		if p.activeAttachment(in.LoadBalancerID, in.BackendSetName) != nil {
			return errConflict("The instance pool " + p.ID + " is already attached to backend set " +
				in.BackendSetName + " of load balancer " + in.LoadBalancerID)
		}

		p.LoadBalancers = append(p.LoadBalancers, PoolLoadBalancer{
			ID: newOCID("instancepoolloadbalancerattachment"), InstancePoolID: p.ID, LoadBalancerID: in.LoadBalancerID,
			BackendSetName: in.BackendSetName, Port: *in.Port, VnicSelection: in.VnicSelection,
			LifecycleState: lbAttaching,
		})
	}

	b.lbActive = true

	return nil
}

// AttachLoadBalancer attaches a backend set of a load balancer to the pool; the pool's instances become backends.
func (b *InMemoryBackend) AttachLoadBalancer(
	poolID, ifMatch string,
	in AttachLoadBalancerIn,
) (Resource, string, *APIError) {
	if aerr := b.checkLoadBalancerTargets([]AttachLoadBalancerIn{in}); aerr != nil {
		return nil, "", aerr
	}

	v, etag, aerr := b.attachLoadBalancer(poolID, ifMatch, in)
	if aerr != nil {
		return nil, "", aerr
	}

	b.syncLB()

	return v, etag, nil
}

func (b *InMemoryBackend) attachLoadBalancer(
	poolID, ifMatch string, in AttachLoadBalancerIn,
) (Resource, string, *APIError) {
	b.mu.Lock("AttachLoadBalancer")
	defer b.mu.Unlock()

	p, aerr := getLive[*InstancePool](b, poolID)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	if p.LifecycleState != poolRunning && p.LifecycleState != poolStopped {
		return nil, "", errState("The instance pool " + p.ID + " is " + p.LifecycleState)
	}

	cfg, aerr := getLive[*InstanceConfiguration](b, p.InstanceConfigurationID)
	if aerr != nil {
		return nil, "", aerr
	}

	next := *p
	next.LoadBalancers = slices.Clone(p.LoadBalancers)

	if aerr = b.newPoolAttachments(&next, cfg, []AttachLoadBalancerIn{in}); aerr != nil {
		return nil, "", aerr
	}

	*p = next
	b.bump(p)

	return p.view(), p.Internal.Etag, nil
}

// DetachLoadBalancer detaches a backend set from the pool and removes the pool's instances from it.
func (b *InMemoryBackend) DetachLoadBalancer(
	poolID, ifMatch string,
	in DetachLoadBalancerIn,
) (Resource, string, *APIError) {
	if in.LoadBalancerID == "" || in.BackendSetName == "" {
		return nil, "", errMissing("loadBalancerId and backendSetName are required")
	}

	v, etag, aerr := b.detachLoadBalancer(poolID, ifMatch, in)
	if aerr != nil {
		return nil, "", aerr
	}

	b.syncLB()

	return v, etag, nil
}

func (b *InMemoryBackend) detachLoadBalancer(
	poolID, ifMatch string, in DetachLoadBalancerIn,
) (Resource, string, *APIError) {
	b.mu.Lock("DetachLoadBalancer")
	defer b.mu.Unlock()

	p, aerr := getLive[*InstancePool](b, poolID)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	a := p.activeAttachment(in.LoadBalancerID, in.BackendSetName)
	if a == nil {
		return nil, "", errNotFound(in.LoadBalancerID)
	}

	if a.LifecycleState != lbDetaching {
		a.LifecycleState = lbDetaching
		b.bump(p)
	}

	return p.view(), p.Internal.Etag, nil
}

// GetPoolLoadBalancer returns one attachment of a pool.
func (b *InMemoryBackend) GetPoolLoadBalancer(poolID, id string) (PoolLoadBalancer, *APIError) {
	b.mu.RLock("GetInstancePoolLoadBalancerAttachment")
	defer b.mu.RUnlock()

	p, aerr := get[*InstancePool](b, poolID)
	if aerr != nil {
		return PoolLoadBalancer{}, aerr
	}

	for _, a := range p.LoadBalancers {
		if a.ID == id {
			a.Backends = nil

			return a, nil
		}
	}

	return PoolLoadBalancer{}, errNotFound(id)
}

func (b *InMemoryBackend) syncLB() { _ = b.SyncLoadBalancers(b.life) }

// vnicIP returns the private IP of the instance's VNIC chosen by an attachment's vnicSelection.
func (b *InMemoryBackend) vnicIP(instanceID, selection string) string {
	for _, r := range b.res {
		a, ok := r.(*VnicAttachment)
		if !ok || a.InstanceID != instanceID || !live(a) {
			continue
		}

		if (selection == primaryVnic && a.NicIndex == 0) || (selection != primaryVnic && a.DisplayName == selection) {
			if v, found := b.res[a.VnicID].(*Vnic); found {
				return v.PrivateIP
			}
		}
	}

	return ""
}

// lbJob is one attachment's pending change to its backend set.
type lbJob struct {
	poolID, attID, lbID, set string
	endState                 string
	want                     []string
	change                   BackendChange
	applied                  bool
}

func endpointName(e BackendEndpoint) string { return net.JoinHostPort(e.IP, strconv.Itoa(e.Port)) }

func parseEndpoint(name string) (BackendEndpoint, bool) {
	host, ps, err := net.SplitHostPort(name)
	if err != nil {
		return BackendEndpoint{}, false
	}

	port, err := strconv.Atoi(ps)

	return BackendEndpoint{IP: host, Port: port}, err == nil
}

// planPoolLB computes what attachment a must change; callers hold b.mu.
func (b *InMemoryBackend) planPoolLB(p *InstancePool, a *PoolLoadBalancer) *lbJob {
	detaching := a.LifecycleState == lbDetaching || !live(p)

	var want []string

	if !detaching {
		for _, m := range b.poolMembers(p) {
			if ip := b.vnicIP(m.ID, a.VnicSelection); ip != "" {
				want = append(want, endpointName(BackendEndpoint{IP: ip, Port: a.Port}))
			}
		}
	}

	slices.Sort(want)
	want = slices.Compact(want)

	job := &lbJob{
		poolID: p.ID, attID: a.ID, lbID: a.LoadBalancerID, set: a.BackendSetName, want: want, endState: lbAttached,
		change: BackendChange{Set: a.BackendSetName},
	}

	if detaching {
		job.endState = lbDetached
	}

	for _, n := range want {
		if e, ok := parseEndpoint(n); ok && !slices.Contains(a.Backends, n) {
			job.change.Add = append(job.change.Add, e)
		}
	}

	for _, n := range a.Backends {
		if e, ok := parseEndpoint(n); ok && !slices.Contains(want, n) {
			job.change.Remove = append(job.change.Remove, e)
		}
	}

	if len(job.change.Add) == 0 && len(job.change.Remove) == 0 && a.LifecycleState == job.endState {
		return nil
	}

	return job
}

func (b *InMemoryBackend) planLB() ([]*lbJob, LoadBalancerClient) {
	b.mu.RLock("planLB")
	defer b.mu.RUnlock()

	if b.lbClient == nil || !b.lbActive {
		return nil, nil
	}

	var jobs []*lbJob

	for _, r := range b.res {
		p, ok := r.(*InstancePool)
		if !ok {
			continue
		}

		for i := range p.LoadBalancers {
			if a := &p.LoadBalancers[i]; a.LifecycleState != lbDetached {
				if job := b.planPoolLB(p, a); job != nil {
					jobs = append(jobs, job)
				}
			}
		}
	}

	slices.SortFunc(jobs, func(x, y *lbJob) int {
		if x.lbID != y.lbID {
			return compareStr(x.lbID, y.lbID)
		}

		return compareStr(x.attID, y.attID)
	})

	return jobs, b.lbClient
}

// callLB applies every job of one load balancer, one by one when the combined update fails.
func callLB(ctx context.Context, client LoadBalancerClient, lbID string, jobs []*lbJob) {
	var changes []BackendChange

	for _, j := range jobs {
		if len(j.change.Add) > 0 || len(j.change.Remove) > 0 {
			changes = append(changes, j.change)
		}
	}

	if len(changes) == 0 {
		for _, j := range jobs {
			j.applied = true
		}

		return
	}

	if err := client.ApplyBackends(lbID, changes); err == nil {
		for _, j := range jobs {
			j.applied = true
		}

		return
	}

	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}

		if len(j.change.Add) == 0 && len(j.change.Remove) == 0 {
			j.applied = true

			continue
		}

		err := client.ApplyBackends(lbID, []BackendChange{j.change})
		j.applied = err == nil || (errors.Is(err, ErrTargetGone) && len(j.change.Add) == 0)
	}
}

func (b *InMemoryBackend) commitLB(jobs []*lbJob) {
	b.mu.Lock("commitLB")
	defer b.mu.Unlock()

	for _, j := range jobs {
		if !j.applied {
			continue
		}

		p, ok := b.res[j.poolID].(*InstancePool)
		if !ok {
			continue
		}

		for i := range p.LoadBalancers {
			a := &p.LoadBalancers[i]
			if a.ID != j.attID {
				continue
			}

			a.Backends = j.want

			if a.LifecycleState == lbAttaching && j.endState == lbAttached {
				a.LifecycleState = lbAttached
			}

			if j.endState == lbDetached {
				a.LifecycleState = lbDetached
				a.Backends = nil
			}

			b.bump(p)
		}
	}
}

// SyncLoadBalancers makes the backend sets of every attached load balancer match the pools' instances. It
// calls the load balancer without holding the backend lock, and stops when ctx is done or the backend is closed.
func (b *InMemoryBackend) SyncLoadBalancers(ctx context.Context) error {
	if err := b.life.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(b.life, cancel)
	defer stop()

	select {
	case b.syncGate <- struct{}{}:
		defer func() { <-b.syncGate }()
	case <-ctx.Done():
		return ctx.Err()
	}

	jobs, client := b.planLB()
	if len(jobs) == 0 {
		return nil
	}

	byLB := map[string][]*lbJob{}
	order := []string{}

	for _, j := range jobs {
		if _, seen := byLB[j.lbID]; !seen {
			order = append(order, j.lbID)
		}

		byLB[j.lbID] = append(byLB[j.lbID], j)
	}

	for _, id := range order {
		if err := ctx.Err(); err != nil {
			break
		}

		callLB(ctx, client, id, byLB[id])
	}

	b.commitLB(jobs)

	if err := ctx.Err(); err != nil {
		logger.Load(ctx).DebugContext(ctx, "ocicore: load balancer synchronisation interrupted", "error", err)

		return err
	}

	return nil
}

func attachLoadBalancerOp(h *Handler, q *req) error {
	var in AttachLoadBalancerIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.AttachLoadBalancer(q.param("instancePoolId"), q.ifMatch(), in)

	return send(q, v, etag, aerr)
}

func detachLoadBalancerOp(h *Handler, q *req) error {
	var in DetachLoadBalancerIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.DetachLoadBalancer(q.param("instancePoolId"), q.ifMatch(), in)

	return send(q, v, etag, aerr)
}

func getPoolLoadBalancerOp(h *Handler, q *req) error {
	a, aerr := h.Backend.GetPoolLoadBalancer(q.param("instancePoolId"), q.param("instancePoolLoadBalancerAttachmentId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, a, "")
}
