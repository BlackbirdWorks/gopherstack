package eks

import (
	"context"
	"errors"
	"maps"
	"time"
)

const (
	reconcileInterval = 5 * time.Second
	reconcilePassMax  = 3 * time.Minute
	statusCreateFail  = "CREATE_FAILED"
)

type ngDesired struct {
	createdAt    time.Time
	labels       map[string]string
	name         string
	status       string
	capacityType string
	taints       []NodegroupTaint
	desired      int
}

type addonDesired struct {
	createdAt time.Time
	name      string
	status    string
}

type desiredState struct {
	nodegroups []ngDesired
	addons     []addonDesired
	bindings   []bindingDesired
}

func (b *InMemoryBackend) snapshotDesired(cluster string) desiredState {
	b.mu.RLock("snapshotDesired")
	defer b.mu.RUnlock()

	var d desiredState

	for _, ng := range b.nodegroupsByCluster.Get(cluster) {
		if ng.Status == statusDeleting {
			continue
		}

		labels := make(map[string]string, len(ng.Labels))
		maps.Copy(labels, ng.Labels)

		d.nodegroups = append(d.nodegroups, ngDesired{
			name: ng.NodegroupName, desired: int(ng.DesiredSize), labels: labels, status: ng.Status,
			taints: append([]NodegroupTaint(nil), ng.Taints...), capacityType: ng.CapacityType, createdAt: ng.CreatedAt,
		})
	}

	for _, a := range b.addonsByCluster.Get(cluster) {
		d.addons = append(d.addons, addonDesired{name: a.AddonName, status: a.Status, createdAt: a.CreatedAt})
	}

	d.bindings = b.desiredBindingsLocked(cluster)

	return d
}

// kickClusterLocked asks the cluster's reconciler to run a pass now. Must be called with b.mu held.
func (b *InMemoryBackend) kickClusterLocked(cluster string) {
	if b.clusterEng == nil {
		return
	}

	if lc, ok := b.clusterEng.clusters[cluster]; ok {
		select {
		case lc.kick <- struct{}{}:
		default:
		}
	}
}

// clusterLiveLocked reports whether a docker engine owns the cluster, so real components drive resource status.
func (b *InMemoryBackend) clusterLiveLocked(cluster string) bool {
	if b.clusterEng == nil {
		return false
	}

	_, ok := b.clusterEng.clusters[cluster]

	return ok
}

type reconciler struct {
	b       *InMemoryBackend
	e       *clusterEngine
	lc      *liveCluster
	api     *k8sAPI
	warned  map[string]string
	name    string
	version string
}

func (b *InMemoryBackend) runReconciler(ctx context.Context, name, version string, lc *liveCluster) {
	r := &reconciler{
		b:       b,
		e:       b.clusterEng,
		lc:      lc,
		api:     lc.api,
		name:    name,
		version: version,
		warned:  map[string]string{},
	}

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		r.pass(ctx)

		select {
		case <-ctx.Done():
			return
		case <-lc.kick:
		case <-ticker.C:
		}
	}
}

func (r *reconciler) pass(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, reconcilePassMax)
	defer cancel()

	d := r.b.snapshotDesired(r.name)

	if err := syncBindings(ctx, r.api, d.bindings); err != nil {
		r.note(ctx, "rbac", err)
	}

	nodes, err := r.api.listNodes(ctx)
	if err != nil {
		r.note(ctx, "nodes", err)

		return
	}

	r.syncNodegroups(ctx, d.nodegroups, nodes)
	r.syncAddons(ctx, d.addons, nodes)
}

// note records a reconcile failure once per distinct message so a persistent fault does not spam the log.
func (r *reconciler) note(ctx context.Context, what string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	msg := err.Error()
	if r.warned[what] == msg {
		return
	}

	r.warned[what] = msg
	r.e.log(ctx).DebugContext(ctx, "eks: cluster reconcile failed", "cluster", r.name, "component", what, "error", err)
}

func (r *reconciler) clear(what string) { delete(r.warned, what) }

// currentLocked reports whether lc is still the registered live cluster. Must be called with b.mu held.
func (r *reconciler) currentLocked() bool { return r.b.clusterEng.clusters[r.name] == r.lc }
