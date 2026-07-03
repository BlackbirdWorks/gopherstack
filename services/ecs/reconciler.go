package ecs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	reconcileInterval = 5 * time.Second
	// maxConcurrentLaunches caps the number of concurrent task launches per cluster
	// to prevent unbounded goroutine fanout when Docker is slow.
	maxConcurrentLaunches = 5
)

// Reconciler manages ECS service desired-count reconciliation.
// It runs in the background, comparing DesiredCount vs running task count
// and starting or stopping tasks as needed.
type Reconciler struct {
	backend  *InMemoryBackend
	sems     map[string]chan struct{} // per-cluster launch semaphore
	semMu    sync.Mutex
	interval time.Duration
}

// NewReconciler creates a new Reconciler for the given backend.
func NewReconciler(backend *InMemoryBackend) *Reconciler {
	return &Reconciler{
		backend:  backend,
		sems:     make(map[string]chan struct{}),
		interval: reconcileInterval,
	}
}

// clusterSem returns the per-cluster launch semaphore, creating it if needed.
func (r *Reconciler) clusterSem(clusterName string) chan struct{} {
	r.semMu.Lock()
	defer r.semMu.Unlock()

	if ch, ok := r.sems[clusterName]; ok {
		return ch
	}

	ch := make(chan struct{}, maxConcurrentLaunches)
	r.sems[clusterName] = ch

	return ch
}

// EvictCluster removes the per-cluster launch semaphore for a deleted cluster.
// Without this, r.sems would grow one permanent entry per cluster ever created,
// leaking memory as clusters are created and destroyed. It is registered as a
// cluster-delete hook on the backend so eviction happens promptly on
// DeleteCluster/Purge.
func (r *Reconciler) EvictCluster(clusterName string) {
	r.semMu.Lock()
	defer r.semMu.Unlock()

	delete(r.sems, clusterName)
}

// SemCount returns the number of live per-cluster semaphores. Intended for tests
// that assert the leak fix.
func (r *Reconciler) SemCount() int {
	r.semMu.Lock()
	defer r.semMu.Unlock()

	return len(r.sems)
}

// Start launches the reconciliation loop. It runs until ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	log := logger.Load(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx, log)
		}
	}
}

// RunOnce performs a single reconciliation pass. Exported for testing.
func (r *Reconciler) RunOnce(ctx context.Context) {
	r.reconcile(ctx, logger.Load(ctx))
}

// reconcile performs a single pass over all services.
func (r *Reconciler) reconcile(ctx context.Context, log *slog.Logger) {
	// Advance any tasks that are moving through observable intermediate
	// lifecycle states (stop/start pipelines) before reconciling desired counts.
	r.backend.stepTaskLifecycle(time.Now())

	snapshots := r.backend.getServicesForReconciler()

	for _, snap := range snapshots {
		if err := r.reconcileService(ctx, log, snap); err != nil {
			log.WarnContext(
				ctx, "ECS reconcile error",
				"cluster", snap.clusterName,
				"service", snap.service.ServiceName,
				"error", err,
			)
		}
	}
}

// reconcileService ensures the running task count matches the desired count.
//
// Scale-up uses goroutine-based concurrent launches bounded by a per-cluster
// semaphore (maxConcurrentLaunches). Each launch runs in its own goroutine;
// the semaphore token is held for the lifetime of the Docker API call and
// released when the call completes. If the semaphore is already full, remaining
// launches are deferred to the next reconcile tick.
//
// MinimumHealthyPercent is honoured during scale-down: tasks are only stopped
// when doing so would not drop the running count below the floor.
func (r *Reconciler) reconcileService(
	ctx context.Context,
	log *slog.Logger,
	snap serviceSnapshot,
) error {
	svc := snap.service

	if svc.Status != statusActive {
		return nil
	}

	running := r.backend.CountRunningTasksForService(snap.clusterName, svc.ServiceName)
	desired := svc.DesiredCount

	// A deployment whose circuit breaker has tripped to FAILED without rolling
	// back is halted: AWS stops launching replacement tasks for it. Skip scale-up
	// so the reconciler does not relaunch the failing task definition forever.
	if primaryDeploymentFailed(&svc) && running < desired {
		log.DebugContext(
			ctx, "ECS reconciler: deployment FAILED, halting scale-up",
			"cluster", snap.clusterName,
			"service", svc.ServiceName,
		)

		return nil
	}

	switch {
	case running < desired:
		r.scaleUp(ctx, log, snap, running, desired)
	case running > desired:
		return r.scaleDown(ctx, log, snap, running, desired)
	}

	return nil
}

// scaleUp launches tasks concurrently (bounded by the per-cluster semaphore) to
// bring the running count up toward desired. Launches that cannot acquire a
// semaphore token are deferred to the next reconcile tick.
func (r *Reconciler) scaleUp(
	ctx context.Context,
	log *slog.Logger,
	snap serviceSnapshot,
	running, desired int,
) {
	svc := snap.service
	toStart := desired - running

	log.DebugContext(
		ctx, "ECS reconciler: starting tasks",
		"cluster", snap.clusterName,
		"service", svc.ServiceName,
		"desired", desired,
		"running", running,
		"toStart", toStart,
	)

	sem := r.clusterSem(snap.clusterName)

	var wg sync.WaitGroup

	for range toStart {
		select {
		case sem <- struct{}{}:
		default:
			// Semaphore full — defer remaining launches to next tick.
			log.DebugContext(
				ctx, "ECS reconciler: launch semaphore full, deferring",
				"cluster", snap.clusterName,
				"service", svc.ServiceName,
			)

			wg.Wait()

			return
		}

		wg.Go(func() {
			defer func() { <-sem }()

			err := r.backend.StartTaskForService(
				snap.clusterName, svc.ServiceName, svc.TaskDefinition,
			)
			if err != nil {
				log.WarnContext(
					ctx, "ECS reconciler: StartTaskForService failed",
					"cluster", snap.clusterName,
					"service", svc.ServiceName,
					"error", err,
				)
			}
		})
	}

	wg.Wait()
}

// scaleDown stops surplus tasks while honouring MinimumHealthyPercent: tasks are
// only stopped when doing so would not drop the running count below the floor.
func (r *Reconciler) scaleDown(
	ctx context.Context,
	log *slog.Logger,
	snap serviceSnapshot,
	running, desired int,
) error {
	svc := snap.service
	toStop := running - desired

	log.DebugContext(
		ctx, "ECS reconciler: stopping tasks",
		"cluster", snap.clusterName,
		"service", svc.ServiceName,
		"desired", desired,
		"running", running,
		"toStop", toStop,
	)

	minFloor := minimumHealthyFloor(svc)

	for range toStop {
		if running-1 < minFloor {
			log.DebugContext(
				ctx, "ECS reconciler: scale-down capped by MinimumHealthyPercent",
				"cluster", snap.clusterName,
				"service", svc.ServiceName,
				"running", running,
				"minFloor", minFloor,
			)

			break
		}

		if err := r.backend.StopOldestServiceTask(snap.clusterName, svc.ServiceName); err != nil {
			return err
		}

		running--
	}

	return nil
}

// minimumHealthyFloor computes the minimum number of healthy tasks that must
// remain running during scale-down, based on DeploymentConfiguration.MinimumHealthyPercent.
// Returns 0 when no configuration is set.
func minimumHealthyFloor(svc Service) int {
	if svc.DeploymentConfiguration == nil ||
		svc.DeploymentConfiguration.MinimumHealthyPercent == nil {
		return 0
	}

	pct := *svc.DeploymentConfiguration.MinimumHealthyPercent
	if pct <= 0 {
		return 0
	}

	const fullPercent = 100
	// Floor of desired * pct / 100.
	return (svc.DesiredCount * pct) / fullPercent
}
