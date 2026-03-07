package ecs

import (
	"context"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const reconcileInterval = 5 * time.Second

// Reconciler manages ECS service desired-count reconciliation.
// It runs in the background, comparing DesiredCount vs running task count
// and starting or stopping tasks as needed.
type Reconciler struct {
	backend  *InMemoryBackend
	interval time.Duration
}

// NewReconciler creates a new Reconciler for the given backend.
func NewReconciler(backend *InMemoryBackend) *Reconciler {
	return &Reconciler{
		backend:  backend,
		interval: reconcileInterval,
	}
}

// Start launches the reconciliation loop. It runs until ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	log := logger.Load(ctx)
	if log == nil {
		log = slog.Default()
	}

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
	log := logger.Load(ctx)
	if log == nil {
		log = slog.Default()
	}

	r.reconcile(ctx, log)
}

// reconcile performs a single pass over all services.
func (r *Reconciler) reconcile(ctx context.Context, log *slog.Logger) {
	snapshots := r.backend.getServicesForReconciler()

	for _, snap := range snapshots {
		if err := r.reconcileService(ctx, log, snap); err != nil {
			log.WarnContext(ctx, "ECS reconcile error",
				"cluster", snap.clusterName,
				"service", snap.service.ServiceName,
				"error", err,
			)
		}
	}
}

// reconcileService ensures the running task count matches the desired count.
func (r *Reconciler) reconcileService(ctx context.Context, log *slog.Logger, snap serviceSnapshot) error {
	svc := snap.service

	if svc.Status != "ACTIVE" {
		return nil
	}

	running := r.backend.CountRunningTasksForService(snap.clusterName, svc.ServiceName)
	desired := svc.DesiredCount

	switch {
	case running < desired:
		toStart := desired - running
		log.DebugContext(ctx, "ECS reconciler: starting tasks",
			"cluster", snap.clusterName,
			"service", svc.ServiceName,
			"desired", desired,
			"running", running,
			"toStart", toStart,
		)

		for range toStart {
			if err := r.backend.StartTaskForService(snap.clusterName, svc.ServiceName, svc.TaskDefinition); err != nil {
				return err
			}
		}

	case running > desired:
		toStop := running - desired
		log.DebugContext(ctx, "ECS reconciler: stopping tasks",
			"cluster", snap.clusterName,
			"service", svc.ServiceName,
			"desired", desired,
			"running", running,
			"toStop", toStop,
		)

		for range toStop {
			if err := r.backend.StopOldestServiceTask(snap.clusterName, svc.ServiceName); err != nil {
				return err
			}
		}
	}

	return nil
}
