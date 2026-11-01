package app

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	orphanListTimeout = 15 * time.Second
	orphanSweepDelay  = 3 * time.Minute
)

// initContainerIdentity pins the container owner id to the data dir when persistence is on, so a restart adopts
// its predecessor's containers; otherwise the id is fresh per run and nothing is adopted.
func initContainerIdentity(persist bool, dataDir string) {
	if persist {
		container.SetPersistentOwner(dataDir)

		return
	}

	container.SetEphemeralOwner()
}

// reconcileContainers removes provably dead leftovers of earlier runs and holds this owner's resource-labelled
// containers for the services to adopt while they restore; whatever is not claimed is removed after a grace period.
func reconcileContainers(ctx context.Context) {
	log := logger.Load(ctx)

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		return
	}

	defer rt.Close()

	lister, ok := rt.(container.ManagedLister)
	if !ok {
		return
	}

	lctx, cancel := context.WithTimeout(ctx, orphanListTimeout)
	defer cancel()

	if err = rt.Ping(lctx); err != nil {
		return
	}

	orphans, err := container.Reconcile(lctx, lister, container.CurrentIdentity(), log)
	if err != nil {
		log.WarnContext(ctx, "container reconcile failed", "error", err)

		return
	}

	container.SetOrphans(orphans)

	go func() {
		t := time.NewTimer(orphanSweepDelay)
		defer t.Stop()

		select {
		case <-ctx.Done():
		case <-t.C:
			if n := container.SweepOrphans(ctx); n > 0 {
				log.InfoContext(ctx, "removed unclaimed containers from an earlier run", "count", n)
			}
		}
	}()
}
