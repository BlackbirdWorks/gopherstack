package gcpsql

import (
	"context"
	"os"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const pgPortNum = 5432

func (e *dockerEngine) resourceKey() string { return e.spec.Project + "/" + e.id }

func (e *dockerEngine) shape() string {
	return e.image + "|" + strconv.FormatInt(e.spec.MemoryBytes, 10) + "|" + strconv.FormatInt(e.spec.NanoCPUs, 10)
}

func (e *dockerEngine) ownerLabels() map[string]string {
	l := container.ResourceLabels("gcpsql", e.resourceKey())
	l[dumpDirLabel] = e.dir
	l[shapeLabel] = e.shape()

	return l
}

// adopt takes over the running container a crashed gopherstack left for this instance; false means launch fresh.
// Replicas, recoveries and fresh instances never adopt: their container is built from state the label cannot carry.
func (e *dockerEngine) adopt(ctx context.Context) bool {
	return e.adoptFrom(ctx, container.ClaimOrphan)
}

func (e *dockerEngine) adoptFrom(
	ctx context.Context,
	claim func(service, resource string) (container.ManagedContainer, bool),
) bool {
	if e.spec.Replica != nil || e.spec.Recovery != nil || e.spec.Fresh {
		return false
	}

	oc, ok := claim("gcpsql", e.resourceKey())
	if !ok {
		return false
	}

	dir, port := oc.Labels[dumpDirLabel], oc.Ports[pgPortNum]
	if !e.adoptable(oc, dir, port) {
		e.discard(ctx, oc.ID)

		return false
	}

	if e.pool != nil && e.pool.Reserve(port, "gcpsql") != nil {
		e.discard(ctx, oc.ID)

		return false
	}

	old := e.dir
	e.mu.Lock("adopt")
	e.dir, e.port, e.ctrName, e.ctrID = dir, port, oc.Name, oc.ID
	e.mu.Unlock()

	wctx, cancel := context.WithTimeout(ctx, adoptReadyWait)
	defer cancel()

	if err := e.waitReady(wctx); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: orphaned container unusable, launching fresh",
			"instance", e.id, "container", oc.ID, "error", err)
		e.discard(ctx, oc.ID)
		e.mu.Lock("adopt")
		e.dir, e.ctrID, e.port = old, "", 0
		e.mu.Unlock()

		if e.pool != nil {
			portalloc.ReleaseTo(e.pool, port)
		}

		return false
	}

	_ = os.RemoveAll(old)

	logger.Load(ctx).InfoContext(ctx, "gcpsql: adopted container from an earlier run",
		"instance", e.id, "container", oc.ID)

	return true
}

func (e *dockerEngine) adoptable(oc container.ManagedContainer, dir string, port int) bool {
	if oc.Labels[shapeLabel] != e.shape() || port == 0 || dir == "" {
		return false
	}

	st, err := os.Stat(dir)

	return err == nil && st.IsDir()
}

func (e *dockerEngine) discard(ctx context.Context, id string) {
	if err := e.rt.StopAndRemove(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: orphan removal failed", "container", id, "error", err)
	}
}
