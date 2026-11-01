package container

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ErrListUnsupported is returned when the runtime client cannot list containers.
var ErrListUnsupported = errors.New("container listing is not supported by this runtime client")

// ManagedContainer is a container carrying LabelManaged.
type ManagedContainer struct {
	Labels map[string]string
	// Ports maps container port to published host port.
	Ports   map[int]int
	ID      string
	Name    string
	Running bool
}

// ManagedLister is implemented by runtimes that can list gopherstack-managed containers.
type ManagedLister interface {
	ListManaged(ctx context.Context) ([]ManagedContainer, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// Orphans holds the previous run's resource-labelled containers until their services adopt them.
type Orphans struct {
	rt   ManagedLister
	log  *slog.Logger
	mu   *lockmetrics.RWMutex
	held map[orphanKey][]ManagedContainer
}

type orphanKey struct{ service, resource string }

// Reconcile lists managed containers and removes the ones that are provably dead leftovers of this instance:
//   - this owner's containers without a LabelResource, and ones whose launcher process is gone and has no
//     resource to adopt for are removed now;
//   - another owner's containers are removed only when they are ephemeral and their launcher process is dead;
//   - this owner's resource-labelled containers (launcher dead or this very process) are held for Claim;
//   - everything else, including this owner's containers whose launcher is still alive elsewhere, is untouched.
func Reconcile(ctx context.Context, rt ManagedLister, id Identity, log *slog.Logger) (*Orphans, error) {
	all, err := rt.ListManaged(ctx)
	if err != nil {
		return nil, err
	}

	o := &Orphans{rt: rt, log: log, mu: lockmetrics.New("container.orphans"), held: map[orphanKey][]ManagedContainer{}}

	for _, c := range all {
		if c.Labels[LabelManaged] != labelTrue {
			continue
		}

		pidLabel := c.Labels[LabelPID]
		ownPID := pidLabel == strconv.Itoa(id.PID)+":"+id.Start

		switch {
		case c.Labels[LabelOwner] == id.Owner:
			if !ownPID && processAlive(pidLabel) {
				continue
			}

			if c.Labels[LabelResource] == "" {
				o.remove(ctx, c)

				continue
			}

			k := orphanKey{c.Labels[LabelService], c.Labels[LabelResource]}
			o.held[k] = append(o.held[k], c)
		case c.Labels[LabelEphemeral] == labelTrue && !processAlive(pidLabel):
			o.remove(ctx, c)
		}
	}

	return o, nil
}

func (o *Orphans) remove(ctx context.Context, c ManagedContainer) {
	if err := o.rt.StopAndRemove(ctx, c.ID); err != nil && o.log != nil {
		o.log.WarnContext(ctx, "container: orphan removal failed", "id", c.ID, "name", c.Name, "error", err)
	}
}

// Claim hands the caller the held container for (service, resource), preferring a running one. The caller owns
// it from then on and must remove it itself if it decides not to adopt it.
func (o *Orphans) Claim(service, resource string) (ManagedContainer, bool) {
	if o == nil {
		return ManagedContainer{}, false
	}

	o.mu.Lock("Claim")
	defer o.mu.Unlock()

	k := orphanKey{service, resource}
	list := o.held[k]

	pick := -1

	for i, c := range list {
		if c.Running {
			pick = i

			break
		}
	}

	if pick < 0 {
		return ManagedContainer{}, false
	}

	c := list[pick]
	o.held[k] = append(list[:pick:pick], list[pick+1:]...)

	return c, true
}

// SweepUnclaimed removes every container still held and returns how many were removed.
func (o *Orphans) SweepUnclaimed(ctx context.Context) int {
	if o == nil {
		return 0
	}

	o.mu.Lock("SweepUnclaimed")
	held := o.held
	o.held = map[orphanKey][]ManagedContainer{}
	o.mu.Unlock()

	n := 0

	for _, list := range held {
		for _, c := range list {
			o.remove(ctx, c)

			n++
		}
	}

	return n
}

var defaultOrphans atomic.Pointer[Orphans] //nolint:gochecknoglobals // set once at startup

// SetOrphans installs the process-wide registry services claim from.
func SetOrphans(o *Orphans) { defaultOrphans.Store(o) }

// ClaimOrphan claims from the process-wide registry; false when nothing is held.
func ClaimOrphan(service, resource string) (ManagedContainer, bool) {
	return defaultOrphans.Load().Claim(service, resource)
}

// SweepOrphans sweeps the process-wide registry.
func SweepOrphans(ctx context.Context) int { return defaultOrphans.Load().SweepUnclaimed(ctx) }

// ListManaged lists the containers carrying LabelManaged, running or not.
func (r *DockerRuntime) ListManaged(ctx context.Context) ([]ManagedContainer, error) {
	api, ok := r.docker.(ListAPIClient)
	if !ok {
		return nil, ErrListUnsupported
	}

	return api.ContainerListByLabel(ctx, LabelManaged+"=true")
}
