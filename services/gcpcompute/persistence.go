package gcpcompute

import (
	"context"
	"fmt"

	computeapi "google.golang.org/api/compute/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot would make an older one unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Networks  map[string]*computeapi.Network          `json:"networks"`
	Subnets   map[string]*computeapi.Subnetwork       `json:"subnets"`
	Firewalls map[string]*computeapi.Firewall         `json:"firewalls"`
	Routes    map[string]*computeapi.Route            `json:"routes"`
	Addresses map[string]*computeapi.Address          `json:"addresses"`
	Routers   map[string]*routerRec                   `json:"routers"`
	Disks     map[string]*computeapi.Disk             `json:"disks"`
	Images    map[string]*computeapi.Image            `json:"images"`
	Snapshots map[string]*computeapi.Snapshot         `json:"snapshots"`
	Instances map[string]*instRec                     `json:"instances"`
	Templates map[string]*computeapi.InstanceTemplate `json:"templates"`
	Healths   map[string]*computeapi.HealthCheck      `json:"healthChecks"`
	Migs      map[string]*migRec                      `json:"instanceGroupManagers"`
	Groups    map[string]*groupRec                    `json:"instanceGroups"`
	Scalers   map[string]*computeapi.Autoscaler       `json:"autoscalers"`
	Ops       map[string]*opRec                       `json:"ops"`
	Projects  map[string]*computeapi.Metadata         `json:"projects"`
	Version   int                                     `json:"version"`
}

// Snapshot serialises the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpcompute", backendSnapshot{
		Version: snapshotVersion, Networks: b.networks, Subnets: b.subnets, Firewalls: b.firewalls,
		Routes: b.routes, Addresses: b.addresses, Routers: b.routers, Disks: b.disks, Images: b.images,
		Snapshots: b.snapshots, Instances: b.instances, Ops: b.ops, Projects: b.projects,
		Templates: b.templates, Healths: b.healths, Migs: b.migs, Groups: b.groups, Scalers: b.scalers,
	})
}

func copyNonNil[T any](dst, src map[string]*T) error {
	for k, v := range src {
		if v == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotNull, k)
		}

		dst[k] = v
	}

	return nil
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpcompute", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpcompute: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		return nil
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.resetMaps()

	for _, err := range []error{
		copyNonNil(b.networks, snap.Networks), copyNonNil(b.subnets, snap.Subnets),
		copyNonNil(b.firewalls, snap.Firewalls), copyNonNil(b.routes, snap.Routes),
		copyNonNil(b.addresses, snap.Addresses), copyNonNil(b.routers, snap.Routers),
		copyNonNil(b.disks, snap.Disks), copyNonNil(b.images, snap.Images),
		copyNonNil(b.snapshots, snap.Snapshots), copyNonNil(b.instances, snap.Instances),
		copyNonNil(b.ops, snap.Ops), copyNonNil(b.projects, snap.Projects),
		copyNonNil(b.templates, snap.Templates), copyNonNil(b.healths, snap.Healths),
		copyNonNil(b.migs, snap.Migs), copyNonNil(b.groups, snap.Groups), copyNonNil(b.scalers, snap.Scalers),
	} {
		if err != nil {
			b.resetMaps()

			return err
		}
	}

	for _, r := range b.routers {
		if r.Router == nil {
			b.resetMaps()

			return fmt.Errorf("%w: router", ErrSnapshotNull)
		}
	}

	for _, r := range b.instances {
		if r.Inst == nil {
			b.resetMaps()

			return fmt.Errorf("%w: instance", ErrSnapshotNull)
		}
	}

	for _, r := range b.migs {
		if r.Mgr == nil {
			b.resetMaps()

			return fmt.Errorf("%w: instance group manager", ErrSnapshotNull)
		}
	}

	for _, r := range b.groups {
		if r.Group == nil {
			b.resetMaps()

			return fmt.Errorf("%w: instance group", ErrSnapshotNull)
		}
	}

	for _, r := range b.ops {
		if r.Op == nil {
			b.resetMaps()

			return fmt.Errorf("%w: operation", ErrSnapshotNull)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpcompute: restore snapshot: %w", err)
	}

	return nil
}
