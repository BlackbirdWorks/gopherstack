package ociloadbalancer

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	LoadBalancers map[string]*LoadBalancer `json:"loadBalancers"`
	WorkReqs      map[string]*WorkRequest  `json:"workRequests"`
	Auth          []ociauth.KeyRecord      `json:"authKeys"`
	Version       int                      `json:"version"`
	Seq           int64                    `json:"seq"`
	IPSeq         int64                    `json:"ipSeq"`
}

// Snapshot serialises the backend state, including registered API keys. TLS private keys are never persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ociloadbalancer", backendSnapshot{
		Version: snapshotVersion, LoadBalancers: b.lbs, WorkReqs: b.workReqs, Auth: keys, Seq: b.seq, IPSeq: b.ipSeq,
	})
}

// Restore loads backend state from a snapshot and reopens the data-plane listeners of active load balancers.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ociloadbalancer", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ociloadbalancer: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.dp.reset()
	b.mu.Lock("Restore")
	b.lbs, b.workReqs, b.seq, b.ipSeq = snap.LoadBalancers, snap.WorkReqs, snap.Seq, snap.IPSeq
	b.keys = map[string]*tls.Certificate{}
	b.mu.Unlock()
	b.Auth.Load(snap.Auth)
	b.dp.sync()

	return nil
}

func nullCheck[T any](kind string, m *map[string]*T) error {
	if *m == nil {
		*m = map[string]*T{}
	}

	for id, v := range *m {
		if v == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}
	}

	return nil
}

func checkSnapshot(snap *backendSnapshot) error {
	if err := nullCheck("load balancer", &snap.LoadBalancers); err != nil {
		return err
	}

	for _, lb := range snap.LoadBalancers {
		lb.fillMaps()
	}

	return nullCheck("work request", &snap.WorkReqs)
}

func (lb *LoadBalancer) fillMaps() {
	if lb.Listeners == nil {
		lb.Listeners = map[string]*Listener{}
	}

	if lb.Hostnames == nil {
		lb.Hostnames = map[string]*Hostname{}
	}

	if lb.SSLCipherSuites == nil {
		lb.SSLCipherSuites = map[string]*SSLCipherSuite{}
	}

	if lb.Certificates == nil {
		lb.Certificates = map[string]*Certificate{}
	}

	if lb.BackendSets == nil {
		lb.BackendSets = map[string]*BackendSet{}
	}

	if lb.PathRouteSets == nil {
		lb.PathRouteSets = map[string]*PathRouteSet{}
	}

	if lb.RuleSets == nil {
		lb.RuleSets = map[string]*RuleSet{}
	}

	if lb.RoutingPolicies == nil {
		lb.RoutingPolicies = map[string]*RoutingPolicy{}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ociloadbalancer: restore snapshot: %w", err)
	}

	return nil
}
