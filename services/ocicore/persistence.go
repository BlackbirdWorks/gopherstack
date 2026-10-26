package ocicore

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to a stored type would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Vcns      map[string]*Vcn                  `json:"vcns"`
	Subnets   map[string]*Subnet               `json:"subnets"`
	IGWs      map[string]*InternetGateway      `json:"internetGateways"`
	NATs      map[string]*NatGateway           `json:"natGateways"`
	SGWs      map[string]*ServiceGateway       `json:"serviceGateways"`
	RouteTabs map[string]*RouteTable           `json:"routeTables"`
	SecLists  map[string]*SecurityList         `json:"securityLists"`
	NSGs      map[string]*NetworkSecurityGroup `json:"networkSecurityGroups"`
	NSGRules  map[string]*NSGRule              `json:"securityRules"`
	Dhcp      map[string]*DhcpOptions          `json:"dhcpOptions"`
	LPGs      map[string]*LocalPeeringGateway  `json:"localPeeringGateways"`
	PublicIPs map[string]*PublicIP             `json:"publicIps"`
	Auth      []ociauth.KeyRecord              `json:"authKeys"`
	Version   int                              `json:"version"`
	Seq       int64                            `json:"seq"`
	IPSeq     int64                            `json:"ipSeq"`
	IPv6Seq   int64                            `json:"ipv6Seq"`
}

func typed[T resource](res map[string]resource) map[string]T {
	out := map[string]T{}

	for id, r := range res {
		if t, ok := r.(T); ok {
			out[id] = t
		}
	}

	return out
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocicore", backendSnapshot{
		Version: snapshotVersion, Vcns: typed[*Vcn](b.res), Subnets: typed[*Subnet](b.res),
		IGWs: typed[*InternetGateway](b.res), NATs: typed[*NatGateway](b.res), SGWs: typed[*ServiceGateway](b.res),
		RouteTabs: typed[*RouteTable](b.res), SecLists: typed[*SecurityList](b.res),
		NSGs: typed[*NetworkSecurityGroup](b.res), NSGRules: b.nsgRules, Dhcp: typed[*DhcpOptions](b.res),
		LPGs: typed[*LocalPeeringGateway](b.res), PublicIPs: typed[*PublicIP](b.res), Auth: keys,
		Seq: b.seq, IPSeq: b.ipSeq, IPv6Seq: b.ipv6Seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocicore", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocicore: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	res := map[string]resource{}

	if err := mergeAll(res, &snap); err != nil {
		return err
	}

	if snap.NSGRules == nil {
		snap.NSGRules = map[string]*NSGRule{}
	}

	for id, r := range snap.NSGRules {
		if r == nil {
			return fmt.Errorf("%w: security rule %q", ErrSnapshotNull, id)
		}
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.res, b.nsgRules = res, snap.NSGRules
	b.seq, b.ipSeq, b.ipv6Seq = snap.Seq, snap.IPSeq, snap.IPv6Seq
	b.Auth.Load(snap.Auth)

	return nil
}

func mergeKind[T resource](kind string, dst map[string]resource, src map[string]T) error {
	for id, r := range src {
		if isNil(r) || r.base().Internal == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}

		dst[id] = r
	}

	return nil
}

func isNil[T resource](r T) bool {
	var zero T

	return any(r) == any(zero)
}

func mergeAll(dst map[string]resource, s *backendSnapshot) error {
	checks := []error{
		mergeKind("vcn", dst, s.Vcns), mergeKind("subnet", dst, s.Subnets),
		mergeKind("internet gateway", dst, s.IGWs), mergeKind("NAT gateway", dst, s.NATs),
		mergeKind("service gateway", dst, s.SGWs), mergeKind("route table", dst, s.RouteTabs),
		mergeKind("security list", dst, s.SecLists), mergeKind("network security group", dst, s.NSGs),
		mergeKind("DHCP options", dst, s.Dhcp), mergeKind("local peering gateway", dst, s.LPGs),
		mergeKind("public IP", dst, s.PublicIPs),
	}

	for _, err := range checks {
		if err != nil {
			return err
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocicore: restore snapshot: %w", err)
	}

	return nil
}
