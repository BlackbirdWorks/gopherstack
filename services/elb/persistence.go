package elb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// lbSnapshot is the serialisable form of a LoadBalancer (Tags excluded; re-created on Restore).
type lbSnapshot struct {
	CreatedTime               time.Time                  `json:"createdTime"`
	HealthCheck               *HealthCheck               `json:"healthCheck,omitempty"`
	LoadBalancerName          string                     `json:"loadBalancerName"`
	AccountID                 string                     `json:"accountId"`
	Region                    string                     `json:"region"`
	CanonicalHostedZoneName   string                     `json:"canonicalHostedZoneName"`
	CanonicalHostedZoneNameID string                     `json:"canonicalHostedZoneNameID"`
	Scheme                    string                     `json:"scheme"`
	ARN                       string                     `json:"arn"`
	VPCId                     string                     `json:"vpcId"`
	DNSName                   string                     `json:"dnsName"`
	Listeners                 []Listener                 `json:"listeners"`
	Instances                 []Instance                 `json:"instances"`
	BackendServerDescriptions []BackendServerDescription `json:"backendServerDescriptions,omitempty"`
	AvailabilityZones         []string                   `json:"availabilityZones"`
	SecurityGroups            []string                   `json:"securityGroups"`
	Subnets                   []string                   `json:"subnets"`
	TagPairs                  []tagPair                  `json:"tags,omitempty"`
	Attributes                LoadBalancerAttributes     `json:"attributes"`
	IsVPC                     bool                       `json:"isVpc,omitempty"`
}

// tagPair serialises a single key-value tag for persistence.
type tagPair struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// backendSnapshot is the top-level JSON structure for Snapshot/Restore.
//
// Version 2 added IsVPC to lbSnapshot. Version 3 nested LoadBalancers and
// Policies by region (outer key = region) so that region-isolated state
// round-tripped correctly. Version 4 (Phase 3.3, the pkgs/store datalayer
// conversion -- see store_setup.go) replaced that nesting with flat lists:
// both b.lbs and b.policies are now [store.Table]s keyed by a composite
// "region|..." key (lbTableKey / policyTableKey), so region-scoping lives in
// each element (lbSnapshot.Region / LoadBalancerPolicy.Region) rather than in
// an outer map layer. This is an incompatible shape change from Version 3 --
// see the Version guard in Restore.
const snapshotVersion = 4

type backendSnapshot struct {
	AccountID     string                `json:"accountId"`
	Region        string                `json:"region"`
	LoadBalancers []*lbSnapshot         `json:"loadBalancers"`
	Policies      []*LoadBalancerPolicy `json:"policies"`
	Version       int                   `json:"version,omitempty"`
}

// toLBSnapshot converts an in-memory LoadBalancer to its serialisable form.
func toLBSnapshot(lb *LoadBalancer) *lbSnapshot {
	s := &lbSnapshot{
		CreatedTime:               lb.CreatedTime,
		HealthCheck:               lb.HealthCheck,
		ARN:                       lb.ARN,
		VPCId:                     lb.VPCId,
		Region:                    lb.Region,
		CanonicalHostedZoneName:   lb.CanonicalHostedZoneName,
		CanonicalHostedZoneNameID: lb.CanonicalHostedZoneNameID,
		Scheme:                    lb.Scheme,
		LoadBalancerName:          lb.LoadBalancerName,
		AccountID:                 lb.AccountID,
		DNSName:                   lb.DNSName,
		Listeners:                 lb.Listeners,
		Instances:                 lb.Instances,
		BackendServerDescriptions: lb.BackendServerDescriptions,
		AvailabilityZones:         lb.AvailabilityZones,
		SecurityGroups:            lb.SecurityGroups,
		Subnets:                   lb.Subnets,
		Attributes:                lb.Attributes,
		IsVPC:                     lb.IsVPC,
	}

	if lb.Tags != nil {
		lb.Tags.Range(func(k, v string) bool {
			s.TagPairs = append(s.TagPairs, tagPair{Key: k, Value: v})

			return true
		})
	}

	return s
}

// fromLBSnapshot reconstructs a LoadBalancer from its snapshot form.
func fromLBSnapshot(s *lbSnapshot) *LoadBalancer {
	lb := &LoadBalancer{
		CreatedTime:               s.CreatedTime,
		HealthCheck:               s.HealthCheck,
		ARN:                       s.ARN,
		VPCId:                     s.VPCId,
		Region:                    s.Region,
		CanonicalHostedZoneName:   s.CanonicalHostedZoneName,
		CanonicalHostedZoneNameID: s.CanonicalHostedZoneNameID,
		Scheme:                    s.Scheme,
		LoadBalancerName:          s.LoadBalancerName,
		AccountID:                 s.AccountID,
		DNSName:                   s.DNSName,
		Listeners:                 s.Listeners,
		Instances:                 s.Instances,
		BackendServerDescriptions: s.BackendServerDescriptions,
		AvailabilityZones:         s.AvailabilityZones,
		SecurityGroups:            s.SecurityGroups,
		Subnets:                   s.Subnets,
		Attributes:                s.Attributes,
		IsVPC:                     s.IsVPC,
		Tags:                      tags.New("elb." + s.LoadBalancerName),
	}

	// Restore non-nil slices.
	if lb.Listeners == nil {
		lb.Listeners = []Listener{}
	}

	if lb.Instances == nil {
		lb.Instances = []Instance{}
	}

	if lb.AvailabilityZones == nil {
		lb.AvailabilityZones = []string{}
	}

	if lb.SecurityGroups == nil {
		lb.SecurityGroups = []string{}
	}

	if lb.Subnets == nil {
		lb.Subnets = []string{}
	}

	if lb.BackendServerDescriptions == nil {
		lb.BackendServerDescriptions = []BackendServerDescription{}
	}

	for _, tp := range s.TagPairs {
		lb.Tags.Set(tp.Key, tp.Value)
	}

	return lb
}

// Snapshot serialises the backend state to JSON.
//
// LoadBalancers is populated from a hand-built DTO list (lbSnapshot), not via
// b.registry.SnapshotAll(), because LoadBalancer carries a live *tags.Tags
// pointer whose zero-value JSON round-trip would reattach it to a generic
// "json.tags" Prometheus label instead of the per-resource "elb.<name>" label
// every other Tags-creation path in this backend uses (see fromLBSnapshot).
// Policies has no such live state, so it round-trips directly as a plain
// b.policies.Snapshot() -- a "clean" table in Phase 3.3 terms.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	lbs := b.lbs.Snapshot()
	lbSnaps := make([]*lbSnapshot, len(lbs))
	for i, lb := range lbs {
		lbSnaps[i] = toLBSnapshot(lb)
	}

	snap := backendSnapshot{
		LoadBalancers: lbSnaps,
		Policies:      b.policies.Snapshot(),
		AccountID:     b.accountID,
		Region:        b.region,
		Version:       snapshotVersion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "elb: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elb", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- Version 3 and earlier
		// nested LoadBalancers/Policies by region, a structurally different
		// (and, for LoadBalancers, no longer even type-compatible) shape from
		// the flat, composite-keyed lists Version 4 introduced. Discard
		// cleanly and start empty instead of erroring, since this is an
		// expected, recoverable condition (e.g. upgrading gopherstack across a
		// snapshot-format change), not data corruption. Mirrors the
		// services/ec2, services/sqs, and services/elbv2 pilots.
		logger.Load(ctx).WarnContext(ctx,
			"elb: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		for _, lb := range b.lbs.All() {
			if lb.Tags != nil {
				lb.Tags.Close()
			}
		}

		b.registry.ResetAll()

		return nil
	}

	// Close tags of any existing LBs before overwriting.
	for _, lb := range b.lbs.All() {
		if lb.Tags != nil {
			lb.Tags.Close()
		}
	}

	newLBs := make([]*LoadBalancer, len(snap.LoadBalancers))
	for i, s := range snap.LoadBalancers {
		newLBs[i] = fromLBSnapshot(s)
	}

	b.lbs.Restore(newLBs)
	b.policies.Restore(snap.Policies)
	b.accountID = snap.AccountID

	// Only adopt the persisted region when the backend has no region set yet,
	// preventing region drift when a snapshot from a different region is loaded
	// into an already-initialised backend.
	if b.region == "" {
		b.region = snap.Region
	}

	return nil
}
