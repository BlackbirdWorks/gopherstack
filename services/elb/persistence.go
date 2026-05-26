package elb

import (
	"encoding/json"
	"log/slog"
	"time"

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
// Version 2 adds IsVPC to lbSnapshot.
const snapshotVersion = 2

type backendSnapshot struct {
	LoadBalancers map[string]*lbSnapshot         `json:"loadBalancers"`
	Policies      map[string]*LoadBalancerPolicy `json:"policies"`
	AccountID     string                         `json:"accountId"`
	Region        string                         `json:"region"`
	Version       int                            `json:"version,omitempty"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.LoadBalancers == nil {
		s.LoadBalancers = make(map[string]*lbSnapshot)
	}

	if s.Policies == nil {
		s.Policies = make(map[string]*LoadBalancerPolicy)
	}
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
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	lbSnaps := make(map[string]*lbSnapshot, len(b.lbs))
	for k, lb := range b.lbs {
		lbSnaps[k] = toLBSnapshot(lb)
	}

	snap := backendSnapshot{
		LoadBalancers: lbSnaps,
		Policies:      b.policies,
		AccountID:     b.accountID,
		Region:        b.region,
		Version:       snapshotVersion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("elb: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close tags of any existing LBs before overwriting.
	for _, lb := range b.lbs {
		if lb.Tags != nil {
			lb.Tags.Close()
		}
	}

	newLBs := make(map[string]*LoadBalancer, len(snap.LoadBalancers))
	for k, s := range snap.LoadBalancers {
		newLBs[k] = fromLBSnapshot(s)
	}

	b.lbs = newLBs
	b.policies = snap.Policies
	b.accountID = snap.AccountID

	// Only adopt the persisted region when the backend has no region set yet,
	// preventing region drift when a snapshot from a different region is loaded
	// into an already-initialised backend.
	if b.region == "" {
		b.region = snap.Region
	}

	return nil
}
