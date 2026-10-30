package cosmosdb

import (
	"errors"
	"slices"
	"strings"
	"time"
)

// Consistency levels, strongest first (Learn, "Consistency levels in Azure Cosmos DB").
const (
	ConsistencyStrong           = "Strong"
	ConsistencyBoundedStaleness = "BoundedStaleness"
	ConsistencySession          = "Session"
	ConsistencyConsistentPrefix = "ConsistentPrefix"
	ConsistencyEventual         = "Eventual"
)

// Bounded staleness limits when a policy names none (the service minimums are 10 versions and 5 seconds; the
// defaults here are the Learn portal defaults of 100,000 versions and 300 seconds).
const (
	defaultStalenessPrefix = 100_000
	defaultStalenessSecs   = 300
)

// minReplicatedRegions is the region count from which an account can hold separate copies.
const minReplicatedRegions = 2

// ErrUnknownRegion is returned for an operation naming a region the account does not have.
var ErrUnknownRegion = errors.New("cosmosdb: unknown region")

// AccountRegion is one geo-replica of the account. The lowest failover priority among online regions is the write
// region; ReplicationLagMs is how long a write elsewhere takes to arrive here.
type AccountRegion struct {
	Name             string `json:"name"`
	FailoverPriority int    `json:"failoverPriority"`
	ReplicationLagMs int    `json:"replicationLagMs,omitempty"`
	Offline          bool   `json:"offline,omitempty"`
}

// ConsistencyPolicy is the account default consistency (userConsistencyPolicy on the account root).
type ConsistencyPolicy struct {
	Level                string `json:"defaultConsistencyLevel"`
	MaxStalenessPrefix   int64  `json:"maxStalenessPrefix,omitempty"`
	MaxIntervalInSeconds int    `json:"maxIntervalInSeconds,omitempty"`
}

// AccountRegions is the account's regional topology.
type AccountRegions struct {
	Consistency    *ConsistencyPolicy `json:"consistency,omitempty"`
	Write          string             `json:"write,omitempty"`
	Backup         string             `json:"backup,omitempty"`
	Regions        []AccountRegion    `json:"regions,omitempty"`
	MultiWrite     bool               `json:"multiWrite,omitempty"`
	NoAutoFailover bool               `json:"noAutoFailover,omitempty"`
}

// normalized drops blank and duplicate names and orders by failover priority; an empty set means the default
// single region.
func (r AccountRegions) normalized() AccountRegions {
	out := AccountRegions{
		MultiWrite:     r.MultiWrite,
		Consistency:    r.Consistency,
		NoAutoFailover: r.NoAutoFailover,
		Write:          r.Write,
		Backup:         r.Backup,
	}
	seen := map[string]struct{}{}

	for _, reg := range r.Regions {
		key := strings.ToLower(strings.TrimSpace(reg.Name))
		if key == "" {
			continue
		}

		if _, dup := seen[key]; dup {
			continue
		}

		seen[key] = struct{}{}
		reg.Name = strings.TrimSpace(reg.Name)
		out.Regions = append(out.Regions, reg)
	}

	slices.SortStableFunc(out.Regions, func(a, b AccountRegion) int { return a.FailoverPriority - b.FailoverPriority })

	return out
}

// mergedRuntime carries the runtime settings (lag, offline state, consistency) of next's regions over from r, so
// an ARM topology update that names only regions and priorities keeps them.
func (r AccountRegions) mergedRuntime(next AccountRegions) AccountRegions {
	for i, reg := range next.Regions {
		for _, old := range r.Regions {
			if strings.EqualFold(old.Name, reg.Name) {
				next.Regions[i].ReplicationLagMs = old.ReplicationLagMs
				next.Regions[i].Offline = old.Offline

				break
			}
		}
	}

	if next.Consistency == nil {
		next.Consistency = r.Consistency
	}

	next.NoAutoFailover = r.NoAutoFailover
	next.Backup = r.Backup

	return next
}

func (r AccountRegions) consistency() ConsistencyPolicy {
	if r.Consistency != nil && r.Consistency.Level != "" {
		return *r.Consistency
	}

	return ConsistencyPolicy{Level: ConsistencySession}
}

// home is the write region: the lowest failover priority among online regions.
func (r AccountRegions) home() string {
	if reg, _, ok := r.find(r.Write); ok && (!reg.Offline || r.NoAutoFailover) {
		return reg.Name
	}

	return r.firstOnline()
}

// firstOnline is the online region with the lowest failover priority.
func (r AccountRegions) firstOnline() string {
	for _, reg := range r.Regions {
		if !reg.Offline {
			return reg.Name
		}
	}

	if len(r.Regions) > 0 {
		return r.Regions[0].Name
	}

	return accountRegionName
}

// withWriteRegion pins the write region to the lowest failover priority among online regions (a topology that was
// just set or whose priorities changed).
func (r AccountRegions) withWriteRegion() AccountRegions {
	r.Write = r.firstOnline()

	if len(r.Regions) == 0 {
		r.Write = ""
	}

	return r
}

// writers lists the regions that accept writes.
func (r AccountRegions) writers() []string {
	if !r.MultiWrite {
		return []string{r.home()}
	}

	var out []string

	for _, reg := range r.Regions {
		if !reg.Offline {
			out = append(out, reg.Name)
		}
	}

	if len(out) == 0 {
		return []string{r.home()}
	}

	return out
}

func (r AccountRegions) find(name string) (AccountRegion, int, bool) {
	for i, reg := range r.Regions {
		if strings.EqualFold(reg.Name, name) {
			return reg, i, true
		}
	}

	return AccountRegion{}, 0, false
}

func (r AccountRegions) lag(name string) time.Duration {
	reg, _, _ := r.find(name)

	return time.Duration(reg.ReplicationLagMs) * time.Millisecond
}

// replicated reports whether regions hold separate copies: more than one region and either a replication lag or
// multi-region writes. Otherwise every region serves the one store.
func (r AccountRegions) replicated() bool {
	if len(r.Regions) < minReplicatedRegions {
		return false
	}

	if r.MultiWrite {
		return true
	}

	for _, reg := range r.Regions {
		if reg.ReplicationLagMs > 0 {
			return true
		}
	}

	return false
}

// regionID is the small stable number a region has in a session token: its 1-based position by name.
func (r AccountRegions) regionID(name string) int {
	names := make([]string, 0, len(r.Regions))
	for _, reg := range r.Regions {
		names = append(names, strings.ToLower(reg.Name))
	}

	slices.Sort(names)

	return slices.Index(names, strings.ToLower(name)) + 1
}

// SetAccountRegions replaces the regional topology reported by the account root. Lag, offline state and the
// consistency policy of regions that stay are kept.
func (b *InMemoryBackend) SetAccountRegions(r AccountRegions) {
	b.mu.Lock("SetAccountRegions")
	b.setRegionsLocked(b.regions.mergedRuntime(r.normalized()).withWriteRegion())
	b.mu.Unlock()
	b.notifyRegions()
}

// ConfigureAccountRegions replaces the whole topology including lag, offline state and consistency (the admin API).
func (b *InMemoryBackend) ConfigureAccountRegions(r AccountRegions) {
	b.mu.Lock("ConfigureAccountRegions")
	b.setRegionsLocked(r.normalized().withWriteRegion())
	b.mu.Unlock()
	b.notifyRegions()
}

// SetRegionOffline marks a region offline or online. The write region going offline fails over to the next online
// region by failover priority unless the account has automatic failover disabled.
func (b *InMemoryBackend) SetRegionOffline(name string, offline bool) error {
	b.mu.Lock("SetRegionOffline")

	next := b.regions
	next.Regions = slices.Clone(b.regions.Regions)

	_, i, ok := next.find(name)
	if !ok {
		b.mu.Unlock()

		return ErrUnknownRegion
	}

	wasHome := strings.EqualFold(b.regionsView().home(), next.Regions[i].Name)
	next.Regions[i].Offline = offline

	if offline && wasHome && !next.NoAutoFailover {
		next.Write = next.firstOnline()
	}

	b.setRegionsLocked(next)
	b.mu.Unlock()
	b.notifyRegions()

	return nil
}

// AccountRegions returns the topology, the default single region when none was set.
func (b *InMemoryBackend) AccountRegions() AccountRegions {
	b.mu.RLock("AccountRegions")
	defer b.mu.RUnlock()

	return b.regionsView()
}

// regionsView is the topology with the default region filled in; the lock must be held.
func (b *InMemoryBackend) regionsView() AccountRegions {
	out := b.regions
	out.Regions = slices.Clone(b.regions.Regions)

	if len(out.Regions) == 0 {
		out.Regions = []AccountRegion{{Name: accountRegionName}}
	}

	return out
}

// regionProvider is implemented by backends that model the account's regions.
type regionProvider interface {
	AccountRegions() AccountRegions
}

func (h *Handler) accountRegions() AccountRegions {
	if rp, ok := h.Backend.(regionProvider); ok {
		return rp.AccountRegions()
	}

	return AccountRegions{Regions: []AccountRegion{{Name: accountRegionName}}}
}

// AccountPolicies are the account settings an ARM account carries into the data plane.
type AccountPolicies struct {
	Consistency        string
	Backup             string
	MaxStalenessPrefix int64
	MaxIntervalSeconds int
	AutomaticFailover  bool
}

// SetAccountPolicies applies the default consistency level, service-managed failover and backup mode.
func (b *InMemoryBackend) SetAccountPolicies(p AccountPolicies) {
	b.mu.Lock("SetAccountPolicies")

	next := b.regions
	next.Regions = slices.Clone(b.regions.Regions)
	next.Consistency = &ConsistencyPolicy{
		Level: canonicalConsistency(p.Consistency), MaxStalenessPrefix: p.MaxStalenessPrefix,
		MaxIntervalInSeconds: p.MaxIntervalSeconds,
	}

	if next.Consistency.Level == "" {
		next.Consistency.Level = ConsistencySession
	}

	next.NoAutoFailover = !p.AutomaticFailover
	next.Backup = p.Backup

	b.setRegionsLocked(next)
	b.mu.Unlock()
	b.notifyRegions()
}

// Backup modes an account reports (ARM backupPolicy): periodic backup keeps no change history, the continuous tiers
// keep it for 7 or 30 days.
const (
	backupPeriodic    = "Periodic"
	backupContinuous7 = "Continuous7Days"
	backupContinuous  = "Continuous30Days"
)

// changeRetention is how long the all-versions-and-deletes change feed keeps versions: the continuous backup window
// (Learn, "Change feed modes"). Periodic backup keeps none; a standalone emulator without an ARM account defaults to
// the 7-day tier.
func (b *InMemoryBackend) changeRetention() time.Duration {
	switch b.regions.Backup {
	case backupPeriodic:
		return noChangeLog
	case backupContinuous:
		return continuous30Retention
	}

	return defaultChangeRetention
}
