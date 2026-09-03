package sesv2

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// warmup status constants for dedicated IP tracking.
const (
	warmupDone            = "DONE"
	warmupInProgress      = "IN_PROGRESS"
	warmupPercentComplete = 100
)

// DedicatedIP represents a single dedicated IP address and its pool/warmup state.
type DedicatedIP struct {
	IP               string `json:"ip"`
	PoolName         string `json:"poolName,omitempty"`
	WarmupStatus     string `json:"warmupStatus"`
	WarmupPercentage int    `json:"warmupPercentage"`
}

// dedicatedIPToMap renders a dedicated IP as the AWS-shaped response map.
func dedicatedIPToMap(ip *DedicatedIP) map[string]any {
	return map[string]any{
		"Ip":               ip.IP,
		"PoolName":         ip.PoolName,
		"WarmupPercentage": ip.WarmupPercentage,
		"WarmupStatus":     ip.WarmupStatus,
	}
}

// GetDedicatedIP returns the stored dedicated IP attributes. IPs that have never
// been assigned to a pool or warmed up are reported as fully warmed up, matching
// the prior stub behaviour for IPs SES manages implicitly.
func (b *InMemoryBackend) GetDedicatedIP(ip string) (map[string]any, error) {
	b.mu.RLock("GetDedicatedIP")
	defer b.mu.RUnlock()

	if d, ok := b.dedicatedIPs.Get(ip); ok {
		return dedicatedIPToMap(d), nil
	}

	return map[string]any{
		"Ip":               ip,
		"WarmupPercentage": warmupPercentComplete,
		"WarmupStatus":     warmupDone,
	}, nil
}

// GetDedicatedIps returns tracked dedicated IPs, optionally filtered by pool
// and paginated.
func (b *InMemoryBackend) GetDedicatedIps(poolName, nextToken string, pageSize int) page.Page[map[string]any] {
	b.mu.RLock("GetDedicatedIps")
	defer b.mu.RUnlock()

	snap := b.dedicatedIPs.Snapshot()

	filtered := make([]*DedicatedIP, 0, len(snap))
	for _, d := range snap {
		if poolName == "" || d.PoolName == poolName {
			filtered = append(filtered, d)
		}
	}

	sort.Slice(filtered, func(i, j int) bool { return filtered[i].IP < filtered[j].IP })

	out := make([]map[string]any, 0, len(filtered))
	for _, d := range filtered {
		out = append(out, dedicatedIPToMap(d))
	}

	return page.New(out, nextToken, pageSize, sesv2DefaultMaxItems)
}

// dedicatedIPLocked returns the tracked dedicated IP, creating a default entry if
// it does not yet exist. Callers must hold the write lock.
func (b *InMemoryBackend) dedicatedIPLocked(ip string) *DedicatedIP {
	d, ok := b.dedicatedIPs.Get(ip)
	if !ok {
		d = &DedicatedIP{
			IP:               ip,
			WarmupPercentage: warmupPercentComplete,
			WarmupStatus:     warmupDone,
		}
		b.dedicatedIPs.Put(d)
	}

	return d
}

// PutDedicatedIPInPool moves a dedicated IP into the requested pool. The
// destination pool must exist.
func (b *InMemoryBackend) PutDedicatedIPInPool(ip, poolName string) error {
	b.mu.Lock("PutDedicatedIPInPool")
	defer b.mu.Unlock()

	if !b.dedicatedIPPools.Has(poolName) {
		return fmt.Errorf("%w: dedicated IP pool %s not found", ErrNotFound, poolName)
	}

	b.dedicatedIPLocked(ip).PoolName = poolName

	return nil
}

// PutDedicatedIPWarmupAttributes records the warmup percentage for a dedicated IP
// and derives the warmup status from it.
func (b *InMemoryBackend) PutDedicatedIPWarmupAttributes(ip string, warmupPercentage int) error {
	b.mu.Lock("PutDedicatedIPWarmupAttributes")
	defer b.mu.Unlock()

	d := b.dedicatedIPLocked(ip)
	d.WarmupPercentage = warmupPercentage

	if warmupPercentage >= warmupPercentComplete {
		d.WarmupStatus = warmupDone
	} else {
		d.WarmupStatus = warmupInProgress
	}

	return nil
}
