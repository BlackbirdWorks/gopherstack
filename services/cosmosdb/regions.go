package cosmosdb

import (
	"slices"
	"strings"
)

// AccountRegion is one geo-replica of the account; the lowest failover priority is the write region.
type AccountRegion struct {
	Name             string `json:"name"`
	FailoverPriority int    `json:"failoverPriority"`
}

// AccountRegions is the account's regional topology. Every region serves the same single in-memory store.
type AccountRegions struct {
	Regions    []AccountRegion `json:"regions,omitempty"`
	MultiWrite bool            `json:"multiWrite,omitempty"`
}

// normalized drops blank and duplicate names and orders by failover priority; an empty set means the default
// single region.
func (r AccountRegions) normalized() AccountRegions {
	out := AccountRegions{MultiWrite: r.MultiWrite}
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

		out.Regions = append(
			out.Regions,
			AccountRegion{Name: strings.TrimSpace(reg.Name), FailoverPriority: reg.FailoverPriority},
		)
	}

	slices.SortStableFunc(out.Regions, func(a, b AccountRegion) int { return a.FailoverPriority - b.FailoverPriority })

	return out
}

// SetAccountRegions replaces the regional topology reported by the account root.
func (b *InMemoryBackend) SetAccountRegions(r AccountRegions) {
	b.mu.Lock("SetAccountRegions")
	defer b.mu.Unlock()

	b.regions = r.normalized()
}

// AccountRegions returns the topology, the default single region when none was set.
func (b *InMemoryBackend) AccountRegions() AccountRegions {
	b.mu.RLock("AccountRegions")
	defer b.mu.RUnlock()

	if len(b.regions.Regions) == 0 {
		return AccountRegions{Regions: []AccountRegion{{Name: accountRegionName}}, MultiWrite: b.regions.MultiWrite}
	}

	return AccountRegions{Regions: slices.Clone(b.regions.Regions), MultiWrite: b.regions.MultiWrite}
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

// locationEntries renders regions as the root resource's location list; all share the endpoint.
func locationEntries(regions []AccountRegion, endpoint string) []map[string]any {
	out := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		out = append(out, map[string]any{"name": r.Name, "databaseAccountEndpoint": endpoint})
	}

	return out
}
