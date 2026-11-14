package ocidns

import (
	"maps"
	"slices"
)

// Compartments lists the distinct compartments that hold DNS resources.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, z := range b.zones {
		seen[z.CompartmentID] = struct{}{}
	}

	for _, v := range b.views {
		seen[v.CompartmentID] = struct{}{}
	}

	for _, r := range b.resolvers {
		seen[r.CompartmentID] = struct{}{}
	}

	for _, k := range b.tsigs {
		seen[k.CompartmentID] = struct{}{}
	}

	for _, p := range b.policies {
		seen[p.CompartmentID] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}

// DataPlanePort is the bound port of the authoritative DNS server, 0 while it is not running.
func (h *Handler) DataPlanePort() int {
	h.srvMu.RLock("DataPlanePort")
	defer h.srvMu.RUnlock()

	if h.dnsSrv == nil {
		return 0
	}

	return h.DNSPort
}
