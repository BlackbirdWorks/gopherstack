package eks

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// sharedRuntime hides the runtime's Close so only the home backend closes the shared runtime.
type sharedRuntime struct{ ClusterRuntime }

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home := h.Backend
	if home == nil {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(home.baseCtx, home.accountID, region)

		if cfg, on := home.engineConfig(); on {
			cfg.Runtime = sharedRuntime{cfg.Runtime}
			_ = nb.EnableClusters(cfg)
		}

		return NewHandler(nb)
	})
}

func (b *InMemoryBackend) engineConfig() (ClusterEngineConfig, bool) {
	b.mu.RLock("engineConfig")
	defer b.mu.RUnlock()

	if b.clusterEng == nil {
		return ClusterEngineConfig{}, false
	}

	return b.clusterEng.cfg, true
}

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) *InMemoryBackend {
	return h.RegionHandler(region).Backend
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []*InMemoryBackend {
	peers := h.peers.All()
	out := make([]*InMemoryBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}

// Shutdown stops every region's timers and cluster containers.
func (h *Handler) Shutdown(_ context.Context) {
	for _, p := range h.peers.All() {
		p.Backend.Close()
	}

	h.Backend.Close()
}
