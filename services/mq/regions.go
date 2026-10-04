package mq

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// sharedRuntime hides the runtime's Close so only the home backend closes the shared runtime.
type sharedRuntime struct{ BrokerRuntime }

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(home.accountID, region)

		if cfg, on := home.brokerConfig(); on {
			cfg.Runtime = sharedRuntime{cfg.Runtime}
			nb.EnableBrokers(cfg)
		}

		return NewHandler(nb)
	})
}

func (b *InMemoryBackend) brokerConfig() (BrokerConfig, bool) {
	b.mu.RLock("brokerConfig")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return BrokerConfig{}, false
	}

	return b.engine.cfg, true
}

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	return h.RegionHandler(region).Backend
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []StorageBackend {
	peers := h.peers.All()
	out := make([]StorageBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}

// Shutdown removes every broker container of every region.
func (h *Handler) Shutdown(ctx context.Context) {
	for _, p := range h.peers.All() {
		p.shutdownBackend(ctx)
	}

	h.shutdownBackend(ctx)
}

func (h *Handler) shutdownBackend(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}
