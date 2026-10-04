package redshift

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// newRegionBackend builds a sibling backend in region carrying home's tuning and DNS registrar.
func newRegionBackend(home *InMemoryBackend, region string) *InMemoryBackend {
	nb := NewInMemoryBackend(home.accountID, region)

	home.mu.RLock("newRegionBackend")
	nb.dnsRegistrar = home.dnsRegistrar
	nb.clusterActivationDelay = home.clusterActivationDelay
	nb.reconcileInterval = home.reconcileInterval
	home.mu.RUnlock()

	return nb
}

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		p := NewHandler(newRegionBackend(home, region))

		if ctx := h.workerCtx.Load(); ctx != nil {
			p.Backend.StartReconciler(*ctx)
		}

		return p
	})
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

func (h *Handler) startRegionWorkers(ctx context.Context) {
	h.workerCtx.Store(&ctx)

	for _, p := range h.peers.All() {
		p.Backend.StartReconciler(ctx)
	}
}

func (h *Handler) stopRegionWorkers() {
	for _, p := range h.peers.All() {
		p.Backend.StopReconciler()
	}
}

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *ServerlessHandler) EnableRegions() {
	home := h.Backend
	h.peers = regionpeers.New(home.region, func(region string) *ServerlessHandler {
		return NewServerlessHandler(newRegionBackend(home, region))
	})
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *ServerlessHandler) BackendFor(region string) *InMemoryBackend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
}
