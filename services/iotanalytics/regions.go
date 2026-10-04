package iotanalytics

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h, the home region's handler, serve every other region through per-region siblings.
func (h *Handler) EnableRegions(home string) {
	h.home = home
	h.peers = regionpeers.New(home, func(region string) *Handler {
		nb := NewInMemoryBackendWithContext(h.baseCtx())
		if h.wire != nil {
			h.wire(region, nb)
		}

		return NewHandler(nb)
	})
}

func (h *Handler) baseCtx() context.Context {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.svcCtx
	}

	return context.Background()
}

// WireRegions runs wire for the home region's backend now and for each sibling as it is built.
func (h *Handler) WireRegions(wire func(region string, b *InMemoryBackend)) {
	h.wire = wire

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		wire(h.home, b)
	}
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
