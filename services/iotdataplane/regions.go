package iotdataplane

import "github.com/blackbirdworks/gopherstack/pkgs/regionpeers"

// EnableRegions makes h, the home region's handler, serve every other region through per-region siblings.
func (h *Handler) EnableRegions(home string) {
	h.peers = regionpeers.New(home, func(_ string) *Handler {
		nb := NewInMemoryBackend()
		nb.SetBroker(h.homeBroker())

		return NewHandler(nb)
	})
}

func (h *Handler) homeBroker() MQTTPublisher {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	b.mu.RLock("homeBroker")
	defer b.mu.RUnlock()

	return b.broker
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
