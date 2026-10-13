package awsconfig

import "github.com/blackbirdworks/gopherstack/pkgs/regionpeers"

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home := h.Backend

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackendWithMeta(home.accountID, region)
		nb.s3Writer, nb.snsPublisher = home.deliveryTargets()
		home.mu.RLock("inheritTemplates")
		nb.templates = home.templates
		home.mu.RUnlock()

		return NewHandler(nb)
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

func (b *InMemoryBackend) deliveryTargets() (S3Writer, SNSPublisher) {
	b.mu.RLock("deliveryTargets")
	defer b.mu.RUnlock()

	return b.s3Writer, b.snsPublisher
}
