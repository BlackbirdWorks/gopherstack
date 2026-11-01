package awsconfig

import "github.com/blackbirdworks/gopherstack/pkgs/regionpeers"

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	h.enableRegions()

	home := h.Backend
	h.accounts = newAccountSet(home.accountID, func(account string) *Handler {
		nb := NewInMemoryBackendWithMeta(account, home.region)
		nb.s3Writer, nb.snsPublisher = home.deliveryTargets()
		home.mu.RLock("inheritTemplates")
		nb.templates = home.templates
		home.mu.RUnlock()

		ah := NewHandler(nb)
		ah.enableRegions()

		return ah
	})
}

func (h *Handler) enableRegions() {
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

// RegionBackends returns the home backend followed by every region and account sibling built so far.
func (h *Handler) RegionBackends() []*InMemoryBackend {
	peers := h.peers.All()
	out := make([]*InMemoryBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	for _, a := range h.accounts.all() {
		out = append(out, a.RegionBackends()...)
	}

	return out
}

func (b *InMemoryBackend) deliveryTargets() (S3Writer, SNSPublisher) {
	b.mu.RLock("deliveryTargets")
	defer b.mu.RUnlock()

	return b.s3Writer, b.snsPublisher
}

// BackendForAccount returns the backend serving account in region; home and unknown accounts use the home set.
func (h *Handler) BackendForAccount(account, region string) *InMemoryBackend {
	if a := h.accounts.get(account); a != nil {
		return a.BackendFor(region)
	}

	return h.BackendFor(region)
}
