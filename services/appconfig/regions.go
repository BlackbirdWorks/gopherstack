package appconfig

import "github.com/blackbirdworks/gopherstack/pkgs/regionpeers"

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(home.accountID, region)
		nb.SetAppConfig(home.appConfig)

		if h.publisherFor != nil {
			nb.SetDeployedConfigurationPublisher(h.publisherFor(region))
		}

		if h.readerFor != nil {
			nb.SetConfigurationContentReader(h.readerFor(region))
		}

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

// SetPublisherResolver sends every region's completed deployments to publisherFor(region).
func (h *Handler) SetPublisherResolver(publisherFor func(region string) DeployedConfigurationPublisher) {
	h.publisherFor = publisherFor

	if home, ok := h.Backend.(*InMemoryBackend); ok {
		home.SetDeployedConfigurationPublisher(publisherFor(home.region))
	}
}

// SetContentReaderResolver serves non-hosted profile content for every region from readerFor(region).
func (h *Handler) SetContentReaderResolver(readerFor func(region string) ConfigurationContentReader) {
	h.readerFor = readerFor

	if home, ok := h.Backend.(*InMemoryBackend); ok {
		home.SetConfigurationContentReader(readerFor(home.region))
	}
}
