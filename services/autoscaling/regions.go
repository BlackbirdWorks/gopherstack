package autoscaling

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own scheduled-action scheduler running under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackendWithConfig(home.accountID, region)
		nb.inheritWiring(home)

		p := NewHandler(nb)
		_ = p.StartWorker(ctx)

		return p
	})
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
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

func (h *Handler) closePeers(ctx context.Context) {
	for _, p := range h.peers.Drain() {
		p.stopOwn(ctx)
	}
}

func (h *Handler) stopOwn(ctx context.Context) {
	h.schedulerRun.Stop(ctx)

	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}

func (b *InMemoryBackend) inheritWiring(home *InMemoryBackend) {
	home.mu.RLock("inheritWiring")
	defer home.mu.RUnlock()

	b.ec2Launcher = home.ec2Launcher
	b.instanceTypeResolver = home.instanceTypeResolver
	b.elbv2Registrar = home.elbv2Registrar
	b.elbRegistrar = home.elbRegistrar
	b.ec2Lookup = home.ec2Lookup
}
