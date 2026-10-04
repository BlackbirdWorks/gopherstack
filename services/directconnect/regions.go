package directconnect

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home := h.Backend
	if home == nil {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(home.baseCtx, home.accountID, region)
		nb.SetEC2GatewayResolver(home.ec2GatewayResolver())
		nb.gatewayHome = home

		return NewHandler(nb)
	})
}

func (b *InMemoryBackend) ec2GatewayResolver() EC2GatewayResolver {
	b.mu.RLock("ec2GatewayResolver")
	defer b.mu.RUnlock()

	return b.ec2Resolver
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

// hasGatewayLocked reports whether the account-global gateway id exists; callers hold b.mu.
func (b *InMemoryBackend) hasGatewayLocked(id string) bool {
	if b.gatewayHome != nil {
		return b.gatewayHome.hasGateway(id)
	}

	_, ok := b.gateways.Get(id)

	return ok
}

func (b *InMemoryBackend) hasGateway(id string) bool {
	b.mu.RLock("hasGateway")
	defer b.mu.RUnlock()

	_, ok := b.gateways.Get(id)

	return ok
}

func (b *InMemoryBackend) gatewayTags(id string) (*tags.Tags, bool) {
	b.mu.RLock("gatewayTags")
	defer b.mu.RUnlock()

	g, ok := b.gateways.Get(id)
	if !ok {
		return nil, false
	}

	return g.Tags, true
}

// ownGateways lists the gateways b stores; siblings hold none, since gateways live in the home backend.
func (b *InMemoryBackend) ownGateways() []*DirectConnectGateway {
	if b.gatewayHome != nil {
		return nil
	}

	return b.gateways.Snapshot()
}

// isGatewayOp reports whether op acts on account-global Direct Connect gateway state.
func isGatewayOp(op string) bool {
	return strings.Contains(op, "DirectConnectGateway")
}
