package networkfirewall

import (
	"context"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	h.peers = regionpeers.New(h.DefaultRegion, func(region string) *Handler {
		bk := NewInMemoryBackend(h.AccountID, region)
		bk.SetSubnetResolver(h.Backend.resolver())
		bk.SetTransitionDelay(h.Backend.transitionDelay())
		bk.SetTransitGatewayResolver(h.Backend.tgwResolver())

		p := NewHandler(bk)
		p.AccountID = h.AccountID
		p.DefaultRegion = region

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

// BackendFor returns the backend serving region.
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

// SetSubnetResolver wires EC2 subnet lookups into the home backend and all current and future siblings.
func (h *Handler) SetSubnetResolver(r SubnetResolver) {
	h.Backend.SetSubnetResolver(r)

	for _, p := range h.peers.All() {
		p.Backend.SetSubnetResolver(r)
	}
}

// Handler serves each request from the handler of its region.
func (h *Handler) Handler() echo.HandlerFunc {
	home := h.homeHandler()

	return func(c *echo.Context) error {
		if p := h.peers.Get(awsmeta.Region(c.Request().Context())); p != nil {
			return p.homeHandler()(c)
		}

		return home(c)
	}
}

// Snapshot implements persistence.Persistable; other regions ride in an additive "regions" key.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.peers.Snapshot(h.Backend.Snapshot(ctx), func(p *Handler) []byte { return p.Backend.Snapshot(ctx) })
}

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return err
	}

	return h.peers.Restore(
		data,
		func(p *Handler, d []byte) error { return p.Backend.Restore(ctx, d) },
		func(p *Handler) { p.Backend.Reset() },
	)
}

// Reset clears every region.
func (h *Handler) Reset() {
	h.Backend.Reset()

	for _, p := range h.peers.Drain() {
		p.Backend.Reset()
	}
}
