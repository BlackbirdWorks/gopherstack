package outposts

import (
	"context"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions(ctx context.Context) {
	home := h.Backend

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend(ctx, home.accountID, region)

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
	return h.peers.Snapshot(h.homeSnapshot(ctx), func(p *Handler) []byte { return p.homeSnapshot(ctx) })
}

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.homeRestore(ctx, data); err != nil {
		return err
	}

	return h.peers.Restore(
		data,
		func(p *Handler, d []byte) error { return p.homeRestore(ctx, d) },
		func(p *Handler) { p.Backend.Close() },
	)
}

// Reset clears every region.
func (h *Handler) Reset() {
	h.resetHome()

	for _, p := range h.peers.Drain() {
		p.Backend.Close()
	}
}

// Shutdown stops the workers of every region.
func (h *Handler) Shutdown(ctx context.Context) {
	h.shutdownHome(ctx)

	for _, p := range h.peers.Drain() {
		p.shutdownHome(ctx)
	}
}
