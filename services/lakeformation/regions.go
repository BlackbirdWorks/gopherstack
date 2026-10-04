package lakeformation

import (
	"context"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions(ctx context.Context) {
	h.peers = regionpeers.New(h.DefaultRegion, func(region string) *Handler {
		pctx, cancel := context.WithCancel(ctx)
		nb := NewInMemoryBackend()
		nb.StartJanitor(pctx)

		p := NewHandler(nb)
		p.AccountID = h.AccountID
		p.DefaultRegion = region
		p.stop = cancel

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
		func(p *Handler) { p.stop() },
	)
}

// Reset clears every region.
func (h *Handler) Reset() {
	h.resetHome()

	for _, p := range h.peers.Drain() {
		p.stop()
	}
}

// Shutdown stops the workers of every region.
func (h *Handler) Shutdown(context.Context) {
	for _, p := range h.peers.Drain() {
		p.stop()
	}
}
