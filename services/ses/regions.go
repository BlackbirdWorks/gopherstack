package ses

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackend().WithRegion(region).WithAccountID(home.accountID)

		home.mu.RLock("newRegionBackend")
		nb.snsPublisher = home.snsPublisher
		nb.relay = home.relay
		nb.emailTTL, nb.configuredEmailTTL = home.emailTTL, home.configuredEmailTTL
		nb.limits, nb.configuredLimits = home.limits, home.configuredLimits
		home.mu.RUnlock()

		p := NewHandler(nb)

		if h.janitor != nil {
			p.WithJanitor(h.janitor.Interval, h.janitor.TaskTimeout)
		}

		if ctx := h.workerCtx.Load(); ctx != nil {
			_ = p.StartWorker(*ctx)
		}

		return p
	})
}

// MailBackends returns the home backend plus every built regional sibling.
func (h *Handler) MailBackends() []*InMemoryBackend {
	var out []*InMemoryBackend

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		out = append(out, b)
	}

	for _, p := range h.peers.All() {
		if b, ok := p.Backend.(*InMemoryBackend); ok {
			out = append(out, b)
		}
	}

	return out
}

func (h *Handler) startRegionWorkers(ctx context.Context) {
	h.workerCtx.Store(&ctx)

	for _, p := range h.peers.All() {
		_ = p.StartWorker(ctx)
	}
}

func (h *Handler) stopRegionWorkers(ctx context.Context) {
	for _, p := range h.peers.All() {
		p.janitorRun.Stop(ctx)
	}
}

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}
