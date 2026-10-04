package codebuild

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own janitor running under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(h.Backend.region, func(region string) *Handler {
		p := NewHandler(NewInMemoryBackend(h.Backend.accountID, region))

		if h.janitor == nil {
			return p
		}

		p.WithJanitor(h.janitor.Interval, h.janitor.BuildTTL, h.janitor.TaskTimeout)

		var pctx context.Context

		pctx, p.stop = context.WithCancel(ctx)
		go p.janitor.Run(pctx)

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
