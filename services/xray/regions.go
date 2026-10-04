package xray

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own janitor running under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		p := NewHandler(NewInMemoryBackend(home.accountID, region))

		if h.janitor == nil {
			return p
		}

		p.WithJanitor(h.janitor.Interval, h.janitor.TraceTTL, h.janitor.TaskTimeout)

		var pctx context.Context

		pctx, p.stop = context.WithCancel(ctx)
		go p.janitor.Run(pctx)

		return p
	})
}

func (h *Handler) close() {
	h.Backend.Reset()

	if h.stop != nil {
		h.stop()
	}
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
