package apigatewaymanagementapi

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// EnableRegions makes h, the home region's handler, serve every other region through per-region siblings.
func (h *Handler) EnableRegions(home string) {
	h.peers = regionpeers.New(home, func(_ string) *Handler {
		p := NewHandler(NewInMemoryBackend())
		p.taskTimeout = h.taskTimeout

		if ctx := h.workerCtx.Load(); ctx != nil {
			p.startJanitor(*ctx)
		}

		return p
	})
}

// StartJanitor runs the idle-connection janitor of h and every sibling until ctx ends.
func (h *Handler) StartJanitor(ctx context.Context, taskTimeout time.Duration) {
	h.taskTimeout = taskTimeout
	h.workerCtx.Store(&ctx)
	h.startJanitor(ctx)

	for _, p := range h.peers.All() {
		p.taskTimeout = taskTimeout
		p.startJanitor(ctx)
	}
}

// startJanitor runs h's janitor once, until ctx ends or stopWorkers.
func (h *Handler) startJanitor(ctx context.Context) {
	jctx, cancel := context.WithCancel(ctx)
	if !h.stopJanitor.CompareAndSwap(nil, &cancel) {
		cancel()

		return
	}

	janitor := NewJanitor(h.Backend, 0, 0)
	janitor.TaskTimeout = h.taskTimeout

	go janitor.Run(jctx)
}

func (h *Handler) stopWorkers() {
	if c := h.stopJanitor.Swap(nil); c != nil {
		(*c)()
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
