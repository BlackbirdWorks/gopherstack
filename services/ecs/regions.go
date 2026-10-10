package ecs

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// regionRunner is implemented by runners that keep per-task state and so need
// their own instance for each region's backend.
type regionRunner interface {
	forRegion() TaskRunner
}

func (r *realDockerRunner) forRegion() TaskRunner {
	return newDockerRunnerWithClient(r.svcCtx, r.cli)
}

// SetCWLogsFactory sets how sibling regions resolve their CloudWatch Logs sink.
func (h *Handler) SetCWLogsFactory(f func(region string) CWLogsBackend) { h.cwLogsFor = f }

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own task runner, reconciler and janitor running under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		return h.buildPeer(ctx, home, region)
	})
}

func (h *Handler) buildPeer(ctx context.Context, home *InMemoryBackend, region string) *Handler {
	runner := home.runner
	if rr, ok := runner.(regionRunner); ok {
		runner = rr.forRegion()
	}

	nb := NewInMemoryBackend(home.accountID, region, runner)
	nb.inheritWiring(home)

	if r, ok := runner.(taskCompletionRunner); ok {
		r.SetTaskCompletionHandler(nb.markTaskStoppedByContainerExit)
	}

	cwl := home.currentCWLogs()
	if h.cwLogsFor != nil {
		cwl = h.cwLogsFor(region)
	}

	if cwl != nil {
		nb.SetCWLogsBackend(cwl)
	}

	p := NewHandler(nb)
	pctx, cancel := context.WithCancel(ctx)
	p.stop = cancel

	rec := NewReconciler(nb)
	nb.RegisterClusterDeleteHook(rec.EvictCluster)

	go rec.Start(pctx)
	go NewJanitor(nb, 0).Run(pctx)

	return p
}

// BackendFor returns the backend serving region: the home backend, or the sibling
// for any other region (built on first use).
func (h *Handler) BackendFor(region string) Backend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []Backend {
	peers := h.peers.All()
	out := make([]Backend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}

func (h *Handler) closePeer() {
	if h.stop != nil {
		h.stop()
	}

	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

func (h *Handler) closePeers() {
	for _, p := range h.peers.Drain() {
		p.closePeer()
	}
}

func (b *InMemoryBackend) inheritWiring(home *InMemoryBackend) {
	home.mu.RLock("inheritWiring")
	defer home.mu.RUnlock()

	b.elbv2Registrar = home.elbv2Registrar
	b.asgResolver = home.asgResolver
	b.alarmStates = home.alarmStates
	b.lambdaInvoker = home.lambdaInvoker
	b.metrics.Set(home.metrics.Emitter())
	b.stopDelay = home.stopDelay
	b.startDelay = home.startDelay
}

func (b *InMemoryBackend) currentCWLogs() CWLogsBackend {
	b.mu.RLock("currentCWLogs")
	defer b.mu.RUnlock()

	return b.cwLogs
}
