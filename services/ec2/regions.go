package ec2

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// regionCompute is implemented by compute providers that render region-specific
// details (public DNS names) and so need a per-region view.
type regionCompute interface {
	forRegion(region string) Compute
}

// EnableRegions makes h serve every other region through lazily built per-region
// siblings, each with its own default VPC, lifecycle reconciler and janitor under ctx.
func (h *Handler) EnableRegions(ctx context.Context) {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.peers = regionpeers.New(home.Region, func(region string) *Handler {
		return h.buildPeer(ctx, home, region)
	})
	home.regionBackend = h.regionBackend
	home.allBackends = h.memBackends
}

func (h *Handler) buildPeer(ctx context.Context, home *InMemoryBackend, region string) *Handler {
	pctx, stop := context.WithCancel(ctx)

	nb := NewInMemoryBackend(home.AccountID, region)
	nb.inheritWiring(home, region)
	nb.StartLifecycleReconciler(pctx)

	p := NewHandler(nb)
	p.AccountID = home.AccountID
	p.Region = region
	p.svcCtx = pctx
	p.stop = stop

	if h.janitor != nil {
		p.WithJanitor(h.janitor.Interval, h.janitor.TerminatedTTL, h.janitor.CancelledSpotTTL, h.janitor.TaskTimeout)
		go p.janitor.Run(pctx)
	}

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

// sourceTags returns the tags of id from whichever region holds it, home first.
func (h *Handler) sourceTags(id string) map[string]string {
	if tags := h.Backend.TagsForResource(id); len(tags) > 0 {
		return tags
	}

	mem, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	for _, bk := range mem.otherBackends() {
		if tags := bk.TagsForResource(id); len(tags) > 0 {
			return tags
		}
	}

	return nil
}

func (h *Handler) memBackends() []*InMemoryBackend {
	var out []*InMemoryBackend

	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			out = append(out, mem)
		}
	}

	return out
}

func (h *Handler) regionBackend(region string) *InMemoryBackend {
	bk, _ := h.BackendFor(region).(*InMemoryBackend)

	return bk
}

// closePeer drops a sibling and removes its containers; stopPeer leaves them running, as the home backend does.
func (h *Handler) closePeer() {
	h.releaseCompute()
	h.stopPeer()
}

func (h *Handler) stopPeer() {
	if h.stop != nil {
		h.stop()
	}

	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		mem.StopLifecycleReconciler()
	}

	h.Backend.Reset()
}

func (h *Handler) closePeers(release bool) {
	for _, p := range h.peers.Drain() {
		if release {
			p.closePeer()
		} else {
			p.stopPeer()
		}
	}
}

func (b *InMemoryBackend) inheritWiring(home *InMemoryBackend, region string) {
	home.mu.RLock("inheritWiring")
	compute, dns, cfg := home.compute, home.dnsRegistrar, home.appConfig
	resolve, all := home.regionBackend, home.allBackends
	home.mu.RUnlock()

	if rc, ok := compute.(regionCompute); ok {
		compute = rc.forRegion(region)
	}

	b.compute = compute
	b.dnsRegistrar = dns
	b.appConfig = cfg
	b.regionBackend = resolve
	b.allBackends = all
	b.metrics.Set(home.metrics.Emitter())
}
