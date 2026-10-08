package lambda

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

// regionalCWLogs is implemented by log sinks that can be bound to another region.
type regionalCWLogs interface {
	ForRegion(region string) CWLogsBackend
}

// EnableRegions makes h serve every other region through lazily built per-region siblings.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackendWithContext(
			home.ctx, home.docker, home.portAlloc, home.settings, home.accountID, region)
		nb.inheritWiring(home)

		p := NewHandler(nb)
		p.DefaultRegion = region
		p.AccountID = h.AccountID

		if ctx := h.workerCtx.Load(); ctx != nil {
			nb.startWorkers(*ctx)
		}

		return p
	})

	home.regionBackend = h.backendInRegion
}

func (h *Handler) backendInRegion(region string) *InMemoryBackend {
	if p := h.peers.Get(region); p != nil {
		bk, _ := p.Backend.(*InMemoryBackend)

		return bk
	}

	bk, _ := h.Backend.(*InMemoryBackend)

	return bk
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
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

// RegionHandler returns the handler serving region: h itself for the home region, else its sibling.
func (h *Handler) RegionHandler(region string) *Handler {
	if p := h.peers.Get(region); p != nil {
		return p
	}

	return h
}

// CloseRegions closes every sibling backend, releasing its URL servers and runtimes.
func (h *Handler) CloseRegions(ctx context.Context) {
	for _, p := range h.peers.Drain() {
		p.closeBackend(ctx)
	}
}

func (h *Handler) closeBackend(ctx context.Context) {
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		bk.Close(ctx)
	}
}

func (b *InMemoryBackend) inheritWiring(home *InMemoryBackend) {
	home.mu.RLock("inheritWiring")
	defer home.mu.RUnlock()

	b.s3Fetcher = home.s3Fetcher
	b.ecrResolver = home.ecrResolver
	b.dnsRegistrar = home.dnsRegistrar
	b.asyncDelivery = home.asyncDelivery
	b.sigV4Secret = home.sigV4Secret
	b.activationDelay = home.activationDelay
	b.pcActivationDelay = home.pcActivationDelay
	b.regionBackend = home.regionBackend
	b.cwLogs = home.cwLogs

	if e := home.metrics.Emitter(); e != nil {
		b.metrics.Set(e)
	}

	if r, ok := home.cwLogs.(regionalCWLogs); ok {
		b.cwLogs = r.ForRegion(b.region)
	}

	if home.kinesisPoller != nil {
		b.kinesisPoller = home.kinesisPoller.siblingFor(b)
	}
}

func (p *EventSourcePoller) siblingFor(b *InMemoryBackend) *EventSourcePoller {
	p.mu.RLock("siblingFor")
	defer p.mu.RUnlock()

	sib := NewEventSourcePoller(b, p.kinesisReader)
	sib.sqsReader = p.sqsReader
	sib.ddbStreamsReader = p.ddbStreamsReader
	sib.mskResolver = p.mskResolver
	sib.mqResolver = p.mqResolver
	sib.mqSecrets = p.mqSecrets

	return sib
}

// startWorkers starts the ESM poller and janitor once; later calls are no-ops.
func (b *InMemoryBackend) startWorkers(ctx context.Context) {
	b.workersOnce.Do(func() {
		select {
		case <-b.shutdown:
			return
		default:
		}

		b.StartKinesisPoller(ctx)

		jctx, cancel := context.WithCancel(ctx)
		if !b.setJanitorCancel(cancel) {
			cancel()

			return
		}

		go NewJanitor(b, b.settings).Run(jctx)
	})
}

// routeRegion returns the backend owning name's region (ARN region, else the context region), or nil to stay local.
func (b *InMemoryBackend) routeRegion(ctx context.Context, name string) *InMemoryBackend {
	if b.regionBackend == nil {
		return nil
	}

	region := arnRegionOf(name)
	if region == "" {
		region = awsmeta.Region(ctx)
	}

	if region == "" || region == b.region {
		return nil
	}

	if sib := b.regionBackend(region); sib != nil && sib != b {
		return sib
	}

	return nil
}

// arnRegionOf returns the region of a Lambda ARN, or "" for a bare name.
func arnRegionOf(name string) string {
	const arnFields = 6

	parts := strings.SplitN(name, ":", arnFields)
	if len(parts) < arnFields || parts[0] != "arn" {
		return ""
	}

	return parts[3]
}

func (b *InMemoryBackend) setJanitorCancel(cancel context.CancelFunc) bool {
	b.mu.Lock("setJanitorCancel")
	defer b.mu.Unlock()

	if b.janitorCancel != nil {
		return false
	}

	b.janitorCancel = cancel

	return true
}
