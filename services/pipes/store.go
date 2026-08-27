package pipes

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Pipes resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend is the in-memory store for pipes.
//
// All resource collections are nested by region (outer key = region) so that
// same-named pipes are isolated across regions. The per-region *store.Table /
// *store.Index values are created lazily via the pipesTable /
// pipesByARNIndex / enrichmentCountsTable accessors in store_setup.go —
// mirroring the lazy map creation the hand-rolled code did before Phase 3.3's
// pkgs/store conversion. Callers must hold b.mu while accessing them.
type InMemoryBackend struct {
	svcCtx context.Context
	// registry is the lifecycle registry for every *store.Table this backend
	// owns; Reset/Snapshot/Restore drive it via ResetAll/SnapshotAll/RestoreAll
	// instead of hand-written per-map boilerplate. See store_setup.go.
	registry *store.Registry
	// pipes holds one *store.Table[Pipe] per region, keyed by pipe name.
	pipes map[string]*store.Table[Pipe]
	// pipesByARN is a secondary index on pipes, keyed by ARN, one per region.
	// It replaces the hand-rolled pipeARNIndex map and is kept consistent
	// automatically by store.Table.Put/Delete/Restore.
	pipesByARN map[string]*store.Index[Pipe]
	// enrichmentCounts holds one *store.Table[enrichmentCounter] per region,
	// keyed by pipe name. enrichmentCounter is an identity-less DTO (a plain
	// int64 counter) wrapped so it can be keyed by store.Table.
	enrichmentCounts map[string]*store.Table[enrichmentCounter]
	mu               *lockmetrics.RWMutex
	cancel           context.CancelFunc
	accountID        string
	region           string
	wg               sync.WaitGroup
}

// NewInMemoryBackend creates a new InMemoryBackend with a background lifecycle
// context. Prefer [NewInMemoryBackendWithContext] when a service context is
// available so delayed state transitions are cancelled on shutdown.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose delayed
// state-transition goroutines are tied to svcCtx, so they are cancelled when the
// service shuts down. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)

	return &InMemoryBackend{
		registry:         store.NewRegistry(),
		pipes:            make(map[string]*store.Table[Pipe]),
		pipesByARN:       make(map[string]*store.Index[Pipe]),
		enrichmentCounts: make(map[string]*store.Table[enrichmentCounter]),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("pipes"),
		svcCtx:           ctx,
		cancel:           cancel,
	}
}

// runDelayed runs fn after delay, unless the backend's lifecycle context is
// cancelled first. The goroutine is tracked by b.wg so [InMemoryBackend.Shutdown]
// can wait for it.
func (b *InMemoryBackend) runDelayed(fn func()) {
	b.wg.Go(func() {
		t := time.NewTimer(stateTransitionDelay)
		defer t.Stop()

		select {
		case <-b.svcCtx.Done():
			return
		case <-t.C:
		}

		fn()
	})
}

// Shutdown cancels in-flight delayed state transitions and waits for their
// goroutines to exit, bounded by ctx. It implements the service shutdown
// contract used by the handler.
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	if b.cancel != nil {
		b.cancel()
	}

	done := make(chan struct{})

	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
}
