package bedrockruntime

import (
	"context"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend stores Bedrock Runtime state in memory.
type InMemoryBackend struct {
	svcCtx             context.Context
	mu                 *lockmetrics.RWMutex
	registry           *store.Registry
	asyncInvokes       *store.Table[AsyncInvoke]
	tokenIndex         map[string]string
	accountID          string
	region             string
	invocations        invocationRing
	asyncInvokeCounter int
}

// NewInMemoryBackend creates a new InMemoryBackend with a background service context.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose background
// goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		invocations: newInvocationRing(maxInvocationHistory),
		tokenIndex:  make(map[string]string),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("bedrockruntime"),
		svcCtx:      svcCtx,
		registry:    store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state, returning the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.invocations.reset()
	b.registry.ResetAll()
	b.tokenIndex = make(map[string]string)
	b.asyncInvokeCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RecordInvocation stores a completed invocation in memory.
// When the ring is full, the oldest entry is evicted and a warning is logged via the background context.
func (b *InMemoryBackend) RecordInvocation(operation, modelID, input, output string) *Invocation {
	b.mu.Lock("RecordInvocation")
	defer b.mu.Unlock()

	prevEvictions := b.invocations.evictions
	inv := &Invocation{
		Operation: operation,
		ModelID:   modelID,
		Input:     input,
		Output:    output,
		CreatedAt: time.Now().UTC(),
	}
	b.invocations.push(inv)

	if b.invocations.evictions > prevEvictions {
		logger.Load(b.svcCtx).Warn(
			"bedrockruntime: invocationRing full, oldest entry evicted",
			"capacity", len(b.invocations.buf),
		)
	}

	cp := *inv

	return &cp
}

// ListInvocations returns all recorded invocations in insertion order (oldest first).
func (b *InMemoryBackend) ListInvocations() []*Invocation {
	b.mu.RLock("ListInvocations")
	defer b.mu.RUnlock()

	raw := b.invocations.snapshot()
	out := make([]*Invocation, len(raw))

	for i, inv := range raw {
		cp := *inv
		out[i] = &cp
	}

	return out
}

// Purge removes all model invocations recorded before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	keep := b.invocations.snapshot()
	b.invocations.reset()

	for _, inv := range keep {
		if ctx.Err() != nil {
			return
		}
		if !inv.CreatedAt.Before(cutoff) {
			b.invocations.push(inv)
		}
	}
}

// copyTags returns a shallow copy of the given tag map; nil-safe.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	cp := make(map[string]string, len(tags))

	maps.Copy(cp, tags)

	return cp
}
