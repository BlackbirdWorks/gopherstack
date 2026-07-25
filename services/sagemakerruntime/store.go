package sagemakerruntime

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend stores SageMaker Runtime state in memory.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment for why sessions and
	// asyncInvocations are both "clean" tables (registered directly, no
	// DTO-registry needed).
	registry         *store.Registry
	mu               *lockmetrics.RWMutex
	sessions         *store.Table[Session]
	asyncInvocations *store.Table[AsyncInvocation]
	// endpointLookup, when wired via SetEndpointLookup, lets InvokeEndpoint/
	// InvokeEndpointAsync/InvokeEndpointWithResponseStream validate
	// EndpointName against services/sagemaker's real endpoint registry. See
	// endpoint_lookup.go.
	endpointLookup EndpointLookup
	accountID      string
	region         string
	invocations    []*Invocation
	nextID         uint64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:    store.NewRegistry(),
		invocations: make([]*Invocation, 0),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("sagemakerruntime"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// evictOldest enforces a FIFO size bound on t: while the table exceeds
// maxSize, it repeatedly removes the entry with the oldest timestamp (as
// reported by createdAt), identified via idFn. This keeps long-running
// sessions/async-invocation tables from growing without bound.
func evictOldest[V any](t *store.Table[V], maxSize int, idFn func(*V) string, createdAt func(*V) time.Time) {
	for t.Len() > maxSize {
		var (
			oldestID   string
			oldestTime time.Time
			found      bool
		)

		for _, v := range t.All() {
			ts := createdAt(v)
			if !found || ts.Before(oldestTime) {
				oldestID = idFn(v)
				oldestTime = ts
				found = true
			}
		}

		if !found {
			return
		}

		t.Delete(oldestID)
	}
}
