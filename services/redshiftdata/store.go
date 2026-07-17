package redshiftdata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionStore holds per-region statement storage and its ring buffer.
//
// Phase 3.3 datalayer audit: statements stays a raw map[string]*Statement
// rather than a [github.com/blackbirdworks/gopherstack/pkgs/store.Table] for
// two independent reasons. (1) ringBuf/ringLen/ringHead implement FIFO
// insertion-order tracking for the per-region maxStatementHistory eviction
// cap; store.Index groups explicitly do not preserve insertion order across
// deletions (swap-and-truncate removal), so replacing this hand-rolled ring
// buffer with an Index would silently corrupt eviction order -- exactly the
// "leave raw" case the rollout calls out for order-sensitive statement
// history. (2) regionStore instances are created lazily per-region by
// storeFor() rather than known at backend-construction time, so there is no
// fixed set of tables to register once with a store.Registry the way every
// other Phase 3.3 conversion does. This was already persisted before this
// rollout (see persistence.go) and remains persisted, unchanged, after it.
type regionStore struct {
	statements map[string]*Statement
	// ring buffer for ordered eviction – head points to the oldest slot.
	ringBuf  [maxStatementHistory]string
	ringLen  int
	ringHead int
}

// addStatement inserts a statement and evicts the oldest via the ring buffer if
// the cap is exceeded. Caller must hold the backend write lock.
func (s *regionStore) addStatement(stmt *Statement) {
	s.statements[stmt.ID] = stmt

	if s.ringLen < maxStatementHistory {
		tail := (s.ringHead + s.ringLen) % maxStatementHistory
		s.ringBuf[tail] = stmt.ID
		s.ringLen++

		return
	}

	delete(s.statements, s.ringBuf[s.ringHead])
	s.ringBuf[s.ringHead] = stmt.ID
	s.ringHead = (s.ringHead + 1) % maxStatementHistory
}

// compactRingBuffer rebuilds the ring buffer from the current statements map,
// preserving insertion order. Must be called with the backend write lock held.
func (s *regionStore) compactRingBuffer() {
	kept := make([]string, 0, s.ringLen)

	for i := range s.ringLen {
		id := s.ringBuf[(s.ringHead+i)%maxStatementHistory]
		if _, ok := s.statements[id]; ok {
			kept = append(kept, id)
		}
	}

	s.ringHead = 0
	s.ringLen = len(kept)

	copy(s.ringBuf[:], kept)

	for i := s.ringLen; i < maxStatementHistory; i++ {
		s.ringBuf[i] = ""
	}
}

// InMemoryBackend is an in-memory store for Redshift Data API statements.
// All regional resource maps are nested by region (outer key = region) so that
// the same-named statement in two regions are fully isolated.
type InMemoryBackend struct {
	stores        map[string]*regionStore
	mu            *lockmetrics.RWMutex
	accountID     string
	defaultRegion string
}

// NewInMemoryBackend creates a new in-memory Redshift Data backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		stores:        make(map[string]*regionStore),
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("redshiftdata"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// storeFor returns the regionStore for the given region, creating it on first use.
// Caller must hold b.mu write lock.
func (b *InMemoryBackend) storeFor(region string) *regionStore {
	if b.stores[region] == nil {
		b.stores[region] = &regionStore{
			statements: make(map[string]*Statement),
		}
	}

	return b.stores[region]
}

// storeForRead returns the regionStore for the given region, or nil if none exists.
// Caller must hold b.mu (read or write). Does not create a store.
func (b *InMemoryBackend) storeForRead(region string) *regionStore {
	return b.stores[region]
}

// Reset clears all stored statements across all regions.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.stores = make(map[string]*regionStore)
}

// cloneStatement returns a deep copy of stmt.
func cloneStatement(stmt *Statement) *Statement {
	cp := *stmt

	if stmt.QueryStrings != nil {
		cp.QueryStrings = append([]string(nil), stmt.QueryStrings...)
	}

	if stmt.Parameters != nil {
		cp.Parameters = append([]SQLParameter(nil), stmt.Parameters...)
	}

	if stmt.SubStatements != nil {
		cp.SubStatements = append([]SubStatementData(nil), stmt.SubStatements...)
	}

	return &cp
}
