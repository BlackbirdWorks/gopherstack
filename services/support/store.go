package support

import (
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory store for Support cases.
type InMemoryBackend struct {
	// registry holds every "clean" store.Table (cases, attachments,
	// checkRefreshStatuses, checkResults) so their Reset/Snapshot/Restore
	// collapse to one call each. attachmentSets is a "dirty" table (see
	// store_setup.go) and is NOT on this registry.
	registry             *store.Registry
	cases                *store.Table[Case]
	attachmentSets       *store.Table[AttachmentSet]
	attachments          *store.Table[Attachment]
	checkRefreshStatuses *store.Table[TrustedAdvisorCheckRefreshStatus]
	checkResults         *store.Table[TrustedAdvisorCheckResult]
	// communications (caseID -> communications) is deliberately left a plain
	// map: case communications are ORDER-SENSITIVE (chronological) and
	// store.Table makes no iteration-order guarantee -- see store_setup.go.
	communications map[string][]Communication
	nextDisplayID  uint64
	mu             sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		communications: make(map[string][]Communication),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.communications = make(map[string][]Communication)
	b.nextDisplayID = 0
}

// resetTablesLocked resets every store.Table-backed resource field to empty:
// the "clean" tables via one b.registry.ResetAll() call, plus the "dirty"
// attachmentSets table individually since it is not registered on b.registry
// (see store_setup.go). The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.attachmentSets.Reset()
}
